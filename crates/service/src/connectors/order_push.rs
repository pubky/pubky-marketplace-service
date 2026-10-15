//! Building the store order from a paid Pubky order.
//!
//! Reads the order row directly: the seller order export deliberately omits
//! payments and addresses, and so does this unless the buyer opted in. The
//! store order carries no buyer pubky. It carries a delivery address only
//! when the buyer ticked "share my address with <shop>" at checkout: the
//! worker then opens the order's connector envelope (sealed to this
//! connection's recipient key) for this one push. Without an opt-in the
//! address stays sealed to the seller's delivery key, which the service
//! cannot open, and the store order names only the Pubky order id.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::keys::ConnectorKeys;
use super::ledger::reason;
use super::provider::{OrderPush, OrderPushLine, ShippingAddress};
use super::recipient;
use super::store::{self, ConnectionRow};

#[derive(Debug, sqlx::FromRow)]
pub struct OrderSnapshot {
    pub id: Uuid,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub state: String,
    pub lines: Value,
    pub subtotal_minor: i64,
    pub shipping_minor: i64,
    pub total_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub payment_method: Option<String>,
    pub external_refund: Option<Value>,
    pub checkout_command_id: Option<Uuid>,
    pub connector_share_domain: Option<String>,
    pub connector_address_envelope: Option<Vec<u8>>,
    pub connector_key_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

pub async fn load_order(
    pool: &PgPool,
    order_id: Uuid,
) -> Result<Option<OrderSnapshot>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, buyer_pubky, seller_pubky, state, lines, subtotal_minor, shipping_minor, \
                total_minor, currency, exponent, payment_method, external_refund, \
                checkout_command_id, connector_share_domain, connector_address_envelope, \
                connector_key_id, created_at \
         FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await
}

/// When the payment confirmed: the instant of the order's
/// `payment.confirmed` event.
pub async fn paid_at(pool: &PgPool, order_id: Uuid) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT MIN(e.occurred_at) FROM events e \
         JOIN payments p ON e.aggregate_id = 'payment:' || p.id::text \
         WHERE p.order_id = $1 AND e.kind = 'payment.confirmed'",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
}

/// Why a push is withheld, as a ledger quarantine reason and a sentence for
/// the seller.
#[derive(Debug, PartialEq, Eq)]
pub struct Withheld {
    pub reason: &'static str,
    pub detail: String,
}

fn withheld(reason: &'static str, detail: impl Into<String>) -> Withheld {
    Withheld {
        reason,
        detail: detail.into(),
    }
}

pub fn pubky_listing_id(seller_pubky: &str, listing_aggregate_id: &str) -> Option<String> {
    listing_aggregate_id
        .strip_prefix(&format!("listing:{seller_pubky}_"))
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

/// The address the buyer chose to share with this connection's store, or
/// `None` when they did not, the envelope is gone, or it cannot be opened.
/// An envelope that cannot be opened is logged (order id only, never the
/// envelope) and the order is pushed without an address.
pub async fn shared_address(
    pool: &PgPool,
    keys: &ConnectorKeys,
    connection: &ConnectionRow,
    order: &OrderSnapshot,
) -> Result<Option<ShippingAddress>, sqlx::Error> {
    let (Some(envelope), Some(key_id), Some(command_id)) = (
        order.connector_address_envelope.as_deref(),
        order.connector_key_id.as_deref(),
        order.checkout_command_id,
    ) else {
        return Ok(None);
    };
    if order.connector_share_domain.as_deref() != Some(connection.shop.as_str()) {
        return Ok(None);
    }
    let secret = match store::open_recipient_secret(pool, keys, connection, key_id).await {
        Ok(Some(secret)) => secret,
        Ok(None) => {
            tracing::warn!(order_id = %order.id, "connector address envelope names an unknown recipient key");
            return Ok(None);
        }
        Err(store::OpenError::Database(error)) => return Err(error),
        Err(_) => {
            tracing::error!(order_id = %order.id, "connector recipient key is unreadable");
            return Ok(None);
        }
    };
    let aad = recipient::envelope_aad(
        &command_id.to_string(),
        &order.buyer_pubky,
        &order.seller_pubky,
        key_id,
    );
    match recipient::open_address(&secret, &aad, envelope) {
        Ok(address) => Ok(Some(address)),
        Err(failure) => {
            tracing::warn!(order_id = %order.id, ?failure, "connector address envelope did not open");
            Ok(None)
        }
    }
}

pub async fn build_push(
    pool: &PgPool,
    keys: &ConnectorKeys,
    connection: &ConnectionRow,
    order: &OrderSnapshot,
    paid_at: DateTime<Utc>,
) -> Result<Result<OrderPush, Withheld>, sqlx::Error> {
    match order.state.as_str() {
        "cancelled" | "refunded_external" => {
            return Ok(Err(withheld(
                reason::ORDER_CANCELLED_BEFORE_PUSH,
                "The order was cancelled before it was sent to the store.",
            )));
        }
        "pending_payment" => {
            return Ok(Err(withheld(
                reason::ORDER_NOT_PAID,
                "The order has not been paid.",
            )));
        }
        _ => {}
    }
    let Some(lines) = order.lines.as_array() else {
        return Ok(Err(withheld(
            reason::UNMAPPED_VARIANT,
            "The order has no lines.",
        )));
    };
    // Resolve every line first. An order with no line in this store is a
    // Pubky-only sale and is not the store's business; an order with some
    // lines mapped and some not cannot be pushed whole and needs the seller.
    let mut resolved = Vec::with_capacity(lines.len());
    let mut unmapped: Vec<String> = Vec::new();
    for line in lines {
        let aggregate = line
            .get("listing_aggregate_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(listing_id) = pubky_listing_id(&order.seller_pubky, aggregate) else {
            return Ok(Err(withheld(
                reason::UNMAPPED_VARIANT,
                "An order line names a listing that is not this seller's.",
            )));
        };
        let variant_id = line
            .get("variant_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut mapping =
            store::mapping_for_pubky_line(pool, connection.id, &listing_id, variant_id).await?;
        if mapping.is_none() && !variant_id.is_empty() {
            mapping = store::mapping_for_pubky_line(pool, connection.id, &listing_id, "").await?;
        }
        match mapping {
            Some(mapping) => resolved.push((line, mapping)),
            None => unmapped.push(if variant_id.is_empty() {
                format!("listing {listing_id}")
            } else {
                format!("listing {listing_id}, variant {variant_id}")
            }),
        }
    }
    if resolved.is_empty() {
        return Ok(Err(withheld(
            reason::NO_MAPPED_LINES,
            "None of the order's lines is mapped to a product in this store.",
        )));
    }
    if !unmapped.is_empty() {
        return Ok(Err(withheld(
            reason::UNMAPPED_VARIANT,
            format!(
                "{} not mapped to a product in the store.",
                if unmapped.len() == 1 {
                    format!("{} is", unmapped[0])
                } else {
                    format!("{} are", unmapped.join("; "))
                }
            ),
        )));
    }
    let mut pushed = Vec::with_capacity(resolved.len());
    for (line, mapping) in resolved {
        let quantity = line.get("quantity").and_then(Value::as_i64).unwrap_or(0);
        let unit_price_minor = line
            .get("unit_price")
            .and_then(|price| price.get("amount_minor"))
            .and_then(Value::as_i64);
        let (true, Some(unit_price_minor)) = (quantity > 0, unit_price_minor) else {
            return Ok(Err(withheld(
                reason::UNMAPPED_VARIANT,
                "An order line has no usable quantity or price.",
            )));
        };
        pushed.push(OrderPushLine {
            provider_product_id: mapping.provider_product_id,
            provider_variant_id: mapping.provider_variant_id,
            sku: mapping.sku,
            title: line
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            quantity,
            unit_price_minor,
        });
    }
    let shipping_address = shared_address(pool, keys, connection, order).await?;
    Ok(Ok(OrderPush {
        pubky_order_id: order.id,
        currency: order.currency.clone(),
        exponent: order.exponent,
        lines: pushed,
        subtotal_minor: order.subtotal_minor,
        shipping_minor: order.shipping_minor,
        total_minor: order.total_minor,
        payment_method: order
            .payment_method
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        paid_at,
        shipping_address,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_id_is_read_from_the_aggregate_of_the_same_seller() {
        assert_eq!(
            pubky_listing_id("seller", "listing:seller_boots_01").as_deref(),
            Some("boots_01")
        );
        assert_eq!(pubky_listing_id("seller", "listing:other_boots_01"), None);
        assert_eq!(pubky_listing_id("seller", "listing:seller_"), None);
        assert_eq!(pubky_listing_id("seller", ""), None);
    }
}
