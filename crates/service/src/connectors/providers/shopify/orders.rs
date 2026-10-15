//! Pushing, finding, cancelling and refunding store orders.
//!
//! A pushed order is already paid: money never moves through the store. It
//! carries the variant ids the mapping names, the Pubky prices, a manual SALE
//! transaction named after the rail the buyer paid on, the tags `pubky` and
//! `pubky-order-{id}` and a `pubky_order_id` note attribute. Inventory is
//! decremented ignoring the store's own oversell policy (the sale already
//! happened), no receipt is sent (there is no customer), and a shipping address
//! is attached only when the buyer opted in.
//!
//! `orderCreate` has no idempotency key in the Admin API. The ledger
//! therefore looks an order up before it ever pushes again after an unknown
//! outcome, and [`find`] must not trust Shopify's search index alone, which
//! lags a create by seconds: it reads the order search first and then scans
//! the most recent orders, which are not index-backed.
//!
//! `refundCreate` is the one mutation with `@idempotent`; its key is derived
//! from the Pubky refund id, so a retry after an unknown outcome returns the
//! first result instead of refunding twice.

use serde_json::{json, Value};
use uuid::Uuid;

use super::client::Client;
use super::documents;
use super::money::{decimal_to_minor, exponent, minor_to_decimal};
use crate::connectors::deterministic_uuid;
use crate::connectors::provider::{
    CancelOrder, ExternalOrder, OrderPush, ProviderError, RefundRecord, ShippingAddress,
};

pub const TAG_PUBKY: &str = "pubky";
const RECENT_ORDER_SCAN: i64 = 50;
const STAFF_NOTE_MAX: usize = 255;

pub fn order_tag(pubky_order_id: Uuid) -> String {
    format!("pubky-order-{pubky_order_id}")
}

fn rejected(code: &str, message: impl Into<String>) -> ProviderError {
    ProviderError::Rejected {
        code: code.to_string(),
        message: message.into(),
    }
}

fn malformed(what: &str) -> ProviderError {
    ProviderError::Unknown(format!("shopify returned an unexpected {what}"))
}

/// `userErrors` of a mutation payload as one message, or `None` when empty.
fn user_errors(payload: &Value, field: &str) -> Option<String> {
    let errors = payload.get(field)?.as_array()?;
    if errors.is_empty() {
        return None;
    }
    Some(
        errors
            .iter()
            .filter_map(|error| error.get("message").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("; ")
            .chars()
            .take(300)
            .collect(),
    )
}

fn money(amount_minor: i64, currency: &str, exp: i32) -> Value {
    json!({ "shopMoney": {
        "amount": minor_to_decimal(amount_minor, exp),
        "currencyCode": currency.to_ascii_uppercase(),
    }})
}

/// The name the store's payment record carries for a Pubky rail.
pub fn gateway_name(payment_method: &str) -> &'static str {
    match payment_method {
        "bitcoin" => "Pubky Bitcoin",
        "paykit" => "Pubky Paykit",
        "usdt" => "Pubky USDT",
        "paypal" => "Pubky PayPal",
        "sandbox" => "Pubky Sandbox",
        _ => "Pubky",
    }
}

fn mailing_address(address: &ShippingAddress) -> Value {
    let (first, last) = match address.name.trim().rsplit_once(' ') {
        Some((first, last)) => (first.trim().to_string(), last.trim().to_string()),
        None => (String::new(), address.name.trim().to_string()),
    };
    let mut value = json!({
        "firstName": first,
        "lastName": last,
        "address1": address.line1,
        "city": address.city,
        "zip": address.postal_code,
        "countryCode": address.country_code.to_ascii_uppercase(),
    });
    if !address.line2.is_empty() {
        value["address2"] = json!(address.line2);
    }
    if !address.region.is_empty() {
        value["provinceCode"] = json!(address.region);
    }
    value
}

/// The `order` and `options` variables of `orderCreate` for a paid Pubky
/// order, in the shop's currency. `Err` when the order cannot be represented
/// faithfully (its lines and shipping do not add up to its total).
pub fn order_create_variables(order: &OrderPush) -> Result<Value, ProviderError> {
    let currency = order.currency.to_ascii_uppercase();
    let lines_total: i64 = order
        .lines
        .iter()
        .map(|line| line.unit_price_minor.saturating_mul(line.quantity))
        .sum();
    if lines_total.saturating_add(order.shipping_minor) != order.total_minor {
        return Err(rejected(
            "total_mismatch",
            "The order's lines and shipping do not add up to its total, so the store order would not be marked paid in full.",
        ));
    }
    let line_items: Vec<Value> = order
        .lines
        .iter()
        .map(|line| {
            json!({
                "variantId": line.provider_variant_id,
                "quantity": line.quantity,
                "priceSet": money(line.unit_price_minor, &currency, order.exponent),
            })
        })
        .collect();
    let mut input = json!({
        "lineItems": line_items,
        "financialStatus": "PAID",
        "currency": currency,
        "processedAt": order.paid_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "tags": [TAG_PUBKY, order_tag(order.pubky_order_id)],
        "customAttributes": [
            { "key": "pubky_order_id", "value": order.pubky_order_id.to_string() }
        ],
        "transactions": [{
            "kind": "SALE",
            "status": "SUCCESS",
            "gateway": gateway_name(&order.payment_method),
            "amountSet": money(order.total_minor, &currency, order.exponent),
            "processedAt": order.paid_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }],
    });
    if order.shipping_minor > 0 {
        input["shippingLines"] = json!([{
            "title": "Shipping",
            "priceSet": money(order.shipping_minor, &currency, order.exponent),
        }]);
    }
    if let Some(address) = &order.shipping_address {
        input["shippingAddress"] = mailing_address(address);
    }
    Ok(json!({
        "order": input,
        "options": {
            "inventoryBehaviour": "DECREMENT_IGNORING_POLICY",
            "sendReceipt": false,
            "sendFulfillmentReceipt": false,
        },
    }))
}

pub async fn push(
    client: &Client,
    shop: &str,
    token: &str,
    order: &OrderPush,
) -> Result<ExternalOrder, ProviderError> {
    let currency = client
        .execute(shop, token, documents::SHOP_CURRENCY, json!({}))
        .await?
        .pointer("/shop/currencyCode")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| malformed("shop currency"))?;
    if !currency.eq_ignore_ascii_case(&order.currency) {
        return Err(rejected(
            "currency_mismatch",
            format!(
                "The order is in {} but the store's currency is {currency}.",
                order.currency
            ),
        ));
    }
    let variables = order_create_variables(order)?;
    let data = client
        .execute(shop, token, documents::ORDER_CREATE, variables)
        .await?;
    let payload = data
        .get("orderCreate")
        .ok_or_else(|| malformed("orderCreate payload"))?;
    if let Some(message) = user_errors(payload, "userErrors") {
        return Err(rejected("validation", message));
    }
    let id = payload
        .pointer("/order/id")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("orderCreate payload"))?;
    Ok(ExternalOrder { id: id.to_string() })
}

fn tagged(node: &Value, tag: &str) -> bool {
    node.get("tags")
        .and_then(Value::as_array)
        .is_some_and(|tags| tags.iter().any(|candidate| candidate.as_str() == Some(tag)))
}

fn find_in(data: &Value, tag: &str) -> Option<String> {
    data.pointer("/orders/nodes")?
        .as_array()?
        .iter()
        .find(|node| tagged(node, tag))
        .and_then(|node| node.get("id")?.as_str().map(str::to_string))
}

pub async fn find(
    client: &Client,
    shop: &str,
    token: &str,
    pubky_order_id: Uuid,
) -> Result<Option<ExternalOrder>, ProviderError> {
    let tag = order_tag(pubky_order_id);
    let searched = client
        .execute(
            shop,
            token,
            documents::ORDERS_BY_TAG,
            json!({ "query": format!("tag:{tag}") }),
        )
        .await?;
    if let Some(id) = find_in(&searched, &tag) {
        return Ok(Some(ExternalOrder { id }));
    }
    let recent = client
        .execute(
            shop,
            token,
            documents::RECENT_ORDERS,
            json!({ "first": RECENT_ORDER_SCAN }),
        )
        .await?;
    Ok(find_in(&recent, &tag).map(|id| ExternalOrder { id }))
}

pub async fn cancel(
    client: &Client,
    shop: &str,
    token: &str,
    order: &ExternalOrder,
    cancel: &CancelOrder,
) -> Result<(), ProviderError> {
    let current = client
        .execute(
            shop,
            token,
            documents::ORDER_TRANSACTIONS,
            json!({ "id": order.id }),
        )
        .await?;
    match current.get("order") {
        Some(found) if found.is_null() => {
            return Err(rejected("order_not_found", "The store has no such order."));
        }
        Some(found) if !found.get("cancelledAt").is_none_or(Value::is_null) => return Ok(()),
        _ => {}
    }
    let note: String = format!(
        "Pubky order {} cancelled: {}",
        cancel.pubky_order_id, cancel.reason
    )
    .chars()
    .take(STAFF_NOTE_MAX)
    .collect();
    let data = client
        .execute(
            shop,
            token,
            documents::ORDER_CANCEL,
            json!({
                "orderId": order.id,
                "reason": "OTHER",
                "restock": true,
                "notifyCustomer": false,
                "staffNote": note,
            }),
        )
        .await?;
    let payload = data
        .get("orderCancel")
        .ok_or_else(|| malformed("orderCancel payload"))?;
    match user_errors(payload, "orderCancelUserErrors") {
        None => Ok(()),
        // A retry while the first cancel is still running.
        Some(message) if message.to_ascii_lowercase().contains("already") => Ok(()),
        Some(message) => Err(rejected("cancel_refused", message)),
    }
}

/// The amount of a Pubky refund in the order's currency, in minor units.
pub fn refund_amount_minor(refund: &RefundRecord) -> Result<i64, ProviderError> {
    if refund.full || refund.amount_minor.is_none() {
        return Ok(refund.order_total_minor);
    }
    let amount = refund.amount_minor.unwrap_or_default();
    if refund
        .amount_unit
        .eq_ignore_ascii_case(&refund.order_currency)
        && amount > 0
    {
        return Ok(amount);
    }
    Err(rejected(
        "refund_amount_unconvertible",
        format!(
            "The partial refund is in {} and the order is in {}; record it in the store by hand.",
            refund.amount_unit, refund.order_currency
        ),
    ))
}

pub fn refund_key(refund_id: &str) -> String {
    deterministic_uuid(&format!("shopify-refund|{refund_id}")).to_string()
}

pub async fn refund(
    client: &Client,
    shop: &str,
    token: &str,
    order: &ExternalOrder,
    refund: &RefundRecord,
) -> Result<(), ProviderError> {
    let amount = refund_amount_minor(refund)?;
    let current = client
        .execute(
            shop,
            token,
            documents::ORDER_TRANSACTIONS,
            json!({ "id": order.id }),
        )
        .await?;
    let found = current
        .get("order")
        .filter(|found| !found.is_null())
        .ok_or_else(|| rejected("order_not_found", "The store has no such order."))?;
    let currency = found
        .get("currencyCode")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("order"))?
        .to_string();
    let exp = exponent(&currency);
    let sale = found
        .get("transactions")
        .and_then(Value::as_array)
        .and_then(|list| {
            list.iter().find(|transaction| {
                transaction.get("kind").and_then(Value::as_str) == Some("SALE")
                    && transaction.get("status").and_then(Value::as_str) == Some("SUCCESS")
            })
        })
        .ok_or_else(|| {
            rejected(
                "no_sale_transaction",
                "The store order has no successful payment to refund against.",
            )
        })?;
    let sale_id = sale
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("transaction"))?;
    let gateway = sale
        .get("gateway")
        .and_then(Value::as_str)
        .unwrap_or("Pubky");
    let sale_minor = sale
        .pointer("/amountSet/shopMoney/amount")
        .and_then(Value::as_str)
        .and_then(|amount| decimal_to_minor(amount, exp));
    if sale_minor.is_some_and(|paid| amount > paid) {
        return Err(rejected(
            "refund_exceeds_payment",
            "The refund is larger than the payment recorded on the store order.",
        ));
    }
    let document = documents::REFUND_CREATE.replace("{{KEY}}", &refund_key(&refund.refund_id));
    let data = client
        .execute(
            shop,
            token,
            &document,
            json!({ "input": {
                "orderId": order.id,
                "notify": false,
                "note": format!("Pubky refund for order {}", refund.pubky_order_id),
                "transactions": [{
                    "orderId": order.id,
                    "parentId": sale_id,
                    "gateway": gateway,
                    "kind": "REFUND",
                    "amount": minor_to_decimal(amount, exp),
                }],
            }}),
        )
        .await?;
    let payload = data
        .get("refundCreate")
        .ok_or_else(|| malformed("refundCreate payload"))?;
    match user_errors(payload, "userErrors") {
        None => Ok(()),
        Some(message) => Err(rejected("refund_refused", message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::provider::OrderPushLine;
    use chrono::TimeZone;

    fn order() -> OrderPush {
        OrderPush {
            pubky_order_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("uuid"),
            currency: "usd".into(),
            exponent: 2,
            lines: vec![OrderPushLine {
                provider_product_id: "gid://shopify/Product/1".into(),
                provider_variant_id: "gid://shopify/ProductVariant/2".into(),
                sku: Some("BOOT".into()),
                title: "Boots".into(),
                quantity: 2,
                unit_price_minor: 5000,
            }],
            subtotal_minor: 10000,
            shipping_minor: 700,
            total_minor: 10700,
            payment_method: "bitcoin".into(),
            paid_at: chrono::Utc.with_ymd_and_hms(2026, 10, 15, 9, 0, 0).unwrap(),
            shipping_address: None,
        }
    }

    #[test]
    fn the_store_order_is_paid_tagged_and_decrements_ignoring_policy() {
        let vars = order_create_variables(&order()).expect("variables");
        let input = &vars["order"];
        assert_eq!(input["financialStatus"], "PAID");
        assert_eq!(
            input["tags"],
            json!(["pubky", "pubky-order-11111111-2222-4333-8444-555555555555"])
        );
        assert_eq!(input["customAttributes"][0]["key"], "pubky_order_id");
        assert_eq!(
            input["lineItems"][0]["variantId"],
            "gid://shopify/ProductVariant/2"
        );
        assert_eq!(
            input["lineItems"][0]["priceSet"]["shopMoney"]["amount"],
            "50.00"
        );
        assert_eq!(
            input["shippingLines"][0]["priceSet"]["shopMoney"]["amount"],
            "7.00"
        );
        assert_eq!(input["transactions"][0]["kind"], "SALE");
        assert_eq!(input["transactions"][0]["gateway"], "Pubky Bitcoin");
        assert_eq!(
            input["transactions"][0]["amountSet"]["shopMoney"]["amount"],
            "107.00"
        );
        assert_eq!(
            input["transactions"][0]["amountSet"]["shopMoney"]["currencyCode"],
            "USD"
        );
        assert!(input.get("shippingAddress").is_none());
        assert!(input.get("email").is_none() && input.get("customer").is_none());
        assert_eq!(
            vars["options"]["inventoryBehaviour"],
            "DECREMENT_IGNORING_POLICY"
        );
        assert_eq!(vars["options"]["sendReceipt"], false);
    }

    #[test]
    fn an_address_is_attached_only_when_the_buyer_shared_one() {
        let mut shared = order();
        shared.shipping_address = Some(ShippingAddress {
            name: "Ada Lovelace".into(),
            line1: "1 Main St".into(),
            line2: String::new(),
            city: "London".into(),
            region: String::new(),
            postal_code: "N1".into(),
            country_code: "gb".into(),
        });
        let vars = order_create_variables(&shared).expect("variables");
        let address = &vars["order"]["shippingAddress"];
        assert_eq!(address["firstName"], "Ada");
        assert_eq!(address["lastName"], "Lovelace");
        assert_eq!(address["countryCode"], "GB");
        assert!(address.get("address2").is_none());
    }

    #[test]
    fn an_order_that_does_not_add_up_is_refused_before_it_is_sent() {
        let mut odd = order();
        odd.total_minor = 10000;
        assert!(matches!(
            order_create_variables(&odd),
            Err(ProviderError::Rejected { code, .. }) if code == "total_mismatch"
        ));
    }

    fn refund_record(full: bool, amount: Option<i64>, unit: &str) -> RefundRecord {
        RefundRecord {
            refund_id: "refund-1".into(),
            pubky_order_id: Uuid::nil(),
            amount_minor: amount,
            amount_unit: unit.into(),
            transaction_id: None,
            order_total_minor: 10700,
            order_currency: "USD".into(),
            order_exponent: 2,
            full,
        }
    }

    #[test]
    fn refund_amounts_are_in_the_order_currency_or_refused() {
        assert_eq!(
            refund_amount_minor(&refund_record(true, Some(1), "sat")).expect("full"),
            10700
        );
        assert_eq!(
            refund_amount_minor(&refund_record(false, None, "")).expect("none"),
            10700
        );
        assert_eq!(
            refund_amount_minor(&refund_record(false, Some(2500), "usd")).expect("fiat"),
            2500
        );
        assert!(matches!(
            refund_amount_minor(&refund_record(false, Some(2500), "sat")),
            Err(ProviderError::Rejected { code, .. }) if code == "refund_amount_unconvertible"
        ));
    }

    #[test]
    fn a_refund_key_is_stable_per_pubky_refund() {
        assert_eq!(refund_key("a"), refund_key("a"));
        assert_ne!(refund_key("a"), refund_key("b"));
        assert!(refund_key("a")
            .chars()
            .all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    #[test]
    fn the_tag_search_result_must_carry_the_exact_tag() {
        let data = json!({"orders": {"nodes": [
            {"id": "gid://shopify/Order/1", "tags": ["pubky", "pubky-order-other"]},
            {"id": "gid://shopify/Order/2", "tags": ["pubky-order-x"]},
        ]}});
        assert_eq!(
            find_in(&data, "pubky-order-x").as_deref(),
            Some("gid://shopify/Order/2")
        );
        assert_eq!(find_in(&data, "pubky-order-missing"), None);
    }
}
