//! Pubky orders in a WooCommerce store: create (already paid), find, cancel
//! and record a refund.
//!
//! The order body is pure and tested. Idempotency lives in the store, not in
//! WooCommerce: every order carries the Pubky order id as order meta
//! (`pubky_order_id`) and in the name of a zero-value fee line, because
//! WooCommerce's order search matches item names under both its storage
//! engines but does not search custom meta. [`find`] searches by that name,
//! verifies the meta on every hit, and falls back to reading the newest
//! orders, so it never depends on a search index catching up.

use serde_json::{json, Value};
use uuid::Uuid;

use crate::connectors::provider::{
    CancelOrder, ExternalOrder, OrderPush, ProviderError, RefundRecord, StoreRefundTotals,
};

use super::api::Api;
use super::catalog::id_of;
use super::money;

pub const META_ORDER_ID: &str = "pubky_order_id";
pub const META_CANCELLED: &str = "pubky_cancelled";
pub const META_REFUND_ID: &str = "pubky_refund_id";
/// Every refund this service records starts its reason with this, so the
/// order webhook can tell it from one the merchant made.
pub const REFUND_REASON_PREFIX: &str = "Pubky refund";
/// WooCommerce adds tax on top of the totals an API client sends, by the
/// store's rates for the shipping country, whatever the "prices include tax"
/// setting says. The buyer already paid a fixed amount, so items are created in
/// a tax class that has no rates, and shipping and adjustments are fee lines
/// with `tax_status: none`, so the order total equals what the buyer paid under
/// any tax configuration. The merchant books tax on Pubky sales from the Pubky
/// order.
const NO_TAX_CLASS: &str = "zero-rate";
const SCAN_PAGES: u32 = 3;
const SCAN_PAGE_SIZE: u32 = 100;
const FINAL_STATUSES: [&str; 4] = ["cancelled", "refunded", "failed", "trash"];

fn rail_label(method: &str) -> &'static str {
    match method {
        "bitcoin" => "Bitcoin",
        "paykit" => "Paykit",
        "usdt" => "USDT",
        "paypal" => "PayPal",
        "sandbox" => "Sandbox",
        _ => "Pubky",
    }
}

fn rejected(code: &str, message: &str) -> ProviderError {
    ProviderError::Rejected {
        code: code.to_string(),
        message: message.to_string(),
    }
}

fn woo_id(text: &str) -> Result<u64, ProviderError> {
    text.parse::<u64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| {
            rejected(
                "unmapped_variant",
                "A mapped store product id is not a WooCommerce id.",
            )
        })
}

fn split_name(name: &str) -> (String, String) {
    let name = name.trim();
    match name.split_once(char::is_whitespace) {
        Some((first, rest)) => (first.to_string(), rest.trim().to_string()),
        None => (name.to_string(), String::new()),
    }
}

/// The `POST /orders` body. `shop_origin` is the Shop's origin, used for the
/// link in the customer note when known.
pub fn order_body(push: &OrderPush, shop_origin: Option<&str>) -> Result<Value, ProviderError> {
    if push.lines.is_empty() {
        return Err(rejected("validation", "The order has no lines."));
    }
    let exponent = push.exponent;
    let mut lines_total: i64 = 0;
    let mut line_items = Vec::with_capacity(push.lines.len());
    for line in &push.lines {
        let product_id = woo_id(&line.provider_product_id)?;
        let line_total = line
            .unit_price_minor
            .checked_mul(line.quantity)
            .filter(|total| *total >= 0 && line.quantity > 0)
            .ok_or_else(|| rejected("validation", "A line has an invalid quantity or price."))?;
        lines_total = lines_total
            .checked_add(line_total)
            .ok_or_else(|| rejected("validation", "The order total is out of range."))?;
        let amount = money::format_minor(line_total, exponent);
        let mut item = json!({
            "product_id": product_id,
            "quantity": line.quantity,
            "subtotal": amount,
            "total": amount,
            "tax_class": NO_TAX_CLASS,
        });
        if line.provider_variant_id != line.provider_product_id {
            item["variation_id"] = json!(woo_id(&line.provider_variant_id)?);
        }
        line_items.push(item);
    }

    let mut fee_lines = vec![json!({
        "name": format!("Pubky order {}", push.pubky_order_id),
        "total": money::format_minor(0, exponent),
        "tax_status": "none",
    })];
    let delta = push
        .total_minor
        .checked_sub(lines_total)
        .and_then(|rest| rest.checked_sub(push.shipping_minor.max(0)))
        .ok_or_else(|| rejected("validation", "The order total is out of range."))?;
    if delta != 0 {
        fee_lines.push(json!({
            "name": "Pubky adjustment",
            "total": money::format_minor(delta, exponent),
            "tax_status": "none",
        }));
    }

    let rail = rail_label(&push.payment_method);
    let note = match shop_origin {
        Some(origin) => format!(
            "Pubky order {}, paid with {rail}. Details and the buyer's messages: {}/marketplace/orders",
            push.pubky_order_id,
            origin.trim_end_matches('/')
        ),
        None => format!("Pubky order {}, paid with {rail}.", push.pubky_order_id),
    };
    let mut body = json!({
        "status": "processing",
        "set_paid": true,
        "currency": push.currency,
        "payment_method": "pubky",
        "payment_method_title": if rail == "Pubky" { "Pubky".to_string() } else { format!("Pubky ({rail})") },
        "customer_note": note,
        "line_items": line_items,
        "fee_lines": fee_lines,
        "meta_data": [
            {"key": META_ORDER_ID, "value": push.pubky_order_id.to_string()},
            {"key": "pubky_payment_rail", "value": push.payment_method},
            {"key": "pubky_paid_at", "value": push.paid_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)},
        ],
    });
    if push.shipping_minor > 0 {
        // A fee line, not a shipping line: WooCommerce taxes a shipping line by
        // the store's "shipping tax class" (Standard on many stores) even when
        // every item is in the zero-rate class, but never a fee line marked
        // `tax_status: none`.
        body["fee_lines"]
            .as_array_mut()
            .expect("fee_lines is an array")
            .push(json!({
                "name": "Pubky shipping",
                "total": money::format_minor(push.shipping_minor, exponent),
                "tax_status": "none",
            }));
    }
    if let Some(address) = &push.shipping_address {
        let (first, last) = split_name(&address.name);
        body["shipping"] = json!({
            "first_name": first,
            "last_name": last,
            "address_1": address.line1,
            "address_2": address.line2,
            "city": address.city,
            "state": address.region,
            "postcode": address.postal_code,
            "country": address.country_code.to_ascii_uppercase(),
        });
    }
    Ok(body)
}

pub async fn push(
    api: &Api<'_>,
    order: &OrderPush,
    shop_origin: Option<&str>,
) -> Result<ExternalOrder, ProviderError> {
    let body = order_body(order, shop_origin)?;
    let reply = api.post("/orders", &body).await?;
    let created = reply.json()?;
    let id = id_of(&created).ok_or_else(|| {
        ProviderError::Unknown("the store created an order but did not return its id".into())
    })?;
    Ok(ExternalOrder { id: id.to_string() })
}

fn has_order_meta(order: &Value, pubky_order_id: &str) -> bool {
    order
        .get("meta_data")
        .and_then(Value::as_array)
        .is_some_and(|meta| {
            meta.iter().any(|entry| {
                entry.get("key").and_then(Value::as_str) == Some(META_ORDER_ID)
                    && entry.get("value").and_then(Value::as_str) == Some(pubky_order_id)
            })
        })
}

/// The order a push may already have created.
pub async fn find(
    api: &Api<'_>,
    pubky_order_id: Uuid,
) -> Result<Option<ExternalOrder>, ProviderError> {
    let wanted = pubky_order_id.to_string();
    let hit = |orders: &Value| -> Option<ExternalOrder> {
        orders
            .as_array()?
            .iter()
            .find(|order| has_order_meta(order, &wanted))
            .and_then(id_of)
            .map(|id| ExternalOrder { id: id.to_string() })
    };
    let searched = api
        .get(
            "/orders",
            &[
                ("search", wanted.clone()),
                ("per_page", "20".into()),
                ("_fields", "id,status,meta_data".into()),
            ],
        )
        .await?
        .json()?;
    if let Some(found) = hit(&searched) {
        return Ok(Some(found));
    }
    for page in 1..=SCAN_PAGES {
        let reply = api
            .get(
                "/orders",
                &[
                    ("per_page", SCAN_PAGE_SIZE.to_string()),
                    ("page", page.to_string()),
                    ("orderby", "id".into()),
                    ("order", "desc".into()),
                    ("_fields", "id,status,meta_data".into()),
                ],
            )
            .await?;
        let orders = reply.json()?;
        if let Some(found) = hit(&orders) {
            return Ok(Some(found));
        }
        if orders.as_array().map_or(0, Vec::len) < SCAN_PAGE_SIZE as usize {
            break;
        }
    }
    Ok(None)
}

/// Cancels the order (WooCommerce puts its stock back) and marks it as
/// cancelled by Pubky, so the order webhook that follows is not mistaken for
/// the merchant cancelling it. Cancelling a cancelled, refunded or missing
/// order succeeds.
pub async fn cancel(
    api: &Api<'_>,
    order: &ExternalOrder,
    cancel: &CancelOrder,
) -> Result<(), ProviderError> {
    let endpoint = format!("/orders/{}", woo_id(&order.id)?);
    let Some(current) = api
        .get_optional(&endpoint, &[("_fields", "id,status".into())])
        .await?
    else {
        return Ok(());
    };
    let current = current.json()?;
    let status = current
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if FINAL_STATUSES.contains(&status) {
        return Ok(());
    }
    let reason: String = cancel.reason.chars().take(200).collect();
    api.put(
        &endpoint,
        &json!({
            "status": "cancelled",
            "meta_data": [{"key": META_CANCELLED, "value": if reason.is_empty() { "cancelled".to_string() } else { reason }}],
        }),
    )
    .await?;
    Ok(())
}

fn refunded_so_far(order: &Value, exponent: i32) -> i64 {
    order
        .get("refunds")
        .and_then(Value::as_array)
        .map(|refunds| {
            refunds
                .iter()
                .filter_map(|refund| {
                    let total = refund.get("total")?.as_str()?;
                    money::parse_minor(total.trim_start_matches('-'), exponent)
                })
                .sum()
        })
        .unwrap_or(0)
}

/// What the store order was paid and what it already shows refunded (ours
/// from an earlier record, or the merchant's own), read fresh. The framework
/// works out how much of a Pubky refund is still to mirror from this.
pub async fn refunded_total(
    api: &Api<'_>,
    order: &ExternalOrder,
) -> Result<StoreRefundTotals, ProviderError> {
    let id = woo_id(&order.id)?;
    let Some(current) = api
        .get_optional(
            &format!("/orders/{id}"),
            &[("_fields", "id,total,currency,refunds".into())],
        )
        .await?
    else {
        return Err(rejected(
            "order_not_found",
            "The store order no longer exists.",
        ));
    };
    let current = current.json()?;
    let currency = current
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let exponent = money::exponent_for(&currency);
    let paid_minor = current
        .get("total")
        .and_then(Value::as_str)
        .and_then(|total| money::parse_minor(total, exponent))
        .ok_or_else(|| ProviderError::Unknown("the store order has no readable total".into()))?;
    Ok(StoreRefundTotals {
        refunded_minor: refunded_so_far(&current, exponent),
        paid_minor,
        currency,
    })
}

/// Writes the refund the framework decided on, without moving money
/// (`api_refund: false`). `refund.amount_minor` is the amount still to mirror,
/// in the order currency, already the difference from what the store shows
/// and never above its payment. A refund already written for this Pubky refund
/// id is never written twice, so a retry after an unknown outcome is safe.
pub async fn record_refund(
    api: &Api<'_>,
    order: &ExternalOrder,
    refund: &RefundRecord,
) -> Result<(), ProviderError> {
    let id = woo_id(&order.id)?;
    if refund.amount_minor <= 0 {
        return Ok(());
    }
    let existing = api
        .get(
            &format!("/orders/{id}/refunds"),
            &[
                ("per_page", "100".into()),
                ("_fields", "id,meta_data".into()),
            ],
        )
        .await?
        .json()?;
    let written = existing.as_array().is_some_and(|refunds| {
        refunds.iter().any(|entry| {
            entry
                .get("meta_data")
                .and_then(Value::as_array)
                .is_some_and(|meta| {
                    meta.iter().any(|item| {
                        item.get("key").and_then(Value::as_str) == Some(META_REFUND_ID)
                            && item.get("value").and_then(Value::as_str) == Some(&refund.refund_id)
                    })
                })
        })
    });
    if written {
        return Ok(());
    }
    let reason: String = match &refund.basis {
        Some(basis) => format!("{REFUND_REASON_PREFIX} {}: {basis}", refund.refund_id),
        None => format!("{REFUND_REASON_PREFIX} {}", refund.refund_id),
    }
    .chars()
    .take(200)
    .collect();
    api.post(
        &format!("/orders/{id}/refunds"),
        &json!({
            "amount": money::format_minor(refund.amount_minor, money::exponent_for(&refund.order_currency)),
            "reason": reason,
            "api_refund": false,
            "meta_data": [{"key": META_REFUND_ID, "value": refund.refund_id}],
        }),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::provider::{OrderPushLine, ShippingAddress};

    fn push() -> OrderPush {
        OrderPush {
            pubky_order_id: Uuid::parse_str("11111111-2222-8333-9444-555555555555").unwrap(),
            currency: "CHF".into(),
            exponent: 2,
            lines: vec![
                OrderPushLine {
                    provider_product_id: "39".into(),
                    provider_variant_id: "39".into(),
                    sku: None,
                    title: "Pubky Test Item".into(),
                    quantity: 2,
                    unit_price_minor: 1250,
                },
                OrderPushLine {
                    provider_product_id: "35".into(),
                    provider_variant_id: "36".into(),
                    sku: None,
                    title: "Socks S".into(),
                    quantity: 1,
                    unit_price_minor: 1990,
                },
            ],
            subtotal_minor: 4490,
            shipping_minor: 600,
            total_minor: 5090,
            payment_method: "bitcoin".into(),
            paid_at: "2026-10-15T12:00:00Z".parse().unwrap(),
            shipping_address: None,
        }
    }

    #[test]
    fn the_order_is_created_paid_with_stock_taken() {
        let body = order_body(&push(), Some("https://shop.pubky.app/")).unwrap();
        assert_eq!(body["status"], "processing");
        assert_eq!(body["set_paid"], true);
        assert_eq!(body["currency"], "CHF");
        assert_eq!(body["payment_method_title"], "Pubky (Bitcoin)");
        assert_eq!(body["line_items"][0]["product_id"], 39);
        assert!(body["line_items"][0].get("variation_id").is_none());
        assert_eq!(body["line_items"][0]["total"], "25.00");
        assert_eq!(
            body["line_items"][0]["tax_class"], "zero-rate",
            "no tax added on top of what was paid"
        );
        assert_eq!(body["line_items"][1]["tax_class"], "zero-rate");
        assert_eq!(body["line_items"][1]["variation_id"], 36);
        assert!(
            body.get("shipping_lines").is_none(),
            "shipping is a taxless fee"
        );
        let fees = body["fee_lines"].as_array().unwrap();
        let shipping = fees
            .iter()
            .find(|fee| fee["name"] == "Pubky shipping")
            .unwrap();
        assert_eq!(shipping["total"], "6.00");
        assert_eq!(shipping["tax_status"], "none");
        assert_eq!(body["meta_data"][0]["key"], "pubky_order_id");
        assert_eq!(
            body["meta_data"][0]["value"],
            "11111111-2222-8333-9444-555555555555"
        );
        let note = body["customer_note"].as_str().unwrap();
        assert!(note.contains("11111111-2222-8333-9444-555555555555"));
        assert!(note.contains("https://shop.pubky.app/marketplace/orders"));
    }

    #[test]
    fn the_order_id_is_searchable_in_an_item_name() {
        let body = order_body(&push(), None).unwrap();
        let fees = body["fee_lines"].as_array().unwrap();
        assert!(
            fees.iter().all(|fee| fee["name"] != "Pubky adjustment"),
            "totals add up, so no adjustment line"
        );
        assert!(
            fees.iter().all(|fee| fee["tax_status"] == "none"),
            "nothing but the lines is taxed"
        );
        assert_eq!(
            fees[0]["name"],
            "Pubky order 11111111-2222-8333-9444-555555555555"
        );
        assert_eq!(fees[0]["total"], "0.00");
    }

    #[test]
    fn a_total_that_differs_from_the_lines_gets_an_adjustment() {
        let mut order = push();
        order.total_minor = 5000;
        let body = order_body(&order, None).unwrap();
        assert_eq!(body["fee_lines"][1]["name"], "Pubky adjustment");
        assert_eq!(body["fee_lines"][1]["total"], "-0.90");
    }

    #[test]
    fn no_address_and_no_billing_unless_the_buyer_opted_in() {
        let body = order_body(&push(), None).unwrap();
        assert!(body.get("shipping").is_none());
        assert!(body.get("billing").is_none());

        let mut opted_in = push();
        opted_in.shipping_address = Some(ShippingAddress {
            name: "Anna Maria Muster".into(),
            line1: "Bahnhofstrasse 1".into(),
            line2: String::new(),
            city: "Z\u{fc}rich".into(),
            region: "ZH".into(),
            postal_code: "8001".into(),
            country_code: "ch".into(),
        });
        let body = order_body(&opted_in, None).unwrap();
        assert_eq!(body["shipping"]["first_name"], "Anna");
        assert_eq!(body["shipping"]["last_name"], "Maria Muster");
        assert_eq!(body["shipping"]["country"], "CH");
        assert!(
            body.get("billing").is_none(),
            "billing stays empty, no email"
        );
    }

    #[test]
    fn bad_ids_and_quantities_are_rejected_before_anything_is_sent() {
        let mut order = push();
        order.lines[0].provider_product_id = "gid://x".into();
        assert!(
            matches!(order_body(&order, None), Err(ProviderError::Rejected { code, .. }) if code == "unmapped_variant")
        );
        let mut order = push();
        order.lines[0].quantity = 0;
        assert!(matches!(
            order_body(&order, None),
            Err(ProviderError::Rejected { .. })
        ));
        let mut order = push();
        order.lines.clear();
        assert!(matches!(
            order_body(&order, None),
            Err(ProviderError::Rejected { .. })
        ));
    }

    #[test]
    fn refunded_amounts_are_summed_from_negative_totals() {
        let order = json!({"refunds": [{"total": "-5.00"}, {"total": "-1.25"}]});
        assert_eq!(refunded_so_far(&order, 2), 625);
        assert_eq!(refunded_so_far(&json!({}), 2), 0);
    }

    #[test]
    fn the_order_meta_must_match_exactly() {
        let order = json!({"meta_data": [{"key": "pubky_order_id", "value": "abc"}]});
        assert!(has_order_meta(&order, "abc"));
        assert!(!has_order_meta(&order, "abd"));
        assert!(!has_order_meta(
            &json!({"meta_data": [{"key": "other", "value": "abc"}]}),
            "abc"
        ));
    }
}
