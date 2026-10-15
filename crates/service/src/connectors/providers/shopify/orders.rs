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
//! lags a create by seconds: it reads the order search first and then pages
//! through the orders created in the last [`SCAN_HORIZON`] (newest first,
//! which is not index-backed), up to [`SCAN_MAX_PAGES`] pages. If that bound is
//! reached with orders still inside the horizon unread, it cannot rule the
//! order out and says so ([`ProviderError::Unknown`]) instead of answering
//! "not found", so the ledger never pushes a second copy on a guess.
//!
//! No tax is added: the pushed order carries `taxesIncluded: false`, an empty
//! `taxLines`, lines marked `taxable: false` and no shipping tax lines, so
//! Shopify has nothing to compute and the store total is what the buyer paid
//! (Pubky prices are what the buyer is charged; there is no net/VAT split).
//! [`verify_total`] reads the created order back and reports a total or a tax
//! that is not what was paid, for a seller alert.
//!
//! A refund is mirrored as a **difference**: the Pubky order's recorded refund
//! total (in the order currency, on every rail) minus what the store order
//! already shows refunded, so a repeated or later, larger record never refunds
//! the same money twice. `refundCreate` is the one mutation with `@idempotent`;
//! its key covers the Pubky refund id and the amount, so a retry after an
//! unknown outcome returns the first result. The store refund's note carries
//! the Pubky refund id and the amount in a mark ([`refund_mark`]) that the
//! refund webhook echoes back, which is how the connector recognises an echo
//! of its own refund.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
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
const SCAN_PAGE: i64 = 50;
/// The most pages of recent orders one lookup reads.
pub const SCAN_MAX_PAGES: usize = 10;
/// How far back the recent-orders scan reads. A pushed order is created at
/// push time, and the ledger retries a push within minutes, so an order older
/// than this is not one whose search-index entry could still be missing.
pub const SCAN_HORIZON: ChronoDuration = ChronoDuration::hours(24);
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
/// faithfully: its exponent is not the currency's, or its lines and shipping
/// do not add up to its total.
pub fn order_create_variables(order: &OrderPush) -> Result<Value, ProviderError> {
    let currency = order.currency.to_ascii_uppercase();
    let exp = exponent(&currency);
    if order.exponent != exp {
        return Err(rejected(
            "exponent_mismatch",
            format!(
                "The order says {currency} has {} decimal places; it has {exp}.",
                order.exponent
            ),
        ));
    }
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
                "priceSet": money(line.unit_price_minor, &currency, exp),
                "taxable": false,
            })
        })
        .collect();
    let mut input = json!({
        "lineItems": line_items,
        "financialStatus": "PAID",
        "currency": currency,
        "taxesIncluded": false,
        "taxLines": [],
        "processedAt": order.paid_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "tags": [TAG_PUBKY, order_tag(order.pubky_order_id)],
        "customAttributes": [
            { "key": "pubky_order_id", "value": order.pubky_order_id.to_string() }
        ],
        "transactions": [{
            "kind": "SALE",
            "status": "SUCCESS",
            "gateway": gateway_name(&order.payment_method),
            "amountSet": money(order.total_minor, &currency, exp),
            "processedAt": order.paid_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }],
    });
    if order.shipping_minor > 0 {
        input["shippingLines"] = json!([{
            "title": "Shipping",
            "priceSet": money(order.shipping_minor, &currency, exp),
            "taxLines": [],
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
        let order_currency = order.currency.to_ascii_uppercase();
        return Err(rejected(
            "currency_mismatch",
            format!(
                "This order is in {order_currency} but the connected store sells in {currency}, \
                 and Pubky does not convert between currencies. Either change the store's \
                 currency to {order_currency}, or list the product on Pubky in {currency}. \
                 Then retry the push from the connection page."
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

/// Reads the created order back: `None` when its total is what the buyer paid
/// and no tax was added, otherwise a sentence for the seller.
pub async fn verify_total(
    client: &Client,
    shop: &str,
    token: &str,
    created: &ExternalOrder,
    pushed: &OrderPush,
) -> Result<Option<String>, ProviderError> {
    let data = client
        .execute(
            shop,
            token,
            documents::ORDER_TOTALS,
            json!({ "id": created.id }),
        )
        .await?;
    let order = data
        .get("order")
        .filter(|order| !order.is_null())
        .ok_or_else(|| malformed("order"))?;
    let currency = pushed.currency.to_ascii_uppercase();
    let exp = exponent(&currency);
    let minor = |pointer: &str| {
        order
            .pointer(pointer)
            .and_then(Value::as_str)
            .and_then(|amount| decimal_to_minor(amount, exp))
    };
    let total = minor("/totalPriceSet/shopMoney/amount").ok_or_else(|| malformed("order total"))?;
    let tax = order
        .pointer("/totalTaxSet/shopMoney/amount")
        .and_then(Value::as_str)
        .and_then(|amount| decimal_to_minor(amount, exp))
        .unwrap_or(0);
    if total == pushed.total_minor && tax == 0 {
        return Ok(None);
    }
    Ok(Some(format!(
        "The store order was created, but its total is {} {currency} and the buyer paid {} {currency}{}. \
         Check the order and the store's tax settings in Shopify; Pubky did not change it.",
        minor_to_decimal(total, exp),
        minor_to_decimal(pushed.total_minor, exp),
        if tax == 0 {
            String::new()
        } else {
            format!(" (the store added {} {currency} tax)", minor_to_decimal(tax, exp))
        }
    )))
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

/// Whether every order on this page, and so every later one, is older than the
/// horizon. Orders come newest first, so the last node decides.
fn page_is_past_horizon(data: &Value, now: DateTime<Utc>) -> bool {
    data.pointer("/orders/nodes")
        .and_then(Value::as_array)
        .and_then(|nodes| nodes.last())
        .and_then(|node| node.get("createdAt")?.as_str())
        .and_then(|created| DateTime::parse_from_rfc3339(created).ok())
        .is_some_and(|created| now - created.with_timezone(&Utc) > SCAN_HORIZON)
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
    let mut after: Option<String> = None;
    for _ in 0..SCAN_MAX_PAGES {
        let page = client
            .execute(
                shop,
                token,
                documents::RECENT_ORDERS,
                json!({ "first": SCAN_PAGE, "after": after }),
            )
            .await?;
        if let Some(id) = find_in(&page, &tag) {
            return Ok(Some(ExternalOrder { id }));
        }
        let has_next = page
            .pointer("/orders/pageInfo/hasNextPage")
            .and_then(Value::as_bool)
            == Some(true);
        if !has_next || page_is_past_horizon(&page, Utc::now()) {
            return Ok(None);
        }
        after = page
            .pointer("/orders/pageInfo/endCursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        if after.is_none() {
            return Ok(None);
        }
    }
    Err(ProviderError::Unknown(format!(
        "the store has more than {} orders newer than {} hours, so the order could not be ruled out",
        SCAN_PAGE as usize * SCAN_MAX_PAGES,
        SCAN_HORIZON.num_hours()
    )))
}

pub async fn cancel(
    client: &Client,
    shop: &str,
    token: &str,
    order: &ExternalOrder,
    cancel: &CancelOrder,
) -> Result<(), ProviderError> {
    if order_is_cancelled(client, shop, token, order).await? {
        return Ok(());
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
    let Some(message) = user_errors(payload, "orderCancelUserErrors") else {
        return Ok(());
    };
    let code = payload
        .pointer("/orderCancelUserErrors/0/code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if code == "NOT_FOUND" {
        return Err(rejected("order_not_found", "The store has no such order."));
    }
    // Shopify's error codes have none for "already cancelled" or "being
    // cancelled", so read the order instead of the message: a cancel that went
    // through (a retry racing the first, which runs as an asynchronous job) is
    // a success; one that has not yet is retried later, not quarantined.
    if order_is_cancelled(client, shop, token, order).await? {
        return Ok(());
    }
    Err(ProviderError::Unknown(format!(
        "the store did not accept the cancel yet ({code}): {message}"
    )))
}

async fn order_is_cancelled(
    client: &Client,
    shop: &str,
    token: &str,
    order: &ExternalOrder,
) -> Result<bool, ProviderError> {
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
            Err(rejected("order_not_found", "The store has no such order."))
        }
        Some(found) => Ok(!found.get("cancelledAt").is_none_or(Value::is_null)),
        None => Err(malformed("order")),
    }
}

/// The recorded refund total, in the order currency's minor units, that the
/// store should show refunded. `refund.amount_minor` is the Pubky books'
/// refunded total in the order currency on every rail; a record in any other
/// unit, or with no amount, cannot be mirrored.
pub fn refund_target_minor(refund: &RefundRecord) -> Result<i64, ProviderError> {
    let Some(amount) = refund.amount_minor.filter(|amount| *amount > 0) else {
        return Err(rejected(
            "refund_amount_missing",
            "The recorded refund has no amount to mirror.",
        ));
    };
    if !refund
        .amount_unit
        .eq_ignore_ascii_case(&refund.order_currency)
    {
        return Err(rejected(
            "refund_amount_unconvertible",
            format!(
                "The refund is recorded in {} and the order is in {}; record it in the store by hand.",
                refund.amount_unit, refund.order_currency
            ),
        ));
    }
    Ok(amount)
}

pub fn refund_key(refund_id: &str, delta_minor: i64) -> String {
    deterministic_uuid(&format!("shopify-refund|{refund_id}|{delta_minor}")).to_string()
}

const MARK_PREFIX: &str = "pubky-refund:";
const NOTE_BASIS_MAX: usize = 400;

/// The mark written at the end of a store refund's note: `pubky-refund:` then
/// the Pubky refund id, the amount in minor units and the currency, joined by
/// colons. The refund webhook carries the note back, so the connector can tell
/// this refund from one the merchant made, and check its amount.
pub fn refund_mark(refund_id: &str, delta_minor: i64, currency: &str) -> String {
    format!(
        "{MARK_PREFIX}{refund_id}:{delta_minor}:{}",
        currency.to_ascii_uppercase()
    )
}

/// Reads a mark back out of a refund note.
pub fn parse_refund_mark(note: &str) -> Option<(String, i64, String)> {
    let start = note.rfind(MARK_PREFIX)? + MARK_PREFIX.len();
    let token = note[start..].split_whitespace().next()?;
    let mut parts = token.split(':');
    let refund_id = parts.next().filter(|id| !id.is_empty())?.to_string();
    let amount = parts.next()?.parse::<i64>().ok()?;
    let currency = parts.next().filter(|c| !c.is_empty())?.to_ascii_uppercase();
    parts
        .next()
        .is_none()
        .then_some((refund_id, amount, currency))
}

fn refund_note(refund: &RefundRecord, delta_minor: i64, currency: &str) -> String {
    let basis: String = refund
        .basis
        .as_deref()
        .unwrap_or_default()
        .chars()
        .take(NOTE_BASIS_MAX)
        .collect();
    format!(
        "Pubky refund for order {}. {basis} {}",
        refund.pubky_order_id,
        refund_mark(&refund.refund_id, delta_minor, currency)
    )
}

pub async fn refund(
    client: &Client,
    shop: &str,
    token: &str,
    order: &ExternalOrder,
    refund: &RefundRecord,
) -> Result<(), ProviderError> {
    let target = refund_target_minor(refund)?;
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
    if !currency.eq_ignore_ascii_case(&refund.order_currency) {
        return Err(rejected(
            "currency_mismatch",
            format!(
                "The store order is in {currency} but the Pubky refund is in {}; record it in the store by hand.",
                refund.order_currency
            ),
        ));
    }
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
    let paid = sale
        .pointer("/amountSet/shopMoney/amount")
        .and_then(Value::as_str)
        .and_then(|amount| decimal_to_minor(amount, exp))
        .ok_or_else(|| malformed("sale amount"))?;
    let refunded_set = found
        .get("totalRefundedSet")
        .ok_or_else(|| malformed("order refunds"))?;
    if !refunded_set
        .pointer("/shopMoney/currencyCode")
        .and_then(Value::as_str)
        .is_some_and(|code| code.eq_ignore_ascii_case(&currency))
    {
        return Err(malformed("order refunds"));
    }
    let refunded = refunded_set
        .pointer("/shopMoney/amount")
        .and_then(Value::as_str)
        .and_then(|amount| decimal_to_minor(amount, exp))
        .ok_or_else(|| malformed("order refunds"))?;
    if target > paid {
        return Err(rejected(
            "refund_exceeds_payment",
            "The refund is larger than the payment recorded on the store order.",
        ));
    }
    let delta = target - refunded;
    if delta <= 0 {
        return Ok(());
    }
    let document =
        documents::REFUND_CREATE.replace("{{KEY}}", &refund_key(&refund.refund_id, delta));
    let data = client
        .execute(
            shop,
            token,
            &document,
            json!({ "input": {
                "orderId": order.id,
                "notify": false,
                "note": refund_note(refund, delta, &currency),
                "transactions": [{
                    "orderId": order.id,
                    "parentId": sale_id,
                    "gateway": gateway,
                    "kind": "REFUND",
                    "amount": minor_to_decimal(delta, exp),
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

    #[test]
    fn an_exponent_that_is_not_the_currencys_is_refused_before_it_is_sent() {
        let mut odd = order();
        odd.exponent = 0;
        assert!(matches!(
            order_create_variables(&odd),
            Err(ProviderError::Rejected { code, .. }) if code == "exponent_mismatch"
        ));
        let mut yen = order();
        yen.currency = "JPY".into();
        yen.exponent = 2;
        assert!(matches!(
            order_create_variables(&yen),
            Err(ProviderError::Rejected { code, .. }) if code == "exponent_mismatch"
        ));
        yen.exponent = 0;
        yen.lines[0].unit_price_minor = 5000;
        yen.subtotal_minor = 10000;
        yen.shipping_minor = 700;
        yen.total_minor = 10700;
        let vars = order_create_variables(&yen).expect("variables");
        assert_eq!(
            vars["order"]["transactions"][0]["amountSet"]["shopMoney"]["amount"],
            "10700"
        );
    }

    fn refund_record(amount: Option<i64>, unit: &str) -> RefundRecord {
        RefundRecord {
            refund_id: "refund-1".into(),
            pubky_order_id: Uuid::nil(),
            amount_minor: amount,
            amount_unit: unit.into(),
            transaction_id: None,
            order_total_minor: 10700,
            order_currency: "USD".into(),
            order_exponent: 2,
            full: false,
            basis: None,
        }
    }

    #[test]
    fn a_refund_target_is_the_recorded_total_in_the_order_currency_or_refused() {
        assert_eq!(
            refund_target_minor(&refund_record(Some(2500), "usd")).expect("fiat"),
            2500
        );
        for (record, code) in [
            (
                refund_record(Some(2500), "sat"),
                "refund_amount_unconvertible",
            ),
            (refund_record(None, "usd"), "refund_amount_missing"),
            (refund_record(Some(0), "usd"), "refund_amount_missing"),
        ] {
            assert!(matches!(
                refund_target_minor(&record),
                Err(ProviderError::Rejected { code: found, .. }) if found == code
            ));
        }
    }

    #[test]
    fn a_refund_key_is_stable_per_pubky_refund_and_amount() {
        assert_eq!(refund_key("a", 500), refund_key("a", 500));
        assert_ne!(refund_key("a", 500), refund_key("b", 500));
        assert_ne!(refund_key("a", 500), refund_key("a", 600));
        assert!(refund_key("a", 500)
            .chars()
            .all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    #[test]
    fn a_refund_mark_survives_the_round_trip_through_a_note() {
        let mark = refund_mark("7d9c0e0e-1111-4222-8333-444455556666", 8700, "usd");
        let note = format!("Pubky refund for order x. Amount recorded in USD. {mark}");
        assert_eq!(
            parse_refund_mark(&note),
            Some((
                "7d9c0e0e-1111-4222-8333-444455556666".into(),
                8700,
                "USD".into()
            ))
        );
        for bad in [
            "no mark here",
            "pubky-refund:",
            "pubky-refund:id:notanumber:USD",
            "pubky-refund:id:5",
            "pubky-refund:id:5:USD:extra",
            "pubky-refund::5:USD",
        ] {
            assert_eq!(parse_refund_mark(bad), None, "{bad}");
        }
        assert_eq!(
            parse_refund_mark("a merchant note pubky-refund:id:5:USD"),
            Some(("id".into(), 5, "USD".into())),
            "the mark is read from anywhere in the note"
        );
    }

    #[test]
    fn the_scan_horizon_stops_at_orders_older_than_a_day() {
        let now = Utc::now();
        let page = |age_hours: i64| {
            json!({"orders": {"nodes": [
                {"id": "a", "createdAt": (now - ChronoDuration::hours(1)).to_rfc3339()},
                {"id": "b", "createdAt": (now - ChronoDuration::hours(age_hours)).to_rfc3339()},
            ]}})
        };
        assert!(!page_is_past_horizon(&page(2), now));
        assert!(page_is_past_horizon(&page(30), now));
        assert!(!page_is_past_horizon(
            &json!({"orders": {"nodes": []}}),
            now
        ));
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
