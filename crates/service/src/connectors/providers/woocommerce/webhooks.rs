//! WooCommerce webhooks: registering ours after a connection, verifying what
//! the store delivers, and turning it into the connector's inbound events.
//!
//! A delivery is a JSON body (the REST representation of the product or
//! order) with `X-WC-Webhook-*` headers; `X-WC-Webhook-Signature` is the
//! base64 HMAC-SHA256 of the raw body under the secret we registered, which
//! is generated per connection and sealed with the credential.
//!
//! WooCommerce also sends an unsigned "ping" (`webhook_id=N`, no
//! `X-WC-Webhook-*` headers) when a webhook is created or saved, and keeps
//! re-sending it until it is answered with a 200. It carries nothing, so it is
//! acknowledged without a connection.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::connectors::provider::{
    InboundEvent, OrderRef, ProviderError, VerifiedWebhook, WebhookContext, WebhookHeaders,
};

use super::api::{Api, Session};
use super::catalog::{id_of, stock_of};
use super::creds::StoredCredentials;
use super::money;
use super::orders::{META_CANCELLED, META_ORDER_ID, REFUND_REASON_PREFIX};
use super::site::{Site, SitePolicy};

pub const TOPICS: [&str; 3] = ["product.updated", "order.updated", "order.deleted"];
pub const WEBHOOK_NAME_PREFIX: &str = "Pubky Shop";
/// Each connection's webhooks are delivered to `{WEBHOOK_PATH}/{connection_id}`.
pub const WEBHOOK_PATH: &str = "/v1/connectors/webhooks/woocommerce/c";

fn invalid(message: &str) -> ProviderError {
    ProviderError::Invalid(message.to_string())
}

/// Whether `url` is this connection's delivery URL: its path is the
/// connection's own, whatever the host (a base URL that changed since the
/// webhook was registered is still this connection's webhook).
fn is_delivery_url_of(url: &str, connection_id: Uuid) -> bool {
    url::Url::parse(url).is_ok_and(|url| url.path() == format!("{WEBHOOK_PATH}/{connection_id}"))
}

/// The ids of the webhooks on the store that belong to this connection: ours
/// by name and delivering to the connection's own URL. A webhook is never
/// matched by topic or name alone, so another connection of the same store, or
/// one the merchant made, is never touched.
async fn list_ours(api: &Api<'_>, connection_id: Uuid) -> Result<Vec<i64>, ProviderError> {
    Ok(api
        .list_all(
            "/webhooks",
            &[
                ("orderby", "id".into()),
                ("order", "asc".into()),
                ("_fields", "id,name,delivery_url".into()),
            ],
        )
        .await?
        .iter()
        .filter(|hook| {
            let name = hook.get("name").and_then(Value::as_str).unwrap_or_default();
            let url = hook
                .get("delivery_url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            name.starts_with(WEBHOOK_NAME_PREFIX) && is_delivery_url_of(url, connection_id)
        })
        .filter_map(id_of)
        .collect())
}

/// Deletes this connection's webhooks: all of them, or only those among
/// `only` (the ones a failed registration created). Already gone is fine.
async fn delete_ours(
    api: &Api<'_>,
    connection_id: Uuid,
    only: Option<&[i64]>,
) -> Result<u32, ProviderError> {
    let mut removed = 0;
    for id in list_ours(api, connection_id).await? {
        if only.is_some_and(|only| !only.contains(&id)) {
            continue;
        }
        if api
            .delete_if_present(&format!("/webhooks/{id}"), &[("force", "true".into())])
            .await?
        {
            removed += 1;
        }
    }
    Ok(removed)
}

/// Registers the three topics at `delivery_url` (the connection's own URL)
/// with `secret`, then removes the connection's earlier webhooks. The new ones
/// are created first, so a failure part way (a key without write access, the
/// store going away) leaves the earlier webhooks in place, and what this call
/// created is removed again, by id and only if it is at this connection's
/// URL. A key without write access is the usual failure, and is reported as
/// such. This is called only after the credential is stored, and only touches
/// webhooks at this connection's own URL.
pub async fn register(
    api: &Api<'_>,
    connection_id: Uuid,
    delivery_url: &str,
    secret: &str,
) -> Result<(), ProviderError> {
    let mut created_ids: Vec<i64> = Vec::new();
    for topic in TOPICS {
        let created = api
            .post(
                "/webhooks",
                &json!({
                    "name": format!("{WEBHOOK_NAME_PREFIX} {topic}"),
                    "topic": topic,
                    "delivery_url": delivery_url,
                    "secret": secret,
                    "status": "active",
                }),
            )
            .await;
        match created {
            Ok(reply) => {
                if let Some(id) = reply.json().ok().as_ref().and_then(id_of) {
                    created_ids.push(id);
                }
            }
            Err(error) => {
                let _ = delete_ours(api, connection_id, Some(&created_ids)).await;
                return Err(match error {
                    ProviderError::Rejected { code, .. }
                        if code == "forbidden" || code == "cannot_create" =>
                    {
                        ProviderError::Rejected {
                            code: "keys_read_only".into(),
                            message: "The API key cannot create webhooks. Create a key with Read/Write permissions.".into(),
                        }
                    }
                    other => other,
                });
            }
        }
    }
    for earlier in list_ours(api, connection_id).await? {
        if !created_ids.contains(&earlier) {
            api.delete_if_present(&format!("/webhooks/{earlier}"), &[("force", "true".into())])
                .await?;
        }
    }
    Ok(())
}

/// Deletes this connection's webhooks from the store, best effort (the caller
/// deletes its own copy of the credential whatever happens here).
pub async fn unregister(
    session: &Session,
    http: &super::http::WooHttp,
    connection_id: Uuid,
) -> Result<u32, ProviderError> {
    delete_ours(&session.api(http), connection_id, None).await
}

/// Whether the key can create webhooks, found without creating one: a create
/// with a topic WooCommerce does not know is refused after the permission
/// check and before anything is saved. A read-only key is refused by the
/// permission check (a typed `keys_read_only`); a read/write key reaches the
/// validation and is refused there.
pub async fn probe_write_access(api: &Api<'_>) -> Result<(), ProviderError> {
    match api
        .post(
            "/webhooks",
            &json!({
                "name": format!("{WEBHOOK_NAME_PREFIX} access check"),
                "topic": "pubky.access_check",
                "delivery_url": "https://pubky.invalid/access-check",
                "status": "disabled",
            }),
        )
        .await
    {
        Err(ProviderError::Rejected { code, message }) if code == "keys_read_only" => {
            Err(ProviderError::Rejected {
                code: "keys_read_only".into(),
                message,
            })
        }
        Err(ProviderError::Rejected { code, .. }) if code == "shop_webhook_invalid_topic" => Ok(()),
        Err(other) => Err(other),
        Ok(created) => {
            // A store that accepted it (not expected) must not keep it.
            if let Some(id) = created.json().ok().as_ref().and_then(id_of) {
                let _ = api
                    .delete_if_present(&format!("/webhooks/{id}"), &[("force", "true".into())])
                    .await;
            }
            Ok(())
        }
    }
}

fn is_ping(body: &[u8]) -> bool {
    body.strip_prefix(b"webhook_id=")
        .is_some_and(|id| !id.is_empty() && id.len() <= 12 && id.iter().all(u8::is_ascii_digit))
}

pub fn shop_of(
    headers: &WebhookHeaders,
    body: &[u8],
    policy: SitePolicy,
) -> Result<String, ProviderError> {
    if let Some(source) = headers.get("x-wc-webhook-source") {
        return Ok(Site::parse(source, policy)?.canonical());
    }
    if headers.get("x-wc-webhook-signature").is_none() && is_ping(body) {
        return Ok(String::new());
    }
    Err(invalid("not a WooCommerce webhook"))
}

fn sign(secret: &str, body: &[u8]) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    mac.finalize().into_bytes().to_vec()
}

pub fn verify(
    context: &WebhookContext,
    body: &[u8],
    headers: &WebhookHeaders,
) -> Result<VerifiedWebhook, ProviderError> {
    let Some(signature) = headers.get("x-wc-webhook-signature") else {
        if headers.get("x-wc-webhook-source").is_none() && is_ping(body) {
            return Ok(VerifiedWebhook {
                event_id: "ping".into(),
                topic: "ping".into(),
            });
        }
        return Err(invalid("the webhook is not signed"));
    };
    let Some(connection) = &context.connection else {
        return Err(invalid("no connected store"));
    };
    let stored = StoredCredentials::open(&connection.credentials)?;
    let provided = STANDARD
        .decode(signature.trim())
        .map_err(|_| invalid("the signature is not valid base64"))?;
    let expected = sign(&stored.webhook_secret, body);
    if provided.len() != expected.len() || !bool::from(provided.ct_eq(&expected)) {
        return Err(invalid("the signature does not match"));
    }
    let topic = headers
        .get("x-wc-webhook-topic")
        .map(str::trim)
        .filter(|topic| {
            !topic.is_empty()
                && topic.len() <= 64
                && topic
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b == b'.' || b == b'_')
        })
        .ok_or_else(|| invalid("the webhook has no topic"))?
        .to_string();
    let event_id = event_id(&topic, body);
    Ok(VerifiedWebhook { event_id, topic })
}

fn body_hash(topic: &str, body: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(topic.as_bytes());
    digest.update(b"\n");
    digest.update(body);
    hex::encode(digest.finalize())
}

/// A redelivery of the same body is a duplicate. A merchant refund is the
/// exception: every later update of that order repeats the refund in its body,
/// so its id names the refund set instead, and the seller is alerted once.
fn event_id(topic: &str, body: &[u8]) -> String {
    if topic == "order.updated" {
        if let Ok(order) = serde_json::from_slice::<Value>(body) {
            if let Ok(Classified {
                change: Change::Refunded { refund_ids, .. },
                order: reference,
            }) = classify(&order)
            {
                return format!(
                    "refund|{}|{}",
                    reference.external_order_id,
                    refund_ids.join(",")
                );
            }
        }
    }
    body_hash(topic, body)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Change {
    NotPubky,
    Fulfillment {
        carrier: String,
        tracking_number: String,
    },
    Cancelled {
        reason: Option<String>,
    },
    CancelledByPubky,
    Refunded {
        amount_minor: Option<i64>,
        currency: Option<String>,
        refund_ids: Vec<String>,
    },
    RefundedByPubky,
    Other(String),
}

#[derive(Debug)]
pub struct Classified {
    pub change: Change,
    pub order: OrderRef,
}

fn text(value: &Value, key: &str) -> String {
    match value.get(key) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        _ => String::new(),
    }
}

fn meta<'a>(order: &'a Value, key: &str) -> Option<&'a Value> {
    order
        .get("meta_data")?
        .as_array()?
        .iter()
        .find(|entry| entry.get("key").and_then(Value::as_str) == Some(key))?
        .get("value")
}

fn tracking_of(order: &Value) -> Option<(String, String)> {
    let clean = |value: &str| -> String { value.trim().chars().take(200).collect() };
    if let Some(items) = meta(order, "_wc_shipment_tracking_items").and_then(Value::as_array) {
        for item in items.iter().rev() {
            let number = clean(&text(item, "tracking_number"));
            if number.is_empty() {
                continue;
            }
            let custom = clean(&text(item, "custom_tracking_provider"));
            let provider = clean(&text(item, "tracking_provider"));
            let carrier = if custom.is_empty() { provider } else { custom };
            return Some((carrier, number));
        }
    }
    for (number_key, carrier_key) in [
        (
            "_aftership_tracking_number",
            "_aftership_tracking_provider_name",
        ),
        ("_tracking_number", "_tracking_provider"),
    ] {
        let number = meta(order, number_key)
            .and_then(Value::as_str)
            .map(clean)
            .unwrap_or_default();
        if !number.is_empty() {
            let carrier = meta(order, carrier_key)
                .and_then(Value::as_str)
                .map(clean)
                .unwrap_or_default();
            return Some((carrier, number));
        }
    }
    None
}

pub fn classify(order: &Value) -> Result<Classified, ProviderError> {
    let id = id_of(order).ok_or_else(|| invalid("the order has no id"))?;
    let pubky_order_id = meta(order, META_ORDER_ID)
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let reference = OrderRef {
        external_order_id: id.to_string(),
        pubky_order_id,
    };
    if pubky_order_id.is_none() {
        return Ok(Classified {
            change: Change::NotPubky,
            order: reference,
        });
    }
    let currency = text(order, "currency");
    let exponent = money::exponent_for(&currency);
    let mut ours = 0;
    let mut theirs_total: i64 = 0;
    let mut theirs_ids = Vec::new();
    for refund in order
        .get("refunds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if text(refund, "reason").starts_with(REFUND_REASON_PREFIX) {
            ours += 1;
            continue;
        }
        if let Some(refund_id) = id_of(refund) {
            theirs_ids.push(refund_id.to_string());
        }
        theirs_total += money::parse_minor(text(refund, "total").trim_start_matches('-'), exponent)
            .unwrap_or(0);
    }
    theirs_ids.sort();
    let refunded = || Change::Refunded {
        amount_minor: (theirs_total > 0).then_some(theirs_total),
        currency: (!currency.is_empty()).then(|| currency.clone()),
        refund_ids: theirs_ids.clone(),
    };
    let status = text(order, "status");
    let change = match status.as_str() {
        "completed" => match tracking_of(order) {
            Some((carrier, tracking_number)) => Change::Fulfillment {
                carrier,
                tracking_number,
            },
            // Completed with no tracking: shipped, carrier unknown. Tracking a
            // plugin adds later arrives as another order.updated.
            None => Change::Fulfillment {
                carrier: "Other".to_string(),
                tracking_number: String::new(),
            },
        },
        "cancelled" if meta(order, META_CANCELLED).is_some() => Change::CancelledByPubky,
        "cancelled" => Change::Cancelled { reason: None },
        "trash" => Change::Cancelled {
            reason: Some("The order was moved to the trash.".into()),
        },
        "refunded" if theirs_ids.is_empty() && ours > 0 => Change::RefundedByPubky,
        "refunded" => refunded(),
        _ if !theirs_ids.is_empty() => refunded(),
        other => Change::Other(other.to_string()),
    };
    Ok(Classified {
        change,
        order: reference,
    })
}

fn ignored(reason: &str) -> InboundEvent {
    InboundEvent::Ignored {
        reason: reason.to_string(),
    }
}

pub fn parse(webhook: &VerifiedWebhook, body: &[u8]) -> Result<InboundEvent, ProviderError> {
    if webhook.topic == "ping" {
        return Ok(ignored("ping"));
    }
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid("the body is not JSON"))?;
    match webhook.topic.as_str() {
        "product.updated" | "product.created" | "product.restored" => Ok(product(&value)),
        "order.updated" => {
            let Classified { change, order } = classify(&value)?;
            Ok(match change {
                Change::NotPubky => ignored("not_a_pubky_order"),
                // The store's tracking for a completed order is the order's current
                // newest item, so a number is always the one that should stand
                // (`TrackingUpdated` ships a paid order with it and replaces what a
                // shipped one has). Without a number the order is only marked shipped.
                Change::Fulfillment {
                    carrier,
                    tracking_number,
                } if tracking_number.is_empty() => InboundEvent::Fulfillment {
                    order,
                    carrier,
                    tracking_number,
                },
                Change::Fulfillment {
                    carrier,
                    tracking_number,
                } => InboundEvent::TrackingUpdated {
                    order,
                    carrier,
                    tracking_number,
                },
                Change::Cancelled { reason } => InboundEvent::OrderCancelled { order, reason },
                Change::CancelledByPubky => ignored("cancelled_by_pubky"),
                Change::Refunded {
                    amount_minor,
                    currency,
                    ..
                } => InboundEvent::Refund {
                    order,
                    amount_minor,
                    currency,
                },
                Change::RefundedByPubky => ignored("refunded_by_pubky"),
                Change::Other(status) => ignored(&format!("order_status_{status}")),
            })
        }
        "order.deleted" => {
            let id = id_of(&value).ok_or_else(|| invalid("the deleted order has no id"))?;
            Ok(InboundEvent::OrderCancelled {
                order: OrderRef {
                    external_order_id: id.to_string(),
                    pubky_order_id: None,
                },
                reason: Some("The order was deleted in the store.".into()),
            })
        }
        _ => Ok(ignored("unsupported_topic")),
    }
}

fn product(value: &Value) -> InboundEvent {
    let Some(id) = id_of(value) else {
        return ignored("no_product_id");
    };
    let (product_id, variant_id) = match text(value, "type").as_str() {
        "simple" => (id, id),
        "variation" => match value
            .get("parent_id")
            .and_then(Value::as_i64)
            .filter(|parent| *parent > 0)
        {
            Some(parent) => (parent, id),
            None => return ignored("variation_without_parent"),
        },
        _ => return ignored("not_a_sellable_variant"),
    };
    match stock_of(value, None) {
        Some(available) => InboundEvent::StockLevel {
            provider_product_id: product_id.to_string(),
            provider_variant_id: variant_id.to_string(),
            available,
        },
        None => ignored("stock_not_tracked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> WebhookHeaders {
        WebhookHeaders::from_pairs(pairs.iter().map(|(k, v)| (*k, *v)))
    }

    #[test]
    fn a_ping_is_recognised_and_belongs_to_no_shop() {
        let none = headers(&[]);
        assert_eq!(
            shop_of(&none, b"webhook_id=12", SitePolicy::default()).unwrap(),
            ""
        );
        assert!(shop_of(&none, b"webhook_id=", SitePolicy::default()).is_err());
        assert!(shop_of(&none, b"webhook_id=1; drop", SitePolicy::default()).is_err());
        assert!(shop_of(&none, b"{}", SitePolicy::default()).is_err());
        let signed = headers(&[("x-wc-webhook-signature", "x")]);
        assert!(shop_of(&signed, b"webhook_id=12", SitePolicy::default()).is_err());
    }

    #[test]
    fn the_source_header_names_the_shop() {
        let h = headers(&[("X-WC-Webhook-Source", "https://Dezentralshop.ch/")]);
        assert_eq!(
            shop_of(&h, b"{}", SitePolicy::default()).unwrap(),
            "dezentralshop.ch"
        );
        let sub = headers(&[("x-wc-webhook-source", "https://shop.example/store/")]);
        assert_eq!(
            shop_of(&sub, b"{}", SitePolicy::default()).unwrap(),
            "shop.example/store"
        );
    }

    #[test]
    fn signatures_are_base64_hmac_sha256_of_the_raw_body() {
        let secret = "s3cret";
        let body = br#"{"id":1,"a":"\/"}"#;
        let expected = STANDARD.encode(sign(secret, body));
        assert_eq!(expected.len(), 44);
        assert_eq!(STANDARD.decode(&expected).unwrap(), sign(secret, body));
        assert_ne!(
            sign(secret, body),
            sign(secret, br#"{"id":1,"a":"/"}"#),
            "raw bytes, not re-serialised JSON"
        );
    }

    fn pubky_order(extra: Value) -> Value {
        let mut order = json!({
            "id": 47, "status": "processing", "currency": "CHF", "total": "50.90",
            "meta_data": [{"id": 1, "key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"}],
            "refunds": []
        });
        for (key, value) in extra.as_object().unwrap() {
            order[key] = value.clone();
        }
        order
    }

    fn event(order: &Value) -> InboundEvent {
        let webhook = VerifiedWebhook {
            event_id: "e".into(),
            topic: "order.updated".into(),
        };
        parse(&webhook, &serde_json::to_vec(order).unwrap()).unwrap()
    }

    #[test]
    fn a_completed_order_with_tracking_is_a_fulfillment() {
        let order = pubky_order(json!({
            "status": "completed",
            "meta_data": [
                {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
                {"key": "_wc_shipment_tracking_items", "value": [
                    {"tracking_provider": "Swiss Post", "custom_tracking_provider": "", "tracking_number": "99.60.1"}]}
            ]
        }));
        match event(&order) {
            InboundEvent::TrackingUpdated {
                order,
                carrier,
                tracking_number,
            } => {
                assert_eq!(order.external_order_id, "47");
                assert!(order.pubky_order_id.is_some());
                assert_eq!(carrier, "Swiss Post");
                assert_eq!(tracking_number, "99.60.1");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn other_tracking_plugins_are_read_and_custom_carriers_win() {
        let custom = pubky_order(json!({"status": "completed", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "_wc_shipment_tracking_items", "value": [
                {"tracking_provider": "", "custom_tracking_provider": "Local Courier", "tracking_number": "ZZ9"}]}]}));
        assert!(
            matches!(event(&custom), InboundEvent::TrackingUpdated { carrier, .. } if carrier == "Local Courier")
        );
        let aftership = pubky_order(json!({"status": "completed", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "_aftership_tracking_number", "value": "AF1"},
            {"key": "_aftership_tracking_provider_name", "value": "dhl"}]}));
        assert!(
            matches!(event(&aftership), InboundEvent::TrackingUpdated { tracking_number, carrier, .. } if tracking_number == "AF1" && carrier == "dhl")
        );
    }

    #[test]
    fn completed_without_tracking_ships_with_the_carrier_other() {
        match event(&pubky_order(json!({"status": "completed"}))) {
            InboundEvent::Fulfillment {
                order,
                carrier,
                tracking_number,
            } => {
                assert_eq!(order.external_order_id, "47");
                assert_eq!(carrier, "Other");
                assert_eq!(tracking_number, "");
            }
            other => panic!("{other:?}"),
        }
        // An item with a blank number is no tracking either.
        let blank = pubky_order(json!({"status": "completed", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "_wc_shipment_tracking_items", "value": [
                {"tracking_provider": "Swiss Post", "tracking_number": "  "}]}]}));
        assert!(matches!(event(&blank),
            InboundEvent::Fulfillment { carrier, tracking_number, .. } if carrier == "Other" && tracking_number.is_empty()));
    }

    #[test]
    fn the_newest_tracking_item_is_the_one_that_counts() {
        let order = pubky_order(json!({"status": "completed", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "_wc_shipment_tracking_items", "value": [
                {"tracking_provider": "swiss-post", "tracking_number": "OLD1"},
                {"tracking_provider": "dhl", "tracking_number": "NEW2"}]}]}));
        assert!(matches!(event(&order),
            InboundEvent::TrackingUpdated { carrier, tracking_number, .. } if carrier == "dhl" && tracking_number == "NEW2"));
    }

    #[test]
    fn foreign_orders_and_unshipped_statuses_are_ignored() {
        let foreign = json!({"id": 9, "status": "completed", "meta_data": [], "refunds": []});
        assert!(
            matches!(event(&foreign), InboundEvent::Ignored { reason } if reason == "not_a_pubky_order")
        );
        assert!(
            matches!(event(&pubky_order(json!({"status": "processing"}))),
            InboundEvent::Ignored { reason } if reason == "order_status_processing")
        );
        let tracked_but_processing = pubky_order(json!({"status": "processing", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "_wc_shipment_tracking_items", "value": [{"tracking_provider": "dhl", "tracking_number": "N1"}]}]}));
        assert!(
            matches!(event(&tracked_but_processing), InboundEvent::Ignored { .. }),
            "tracking on an order the store has not completed is not a shipment"
        );
    }

    #[test]
    fn a_store_side_cancel_is_reported_but_ours_is_not() {
        assert!(matches!(
            event(&pubky_order(json!({"status": "cancelled"}))),
            InboundEvent::OrderCancelled { .. }
        ));
        let ours = pubky_order(json!({"status": "cancelled", "meta_data": [
            {"key": "pubky_order_id", "value": "11111111-2222-8333-9444-555555555555"},
            {"key": "pubky_cancelled", "value": "buyer cancelled"}]}));
        assert!(
            matches!(event(&ours), InboundEvent::Ignored { reason } if reason == "cancelled_by_pubky")
        );
        assert!(matches!(
            event(&pubky_order(json!({"status": "trash"}))),
            InboundEvent::OrderCancelled { .. }
        ));
    }

    #[test]
    fn a_merchant_refund_is_reported_but_our_own_record_is_not() {
        let merchant = pubky_order(json!({"status": "processing", "refunds": [
            {"id": 45, "reason": "Damaged", "total": "-5.00"}]}));
        match event(&merchant) {
            InboundEvent::Refund {
                amount_minor,
                currency,
                ..
            } => {
                assert_eq!(amount_minor, Some(500));
                assert_eq!(currency.as_deref(), Some("CHF"));
            }
            other => panic!("{other:?}"),
        }
        let ours = pubky_order(json!({"status": "refunded", "refunds": [
            {"id": 46, "reason": "Pubky refund r2", "total": "-50.90"}]}));
        assert!(
            matches!(event(&ours), InboundEvent::Ignored { reason } if reason == "refunded_by_pubky")
        );
        let ours_partial = pubky_order(json!({"status": "processing", "refunds": [
            {"id": 46, "reason": "Pubky refund r2", "total": "-5.00"}]}));
        assert!(matches!(event(&ours_partial), InboundEvent::Ignored { .. }));
    }

    #[test]
    fn a_merchant_refund_keeps_one_event_id_across_later_updates() {
        let first = serde_json::to_vec(&pubky_order(
            json!({"refunds": [{"id": 45, "reason": "x", "total": "-5.00"}], "date_modified": "a"}),
        ))
        .unwrap();
        let later = serde_json::to_vec(&pubky_order(
            json!({"refunds": [{"id": 45, "reason": "x", "total": "-5.00"}], "date_modified": "b"}),
        ))
        .unwrap();
        assert_eq!(
            event_id("order.updated", &first),
            event_id("order.updated", &later)
        );
        assert_eq!(event_id("order.updated", &first), "refund|47|45");
        let plain = serde_json::to_vec(&pubky_order(json!({"date_modified": "a"}))).unwrap();
        let plain_later = serde_json::to_vec(&pubky_order(json!({"date_modified": "b"}))).unwrap();
        assert_ne!(
            event_id("order.updated", &plain),
            event_id("order.updated", &plain_later)
        );
        assert_eq!(
            event_id("order.updated", &plain),
            event_id("order.updated", &plain)
        );
    }

    #[test]
    fn a_deleted_order_is_a_cancel_without_a_pubky_id() {
        let webhook = VerifiedWebhook {
            event_id: "e".into(),
            topic: "order.deleted".into(),
        };
        match parse(&webhook, br#"{"id":727}"#).unwrap() {
            InboundEvent::OrderCancelled { order, .. } => {
                assert_eq!(order.external_order_id, "727");
                assert_eq!(order.pubky_order_id, None);
            }
            other => panic!("{other:?}"),
        }
        assert!(parse(&webhook, b"nope").is_err());
    }

    #[test]
    fn stock_events_name_the_product_and_variant() {
        let webhook = VerifiedWebhook {
            event_id: "e".into(),
            topic: "product.updated".into(),
        };
        let simple = serde_json::to_vec(
            &json!({"id": 39, "type": "simple", "manage_stock": true, "stock_quantity": 6}),
        )
        .unwrap();
        assert_eq!(
            parse(&webhook, &simple).unwrap(),
            InboundEvent::StockLevel {
                provider_product_id: "39".into(),
                provider_variant_id: "39".into(),
                available: 6
            }
        );
        let variation = serde_json::to_vec(&json!({"id": 36, "type": "variation", "parent_id": 35, "manage_stock": true, "stock_quantity": 2})).unwrap();
        assert_eq!(
            parse(&webhook, &variation).unwrap(),
            InboundEvent::StockLevel {
                provider_product_id: "35".into(),
                provider_variant_id: "36".into(),
                available: 2
            }
        );
        let untracked = serde_json::to_vec(
            &json!({"id": 5, "type": "simple", "manage_stock": false, "stock_status": "instock"}),
        )
        .unwrap();
        assert!(matches!(
            parse(&webhook, &untracked).unwrap(),
            InboundEvent::Ignored { .. }
        ));
        let variable = serde_json::to_vec(
            &json!({"id": 35, "type": "variable", "manage_stock": true, "stock_quantity": 2}),
        )
        .unwrap();
        assert!(matches!(
            parse(&webhook, &variable).unwrap(),
            InboundEvent::Ignored { .. }
        ));
    }

    #[test]
    fn a_ping_and_unknown_topics_do_nothing() {
        let ping = VerifiedWebhook {
            event_id: "ping".into(),
            topic: "ping".into(),
        };
        assert!(
            matches!(parse(&ping, b"webhook_id=1").unwrap(), InboundEvent::Ignored { reason } if reason == "ping")
        );
        let other = VerifiedWebhook {
            event_id: "e".into(),
            topic: "coupon.created".into(),
        };
        assert!(matches!(
            parse(&other, b"{}").unwrap(),
            InboundEvent::Ignored { .. }
        ));
    }
}
