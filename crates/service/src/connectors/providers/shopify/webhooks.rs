//! Webhook subscriptions and the meaning of Shopify's webhook payloads.
//!
//! The service subscribes the store to five topics through the Admin API
//! (`webhookSubscriptionCreate`), all pointing at the connection's own webhooks
//! URL. Shopify's three mandatory privacy topics (`customers/data_request`,
//! `customers/redact`, `shop/redact`) are **not** in `WebhookSubscriptionTopic`
//! and cannot be subscribed through the API: they are configured in the app's
//! configuration (`shopify.app.toml` `[webhooks.privacy_compliance]`, or the
//! Dev Dashboard's app version) with the same connection webhooks URL. They are
//! signed with the same app secret and are parsed here once they arrive.

use serde_json::{json, Value};
use uuid::Uuid;

use super::auth::{normalize_shop, verify_webhook_hmac};
use super::client::Client;
use super::documents;
use super::money::{decimal_to_minor, exponent};
use crate::connectors::provider::{
    AppContext, ComplianceTopic, InboundEvent, OrderRef, ProviderError, VerifiedWebhook,
    WebhookHeaders,
};

pub const HEADER_HMAC: &str = "x-shopify-hmac-sha256";
pub const HEADER_TOPIC: &str = "x-shopify-topic";
pub const HEADER_SHOP: &str = "x-shopify-shop-domain";
pub const HEADER_EVENT_ID: &str = "x-shopify-event-id";
pub const HEADER_WEBHOOK_ID: &str = "x-shopify-webhook-id";

/// The webhook topic as it appears in `X-Shopify-Topic` and the matching
/// `WebhookSubscriptionTopic` value.
pub const SUBSCRIBED_TOPICS: [(&str, &str); 5] = [
    ("inventory_levels/update", "INVENTORY_LEVELS_UPDATE"),
    ("fulfillments/create", "FULFILLMENTS_CREATE"),
    ("orders/cancelled", "ORDERS_CANCELLED"),
    ("refunds/create", "REFUNDS_CREATE"),
    ("app/uninstalled", "APP_UNINSTALLED"),
];

/// The topics configured in the app's configuration rather than through the
/// API; the merchant guide lists them.
pub const PRIVACY_TOPICS: [&str; 3] = ["customers/data_request", "customers/redact", "shop/redact"];

const MANAGED_PATH: &str = "/v1/connectors/webhooks/shopify/";

fn malformed(what: &str) -> ProviderError {
    ProviderError::Unknown(format!("shopify returned an unexpected {what}"))
}

pub async fn register(
    client: &Client,
    shop: &str,
    token: &str,
    webhook_url: &str,
) -> Result<(), ProviderError> {
    let existing = client
        .execute(shop, token, documents::WEBHOOK_SUBSCRIPTIONS, json!({}))
        .await?;
    let nodes = existing
        .pointer("/webhookSubscriptions/nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("webhook subscription list"))?;
    let entry = |node: &Value| {
        Some((
            node.get("id")?.as_str()?.to_string(),
            node.get("topic")?.as_str()?.to_string(),
            node.get("uri")?.as_str()?.to_string(),
        ))
    };
    let known: Vec<(String, String, String)> = nodes.iter().filter_map(entry).collect();
    for (_, topic) in SUBSCRIBED_TOPICS {
        if !known
            .iter()
            .any(|(_, found, uri)| found == topic && uri == webhook_url)
        {
            let data = client
                .execute(
                    shop,
                    token,
                    documents::WEBHOOK_SUBSCRIPTION_CREATE,
                    json!({
                        "topic": topic,
                        "webhookSubscription": { "uri": webhook_url, "format": "JSON" },
                    }),
                )
                .await?;
            let payload = data
                .get("webhookSubscriptionCreate")
                .ok_or_else(|| malformed("webhookSubscriptionCreate payload"))?;
            let errors = payload
                .get("userErrors")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let message = errors
                .iter()
                .filter_map(|error| error.get("message").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("; ");
            // Another run already subscribed this address.
            if !errors.is_empty() && !message.to_ascii_lowercase().contains("already") {
                return Err(ProviderError::Rejected {
                    code: "webhook_subscription".into(),
                    message: message.chars().take(300).collect(),
                });
            }
        }
        // A subscription this service made earlier for another address (a
        // re-created connection, a changed public URL) would deliver every
        // event twice.
        for (id, found, uri) in &known {
            if found == topic && uri != webhook_url && uri.contains(MANAGED_PATH) {
                client
                    .execute(
                        shop,
                        token,
                        documents::WEBHOOK_SUBSCRIPTION_DELETE,
                        json!({ "id": id }),
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

pub fn shop_of(headers: &WebhookHeaders, body: &[u8]) -> Result<String, ProviderError> {
    let claimed = headers
        .get(HEADER_SHOP)
        .map(str::to_string)
        .or_else(|| {
            serde_json::from_slice::<Value>(body)
                .ok()?
                .get("shop_domain")?
                .as_str()
                .map(str::to_string)
        })
        .ok_or_else(|| ProviderError::Invalid("webhook names no shop".into()))?;
    normalize_shop(&claimed)
}

pub fn verify(
    app: Option<&AppContext>,
    body: &[u8],
    headers: &WebhookHeaders,
) -> Result<VerifiedWebhook, ProviderError> {
    let app =
        app.ok_or_else(|| ProviderError::Invalid("the connection has no app secret".into()))?;
    let signature = headers
        .get(HEADER_HMAC)
        .ok_or_else(|| ProviderError::Invalid("webhook is not signed".into()))?;
    if !verify_webhook_hmac(app.client_secret.expose(), body, signature) {
        return Err(ProviderError::Invalid("webhook signature is wrong".into()));
    }
    let topic = headers
        .get(HEADER_TOPIC)
        .ok_or_else(|| ProviderError::Invalid("webhook names no topic".into()))?
        .to_ascii_lowercase();
    let event_id = headers
        .get(HEADER_EVENT_ID)
        .or_else(|| headers.get(HEADER_WEBHOOK_ID))
        .filter(|id| !id.is_empty())
        .ok_or_else(|| ProviderError::Invalid("webhook carries no event id".into()))?
        .to_string();
    Ok(VerifiedWebhook { event_id, topic })
}

fn gid(kind: &str, payload: &Value, numeric: &str) -> Option<String> {
    if let Some(id) = payload.get("admin_graphql_api_id").and_then(Value::as_str) {
        if id.starts_with(&format!("gid://shopify/{kind}/")) {
            return Some(id.to_string());
        }
    }
    let id = payload.get(numeric)?;
    let id = id
        .as_u64()
        .map(|number| number.to_string())
        .or_else(|| id.as_str().map(str::to_string))?;
    id.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| format!("gid://shopify/{kind}/{id}"))
}

/// The Pubky order id the push stamped on a store order: its
/// `pubky-order-{uuid}` tag, or its `pubky_order_id` note attribute.
fn pubky_order_id(payload: &Value) -> Option<Uuid> {
    let from_tags = payload.get("tags").and_then(|tags| {
        let list: Vec<String> = match tags {
            Value::String(text) => text.split(',').map(|tag| tag.trim().to_string()).collect(),
            Value::Array(items) => items
                .iter()
                .filter_map(|tag| tag.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        list.iter()
            .find_map(|tag| Uuid::parse_str(tag.strip_prefix("pubky-order-")?).ok())
    });
    from_tags.or_else(|| {
        payload
            .get("note_attributes")?
            .as_array()?
            .iter()
            .find(|attribute| {
                attribute.get("name").and_then(Value::as_str) == Some("pubky_order_id")
            })
            .and_then(|attribute| Uuid::parse_str(attribute.get("value")?.as_str()?).ok())
    })
}

fn ignored(reason: &str) -> Result<InboundEvent, ProviderError> {
    Ok(InboundEvent::Ignored {
        reason: reason.to_string(),
    })
}

pub fn parse(webhook: &VerifiedWebhook, body: &[u8]) -> Result<InboundEvent, ProviderError> {
    let payload: Value = serde_json::from_slice(body)
        .map_err(|_| ProviderError::Invalid("webhook body is not JSON".into()))?;
    let missing =
        |what: &str| ProviderError::Invalid(format!("webhook {} has no {what}", webhook.topic));
    match webhook.topic.as_str() {
        "inventory_levels/update" => Ok(InboundEvent::InventoryChanged {
            inventory_item_id: gid("InventoryItem", &payload, "inventory_item_id")
                .ok_or_else(|| missing("inventory item"))?,
        }),
        "fulfillments/create" => {
            let tracking = payload
                .get("tracking_number")
                .and_then(Value::as_str)
                .or_else(|| {
                    payload
                        .get("tracking_numbers")?
                        .as_array()?
                        .first()?
                        .as_str()
                })
                .map(str::trim)
                .filter(|number| !number.is_empty());
            let Some(tracking) = tracking else {
                return ignored("fulfillment_without_tracking_number");
            };
            Ok(InboundEvent::Fulfillment {
                order: OrderRef {
                    external_order_id: gid("Order", &payload, "order_id")
                        .ok_or_else(|| missing("order"))?,
                    pubky_order_id: None,
                },
                carrier: payload
                    .get("tracking_company")
                    .and_then(Value::as_str)
                    .filter(|company| !company.is_empty())
                    .unwrap_or("other")
                    .to_string(),
                tracking_number: tracking.to_string(),
            })
        }
        "orders/cancelled" => Ok(InboundEvent::OrderCancelled {
            order: OrderRef {
                external_order_id: gid("Order", &payload, "id").ok_or_else(|| missing("order"))?,
                pubky_order_id: pubky_order_id(&payload),
            },
            reason: payload
                .get("cancel_reason")
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
        "refunds/create" => {
            let refunded: Vec<&Value> = payload
                .get("transactions")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter(|transaction| {
                            transaction.get("kind").and_then(Value::as_str) == Some("refund")
                                && transaction.get("status").and_then(Value::as_str)
                                    != Some("failure")
                        })
                        .collect()
                })
                .unwrap_or_default();
            let currency = refunded
                .iter()
                .find_map(|transaction| transaction.get("currency")?.as_str())
                .map(str::to_ascii_uppercase);
            let amount_minor = currency.as_ref().and_then(|currency| {
                refunded
                    .iter()
                    .map(|transaction| {
                        decimal_to_minor(transaction.get("amount")?.as_str()?, exponent(currency))
                    })
                    .sum::<Option<i64>>()
            });
            Ok(InboundEvent::Refund {
                order: OrderRef {
                    external_order_id: gid("Order", &payload, "order_id")
                        .ok_or_else(|| missing("order"))?,
                    pubky_order_id: None,
                },
                amount_minor,
                currency,
            })
        }
        "app/uninstalled" => Ok(InboundEvent::AppUninstalled),
        "customers/data_request" => Ok(InboundEvent::Compliance {
            topic: ComplianceTopic::CustomerDataRequest,
        }),
        "customers/redact" => Ok(InboundEvent::Compliance {
            topic: ComplianceTopic::CustomerRedact,
        }),
        "shop/redact" => Ok(InboundEvent::Compliance {
            topic: ComplianceTopic::ShopRedact,
        }),
        _ => ignored("unsubscribed_topic"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::provider::CredentialSecret;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    fn app() -> AppContext {
        AppContext {
            connection_id: Uuid::nil(),
            client_id: "id".into(),
            client_secret: CredentialSecret::new(b"shhh".to_vec()),
        }
    }

    fn signed_headers(body: &[u8], topic: &str) -> WebhookHeaders {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"shhh").expect("key");
        mac.update(body);
        WebhookHeaders::from_pairs([
            (
                "X-Shopify-Hmac-Sha256",
                STANDARD.encode(mac.finalize().into_bytes()),
            ),
            ("X-Shopify-Topic", topic.to_string()),
            ("X-Shopify-Event-Id", "evt-1".to_string()),
            ("X-Shopify-Shop-Domain", "Shop.myshopify.com".to_string()),
        ])
    }

    fn webhook(topic: &str) -> VerifiedWebhook {
        VerifiedWebhook {
            event_id: "e".into(),
            topic: topic.into(),
        }
    }

    #[test]
    fn a_webhook_is_accepted_only_with_the_connections_signature() {
        let body = br#"{"id":1}"#;
        let headers = signed_headers(body, "orders/cancelled");
        let verified = verify(Some(&app()), body, &headers).expect("verified");
        assert_eq!(verified.event_id, "evt-1");
        assert_eq!(verified.topic, "orders/cancelled");
        assert!(verify(Some(&app()), br#"{"id":2}"#, &headers).is_err());
        assert!(verify(None, body, &headers).is_err());
        let unsigned = WebhookHeaders::from_pairs([("X-Shopify-Topic", "orders/cancelled")]);
        assert!(verify(Some(&app()), body, &unsigned).is_err());
    }

    #[test]
    fn the_claimed_shop_is_normalised_or_refused() {
        let headers = signed_headers(b"{}", "t");
        assert_eq!(
            shop_of(&headers, b"{}").expect("shop"),
            "shop.myshopify.com"
        );
        let none = WebhookHeaders::default();
        assert_eq!(
            shop_of(&none, br#"{"shop_domain":"x.myshopify.com"}"#).expect("body shop"),
            "x.myshopify.com"
        );
        assert!(shop_of(&none, b"{}").is_err());
        assert!(shop_of(
            &WebhookHeaders::from_pairs([("X-Shopify-Shop-Domain", "evil.com")]),
            b"{}"
        )
        .is_err());
    }

    #[test]
    fn an_inventory_webhook_names_the_item() {
        let event = parse(
            &webhook("inventory_levels/update"),
            br#"{"inventory_item_id":4242,"location_id":1,"available":3}"#,
        )
        .expect("event");
        assert_eq!(
            event,
            InboundEvent::InventoryChanged {
                inventory_item_id: "gid://shopify/InventoryItem/4242".into()
            }
        );
    }

    #[test]
    fn a_store_cancel_carries_the_pubky_order_from_its_tag_or_attribute() {
        let id = "11111111-2222-4333-8444-555555555555";
        let by_tag =
            format!(r#"{{"id":99,"cancel_reason":"customer","tags":"pubky, pubky-order-{id}"}}"#);
        let by_attribute = format!(
            r#"{{"id":99,"note_attributes":[{{"name":"pubky_order_id","value":"{id}"}}]}}"#
        );
        for body in [by_tag, by_attribute] {
            match parse(&webhook("orders/cancelled"), body.as_bytes()).expect("event") {
                InboundEvent::OrderCancelled { order, .. } => {
                    assert_eq!(order.external_order_id, "gid://shopify/Order/99");
                    assert_eq!(order.pubky_order_id, Uuid::parse_str(id).ok());
                }
                other => panic!("{other:?}"),
            }
        }
        match parse(&webhook("orders/cancelled"), br#"{"id":5,"tags":""}"#).expect("event") {
            InboundEvent::OrderCancelled { order, reason } => {
                assert_eq!(order.pubky_order_id, None);
                assert_eq!(reason, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_store_refund_sums_its_refund_transactions() {
        let body = br#"{"id":1,"order_id":77,"transactions":[
            {"kind":"refund","status":"success","amount":"10.50","currency":"usd"},
            {"kind":"refund","status":"success","amount":"2.00","currency":"usd"},
            {"kind":"refund","status":"failure","amount":"99.00","currency":"usd"}]}"#;
        match parse(&webhook("refunds/create"), body).expect("event") {
            InboundEvent::Refund {
                order,
                amount_minor,
                currency,
            } => {
                assert_eq!(order.external_order_id, "gid://shopify/Order/77");
                assert_eq!(amount_minor, Some(1250));
                assert_eq!(currency.as_deref(), Some("USD"));
            }
            other => panic!("{other:?}"),
        }
        match parse(&webhook("refunds/create"), br#"{"order_id":77}"#).expect("event") {
            InboundEvent::Refund { amount_minor, .. } => assert_eq!(amount_minor, None),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_fulfillment_needs_a_tracking_number() {
        let body =
            br#"{"id":1,"order_id":77,"tracking_company":"DHL","tracking_numbers":["JD01"]}"#;
        match parse(&webhook("fulfillments/create"), body).expect("event") {
            InboundEvent::Fulfillment {
                order,
                carrier,
                tracking_number,
            } => {
                assert_eq!(order.external_order_id, "gid://shopify/Order/77");
                assert_eq!(
                    (carrier.as_str(), tracking_number.as_str()),
                    ("DHL", "JD01")
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse(
                &webhook("fulfillments/create"),
                br#"{"order_id":77,"tracking_number":null}"#
            ),
            Ok(InboundEvent::Ignored { .. })
        ));
    }

    #[test]
    fn privacy_and_lifecycle_topics_are_recognised() {
        for (topic, expected) in [
            (
                "customers/data_request",
                ComplianceTopic::CustomerDataRequest,
            ),
            ("customers/redact", ComplianceTopic::CustomerRedact),
            ("shop/redact", ComplianceTopic::ShopRedact),
        ] {
            assert_eq!(
                parse(&webhook(topic), b"{}").expect("event"),
                InboundEvent::Compliance { topic: expected }
            );
        }
        assert_eq!(
            parse(&webhook("app/uninstalled"), b"{}").expect("event"),
            InboundEvent::AppUninstalled
        );
        assert!(matches!(
            parse(&webhook("products/update"), b"{}"),
            Ok(InboundEvent::Ignored { .. })
        ));
        assert!(parse(&webhook("orders/cancelled"), b"not json").is_err());
    }
}
