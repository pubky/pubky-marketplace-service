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

use std::collections::BTreeMap;

use serde_json::{json, Value};
use uuid::Uuid;

use super::auth::{normalize_shop, verify_webhook_hmac};
use super::client::Client;
use super::documents;
use super::money::{decimal_to_minor, exponent};
use super::orders;
use crate::connectors::provider::{
    AppContext, ComplianceTopic, InboundEvent, OrderRef, ProviderError, PubkyRefundMark,
    RefundAmount, VerifiedWebhook, WebhookHeaders,
};

pub const HEADER_HMAC: &str = "x-shopify-hmac-sha256";
pub const HEADER_TOPIC: &str = "x-shopify-topic";
pub const HEADER_SHOP: &str = "x-shopify-shop-domain";
pub const HEADER_EVENT_ID: &str = "x-shopify-event-id";
pub const HEADER_WEBHOOK_ID: &str = "x-shopify-webhook-id";

/// The webhook topic as it appears in `X-Shopify-Topic` and the matching
/// `WebhookSubscriptionTopic` value.
pub const SUBSCRIBED_TOPICS: [(&str, &str); 6] = [
    ("inventory_levels/update", "INVENTORY_LEVELS_UPDATE"),
    ("fulfillments/create", "FULFILLMENTS_CREATE"),
    ("fulfillments/update", "FULFILLMENTS_UPDATE"),
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

/// The most pages of subscriptions one registration reads (100 each).
const MAX_SUBSCRIPTION_PAGES: usize = 20;

type Subscription = (String, String, String);

/// Every webhook subscription this app has in the store, page by page.
async fn subscriptions(
    client: &Client,
    shop: &str,
    token: &str,
) -> Result<Vec<Subscription>, ProviderError> {
    let mut found = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..MAX_SUBSCRIPTION_PAGES {
        let data = client
            .execute(
                shop,
                token,
                documents::WEBHOOK_SUBSCRIPTIONS,
                json!({ "after": after }),
            )
            .await?;
        let page = data
            .get("webhookSubscriptions")
            .ok_or_else(|| malformed("webhook subscription list"))?;
        let nodes = page
            .get("nodes")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed("webhook subscription list"))?;
        found.extend(nodes.iter().filter_map(|node| {
            Some((
                node.get("id")?.as_str()?.to_string(),
                node.get("topic")?.as_str()?.to_string(),
                node.get("uri")?.as_str()?.to_string(),
            ))
        }));
        if page
            .pointer("/pageInfo/hasNextPage")
            .and_then(Value::as_bool)
            != Some(true)
        {
            return Ok(found);
        }
        after = page
            .pointer("/pageInfo/endCursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        if after.is_none() {
            return Ok(found);
        }
    }
    Err(ProviderError::Rejected {
        code: "too_many_webhook_subscriptions".into(),
        message: format!(
            "The app has more than {} webhook subscriptions, so stale ones cannot be ruled out.",
            MAX_SUBSCRIPTION_PAGES * 100
        ),
    })
}

pub async fn register(
    client: &Client,
    shop: &str,
    token: &str,
    webhook_url: &str,
) -> Result<(), ProviderError> {
    let known = subscriptions(client, shop, token).await?;
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
            if !errors.is_empty() {
                // `userErrors` of this mutation carry no code, so the message
                // is not interpreted. A concurrent registration (the merchant
                // opening the app twice) can make this one fail because the
                // other won: read the list again, and only a subscription
                // that is still missing is a failure.
                let now_known = subscriptions(client, shop, token).await?;
                if !now_known
                    .iter()
                    .any(|(_, found, uri)| found == topic && uri == webhook_url)
                {
                    let message = errors
                        .iter()
                        .filter_map(|error| error.get("message").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(ProviderError::Rejected {
                        code: "webhook_subscription".into(),
                        message: message.chars().take(300).collect(),
                    });
                }
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
        "fulfillments/create" | "fulfillments/update" => {
            let update = webhook.topic == "fulfillments/update";
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
            if update && payload.get("status").and_then(Value::as_str) == Some("cancelled") {
                return ignored("fulfillment_cancelled");
            }
            let carrier = payload
                .get("tracking_company")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|company| !company.is_empty())
                .unwrap_or("Other")
                .to_string();
            let order = OrderRef {
                external_order_id: gid("Order", &payload, "order_id")
                    .ok_or_else(|| missing("order"))?,
                pubky_order_id: None,
            };
            // A fulfilment with no tracking still ships the Pubky order (the
            // number is added later); an update with none changes nothing.
            match (update, tracking) {
                (false, tracking) => Ok(InboundEvent::Fulfillment {
                    order,
                    carrier,
                    tracking_number: tracking.unwrap_or_default().to_string(),
                }),
                (true, Some(tracking)) => Ok(InboundEvent::TrackingUpdated {
                    order,
                    carrier,
                    tracking_number: tracking.to_string(),
                }),
                (true, None) => ignored("no_tracking_number"),
            }
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
            // Refunded money per currency: a payload that mixes currencies is
            // not summed into one figure.
            let mut by_currency: BTreeMap<String, Option<i64>> = BTreeMap::new();
            for transaction in payload
                .get("transactions")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                .filter(|transaction| {
                    transaction.get("kind").and_then(Value::as_str) == Some("refund")
                        && transaction.get("status").and_then(Value::as_str) != Some("failure")
                })
            {
                let Some(currency) = transaction
                    .get("currency")
                    .and_then(Value::as_str)
                    .map(str::to_ascii_uppercase)
                else {
                    continue;
                };
                let minor = transaction
                    .get("amount")
                    .and_then(Value::as_str)
                    .and_then(|amount| decimal_to_minor(amount, exponent(&currency)));
                let total = by_currency.entry(currency).or_insert(Some(0));
                *total = total.zip(minor).and_then(|(sum, add)| sum.checked_add(add));
            }
            let amounts = by_currency
                .into_iter()
                .filter_map(|(currency, total)| {
                    Some(RefundAmount {
                        currency,
                        amount_minor: total?,
                    })
                })
                .collect();
            let mark = payload
                .get("note")
                .and_then(Value::as_str)
                .and_then(orders::parse_refund_mark)
                .map(|(refund_id, amount_minor, currency)| PubkyRefundMark {
                    refund_id,
                    amount: RefundAmount {
                        currency,
                        amount_minor,
                    },
                });
            Ok(InboundEvent::StoreRefund {
                order: OrderRef {
                    external_order_id: gid("Order", &payload, "order_id")
                        .ok_or_else(|| missing("order"))?,
                    pubky_order_id: None,
                },
                amounts,
                mark,
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
    fn a_store_refund_sums_its_refund_transactions_per_currency() {
        let body = br#"{"id":1,"order_id":77,"transactions":[
            {"kind":"refund","status":"success","amount":"10.50","currency":"usd"},
            {"kind":"refund","status":"success","amount":"2.00","currency":"usd"},
            {"kind":"refund","status":"failure","amount":"99.00","currency":"usd"},
            {"kind":"refund","status":"success","amount":"500","currency":"jpy"},
            {"kind":"sale","status":"success","amount":"7.00","currency":"usd"}]}"#;
        match parse(&webhook("refunds/create"), body).expect("event") {
            InboundEvent::StoreRefund {
                order,
                amounts,
                mark,
            } => {
                assert_eq!(order.external_order_id, "gid://shopify/Order/77");
                assert_eq!(
                    amounts,
                    vec![
                        RefundAmount {
                            currency: "JPY".into(),
                            amount_minor: 500
                        },
                        RefundAmount {
                            currency: "USD".into(),
                            amount_minor: 1250
                        },
                    ],
                    "currencies are never added together"
                );
                assert_eq!(mark, None);
            }
            other => panic!("{other:?}"),
        }
        match parse(&webhook("refunds/create"), br#"{"order_id":77}"#).expect("event") {
            InboundEvent::StoreRefund { amounts, .. } => assert!(amounts.is_empty()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_refund_this_service_made_carries_its_mark_back() {
        let mark = orders::refund_mark("7d9c0e0e-1111-4222-8333-444455556666", 8700, "USD");
        let body = format!(
            r#"{{"id":1,"order_id":77,"note":"Pubky refund for order x. {mark}","transactions":[
                {{"kind":"refund","status":"success","amount":"87.00","currency":"USD"}}]}}"#
        );
        match parse(&webhook("refunds/create"), body.as_bytes()).expect("event") {
            InboundEvent::StoreRefund { mark, amounts, .. } => {
                assert_eq!(
                    mark,
                    Some(PubkyRefundMark {
                        refund_id: "7d9c0e0e-1111-4222-8333-444455556666".into(),
                        amount: RefundAmount {
                            currency: "USD".into(),
                            amount_minor: 8700
                        },
                    })
                );
                assert_eq!(amounts[0].amount_minor, 8700);
            }
            other => panic!("{other:?}"),
        }
        let merchants =
            br#"{"order_id":77,"note":"customer returned the boots","transactions":[]}"#;
        match parse(&webhook("refunds/create"), merchants).expect("event") {
            InboundEvent::StoreRefund { mark, .. } => assert_eq!(mark, None),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_fulfillment_ships_with_or_without_tracking_and_an_update_carries_it_later() {
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
        // No tracking: still a shipment, carrier Other, empty number.
        match parse(
            &webhook("fulfillments/create"),
            br#"{"order_id":77,"tracking_number":null,"tracking_company":null}"#,
        )
        .expect("event")
        {
            InboundEvent::Fulfillment {
                carrier,
                tracking_number,
                ..
            } => {
                assert_eq!((carrier.as_str(), tracking_number.as_str()), ("Other", ""));
            }
            other => panic!("{other:?}"),
        }
        // The tracking arrives later, on an update.
        match parse(
            &webhook("fulfillments/update"),
            br#"{"order_id":77,"status":"success","tracking_company":"UPS","tracking_number":" 1Z999 "}"#,
        )
        .expect("event")
        {
            InboundEvent::TrackingUpdated { order, carrier, tracking_number } => {
                assert_eq!(order.external_order_id, "gid://shopify/Order/77");
                assert_eq!((carrier.as_str(), tracking_number.as_str()), ("UPS", "1Z999"));
            }
            other => panic!("{other:?}"),
        }
        // An update that carries no tracking, or cancels the fulfilment, changes nothing.
        for body in [
            &br#"{"order_id":77,"tracking_number":null}"#[..],
            &br#"{"order_id":77,"status":"cancelled","tracking_number":"X1"}"#[..],
        ] {
            assert!(matches!(
                parse(&webhook("fulfillments/update"), body),
                Ok(InboundEvent::Ignored { .. })
            ));
        }
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
