//! Provider webhooks: dedupe on receipt, apply on the worker.
//!
//! The ingress route verifies the signature, normalises the event and writes
//! it to `connector_inbound_events`, unique on the provider's own event id
//! per connection, then answers. The worker applies it later, so a slow
//! `inventory.adjust` never makes the store's delivery time out and a
//! redelivery changes nothing. Only the normalised event is stored, never the
//! raw body: a store's order webhooks carry customer data this service has no
//! use for.
//!
//! What each event does:
//!
//! * stock level: `inventory.adjust` for a mapped variant, nothing otherwise;
//! * fulfillment: `fulfillment.ship` on the Pubky order the push created;
//! * order cancelled in the store: a seller alert, never a refund;
//! * refund in the store: a seller alert, never a refund;
//! * app uninstalled: the credential is deleted and the connection closed;
//! * privacy topics (`customers/data_request`, `customers/redact`,
//!   `shop/redact`): the service keeps no store customer data, so the first
//!   two have nothing to return or erase; `shop/redact` closes the
//!   connection if it is still open, deletes the retained app secret and
//!   empties the stored event payloads. They are applied even for a
//!   connection that already disconnected, because Shopify sends `shop/redact`
//!   48 hours after an uninstall; the connection keeps its app secret for up
//!   to 30 days after a disconnect so those webhooks can be verified.

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::AppState;

use super::deterministic_uuid;
use super::provider::{ComplianceTopic, InboundEvent, OrderRef, VerifiedWebhook};
use super::stock::{self, StockOutcome};
use super::store::{self, AlertKind, ConnectionRow};
use super::ConnectorRuntime;

#[derive(Debug, PartialEq, Eq)]
pub enum Recorded {
    New(Uuid),
    Duplicate,
}

/// Records a verified webhook once per `(connection, provider event id)`.
pub async fn record(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection: &ConnectionRow,
    webhook: &VerifiedWebhook,
    event: &InboundEvent,
) -> Result<Recorded, sqlx::Error> {
    let id = Uuid::new_v4();
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO connector_inbound_events \
         (id, connection_id, provider, provider_event_id, topic, event, state, attempts, \
          next_attempt_at, received_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $7) \
         ON CONFLICT (connection_id, provider_event_id) DO NOTHING RETURNING id",
    )
    .bind(id)
    .bind(connection.id)
    .bind(&connection.provider)
    .bind(&webhook.event_id)
    .bind(&webhook.topic)
    .bind(serde_json::to_value(event).expect("inbound event serializes"))
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(match inserted {
        Some(id) => Recorded::New(id),
        None => Recorded::Duplicate,
    })
}

#[derive(sqlx::FromRow)]
struct DueEvent {
    id: Uuid,
    connection_id: Uuid,
    provider_event_id: String,
    event: serde_json::Value,
    attempts: i32,
}

enum Handled {
    Applied(String),
    Ignored(String),
    Retry(String),
}

/// Applies every due inbound event, one at a time. Returns how many reached
/// a final state.
pub async fn process_due(
    state: &AppState,
    runtime: &ConnectorRuntime,
    limit: usize,
) -> Result<u64, sqlx::Error> {
    let mut finished = 0;
    for _ in 0..limit {
        let now = state.clock.now();
        let lease_until =
            now + chrono::Duration::from_std(runtime.settings.lease).unwrap_or_default();
        let due: Option<DueEvent> = sqlx::query_as(
            "UPDATE connector_inbound_events SET attempts = attempts + 1, lease_until = $2 \
             WHERE id = ( \
                 SELECT id FROM connector_inbound_events \
                 WHERE state = 'pending' AND next_attempt_at <= $1 \
                   AND (lease_until IS NULL OR lease_until <= $1) \
                 ORDER BY next_attempt_at, id FOR UPDATE SKIP LOCKED LIMIT 1) \
             RETURNING id, connection_id, provider_event_id, event, attempts",
        )
        .bind(now)
        .bind(lease_until)
        .fetch_optional(&state.pool)
        .await?;
        let Some(due) = due else {
            break;
        };
        let handled = match serde_json::from_value::<InboundEvent>(due.event.clone()) {
            Ok(event) => handle(state, &due, event).await?,
            Err(_) => Handled::Ignored("unreadable_event".to_string()),
        };
        let now = state.clock.now();
        match handled {
            Handled::Applied(outcome) => {
                settle(&state.pool, due.id, "applied", &outcome, now).await?;
                finished += 1;
            }
            Handled::Ignored(outcome) => {
                settle(&state.pool, due.id, "ignored", &outcome, now).await?;
                finished += 1;
            }
            Handled::Retry(outcome) if due.attempts >= runtime.settings.max_attempts => {
                settle(&state.pool, due.id, "failed", &outcome, now).await?;
                if let Some(connection) = store::get(&state.pool, due.connection_id).await? {
                    store::raise_alert(
                        &state.pool,
                        now,
                        &connection,
                        AlertKind::InboundFailed,
                        None,
                        &format!("A store event could not be applied: {outcome}"),
                        &format!("inbound|{}", due.id),
                    )
                    .await?;
                }
                finished += 1;
            }
            Handled::Retry(outcome) => {
                let retry_at = now
                    + chrono::Duration::from_std(runtime.settings.backoff(due.attempts))
                        .unwrap_or_default();
                sqlx::query(
                    "UPDATE connector_inbound_events SET lease_until = NULL, \
                     next_attempt_at = $2, outcome = $3 WHERE id = $1",
                )
                .bind(due.id)
                .bind(retry_at)
                .bind(store::truncate(&outcome, 500))
                .execute(&state.pool)
                .await?;
            }
        }
    }
    Ok(finished)
}

async fn settle(
    pool: &PgPool,
    id: Uuid,
    final_state: &str,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE connector_inbound_events SET state = $2, outcome = $3, lease_until = NULL, \
         processed_at = $4 WHERE id = $1",
    )
    .bind(id)
    .bind(final_state)
    .bind(store::truncate(outcome, 500))
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

async fn handle(
    state: &AppState,
    due: &DueEvent,
    event: InboundEvent,
) -> Result<Handled, sqlx::Error> {
    let Some(connection) = store::get(&state.pool, due.connection_id).await? else {
        return Ok(Handled::Ignored("connection_missing".to_string()));
    };
    let now = state.clock.now();
    if let InboundEvent::Compliance { topic } = event {
        return compliance(state, &connection, due, topic, now).await;
    }
    if !connection.is_live() {
        return Ok(Handled::Ignored("connection_not_live".to_string()));
    }
    match event {
        InboundEvent::StockLevel {
            provider_product_id,
            provider_variant_id,
            available,
        } => {
            let Some(mapping) = store::mapping_for_provider_variant(
                &state.pool,
                connection.id,
                &provider_product_id,
                &provider_variant_id,
            )
            .await?
            else {
                return Ok(Handled::Ignored("unmapped_variant".to_string()));
            };
            Ok(
                match stock::apply_stock_level(
                    state,
                    &connection,
                    &mapping,
                    available,
                    Some(&due.provider_event_id),
                )
                .await?
                {
                    StockOutcome::Applied { delta } => Handled::Applied(format!("delta {delta}")),
                    StockOutcome::AlreadyApplied => Handled::Applied("already applied".to_string()),
                    StockOutcome::Unchanged => Handled::Ignored("unchanged".to_string()),
                    StockOutcome::Ignored(reason) => Handled::Ignored(reason.to_string()),
                    StockOutcome::Retry(reason) => Handled::Retry(reason),
                },
            )
        }
        InboundEvent::Fulfillment {
            order,
            carrier,
            tracking_number,
        } => {
            let Some(order_id) = resolve_order(&state.pool, &connection, &order).await? else {
                return Ok(Handled::Ignored("unknown_order".to_string()));
            };
            ship(
                state,
                &connection,
                due,
                order_id,
                &carrier,
                &tracking_number,
            )
            .await
        }
        InboundEvent::OrderCancelled { order, reason } => {
            let Some(order_id) = resolve_order(&state.pool, &connection, &order).await? else {
                return Ok(Handled::Ignored("unknown_order".to_string()));
            };
            let why = reason
                .map(|reason| format!(" Reason given: {reason}."))
                .unwrap_or_default();
            store::raise_alert(
                &state.pool,
                now,
                &connection,
                AlertKind::MerchantCancelled,
                Some(order_id),
                &format!(
                    "The order was cancelled in the store.{why} Nothing was refunded: if the \
                     buyer is owed money, cancel the order here and send the refund yourself."
                ),
                &format!(
                    "merchant-cancel|{}|{}",
                    connection.id, order.external_order_id
                ),
            )
            .await?;
            Ok(Handled::Applied("seller alerted".to_string()))
        }
        InboundEvent::Refund {
            order,
            amount_minor,
            currency,
        } => {
            let Some(order_id) = resolve_order(&state.pool, &connection, &order).await? else {
                return Ok(Handled::Ignored("unknown_order".to_string()));
            };
            let amount = match (amount_minor, currency) {
                (Some(amount), Some(currency)) => format!(" ({amount} minor units {currency})"),
                (Some(amount), None) => format!(" ({amount} minor units)"),
                _ => String::new(),
            };
            store::raise_alert(
                &state.pool,
                now,
                &connection,
                AlertKind::MerchantRefunded,
                Some(order_id),
                &format!(
                    "The order was refunded in the store{amount}. Nothing was sent to the \
                     buyer from here: record or send the refund yourself."
                ),
                &format!(
                    "merchant-refund|{}|{}",
                    connection.id, due.provider_event_id
                ),
            )
            .await?;
            Ok(Handled::Applied("seller alerted".to_string()))
        }
        InboundEvent::AppUninstalled => {
            store::disconnect(&state.pool, connection.id, now, retention_end(now)).await?;
            store::raise_alert(
                &state.pool,
                now,
                &connection,
                AlertKind::StoreUninstalled,
                None,
                "The store removed the connection. Its credential was deleted; connect \
                 the store again to resume syncing.",
                &format!("uninstalled|{}|{}", connection.id, due.provider_event_id),
            )
            .await?;
            Ok(Handled::Applied("connection closed".to_string()))
        }
        InboundEvent::Compliance { .. } => unreachable!("handled above"),
        InboundEvent::Ignored { reason } => Ok(Handled::Ignored(reason)),
    }
}

async fn compliance(
    state: &AppState,
    connection: &ConnectionRow,
    due: &DueEvent,
    topic: ComplianceTopic,
    now: DateTime<Utc>,
) -> Result<Handled, sqlx::Error> {
    match topic {
        ComplianceTopic::CustomerDataRequest | ComplianceTopic::CustomerRedact => Ok(
            Handled::Applied("no store customer data is held".to_string()),
        ),
        ComplianceTopic::ShopRedact => {
            store::disconnect(&state.pool, connection.id, now, retention_end(now)).await?;
            // The privacy topics that matter for this store are handled: the
            // retained app secret has done its job.
            store::delete_app_credentials(&state.pool, connection.id).await?;
            sqlx::query(
                "UPDATE connector_inbound_events SET event = '{}'::jsonb \
                 WHERE connection_id = $1 AND id <> $2",
            )
            .bind(connection.id)
            .bind(due.id)
            .execute(&state.pool)
            .await?;
            Ok(Handled::Applied("store data deleted".to_string()))
        }
    }
}

fn retention_end(now: DateTime<Utc>) -> DateTime<Utc> {
    now + chrono::Duration::seconds(super::config::APP_SECRET_RETENTION_SECONDS)
}

/// The Pubky order a store order refers to, only when the ledger shows this
/// connection created it.
async fn resolve_order(
    pool: &PgPool,
    connection: &ConnectionRow,
    order: &OrderRef,
) -> Result<Option<Uuid>, sqlx::Error> {
    let by_external: Option<Uuid> = sqlx::query_scalar(
        "SELECT pubky_order_id FROM connector_order_push \
         WHERE connection_id = $1 AND external_id = $2 AND state = 'created'",
    )
    .bind(connection.id)
    .bind(&order.external_order_id)
    .fetch_optional(pool)
    .await?;
    if by_external.is_some() {
        return Ok(by_external);
    }
    let Some(pubky_order_id) = order.pubky_order_id else {
        return Ok(None);
    };
    sqlx::query_scalar(
        "SELECT pubky_order_id FROM connector_order_push \
         WHERE connection_id = $1 AND pubky_order_id = $2 AND state = 'created'",
    )
    .bind(connection.id)
    .bind(pubky_order_id)
    .fetch_optional(pool)
    .await
}

/// Applies a store shipment to the Pubky order.
///
/// An order not yet shipped is shipped with `fulfillment.ship`, with the
/// tracking number when the store has one and without it when it has none
/// (carrier `Other` unless the store names one). An order that is already
/// shipped has its tracking added or changed with
/// `fulfillment.update_tracking`; the same values again change nothing.
///
/// Both commands are newer than the rest of the service. A service without
/// them refuses the command as `INVALID_COMMAND` (an unknown kind, or a
/// `fulfillment.ship` without a tracking number), and that refusal is final and
/// quiet: the event is ignored with a stable reason, never retried and never
/// an alert, so the connector works the same whether or not the service has
/// them yet.
async fn ship(
    state: &AppState,
    connection: &ConnectionRow,
    due: &DueEvent,
    order_id: Uuid,
    carrier: &str,
    tracking_number: &str,
) -> Result<Handled, sqlx::Error> {
    let tracking: String = tracking_number.trim().chars().take(200).collect();
    let carrier: String = {
        let trimmed: String = carrier.trim().chars().take(100).collect();
        if trimmed.is_empty() {
            "Other".to_string()
        } else {
            trimmed
        }
    };
    let current: Option<(i64, String, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT revision, state, shipment FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await?;
    let Some((revision, order_state, shipment)) = current else {
        return Ok(Handled::Ignored("order_missing".to_string()));
    };
    let already_shipped = order_state == "shipped";
    if already_shipped {
        let held = |name: &str| {
            shipment
                .as_ref()
                .and_then(|shipment| shipment.get(name))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        if tracking.is_empty() {
            return Ok(Handled::Ignored("already_shipped".to_string()));
        }
        if held("tracking_number") == tracking && held("carrier") == carrier {
            return Ok(Handled::Ignored("tracking_unchanged".to_string()));
        }
    }
    let (kind, command_seed, payload) = if already_shipped {
        (
            "fulfillment.update_tracking",
            "connector-tracking",
            json!({
                "order_id": order_id,
                "carrier": carrier,
                "tracking_number": tracking,
            }),
        )
    } else {
        let mut payload = json!({ "order_id": order_id, "carrier": carrier });
        if !tracking.is_empty() {
            payload["tracking_number"] = json!(tracking);
        }
        ("fulfillment.ship", "connector-ship", payload)
    };
    let raw = json!({
        "version": 1,
        "command_id": deterministic_uuid(&format!("{command_seed}|{}", due.id)),
        "aggregate_id": marketplace_domain::ids::order_aggregate_id(order_id),
        "expected_revision": revision,
        "issued_at": format_timestamp(state.clock.now()),
        "kind": kind,
        "payload": payload,
    });
    let (status, body) = crate::executor::execute(state, &connection.seller_pubky, &raw).await?;
    if status.is_success() {
        return Ok(Handled::Applied(
            if already_shipped {
                "tracking_updated"
            } else {
                "shipped"
            }
            .to_string(),
        ));
    }
    let code = body
        .pointer("/error/code")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    Ok(match code.as_str() {
        "revision_conflict" => Handled::Retry("revision_conflict".to_string()),
        "invalid_state" => Handled::Ignored("order_not_shippable".to_string()),
        "invalid_command" if already_shipped => {
            Handled::Ignored("tracking_update_not_supported".to_string())
        }
        "invalid_command" if tracking.is_empty() => {
            Handled::Ignored("tracking_optional_not_supported".to_string())
        }
        "" => Handled::Retry(format!("status_{}", status.as_u16())),
        other => Handled::Ignored(format!("refused_{other}")),
    })
}
