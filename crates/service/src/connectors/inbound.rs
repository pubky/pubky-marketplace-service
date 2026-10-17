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
use super::provider::{
    ComplianceTopic, InboundEvent, OrderRef, PubkyRefundMark, RefundAmount, VerifiedWebhook,
};
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
    received_at: DateTime<Utc>,
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
             RETURNING id, connection_id, provider_event_id, event, attempts, received_at",
        )
        .bind(now)
        .bind(lease_until)
        .fetch_optional(&state.pool)
        .await?;
        let Some(due) = due else {
            break;
        };
        let handled = match serde_json::from_value::<InboundEvent>(due.event.clone()) {
            Ok(event) => handle(state, runtime, &due, event).await?,
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
    runtime: &ConnectorRuntime,
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
    let event = match event {
        InboundEvent::InventoryChanged { inventory_item_id } => {
            match resolve_inventory(state, runtime, &connection, &inventory_item_id).await? {
                Ok(resolved) => resolved,
                Err(handled) => return Ok(handled),
            }
        }
        other => other,
    };
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
                    StockOutcome::MappingError(reason) => {
                        Handled::Ignored(format!("variant_mapping_error: {reason}"))
                    }
                    StockOutcome::Switched { .. } => Handled::Applied("counted again".to_string()),
                },
            )
        }
        InboundEvent::MadeToOrder {
            provider_product_id,
            provider_variant_id,
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
                match stock::apply_made_to_order(state, &connection, &mapping).await? {
                    StockOutcome::Switched { .. } => Handled::Applied("made to order".to_string()),
                    StockOutcome::Unchanged => Handled::Ignored("unchanged".to_string()),
                    StockOutcome::Ignored(reason) => Handled::Ignored(reason.to_string()),
                    StockOutcome::MappingError(reason) => {
                        Handled::Ignored(format!("variant_mapping_error: {reason}"))
                    }
                    StockOutcome::Retry(reason) => Handled::Retry(reason),
                    StockOutcome::Applied { .. } | StockOutcome::AlreadyApplied => {
                        Handled::Applied("made to order".to_string())
                    }
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
            apply_tracking(
                state,
                runtime,
                &connection,
                due,
                order_id,
                &carrier,
                &tracking_number,
                false,
            )
            .await
        }
        InboundEvent::TrackingUpdated {
            order,
            carrier,
            tracking_number,
        } => {
            let Some(order_id) = resolve_order(&state.pool, &connection, &order).await? else {
                return Ok(Handled::Ignored("unknown_order".to_string()));
            };
            apply_tracking(
                state,
                runtime,
                &connection,
                due,
                order_id,
                &carrier,
                &tracking_number,
                true,
            )
            .await
        }
        InboundEvent::OrderCancelled { order, reason } => {
            let Some(order_id) = resolve_order(&state.pool, &connection, &order).await? else {
                return Ok(Handled::Ignored("unknown_order".to_string()));
            };
            match mirror_echo(
                &state.pool,
                &connection,
                order_id,
                EchoOf::Cancel,
                due.received_at,
            )
            .await?
            {
                Echo::Yes => return Ok(Handled::Ignored("pubky_cancel_echo".to_string())),
                Echo::InFlight => return Ok(Handled::Retry("pubky_cancel_in_flight".to_string())),
                Echo::No => {}
            }
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
            let amounts = match (amount_minor, currency) {
                (Some(amount_minor), Some(currency)) => vec![RefundAmount {
                    currency,
                    amount_minor,
                }],
                _ => Vec::new(),
            };
            let echo = EchoOf::Refund {
                mark: None,
                amounts: &amounts,
                by_amount: true,
                amount_known: amount_minor.is_some(),
            };
            store_refund(state, &connection, due, &order, &amounts, echo, now).await
        }
        InboundEvent::StoreRefund {
            order,
            amounts,
            mark,
        } => {
            let echo = EchoOf::Refund {
                mark: mark.as_ref(),
                amounts: &amounts,
                by_amount: false,
                amount_known: true,
            };
            store_refund(state, &connection, due, &order, &amounts, echo, now).await
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
        InboundEvent::Compliance { .. } | InboundEvent::InventoryChanged { .. } => {
            unreachable!("handled above")
        }
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

/// How long after a mirrored cancel or refund finished the store's webhook for
/// it is still treated as its echo. The store reports its own change within
/// seconds; this allows for the webhook queue and a few delivery retries while
/// keeping the window in which a real merchant action could be mistaken for the
/// echo short. A webhook later than this raises an alert even if it was the
/// echo: a spurious alert is cheap, a swallowed refund is not.
pub const ECHO_WINDOW: chrono::Duration = chrono::Duration::hours(6);

enum EchoOf<'a> {
    Cancel,
    Refund {
        /// What the provider wrote on a refund it created, read back from the
        /// webhook.
        mark: Option<&'a PubkyRefundMark>,
        amounts: &'a [RefundAmount],
        /// A provider with no mark (the simple `Refund` event): match on the
        /// recorded amount instead.
        by_amount: bool,
        amount_known: bool,
    },
}

enum Echo {
    /// The webhook reports a change this connector applied.
    Yes,
    /// The mirror that would explain it is still running; look again later.
    InFlight,
    /// Not an echo: the merchant acted.
    No,
}

#[derive(sqlx::FromRow)]
struct MirrorRow {
    action_key: String,
    state: String,
    payload: serde_json::Value,
}

/// Whether a store cancel or refund webhook is the echo of a mirror this
/// connector applied. Only a mirror that was **applied** (state `done`),
/// whose action existed before the webhook arrived and finished at most
/// [`ECHO_WINDOW`] before it, counts; a mirror that never reached the store
/// (pending, quarantined) explains nothing. For a refund the store's refund
/// must also carry the Pubky refund id this connector wrote on it and the
/// amount it created. A mirror still running when the webhook is handled
/// answers [`Echo::InFlight`], so the webhook is looked at again once it is
/// settled instead of being judged on a half-written state.
async fn mirror_echo(
    pool: &PgPool,
    connection: &ConnectionRow,
    order_id: Uuid,
    of: EchoOf<'_>,
    received_at: DateTime<Utc>,
) -> Result<Echo, sqlx::Error> {
    let kind = match of {
        EchoOf::Cancel => "cancel",
        EchoOf::Refund { .. } => "refund",
    };
    let rows: Vec<MirrorRow> = sqlx::query_as(
        "SELECT a.action_key, a.state, a.payload \
         FROM connector_order_actions a \
         JOIN connector_order_push p ON p.id = a.push_id \
         WHERE p.connection_id = $1 AND p.pubky_order_id = $2 AND a.kind = $3 \
           AND a.created_at <= $4 \
           AND (a.state = 'sending' \
                OR (a.state = 'done' AND a.updated_at >= $5)) \
         ORDER BY a.updated_at DESC",
    )
    .bind(connection.id)
    .bind(order_id)
    .bind(kind)
    .bind(received_at)
    .bind(received_at - ECHO_WINDOW)
    .fetch_all(pool)
    .await?;
    let mut in_flight = false;
    for row in &rows {
        let applies = match &of {
            EchoOf::Cancel => true,
            EchoOf::Refund {
                mark: Some(mark), ..
            } => row.action_key == mark.refund_id,
            EchoOf::Refund {
                mark: None,
                by_amount: true,
                ..
            } => true,
            EchoOf::Refund {
                mark: None,
                by_amount: false,
                ..
            } => false,
        };
        if !applies {
            continue;
        }
        if row.state == "sending" {
            in_flight = true;
            continue;
        }
        let matches = match &of {
            EchoOf::Cancel => true,
            EchoOf::Refund {
                mark: Some(mark),
                amounts,
                ..
            } => {
                amounts.len() == 1
                    && amounts[0]
                        .currency
                        .eq_ignore_ascii_case(&mark.amount.currency)
                    && amounts[0].amount_minor == mark.amount.amount_minor
            }
            EchoOf::Refund {
                mark: None,
                amounts,
                amount_known,
                ..
            } => {
                !*amount_known
                    || match (*amounts, row.payload.get("amount_minor")) {
                        ([only], Some(recorded)) => {
                            recorded.as_i64() == Some(only.amount_minor)
                                && row
                                    .payload
                                    .get("order_currency")
                                    .and_then(serde_json::Value::as_str)
                                    .is_none_or(|currency| {
                                        currency.eq_ignore_ascii_case(&only.currency)
                                    })
                        }
                        _ => false,
                    }
            }
        };
        if matches {
            return Ok(Echo::Yes);
        }
    }
    Ok(if in_flight { Echo::InFlight } else { Echo::No })
}

/// The seller alert for a refund made in the store, unless it is the echo of
/// one this connector mirrored.
async fn store_refund(
    state: &AppState,
    connection: &ConnectionRow,
    due: &DueEvent,
    order: &OrderRef,
    amounts: &[RefundAmount],
    echo: EchoOf<'_>,
    now: DateTime<Utc>,
) -> Result<Handled, sqlx::Error> {
    let Some(order_id) = resolve_order(&state.pool, connection, order).await? else {
        return Ok(Handled::Ignored("unknown_order".to_string()));
    };
    match mirror_echo(&state.pool, connection, order_id, echo, due.received_at).await? {
        Echo::Yes => return Ok(Handled::Ignored("pubky_refund_echo".to_string())),
        Echo::InFlight => return Ok(Handled::Retry("pubky_refund_in_flight".to_string())),
        Echo::No => {}
    }
    let order_currency: Option<String> =
        sqlx::query_scalar("SELECT currency FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await?;
    let amount = describe_refund(amounts, order_currency.as_deref());
    store::raise_alert(
        &state.pool,
        now,
        connection,
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

/// " (125.00 USD)" style text for the alert; says so when the store refunded
/// in a currency other than the order's, or in several.
fn describe_refund(amounts: &[RefundAmount], order_currency: Option<&str>) -> String {
    if amounts.is_empty() {
        return String::new();
    }
    let listed = amounts
        .iter()
        .map(|amount| format!("{} minor units {}", amount.amount_minor, amount.currency))
        .collect::<Vec<_>>()
        .join(", ");
    let foreign = order_currency.is_some_and(|order| {
        amounts
            .iter()
            .any(|amount| !amount.currency.eq_ignore_ascii_case(order))
    });
    if amounts.len() > 1 {
        format!(
            " ({listed}); the store refunded in more than one currency, so check the amounts before recording anything"
        )
    } else if foreign {
        format!(
            " ({listed}); the order is in {}, so the amount is not directly comparable",
            order_currency.unwrap_or_default()
        )
    } else {
        format!(" ({listed})")
    }
}

/// Turns a store inventory-item change into the variant and stock it means.
/// `Err` carries the final disposition when there is nothing to apply.
async fn resolve_inventory(
    state: &AppState,
    runtime: &ConnectorRuntime,
    connection: &ConnectionRow,
    inventory_item_id: &str,
) -> Result<Result<InboundEvent, Handled>, sqlx::Error> {
    let Some(provider) = connection
        .provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
    else {
        return Ok(Err(Handled::Ignored("provider_unavailable".to_string())));
    };
    let context = match runtime.open(&state.pool, connection).await {
        Ok(context) => context,
        Err(store::OpenError::Database(error)) => return Err(error),
        Err(_) => return Ok(Err(Handled::Retry("connection_unavailable".to_string()))),
    };
    let resolved = tokio::time::timeout(
        runtime.settings.provider_timeout,
        provider.resolve_inventory(&context, inventory_item_id),
    )
    .await;
    Ok(match resolved {
        Ok(Ok(Some(stock))) if stock.made_to_order => Ok(InboundEvent::MadeToOrder {
            provider_product_id: stock.provider_product_id,
            provider_variant_id: stock.provider_variant_id,
        }),
        Ok(Ok(Some(stock))) => match stock.available {
            Some(available) => Ok(InboundEvent::StockLevel {
                provider_product_id: stock.provider_product_id,
                provider_variant_id: stock.provider_variant_id,
                available,
            }),
            None => Err(Handled::Ignored("stock_not_tracked".to_string())),
        },
        Ok(Ok(None)) => Err(Handled::Ignored("not_a_variant".to_string())),
        Ok(Err(super::provider::ProviderError::Unauthorized)) => {
            Err(Handled::Ignored("credentials_rejected".to_string()))
        }
        Ok(Err(
            super::provider::ProviderError::Rejected { .. }
            | super::provider::ProviderError::Invalid(_),
        )) => Err(Handled::Ignored("provider_rejected".to_string())),
        Ok(Err(super::provider::ProviderError::RateLimited { .. })) => {
            Err(Handled::Retry("rate_limited".to_string()))
        }
        Ok(Err(super::provider::ProviderError::Unknown(_))) | Err(_) => {
            Err(Handled::Retry("provider_unavailable".to_string()))
        }
    })
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

/// Marks the Pubky order shipped, or adds or changes its tracking, from what
/// the store reports.
///
/// * A paid order is shipped. Without a tracking number (the store fulfilled
///   it with none) it is shipped with whatever carrier the store named, or
///   `Other`, when this service has optional tracking
///   ([`ConnectorRuntime::optional_tracking`]); a service without it ignores
///   such a fulfilment, as before.
/// * A shipped order whose tracking the store adds or changes gets
///   `fulfillment.update_tracking`. A second fulfilment (a later parcel) never
///   replaces tracking the order already has: only an update does.
/// * Anything else is ignored: the order is not in a shippable state.
#[allow(clippy::too_many_arguments)]
async fn apply_tracking(
    state: &AppState,
    runtime: &ConnectorRuntime,
    connection: &ConnectionRow,
    due: &DueEvent,
    order_id: Uuid,
    carrier: &str,
    tracking_number: &str,
    replace_existing: bool,
) -> Result<Handled, sqlx::Error> {
    let tracking: String = tracking_number.trim().chars().take(200).collect();
    let tracking = (!tracking.is_empty()).then_some(tracking);
    let carrier: String = {
        let trimmed: String = carrier.trim().chars().take(100).collect();
        if trimmed.is_empty() {
            "Other".to_string()
        } else {
            trimmed
        }
    };
    let order: Option<(i64, String, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT revision, state, shipment FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await?;
    let Some((revision, order_state, shipment)) = order else {
        return Ok(Handled::Ignored("order_missing".to_string()));
    };
    let (kind, payload, applied) = match order_state.as_str() {
        "paid" | "processing" => {
            if tracking.is_none() && !runtime.optional_tracking {
                return Ok(Handled::Ignored(
                    "tracking_commands_unavailable".to_string(),
                ));
            }
            let mut payload = json!({ "order_id": order_id, "carrier": carrier });
            if let Some(number) = &tracking {
                payload["tracking_number"] = json!(number);
            }
            ("fulfillment.ship", payload, "shipped")
        }
        "shipped" => {
            let Some(number) = tracking else {
                return Ok(Handled::Ignored("no_tracking_number".to_string()));
            };
            let existing = shipment
                .as_ref()
                .and_then(|shipment| shipment.get("tracking_number"))
                .and_then(serde_json::Value::as_str);
            let existing_carrier = shipment
                .as_ref()
                .and_then(|shipment| shipment.get("carrier"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if existing == Some(number.as_str()) && existing_carrier.eq_ignore_ascii_case(&carrier)
            {
                return Ok(Handled::Ignored("tracking_unchanged".to_string()));
            }
            if existing.is_some() && !replace_existing {
                return Ok(Handled::Ignored(
                    "already_shipped_with_tracking".to_string(),
                ));
            }
            if !runtime.optional_tracking {
                return Ok(Handled::Ignored(
                    "tracking_commands_unavailable".to_string(),
                ));
            }
            (
                "fulfillment.update_tracking",
                json!({ "order_id": order_id, "carrier": carrier, "tracking_number": number }),
                "tracking updated",
            )
        }
        _ => return Ok(Handled::Ignored("order_not_shippable".to_string())),
    };
    let raw = json!({
        "version": 1,
        "command_id": deterministic_uuid(&format!("connector-tracking|{}", due.id)),
        "aggregate_id": marketplace_domain::ids::order_aggregate_id(order_id),
        "expected_revision": revision,
        "issued_at": format_timestamp(state.clock.now()),
        "kind": kind,
        "payload": payload,
    });
    let (status, body) = crate::executor::execute(state, &connection.seller_pubky, &raw).await?;
    if status.is_success() {
        return Ok(Handled::Applied(applied.to_string()));
    }
    let code = body
        .pointer("/error/code")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let reason = body
        .pointer("/error/reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let outcome = match (code.as_str(), reason) {
        ("revision_conflict", _) => return Ok(Handled::Retry("revision_conflict".to_string())),
        ("invalid_state", "payment_not_final") => {
            return Ok(Handled::Retry("payment_not_final".to_string()))
        }
        ("", _) => return Ok(Handled::Retry(format!("status_{}", status.as_u16()))),
        ("invalid_state", "order_not_shipped") => "order_not_shipped".to_string(),
        ("invalid_state", "tracking_update_limit") => "tracking_update_limit".to_string(),
        ("invalid_state", _) => "order_not_shippable".to_string(),
        (other, _) => format!("refused_{other}"),
    };
    // Any other refusal is final, and it is not quiet: the store says the order
    // shipped (or its tracking changed) and the marketplace would not record it,
    // which the seller has to see.
    store::raise_alert(
        &state.pool,
        state.clock.now(),
        connection,
        AlertKind::InboundFailed,
        Some(order_id),
        &format!(
            "The store reports this order as shipped, but the marketplace refused to record \
             it ({code}). Mark the order shipped here yourself."
        ),
        &format!("inbound|{}", due.id),
    )
    .await?;
    Ok(Handled::Ignored(outcome))
}
