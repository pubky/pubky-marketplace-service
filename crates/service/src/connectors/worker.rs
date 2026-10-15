//! The connector worker's passes: consume events, drive pushes and order
//! actions through the provider, apply inbound webhooks, poll stock.
//!
//! Each pass is a plain async function over an [`AppState`] so tests drive
//! it one step at a time; [`run_forever`] is the loop the `connector-worker`
//! binary runs.
//!
//! Provider calls run under a timeout well inside the claim's lease
//! (`ConnectorSettings::validate` enforces `lease > 2 * timeout`), so a call
//! that outlives its lease cannot exist: by the time another worker may take
//! the row over, the first worker's request has been cancelled.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::AppState;

use super::inbound;
use super::ledger::{self, reason, ClaimedPush};
use super::order_push::{self, Withheld};
use super::outbox;
use super::provider::{
    CancelOrder, ConnectionContext, ConnectorProvider, ExternalOrder, ProviderError, RefundRecord,
};
use super::stock::{self, StockOutcome};
use super::store::{self, AlertKind, ConnectionRow, OpenError};
use super::{ConnectorRuntime, MAX_CATALOG_PAGES_PER_POLL};

const MAX_BATCHES_PER_PASS: usize = 20;
const MAX_ITEMS_PER_PASS: usize = 100;
const RESEAL_INTERVAL: Duration = Duration::from_secs(3600);

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    pub events_scanned: u64,
    pub pushes_queued: u64,
    pub pushes_handled: u64,
    pub actions_handled: u64,
    pub inbound_handled: u64,
    pub drafts_purged: u64,
    pub app_secrets_purged: u64,
}

fn runtime_of(state: &AppState) -> Option<&std::sync::Arc<ConnectorRuntime>> {
    state.connectors.as_ref()
}

fn provider_names(runtime: &ConnectorRuntime) -> Vec<String> {
    runtime
        .providers
        .kinds()
        .into_iter()
        .map(|kind| kind.as_str().to_string())
        .collect()
}

fn after(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    now + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::hours(1))
}

/// One full pass in dependency order: events first so a payment confirmed a
/// moment ago is pushed in the same pass. It ends by deleting drafts that
/// expired and app secrets whose retention ended.
pub async fn run_pass(state: &AppState) -> Result<PassReport, sqlx::Error> {
    let mut report = PassReport::default();
    if runtime_of(state).is_none() {
        return Ok(report);
    }
    let consumed = consume_events(state).await?;
    report.events_scanned = consumed.0;
    report.pushes_queued = consumed.1;
    report.pushes_handled = drive_pushes(state, MAX_ITEMS_PER_PASS).await?;
    report.actions_handled = drive_actions(state, MAX_ITEMS_PER_PASS).await?;
    report.inbound_handled = drive_inbound(state, MAX_ITEMS_PER_PASS).await?;
    let purged = store::purge_expired(&state.pool, state.clock.now()).await?;
    report.drafts_purged = purged.drafts;
    report.app_secrets_purged = purged.app_secrets;
    Ok(report)
}

/// Consumes events until the cursor catches up. Returns `(events scanned,
/// pushes queued)`.
pub async fn consume_events(state: &AppState) -> Result<(u64, u64), sqlx::Error> {
    let mut scanned = 0;
    let mut queued = 0;
    for _ in 0..MAX_BATCHES_PER_PASS {
        let batch = outbox::consume_batch(&state.pool, state.clock.now()).await?;
        scanned += batch.scanned;
        queued += batch.pushes_queued;
        if batch.scanned == 0 {
            break;
        }
    }
    Ok((scanned, queued))
}

pub async fn drive_pushes(state: &AppState, limit: usize) -> Result<u64, sqlx::Error> {
    let Some(runtime) = runtime_of(state) else {
        return Ok(0);
    };
    let providers = provider_names(runtime);
    let mut handled = 0;
    for _ in 0..limit {
        let Some(claim) = ledger::claim_next_push(
            &state.pool,
            state.clock.now(),
            runtime.settings.lease,
            &providers,
        )
        .await?
        else {
            break;
        };
        process_push(state, runtime, claim).await?;
        handled += 1;
    }
    Ok(handled)
}

pub async fn drive_actions(state: &AppState, limit: usize) -> Result<u64, sqlx::Error> {
    let Some(runtime) = runtime_of(state) else {
        return Ok(0);
    };
    let providers = provider_names(runtime);
    let mut handled = 0;
    for _ in 0..limit {
        let Some((action, push)) = ledger::claim_next_action(
            &state.pool,
            state.clock.now(),
            runtime.settings.lease,
            &providers,
        )
        .await?
        else {
            break;
        };
        process_action(state, runtime, action, push).await?;
        handled += 1;
    }
    Ok(handled)
}

pub async fn drive_inbound(state: &AppState, limit: usize) -> Result<u64, sqlx::Error> {
    let Some(runtime) = runtime_of(state) else {
        return Ok(0);
    };
    inbound::process_due(state, runtime, limit).await
}

async fn with_timeout<T>(
    timeout: Duration,
    call: impl Future<Output = Result<T, ProviderError>>,
) -> Result<T, ProviderError> {
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result,
        Err(_) => Err(ProviderError::Unknown("the provider call timed out".into())),
    }
}

/// A short machine value safe to store as a quarantine reason.
fn reason_from_code(code: &str) -> String {
    let cleaned: String = code
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if cleaned.is_empty() {
        reason::PROVIDER_REJECTED.to_string()
    } else {
        cleaned
    }
}

async fn open_for_call(
    state: &AppState,
    runtime: &ConnectorRuntime,
    connection: &ConnectionRow,
) -> Result<Result<ConnectionContext, (&'static str, String)>, sqlx::Error> {
    match runtime.open(&state.pool, connection).await {
        Ok(context) => Ok(Ok(context)),
        Err(OpenError::Unavailable) => Ok(Err((
            reason::CONNECTION_UNAVAILABLE,
            "The store is not connected.".to_string(),
        ))),
        Err(OpenError::Unreadable) => {
            tracing::error!(
                connection_id = %connection.id,
                "connector credential does not open under the configured keys"
            );
            Ok(Err((
                reason::CONNECTION_UNAVAILABLE,
                "The stored credential cannot be read.".to_string(),
            )))
        }
        Err(OpenError::Database(error)) => Err(error),
    }
}

async fn process_push(
    state: &AppState,
    runtime: &ConnectorRuntime,
    claim: ClaimedPush,
) -> Result<(), sqlx::Error> {
    let pool = &state.pool;
    let row = &claim.row;
    let now = state.clock.now();
    let Some(connection) = store::get(pool, row.connection_id).await? else {
        return Ok(());
    };
    let Some(provider) = connection
        .provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
    else {
        ledger::release_to_pending(pool, row, after(now, runtime.settings.tick), "", now).await?;
        return Ok(());
    };
    if row.attempts > runtime.settings.max_attempts {
        ledger::quarantine_push(
            pool,
            row,
            &connection,
            reason::MAX_ATTEMPTS_EXCEEDED,
            "The store did not answer after repeated attempts.",
            now,
        )
        .await?;
        return Ok(());
    }
    let context = match open_for_call(state, runtime, &connection).await? {
        Ok(context) => context,
        Err((why, detail)) => {
            ledger::quarantine_push(pool, row, &connection, why, &detail, now).await?;
            return Ok(());
        }
    };
    if claim.lookup_first {
        let found = with_timeout(
            runtime.settings.provider_timeout,
            provider.find_pushed_order(&context, row.pubky_order_id),
        )
        .await;
        match found {
            Ok(Some(existing)) => {
                ledger::mark_created(pool, row, &existing.id, state.clock.now()).await?;
                return Ok(());
            }
            Ok(None) => {}
            Err(error) => {
                return push_failed(state, runtime, &connection, &claim, error, true).await;
            }
        }
    }
    let Some(order) = order_push::load_order(pool, row.pubky_order_id).await? else {
        ledger::quarantine_push(
            pool,
            row,
            &connection,
            reason::ORDER_NOT_FOUND,
            "The Pubky order no longer exists.",
            now,
        )
        .await?;
        return Ok(());
    };
    let paid_at = order_push::paid_at(pool, order.id)
        .await?
        .unwrap_or(order.created_at);
    let push =
        match order_push::build_push(pool, &runtime.keys, &connection, &order, paid_at).await? {
            Ok(push) => push,
            Err(Withheld { reason, detail }) => {
                ledger::quarantine_push(pool, row, &connection, reason, &detail, now).await?;
                return Ok(());
            }
        };
    match with_timeout(
        runtime.settings.provider_timeout,
        provider.push_order(&context, &push),
    )
    .await
    {
        Ok(created) => {
            if !ledger::mark_created(pool, row, &created.id, state.clock.now()).await? {
                tracing::warn!(
                    push_id = %row.id,
                    "push finished after its claim was taken over; the new owner reconciles it"
                );
            }
            Ok(())
        }
        Err(error) => push_failed(state, runtime, &connection, &claim, error, false).await,
    }
}

/// `during_lookup` is true when the failing call was the lookup of an order
/// an earlier attempt may have created: a throttled lookup must not turn the
/// row back into a plain `pending` one, or the next claim would push without
/// looking first.
async fn push_failed(
    state: &AppState,
    runtime: &ConnectorRuntime,
    connection: &ConnectionRow,
    claim: &ClaimedPush,
    error: ProviderError,
    during_lookup: bool,
) -> Result<(), sqlx::Error> {
    let pool = &state.pool;
    let row = &claim.row;
    let now = state.clock.now();
    let backoff = runtime.settings.backoff(row.attempts);
    match error {
        ProviderError::Rejected { code, message } => {
            ledger::quarantine_push(
                pool,
                row,
                connection,
                &reason_from_code(&code),
                &message,
                now,
            )
            .await?;
        }
        ProviderError::Invalid(message) => {
            ledger::quarantine_push(
                pool,
                row,
                connection,
                reason::PROVIDER_REJECTED,
                &message,
                now,
            )
            .await?;
        }
        ProviderError::Unauthorized => {
            credentials_rejected(pool, connection, now).await?;
            ledger::quarantine_push(
                pool,
                row,
                connection,
                reason::CREDENTIALS_REJECTED,
                "The store no longer accepts the connection.",
                now,
            )
            .await?;
        }
        ProviderError::RateLimited { retry_after } => {
            let delay = retry_after.map_or(backoff, |wait| wait.max(backoff));
            if during_lookup {
                ledger::release_unknown(
                    pool,
                    row,
                    after(now, delay),
                    "The store is rate limiting.",
                    now,
                )
                .await?;
            } else {
                ledger::release_to_pending(
                    pool,
                    row,
                    after(now, delay),
                    "The store is rate limiting.",
                    now,
                )
                .await?;
            }
        }
        ProviderError::Unknown(message) => {
            if row.attempts >= runtime.settings.max_attempts {
                ledger::quarantine_push(
                    pool,
                    row,
                    connection,
                    reason::MAX_ATTEMPTS_EXCEEDED,
                    &message,
                    now,
                )
                .await?;
            } else {
                ledger::release_unknown(pool, row, after(now, backoff), &message, now).await?;
            }
        }
    }
    Ok(())
}

async fn credentials_rejected(
    pool: &PgPool,
    connection: &ConnectionRow,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    store::mark_error(pool, connection.id, "credentials_rejected", now).await?;
    store::raise_alert(
        pool,
        now,
        connection,
        AlertKind::CredentialsRejected,
        None,
        "The store no longer accepts the connection. Connect the store again.",
        &format!("credentials|{}", connection.id),
    )
    .await
}

async fn process_action(
    state: &AppState,
    runtime: &ConnectorRuntime,
    action: ledger::ActionRow,
    push: ledger::PushRow,
) -> Result<(), sqlx::Error> {
    let pool = &state.pool;
    let now = state.clock.now();
    let Some(connection) = store::get(pool, push.connection_id).await? else {
        return Ok(());
    };
    let Some(provider) = connection
        .provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
    else {
        return Ok(());
    };
    let Some(external_id) = push.external_id.clone() else {
        ledger::release_action(pool, &action, after(now, runtime.settings.tick), "", now).await?;
        return Ok(());
    };
    let context = match open_for_call(state, runtime, &connection).await? {
        Ok(context) => context,
        Err((why, detail)) => {
            ledger::quarantine_action(pool, &action, &push, &connection, why, &detail, now).await?;
            return Ok(());
        }
    };
    let order = ExternalOrder { id: external_id };
    let result = run_action(runtime, provider.as_ref(), &context, &order, &action, &push).await;
    match result {
        Ok(()) => {
            ledger::mark_action_done(pool, &action, state.clock.now()).await?;
        }
        Err(ProviderError::Rejected { code, message }) => {
            ledger::quarantine_action(
                pool,
                &action,
                &push,
                &connection,
                &reason_from_code(&code),
                &message,
                now,
            )
            .await?;
        }
        Err(ProviderError::Invalid(message)) => {
            ledger::quarantine_action(
                pool,
                &action,
                &push,
                &connection,
                reason::PROVIDER_REJECTED,
                &message,
                now,
            )
            .await?;
        }
        Err(ProviderError::Unauthorized) => {
            credentials_rejected(pool, &connection, now).await?;
            ledger::quarantine_action(
                pool,
                &action,
                &push,
                &connection,
                reason::CREDENTIALS_REJECTED,
                "The store no longer accepts the connection.",
                now,
            )
            .await?;
        }
        Err(ProviderError::RateLimited { retry_after }) => {
            let backoff = runtime.settings.backoff(action.attempts);
            let delay = retry_after.map_or(backoff, |wait| wait.max(backoff));
            ledger::release_action(
                pool,
                &action,
                after(now, delay),
                "The store is rate limiting.",
                now,
            )
            .await?;
        }
        Err(ProviderError::Unknown(message)) => {
            if action.attempts >= runtime.settings.max_attempts {
                ledger::quarantine_action(
                    pool,
                    &action,
                    &push,
                    &connection,
                    reason::MAX_ATTEMPTS_EXCEEDED,
                    &message,
                    now,
                )
                .await?;
            } else {
                let backoff = runtime.settings.backoff(action.attempts);
                ledger::release_action(pool, &action, after(now, backoff), &message, now).await?;
            }
        }
    }
    Ok(())
}

async fn run_action(
    runtime: &ConnectorRuntime,
    provider: &dyn ConnectorProvider,
    context: &ConnectionContext,
    order: &ExternalOrder,
    action: &ledger::ActionRow,
    push: &ledger::PushRow,
) -> Result<(), ProviderError> {
    let timeout = runtime.settings.provider_timeout;
    match action.kind.as_str() {
        "cancel" => {
            let reason = action
                .payload
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("The order was cancelled in Pubky.")
                .to_string();
            let cancel = CancelOrder {
                pubky_order_id: push.pubky_order_id,
                reason,
            };
            with_timeout(timeout, provider.cancel_order(context, order, &cancel)).await
        }
        "refund" => {
            let payload = &action.payload;
            let text = |name: &str| {
                payload
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            let refund = RefundRecord {
                refund_id: action.action_key.clone(),
                pubky_order_id: push.pubky_order_id,
                amount_minor: payload
                    .get("amount_minor")
                    .and_then(serde_json::Value::as_i64),
                amount_unit: text("amount_unit"),
                transaction_id: payload
                    .get("transaction_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                order_total_minor: payload
                    .get("order_total_minor")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or_default(),
                order_currency: text("order_currency"),
                order_exponent: payload
                    .get("order_exponent")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(2) as i32,
                full: payload
                    .get("full")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                quote: payload
                    .get("quote")
                    .and_then(|quote| serde_json::from_value(quote.clone()).ok()),
            };
            with_timeout(timeout, provider.record_refund(context, order, &refund)).await
        }
        other => Err(ProviderError::Invalid(format!(
            "unknown action kind {other}"
        ))),
    }
}

/// Reads each mapped connection's catalog and applies the stock it reports.
/// The webhook path is the fast one; this closes the gaps a missed webhook
/// leaves. Returns how many listings changed.
pub async fn poll_stock(state: &AppState) -> Result<u64, sqlx::Error> {
    let Some(runtime) = runtime_of(state) else {
        return Ok(0);
    };
    let mut applied = 0;
    for connection in store::connected_with_mappings(&state.pool).await? {
        let Some(provider) = connection
            .provider_kind()
            .and_then(|kind| runtime.providers.get(kind))
        else {
            continue;
        };
        let context = match open_for_call(state, runtime, &connection).await? {
            Ok(context) => context,
            Err(_) => continue,
        };
        let mut page: Option<String> = None;
        for _ in 0..MAX_CATALOG_PAGES_PER_POLL {
            let catalog = match with_timeout(
                runtime.settings.provider_timeout,
                provider.list_catalog(&context, page.as_deref()),
            )
            .await
            {
                Ok(catalog) => catalog,
                Err(ProviderError::Unauthorized) => {
                    credentials_rejected(&state.pool, &connection, state.clock.now()).await?;
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        connection_id = %connection.id,
                        error = %error,
                        "stock poll could not read the catalog"
                    );
                    break;
                }
            };
            for product in &catalog.products {
                for variant in &product.variants {
                    let Some(level) = variant.stock else {
                        continue;
                    };
                    let Some(mapping) = store::mapping_for_provider_variant(
                        &state.pool,
                        connection.id,
                        &product.provider_product_id,
                        &variant.provider_variant_id,
                    )
                    .await?
                    else {
                        continue;
                    };
                    if let StockOutcome::Applied { .. } =
                        stock::apply_stock_level(state, &connection, &mapping, level, None).await?
                    {
                        applied += 1;
                    }
                }
            }
            page = catalog.next_page;
            if page.is_none() {
                break;
            }
        }
    }
    Ok(applied)
}

/// The `connector-worker` loop: a pass every tick, a stock poll every poll
/// interval, a credential re-seal while a previous key is configured, until
/// the process is told to stop.
pub async fn run_forever(state: AppState) -> anyhow::Result<()> {
    let Some(runtime) = runtime_of(&state).cloned() else {
        anyhow::bail!("connectors are not enabled");
    };
    let mut tick = tokio::time::interval(runtime.settings.tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_poll = tokio::time::Instant::now();
    let mut last_reseal: Option<tokio::time::Instant> = None;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("connector worker stopping");
                return Ok(());
            }
            _ = tick.tick() => {
                if let Err(error) = run_pass(&state).await {
                    tracing::error!(error = %error, "connector pass failed");
                }
                if runtime.keys.has_previous()
                    && last_reseal.is_none_or(|at| at.elapsed() >= RESEAL_INTERVAL)
                {
                    last_reseal = Some(tokio::time::Instant::now());
                    match super::reseal_previous_key(&state.pool, &runtime.keys, state.clock.now()).await {
                        Ok(progress) => tracing::info!(
                            resealed = progress.resealed,
                            remaining_under_previous = progress.remaining_under_previous,
                            "connector credential re-seal pass finished"
                        ),
                        Err(error) => tracing::error!(error = %error, "connector credential re-seal failed"),
                    }
                }
                if last_poll.elapsed() >= runtime.settings.stock_poll_interval {
                    last_poll = tokio::time::Instant::now();
                    if let Err(error) = poll_stock(&state).await {
                        tracing::error!(error = %error, "connector stock poll failed");
                    }
                }
            }
        }
    }
}
