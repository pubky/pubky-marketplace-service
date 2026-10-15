//! The connector worker's input: the service's append-only `events` feed,
//! read with a durable cursor.
//!
//! This is the same feed the seller event API and the seller webhooks are
//! built on, read in process, so the worker needs no seller bearer and no
//! HTTP hop. It does not consume the notification `outbox` table: that table
//! is claimed and marked delivered by the notification worker, and a second
//! consumer there would steal or double-deliver rows. The cursor lives in
//! `connector_event_cursor`.
//!
//! Delivery is at least once. A crash after the ledger writes but before the
//! cursor commits replays the batch, and every effect of an event is
//! idempotent: the ledger insert is `ON CONFLICT DO NOTHING`, the cancel and
//! refund actions are unique per `(push, kind, key)`.
//!
//! `events.sequence` is an identity column, so a transaction that took
//! sequence 5 can commit after one that took 6. A reader that moved its
//! cursor to 6 would never see 5. The pass therefore reads only rows whose
//! writing transaction is older than the oldest transaction still in
//! flight (`xmin` against the snapshot's `xmin`, compared with `age()` so it
//! survives transaction-id wraparound), and stops at the first row that is
//! not yet settled. Everything before it is final, in sequence order.

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::ledger;
use super::store::{self, ConnectionRow};

pub const CURSOR_NAME: &str = "events";
const BATCH: i64 = 200;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConsumeReport {
    pub scanned: u64,
    pub pushes_queued: u64,
    pub cursor: i64,
    /// Another consumer holds the cursor; nothing was read.
    pub busy: bool,
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: Uuid,
    sequence: i64,
    aggregate_id: String,
    kind: String,
    occurred_at: DateTime<Utc>,
    settled: bool,
}

/// One pass: reads the next batch of settled events and applies them.
/// Returns after one batch; the caller loops while `scanned > 0`.
pub async fn consume_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> Result<ConsumeReport, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let cursor: Option<i64> = sqlx::query_scalar(
        "SELECT sequence FROM connector_event_cursor WHERE name = $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(CURSOR_NAME)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(cursor) = cursor else {
        tx.rollback().await?;
        return Ok(ConsumeReport {
            busy: true,
            ..ConsumeReport::default()
        });
    };
    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT e.id, e.sequence, e.aggregate_id, e.kind, e.occurred_at, \
                age(e.xmin) > age(pg_snapshot_xmin(pg_current_snapshot())::xid) AS settled \
         FROM events e WHERE e.sequence > $1 ORDER BY e.sequence LIMIT $2",
    )
    .bind(cursor)
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;
    let mut report = ConsumeReport {
        cursor,
        ..ConsumeReport::default()
    };
    for event in events {
        if !event.settled {
            break;
        }
        report.pushes_queued += apply(&mut tx, now, &event).await?;
        report.scanned += 1;
        report.cursor = event.sequence;
    }
    if report.scanned > 0 {
        sqlx::query(
            "UPDATE connector_event_cursor SET sequence = $2, updated_at = $3 WHERE name = $1",
        )
        .bind(CURSOR_NAME)
        .bind(report.cursor)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(report)
}

async fn apply(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    event: &EventRow,
) -> Result<u64, sqlx::Error> {
    match event.kind.as_str() {
        "payment.confirmed" => on_payment_confirmed(tx, now, event).await,
        "order.cancelled" | "order.cancelled_terms_change" => {
            let Some(order_id) = order_id_of(&event.aggregate_id) else {
                return Ok(0);
            };
            ledger::on_order_cancelled(
                tx,
                now,
                order_id,
                "The order was cancelled in Pubky.",
                event.id,
            )
            .await?;
            Ok(0)
        }
        "refund.recorded_external" => {
            let Some(order_id) = order_id_of(&event.aggregate_id) else {
                return Ok(0);
            };
            on_refund_recorded(tx, now, event, order_id).await?;
            Ok(0)
        }
        _ => Ok(0),
    }
}

fn order_listing_ids(seller: &str, lines: &serde_json::Value) -> Vec<String> {
    lines
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|line| {
            line.get("listing_aggregate_id")
                .and_then(serde_json::Value::as_str)
        })
        .filter_map(|aggregate| super::order_push::pubky_listing_id(seller, aggregate))
        .collect()
}

fn order_id_of(aggregate_id: &str) -> Option<Uuid> {
    aggregate_id
        .strip_prefix("order:")
        .and_then(|id| Uuid::parse_str(id).ok())
}

async fn on_payment_confirmed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    event: &EventRow,
) -> Result<u64, sqlx::Error> {
    let order: Option<(Uuid, String, serde_json::Value)> = sqlx::query_as(
        "SELECT o.id, o.seller_pubky, o.lines FROM payments p JOIN orders o ON o.id = p.order_id \
         WHERE 'payment:' || p.id::text = $1",
    )
    .bind(&event.aggregate_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((order_id, seller, lines)) = order else {
        return Ok(0);
    };
    let listing_ids = order_listing_ids(&seller, &lines);
    // Only connections that were live when the payment confirmed: an order
    // paid before the seller connected is never pushed.
    let connections: Vec<ConnectionRow> = sqlx::query_as(&format!(
        "SELECT {} FROM connector_connections \
         WHERE seller_pubky = $1 AND status IN ('connected', 'needs_attention') \
           AND connected_at <= $2",
        store::CONNECTION_COLUMNS
    ))
    .bind(&seller)
    .bind(event.occurred_at)
    .fetch_all(&mut **tx)
    .await?;
    let mut queued = 0;
    for connection in &connections {
        // An order none of whose lines is mapped in this store is a Pubky-only
        // sale: no ledger row, no alert. One with only some lines mapped is
        // queued and quarantined at push time with an alert.
        let mapped: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM connector_variant_map \
             WHERE connection_id = $1 AND listing_id = ANY($2))",
        )
        .bind(connection.id)
        .bind(&listing_ids)
        .fetch_one(&mut **tx)
        .await?;
        if !mapped {
            continue;
        }
        if ledger::enqueue_push(&mut **tx, now, connection, order_id, Some(event.id)).await? {
            queued += 1;
        }
    }
    Ok(queued)
}

/// `state, external_refund, total_minor, currency, exponent, payment_method`.
type RefundOrderRow = (
    String,
    Option<serde_json::Value>,
    i64,
    String,
    i32,
    Option<String>,
);

/// What the order records about how it was paid, as plain columns plus the
/// optional USD-conversion columns that only exist once the multi-currency
/// migration has run (read through `to_jsonb`, so this file compiles and runs
/// with or without them).
#[derive(sqlx::FromRow)]
struct PaymentBasisRow {
    bitcoin_quote_rate: Option<String>,
    bitcoin_quote_source: Option<String>,
    bitcoin_quote_fetched_at: Option<DateTime<Utc>>,
    bitcoin_quote_currency: Option<String>,
    payment_asset: Option<String>,
    payment_quote_basis: Option<String>,
    usd_conversion_pair: Option<String>,
    usd_conversion_source: Option<String>,
    usd_conversion_usd_rate: Option<String>,
    usd_conversion_currency_rate: Option<String>,
    usd_conversion_fetched_at: Option<String>,
}

const BASIS_MAX_CHARS: usize = 500;

/// The refund note's statement of how the recorded amount relates to the
/// payment. `refund.record_external` validates the amount against the order
/// total, so it is in the order currency on every rail and nothing is
/// converted; the note says so and names the order's own recorded quote
/// (Bitcoin rate, USDT basis, USD conversion) so the merchant can reconcile
/// what the buyer actually sent.
async fn refund_basis(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    payment_method: Option<&str>,
    currency: &str,
) -> Result<String, sqlx::Error> {
    let row: Option<PaymentBasisRow> = sqlx::query_as(
        "SELECT bitcoin_quote_rate::text AS bitcoin_quote_rate, bitcoin_quote_source, \
                bitcoin_quote_fetched_at, bitcoin_quote_currency, payment_asset, \
                payment_quote_basis, \
                to_jsonb(o)->>'usd_conversion_pair' AS usd_conversion_pair, \
                to_jsonb(o)->>'usd_conversion_source' AS usd_conversion_source, \
                to_jsonb(o)->>'usd_conversion_usd_rate' AS usd_conversion_usd_rate, \
                to_jsonb(o)->>'usd_conversion_currency_rate' AS usd_conversion_currency_rate, \
                to_jsonb(o)->>'usd_conversion_fetched_at' AS usd_conversion_fetched_at \
         FROM orders o WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?;
    let mut parts = vec![format!(
        "Amount recorded in {currency}, the order currency; no currency conversion."
    )];
    if let Some(row) = row {
        match payment_method {
            Some("bitcoin" | "paykit") => parts.push("Paid with Bitcoin.".to_string()),
            Some("usdt") => parts.push("Paid with USDT.".to_string()),
            _ => {}
        }
        if let (Some(rate), Some(quote_currency)) =
            (&row.bitcoin_quote_rate, &row.bitcoin_quote_currency)
        {
            parts.push(format!(
                "Order quote: BTC{} {rate} from {} at {}.",
                quote_currency.trim_end(),
                row.bitcoin_quote_source
                    .as_deref()
                    .unwrap_or("unknown source"),
                row.bitcoin_quote_fetched_at
                    .map(crate::clock::format_timestamp)
                    .unwrap_or_else(|| "unknown time".to_string()),
            ));
        }
        if let (Some(asset), Some(basis)) = (&row.payment_asset, &row.payment_quote_basis) {
            parts.push(format!("{asset} amount basis: {basis}."));
        }
        if let (Some(pair), Some(source), Some(usd_rate), Some(currency_rate), Some(at)) = (
            &row.usd_conversion_pair,
            &row.usd_conversion_source,
            &row.usd_conversion_usd_rate,
            &row.usd_conversion_currency_rate,
            &row.usd_conversion_fetched_at,
        ) {
            parts.push(format!(
                "Price converted to USD for the payment request: {pair} (USD rate {usd_rate}, {currency} rate {currency_rate}) from {source} at {at}."
            ));
        }
    }
    Ok(parts.join(" ").chars().take(BASIS_MAX_CHARS).collect())
}

async fn on_refund_recorded(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    event: &EventRow,
    order_id: Uuid,
) -> Result<(), sqlx::Error> {
    let has_push = ledger::has_mirrorable_push(tx, order_id).await?;
    if !has_push {
        return Ok(());
    }
    let order: Option<RefundOrderRow> = sqlx::query_as(
        "SELECT state, external_refund, total_minor, currency, exponent, payment_method \
             FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((_state, external_refund, total_minor, currency, exponent, payment_method)) = order
    else {
        return Ok(());
    };
    // The payload is read at consume time. A refund that was reversed before
    // this event was consumed leaves no record, and there is nothing to mirror.
    let Some(recorded) = external_refund
        .as_ref()
        .and_then(|refund| refund.get("amount_minor"))
        .and_then(serde_json::Value::as_i64)
    else {
        return Ok(());
    };
    let transaction_id = external_refund
        .as_ref()
        .and_then(|refund| refund.get("transaction_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let basis = refund_basis(tx, order_id, payment_method.as_deref(), &currency).await?;
    let payload = json!({
        "amount_minor": recorded,
        "amount_unit": currency.to_lowercase(),
        "transaction_id": transaction_id,
        "order_total_minor": total_minor,
        "order_currency": currency,
        "order_exponent": exponent,
        "full": recorded >= total_minor,
        "basis": basis,
    });
    ledger::on_refund_recorded(tx, now, order_id, &event.id.to_string(), &payload, event.id).await
}
