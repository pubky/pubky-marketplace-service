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
use super::refund;
use super::store::{self, ConnectionRow};
use crate::refund_destination::{is_usdt_payment, usdt_offered, BitcoinPayment, RefundUnit};

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
        "refund.reversal_cancelled" => {
            let Some(order_id) = order_id_of(&event.aggregate_id) else {
                return Ok(0);
            };
            on_refund_reversed(tx, now, event, order_id).await?;
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

/// The `orders` columns a recorded refund is mirrored against.
#[derive(sqlx::FromRow)]
struct RefundOrderRow {
    currency: String,
    exponent: i32,
    total_minor: i64,
    payment_method: Option<String>,
    payment_asset: Option<String>,
    payment_exponent: Option<i16>,
    usd_conversion_pair: Option<String>,
    usd_conversion_cents: Option<i64>,
    external_refund: Option<serde_json::Value>,
    payment_amount_minor: Option<i64>,
    paykit_observed_sats: Option<i64>,
    paykit_total_sats: Option<i64>,
    second_rail_paid: bool,
    paykit_accepted_assets: Option<Vec<String>>,
    paykit_payment_amounts: Option<serde_json::Value>,
    /// A refunded manual-review resolution wrote the refund: before refunds
    /// named their unit, the one writer that recorded sats.
    refunded_by_review: bool,
}

async fn on_refund_recorded(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    event: &EventRow,
    order_id: Uuid,
) -> Result<(), sqlx::Error> {
    mirror_current_refund(tx, now, order_id, event.id).await
}

/// Queues the mirror of the refund the order holds now, keyed on that record.
/// Idempotent: a refund already queued or mirrored under the same key is left
/// as it is.
async fn mirror_current_refund(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    order_id: Uuid,
    source_event_id: Uuid,
) -> Result<(), sqlx::Error> {
    if !ledger::has_mirrorable_push(tx, order_id).await? {
        return Ok(());
    }
    let row: Option<RefundOrderRow> = sqlx::query_as(
        "SELECT currency, exponent, total_minor, payment_method, payment_asset, \
                payment_exponent, usd_conversion_pair, usd_conversion_cents, external_refund, \
                payment_amount_minor, paykit_observed_sats, paykit_total_sats, \
                paykit_second_rail_at IS NOT NULL AS second_rail_paid, \
                paykit_accepted_assets, paykit_payment_amounts, \
                EXISTS (SELECT 1 FROM paykit_manual_resolutions r \
                        WHERE r.order_id = orders.id AND r.outcome = 'refunded') \
                    AS refunded_by_review \
         FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let received_sats = BitcoinPayment {
        payment_method: row.payment_method.as_deref(),
        payment_asset: row.payment_asset.as_deref(),
        payment_amount_minor: row.payment_amount_minor,
        paykit_observed_sats: row.paykit_observed_sats,
        paykit_total_sats: row.paykit_total_sats,
        second_rail_paid: row.second_rail_paid,
        usdt_offered: usdt_offered(
            row.paykit_accepted_assets.as_deref(),
            row.paykit_payment_amounts.as_ref(),
        ),
    }
    .received_sats();
    let usdt_paid = is_usdt_payment(row.payment_method.as_deref(), row.payment_asset.as_deref());
    let unit = row
        .external_refund
        .as_ref()
        .and_then(RefundUnit::of_record)
        .unwrap_or_else(|| RefundUnit::unnamed(usdt_paid, &row.currency, row.refunded_by_review));
    let basis = refund::OrderBasis {
        currency: row.currency,
        exponent: row.exponent,
        total_minor: row.total_minor,
        unit,
        payment_exponent: row.payment_exponent.map(i32::from),
        conversion: match (row.usd_conversion_pair, row.usd_conversion_cents) {
            (Some(pair), Some(cents)) if cents > 0 => Some(refund::UsdConversion { pair, cents }),
            _ => None,
        },
        received_sats,
        usdt_paid,
    };
    let external_refund = row.external_refund.as_ref();
    // The refund is read as the order holds it now. A refund reversed since
    // the event left none, and there is nothing to mirror.
    let Some(recorded) = external_refund
        .and_then(|refund| refund.get("amount_minor"))
        .and_then(serde_json::Value::as_i64)
    else {
        return Ok(());
    };
    let Some(key) = refund_key(tx, external_refund).await? else {
        return Ok(());
    };
    let transaction_id = external_refund
        .and_then(|refund| refund.get("transaction_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let mut payload = json!({
        "transaction_id": transaction_id,
        "order_total_minor": basis.total_minor,
        "order_currency": basis.currency,
        "order_exponent": basis.exponent,
    });
    match refund::order_currency_amount(&basis, recorded) {
        Ok(converted) => {
            payload["amount_minor"] = json!(converted.amount_minor);
            payload["full"] = json!(converted.amount_minor >= basis.total_minor);
            payload["basis"] = json!(converted.basis);
        }
        Err(blocked) => {
            payload["blocked_code"] = json!(blocked.code);
            payload["blocked_detail"] = json!(blocked.detail);
        }
    }
    ledger::on_refund_recorded(tx, now, order_id, &key, &payload, source_event_id).await
}

/// The identity of a recorded refund: a digest of the amount, transaction id
/// and `recorded_at` of the record the order holds, which every write of a
/// refund sets afresh. Each recorded refund mirrors once, with its own
/// amount, however many events refer to it and whenever they are consumed;
/// a refund recorded again after a reversal is another refund. `None` when
/// the order holds no refund.
async fn refund_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    external_refund: Option<&serde_json::Value>,
) -> Result<Option<String>, sqlx::Error> {
    let Some(refund) = external_refund else {
        return Ok(None);
    };
    let Some(amount) = refund
        .get("amount_minor")
        .and_then(serde_json::Value::as_i64)
    else {
        return Ok(None);
    };
    let text = |name: &str| {
        refund
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let identity = format!(
        "{amount}|{}|{}",
        text("transaction_id"),
        text("recorded_at")
    );
    sqlx::query_scalar("SELECT md5($1)::uuid::text")
        .bind(identity)
        .fetch_one(&mut **tx)
        .await
        .map(Some)
}

/// A reversal was cancelled: the order's refund is now what it holds, if
/// anything. Mirrors of earlier refunds are settled against it. When a refund
/// still stands under a record of its own and every earlier mirror was only
/// queued, it replaces them, so it is mirrored once. When an earlier mirror
/// was already sent the store holds that amount, and a second mirror would
/// add to it; the seller is alerted instead. A standing refund that was never
/// mirrored (only a full refund is) stays unmirrored.
async fn on_refund_reversed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    event: &EventRow,
    order_id: Uuid,
) -> Result<(), sqlx::Error> {
    let external_refund: Option<Option<serde_json::Value>> =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(external_refund) = external_refund else {
        return Ok(());
    };
    let current = refund_key(tx, external_refund.as_ref()).await?;
    let outcome = ledger::on_refund_reversed(tx, now, order_id, current.as_deref()).await?;
    if current.is_some() && outcome.dropped > 0 && outcome.flagged == 0 {
        mirror_current_refund(tx, now, order_id, event.id).await?;
    }
    Ok(())
}
