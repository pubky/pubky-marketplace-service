//! The order-push ledger: the state machine that makes at-least-once event
//! delivery produce at most one store order.
//!
//! `connector_order_push` has one row per `(seller, Pubky order, provider)`.
//! The states and their only transitions:
//!
//! ```text
//!   pending ──claim──► sending ──► created(external_id)
//!      ▲                  │  └────► quarantined(reason)
//!      └── rate limited ──┘
//!   sending ──lease lapses or outcome unknown──► sending (looked up first)
//!   quarantined ──seller retry──► sending (looked up first)
//! ```
//!
//! * `sending` is written, and committed, before the provider is called.
//! * A claim bumps `attempts`. Every write that ends a claim names the
//!   attempt number it holds and applies only while the row is still
//!   `sending` at that attempt, so a worker whose lease lapsed cannot
//!   overwrite the worker that took over.
//! * A claim of a row that was already `sending` (a crash, a timeout, an
//!   unknown outcome, or a seller retry) is flagged `lookup_first`: the
//!   worker asks the store whether the order exists before it pushes.
//! * `SELECT ... FOR UPDATE SKIP LOCKED` hands one row to one worker.
//! * Recording `created` also deletes the order's connector address
//!   envelope, in the same transaction, but only when the buyer shared it
//!   with this push's own store: one order can push to two stores, and the
//!   push to the other store must leave the envelope for the store that was
//!   chosen.
//! * A quarantined push takes its pending cancel and refund mirrors with it,
//!   in the same transaction (`settle_actions_of_quarantined_push`): they are
//!   set aside as `push_quarantined` and a seller retry of the push puts them
//!   back, or, when the order was cancelled before any push, dropped, because
//!   no store order can ever exist for them.
//!   The event handlers that queue a mirror lock the push rows they read
//!   (`FOR NO KEY UPDATE`), so each serialises with a quarantine or a retry of
//!   the same push: a mirror is never parked behind a push that has since
//!   moved on, and never left pending behind one that has been quarantined.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

use super::store::{raise_alert, truncate, AlertKind, ConnectionRow};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PushRow {
    pub id: Uuid,
    pub connection_id: Uuid,
    pub seller_pubky: String,
    pub pubky_order_id: Uuid,
    pub provider: String,
    pub state: String,
    pub external_id: Option<String>,
    pub quarantine_reason: Option<String>,
    pub attempts: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub source_event_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const PUSH_COLUMNS: &str = "id, connection_id, seller_pubky, pubky_order_id, provider, \
     state, external_id, quarantine_reason, attempts, next_attempt_at, lease_until, \
     last_error, source_event_id, created_at, updated_at";

fn prefixed(columns: &str, alias: &str) -> String {
    columns
        .split(',')
        .map(|column| format!("{alias}.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Clone)]
pub struct ClaimedPush {
    pub row: PushRow,
    /// The row was already `sending` when claimed: an earlier attempt may
    /// have reached the store, so look the order up before pushing.
    pub lookup_first: bool,
}

/// Quarantine reasons that are not faults: the push was withheld on
/// purpose. The Shop shows these as information, not as a to-do.
pub fn reason_is_informational(reason: &str) -> bool {
    matches!(reason, "order_cancelled_before_push" | "no_mapped_lines")
}

pub mod reason {
    pub const ORDER_CANCELLED_BEFORE_PUSH: &str = "order_cancelled_before_push";
    pub const ORDER_NOT_PAID: &str = "order_not_paid";
    pub const ORDER_NOT_FOUND: &str = "order_not_found";
    pub const UNMAPPED_VARIANT: &str = "unmapped_variant";
    /// No line of the order is mapped to this store: a Pubky-only sale. Not
    /// pushed, no alert.
    pub const NO_MAPPED_LINES: &str = "no_mapped_lines";
    pub const CONNECTION_UNAVAILABLE: &str = "connection_unavailable";
    pub const CREDENTIALS_REJECTED: &str = "credentials_rejected";
    pub const PROVIDER_REJECTED: &str = "provider_rejected";
    pub const MAX_ATTEMPTS_EXCEEDED: &str = "max_attempts_exceeded";
}

/// The `quarantine_reason` of a mirror action set aside because its push is
/// quarantined. A seller retry of the push puts these actions back.
pub const ACTION_PUSH_QUARANTINED: &str = "push_quarantined";

/// A push that a cancel or refund can still be mirrored onto: one that has
/// created, or may yet create, a store order. A push set aside because the
/// order was cancelled before it was sent is not: no store order exists and a
/// retry is withheld again. Expects the push aliased as `p`.
const MIRRORABLE_PUSH: &str = "(p.state IN ('pending', 'sending', 'created') \
     OR (p.state = 'quarantined' AND p.quarantine_reason <> 'order_cancelled_before_push'))";

/// Adds the ledger row for an order, once. Returns whether this call
/// created it; a duplicate delivery of the same event creates nothing.
pub async fn enqueue_push<'e, E>(
    executor: E,
    now: DateTime<Utc>,
    connection: &ConnectionRow,
    pubky_order_id: Uuid,
    source_event_id: Option<Uuid>,
) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let inserted = sqlx::query(
        "INSERT INTO connector_order_push \
         (id, connection_id, seller_pubky, pubky_order_id, provider, state, attempts, \
          next_attempt_at, source_event_id, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, 'pending', 0, $6, $7, $6, $6) \
         ON CONFLICT (seller_pubky, pubky_order_id, provider) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(connection.id)
    .bind(&connection.seller_pubky)
    .bind(pubky_order_id)
    .bind(&connection.provider)
    .bind(now)
    .bind(source_event_id)
    .execute(executor)
    .await?
    .rows_affected();
    Ok(inserted == 1)
}

/// Hands the next due push to the caller, as `sending`, in its own committed
/// statement. Only providers in `providers` are claimed.
pub async fn claim_next_push(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease: std::time::Duration,
    providers: &[String],
) -> Result<Option<ClaimedPush>, sqlx::Error> {
    let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::hours(1));
    let claimed: Option<(PushRow, String)> = sqlx::query(&format!(
        "WITH due AS ( \
             SELECT id, state AS prior_state FROM connector_order_push \
             WHERE provider = ANY($3) \
               AND ((state = 'pending' AND next_attempt_at <= $1) \
                    OR (state = 'sending' AND lease_until <= $1)) \
             ORDER BY COALESCE(lease_until, next_attempt_at), id \
             FOR UPDATE SKIP LOCKED LIMIT 1 \
         ) \
         UPDATE connector_order_push p SET state = 'sending', attempts = p.attempts + 1, \
             lease_until = $2, updated_at = $1 \
         FROM due WHERE p.id = due.id \
         RETURNING {}, due.prior_state",
        prefixed(PUSH_COLUMNS, "p")
    ))
    .bind(now)
    .bind(lease_until)
    .bind(providers)
    .try_map(|row: sqlx::postgres::PgRow| {
        use sqlx::FromRow;
        Ok((
            PushRow::from_row(&row)?,
            sqlx::Row::try_get(&row, "prior_state")?,
        ))
    })
    .fetch_optional(pool)
    .await?;
    Ok(claimed.map(|(row, prior)| ClaimedPush {
        lookup_first: prior == "sending",
        row,
    }))
}

/// The attempt succeeded: the store holds the order. The order's connector
/// address envelope is deleted in the same transaction when the buyer shared
/// it with this push's store.
pub async fn mark_created(
    pool: &PgPool,
    claim: &PushRow,
    external_id: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query(
        "UPDATE connector_order_push SET state = 'created', external_id = $3, lease_until = NULL, \
         last_error = NULL, updated_at = $4 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(claim.id)
    .bind(claim.attempts)
    .bind(external_id)
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed == 1 {
        delete_address_envelope(&mut *tx, claim.pubky_order_id, claim.connection_id).await?;
    }
    tx.commit().await?;
    Ok(changed == 1)
}

/// Clears the connector address envelope of an order, only when it was shared
/// with `connection_id`'s store: the order's share domain is the connection's
/// shop and the envelope is sealed to the connection's recipient key. A push
/// to any other store of the same order neither opens nor deletes it. The
/// consent columns stay: they are the record that the buyer opted in.
pub async fn delete_address_envelope<'e, E>(
    executor: E,
    pubky_order_id: Uuid,
    connection_id: Uuid,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    Ok(sqlx::query(
        "UPDATE orders SET connector_address_envelope = NULL, connector_key_id = NULL \
         WHERE id = $1 AND connector_address_envelope IS NOT NULL \
           AND EXISTS ( \
               SELECT 1 FROM connector_connections c \
               JOIN connector_recipient_keys k ON k.connection_id = c.id \
               WHERE c.id = $2 AND c.shop = orders.connector_share_domain \
                 AND k.key_id = orders.connector_key_id)",
    )
    .bind(pubky_order_id)
    .bind(connection_id)
    .execute(executor)
    .await?
    .rows_affected())
}

/// The push can never succeed as it stands. Sets the row aside with a
/// reason and raises the seller alert in the same transaction.
pub async fn quarantine_push(
    pool: &PgPool,
    claim: &PushRow,
    connection: &ConnectionRow,
    reason: &str,
    detail: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query(
        "UPDATE connector_order_push SET state = 'quarantined', quarantine_reason = $3, \
         lease_until = NULL, last_error = $4, updated_at = $5 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(claim.id)
    .bind(claim.attempts)
    .bind(reason)
    .bind(truncate(detail, 500))
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed == 1 {
        settle_actions_of_quarantined_push(&mut tx, claim.id, reason, now).await?;
    }
    if changed == 1 && !reason_is_informational(reason) {
        raise_alert(
            &mut *tx,
            now,
            connection,
            AlertKind::PushQuarantined,
            Some(claim.pubky_order_id),
            &format!("{reason}: {detail}"),
            &format!("push|{}|{reason}|{}", claim.id, claim.attempts),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(changed == 1)
}

/// Resolves the pending mirror actions of a push that was just quarantined,
/// in the quarantine's transaction. None of them can run: the worker claims an
/// action only once its push is `created`.
///
/// * The order was cancelled before any push: no store order exists or can,
///   so there is nothing to mirror and the actions are dropped.
/// * Any other reason: the actions are set aside as `push_quarantined`, which
///   the seller sees on the push next to its own quarantine, and `retry_push`
///   puts them back. A retry may find the order already in the store, and the
///   cancel or refund must then reach it.
async fn settle_actions_of_quarantined_push<'c>(
    tx: &mut sqlx::Transaction<'c, Postgres>,
    push_id: Uuid,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    if reason == self::reason::ORDER_CANCELLED_BEFORE_PUSH {
        drop_actions_of_pushes(tx, &[push_id]).await
    } else {
        sqlx::query(
            "UPDATE connector_order_actions SET state = 'quarantined', quarantine_reason = $2, \
             lease_until = NULL, updated_at = $3 \
             WHERE push_id = $1 AND state = 'pending'",
        )
        .bind(push_id)
        .bind(ACTION_PUSH_QUARANTINED)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

async fn drop_actions_of_pushes<'c>(
    tx: &mut sqlx::Transaction<'c, Postgres>,
    push_ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM connector_order_actions WHERE push_id = ANY($1) AND state = 'pending'",
    )
    .bind(push_ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The store did not apply the request (rate limited): try again later,
/// without a lookup.
pub async fn release_to_pending(
    pool: &PgPool,
    claim: &PushRow,
    retry_at: DateTime<Utc>,
    error: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE connector_order_push SET state = 'pending', lease_until = NULL, \
         next_attempt_at = $3, last_error = $4, updated_at = $5 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(claim.id)
    .bind(claim.attempts)
    .bind(retry_at)
    .bind(truncate(error, 500))
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

/// The outcome is unknown: leave the row `sending` until `retry_at`, when
/// the next claim looks the order up first.
pub async fn release_unknown(
    pool: &PgPool,
    claim: &PushRow,
    retry_at: DateTime<Utc>,
    error: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE connector_order_push SET lease_until = $3, last_error = $4, updated_at = $5 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(claim.id)
    .bind(claim.attempts)
    .bind(retry_at)
    .bind(truncate(error, 500))
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

#[derive(Debug, PartialEq, Eq)]
pub enum RetryOutcome {
    Queued(Box<PushView>),
    NotFound,
    /// Only a quarantined push can be retried.
    NotQuarantined(String),
}

/// A seller's retry of a quarantined push. The row goes back to `sending`
/// with an expired lease, so the next claim looks the order up before it
/// pushes: the quarantine may have followed an attempt that reached the
/// store.
pub async fn retry_push(
    pool: &PgPool,
    connection_id: Uuid,
    pubky_order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<RetryOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let queued: Option<PushRow> = sqlx::query_as(&format!(
        "UPDATE connector_order_push SET state = 'sending', quarantine_reason = NULL, \
         attempts = 0, lease_until = $3, next_attempt_at = $3, last_error = NULL, updated_at = $3 \
         WHERE connection_id = $1 AND pubky_order_id = $2 AND state = 'quarantined' \
         RETURNING {PUSH_COLUMNS}"
    ))
    .bind(connection_id)
    .bind(pubky_order_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = &queued {
        sqlx::query(
            "UPDATE connector_order_actions SET state = 'pending', quarantine_reason = NULL, \
             attempts = 0, next_attempt_at = $3, last_error = NULL, updated_at = $3 \
             WHERE push_id = $1 AND state = 'quarantined' AND quarantine_reason = $2",
        )
        .bind(row.id)
        .bind(ACTION_PUSH_QUARANTINED)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    if let Some(row) = queued {
        let view = PushView::from_row(pool, row).await?;
        return Ok(RetryOutcome::Queued(Box::new(view)));
    }
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM connector_order_push WHERE connection_id = $1 AND pubky_order_id = $2",
    )
    .bind(connection_id)
    .bind(pubky_order_id)
    .fetch_optional(pool)
    .await?;
    Ok(match state {
        Some(state) => RetryOutcome::NotQuarantined(state),
        None => RetryOutcome::NotFound,
    })
}

/// A new event cancelled the order. A push that has not started is set
/// aside, and any mirror already queued for it is dropped: no store order
/// exists. A created push gets a cancel action; so does a push in flight,
/// which runs once the push resolves to `created`, and a quarantined one,
/// which the cancel waits on until a seller retry (the retry may find the
/// order in the store).
pub async fn on_order_cancelled<'c>(
    tx: &mut sqlx::Transaction<'c, Postgres>,
    now: DateTime<Utc>,
    pubky_order_id: Uuid,
    reason: &str,
    source_event_id: Uuid,
) -> Result<(), sqlx::Error> {
    let set_aside: Vec<Uuid> = sqlx::query_scalar(
        "UPDATE connector_order_push SET state = 'quarantined', quarantine_reason = $2, \
         last_error = NULL, updated_at = $3 \
         WHERE pubky_order_id = $1 AND state = 'pending' RETURNING id",
    )
    .bind(pubky_order_id)
    .bind(self::reason::ORDER_CANCELLED_BEFORE_PUSH)
    .bind(now)
    .fetch_all(&mut **tx)
    .await?;
    drop_actions_of_pushes(tx, &set_aside).await?;
    sqlx::query(&format!(
        "INSERT INTO connector_order_actions \
         (id, push_id, kind, action_key, state, quarantine_reason, payload, attempts, \
          next_attempt_at, source_event_id, created_at, updated_at) \
         SELECT md5(p.id::text || '|cancel|cancel')::uuid, p.id, 'cancel', 'cancel', \
                CASE WHEN p.state = 'quarantined' THEN 'quarantined' ELSE 'pending' END, \
                CASE WHEN p.state = 'quarantined' THEN $5::text END, \
                $2, 0, $3, $4, $3, $3 \
         FROM connector_order_push p \
         WHERE p.pubky_order_id = $1 AND p.state IN ('created', 'sending', 'quarantined') \
           AND {MIRRORABLE_PUSH} \
         FOR NO KEY UPDATE OF p \
         ON CONFLICT (push_id, kind, action_key) DO NOTHING"
    ))
    .bind(pubky_order_id)
    .bind(serde_json::json!({ "reason": reason }))
    .bind(now)
    .bind(source_event_id)
    .bind(ACTION_PUSH_QUARANTINED)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Whether a refund recorded on the order has a push to be mirrored onto.
pub async fn has_mirrorable_push<'c>(
    tx: &mut sqlx::Transaction<'c, Postgres>,
    pubky_order_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM connector_order_push p \
         WHERE p.pubky_order_id = $1 AND {MIRRORABLE_PUSH})"
    ))
    .bind(pubky_order_id)
    .fetch_one(&mut **tx)
    .await
}

/// A refund was recorded on the Pubky order: mirror it, once per refund
/// event, onto every push that created or may create a store order: one
/// still `pending` or in flight queues it for after the creation, one
/// quarantined holds it set aside until a seller retry.
pub async fn on_refund_recorded<'c>(
    tx: &mut sqlx::Transaction<'c, Postgres>,
    now: DateTime<Utc>,
    pubky_order_id: Uuid,
    refund_key: &str,
    payload: &Value,
    source_event_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "INSERT INTO connector_order_actions \
         (id, push_id, kind, action_key, state, quarantine_reason, payload, attempts, \
          next_attempt_at, source_event_id, created_at, updated_at) \
         SELECT md5(p.id::text || '|refund|' || $2::text)::uuid, p.id, 'refund', $2, \
                CASE WHEN p.state = 'quarantined' THEN 'quarantined' ELSE 'pending' END, \
                CASE WHEN p.state = 'quarantined' THEN $6::text END, \
                $3, 0, $4, $5, $4, $4 \
         FROM connector_order_push p \
         WHERE p.pubky_order_id = $1 AND {MIRRORABLE_PUSH} \
         FOR NO KEY UPDATE OF p \
         ON CONFLICT (push_id, kind, action_key) DO NOTHING"
    ))
    .bind(pubky_order_id)
    .bind(refund_key)
    .bind(payload)
    .bind(now)
    .bind(source_event_id)
    .bind(ACTION_PUSH_QUARANTINED)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// === Order actions (cancel and refund mirrors) ===============================

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActionRow {
    pub id: Uuid,
    pub push_id: Uuid,
    pub kind: String,
    pub action_key: String,
    pub state: String,
    pub payload: Value,
    pub attempts: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub quarantine_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const ACTION_COLUMNS: &str = "id, push_id, kind, action_key, state, payload, attempts, \
     next_attempt_at, lease_until, last_error, quarantine_reason, created_at, updated_at";

/// The next due action whose push has created the store order.
pub async fn claim_next_action(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease: std::time::Duration,
    providers: &[String],
) -> Result<Option<(ActionRow, PushRow)>, sqlx::Error> {
    let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::hours(1));
    let action: Option<ActionRow> = sqlx::query_as(&format!(
        "WITH due AS ( \
             SELECT a.id FROM connector_order_actions a \
             JOIN connector_order_push p ON p.id = a.push_id \
             WHERE p.state = 'created' AND p.provider = ANY($3) \
               AND ((a.state = 'pending' AND a.next_attempt_at <= $1) \
                    OR (a.state = 'sending' AND a.lease_until <= $1)) \
             ORDER BY a.next_attempt_at, a.id \
             FOR UPDATE OF a SKIP LOCKED LIMIT 1 \
         ) \
         UPDATE connector_order_actions a SET state = 'sending', attempts = a.attempts + 1, \
             lease_until = $2, updated_at = $1 \
         FROM due WHERE a.id = due.id RETURNING {}",
        prefixed(ACTION_COLUMNS, "a")
    ))
    .bind(now)
    .bind(lease_until)
    .bind(providers)
    .fetch_optional(pool)
    .await?;
    let Some(action) = action else {
        return Ok(None);
    };
    let push: PushRow = sqlx::query_as(&format!(
        "SELECT {PUSH_COLUMNS} FROM connector_order_push WHERE id = $1"
    ))
    .bind(action.push_id)
    .fetch_one(pool)
    .await?;
    Ok(Some((action, push)))
}

pub async fn mark_action_done(
    pool: &PgPool,
    action: &ActionRow,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE connector_order_actions SET state = 'done', lease_until = NULL, last_error = NULL, \
         updated_at = $3 WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(action.id)
    .bind(action.attempts)
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

pub async fn quarantine_action(
    pool: &PgPool,
    action: &ActionRow,
    push: &PushRow,
    connection: &ConnectionRow,
    reason: &str,
    detail: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query(
        "UPDATE connector_order_actions SET state = 'quarantined', quarantine_reason = $3, \
         lease_until = NULL, last_error = $4, updated_at = $5 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(action.id)
    .bind(action.attempts)
    .bind(reason)
    .bind(truncate(detail, 500))
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed == 1 {
        raise_alert(
            &mut *tx,
            now,
            connection,
            AlertKind::ActionQuarantined,
            Some(push.pubky_order_id),
            &format!("{} {reason}: {detail}", action.kind),
            &format!("action|{}|{reason}", action.id),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(changed == 1)
}

pub async fn release_action(
    pool: &PgPool,
    action: &ActionRow,
    retry_at: DateTime<Utc>,
    error: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE connector_order_actions SET state = 'pending', lease_until = NULL, \
         next_attempt_at = $3, last_error = $4, updated_at = $5 \
         WHERE id = $1 AND state = 'sending' AND attempts = $2",
    )
    .bind(action.id)
    .bind(action.attempts)
    .bind(retry_at)
    .bind(truncate(error, 500))
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

// === Reads for the seller routes =============================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionView {
    pub kind: String,
    pub action_key: String,
    pub state: String,
    pub attempts: i32,
    pub quarantine_reason: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushView {
    pub id: Uuid,
    pub pubky_order_id: Uuid,
    pub provider: String,
    pub state: String,
    pub external_id: Option<String>,
    pub quarantine_reason: Option<String>,
    pub attempts: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub actions: Vec<ActionView>,
}

impl PushView {
    async fn from_row(pool: &PgPool, row: PushRow) -> Result<Self, sqlx::Error> {
        let actions = actions_for(pool, &[row.id])
            .await?
            .into_iter()
            .map(|(_, view)| view)
            .collect();
        Ok(Self::assemble(row, actions))
    }

    fn assemble(row: PushRow, actions: Vec<ActionView>) -> Self {
        let next_attempt_at = match row.state.as_str() {
            "pending" => Some(row.next_attempt_at),
            "sending" => row.lease_until,
            _ => None,
        };
        Self {
            id: row.id,
            pubky_order_id: row.pubky_order_id,
            provider: row.provider,
            state: row.state,
            external_id: row.external_id,
            quarantine_reason: row.quarantine_reason,
            attempts: row.attempts,
            next_attempt_at,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
            actions,
        }
    }
}

async fn actions_for(
    pool: &PgPool,
    push_ids: &[Uuid],
) -> Result<Vec<(Uuid, ActionView)>, sqlx::Error> {
    let rows: Vec<ActionRow> = sqlx::query_as(&format!(
        "SELECT {ACTION_COLUMNS} FROM connector_order_actions \
         WHERE push_id = ANY($1) ORDER BY created_at, id"
    ))
    .bind(push_ids)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.push_id,
                ActionView {
                    kind: row.kind,
                    action_key: row.action_key,
                    state: row.state,
                    attempts: row.attempts,
                    quarantine_reason: row.quarantine_reason,
                    last_error: row.last_error,
                    updated_at: row.updated_at,
                },
            )
        })
        .collect())
}

/// One page of a connection's ledger, newest first. The cursor is the
/// `(created_at, id)` of the last row of the previous page.
pub async fn list_pushes(
    pool: &PgPool,
    connection_id: Uuid,
    state: Option<&str>,
    after: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<(Vec<PushView>, bool), sqlx::Error> {
    let (after_at, after_id) = match after {
        Some((at, id)) => (Some(at), Some(id)),
        None => (None, None),
    };
    let mut rows: Vec<PushRow> = sqlx::query_as(&format!(
        "SELECT {PUSH_COLUMNS} FROM connector_order_push \
         WHERE connection_id = $1 AND ($2::text IS NULL OR state = $2) \
           AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4)) \
         ORDER BY created_at DESC, id DESC LIMIT $5"
    ))
    .bind(connection_id)
    .bind(state)
    .bind(after_at)
    .bind(after_id)
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;
    let more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let ids: Vec<Uuid> = rows.iter().map(|row| row.id).collect();
    let mut by_push: std::collections::HashMap<Uuid, Vec<ActionView>> =
        std::collections::HashMap::new();
    for (push_id, view) in actions_for(pool, &ids).await? {
        by_push.entry(push_id).or_default().push(view);
    }
    let views = rows
        .into_iter()
        .map(|row| {
            let own = by_push.remove(&row.id).unwrap_or_default();
            PushView::assemble(row, own)
        })
        .collect();
    Ok((views, more))
}

/// Counts per state for one connection, for the connection list.
pub async fn push_counts(
    pool: &PgPool,
    connection_id: Uuid,
) -> Result<std::collections::BTreeMap<String, i64>, sqlx::Error> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT state, COUNT(*) FROM connector_order_push WHERE connection_id = $1 GROUP BY state",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}
