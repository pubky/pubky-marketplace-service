//! Double-blind seller-to-buyer reviews (Pubky Market reviews, SV2).
//!
//! With `REVIEW_BLIND_WINDOW_DAYS` above 0, an order's first review stamps
//! `orders.review_window_ends_at`. Until `orders.reviews_released_at` is set
//! each side's review stays withheld from the other (`ReviewRow::view_for`),
//! and a reviewer's attestations are stored but not served. The reviews are
//! released once both roles have reviewed, or when the window ends: by the
//! `review.create` that completes the pair or arrives late, or by the
//! `review_release` worker. Every release is the same conditional update,
//! `WHERE reviews_released_at IS NULL`, so whichever path takes the order's
//! row lock first releases it and every other path finds it released: one
//! event and one set of notification intents however the paths interleave.
//!
//! An order with no window stamp is never withheld: orders reviewed while the
//! window was off, and those the 0087 migration released, read exactly as
//! they always did.

use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use sqlx::{PgExecutor, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::handlers::insert_notification_intent;
use crate::model::OrderRow;
use crate::workers::SYSTEM_ACTOR;

/// An order's double-blind state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewWindow {
    pub ends_at: Option<DateTime<Utc>>,
    pub released_at: Option<DateTime<Utc>>,
}

impl ReviewWindow {
    /// When the withheld reviews publish on their own, or `None` when
    /// nothing on the order is withheld.
    pub fn pending_until(&self) -> Option<DateTime<Utc>> {
        self.ends_at.filter(|_| self.released_at.is_none())
    }
}

pub async fn read_window<'e, E>(executor: E, order_id: Uuid) -> Result<ReviewWindow, sqlx::Error>
where
    E: PgExecutor<'e>,
{
    let (ends_at, released_at): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT review_window_ends_at, reviews_released_at FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(executor)
    .await?;
    Ok(ReviewWindow {
        ends_at,
        released_at,
    })
}

/// When the order became reviewable, which the window counts from: the
/// delivery confirmation (`fulfillment.delivered` for shipped and pickup
/// orders, `fulfillment.digital_delivered` or the order's
/// `digital_delivered_at` for digital ones, which an instant digital order
/// stamps at payment confirmation with no delivery event of its own), else
/// the first move to `completed` (`order.completed`; `closed` has no
/// transition into it), else `now`, the first review.
pub async fn eligible_since(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE( \
            (SELECT MIN(occurred_at) FROM events WHERE aggregate_id = $1 \
               AND kind IN ('fulfillment.delivered', 'fulfillment.digital_delivered')), \
            (SELECT digital_delivered_at FROM orders WHERE id = $2), \
            (SELECT MIN(occurred_at) FROM events WHERE aggregate_id = $1 \
               AND kind = 'order.completed'), \
            $3)",
    )
    .bind(ids::order_aggregate_id(order.id))
    .bind(order.id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
}

/// Stamps the window end on an order that has none. The caller holds the
/// order's row lock (it has just won the revision compare-and-swap).
pub async fn stamp_window(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    ends_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE orders SET review_window_ends_at = $2 \
         WHERE id = $1 AND review_window_ends_at IS NULL AND reviews_released_at IS NULL",
    )
    .bind(order_id)
    .bind(ends_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Marks reviews that were already public (written while the window was
/// off) as released, with no event: nothing was withheld and nobody waits.
pub async fn mark_public(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE orders SET reviews_released_at = $2 \
         WHERE id = $1 AND reviews_released_at IS NULL",
    )
    .bind(order_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Enqueues `kind` for `recipient` unless the order already has it.
pub async fn notify_once(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    kind: &str,
    order_id: Uuid,
    recipient: &str,
    actor: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let first = sqlx::query(
        "INSERT INTO review_notifications (order_id, kind, recipient_pubky, created_at) \
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
    )
    .bind(order_id)
    .bind(kind)
    .bind(recipient)
    .bind(now)
    .execute(&mut **tx)
    .await?
    .rows_affected()
        == 1;
    if first {
        insert_notification_intent(
            tx,
            event_id,
            kind,
            recipient,
            actor,
            &ids::order_aggregate_id(order_id),
            None,
            now,
        )
        .await?;
    }
    Ok(first)
}

/// What released an order's reviews.
#[derive(Debug, Clone, Copy)]
pub enum ReleasedBy<'a> {
    /// A `review.create` that completed the pair or arrived after the
    /// window. `created_event` is its `review.created` event, which carries
    /// the `review_received` intents: the notifications table keeps one row
    /// per (event, recipient), and a participant who reviewed hears both
    /// `review_released` and `review_received`.
    Create {
        actor: &'a str,
        command_id: Uuid,
        created_event: Uuid,
    },
    /// The `review_release` worker, once the window had ended.
    Worker,
}

/// The release a caller performed.
#[derive(Debug, Clone, Copy)]
pub struct Released {
    pub revision: i64,
    pub event_id: Uuid,
}

/// Releases the order's reviews: the conditional update, one
/// `review.released` event at the next revision and the notification
/// intents. `None` when the order was already released (or, for the
/// worker, is not yet due), which leaves everything untouched.
///
/// The update takes the order's row lock. A `review.create` already holding
/// it is waited for, so the worker never releases beneath it; a create that
/// loses the lock to the worker fails its revision compare-and-swap and
/// retries against the released order.
pub async fn release_reviews(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    by: ReleasedBy<'_>,
    now: DateTime<Utc>,
) -> Result<Option<Released>, sqlx::Error> {
    let due = match by {
        ReleasedBy::Create { .. } => "",
        ReleasedBy::Worker => " AND review_window_ends_at <= $2",
    };
    let revision: Option<i64> = sqlx::query_scalar(&format!(
        "UPDATE orders SET reviews_released_at = $2, revision = revision + 1, updated_at = $2 \
         WHERE id = $1 AND reviews_released_at IS NULL AND review_window_ends_at IS NOT NULL{due} \
         AND EXISTS (SELECT 1 FROM reviews WHERE order_id = $1) \
         RETURNING revision"
    ))
    .bind(order_id)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(revision) = revision else {
        return Ok(None);
    };

    let (actor, command_id) = match by {
        ReleasedBy::Create {
            actor, command_id, ..
        } => (actor, command_id),
        ReleasedBy::Worker => (SYSTEM_ACTOR, Uuid::new_v4()),
    };
    let event_id = insert_event(
        tx,
        command_id,
        &ids::order_aggregate_id(order_id),
        revision,
        actor,
        "review.released",
        now,
    )
    .await?;

    let reviewed: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT reviewer_pubky, subject_pubky FROM reviews \
         WHERE order_id = $1 ORDER BY reviewer_pubky",
    )
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut reviewers: Vec<&str> = Vec::new();
    let mut subjects: Vec<&str> = Vec::new();
    for (reviewer, subject) in &reviewed {
        if !reviewers.contains(&reviewer.as_str()) {
            reviewers.push(reviewer);
        }
        if !subjects.contains(&subject.as_str()) {
            subjects.push(subject);
        }
    }
    for reviewer in &reviewers {
        notify_once(
            tx,
            event_id,
            "review_released",
            order_id,
            reviewer,
            actor,
            now,
        )
        .await?;
    }
    let received_on = match by {
        ReleasedBy::Create { created_event, .. } => created_event,
        ReleasedBy::Worker => event_id,
    };
    for subject in &subjects {
        // On the release event itself, a participant who is also a reviewer
        // already has this event's row for `review_released`; the worker
        // releases only a lone reviewer's reviews, so this skips nobody it
        // would otherwise tell.
        if received_on == event_id && reviewers.contains(subject) {
            continue;
        }
        notify_once(
            tx,
            received_on,
            "review_received",
            order_id,
            subject,
            actor,
            now,
        )
        .await?;
    }
    Ok(Some(Released { revision, event_id }))
}

/// The `review_release` worker pass: releases every order whose window has
/// ended and whose reviews are still withheld. Each order is its own
/// transaction, so one failure never holds the rest, and a replayed pass
/// finds nothing due. Returns how many orders it released.
pub async fn release_due_orders(
    pool: &PgPool,
    now: DateTime<Utc>,
    batch_size: i64,
    max_batches: u32,
) -> anyhow::Result<u64> {
    let mut released = 0u64;
    for _ in 0..max_batches {
        let due: Vec<Uuid> = sqlx::query_scalar(
            "SELECT o.id FROM orders o \
             WHERE o.review_window_ends_at IS NOT NULL AND o.reviews_released_at IS NULL \
             AND o.review_window_ends_at <= $1 \
             AND EXISTS (SELECT 1 FROM reviews r WHERE r.order_id = o.id) \
             ORDER BY o.review_window_ends_at, o.id LIMIT $2",
        )
        .bind(now)
        .bind(batch_size)
        .fetch_all(pool)
        .await?;
        let mut batch_released = 0u64;
        for order_id in &due {
            let mut tx = pool.begin().await?;
            let outcome = release_reviews(&mut tx, *order_id, ReleasedBy::Worker, now).await?;
            tx.commit().await?;
            if outcome.is_some() {
                batch_released += 1;
            }
        }
        released += batch_released;
        if (due.len() as i64) < batch_size || batch_released == 0 {
            break;
        }
    }
    Ok(released)
}
