//! Background worker runtime (plan tasks 3.4 and 4.5).
//!
//! One runtime drains six server-time tasks: (a) reservation expiry,
//! (b) offer expiry, (c) auction close, (d) the outbox, (e) Locks lifecycle
//! verification, and (f) the marketplace payment window. Each task is
//! guarded by a lease row in `worker_leases`, so two service instances never
//! drain the same task concurrently; a holder that dies mid-lease is
//! recovered by any instance once the lease lapses. Within a task, due rows
//! are additionally locked with `FOR UPDATE SKIP LOCKED`, so even a lease
//! violation cannot double-process a row.
//!
//! Outbox semantics: intents are written in the command transaction
//! (ADR-0019 §4) and delivered at least once. A claim stamps `lease_until`
//! on the row; delivery inserts the notification and marks `delivered_at` in
//! one transaction. A crash between claim and delivery leaves the row leased
//! but undelivered — it is redelivered after the lease lapses. The consumer
//! side dedups by (event id, recipient), so redelivery never duplicates the
//! effect.
//!
//! Locks verification semantics (ADR-0019 §7): the service independently
//! verifies each pending correlation against the Lock Server's lifecycle
//! lookup and advances the payment on a completed result exactly once — the
//! payment-state compare-and-swap plus the `events_one_payment_confirmed`
//! unique index enforce the once, not application logic. A claim only stamps
//! `last_checked_at`, so a crash between the lookup and the effect leaves
//! the correlation pending and it is re-verified after the poll interval:
//! bounded, abortable, and resumable across restarts. Marketplace
//! payment-window expiry is a separate server-time transition over the
//! ORDER's armed inventory-hold window ("only a payment locks an item"):
//! a lapsed hold restocks, the payment expires, and the order cancels.
//! Locks v1 leaves transport/status failures pending, so upstream trouble
//! never expires a payment by itself — and a completion verified after the
//! window goes to `manual_review`, never silently discarded.
//!
//! Delivery autocomplete semantics (ADR-0019): there is NO carrier
//! tracking feed, so post-purchase liveness is server time. A `shipped`
//! order is marked `delivered` once `DELIVERY_ASSUME_DAYS` has elapsed
//! since the ship timestamp, flagged `delivery_assumed = true` so the UI
//! can say "marked delivered automatically after N days; tell us if it
//! hasn't arrived" (the `delivery_assume` trigger on the machine's
//! shipped → delivered edge). A `delivered` order completes once
//! `AUTO_COMPLETE_DAYS` has elapsed (`order_auto_complete` on the
//! delivered → completed edge); an open return or cancel request is its
//! own order state, so it blocks the sweep by construction. Both events
//! are attributed to the system actor, never to a peer.

use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

use crate::bitcoin_review::{apply_late_money, LateMoneyOutcome};
use crate::handlers::auction::{close_locked_auction, parse_auction};
use crate::handlers::payment::confirm_order;
use crate::handlers::{fetch_order_for_update, insert_notification_intent, LISTING_COLUMNS};
use crate::homeserver::HomeserverListingClient;
use crate::locks::{LocksLookupOutcome, LocksRuntime, LocksTaskStatus};
use crate::model::{ListingRow, PaymentRow};
use crate::payment_attempt::{AcceptedAsset, OfferedAssets, PaymentAsset, PaymentTerms};
use crate::payments::{
    ActivationTerms, PaykitClient, PaykitCommandError, PaykitLifecycleTarget, PaykitStatusOutcome,
    PaykitStatusSource, PaykitUsdtObservation,
};
use crate::queries::PAYMENT_COLUMNS;
use crate::{expiry, fx, AppState};

pub const TASK_RESERVATION_EXPIRY: &str = "reservation_expiry";
pub const TASK_DROP_TRANSITIONS: &str = "drop_transitions";
pub const TASK_OFFER_EXPIRY: &str = "offer_expiry";
pub const TASK_AUCTION_CLOSE: &str = "auction_close";
pub const TASK_OUTBOX: &str = "outbox";
pub const TASK_LOCKS_VERIFICATION: &str = "locks_verification";
pub const TASK_PAYKIT_VERIFICATION: &str = "paykit_verification";
pub const TASK_PAYMENT_WINDOW: &str = "payment_window";
pub const TASK_STAT_ATTESTATIONS: &str = "stat_attestations";
pub const TASK_DELIVERY_AUTOCOMPLETE: &str = "delivery_autocomplete";
pub const TASK_PICKUP_RESEAL: &str = "pickup_reseal";
pub const TASK_DIGITAL_RESEAL: &str = "digital_reseal";
pub const TASK_PRIV_KEY_RESEAL: &str = "priv_key_reseal";
pub const TASK_DELIVERY_EMAIL_PURGE: &str = "delivery_email_purge";
pub const TASK_DELIVERY_ADDRESS_PURGE: &str = "delivery_address_purge";
pub const TASK_READ_RATE_PURGE: &str = "read_rate_purge";
pub const TASK_PICKUP_RETENTION: &str = "pickup_retention";
pub const TASK_SELLER_CONFIRMATION_WINDOW: &str = "seller_confirmation_window";
pub const TASK_MANUAL_REVIEW_WATCH: &str = "manual_review_watch";
pub const TASK_PAYKIT_RESOLVE_DELIVERY: &str = "paykit_resolve_delivery";
pub const TASK_FX_SAMPLER: &str = "fx_sampler";
pub const TASK_REFUSAL_AUDIT_RETENTION: &str = "refusal_audit_retention";
pub const TASK_LISTING_DELETIONS: &str = "listing_deletions";
pub const TASK_REVIEW_RELEASE: &str = "review_release";

static AWARD_EXPIRY_RETRY_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Returns the process-wide count of award-expiry deadlock retries.
///
/// This is an observation seam for integration tests; it does not alter retry
/// timing or outcome.
pub fn award_expiry_retry_count() -> usize {
    AWARD_EXPIRY_RETRY_COUNT.load(Ordering::SeqCst)
}

/// The actor stamped on server-time post-purchase events and their
/// notifications: the system, never a peer (ADR-0019).
pub const SYSTEM_ACTOR: &str = "system";

/// Max inner claim loops per lease so a huge backlog cannot hold
/// `TASK_DELIVERY_AUTOCOMPLETE` indefinitely (next interval resumes).
pub const DELIVERY_SWEEP_MAX_BATCHES: u32 = 10;

const OUTBOX_BATCH_SIZE: i64 = 100;
const AUCTION_CLOSE_BATCH_SIZE: i64 = 100;
const LOCKS_VERIFY_BATCH_SIZE: i64 = 25;
const PAYKIT_VERIFY_BATCH_SIZE: i64 = 25;
const FX_SAMPLE_INTERVAL_SECONDS: i64 = 60;

/// Stat attestation cadence (ratified D3: weekly) and window (trailing 90
/// days, matching the design's period example).
const STAT_ATTESTATION_INTERVAL_DAYS: i64 = 7;
const STAT_ATTESTATION_WINDOW_DAYS: i64 = 90;

/// Takes (or renews) the lease for one task. Returns false when another
/// live holder owns it.
pub async fn try_acquire_lease(
    pool: &PgPool,
    task: &str,
    holder: Uuid,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<bool, sqlx::Error> {
    Ok(
        try_acquire_fenced_lease(pool, task, holder, now, lease_seconds)
            .await?
            .is_some(),
    )
}

/// [`try_acquire_lease`] returning the lease's fence: a number that grows
/// with every acquisition, renewals included, so a pass can tell whether
/// the lease row is still the one it took. `None` when another live
/// holder owns the task.
pub async fn try_acquire_fenced_lease<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    task: &str,
    holder: Uuid,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let lease_until = now + chrono::Duration::seconds(lease_seconds);
    sqlx::query_scalar(
        "INSERT INTO worker_leases (task, holder, lease_until, fence) VALUES ($1, $2, $3, 1) \
         ON CONFLICT (task) DO UPDATE SET holder = EXCLUDED.holder, \
         lease_until = EXCLUDED.lease_until, fence = worker_leases.fence + 1 \
         WHERE worker_leases.lease_until <= $4 OR worker_leases.holder = EXCLUDED.holder \
         RETURNING fence",
    )
    .bind(task)
    .bind(holder)
    .bind(lease_until)
    .bind(now)
    .fetch_optional(executor)
    .await
}

/// Ends the holder's lease so any instance can take the task immediately.
pub async fn release_lease(
    pool: &PgPool,
    task: &str,
    holder: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE worker_leases SET lease_until = $3 WHERE task = $1 AND holder = $2")
        .bind(task)
        .bind(holder)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

/// [`release_lease`] for a fenced pass: a no-op once a later acquisition,
/// by this holder or another, owns the row.
pub async fn release_fenced_lease<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    task: &str,
    holder: Uuid,
    fence: i64,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE worker_leases SET lease_until = LEAST(lease_until, $4) \
         WHERE task = $1 AND holder = $2 AND fence = $3",
    )
    .bind(task)
    .bind(holder)
    .bind(fence)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

/// Expires pending/countered offers whose server-time deadline has passed.
/// The offer moves to the terminal `expired` state with a revision bump and
/// an `offer.expired` event traceable through the offer id.
pub async fn expire_due_offers(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    #[derive(sqlx::FromRow)]
    struct DueNegotiatingOffer {
        id: Uuid,
        aggregate_id: String,
        revision: i64,
        offered_by: String,
    }
    let due: Vec<DueNegotiatingOffer> = sqlx::query_as(
        "SELECT id, aggregate_id, revision, offered_by FROM offers \
         WHERE state IN ('pending', 'countered') AND expires_at <= $1 \
         ORDER BY expires_at FOR UPDATE SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for offer in due {
        let flipped = sqlx::query(
            "UPDATE offers SET state = 'expired', expiry_reason = 'negotiation_window', \
             revision = revision + 1, updated_at = $2 \
             WHERE id = $1 AND state IN ('pending', 'countered')",
        )
        .bind(offer.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if flipped.rows_affected() != 1 {
            continue;
        }
        sqlx::query(
            "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
             occurred_at) VALUES ($1, $2, $3, $4, $5, 'offer.expired', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(offer.id)
        .bind(&offer.aggregate_id)
        .bind(offer.revision + 1)
        .bind(&offer.offered_by)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tracing::info!(offer_id = %offer.id, aggregate_id = %offer.aggregate_id, "expired offer");
        expired += 1;
    }
    tx.commit().await?;

    let due_awards: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM offers WHERE state = 'accepted' AND award_expires_at <= $1 ORDER BY id",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    for offer_id in due_awards {
        for attempt in 0..3u64 {
            match expire_due_award(pool, offer_id, now).await {
                Ok(true) => {
                    expired += 1;
                    break;
                }
                Ok(false) => break,
                Err(error) if is_deadlock(&error) && attempt < 2 => {
                    AWARD_EXPIRY_RETRY_COUNT.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
                }
                Err(error) => {
                    tracing::warn!(offer_id = %offer_id, %error, "skipping failed award expiry");
                    break;
                }
            }
        }
    }
    Ok(expired)
}

#[derive(sqlx::FromRow)]
struct DueAward {
    id: Uuid,
    aggregate_id: String,
    revision: i64,
    offered_by: String,
    reservation_id: Uuid,
    award_id: Uuid,
    listing_aggregate_id: String,
    quantity: i64,
}

async fn expire_due_award(
    pool: &PgPool,
    offer_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let offer: Option<DueAward> = sqlx::query_as(
        "SELECT id, aggregate_id, revision, offered_by, reservation_id, award_id, \
         listing_aggregate_id, quantity FROM offers \
         WHERE id = $1 AND state = 'accepted' AND award_expires_at <= $2 FOR UPDATE",
    )
    .bind(offer_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(offer) = offer else {
        tx.commit().await?;
        return Ok(false);
    };
    let reservation: Option<(String, i64, Option<String>)> = sqlx::query_as(
        "UPDATE reservations SET status = 'expired', updated_at = $2 \
         WHERE id = $1 AND status = 'active' AND offer_award_id = $3 \
         RETURNING listing_aggregate_id, quantity, variant_id",
    )
    .bind(offer.reservation_id)
    .bind(now)
    .bind(offer.award_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((reservation_listing, reservation_quantity, reservation_variant)) = reservation else {
        return Err(sqlx::Error::Protocol(
            "award reservation is not active".to_string(),
        ));
    };
    if reservation_listing != offer.listing_aggregate_id || reservation_quantity != offer.quantity {
        return Err(sqlx::Error::Protocol(
            "award reservation does not match its locked offer".to_string(),
        ));
    }
    let rows = crate::variant_stock::lock_rows(&mut tx, &offer.listing_aggregate_id).await?;
    let held_row = crate::variant_stock::resolve_held(&rows, reservation_variant.as_deref())
        .map_err(|refusal| {
            sqlx::Error::Protocol(format!(
                "award reservation names no variant row of its listing: {refusal:?}"
            ))
        })?;
    let released = crate::variant_stock::release(
        &mut tx,
        &offer.listing_aggregate_id,
        &held_row.variant_id,
        offer.quantity,
        crate::variant_stock::Held::Reserved,
    )
    .await?;
    if !released {
        return Err(sqlx::Error::Protocol(
            "award reservation inventory is not held by its listing".to_string(),
        ));
    }
    sqlx::query(&format!(
        "UPDATE listings SET server_revision = server_revision + 1, \
         state = {}, updated_at = $2 WHERE aggregate_id = $1",
        crate::variant_stock::STATE_FROM_TOTALS_SQL
    ))
    .bind(&offer.listing_aggregate_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    let offer_flipped = sqlx::query(
        "UPDATE offers SET state = 'expired', expiry_reason = 'award_window', \
         revision = revision + 1, updated_at = $2 WHERE id = $1 AND state = 'accepted'",
    )
    .bind(offer.id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if offer_flipped.rows_affected() != 1 {
        return Err(sqlx::Error::Protocol(
            "locked accepted offer could not be expired".to_string(),
        ));
    }
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, $4, $5, 'offer.expired', $6)",
    )
    .bind(Uuid::new_v4())
    .bind(offer.id)
    .bind(&offer.aggregate_id)
    .bind(offer.revision + 1)
    .bind(&offer.offered_by)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::info!(offer_id = %offer.id, aggregate_id = %offer.aggregate_id, "expired offer award");
    Ok(true)
}

fn is_deadlock(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("40P01"))
}

/// Applies the server-time drop transitions for drops no command has
/// touched — announced → live at `starts_at`, live (or a fully elapsed
/// announced window) → ended_closed at `ends_at` — through the same shared
/// transition path gating uses, so projections and gating agree without
/// traffic. Due rows are locked with `FOR UPDATE SKIP LOCKED`.
pub async fn transition_due_drops(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<crate::model::DropRow> = sqlx::query_as(&format!(
        "SELECT {columns} FROM drops \
         WHERE (state = 'announced' AND starts_at <= $1) \
         OR (state = 'live' AND ends_at IS NOT NULL AND ends_at <= $1) \
         ORDER BY aggregate_id FOR UPDATE SKIP LOCKED",
        columns = crate::handlers::drops::DROP_COLUMNS
    ))
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut transitioned = 0u64;
    for drop in due {
        let from = drop.state.clone();
        let after =
            crate::handlers::drops::apply_time_transitions(&mut tx, drop, Uuid::new_v4(), now)
                .await?;
        if after.state != from {
            tracing::info!(
                aggregate_id = %after.aggregate_id,
                from = %from,
                to = %after.state,
                "transitioned drop on server time"
            );
            transitioned += 1;
        }
    }
    tx.commit().await?;
    Ok(transitioned)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct AuctionCloseBatchSummary {
    pub closed: u64,
    pub failed: u64,
}

/// Canonical auction timestamps are fixed-width UTC strings. PostgreSQL's
/// non-throwing input check validates their calendar values before a guarded
/// cast; the locked Rust path remains authoritative for validity and due-ness.
const AUCTION_INSTANT: &str = r"^[0-9]{4}-(0[1-9]|1[0-2])-([012][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$";

/// Starts a bounded batch of scheduled auctions whose start time has passed
/// on server time, so their projection reads `active` (and the close sweep
/// below picks them up) without waiting for a bid. Bidding and the seller's
/// close apply the same transition themselves. A malformed start time is
/// never cast: the CASE keeps the cast off such rows.
pub async fn start_due_auctions(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<ListingRow> = sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings \
         WHERE sale_format = 'auction' AND auction->>'status' = 'scheduled' \
         AND CASE \
             WHEN auction->>'starts_at' ~ $2 \
                 AND pg_input_is_valid(auction->>'starts_at', 'timestamp with time zone') \
             THEN (auction->>'starts_at')::timestamptz <= $1 \
             ELSE FALSE \
         END \
         ORDER BY aggregate_id LIMIT $3 FOR UPDATE SKIP LOCKED"
    ))
    .bind(now)
    .bind(AUCTION_INSTANT)
    .bind(AUCTION_CLOSE_BATCH_SIZE)
    .fetch_all(&mut *tx)
    .await?;

    let mut started = 0u64;
    for listing in due {
        let before = listing.server_revision;
        let after =
            crate::handlers::auction::apply_start_transition(&mut tx, listing, Uuid::new_v4(), now)
                .await?;
        if after.server_revision != before {
            tracing::info!(
                aggregate_id = %after.aggregate_id,
                "started auction on server time"
            );
            started += 1;
        }
    }
    tx.commit().await?;
    Ok(started)
}

/// Authoritatively closes a bounded batch of active auctions whose end time
/// has passed on server time, using the same close path as the seller command.
/// Canonical due rows and malformed timestamp rows have independent limits:
/// malformed rows stay observable but cannot consume the oldest-due budget.
/// Each auction gets its own transaction so one corrupt row cannot roll back
/// a peer. Dynamic errors are deliberately omitted from telemetry because
/// they can include secret values or payload fragments.
///
/// With a homeserver, each due auction's seller record is read first and a
/// removed listing closes unsold. A homeserver that cannot answer does not
/// hold the close back: closing takes no payment, and the winner's payment
/// start refuses a removed listing on its own, failing closed.
pub async fn close_due_auctions(
    pool: &PgPool,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> anyhow::Result<AuctionCloseBatchSummary> {
    let due: Vec<String> = sqlx::query_scalar(
        "WITH classified AS MATERIALIZED ( \
             SELECT aggregate_id, \
                 CASE \
                     WHEN auction->>'ends_at' ~ $2 \
                         AND pg_input_is_valid( \
                             auction->>'ends_at', 'timestamp with time zone' \
                         ) \
                     THEN (auction->>'ends_at')::timestamptz \
                     ELSE NULL \
                 END AS ends_at \
             FROM listings \
             WHERE sale_format = 'auction' AND auction->>'status' = 'active' \
         ), valid_due AS ( \
             SELECT aggregate_id, ends_at, 0 AS lane \
             FROM classified \
             WHERE ends_at IS NOT NULL AND ends_at <= $1 \
             ORDER BY ends_at, aggregate_id LIMIT $3 \
         ), malformed AS ( \
             SELECT aggregate_id, NULL::timestamptz AS ends_at, 1 AS lane \
             FROM classified \
             WHERE ends_at IS NULL \
             ORDER BY aggregate_id LIMIT $3 \
         ) \
         SELECT aggregate_id FROM ( \
             SELECT * FROM valid_due UNION ALL SELECT * FROM malformed \
         ) candidates \
         ORDER BY lane, ends_at, aggregate_id",
    )
    .bind(now)
    .bind(AUCTION_INSTANT)
    .bind(AUCTION_CLOSE_BATCH_SIZE)
    .fetch_all(pool)
    .await?;

    let mut summary = AuctionCloseBatchSummary::default();
    for aggregate_id in due {
        match close_one_due_auction(pool, homeserver, &aggregate_id, now).await {
            Ok(Some(sold)) => {
                tracing::info!(
                    aggregate_id = %aggregate_id,
                    outcome = if sold { "sold" } else { "unsold" },
                    "closed auction on server time"
                );
                summary.closed += 1;
            }
            Ok(None) => {}
            Err(_) => {
                tracing::error!(
                    aggregate_id = %aggregate_id,
                    failure = "auction_close_failed",
                    "auction close failed; continuing batch"
                );
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

async fn close_one_due_auction(
    pool: &PgPool,
    homeserver: Option<&dyn HomeserverListingClient>,
    aggregate_id: &str,
    now: DateTime<Utc>,
) -> Result<Option<bool>, sqlx::Error> {
    let withdrawn = match homeserver {
        Some(homeserver) => {
            let record: Option<(String, String)> = sqlx::query_as(
                "SELECT seller_pubky, listing_id FROM listings \
                 WHERE aggregate_id = $1 AND deleted_at IS NULL",
            )
            .bind(aggregate_id)
            .fetch_optional(pool)
            .await?;
            match record {
                Some((seller_pubky, listing_id)) => {
                    match crate::listing_lifecycle::is_removed(
                        homeserver,
                        &seller_pubky,
                        &listing_id,
                    )
                    .await
                    {
                        Some(removed) => removed,
                        None => {
                            tracing::warn!(
                                aggregate_id = %aggregate_id,
                                "auction close could not read the seller record; closing on stored state"
                            );
                            false
                        }
                    }
                }
                None => false,
            }
        }
        None => false,
    };
    let mut tx = pool.begin().await?;
    let listing: Option<ListingRow> = sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings \
         WHERE aggregate_id = $1 AND sale_format = 'auction' \
         AND auction->>'status' = 'active' \
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(listing) = listing else {
        tx.commit().await?;
        return Ok(None);
    };
    let auction = parse_auction(&listing)
        .ok_or_else(|| sqlx::Error::Protocol("stored auction state is invalid".to_string()))?;
    if auction.ends_at > now {
        tx.commit().await?;
        return Ok(None);
    }
    let seller = listing.seller_pubky.clone();
    let outcome = close_locked_auction(
        &mut tx,
        &listing,
        auction,
        &seller,
        Uuid::new_v4(),
        withdrawn,
        now,
    )
    .await?;
    tx.commit().await?;
    Ok(Some(outcome.sold))
}

#[derive(Debug, sqlx::FromRow)]
pub struct ClaimedOutboxRow {
    pub id: i64,
    pub event_id: Uuid,
    pub kind: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

/// Claims a batch of undelivered outbox rows by stamping `lease_until`.
/// Rows whose previous claim lapsed (crashed deliverer) are reclaimed.
pub async fn claim_outbox_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<Vec<ClaimedOutboxRow>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE outbox SET lease_until = $2 WHERE id IN (\
             SELECT id FROM outbox \
             WHERE delivered_at IS NULL AND quarantined_at IS NULL \
             AND (lease_until IS NULL OR lease_until <= $1) \
             ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED\
         ) RETURNING id, event_id, kind, payload, created_at",
    )
    .bind(now)
    .bind(now + chrono::Duration::seconds(lease_seconds))
    .bind(OUTBOX_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Delivers claimed intents. Each row is consumed in one transaction: the
/// notification insert (deduplicated by event id + recipient) and the
/// `delivered_at` mark commit together, so a redelivered intent can never
/// apply its effect twice. `paykit.activate` / `paykit.void` rows drive the
/// two-phase protocol against the order's persisted stack endpoint
/// (§B.11.8); a paykit row on a deployment without the signed client is
/// skipped (logged), never head-of-line for the rest of the batch. A
/// notification row that can never deliver (an unroutable kind, a payload
/// missing a required field) is quarantined on its own and the batch
/// continues.
pub async fn deliver_claimed(
    pool: &PgPool,
    paykit: Option<&PaykitClient>,
    rows: &[ClaimedOutboxRow],
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let mut delivered = 0u64;
    for row in rows {
        if row.kind == "paykit.activate" {
            let Some(paykit) = paykit else {
                tracing::error!(
                    row_id = row.id,
                    "paykit.activate row on a deployment without the paykit client; skipping"
                );
                continue;
            };
            if deliver_paykit_activation(pool, paykit, row, now).await? {
                delivered += 1;
            }
            continue;
        }
        if row.kind == "paykit.void" {
            let Some(paykit) = paykit else {
                tracing::error!(
                    row_id = row.id,
                    "paykit.void row on a deployment without the paykit client; skipping"
                );
                continue;
            };
            if deliver_paykit_void(pool, paykit, row, now).await? {
                delivered += 1;
            }
            continue;
        }
        let intent = match notification_intent(row) {
            Ok(intent) => intent,
            Err(reason) => {
                quarantine_outbox_row(pool, row.id, reason, now).await?;
                continue;
            }
        };
        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
             aggregate_id, amount, review_reason, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (event_id, recipient_pubky) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(row.event_id)
        .bind(intent.recipient)
        .bind(intent.actor)
        .bind(intent.notification_type)
        .bind(intent.aggregate_id)
        .bind(intent.amount)
        .bind(intent.review_reason)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
            .bind(row.id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        delivered += 1;
    }
    Ok(delivered)
}

/// Why a notification outbox row can never deliver (the 0049
/// `outbox_quarantine_check` vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxQuarantine {
    UnroutableKind,
    MissingRecipientPubky,
    MissingActorPubky,
    MissingAggregateId,
}

impl OutboxQuarantine {
    pub const ALL: [Self; 4] = [
        Self::UnroutableKind,
        Self::MissingRecipientPubky,
        Self::MissingActorPubky,
        Self::MissingAggregateId,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnroutableKind => "unroutable_kind",
            Self::MissingRecipientPubky => "missing_recipient_pubky",
            Self::MissingActorPubky => "missing_actor_pubky",
            Self::MissingAggregateId => "missing_aggregate_id",
        }
    }
}

/// A notification outbox row, validated before anything is written.
struct NotificationIntent<'a> {
    notification_type: &'a str,
    recipient: &'a str,
    actor: &'a str,
    aggregate_id: &'a str,
    amount: Option<Value>,
    review_reason: Option<&'static str>,
}

fn notification_intent(row: &ClaimedOutboxRow) -> Result<NotificationIntent<'_>, OutboxQuarantine> {
    let notification_type = row
        .kind
        .strip_prefix("notification.")
        .filter(|kind| !kind.is_empty())
        .ok_or(OutboxQuarantine::UnroutableKind)?;
    let field = |name: &str, missing: OutboxQuarantine| {
        row.payload[name]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or(missing)
    };
    let recipient = field("recipient_pubky", OutboxQuarantine::MissingRecipientPubky)?;
    let actor = field("actor_pubky", OutboxQuarantine::MissingActorPubky)?;
    let aggregate_id = field("aggregate_id", OutboxQuarantine::MissingAggregateId)?;
    // Optional monetary context; intents written before amounts existed
    // have no key and deliver as NULL.
    let amount = match &row.payload["amount"] {
        Value::Null => None,
        value => Some(value.clone()),
    };
    // The 0048 CHECK refuses an unknown reason. An intent carrying one
    // still delivers, without it, so it never stalls the rows behind it.
    let review_reason = match &row.payload["review_reason"] {
        Value::Null => None,
        value => {
            let known = value
                .as_str()
                .and_then(crate::handlers::BitcoinReviewNotice::parse);
            if known.is_none() {
                tracing::warn!(
                    row_id = row.id,
                    "outbox notification carries an unknown review_reason; delivering without it"
                );
            }
            known.map(crate::handlers::BitcoinReviewNotice::as_str)
        }
    };
    Ok(NotificationIntent {
        notification_type,
        recipient,
        actor,
        aggregate_id,
        amount,
        review_reason,
    })
}

/// Sets one undeliverable row aside: it is never claimed again, and the
/// rest of the batch continues.
async fn quarantine_outbox_row(
    pool: &PgPool,
    row_id: i64,
    reason: OutboxQuarantine,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE outbox SET quarantined_at = $2, quarantine_reason = $3, lease_until = NULL \
         WHERE id = $1 AND delivered_at IS NULL",
    )
    .bind(row_id)
    .bind(now)
    .bind(reason.as_str())
    .execute(pool)
    .await?;
    tracing::error!(
        row_id,
        reason = reason.as_str(),
        "ALERT quarantined an undeliverable outbox row; continuing the batch"
    );
    Ok(())
}

fn payload_str<'a>(payload: &'a Value, field: &str, row_id: i64) -> anyhow::Result<&'a str> {
    payload[field]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("outbox row {row_id} payload is missing {field}"))
}

/// Claims and delivers due outbox intents.
pub async fn drain_outbox(
    pool: &PgPool,
    paykit: Option<&PaykitClient>,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> anyhow::Result<u64> {
    let claimed = claim_outbox_batch(pool, now, lease_seconds).await?;
    deliver_claimed(pool, paykit, &claimed, now).await
}

// ---------------------------------------------------------------------------
// Two-phase paykit activation (§B.11.2, §B.11.8)
// ---------------------------------------------------------------------------

/// What an activation must echo, from the order's persisted request terms.
#[derive(Debug)]
enum Activation {
    /// The legacy `amount_sats` request: its satoshi total.
    Legacy(u64),
    /// The multi-asset request: its denomination, amount and offer.
    Assets(ActivationTerms),
}

/// The parsed `paykit.activate` payload (written in the bind transaction).
struct ActivatePayload {
    invoice_id: Uuid,
    order_id: Uuid,
    activation_attempt: i64,
}

#[derive(sqlx::FromRow)]
struct ActivationBounds {
    hold_expires_at: Option<DateTime<Utc>>,
    paykit_prepare_expires_at: Option<DateTime<Utc>>,
    paykit_api: Option<String>,
}

fn parse_activate_payload(row: &ClaimedOutboxRow) -> anyhow::Result<ActivatePayload> {
    let invoice_id = payload_str(&row.payload, "invoice_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} invoice_id is not a uuid", row.id))?;
    let order_id = payload_str(&row.payload, "order_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} order_id is not a uuid", row.id))?;
    let activation_attempt = row.payload["activation_attempt"].as_i64().ok_or_else(|| {
        anyhow::anyhow!(
            "outbox row {} payload is missing activation_attempt",
            row.id
        )
    })?;
    Ok(ActivatePayload {
        invoice_id,
        order_id,
        activation_attempt,
    })
}

/// One transaction stamping the outbox row delivered and nothing else (the
/// idempotent no-op for a row whose order already left `preparing`).
async fn stamp_delivered_only(
    pool: &PgPool,
    row_id: i64,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(row_id)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

/// The terminal-void transaction shared by every `preparing → voided` edge
/// (§B.11.2): the bind is released — hold returned to the listing, the
/// payment row restored to its pre-bind state, the method cleared so the
/// buyer can choose again — and the `payment.bitcoin_prepare_voided`
/// notification intent is emitted, all atomically with the outbox mark.
/// Caller holds the outbox row's lease.
async fn void_prepare_effects(
    pool: &PgPool,
    row_id: i64,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    lock_order_payment(&mut tx, order_id).await?;
    let Some(order) = fetch_order_for_update(&mut tx, order_id).await? else {
        anyhow::bail!("paykit.activate row {row_id} references a missing order {order_id}");
    };
    if order.paykit_activation_state.as_deref() != Some("preparing") {
        // A redelivery or a concurrently resolved row: mark only.
        sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
            .bind(row_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(());
    }
    crate::paykit_attempts::record_released_attempt(&mut tx, order_id, now).await?;
    if order.stock_held {
        if let Err(failure) = crate::handlers::holds::release_lines(
            &mut tx,
            &order,
            crate::handlers::holds::HeldQuantity::Reserved,
            now,
        )
        .await?
        {
            anyhow::bail!("voided order {order_id} could not release its hold: {failure:?}");
        }
    }
    let (revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET revision = revision + 1, paykit_activation_state = 'voided', \
         paykit_request_state = NULL, payment_method = NULL, stock_held = false, \
         hold_expires_at = NULL, updated_at = $2 WHERE id = $1 RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    // The payment row returns to its pre-bind state (sandbox adapter,
    // listing-price amount), so a fresh bind starts clean.
    sqlx::query(
        "UPDATE payments SET revision = revision + 1, adapter = 'sandbox', \
         amount_minor = $2, updated_at = $3 WHERE id = $1",
    )
    .bind(order.payment_id)
    .bind(order.total_minor)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    let event_id = crate::executor::insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order_id),
        revision,
        SYSTEM_ACTOR,
        "payment.bitcoin_prepare_voided",
        now,
    )
    .await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        "bitcoin_prepare_voided",
        &order.buyer_pubky,
        SYSTEM_ACTOR,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await?;
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(row_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    tracing::info!(order_id = %order_id, "voided a prepared bitcoin bind");
    Ok(())
}

/// Delivers one `paykit.activate` row (§B.11.8): POSTs the signed activate
/// to the order's PERSISTED stack endpoint (never the current
/// configuration), then commits the state flip and the `delivered_at` mark
/// in one transaction so a redelivered row cannot apply twice.
async fn deliver_paykit_activation(
    pool: &PgPool,
    paykit: &PaykitClient,
    row: &ClaimedOutboxRow,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let payload = parse_activate_payload(row)?;
    // Read the order's persisted pin under its row lock; count this try by
    // incrementing the attempt in the payload before the call.
    let (target, activation, attempt) = {
        let mut tx = pool.begin().await?;
        lock_order_payment(&mut tx, payload.order_id).await?;
        let Some(order) = fetch_order_for_update(&mut tx, payload.order_id).await? else {
            anyhow::bail!(
                "paykit.activate row {} references a missing order {}",
                row.id,
                payload.order_id
            );
        };
        if order.paykit_activation_state.as_deref() != Some("preparing") {
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(true);
        }
        if order.state != "pending_payment" {
            // An order that left `pending_payment` with its request still
            // `preparing` (a cancel from before cancel voided the request):
            // never publish it — void instead.
            let pin = match (order.paykit_invoice_id, order.paykit_api.as_deref()) {
                (Some(invoice_id), Some("fork")) => match (
                    order.paykit_stack_id.clone(),
                    order.paykit_stack_endpoint.clone(),
                ) {
                    (Some(stack_id), Some(stack_endpoint)) => {
                        Some(crate::handlers::cancellation::PaykitVoidPin::Fork {
                            invoice_id,
                            stack_id,
                            stack_endpoint,
                        })
                    }
                    _ => None,
                },
                (Some(invoice_id), Some("upstream")) => {
                    Some(crate::handlers::cancellation::PaykitVoidPin::Upstream {
                        invoice_id,
                        creator: order.seller_pubky.clone(),
                    })
                }
                _ => None,
            };
            crate::handlers::cancellation::void_preparing_request(&mut tx, order.id, now).await?;
            if let Some(pin) = &pin {
                crate::handlers::cancellation::enqueue_paykit_void(
                    &mut tx,
                    row.event_id,
                    order.id,
                    pin,
                    "order_not_payable",
                    now,
                )
                .await?;
            }
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::warn!(
                order_id = %order.id,
                order_state = %order.state,
                "refused to activate a paykit request for an order that is no longer payable"
            );
            return Ok(true);
        }
        if order.paykit_api.as_deref() == Some("upstream") {
            // `now` is sampled once per worker pass and may be stale behind a
            // slow batch. PostgreSQL clock time under the locked order is the
            // authoritative local guard; `GREATEST` retains deterministic
            // future-time test/sweep calls without letting a stale caller
            // move the guard backwards in production.
            let guard_now: DateTime<Utc> =
                sqlx::query_scalar("SELECT GREATEST($1::timestamptz, clock_timestamp())")
                    .bind(now)
                    .fetch_one(&mut *tx)
                    .await?;
            let activation_cutoff = order.paykit_prepare_expires_at.map(|deadline| {
                deadline
                    - chrono::Duration::seconds(
                        crate::payment_methods::UPSTREAM_HOLD_CLOCK_SKEW_SECONDS,
                    )
            });
            let hold_live = if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
                // An auction order never takes a bind hold: its inventory
                // hold is the winner's reservation (`acquire_payment_hold`).
                // Read it under this transaction so the reservation sweep
                // cannot flip it while the guard decides. Upstream starts
                // the payment window at activation, so the window must end
                // one skew margin before the shown hold (reservation - one
                // margin): Paykit's activation clock may run up to one
                // margin ahead, and its deadline then still fits the hold.
                let reservation_expires_at: Option<DateTime<Utc>> = sqlx::query_scalar(
                    "SELECT expires_at FROM reservations \
                     WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 \
                     AND status = 'active' FOR SHARE",
                )
                .bind(auction_aggregate_id)
                .bind(&order.buyer_pubky)
                .fetch_optional(&mut *tx)
                .await?;
                match (reservation_expires_at, order.paykit_payment_window_seconds) {
                    (Some(deadline), Some(seconds)) => {
                        crate::payment_methods::upstream_auction_window_fits(
                            deadline,
                            guard_now,
                            i64::from(seconds),
                        )
                    }
                    _ => false,
                }
            } else {
                order.stock_held
                    && order
                        .hold_expires_at
                        .is_some_and(|deadline| guard_now < deadline)
            };
            let activation_allowed =
                hold_live && activation_cutoff.is_some_and(|deadline| guard_now < deadline);
            if !activation_allowed {
                // No signed activate leaves this process. A previous attempt
                // may have committed remotely with a lost response, so record
                // the released attempt for signed-status reconciliation; do
                // not issue a blind remote void.
                tx.commit().await?;
                tracing::warn!(
                    order_id = %order.id,
                    activation_attempt = payload.activation_attempt,
                    "refused upstream paykit activation outside the original inventory hold boundary"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                return Ok(true);
            }
        }
        let attempt = payload.activation_attempt + 1;
        sqlx::query(
            "UPDATE outbox SET payload = jsonb_set(payload, '{activation_attempt}', $2) \
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(serde_json::json!(attempt))
        .execute(&mut *tx)
        .await?;
        let target = match order.paykit_api.as_deref() {
            Some("fork") => match (
                order.paykit_stack_endpoint.clone(),
                order.paykit_stack_id.clone(),
            ) {
                (Some(endpoint), Some(stack_id)) => {
                    PaykitLifecycleTarget::Fork { endpoint, stack_id }
                }
                _ => anyhow::bail!(
                    "preparing fork order {} is missing its persisted paykit pin",
                    payload.order_id
                ),
            },
            Some("upstream") => PaykitLifecycleTarget::Upstream {
                creator: order.seller_pubky.clone(),
            },
            other => anyhow::bail!(
                "preparing order {} has invalid paykit_api {:?}",
                payload.order_id,
                other
            ),
        };
        // A multi-asset request activates by its denomination and amount;
        // every other request by its satoshi total.
        let activation = match (
            order.paykit_amount_minor,
            order
                .paykit_asset
                .as_deref()
                .and_then(PaymentAsset::from_column),
            order
                .paykit_payment_amounts
                .as_ref()
                .and_then(OfferedAssets::from_json),
        ) {
            (Some(amount_minor), Some(denomination), Some(offered)) => {
                Activation::Assets(ActivationTerms {
                    denomination,
                    amount_minor: u64::try_from(amount_minor).map_err(|_| {
                        anyhow::anyhow!(
                            "order {} paykit_amount_minor is negative",
                            payload.order_id
                        )
                    })?,
                    offered,
                })
            }
            (None, _, _) => {
                let Some(total_sats) = order.paykit_total_sats else {
                    anyhow::bail!(
                        "preparing order {} is missing paykit_total_sats",
                        payload.order_id
                    );
                };
                Activation::Legacy(u64::try_from(total_sats).map_err(|_| {
                    anyhow::anyhow!("order {} paykit_total_sats is negative", payload.order_id)
                })?)
            }
            _ => anyhow::bail!(
                "preparing order {} carries incomplete multi-asset request terms",
                payload.order_id
            ),
        };
        tx.commit().await?;
        (target, activation, attempt)
    };
    let acknowledged = match &activation {
        Activation::Legacy(total_sats) => {
            match paykit
                .activate_payment_request(
                    &target,
                    payload.invoice_id,
                    *total_sats,
                    u64::try_from(attempt).unwrap_or(0),
                )
                .await
            {
                Ok(activated) if activated.total_sats != *total_sats => {
                    tracing::error!(
                        order_id = %payload.order_id,
                        persisted_total_sats = *total_sats,
                        activated_total_sats = activated.total_sats,
                        "ALERT paykit activate acknowledged a different total than persisted; \
                         voiding the bind"
                    );
                    void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                    return Ok(true);
                }
                Ok(activated) => Ok((activated.activated_at, activated.payment_deadline)),
                Err(error) => Err(error),
            }
        }
        Activation::Assets(terms) => paykit
            .activate_marketplace_request(&target, payload.invoice_id, terms)
            .await
            .map(|activated| (activated.activated_at, activated.payment_deadline)),
    };
    match acknowledged {
        Ok((activated_at, payment_deadline)) => {
            let mut tx = pool.begin().await?;
            lock_order_payment(&mut tx, payload.order_id).await?;
            let bounds: Option<ActivationBounds> = sqlx::query_as(
                "SELECT hold_expires_at, paykit_prepare_expires_at, paykit_api \
                 FROM orders WHERE id = $1 FOR UPDATE",
            )
            .bind(payload.order_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(bounds) = bounds else {
                anyhow::bail!("activated paykit order {} vanished", payload.order_id);
            };
            if bounds.paykit_api.as_deref() == Some("upstream")
                && (bounds
                    .hold_expires_at
                    .is_none_or(|hold| payment_deadline > hold)
                    || bounds
                        .paykit_prepare_expires_at
                        .is_none_or(|prepare| activated_at >= prepare))
            {
                // Remote activation is already durable. Preserve its exact
                // terms and original local hold; alert instead of extending
                // inventory or blindly voiding a potentially live request.
                tracing::error!(
                    order_id = %payload.order_id,
                    %activated_at,
                    %payment_deadline,
                    prepare_expires_at = ?bounds.paykit_prepare_expires_at,
                    hold_expires_at = ?bounds.hold_expires_at,
                    "ALERT upstream paykit activation exceeded persisted inventory bounds"
                );
            }
            // Conditional on `preparing` under the row lock: a redelivered
            // row cannot apply the flip twice. An upstream hold shrinks to
            // the request's deadline plus the skew margin, so stock is not
            // held for a request the wallet already treats as expired.
            let flipped = sqlx::query(&format!(
                "UPDATE orders SET paykit_activation_state = 'active', \
                 paykit_request_state = 'pending', paykit_expires_at = $3, \
                 bitcoin_quote_expires_at = CASE \
                     WHEN bitcoin_quote_rate IS NULL THEN NULL ELSE $3 END, \
                 hold_expires_at = CASE WHEN paykit_api = 'fork' THEN $3 \
                     ELSE {UPSTREAM_ACTIVATED_HOLD} END, \
                 updated_at = $2 \
                 WHERE id = $1 AND paykit_activation_state = 'preparing'"
            ))
            .bind(payload.order_id)
            .bind(now)
            .bind(payment_deadline)
            .execute(&mut *tx)
            .await?;
            let retracked = flipped.rows_affected() == 0
                && track_invoice_live_after_void(
                    &mut tx,
                    payload.order_id,
                    payload.invoice_id,
                    now,
                )
                .await?;
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            if flipped.rows_affected() == 1 {
                tracing::info!(
                    order_id = %payload.order_id,
                    invoice_id = %payload.invoice_id,
                    "activated a prepared bitcoin payment request"
                );
            }
            if retracked {
                tracing::error!(
                    order_id = %payload.order_id,
                    invoice_id = %payload.invoice_id,
                    "ALERT paykit_activated_after_cancel: the activation committed at paykit \
                     before the void; the cancelled order's request is tracked as pending"
                );
            }
            Ok(true)
        }
        Err(error) => match error {
            PaykitCommandError::PrepareExpired | PaykitCommandError::InvoiceFinalized => {
                tracing::warn!(
                    order_id = %payload.order_id,
                    error = ?error,
                    "paykit prepare is finalized; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::UnknownInvoice => {
                tracing::error!(
                    order_id = %payload.order_id,
                    invoice_id = %payload.invoice_id,
                    "ALERT paykit reports an unknown invoice (stack mixup); voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::ActivationTotalMismatch => {
                tracing::error!(
                    order_id = %payload.order_id,
                    persisted_terms = ?activation,
                    "ALERT paykit refused activation with a total mismatch; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::StackIdentityMismatch => {
                tracing::error!(
                    order_id = %payload.order_id,
                    "ALERT the persisted paykit endpoint answered with a different stack \
                     identity; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::UnexpectedRejection(code) => {
                tracing::error!(
                    order_id = %payload.order_id,
                    code = %code,
                    "ALERT paykit activation hit an unknown contract state; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::Unavailable => {
                tracing::warn!(
                    order_id = %payload.order_id,
                    attempt,
                    "paykit activation unreachable; the row retries under its lease"
                );
                Ok(false)
            }
        },
    }
}

/// Locks the order's payment row. Every transaction that writes both an
/// order and its payment takes the payment first — cancel, settlement, and
/// the Paykit activation and void arms — so two of them racing on one order
/// wait on each other instead of deadlocking. Re-locking inside the same
/// transaction is a no-op.
async fn lock_order_payment(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT id FROM payments WHERE order_id = $1 FOR UPDATE")
        .bind(order_id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(())
}

/// Paykit reports the invoice published after the marketplace voided the
/// order's request (an activation that committed before the void landed).
/// A voided order that already left `pending_payment` goes back to
/// tracking that same invoice as `active` / `pending`, and an unpaid Paykit
/// payment on it expires, so the status poll's observation tail keeps
/// watching and any settlement is late money. Returns whether the order
/// was re-tracked.
async fn track_invoice_live_after_void(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    invoice_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    lock_order_payment(tx, order_id).await?;
    let retracked = sqlx::query(
        "UPDATE orders SET paykit_activation_state = 'active', \
         paykit_request_state = 'pending', updated_at = $3 \
         WHERE id = $1 AND paykit_invoice_id = $2 \
         AND paykit_activation_state = 'voided' AND state <> 'pending_payment'",
    )
    .bind(order_id)
    .bind(invoice_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if retracked.rows_affected() == 0 {
        return Ok(false);
    }
    let unpaid: Option<(Uuid, i64, String)> = sqlx::query_as(
        "UPDATE payments p SET state = 'expired', revision = p.revision + 1, updated_at = $2 \
         FROM orders o WHERE o.id = $1 AND p.order_id = o.id \
         AND p.adapter = 'paykit' AND p.state = 'awaiting_entitlement' \
         RETURNING p.id, p.revision, o.buyer_pubky",
    )
    .bind(order_id)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((payment_id, revision, buyer_pubky)) = unpaid {
        crate::executor::insert_event(
            tx,
            Uuid::new_v4(),
            &ids::payment_aggregate_id(payment_id),
            revision,
            &buyer_pubky,
            "payment.expired",
            now,
        )
        .await?;
    }
    Ok(true)
}

/// Delivers one `paykit.void` row (the preparing-order hold-expiry hard
/// bound, a buyer cancel of a `preparing` request, or an activation refused
/// for an order that is no longer payable): an idempotent void retried
/// under the ordinary lease until delivered or 24 h old, then stamped with
/// a `gave_up` log.
async fn deliver_paykit_void(
    pool: &PgPool,
    paykit: &PaykitClient,
    row: &ClaimedOutboxRow,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    if row.created_at + chrono::Duration::hours(24) <= now {
        tracing::warn!(
            row_id = row.id,
            "gave_up delivering a paykit.void row after 24h; stamping it delivered"
        );
        stamp_delivered_only(pool, row.id, now).await?;
        return Ok(true);
    }
    let invoice_id: Uuid = payload_str(&row.payload, "invoice_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} invoice_id is not a uuid", row.id))?;
    let reason = payload_str(&row.payload, "reason", row.id)?;
    let target = match row.payload["paykit_api"].as_str().unwrap_or("fork") {
        "fork" => PaykitLifecycleTarget::Fork {
            stack_id: payload_str(&row.payload, "stack_id", row.id)?.to_string(),
            endpoint: payload_str(&row.payload, "stack_endpoint", row.id)?.to_string(),
        },
        "upstream" => PaykitLifecycleTarget::Upstream {
            creator: payload_str(&row.payload, "creator", row.id)?.to_string(),
        },
        value => anyhow::bail!("outbox row {} has invalid paykit_api {value}", row.id),
    };
    match paykit
        .void_payment_request(&target, invoice_id, reason)
        .await
    {
        Ok(voided) => {
            tracing::info!(
                invoice_id = %invoice_id,
                state = %voided.state,
                "delivered a paykit void"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        // Idempotent by contract (§B.11.6): the invoice is already in a
        // final state, so the caller's intent is satisfied.
        Err(PaykitCommandError::PrepareExpired) | Err(PaykitCommandError::UnknownInvoice) => {
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        // Paykit refuses to void a published invoice: the activation won
        // the race. The order must keep watching it so any money reaching
        // it takes the late-money fork.
        Err(PaykitCommandError::InvoiceFinalized) => {
            let order_id: Option<Uuid> = row.payload["order_id"]
                .as_str()
                .and_then(|value| value.parse().ok());
            let mut tx = pool.begin().await?;
            let retracked = match order_id {
                Some(order_id) => {
                    track_invoice_live_after_void(&mut tx, order_id, invoice_id, now).await?
                }
                None => false,
            };
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            if retracked {
                tracing::error!(
                    row_id = row.id,
                    invoice_id = %invoice_id,
                    reason = %reason,
                    "ALERT paykit_activated_after_cancel: paykit refused the void of a published \
                     invoice; the order's request is tracked as pending"
                );
            }
            Ok(true)
        }
        Err(PaykitCommandError::StackIdentityMismatch) => {
            tracing::error!(
                row_id = row.id,
                invoice_id = %invoice_id,
                "ALERT paykit void refused with a stack identity mismatch; stamping delivered"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        Err(PaykitCommandError::ActivationTotalMismatch)
        | Err(PaykitCommandError::UnexpectedRejection(_)) => {
            tracing::error!(
                row_id = row.id,
                invoice_id = %invoice_id,
                "ALERT paykit void hit an unknown contract state; stamping delivered"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        Err(PaykitCommandError::Unavailable) => {
            tracing::warn!(
                row_id = row.id,
                invoice_id = %invoice_id,
                "paykit void unreachable; the row retries under its lease"
            );
            Ok(false)
        }
    }
}

/// One claimed pending correlation. The bundle id leaves this struct only
/// as ciphertext; a derived Debug would print bytes, not the secret.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedCorrelation {
    id: Uuid,
    payment_id: Uuid,
    creator_pubky: String,
    bundle_id_ciphertext: Vec<u8>,
    last_observed_status: Option<String>,
}

/// Claims a batch of pending correlations due for a lifecycle lookup by
/// stamping `last_checked_at`. The stamp is the only pre-effect write, so a
/// holder that dies after claiming loses nothing: the row stays `pending`
/// and is re-verified once the poll interval elapses. Only `registered`
/// rows with a non-null bundle are claimable (the same predicates as the
/// `payment_locks_correlations_pending_idx` partial index): a prepared row
/// has no bundle to look up and must never enter the batch.
async fn claim_due_correlations(
    pool: &PgPool,
    now: DateTime<Utc>,
    poll_seconds: i64,
) -> Result<Vec<ClaimedCorrelation>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE payment_locks_correlations SET last_checked_at = $1, updated_at = $1 \
         WHERE id IN (\
             SELECT id FROM payment_locks_correlations \
             WHERE verification_state = 'pending' \
             AND preparation_state = 'registered' \
             AND bundle_id_ciphertext IS NOT NULL \
             AND (last_checked_at IS NULL OR last_checked_at <= $2) \
             ORDER BY last_checked_at ASC NULLS FIRST LIMIT $3 FOR UPDATE SKIP LOCKED\
         ) RETURNING id, payment_id, creator_pubky, bundle_id_ciphertext, last_observed_status",
    )
    .bind(now)
    .bind(now - chrono::Duration::seconds(poll_seconds))
    .bind(LOCKS_VERIFY_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Appends one reconciliation-history row (append-only by trigger).
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    correlation_id: Uuid,
    observed_status: &str,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO payment_locks_observations (correlation_id, observed_status, outcome, \
         observed_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(correlation_id)
    .bind(observed_status)
    .bind(outcome)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Records a non-terminal upstream status (pending / in_progress /
/// not_found). History rows are appended only on change, so steady polling
/// does not grow the table.
async fn record_status_observation(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    observed_status: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    if row.last_observed_status.as_deref() == Some(observed_status) {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE payment_locks_correlations SET last_observed_status = $2, updated_at = $3 \
         WHERE id = $1",
    )
    .bind(row.id)
    .bind(observed_status)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    insert_observation(&mut tx, row.id, observed_status, "none", now).await?;
    tx.commit().await?;
    Ok(())
}

/// Records a terminal upstream failure (`failed`/`expired` from Locks) and
/// stops polling the lifecycle. The payment is deliberately untouched: an
/// upstream failure is not a marketplace expiry (ADR-0019 §7) — the payment
/// window moves the payment to `expired` on its own schedule.
async fn record_upstream_terminal(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    observed_status: &str,
    verification_state: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let flipped = sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = $2, \
         last_observed_status = $3, updated_at = $4 \
         WHERE id = $1 AND verification_state = 'pending'",
    )
    .bind(row.id)
    .bind(verification_state)
    .bind(observed_status)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if flipped.rows_affected() == 1 {
        insert_observation(&mut tx, row.id, observed_status, "none", now).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Marks the correlation completed inside the caller's effect transaction.
async fn mark_correlation_completed(
    tx: &mut Transaction<'_, Postgres>,
    correlation_id: Uuid,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let flipped = sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = 'completed', \
         last_observed_status = 'completed', completed_at = $2, updated_at = $2 \
         WHERE id = $1 AND verification_state = 'pending'",
    )
    .bind(correlation_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if flipped.rows_affected() == 1 {
        insert_observation(tx, correlation_id, "completed", outcome, now).await?;
    }
    Ok(())
}

/// Moves the payment to `manual_review` from `from_state` (a completion that
/// can no longer confirm the order: verified after the marketplace window,
/// or refused by the confirmation invariants). The compare-and-swap makes a
/// redelivered completion harmless, and the correlation plus its observation
/// row retain the history — a late completion is never silently discarded.
async fn apply_manual_review(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    from_state: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let updated: Option<(i64, String)> = sqlx::query_as(
        "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
         manual_review_entered_at = $3, updated_at = $3 \
         WHERE id = $1 AND state = $2 RETURNING revision, buyer_pubky",
    )
    .bind(row.payment_id)
    .bind(from_state)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let applied = match updated {
        Some((revision, buyer_pubky)) => {
            crate::executor::insert_event(
                &mut tx,
                row.id,
                &ids::payment_aggregate_id(row.payment_id),
                revision,
                &buyer_pubky,
                "payment.manual_review",
                now,
            )
            .await?;
            mark_correlation_completed(&mut tx, row.id, "manual_review", now).await?;
            true
        }
        None => {
            // The payment moved concurrently; keep the completed fact.
            mark_correlation_completed(&mut tx, row.id, "none", now).await?;
            false
        }
    };
    tx.commit().await?;
    Ok(applied)
}

/// Applies one independently verified completed Locks result. Advances
/// `awaiting_entitlement → confirmed` exactly once (payment CAS + the
/// `events_one_payment_confirmed` unique index); routes a completion that
/// arrives after marketplace expiry — or one whose order can no longer be
/// confirmed — to `manual_review`; treats an already-advanced payment as a
/// harmless duplicate.
async fn apply_completed_lifecycle(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    keys: crate::ConfirmKeys<'_>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(row.payment_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(payment) = payment else {
        anyhow::bail!("correlation {} references a missing payment", row.id);
    };

    match payment.state.as_str() {
        "awaiting_entitlement" => {
            let Some(order) = fetch_order_for_update(&mut tx, payment.order_id).await? else {
                anyhow::bail!("correlation {} references a missing order", row.id);
            };
            match confirm_order(
                &mut tx,
                &payment.buyer_pubky,
                row.id,
                &payment,
                order,
                keys,
                now,
            )
            .await?
            {
                Ok((order, _receipt, _receipt_event_id)) => {
                    let (revision,): (i64,) = sqlx::query_as(
                        "UPDATE payments SET state = 'confirmed', revision = revision + 1, \
                         updated_at = $2 WHERE id = $1 RETURNING revision",
                    )
                    .bind(payment.id)
                    .bind(now)
                    .fetch_one(&mut *tx)
                    .await?;
                    let event_id = crate::executor::insert_event(
                        &mut tx,
                        row.id,
                        &ids::payment_aggregate_id(payment.id),
                        revision,
                        &payment.buyer_pubky,
                        "payment.confirmed",
                        now,
                    )
                    .await?;
                    insert_notification_intent(
                        &mut tx,
                        event_id,
                        "payment_confirmed",
                        &order.seller_pubky,
                        &payment.buyer_pubky,
                        &ids::order_aggregate_id(order.id),
                        None,
                        now,
                    )
                    .await?;
                    mark_correlation_completed(&mut tx, row.id, "payment_confirmed", now).await?;
                    tx.commit().await?;
                    tracing::info!(
                        payment_id = %payment.id,
                        correlation_id = %row.id,
                        "confirmed payment on verified locks completion"
                    );
                    Ok(true)
                }
                Err(failure) => {
                    // The entitlement is real but the order can no longer be
                    // confirmed (e.g. a lapsed auction hold). Roll back the
                    // partial confirmation effects, then retain the fact
                    // under manual review.
                    tx.rollback().await?;
                    tracing::warn!(
                        payment_id = %payment.id,
                        correlation_id = %row.id,
                        code = ?failure.code(),
                        "verified locks completion could not confirm the order; routing to manual review"
                    );
                    apply_manual_review(pool, row, "awaiting_entitlement", now).await
                }
            }
        }
        "expired" => {
            let Some(order) = fetch_order_for_update(&mut tx, payment.order_id).await? else {
                anyhow::bail!("correlation {} references a missing order", row.id);
            };
            match apply_late_money(
                &mut tx,
                keys,
                &payment,
                &order,
                false,
                false,
                row.id,
                &payment.buyer_pubky,
                now,
            )
            .await
            {
                Ok(outcome) => {
                    let correlation_outcome = match outcome {
                        LateMoneyOutcome::Completed => "payment_confirmed",
                        LateMoneyOutcome::RefundRequired
                        | LateMoneyOutcome::HeldLateSettlement
                        | LateMoneyOutcome::SellerReview
                        | LateMoneyOutcome::AmountMismatch => "manual_review",
                    };
                    mark_correlation_completed(&mut tx, row.id, correlation_outcome, now).await?;
                    tx.commit().await?;
                    tracing::info!(
                        payment_id = %payment.id,
                        correlation_id = %row.id,
                        ?outcome,
                        "verified locks completion after the payment window; applied late-money fork"
                    );
                    Ok(true)
                }
                Err(failure) => {
                    tx.rollback().await?;
                    anyhow::bail!(
                        "late locks completion for {} failed: {:?}",
                        row.id,
                        std::mem::discriminant(&failure)
                    );
                }
            }
        }

        _ => {
            // Already confirmed or under review: a duplicate or reordered
            // completion has no further effect.
            mark_correlation_completed(&mut tx, row.id, "none", now).await?;
            tx.commit().await?;
            Ok(false)
        }
    }
}

/// Applies the independently verified lifecycle outcome for ONE claimed
/// correlation. Errors propagate to the caller's per-item handler — one
/// bad row must never stall the rest of the claimed batch.
async fn apply_locks_lookup_outcome(
    state: &AppState,
    locks: &LocksRuntime,
    row: &ClaimedCorrelation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let bundle_id = locks
        .keys
        .decrypt_bundle_id(row.payment_id, &row.bundle_id_ciphertext)
        .map_err(|error| anyhow::anyhow!("correlation {} cannot be decrypted: {error}", row.id))?;
    let outcome = locks.client.lookup(&row.creator_pubky, &bundle_id).await;
    match outcome {
        LocksLookupOutcome::Status(LocksTaskStatus::Completed) => {
            apply_completed_lifecycle(&state.pool, row, state.confirm_keys(), now).await
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Failed) => {
            record_upstream_terminal(&state.pool, row, "failed", "upstream_failed", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Expired) => {
            record_upstream_terminal(&state.pool, row, "expired", "upstream_expired", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Pending) => {
            record_status_observation(&state.pool, row, "pending", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::InProgress) => {
            record_status_observation(&state.pool, row, "in_progress", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::NotFound => {
            record_status_observation(&state.pool, row, "not_found", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Unavailable => {
            // Transport/status trouble stays pending and retryable
            // (Locks v1 has no terminal payment failure); the claim
            // stamp already deferred the next attempt.
            Ok(false)
        }
    }
}

/// One Locks verification pass: claims due pending correlations, performs
/// the independent lifecycle lookup for each, and applies the outcome.
/// Returns the number of payments advanced (confirmed or manual review).
/// A per-item failure (an undecryptable bundle id, an unopenable sealed
/// pickup row, a database error mid-effect) is logged with the correlation
/// identity and the pass CONTINUES with the rest of the claimed batch —
/// one bad row is never head-of-line for the others.
pub async fn verify_due_locks_lifecycles(
    state: &AppState,
    locks: &LocksRuntime,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let claimed = claim_due_correlations(&state.pool, now, state.config.locks_poll_seconds).await?;
    let mut applied = 0u64;
    for row in &claimed {
        match apply_locks_lookup_outcome(state, locks, row, now).await {
            Ok(true) => applied += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    correlation_id = %row.id,
                    payment_id = %row.payment_id,
                    error = %error,
                    "locks verification failed for a claimed correlation; continuing the batch"
                );
            }
        }
    }
    Ok(applied)
}

/// One claimed pending paykit-verified bitcoin order.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedPaykitOrder {
    id: Uuid,
    payment_id: Uuid,
    buyer_pubky: String,
    seller_pubky: String,
    paykit_invoice_id: Uuid,
    paykit_api: String,
    paykit_request_reference: String,
    paykit_request_state: String,
    paykit_expires_at: Option<DateTime<Utc>>,
    paykit_allocation_mode: Option<String>,
    paykit_stack_id: Option<String>,
    paykit_stack_endpoint: Option<String>,
    /// The seller's Bitcoin confirmation option frozen at the payment start
    /// (`orders.seller_confirms_bitcoin`); NULL reads as not opted in.
    seller_confirms_bitcoin: Option<bool>,
    /// Who marks the order paid, frozen at the payment start
    /// (`orders.payment_confirmation`); NULL reads as `automatic`.
    payment_confirmation: Option<String>,
}

impl ClaimedPaykitOrder {
    /// Whether every on-time matching Paykit payment of this order, Bitcoin
    /// and USDT alike, waits for the seller's attestation
    /// (`PAYMENT_CONFIRMATION=seller` at the payment start). An order on the
    /// retired fork API keeps its creator's own mode.
    fn seller_confirms_payment(&self) -> bool {
        self.paykit_api == "upstream" && self.payment_confirmation.as_deref() == Some("seller")
    }

    /// Whether this order's Bitcoin payments wait for the seller: every
    /// payment of a seller-confirmed order, or the seller's own Bitcoin
    /// opt-in on an automatic one.
    fn seller_confirms_bitcoin_payment(&self) -> bool {
        self.seller_confirms_payment()
            || (self.paykit_api == "upstream" && self.seller_confirms_bitcoin == Some(true))
    }
}

/// Claims a batch of bitcoin orders due for a paykit status poll by stamping
/// `paykit_last_checked_at` — the only pre-effect write, so a crashed holder
/// loses nothing.
async fn claim_due_paykit_orders(
    pool: &PgPool,
    now: DateTime<Utc>,
    poll_seconds: i64,
) -> Result<Vec<ClaimedPaykitOrder>, sqlx::Error> {
    // Expired payments keep polling only while money was already DETECTED
    // on-chain: a settlement confirmed after the hold window elapsed must
    // surface as manual_review, never vanish. An expired order that never
    // saw a detection stops polling — except inside the §B.9 observation
    // tail, where a LATE FIRST observation can still arrive and must route
    // to manual_review, never to awaiting_seller_confirmation. Orders in
    // `awaiting_seller_confirmation` are claimed on the status-only path:
    // the poll refreshes facts and can never transition them to `paid`.
    // An order bound through `paykit` that is already paid keeps polling for
    // its USDT finality and, until the request's observation tail ends, for
    // money on the rail that did not pay it.
    sqlx::query_as(
        "UPDATE orders SET paykit_last_checked_at = $1, updated_at = updated_at \
         WHERE id IN (\
             SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
             WHERE p.adapter = 'paykit' \
             AND ((p.state = 'awaiting_entitlement' \
                   AND o.paykit_request_state IN ('pending', 'detected', 'awaiting_seller_confirmation')) \
                  OR (p.state = 'expired' AND o.paykit_request_state = 'detected') \
                  OR (p.state = 'expired' AND o.paykit_request_state = 'pending' \
                      AND o.paykit_expires_at IS NOT NULL \
                      AND o.paykit_expires_at + INTERVAL '24 hours' > $1) \
                  OR (p.state = 'confirmed' AND o.paykit_request_state = 'confirmed' \
                      AND o.payment_method = 'paykit' AND o.paykit_api = 'upstream' \
                      AND ((o.payment_finality IN ('pending', 'reverted') \
                            AND o.payment_finality_at + INTERVAL '7 days' > $1) \
                           OR (o.paykit_second_rail_at IS NULL \
                               AND o.paykit_expires_at IS NOT NULL \
                               AND o.paykit_expires_at + INTERVAL '24 hours' > $1)))) \
             AND o.paykit_request_reference IS NOT NULL \
             AND (o.paykit_last_checked_at IS NULL OR o.paykit_last_checked_at <= $2) \
             ORDER BY o.paykit_last_checked_at ASC NULLS FIRST LIMIT $3 \
             FOR UPDATE OF o SKIP LOCKED\
         ) RETURNING id, payment_id, buyer_pubky, seller_pubky, paykit_invoice_id, paykit_api, \
         paykit_request_reference, paykit_request_state, paykit_expires_at, paykit_allocation_mode, \
         paykit_stack_id, paykit_stack_endpoint, seller_confirms_bitcoin, payment_confirmation",
    )
    .bind(now)
    .bind(now - chrono::Duration::seconds(poll_seconds))
    .bind(PAYKIT_VERIFY_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Whether the observed amount differs from what the order offered. A
/// satoshi observation is compared only where the marketplace froze the
/// figure at bind (an FX-quoted order: `paykit_total_sats`). A USDT
/// observation is compared with the USDT amount paykit-server offered for the
/// request, and an observation that names no amount is never a mismatch here
/// (paykit-server's own `amount_matched` still decides).
pub(crate) fn observed_differs_from_offer(
    order: &crate::model::OrderRow,
    observation: &crate::payments::PaykitObservation,
) -> bool {
    match observation.asset {
        Some(AcceptedAsset::Usdt) => {
            let offered = order
                .paykit_payment_amounts
                .as_ref()
                .and_then(OfferedAssets::from_json)
                .and_then(|offered| offered.usdt_units);
            observation
                .observed_units
                .is_some_and(|observed| Some(observed) != offered)
        }
        _ => {
            order.bitcoin_quoted_sats.is_some()
                && observation
                    .observed_sats
                    .is_some_and(|observed| i64::try_from(observed).ok() != order.paykit_total_sats)
        }
    }
}

/// What one confirmed observation says about the money: whether the amount
/// matched, what was observed, whether paykit-server marked it late, and the
/// USDT leg when the observation is one.
#[derive(Clone, Copy)]
struct Settlement<'a> {
    amount_matched: bool,
    observation: &'a crate::payments::PaykitObservation,
    late_settlement: bool,
    usdt: Option<&'a PaykitUsdtObservation>,
}

impl<'a> Settlement<'a> {
    fn bitcoin(
        amount_matched: bool,
        observation: &'a crate::payments::PaykitObservation,
        late_settlement: bool,
    ) -> Self {
        Self {
            amount_matched,
            observation,
            late_settlement,
            usdt: None,
        }
    }
}

/// Records the rail that paid an order bound through `paykit`: the S1 asset
/// terms (`payment_asset`, network, amount, exponent, basis), the payment
/// row retyped to that asset, and, for USDT, the finality of the transfer.
/// Written once, under the caller's payment and order locks, by the first
/// settlement or review entry that sees money, so a later poll never moves
/// it. An order bound through the legacy `bitcoin` bind keeps its legacy
/// shape and records nothing.
async fn record_settlement_rail(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    observation: &crate::payments::PaykitObservation,
    usdt: Option<&PaykitUsdtObservation>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let Some(order) = fetch_order_for_update(tx, order_id).await? else {
        anyhow::bail!("paykit order {order_id} is missing while recording its rail");
    };
    if order.payment_method.as_deref() != Some("paykit") {
        return Ok(());
    }
    let asset = observation.asset.unwrap_or(AcceptedAsset::Btc);
    // A `paykit` order bound through the fork carries no denomination and no
    // request terms by design: its rail is Bitcoin and nothing is recorded.
    if order.paykit_asset.is_none() && order.paykit_amount_minor.is_none() {
        return Ok(());
    }
    let Some(denomination) = order
        .paykit_asset
        .as_deref()
        .and_then(PaymentAsset::from_column)
    else {
        tracing::error!(
            order_id = %order.id,
            paykit_asset = ?order.paykit_asset,
            "ALERT paykit order carries a denomination this build does not know; \
             the paid rail is not recorded"
        );
        return Ok(());
    };
    let offered = order
        .paykit_payment_amounts
        .as_ref()
        .and_then(OfferedAssets::from_json)
        .or_else(|| {
            order
                .paykit_total_sats
                .and_then(|sats| u64::try_from(sats).ok())
                .map(|btc_sats| OfferedAssets {
                    btc_sats: Some(btc_sats),
                    usdt_units: None,
                })
        });
    let observed = match asset {
        AcceptedAsset::Btc => observation.observed_sats,
        AcceptedAsset::Usdt => observation.observed_units,
    };
    let Some(amount) = observed
        .or_else(|| offered.and_then(|offered| offered.amount(asset)))
        .and_then(|amount| i64::try_from(amount).ok())
    else {
        return Ok(());
    };
    let Some(terms) = PaymentTerms::settled(denomination, asset, amount) else {
        return Ok(());
    };
    if terms.store(tx, order_id).await? && asset == AcceptedAsset::Usdt {
        sqlx::query(
            "UPDATE payments SET amount_minor = $2, currency = 'USDT', exponent = 6, \
             updated_at = $3 WHERE order_id = $1",
        )
        .bind(order_id)
        .bind(amount)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    }
    if let Some(usdt) = usdt {
        set_payment_finality(tx, order_id, finality_of(usdt), now).await?;
    }
    Ok(())
}

/// The finality an included USDT transfer reports.
fn finality_of(usdt: &PaykitUsdtObservation) -> &'static str {
    if usdt.finalized {
        "final"
    } else {
        "pending"
    }
}

/// Moves a USDT payment's finality. `final` is terminal: a finalized transfer
/// cannot be reverted by a later poll, whatever the status then says.
/// Returns whether the value changed. A change bumps the order revision and
/// records one event, so clients that follow revisions see it.
async fn set_payment_finality(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    finality: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let updated: Option<(i64,)> = sqlx::query_as(
        "UPDATE orders SET payment_finality = $2, payment_finality_at = $3, \
         revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND payment_asset = 'USDT' \
         AND payment_finality IS DISTINCT FROM 'final' \
         AND payment_finality IS DISTINCT FROM $2 \
         RETURNING revision",
    )
    .bind(order_id)
    .bind(finality)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((revision,)) = updated else {
        return Ok(false);
    };
    crate::executor::insert_event(
        tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order_id),
        revision,
        SYSTEM_ACTOR,
        "payment.finality_changed",
        now,
    )
    .await?;
    if finality == "reverted" {
        tracing::error!(
            order_id = %order_id,
            code = "usdt_payment_reverted",
            "ALERT an included USDT transfer disappeared before it was final; \
             physical fulfilment stays blocked"
        );
    }
    Ok(true)
}

/// Applies one confirmed paykit payment: `awaiting_entitlement → confirmed`
/// exactly once via the shared confirmation effects; a confirmation the
/// order can no longer accept (or an amount mismatch paykit observed) routes
/// the payment to `manual_review`. A LATE entry — the payment already
/// expired, OR the producer's mandatory `late_settlement` flag marking a
/// settlement outside the invoice's window — takes the late-money fork
/// (`apply_late_money`) with the observation frozen onto the order first, so
/// a later resolution's audit snapshot carries the facts the seller acted
/// on. A late amount mismatch never pays: it enters the same
/// `amount_mismatch` `manual_review` as on-time money, before any unit is
/// re-taken. Only an exact late payment can complete a still-free order
/// (`late_completion`), and on a seller-confirmed order not even that: it
/// waits as `late_settlement` for the seller. A unit that is gone is named
/// `refund_required`, and a still-held order waits as `late_settlement`.
async fn freeze_paykit_observation(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    observed_sats: Option<u64>,
) -> anyhow::Result<()> {
    let Some(observed_sats) = observed_sats else {
        return Ok(());
    };
    sqlx::query(
        "UPDATE orders SET paykit_observed_sats = $2 \
         WHERE id = $1 AND paykit_observed_sats IS NULL",
    )
    .bind(order_id)
    .bind(i64::try_from(observed_sats)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn apply_confirmed_paykit_payment(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    facts: Settlement<'_>,
    keys: crate::ConfirmKeys<'_>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let Settlement {
        amount_matched,
        observation,
        late_settlement,
        usdt,
    } = facts;
    let resolution_pins_present = match row.paykit_api.as_str() {
        "fork" => row.paykit_stack_id.is_some() && row.paykit_stack_endpoint.is_some(),
        "upstream" => row.paykit_stack_id.is_none() && row.paykit_stack_endpoint.is_none(),
        _ => false,
    };
    let mut tx = pool.begin().await?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(row.payment_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(payment) = payment else {
        anyhow::bail!("paykit order {} references a missing payment", row.id);
    };
    // The seller gate, rechecked under the order lock: the caller routed on
    // the claim-time state, and a waiting order leaves only through its
    // seller or the seller-window reaper.
    let request_state: Option<(Option<String>,)> =
        sqlx::query_as("SELECT paykit_request_state FROM orders WHERE id = $1 FOR UPDATE")
            .bind(row.id)
            .fetch_optional(&mut *tx)
            .await?;
    if matches!(
        request_state,
        Some((Some(ref state),)) if state == "awaiting_seller_confirmation"
    ) {
        tx.rollback().await?;
        tracing::warn!(
            order_id = %row.id,
            "refused an automatic paykit settlement for an order awaiting seller confirmation"
        );
        return Ok(false);
    }
    // The rail that took the money is recorded first, under the payment and
    // order locks, so every exit below (settled, late, mismatched, or
    // routed to review) carries the paid asset and amount, and a refund
    // follows the rail that was paid.
    record_settlement_rail(&mut tx, row.id, observation, usdt, now).await?;
    // An on-time settlement first seen after the reservation sweep returned
    // an auction winner's unit: the plain confirmation refuses a lapsed
    // hold, so it takes the late-money fork, which re-takes the unit when it
    // is still free and otherwise names `refund_required`. A mismatched
    // amount keeps its own `amount_mismatch` review below.
    let lapsed_auction_hold = if payment.state == "awaiting_entitlement" && amount_matched {
        match fetch_order_for_update(&mut tx, row.id).await? {
            Some(order) => {
                let quote_mismatch = observed_differs_from_offer(&order, observation);
                !quote_mismatch
                    && crate::bitcoin_review::auction_hold_lapsed(&mut tx, &order).await?
            }
            None => false,
        }
    } else {
        false
    };
    if payment.state == "expired"
        || (late_settlement && payment.state == "awaiting_entitlement")
        || lapsed_auction_hold
    {
        if !resolution_pins_present {
            tx.rollback().await?;
            tracing::error!(
                order_id = %row.id,
                code = "paykit_resolution_pins_missing",
                "blocked late paykit manual-review entry for an unpinned legacy order"
            );
            return Ok(false);
        }
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', \
             paykit_seller_confirmation_entered_at = NULL, \
             paykit_seller_confirmation_deadline = NULL, \
             paykit_observation = $3, updated_at = $2 WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .bind(crate::bitcoin_review::observation_json(
            "confirmed",
            amount_matched,
            observation,
            now,
            false,
        ))
        .execute(&mut *tx)
        .await?;
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
        let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
            anyhow::bail!(
                "paykit order {} is missing after observation freeze",
                row.id
            );
        };
        let amount_mismatch = !amount_matched || observed_differs_from_offer(&order, observation);
        match apply_late_money(
            &mut tx,
            keys,
            &payment,
            &order,
            amount_mismatch,
            order.seller_confirms_paykit_payment(),
            row.id,
            &row.buyer_pubky,
            now,
        )
        .await
        {
            Ok(outcome) => {
                tx.commit().await?;
                tracing::warn!(
                    order_id = %row.id,
                    late_settlement,
                    local_expired = payment.state == "expired",
                    ?outcome,
                    "applied late-money fork for paykit settlement"
                );
                return Ok(true);
            }
            Err(failure) => {
                tx.rollback().await?;
                anyhow::bail!(
                    "late paykit settlement for {} failed: {:?}",
                    row.id,
                    std::mem::discriminant(&failure)
                );
            }
        }
    }
    if payment.state != "awaiting_entitlement" {
        tx.rollback().await?;
        return Ok(false);
    }
    if !amount_matched {
        // Money arrived but not the required amount: never silently confirm.
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             review_reason = 'amount_mismatch', manual_review_entered_at = $2, updated_at = $2 \
             WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        let event_id = crate::executor::insert_event(
            &mut tx,
            row.id,
            &ids::payment_aggregate_id(payment.id),
            revision,
            &row.buyer_pubky,
            "payment.manual_review",
            now,
        )
        .await?;
        // An unpinned legacy order cannot take a seller resolution.
        if resolution_pins_present {
            crate::handlers::insert_bitcoin_manual_review_intent(
                &mut tx,
                event_id,
                &row.seller_pubky,
                row.id,
                crate::handlers::BitcoinReviewNotice::AmountMismatch,
                now,
            )
            .await?;
        }
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', \
             paykit_seller_confirmation_entered_at = NULL, \
             paykit_seller_confirmation_deadline = NULL, \
             updated_at = $2 WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %row.id,
            "confirmed paykit payment did not match the required amount; routing to manual review"
        );
        return Ok(true);
    }
    let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
        anyhow::bail!("paykit order {} is missing", row.id);
    };
    // Exact-settlement enforcement applies only where the marketplace itself
    // froze the satoshi figure at bind (an FX-quoted order): the buyer was
    // invoiced `paykit_total_sats` (quote + nonce), so anything else the
    // observer reports is an under/over-payment a human must review. For
    // legacy SAT/BTC orders the invoice amount is the order total and
    // paykit-server's own `amount_matched` (handled above) remains the sole
    // amount arbiter — a later observation never un-pays through this path.
    let observed_amount_mismatch = observed_differs_from_offer(&order, observation);
    if observed_amount_mismatch {
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             review_reason = 'amount_mismatch', manual_review_entered_at = $2, updated_at = $2 \
             WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        let event_id = crate::executor::insert_event(
            &mut tx,
            row.id,
            &ids::payment_aggregate_id(payment.id),
            revision,
            &row.buyer_pubky,
            "payment.manual_review",
            now,
        )
        .await?;
        // An unpinned legacy order cannot take a seller resolution.
        if resolution_pins_present {
            crate::handlers::insert_bitcoin_manual_review_intent(
                &mut tx,
                event_id,
                &row.seller_pubky,
                row.id,
                crate::handlers::BitcoinReviewNotice::AmountMismatch,
                now,
            )
            .await?;
        }
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', \
             paykit_seller_confirmation_entered_at = NULL, \
             paykit_seller_confirmation_deadline = NULL, paykit_observation = $3, updated_at = $2 \
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .bind(crate::bitcoin_review::observation_json(
            "confirmed",
            amount_matched,
            observation,
            now,
            false,
        ))
        .execute(&mut *tx)
        .await?;
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %row.id,
            "confirmed paykit payment observed amount differs from the bind quote; routing to manual review"
        );
        return Ok(true);
    }
    // A digital order the seller can no longer deliver never takes a
    // receipt: the money routes to a seller refund and the observation is
    // consumed, so no retry sits on money already taken (§6 D3).
    if crate::handlers::digital_orders::unpinnable(&mut tx, &order).await? {
        crate::bitcoin_review::refund_required(
            &mut tx,
            &payment,
            &order,
            "digital delivery unavailable at payment",
            row.id,
            &row.buyer_pubky,
            now,
        )
        .await
        .map_err(|failure| {
            anyhow::anyhow!(
                "digital refund route for {} failed: {:?}",
                row.id,
                std::mem::discriminant(&failure)
            )
        })?;
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', \
             paykit_seller_confirmation_entered_at = NULL, \
             paykit_seller_confirmation_deadline = NULL, paykit_observation = $3, updated_at = $2 \
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .bind(crate::bitcoin_review::observation_json(
            "confirmed",
            amount_matched,
            observation,
            now,
            false,
        ))
        .execute(&mut *tx)
        .await?;
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %row.id,
            "paykit payment confirmed for a digital order with no deliverable; refund required"
        );
        return Ok(true);
    }
    match confirm_order(
        &mut tx,
        &row.buyer_pubky,
        row.id,
        &payment,
        order,
        keys,
        now,
    )
    .await?
    {
        Ok((order, _receipt, _receipt_event_id)) => {
            let (revision,): (i64,) = sqlx::query_as(
                "UPDATE payments SET state = 'confirmed', revision = revision + 1, \
                 updated_at = $2 WHERE id = $1 RETURNING revision",
            )
            .bind(payment.id)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;
            let event_id = crate::executor::insert_event(
                &mut tx,
                row.id,
                &ids::payment_aggregate_id(payment.id),
                revision,
                &row.buyer_pubky,
                "payment.confirmed",
                now,
            )
            .await?;
            insert_notification_intent(
                &mut tx,
                event_id,
                "payment_confirmed",
                &order.seller_pubky,
                &row.buyer_pubky,
                &ids::order_aggregate_id(order.id),
                None,
                now,
            )
            .await?;
            sqlx::query(
                "UPDATE orders SET paykit_request_state = 'confirmed', \
                 paykit_seller_confirmation_entered_at = NULL, \
                 paykit_seller_confirmation_deadline = NULL, paykit_observation = $3, updated_at = $2 \
                 WHERE id = $1",
            )
            .bind(row.id)
            .bind(now)
            .bind(crate::bitcoin_review::observation_json(
                "confirmed",
                amount_matched,
                observation,
                now,
                false,
            ))
            .execute(&mut *tx)
            .await?;
            freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
            tx.commit().await?;
            tracing::info!(
                order_id = %row.id,
                "confirmed payment on observed paykit settlement"
            );
            Ok(true)
        }
        Err(failure) => {
            tx.rollback().await?;
            tracing::warn!(
                order_id = %row.id,
                code = ?failure.code(),
                "confirmed paykit payment could not confirm the order; routing to manual review"
            );
            let mut tx = pool.begin().await?;
            record_settlement_rail(&mut tx, row.id, observation, usdt, now).await?;
            let updated: Option<(i64,)> = sqlx::query_as(
                "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
                 manual_review_entered_at = $2, updated_at = $2 \
                 WHERE id = $1 AND state = 'awaiting_entitlement' \
                 RETURNING revision",
            )
            .bind(payment.id)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some((revision,)) = updated {
                let event_id = crate::executor::insert_event(
                    &mut tx,
                    row.id,
                    &ids::payment_aggregate_id(payment.id),
                    revision,
                    &row.buyer_pubky,
                    "payment.manual_review",
                    now,
                )
                .await?;
                // An unpinned legacy order cannot take a seller resolution.
                if resolution_pins_present {
                    crate::handlers::insert_bitcoin_manual_review_intent(
                        &mut tx,
                        event_id,
                        &row.seller_pubky,
                        row.id,
                        crate::handlers::BitcoinReviewNotice::ConfirmationFailed,
                        now,
                    )
                    .await?;
                }
            }
            sqlx::query(
                "UPDATE orders SET paykit_request_state = 'confirmed', \
                 paykit_seller_confirmation_entered_at = NULL, \
                 paykit_seller_confirmation_deadline = NULL, \
                 paykit_observation = $3, updated_at = $2 \
                 WHERE id = $1",
            )
            .bind(row.id)
            .bind(now)
            .bind(crate::bitcoin_review::observation_json(
                "confirmed",
                amount_matched,
                observation,
                now,
                false,
            ))
            .execute(&mut *tx)
            .await?;
            freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
            tx.commit().await?;
            Ok(updated.is_some())
        }
    }
}

/// The display-only `detected` flip: an on-chain detection advances the
/// buyer-facing request state and nothing else — no payment authority.
async fn mark_paykit_detected(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    if row.paykit_request_state != "detected" {
        sqlx::query(MARK_PAYKIT_DETECTED)
            .bind(row.id)
            .bind(now)
            .execute(pool)
            .await?;
    }
    Ok(false)
}

/// Applies one matching observation for a `shared_manual` order (§B.8.8):
/// entry to `awaiting_seller_confirmation` with the hold extended to the
/// 24-hour seller-confirmation window, or a status-only fact refresh once
/// inside. This path NEVER transitions the order to `paid` — the seller's
/// attestation is the only exit — and a late observation (payment already
/// expired) never enters the state; it takes the existing late path.
#[allow(clippy::too_many_arguments)]
async fn apply_shared_manual_observation(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    observed_state: &str,
    amount_matched: bool,
    observation: &crate::payments::PaykitObservation,
    usdt: Option<&PaykitUsdtObservation>,
    keys: crate::ConfirmKeys<'_>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let settlement = |late_settlement| Settlement {
        amount_matched,
        observation,
        late_settlement,
        usdt,
    };
    // A fork order resolves through the pinned stack; an upstream order has
    // no stack and carries neither pin.
    let resolution_pins_present = match row.paykit_api.as_str() {
        "fork" => row.paykit_stack_id.is_some() && row.paykit_stack_endpoint.is_some(),
        "upstream" => row.paykit_stack_id.is_none() && row.paykit_stack_endpoint.is_none(),
        _ => false,
    };
    if !resolution_pins_present {
        tracing::error!(
            order_id = %row.id,
            code = "paykit_resolution_pins_missing",
            "blocked shared_manual seller-confirmation entry for an unpinned legacy order"
        );
        return Ok(false);
    }
    let mut tx = pool.begin().await?;
    // Canonical lock order: payment, then order.
    let payment_state: Option<(String,)> =
        sqlx::query_as("SELECT state FROM payments WHERE id = $1 FOR UPDATE")
            .bind(row.payment_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((payment_state,)) = payment_state else {
        anyhow::bail!("paykit order {} references a missing payment", row.id);
    };
    if payment_state != "awaiting_entitlement" {
        // Late settlement (or an already-advanced payment): the
        // `late_settlement` edge fires and the
        // `awaiting_seller_confirmation` edge does not (Kimi P2). The
        // existing late path routes a confirmed observation to
        // manual_review; a detected one only advances the display state.
        tx.rollback().await?;
        if observed_state == "confirmed" {
            return apply_confirmed_paykit_payment(pool, row, settlement(false), keys, now).await;
        }
        if row.paykit_request_state != "detected" {
            sqlx::query(MARK_PAYKIT_DETECTED)
                .bind(row.id)
                .bind(now)
                .execute(pool)
                .await?;
        }
        return Ok(false);
    }
    let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
        anyhow::bail!("paykit order {} is missing", row.id);
    };
    // A USDT amount that differs from what paykit-server offered is never
    // shown to the seller as waiting: it takes the amount-mismatch review.
    if usdt.is_some() && observed_differs_from_offer(&order, observation) {
        tx.rollback().await?;
        return apply_confirmed_paykit_payment(pool, row, settlement(false), keys, now).await;
    }
    let observation_doc = crate::bitcoin_review::observation_json(
        observed_state,
        amount_matched,
        observation,
        now,
        false,
    );
    let deadline =
        now + chrono::Duration::seconds(crate::bitcoin_review::SELLER_CONFIRMATION_WINDOW_SECONDS);
    // The entry edge: one conditional UPDATE from the non-late matching
    // observation. The hold is EXTENDED to the 24-hour window, not expired
    // (a buyer who demonstrably paid must not lose stock to a 3600 s
    // timeout while waiting for a human).
    let entered: Option<(i64,)> = sqlx::query_as(
        "UPDATE orders SET paykit_request_state = 'awaiting_seller_confirmation', \
         paykit_observation = $2, paykit_seller_confirmation_entered_at = $3, \
         paykit_seller_confirmation_deadline = $4, \
         hold_expires_at = CASE WHEN auction_aggregate_id IS NULL THEN $4 ELSE hold_expires_at END, \
         revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND paykit_request_state IN ('pending', 'detected') \
         RETURNING revision",
    )
    .bind(row.id)
    .bind(&observation_doc)
    .bind(now)
    .bind(deadline)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((revision,)) = entered {
        // The rail the money arrived on is recorded at entry, so the seller's
        // confirmation, a review and a refund all follow it, and a USDT
        // transfer's finality keeps being tracked while the seller decides.
        record_settlement_rail(&mut tx, row.id, observation, usdt, now).await?;
        // The seller is the only exit to `paid`: tell them the payment was
        // seen and that it waits for their confirmation.
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &ids::order_aggregate_id(row.id),
            revision,
            SYSTEM_ACTOR,
            "payment.awaiting_seller_confirmation",
            now,
        )
        .await?;
        let seen_kind = if usdt.is_some() {
            "usdt_payment_seen"
        } else {
            "bitcoin_payment_seen"
        };
        insert_notification_intent(
            &mut tx,
            event_id,
            seen_kind,
            &order.seller_pubky,
            SYSTEM_ACTOR,
            &ids::order_aggregate_id(row.id),
            None,
            now,
        )
        .await?;
        // A1: the FIRST authority-establishing observation is frozen onto
        // the order in the same transaction as the state change. Later
        // status refreshes update the JSON facts only; the seller-window
        // reaper's COALESCE freeze remains a fallback for rows that entered
        // before this freeze existed and can never win against this value.
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
        // An auction order's hold is the winning reservation: extend it to
        // the same window so the sweep cannot release it mid-confirmation.
        if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
            sqlx::query(
                "UPDATE reservations SET expires_at = $3, updated_at = $3 \
                 WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 AND status = 'active'",
            )
            .bind(auction_aggregate_id)
            .bind(&order.buyer_pubky)
            .bind(deadline)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        tracing::info!(
            order_id = %row.id,
            observed_state,
            "shared_manual observation entered awaiting_seller_confirmation (hold extended 24h)"
        );
        return Ok(true);
    }
    refresh_seller_confirmation_facts(&mut tx, row, &observation_doc, observation, now).await?;
    tx.commit().await?;
    Ok(false)
}

/// The status-only path for an order already inside
/// `awaiting_seller_confirmation`: refreshes the live facts (confirmations
/// progress, a refreshed observation after a disappearance) without ever
/// advancing the order and without touching the observation frozen at
/// entry (A1). The caller holds the payment row lock. Returns whether the
/// order was still inside.
async fn refresh_seller_confirmation_facts(
    tx: &mut Transaction<'_, Postgres>,
    row: &ClaimedPaykitOrder,
    observation_doc: &Value,
    observation: &crate::payments::PaykitObservation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let refreshed = sqlx::query(
        "UPDATE orders SET paykit_observation = $2, updated_at = $3 \
         WHERE id = $1 AND paykit_request_state = 'awaiting_seller_confirmation'",
    )
    .bind(row.id)
    .bind(observation_doc)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if refreshed.rows_affected() != 1 {
        return Ok(false);
    }
    if let Some(confirmations) = observation.confirmations {
        sqlx::query("UPDATE payments SET confirmations = $2, updated_at = $3 WHERE id = $1")
            .bind(row.payment_id)
            .bind(i32::try_from(confirmations).unwrap_or(i32::MAX))
            .bind(now)
            .execute(&mut **tx)
            .await?;
    }
    Ok(true)
}

/// Applies one Paykit report to an order inside
/// `awaiting_seller_confirmation`. Once the seller gate is armed no report
/// pays the order or routes it to review: not an `exclusive` mode Paykit
/// now reports, not a late flag (Paykit judges lateness against the
/// invoice's own expiry, which the 24-hour window outlives; entry happened
/// on an on-time sighting), not an amount mismatch. Detected and confirmed
/// reports refresh the facts; a disappearance is marked. The seller's
/// attestation and the seller-window reaper stay the only exits. A report
/// that finds the order already gone from the state changes nothing, and
/// the next poll reads the order afresh.
async fn apply_report_inside_seller_window(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    outcome: &PaykitStatusOutcome,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let (observed_state, amount_matched, facts) = match outcome {
        PaykitStatusOutcome::Confirmed {
            amount_matched,
            facts,
        } => ("confirmed", *amount_matched, facts),
        PaykitStatusOutcome::AcceptancePending { facts } => ("confirmed", true, facts),
        PaykitStatusOutcome::Detected { facts } => {
            ("detected", facts.amount_matched.unwrap_or(true), facts)
        }
        PaykitStatusOutcome::UsdtObserved { usdt, .. } => {
            return refresh_usdt_inside_seller_window(pool, row, usdt, now).await
        }
        PaykitStatusOutcome::ManualReview { .. } => return Ok(false),
        PaykitStatusOutcome::Undetected | PaykitStatusOutcome::ActiveUndetected { .. } => {
            return mark_shared_manual_disappeared(pool, row, now).await
        }
        PaykitStatusOutcome::NotFound | PaykitStatusOutcome::Unavailable => return Ok(false),
    };
    if !matches!(
        facts.allocation_mode.as_str(),
        "shared_manual" | "exclusive"
    ) {
        tracing::warn!(
            order_id = %row.id,
            allocation_mode = %facts.allocation_mode,
            "paykit status reported an unknown allocation_mode; failing closed"
        );
        return Ok(false);
    }
    refresh_inside_seller_window(
        pool,
        row,
        observed_state,
        amount_matched,
        &facts.observation,
        now,
    )
    .await?;
    Ok(false)
}

/// Refreshes an order waiting for its seller from one report, under the
/// canonical payment-then-order lock. Returns false, having changed
/// nothing, when the payment or the order is no longer waiting.
async fn refresh_inside_seller_window(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    observed_state: &str,
    amount_matched: bool,
    observation: &crate::payments::PaykitObservation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let payment_state: Option<(String,)> =
        sqlx::query_as("SELECT state FROM payments WHERE id = $1 FOR UPDATE")
            .bind(row.payment_id)
            .fetch_optional(&mut *tx)
            .await?;
    if payment_state.as_ref().map(|(state,)| state.as_str()) != Some("awaiting_entitlement") {
        tx.rollback().await?;
        return Ok(false);
    }
    let observation_doc = crate::bitcoin_review::observation_json(
        observed_state,
        amount_matched,
        observation,
        now,
        false,
    );
    if !refresh_seller_confirmation_facts(&mut tx, row, &observation_doc, observation, now).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    tx.commit().await?;
    tracing::info!(
        order_id = %row.id,
        observed_state,
        "paykit report inside the seller-confirmation window; refreshed facts only"
    );
    Ok(true)
}

/// A USDT report for an order waiting for its seller. An order that entered
/// on its USDT transfer refreshes the transfer's facts and finality, under the
/// canonical payment-then-order lock; a transfer the report no longer shows
/// in a block is marked `disappeared` and its finality `reverted`. Nothing
/// advances the order. An order that entered on Bitcoin ignores the USDT leg
/// here: once paid, the settled poll holds second-rail money for review.
async fn refresh_usdt_inside_seller_window(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    usdt: &PaykitUsdtObservation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let payment_state: Option<(String,)> =
        sqlx::query_as("SELECT state FROM payments WHERE id = $1 FOR UPDATE")
            .bind(row.payment_id)
            .fetch_optional(&mut *tx)
            .await?;
    if payment_state.as_ref().map(|(state,)| state.as_str()) != Some("awaiting_entitlement") {
        tx.rollback().await?;
        return Ok(false);
    }
    let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
        anyhow::bail!("paykit order {} is missing", row.id);
    };
    if order.payment_asset.as_deref() != Some("USDT") {
        tx.rollback().await?;
        return Ok(false);
    }
    let observation = usdt.as_observation();
    let observation_doc = crate::bitcoin_review::observation_json(
        "confirmed",
        usdt.amount_matched,
        &observation,
        now,
        !usdt.included(),
    );
    if !refresh_seller_confirmation_facts(&mut tx, row, &observation_doc, &observation, now).await?
    {
        tx.rollback().await?;
        return Ok(false);
    }
    let finality = if usdt.included() {
        finality_of(usdt)
    } else {
        "reverted"
    };
    set_payment_finality(&mut tx, row.id, finality, now).await?;
    tx.commit().await?;
    Ok(true)
}

/// A disappearance/reorg fact for an order awaiting seller confirmation:
/// the observation is marked `disappeared` and shown to the seller; the
/// order does not silently revert and does not advance (Q10). A USDT
/// transfer that is no longer reported also has its finality `reverted`.
async fn mark_shared_manual_disappeared(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let marked = sqlx::query(
        "UPDATE orders SET paykit_observation = jsonb_set(paykit_observation, '{disappeared}', 'true'), \
         updated_at = $2 \
         WHERE id = $1 AND paykit_request_state = 'awaiting_seller_confirmation' \
         AND paykit_observation IS NOT NULL",
    )
    .bind(row.id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if marked.rows_affected() == 1 {
        set_payment_finality(&mut tx, row.id, "reverted", now).await?;
    }
    tx.commit().await?;
    Ok(false)
}

/// Producer lifecycle corruption is never payment authority. Stop automatic
/// handling, preserve the current hold, and surface one durable review.
async fn apply_upstream_manual_review(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    reason: &str,
    observation: Option<&crate::payments::PaykitObservation>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let updated: Option<(i64,)> = sqlx::query_as(
        "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
         review_reason = 'upstream_inconsistent', manual_review_entered_at = $2, updated_at = $2 \
         WHERE id = $1 AND state IN ('awaiting_entitlement', 'expired') RETURNING revision",
    )
    .bind(row.payment_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((revision,)) = updated else {
        tx.rollback().await?;
        return Ok(false);
    };
    let event_id = crate::executor::insert_event(
        &mut tx,
        row.id,
        &ids::payment_aggregate_id(row.payment_id),
        revision,
        &row.buyer_pubky,
        "payment.manual_review",
        now,
    )
    .await?;
    crate::handlers::insert_bitcoin_manual_review_intent(
        &mut tx,
        event_id,
        &row.seller_pubky,
        row.id,
        crate::handlers::BitcoinReviewNotice::ConfirmationFailed,
        now,
    )
    .await?;
    let observation_json = observation.map(|facts| {
        crate::bitcoin_review::observation_json("confirmed", false, facts, now, false)
    });
    sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed', \
         paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, \
         paykit_observation = COALESCE($3, paykit_observation), updated_at = $2 WHERE id = $1",
    )
    .bind(row.id)
    .bind(now)
    .bind(observation_json)
    .execute(&mut *tx)
    .await?;
    if let Some(observation) = observation {
        freeze_paykit_observation(&mut tx, row.id, observation.observed_sats).await?;
    }
    tx.commit().await?;
    tracing::error!(
        order_id = %row.id,
        %reason,
        "ALERT upstream paykit lifecycle routed payment to manual review"
    );
    Ok(true)
}

/// Seller confirmation of Bitcoin payments. Upstream Paykit reports every
/// order as `exclusive`, so a Bitcoin payment on time would pay the order by
/// itself. For an order frozen as seller-confirmed at its payment start
/// (`orders.payment_confirmation = 'seller'`, or the seller's own opt-in in
/// `orders.seller_confirms_bitcoin` on an automatic order) the report is read
/// as `shared_manual` instead: on-time matching money enters the existing
/// seller-confirmation window and only the seller's attestation pays the
/// order, and Paykit's report only informs the seller. A late settlement still
/// takes the late path whatever the mode. The USDT leg is handled by
/// [`apply_usdt_observation`], and an automatic order whose seller did not opt
/// in (or whose option was never read) is untouched.
///
/// Only money that matches the order's amount waits for the seller. A
/// `confirmed` report whose amount does not match keeps the `exclusive` mode, so
/// it takes the same route as for any other seller (durable manual review with
/// the amount-mismatch reason), and an unmatched `detected` sighting stays a
/// display-only detection. The seller is never shown a short payment as
/// "waiting for your confirmation".
fn with_seller_confirmation(
    outcome: PaykitStatusOutcome,
    row: &ClaimedPaykitOrder,
) -> PaykitStatusOutcome {
    if !row.seller_confirms_bitcoin_payment() {
        return outcome;
    }
    fn shared(mut facts: crate::payments::PaykitStatusFacts) -> crate::payments::PaykitStatusFacts {
        if facts.allocation_mode == "exclusive" {
            facts.allocation_mode = "shared_manual".to_string();
        }
        facts
    }
    match outcome {
        PaykitStatusOutcome::Confirmed {
            amount_matched,
            facts,
        } => PaykitStatusOutcome::Confirmed {
            amount_matched,
            facts: if amount_matched { shared(facts) } else { facts },
        },
        PaykitStatusOutcome::AcceptancePending { facts } => {
            PaykitStatusOutcome::AcceptancePending {
                facts: shared(facts),
            }
        }
        PaykitStatusOutcome::Detected { facts } => PaykitStatusOutcome::Detected {
            facts: if facts.amount_matched == Some(false) {
                facts
            } else {
                shared(facts)
            },
        },
        PaykitStatusOutcome::UsdtObserved {
            usdt,
            bitcoin,
            payment_deadline,
            paid_asset,
        } => PaykitStatusOutcome::UsdtObserved {
            usdt,
            bitcoin: Box::new(with_seller_confirmation(*bitcoin, row)),
            payment_deadline,
            paid_asset,
        },
        other => other,
    }
}

/// Whether the order's payments wait for the seller: a fork order the
/// creator's mode pinned to `shared_manual`, or an upstream order frozen as
/// seller-confirmed (or whose seller opted in for Bitcoin).
fn waits_for_seller(row: &ClaimedPaykitOrder) -> bool {
    row.paykit_allocation_mode.as_deref() == Some("shared_manual")
        || row.seller_confirms_bitcoin_payment()
}

/// Applies the polled paykit status for ONE claimed order. Errors
/// propagate to the caller's per-item handler — one bad row must never
/// stall the rest of the claimed batch.
async fn apply_paykit_status_outcome(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    row: &ClaimedPaykitOrder,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let api = match row.paykit_api.as_str() {
        "fork" => crate::payments::PaykitApi::Fork,
        "upstream" => crate::payments::PaykitApi::Upstream,
        _ => return Ok(false),
    };
    let (outcome, delivery) = source
        .status_for(
            &row.seller_pubky,
            &row.paykit_request_reference,
            api,
            row.paykit_invoice_id,
            row.paykit_expires_at,
        )
        .await;
    let outcome = with_seller_confirmation(outcome, row);
    let recovered_deadline = match &outcome {
        PaykitStatusOutcome::Confirmed { facts, .. }
        | PaykitStatusOutcome::AcceptancePending { facts }
        | PaykitStatusOutcome::Detected { facts } => facts.payment_deadline,
        PaykitStatusOutcome::ActiveUndetected { payment_deadline }
        | PaykitStatusOutcome::UsdtObserved {
            payment_deadline, ..
        } => Some(*payment_deadline),
        _ => None,
    };
    if row.paykit_api == "upstream" && row.paykit_expires_at.is_none() {
        if let Some(deadline) = recovered_deadline {
            sqlx::query(
                "UPDATE orders SET paykit_expires_at = $3, \
                 bitcoin_quote_expires_at = CASE \
                     WHEN bitcoin_quote_rate IS NULL THEN NULL ELSE $3 END \
                 WHERE id = $1 \
                 AND paykit_invoice_id = $2 AND paykit_expires_at IS NULL",
            )
            .bind(row.id)
            .bind(row.paykit_invoice_id)
            .bind(deadline)
            .execute(&state.pool)
            .await?;
        }
    }
    if let Some(delivery) = delivery {
        record_paykit_delivery(&state.pool, row, delivery).await?;
    }
    if row.paykit_request_state == "confirmed" {
        return apply_settled_status(state, row, &outcome, now).await;
    }
    if row.paykit_request_state == "awaiting_seller_confirmation" {
        return apply_report_inside_seller_window(&state.pool, row, &outcome, now).await;
    }
    apply_status_outcome(state, row, outcome, now).await
}

/// Applies one status to an order whose payment is still open.
async fn apply_status_outcome(
    state: &AppState,
    row: &ClaimedPaykitOrder,
    outcome: PaykitStatusOutcome,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    match outcome {
        PaykitStatusOutcome::UsdtObserved {
            usdt,
            bitcoin,
            paid_asset,
            ..
        } => apply_usdt_observation(state, row, &usdt, *bitcoin, paid_asset, now).await,
        PaykitStatusOutcome::Confirmed {
            amount_matched,
            facts,
        } => {
            // The producer's mandatory late-settlement flag governs BEFORE
            // any mode-based handling: a late confirmed settlement NEVER
            // enters awaiting_seller_confirmation (shared_manual) — it takes
            // the late-money fork with the observation frozen. A mismatched
            // amount, a gone unit or a still-held order waits in durable
            // manual review; only an exact payment completes a free unit.
            if facts.late_settlement {
                return apply_confirmed_paykit_payment(
                    &state.pool,
                    row,
                    Settlement::bitcoin(amount_matched, &facts.observation, true),
                    state.confirm_keys(),
                    now,
                )
                .await;
            }
            // The creator's CURRENT mode decides (§B.11.4 A3): a downgrade
            // lands while an invoice is observing and the next matching
            // observation enters awaiting_seller_confirmation. An unknown
            // mode fails closed — no automatic paid handling, no entry.
            match facts.allocation_mode.as_str() {
                "shared_manual" => {
                    apply_shared_manual_observation(
                        &state.pool,
                        row,
                        "confirmed",
                        amount_matched,
                        &facts.observation,
                        None,
                        state.confirm_keys(),
                        now,
                    )
                    .await
                }
                "exclusive" => {
                    apply_confirmed_paykit_payment(
                        &state.pool,
                        row,
                        Settlement::bitcoin(amount_matched, &facts.observation, false),
                        state.confirm_keys(),
                        now,
                    )
                    .await
                }
                unknown => {
                    tracing::warn!(
                        order_id = %row.id,
                        allocation_mode = %unknown,
                        "paykit status reported an unknown allocation_mode; failing closed"
                    );
                    Ok(false)
                }
            }
        }
        PaykitStatusOutcome::AcceptancePending { facts } => {
            if row
                .paykit_expires_at
                .or(facts.payment_deadline)
                .is_some_and(|deadline| now >= deadline)
            {
                tracing::error!(
                    order_id = %row.id,
                    deadline = ?row.paykit_expires_at,
                    "ALERT matched upstream money never reached an accepted request state; \
                     routing to manual review"
                );
                apply_confirmed_paykit_payment(
                    &state.pool,
                    row,
                    Settlement::bitcoin(true, &facts.observation, true),
                    state.confirm_keys(),
                    now,
                )
                .await
            } else {
                // Reconciliation may expose Bitcoin before request cursor.
                // Poll within existing deadline; never extend hold or pay.
                Ok(false)
            }
        }
        PaykitStatusOutcome::ManualReview {
            reason,
            observation,
        } => {
            apply_upstream_manual_review(&state.pool, row, &reason, observation.as_ref(), now).await
        }
        PaykitStatusOutcome::Detected { facts } => {
            // A LATE detection is fail-safe: display-only at most, never
            // the seller-confirmation entry and never a payment authority
            // transition.
            if facts.late_settlement {
                return mark_paykit_detected(&state.pool, row, now).await;
            }
            match facts.allocation_mode.as_str() {
                "shared_manual" => {
                    apply_shared_manual_observation(
                        &state.pool,
                        row,
                        "detected",
                        facts.amount_matched.unwrap_or(true),
                        &facts.observation,
                        None,
                        state.confirm_keys(),
                        now,
                    )
                    .await
                }
                "exclusive" => mark_paykit_detected(&state.pool, row, now).await,
                unknown => {
                    tracing::warn!(
                        order_id = %row.id,
                        allocation_mode = %unknown,
                        "paykit status reported an unknown allocation_mode; failing closed"
                    );
                    Ok(false)
                }
            }
        }
        PaykitStatusOutcome::Undetected | PaykitStatusOutcome::ActiveUndetected { .. } => {
            if waits_for_seller(row) {
                mark_shared_manual_disappeared(&state.pool, row, now).await
            } else {
                Ok(false)
            }
        }
        // The request never reached paykit-server (NotFound stays
        // retryable: creation is idempotent and the binding transaction
        // only commits after a successful create).
        PaykitStatusOutcome::NotFound | PaykitStatusOutcome::Unavailable => Ok(false),
    }
}

/// A USDT0 transfer is in a block while Bitcoin shows no confirmed money:
/// the order settles on USDT. With confirmed money on both rails the order
/// takes the manual-review path instead and nothing settles on its own. A
/// USDT leg that is not in a block yet pays nothing and the Bitcoin leg
/// decides as a Bitcoin-only status would.
async fn apply_usdt_observation(
    state: &AppState,
    row: &ClaimedPaykitOrder,
    usdt: &PaykitUsdtObservation,
    bitcoin: PaykitStatusOutcome,
    paid_asset: Option<AcceptedAsset>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    if usdt.included() && bitcoin_holds_money(&bitcoin) {
        return apply_dual_rail_review(state, row, usdt, &bitcoin, paid_asset, now).await;
    }
    if usdt.included() {
        let observation = usdt.as_observation();
        // A seller-confirmed order's on-time matching transfer waits for the
        // seller like Bitcoin does; late or short money keeps its review route.
        if row.seller_confirms_payment() && usdt.amount_matched && usdt.paid_on_time {
            return apply_shared_manual_observation(
                &state.pool,
                row,
                "confirmed",
                true,
                &observation,
                Some(usdt),
                state.confirm_keys(),
                now,
            )
            .await;
        }
        return apply_confirmed_paykit_payment(
            &state.pool,
            row,
            Settlement {
                amount_matched: usdt.amount_matched,
                observation: &observation,
                late_settlement: !usdt.paid_on_time,
                usdt: Some(usdt),
            },
            state.confirm_keys(),
            now,
        )
        .await;
    }
    mark_paykit_detected(&state.pool, row, now).await?;
    Box::pin(apply_status_outcome(state, row, bitcoin, now)).await
}

/// Whether the Bitcoin leg of a status holds money on chain.
fn bitcoin_holds_money(bitcoin: &PaykitStatusOutcome) -> bool {
    matches!(
        bitcoin,
        PaykitStatusOutcome::Confirmed { .. } | PaykitStatusOutcome::AcceptancePending { .. }
    )
}

/// The observation document of an order with money on both rails: the rail
/// that paid first as the observation, the other rail named beside it so the
/// seller sees both.
fn dual_rail_observation(
    first: &crate::payments::PaykitObservation,
    first_matched: bool,
    other: &crate::payments::PaykitObservation,
    now: DateTime<Utc>,
) -> Value {
    let mut document =
        crate::bitcoin_review::observation_json("confirmed", first_matched, first, now, false);
    document["other_rail_asset"] = serde_json::json!(other.asset.unwrap_or(AcceptedAsset::Btc));
    document["other_rail_amount"] = serde_json::json!(match other.asset {
        Some(AcceptedAsset::Usdt) => other.observed_units,
        _ => other.observed_sats,
    });
    document["other_rail_reference"] = serde_json::json!(other.txid);
    document
}

/// Money on both rails of one request, before the order settled: the order
/// goes to the existing manual-review path with both observations frozen. The
/// rail paykit-server names as paid first is the order's paid rail (so a
/// refund follows it); the seller sees the other rail beside it and returns
/// that payment from their own wallet. When paykit-server names no first
/// rail (neither payment qualified), the service does not guess: the paid
/// rail stays unrecorded, the observation says so, and a refund is refused
/// until the seller resolves the review as paid or abandoned. Nothing is
/// refunded and nothing settles automatically.
async fn apply_dual_rail_review(
    state: &AppState,
    row: &ClaimedPaykitOrder,
    usdt: &PaykitUsdtObservation,
    bitcoin: &PaykitStatusOutcome,
    paid_asset: Option<AcceptedAsset>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let (PaykitStatusOutcome::Confirmed { facts, .. }
    | PaykitStatusOutcome::AcceptancePending { facts }) = bitcoin
    else {
        anyhow::bail!("a dual-rail review needs a Bitcoin leg holding money");
    };
    let bitcoin_matched = matches!(
        bitcoin,
        PaykitStatusOutcome::Confirmed {
            amount_matched: true,
            ..
        } | PaykitStatusOutcome::AcceptancePending { .. }
    );
    let usdt_observation = usdt.as_observation();
    let rail_known = paid_asset.is_some();
    let first_is_usdt = paid_asset == Some(AcceptedAsset::Usdt);
    let (first, first_matched, other) = if first_is_usdt {
        (&usdt_observation, usdt.amount_matched, &facts.observation)
    } else {
        (&facts.observation, bitcoin_matched, &usdt_observation)
    };
    let mut tx = state.pool.begin().await?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(row.payment_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(payment) = payment else {
        anyhow::bail!("paykit order {} references a missing payment", row.id);
    };
    if !matches!(payment.state.as_str(), "awaiting_entitlement" | "expired") {
        tx.rollback().await?;
        return Ok(false);
    }
    if rail_known {
        record_settlement_rail(&mut tx, row.id, first, first_is_usdt.then_some(usdt), now).await?;
    }
    let (revision,): (i64,) = sqlx::query_as(
        "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
         review_reason = 'amount_mismatch', manual_review_entered_at = $2, updated_at = $2 \
         WHERE id = $1 RETURNING revision",
    )
    .bind(payment.id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    let event_id = crate::executor::insert_event(
        &mut tx,
        row.id,
        &ids::payment_aggregate_id(payment.id),
        revision,
        &row.buyer_pubky,
        "payment.manual_review",
        now,
    )
    .await?;
    crate::handlers::insert_bitcoin_manual_review_intent(
        &mut tx,
        event_id,
        &row.seller_pubky,
        row.id,
        crate::handlers::BitcoinReviewNotice::AmountMismatch,
        now,
    )
    .await?;
    let mut document = dual_rail_observation(first, first_matched, other, now);
    if !rail_known {
        document["paid_rail_unknown"] = serde_json::json!(true);
    }
    sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed', \
         paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, paykit_observation = $3, \
         paykit_second_rail_at = $2, updated_at = $2 WHERE id = $1",
    )
    .bind(row.id)
    .bind(now)
    .bind(document)
    .execute(&mut *tx)
    .await?;
    if rail_known {
        freeze_paykit_observation(&mut tx, row.id, first.observed_sats).await?;
    }
    tx.commit().await?;
    tracing::error!(
        order_id = %row.id,
        code = "paykit_both_rails_paid",
        "ALERT money confirmed on both the Bitcoin and the USDT rail of one request; \
         routed to manual review"
    );
    Ok(true)
}

/// How long a USDT payment is polled while its finality is `pending` or
/// `reverted`, from the moment it took that value. Past it the order is no
/// longer polled.
const USDT_FINALITY_POLL_DAYS: i64 = 7;

/// Escalates USDT payments whose finality stayed `pending` (included, never
/// finalized) or `reverted` (reorganised away, never returned) for
/// [`USDT_FINALITY_POLL_DAYS`]: polling stops (the claim bounds it), and one
/// durable operator row is written (`needs_review`, `detected_unconfirmed`,
/// `observation.status` `usdt_finality_pending` or `usdt_finality_reverted`),
/// listed by `paykit-attempts-admin list`. The order stays paid and its
/// finality stays as it was, so physical fulfilment stays blocked until an
/// operator has checked the transfer. A reappearing transfer takes a new
/// finality value, which restarts the clock.
/// Idempotent: an order that already has a held row for its invoice is left
/// alone. Returns how many orders were escalated.
pub async fn escalate_stale_usdt_finality(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let stale: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.payment_method = 'paykit' \
         AND o.payment_finality IN ('pending', 'reverted') \
         AND o.payment_finality_at + make_interval(days => $2) <= $1 \
         AND p.state = 'confirmed' AND o.paykit_invoice_id IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM paykit_superseded_attempts h \
                         WHERE h.order_id = o.id AND h.invoice_id = o.paykit_invoice_id) \
         ORDER BY o.payment_finality_at LIMIT 50",
    )
    .bind(now)
    .bind(i32::try_from(USDT_FINALITY_POLL_DAYS)?)
    .fetch_all(pool)
    .await?;
    let mut escalated = 0u64;
    for (order_id,) in stale {
        let mut tx = pool.begin().await?;
        lock_order_payment(&mut tx, order_id).await?;
        let Some(order) = fetch_order_for_update(&mut tx, order_id).await? else {
            tx.rollback().await?;
            continue;
        };
        let (Some(invoice_id), Some(since)) = (order.paykit_invoice_id, order.payment_finality_at)
        else {
            tx.rollback().await?;
            continue;
        };
        let Some(finality) = order
            .payment_finality
            .as_deref()
            .filter(|finality| matches!(*finality, "pending" | "reverted"))
        else {
            tx.rollback().await?;
            continue;
        };
        if since + chrono::Duration::days(USDT_FINALITY_POLL_DAYS) > now {
            tx.rollback().await?;
            continue;
        }
        crate::paykit_attempts::record_released_attempt(&mut tx, order_id, now).await?;
        let held = sqlx::query(
            "UPDATE paykit_superseded_attempts SET state = 'needs_review', \
             review_reason = 'detected_unconfirmed', observation = $3, closed_at = $4 \
             WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching'",
        )
        .bind(order_id)
        .bind(invoice_id)
        .bind(serde_json::json!({
            "status": format!("usdt_finality_{finality}"),
            "finality": finality,
            "paid_asset": order.payment_asset,
            "finality_since": since,
            "observed_at": now,
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        if held.rows_affected() == 1 {
            escalated += 1;
            tracing::error!(
                order_id = %order_id,
                invoice_id = %invoice_id,
                finality,
                code = "usdt_finality_pending_too_long",
                "ALERT a USDT payment was included but never finalized (or was reorganised away \
                 and never returned) within the polling bound; held for review, fulfilment \
                 stays blocked"
            );
        } else {
            // `record_released_attempt` writes no row for an order without
            // complete pins, so nothing was held. The order is no longer
            // polled and would otherwise be silent; say so on every pass.
            tracing::error!(
                order_id = %order_id,
                invoice_id = %invoice_id,
                finality,
                code = "usdt_finality_escalation_not_recorded",
                "ALERT a USDT payment past its finality polling bound could not be held for \
                 review (no review row could be written); fulfilment stays blocked"
            );
        }
    }
    Ok(escalated)
}

/// Applies one status to an order bound through `paykit` whose payment is
/// already settled. It keeps two facts current and never un-pays the order:
///
/// - the finality of a USDT payment: `pending` until paykit-server reports
///   the transfer finalized, `reverted` if an included transfer disappears
///   first, and back to `pending` or `final` when it reappears. `final` is
///   terminal;
/// - money on the rail that did not pay the order, which can no longer be
///   taken, is held for review exactly like money on a released attempt of a
///   settled order.
async fn apply_settled_status(
    state: &AppState,
    row: &ClaimedPaykitOrder,
    outcome: &PaykitStatusOutcome,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let (usdt, bitcoin): (Option<&PaykitUsdtObservation>, Option<&PaykitStatusOutcome>) =
        match outcome {
            PaykitStatusOutcome::UsdtObserved { usdt, bitcoin, .. } => {
                (Some(usdt), Some(bitcoin.as_ref()))
            }
            PaykitStatusOutcome::Confirmed { .. }
            | PaykitStatusOutcome::AcceptancePending { .. }
            | PaykitStatusOutcome::Detected { .. }
            | PaykitStatusOutcome::Undetected
            | PaykitStatusOutcome::ActiveUndetected { .. } => (None, Some(outcome)),
            PaykitStatusOutcome::NotFound
            | PaykitStatusOutcome::Unavailable
            | PaykitStatusOutcome::ManualReview { .. } => return Ok(false),
        };
    let mut tx = state.pool.begin().await?;
    let payment_state: Option<(String,)> =
        sqlx::query_as("SELECT state FROM payments WHERE id = $1 FOR UPDATE")
            .bind(row.payment_id)
            .fetch_optional(&mut *tx)
            .await?;
    if payment_state.as_ref().map(|(state,)| state.as_str()) != Some("confirmed") {
        tx.rollback().await?;
        return Ok(false);
    }
    let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
        anyhow::bail!("paykit order {} is missing", row.id);
    };
    if order.payment_method.as_deref() != Some("paykit") {
        tx.rollback().await?;
        return Ok(false);
    }
    let paid_on = order.payment_asset.as_deref();
    let mut changed = false;
    if paid_on == Some("USDT") {
        match usdt {
            Some(usdt) if usdt.finalized => {
                changed |= set_payment_finality(&mut tx, row.id, "final", now).await?;
            }
            Some(usdt) if usdt.included() => {
                changed |= set_payment_finality(&mut tx, row.id, "pending", now).await?;
            }
            // An included transfer that the status no longer carries was
            // reorganised away. Only a definitive answer reverts it.
            _ => {
                changed |= set_payment_finality(&mut tx, row.id, "reverted", now).await?;
            }
        }
    }
    let other_rail = match (paid_on, usdt, bitcoin) {
        (Some("USDT"), _, Some(bitcoin)) if bitcoin_holds_money(bitcoin) => {
            let (PaykitStatusOutcome::Confirmed { facts, .. }
            | PaykitStatusOutcome::AcceptancePending { facts }) = bitcoin
            else {
                anyhow::bail!("a Bitcoin leg holding money carries facts");
            };
            Some(facts.observation.clone())
        }
        (Some("USDT"), _, _) => None,
        (_, Some(usdt), _) if usdt.included() => Some(usdt.as_observation()),
        _ => None,
    };
    if let Some(observation) = other_rail {
        changed |= flag_second_rail(&mut tx, &order, &observation, now).await?;
    }
    if changed {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(changed)
}

/// Holds money seen on the rail that did not pay an order for an operator.
/// The order and its payment are untouched. Once per order.
async fn flag_second_rail(
    tx: &mut Transaction<'_, Postgres>,
    order: &crate::model::OrderRow,
    observation: &crate::payments::PaykitObservation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let first = sqlx::query(
        "UPDATE orders SET paykit_second_rail_at = $2, updated_at = $2 \
         WHERE id = $1 AND paykit_second_rail_at IS NULL",
    )
    .bind(order.id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if first.rows_affected() != 1 {
        return Ok(false);
    }
    crate::paykit_attempts::record_released_attempt(tx, order.id, now).await?;
    let details = serde_json::json!({
        "status": "second_rail",
        "paid_asset": order.payment_asset,
        "observed_at": now,
        "observation": observation,
    });
    sqlx::query(
        "UPDATE paykit_superseded_attempts SET state = 'needs_review', \
         review_reason = 'payment_settled', observation = $3, closed_at = $4 \
         WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching'",
    )
    .bind(order.id)
    .bind(order.paykit_invoice_id)
    .bind(&details)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    tracing::error!(
        order_id = %order.id,
        paid_asset = ?order.payment_asset,
        other_rail = ?observation.asset,
        code = "paykit_second_rail_paid_after_settlement",
        "ALERT money confirmed on the rail that did not pay a settled order; held for review"
    );
    Ok(true)
}

/// Records whether the polled request reached the buyer's wallet, for the
/// attempt the claim polled (a re-bind since then is left alone). A final
/// state (`delivered`, `failed`) is never moved back to `pending`.
async fn record_paykit_delivery(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    delivery: crate::payments::PaykitDeliveryState,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE orders SET paykit_delivery_state = $3 \
         WHERE id = $1 AND paykit_request_reference = $2 \
         AND paykit_delivery_state IS DISTINCT FROM $3 \
         AND (paykit_delivery_state IS NULL OR paykit_delivery_state = 'pending')",
    )
    .bind(row.id)
    .bind(&row.paykit_request_reference)
    .bind(delivery.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

/// One paykit verification pass: claims due bitcoin orders, polls the
/// paykit-server status for each, and applies the outcome. Returns the
/// number of payments advanced (confirmed or manual review). A per-item
/// failure is logged with the order identity and the pass CONTINUES with
/// the rest of the claimed batch — one bad row is never head-of-line for
/// the others.
pub async fn verify_due_paykit_payments(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    if let Err(error) = escalate_stale_usdt_finality(&state.pool, now).await {
        tracing::error!(error = %error, "USDT finality escalation pass failed");
    }
    let claimed =
        claim_due_paykit_orders(&state.pool, now, state.config.paykit_poll_seconds).await?;
    let mut applied = 0u64;
    for row in &claimed {
        match apply_paykit_status_outcome(state, source, row, now).await {
            Ok(true) => applied += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    order_id = %row.id,
                    payment_id = %row.payment_id,
                    error = %error,
                    "paykit verification failed for a claimed order; continuing the batch"
                );
            }
        }
    }
    match crate::paykit_attempts::verify_due_released_attempts(state, source, now).await {
        Ok(routed) => applied += routed,
        Err(error) => tracing::error!(
            error = %error,
            "released paykit attempt verification pass failed"
        ),
    }
    Ok(applied)
}

/// Expires pending orders whose armed inventory-hold window has elapsed on
/// server time ("only a payment locks an item"): the hold releases
/// (`reserved → available`, crediting a stamped drop first under the shared
/// lock order), the payment moves to `expired` (the machine's
/// `payment_window` edge from `awaiting_entitlement`), and the order is
/// cancelled with the stored reason "payment window elapsed" — post-expiry
/// the buyer simply checks out again. A held order with `hold_expires_at`
/// NULL (pre-#50 rows the 0012 backfill missed) is already elapsed.
///
/// This is a marketplace policy transition, independent of upstream state; a
/// Locks correlation keeps polling (bounded by the Lock Server's own task
/// ageing), so a completion verified later still surfaces as
/// `manual_review`. Payments already under `manual_review` are deliberately
/// NOT swept: a human decides those — their money may be real, so their
/// hold is never auto-released. A sandbox payment the buyer drove to
/// `detected` has no `detected → expired` edge; its lapsed order still
/// cancels and restocks, and the payment record stays untouched exactly as
/// buyer cancellation leaves it. Confirmed orders are `paid` and never
/// match the sweep.
pub async fn expire_due_payment_windows(
    state: &AppState,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let pool = &state.pool;
    let mut tx = pool.begin().await?;
    // A `preparing` bitcoin order is excluded here: its expiry must first
    // settle the prepared invoice with paykit (§B.11.2 hold-expiry rule,
    // the coordinator-decided cell), handled per order below. An order in
    // `awaiting_seller_confirmation` is likewise excluded: its hold was
    // extended to the 24-hour seller-confirmation window at entry, and the
    // seller-window reaper (which routes the payment to `manual_review`
    // with the hold PRESERVED) owns it — the buyer demonstrably paid on
    // chain, so the ordinary sweep must never release that stock.
    // Discovery locks only the payment rows; each order is locked after
    // its payment (see `lock_sweep_order`).
    let due: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(&format!(
        "SELECT o.id, p.id, p.state, o.buyer_pubky \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE {HELD_DUE_ORDER} \
         AND p.state IN ('awaiting_entitlement', 'detected', 'expired') \
         ORDER BY o.hold_expires_at NULLS FIRST FOR UPDATE OF p SKIP LOCKED"
    ))
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for (order_id, payment_id, payment_state, buyer_pubky) in due {
        if !lock_sweep_order(&mut tx, HELD_DUE_ORDER, order_id, now).await? {
            continue;
        }
        expire_held_order(
            &mut tx,
            order_id,
            payment_id,
            &payment_state,
            &buyer_pubky,
            now,
        )
        .await?;
        expired += 1;
    }
    tx.commit().await?;

    // §B.11.2 hold-expiry rule for `preparing` orders (coordinator
    // decision; design §B.11.2 leaves this cell undefined — W9.3
    // reconcile): the prepared invoice is settled with paykit BEFORE the
    // ordinary expiry effects run, each order in its own transaction so no
    // HTTP call rides the batch transaction.
    let preparing: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.state = 'pending_payment' AND o.stock_held \
         AND (o.hold_expires_at IS NULL OR o.hold_expires_at <= $1) \
         AND p.state = 'awaiting_entitlement' AND o.paykit_activation_state = 'preparing' \
         ORDER BY o.hold_expires_at NULLS FIRST",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    for (order_id,) in preparing {
        match expire_preparing_order(state, order_id, now).await {
            Ok(true) => expired += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    order_id = %order_id,
                    error = %error,
                    "preparing-order hold expiry failed; continuing the batch"
                );
            }
        }
    }
    let lapsed_auction = expire_lapsed_auction_orders(state, now).await?;
    let idle = expire_idle_unbound_checkouts(state, now).await?;
    Ok(expired + lapsed_auction + idle)
}

/// Cancels an auction winner's unpaid order once its winning reservation has
/// lapsed. An auction order never takes a bind hold (`stock_held` stays
/// false), so the held-window sweep above cannot match it, and the
/// reservation sweep only returns the unit: without this step the order and
/// its awaiting payment stayed open indefinitely after the hold ended. The
/// reservation sweep already released the stock, so the expiry effects here
/// skip the release; a paid order, an order awaiting seller confirmation,
/// and a payment under manual review never match (their reservation is
/// preserved or converted, never `expired`).
async fn expire_lapsed_auction_orders(state: &AppState, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = state.pool.begin().await?;
    let due: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(&format!(
        "SELECT o.id, p.id, p.state, o.buyer_pubky \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE {LAPSED_AUCTION_ORDER} \
         AND p.state IN ('awaiting_entitlement', 'detected', 'expired') \
         ORDER BY o.created_at FOR UPDATE OF p SKIP LOCKED"
    ))
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for (order_id, payment_id, payment_state, buyer_pubky) in due {
        if !lock_sweep_order(&mut tx, LAPSED_AUCTION_ORDER, order_id, now).await? {
            continue;
        }
        expire_held_order(
            &mut tx,
            order_id,
            payment_id,
            &payment_state,
            &buyer_pubky,
            now,
        )
        .await?;
        expired += 1;
    }
    tx.commit().await?;
    Ok(expired)
}

/// The shared expiry effects for one due order, inside the caller's
/// transaction: the hold releases (`reserved → available`, crediting a
/// stamped drop first under the shared lock order), an
/// `awaiting_entitlement` payment moves to `expired`, and the order is
/// cancelled with the stored reason "payment window elapsed".
async fn expire_held_order(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    payment_id: Uuid,
    payment_state: &str,
    buyer_pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let Some(order) = fetch_order_for_update(tx, order_id).await? else {
        return Ok(());
    };
    // A Locks-correlated payment's expiry keeps its reconciliation
    // trail: the correlation id is the traceable command id and the
    // observation history records the window elapsing.
    let correlation: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM payment_locks_correlations WHERE order_id = $1")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await?;
    let command_id = correlation
        .map(|(id,)| id)
        .unwrap_or_else(uuid::Uuid::new_v4);

    if payment_state == "awaiting_entitlement" {
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'expired', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment_id)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        crate::executor::insert_event(
            tx,
            command_id,
            &ids::payment_aggregate_id(payment_id),
            revision,
            buyer_pubky,
            "payment.expired",
            now,
        )
        .await?;
        if let Some((correlation_id,)) = correlation {
            insert_observation(tx, correlation_id, "window_elapsed", "payment_expired", now)
                .await?;
        }
    }

    // A drop-stamped hold credits its drop first (drop lock before the
    // listing lock, the shared order): while live the unit restocks and
    // the buyer's per-drop cap frees; an ended drop keeps honest books
    // but nothing reopens. An auction order's reservation sweep already
    // credited its drop, so the order sweep never credits it again.
    if let (Some(drop_aggregate_id), None) = (&order.drop_aggregate_id, &order.auction_aggregate_id)
    {
        let units: i64 = order
            .lines
            .as_array()
            .expect("order lines are an array")
            .iter()
            .map(|line| {
                line["quantity"]
                    .as_i64()
                    .expect("order line carries its quantity")
            })
            .sum();
        if !crate::handlers::drops::credit_drop_release(
            tx,
            drop_aggregate_id,
            &order.buyer_pubky,
            units,
            now,
        )
        .await?
        {
            if order.hold_expires_at.is_none() {
                tracing::warn!(
                    order_id = %order_id,
                    drop_aggregate_id = %drop_aggregate_id,
                    "legacy null-expiry hold: drop units unaccounted; still expiring"
                );
            } else {
                anyhow::bail!("expired order {order_id} could not credit drop {drop_aggregate_id}");
            }
        }
    }
    // An auction order's hold is its winning reservation, which the
    // reservation sweep already returned: releasing again would double-credit.
    if order.auction_aggregate_id.is_none() {
        if let Err(failure) = crate::handlers::holds::release_lines(
            tx,
            &order,
            crate::handlers::holds::HeldQuantity::Reserved,
            now,
        )
        .await?
        {
            if order.hold_expires_at.is_none() {
                tracing::warn!(
                    order_id = %order_id,
                    error = ?failure,
                    "legacy null-expiry hold: listing unaccounted; still expiring"
                );
            } else {
                anyhow::bail!("expired order {order_id} could not release its hold: {failure:?}");
            }
        }
    }

    debug_assert!(marketplace_domain::state_machines::can_transition(
        &marketplace_domain::state_machines::order_machine(),
        "pending_payment",
        "cancelled"
    ));
    let (order_revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET state = 'cancelled', revision = revision + 1, \
         cancellation_reason = 'payment window elapsed', stock_held = false, \
         hold_expires_at = NULL, updated_at = $2 WHERE id = $1 RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    crate::executor::insert_event(
        tx,
        command_id,
        &ids::order_aggregate_id(order_id),
        order_revision,
        buyer_pubky,
        "order.cancelled",
        now,
    )
    .await?;

    tracing::info!(
        order_id = %order_id,
        payment_id = %payment_id,
        "expired hold window: released stock, expired payment, cancelled order"
    );
    Ok(())
}

/// An upstream order's hold once its request is active, given its deadline
/// as `$3`: the deadline plus `UPSTREAM_HOLD_CLOCK_SKEW_SECONDS`, never
/// later than the bind hold. An auction order keeps its reservation hold.
const UPSTREAM_ACTIVATED_HOLD: &str = "CASE WHEN auction_aggregate_id IS NULL \
     THEN LEAST(hold_expires_at, $3 + INTERVAL '60 seconds') ELSE hold_expires_at END";

/// Marks a pending Paykit order `detected`; `$1` is the order, `$2` now. A
/// held upstream order gets its bind hold back (latest prepare expiry +
/// payment window + skew margin): Bitcoin is on time by first sighting, so a
/// payment seen before the request's deadline may still confirm after it.
const MARK_PAYKIT_DETECTED: &str = "UPDATE orders SET paykit_request_state = 'detected', \
     hold_expires_at = CASE WHEN paykit_api = 'upstream' AND auction_aggregate_id IS NULL \
         AND state = 'pending_payment' AND stock_held \
         THEN GREATEST(hold_expires_at, paykit_prepare_expires_at \
             + make_interval(secs => paykit_payment_window_seconds + 60)) \
         ELSE hold_expires_at END, \
     updated_at = $2 \
     WHERE id = $1 AND paykit_request_state = 'pending'";

/// The order half of the held-window expiry predicate; `$1` is `now`.
const HELD_DUE_ORDER: &str = "o.state = 'pending_payment' AND o.stock_held \
     AND (o.hold_expires_at IS NULL OR o.hold_expires_at <= $1) \
     AND o.paykit_activation_state IS DISTINCT FROM 'preparing' \
     AND o.paykit_request_state IS DISTINCT FROM 'awaiting_seller_confirmation'";

/// The order half of the lapsed auction-winner predicate; `$1` is `now`. The
/// winning reservation must be `expired` (swept) with no live one left, and
/// the order must not be mid-activation or waiting on its seller.
const LAPSED_AUCTION_ORDER: &str = "o.state = 'pending_payment' \
     AND o.auction_aggregate_id IS NOT NULL \
     AND o.paykit_activation_state IS DISTINCT FROM 'preparing' \
     AND o.paykit_request_state IS DISTINCT FROM 'awaiting_seller_confirmation' \
     AND EXISTS (SELECT 1 FROM reservations r \
         WHERE r.listing_aggregate_id = o.auction_aggregate_id \
         AND r.buyer_pubky = o.buyer_pubky \
         AND r.status = 'expired' AND r.expires_at <= $1) \
     AND NOT EXISTS (SELECT 1 FROM reservations r \
         WHERE r.listing_aggregate_id = o.auction_aggregate_id \
         AND r.buyer_pubky = o.buyer_pubky AND r.status = 'active')";

/// The order half of the idle unbound-checkout predicate; `$1` is the
/// creation cutoff.
const IDLE_UNBOUND_ORDER: &str = "o.state = 'pending_payment' AND NOT o.stock_held \
     AND o.payment_method IS NULL AND o.auction_aggregate_id IS NULL \
     AND o.drop_aggregate_id IS NULL AND o.created_at <= $1";

/// Locks one sweep candidate's order after the discovery statement locked
/// its payment, re-checking the order half of the predicate that statement
/// read without locking the order. `false` skips the candidate this tick:
/// the order no longer matches, or another transaction holds it. A joined
/// `FOR UPDATE OF o, p` would lock the order row first and keep it even
/// when the payment is skipped, inverting the payment-then-order order.
async fn lock_sweep_order(
    tx: &mut Transaction<'_, Postgres>,
    predicate: &str,
    order_id: Uuid,
    bound: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let locked: Option<(Uuid,)> = sqlx::query_as(&format!(
        "SELECT o.id FROM orders o WHERE {predicate} AND o.id = $2 \
         FOR UPDATE OF o SKIP LOCKED"
    ))
    .bind(bound)
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(locked.is_some())
}

/// Cancels leftover unbound checkouts (`pending_payment`, no hold, no
/// payment method, not auction/drop) after `CHECKOUT_HOLD_WINDOW_SECONDS`.
/// No inventory moves: ordinary create never reserved a unit. Bound
/// invoices are excluded — TTL is their only safety net.
async fn expire_idle_unbound_checkouts(
    state: &AppState,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let cutoff = now - chrono::Duration::seconds(state.config.checkout_hold_window_seconds);
    let mut tx = state.pool.begin().await?;
    // Payment first, then each order (see `lock_sweep_order`).
    let due: Vec<(Uuid, Uuid, String)> = sqlx::query_as(&format!(
        "SELECT o.id, p.id, o.buyer_pubky \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE {IDLE_UNBOUND_ORDER} \
         AND p.state = 'awaiting_entitlement' \
         ORDER BY o.created_at FOR UPDATE OF p SKIP LOCKED"
    ))
    .bind(cutoff)
    .fetch_all(&mut *tx)
    .await?;

    let mut cancelled = 0u64;
    for (order_id, payment_id, buyer_pubky) in due {
        if !lock_sweep_order(&mut tx, IDLE_UNBOUND_ORDER, order_id, cutoff).await? {
            continue;
        }
        let command_id = Uuid::new_v4();
        let (payment_revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'expired', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        crate::executor::insert_event(
            &mut tx,
            command_id,
            &ids::payment_aggregate_id(payment_id),
            payment_revision,
            &buyer_pubky,
            "payment.expired",
            now,
        )
        .await?;
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "pending_payment",
            "cancelled"
        ));
        let (order_revision,): (i64,) = sqlx::query_as(
            "UPDATE orders SET state = 'cancelled', revision = revision + 1, \
             cancellation_reason = 'checkout idle elapsed', stock_held = false, \
             hold_expires_at = NULL, updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(order_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        crate::executor::insert_event(
            &mut tx,
            command_id,
            &ids::order_aggregate_id(order_id),
            order_revision,
            &buyer_pubky,
            "order.cancelled",
            now,
        )
        .await?;
        tracing::info!(
            order_id = %order_id,
            payment_id = %payment_id,
            "expired unbound checkout idle: cancelled leftover with no hold"
        );
        cancelled += 1;
    }
    tx.commit().await?;
    Ok(cancelled)
}

/// How long past the hold deadline a `preparing` order may wait for an
/// unreachable paykit before the marketplace voids locally and defers the
/// void to a `paykit.void` outbox row.
const PREPARING_VOID_GRACE_SECONDS: i64 = 30 * 60;

/// §B.11.2's hold-expiry cell for a `preparing` bitcoin order (coordinator
/// decision; design §B.11.2 leaves this cell undefined — W9.3 reconcile):
/// the prepared invoice is voided at its persisted endpoint BEFORE the
/// ordinary expiry effects run. Returns true when the order expired.
///
/// - `200` / `prepare_expired` / `unknown_invoice`: the prepare is gone —
///   `voided`, the activate row stamped, ordinary expiry effects, and the
///   `payment.bitcoin_prepare_voided` intent, in one transaction.
/// - `invoice_finalized`: paykit already activated the invoice (a lost
///   2xx) — mark `active` + `paykit_request_state='pending'` and let the
///   ordinary expiry for a pending bitcoin order run next tick (paykit's
///   `expires_at` equals the hold deadline, so the invoice expires too).
/// - unreachable: the activate row's lease is released for the next tick;
///   past `hold_expires_at + 30 min` the marketplace voids locally and
///   defers the remote void to a `paykit.void` outbox row (alert
///   `paykit_unreachable_at_void`).
async fn expire_preparing_order(
    state: &AppState,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let pool = &state.pool;
    // Take the activate row's lease first: a delivery in flight settles the
    // invoice itself, so this order is skipped this tick.
    let leased: Option<(i64,)> = sqlx::query_as(
        "UPDATE outbox SET lease_until = $2 WHERE id = (\
             SELECT id FROM outbox WHERE kind = 'paykit.activate' \
             AND payload->>'order_id' = $3 AND delivered_at IS NULL \
             ORDER BY id LIMIT 1\
         ) AND delivered_at IS NULL AND (lease_until IS NULL OR lease_until <= $1) \
         RETURNING id",
    )
    .bind(now)
    .bind(now + chrono::Duration::seconds(state.config.worker_lease_seconds))
    .bind(order_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some((outbox_row_id,)) = leased else {
        return Ok(false);
    };
    #[derive(sqlx::FromRow)]
    struct PreparingPin {
        paykit_invoice_id: Option<Uuid>,
        paykit_api: Option<String>,
        paykit_stack_id: Option<String>,
        paykit_stack_endpoint: Option<String>,
        seller_pubky: String,
        hold_expires_at: Option<DateTime<Utc>>,
        payment_id: Uuid,
    }
    let pin: Option<PreparingPin> = sqlx::query_as(
        "SELECT o.paykit_invoice_id, o.paykit_api, o.paykit_stack_id, o.paykit_stack_endpoint, \
             o.seller_pubky, o.hold_expires_at, p.id AS payment_id \
             FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await?;
    let Some(PreparingPin {
        paykit_invoice_id: Some(invoice_id),
        paykit_api: Some(api),
        paykit_stack_id,
        paykit_stack_endpoint,
        seller_pubky,
        hold_expires_at,
        payment_id,
    }) = pin
    else {
        anyhow::bail!("preparing order {order_id} is missing its persisted paykit pin");
    };
    let target = match (api.as_str(), paykit_stack_endpoint, paykit_stack_id) {
        ("fork", Some(endpoint), Some(stack_id)) => {
            PaykitLifecycleTarget::Fork { endpoint, stack_id }
        }
        ("upstream", None, None) => PaykitLifecycleTarget::Upstream {
            creator: seller_pubky,
        },
        _ => anyhow::bail!("preparing order {order_id} has invalid paykit lifecycle pins"),
    };
    let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    else {
        tracing::error!(
            order_id = %order_id,
            "preparing-order hold expiry on a deployment without the paykit client; skipping"
        );
        return Ok(false);
    };
    match paykit
        .void_payment_request(&target, invoice_id, "hold_expired")
        .await
    {
        Ok(voided) => {
            tracing::info!(
                order_id = %order_id,
                invoice_state = %voided.state,
                "voided a preparing order's invoice at hold expiry"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::PrepareExpired) | Err(PaykitCommandError::UnknownInvoice) => {
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::InvoiceFinalized) => {
            // Paykit already activated the invoice — a lost 2xx. The order
            // becomes a live pending bitcoin order; the ordinary expiry
            // path handles it from the next tick.
            let mut tx = pool.begin().await?;
            sqlx::query(
                "UPDATE orders SET paykit_activation_state = 'active', \
                 paykit_request_state = 'pending', updated_at = $2 \
                 WHERE id = $1 AND paykit_activation_state = 'preparing'",
            )
            .bind(order_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(outbox_row_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::warn!(
                order_id = %order_id,
                "paykit had already activated the invoice (lost 2xx); order is now live-pending"
            );
            Ok(false)
        }
        Err(PaykitCommandError::StackIdentityMismatch) => {
            tracing::error!(
                order_id = %order_id,
                "ALERT the persisted paykit endpoint answered with a different stack identity \
                 at hold-expiry void; voiding locally"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::ActivationTotalMismatch)
        | Err(PaykitCommandError::UnexpectedRejection(_)) => {
            tracing::error!(
                order_id = %order_id,
                "ALERT paykit hold-expiry void hit an unknown contract state; voiding locally"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::Unavailable) => {
            let grace_elapsed = hold_expires_at.is_some_and(|deadline| {
                now > deadline + chrono::Duration::seconds(PREPARING_VOID_GRACE_SECONDS)
            });
            if !grace_elapsed {
                // Release the lease; retried on the next tick.
                sqlx::query("UPDATE outbox SET lease_until = NULL WHERE id = $1")
                    .bind(outbox_row_id)
                    .execute(pool)
                    .await?;
                tracing::warn!(
                    order_id = %order_id,
                    "paykit unreachable at hold-expiry void; retrying next tick"
                );
                return Ok(false);
            }
            tracing::error!(
                order_id = %order_id,
                invoice_id = %invoice_id,
                "ALERT paykit_unreachable_at_void: voiding locally past the 30-minute grace \
                 and deferring the remote void to a paykit.void outbox row"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, true)
                .await?;
            Ok(true)
        }
    }
}

/// One transaction for the `preparing → voided` hold-expiry edge: the
/// activation state flips to `voided`, the activate row is stamped
/// delivered, the ordinary expiry effects run (hold released, payment
/// expired, order cancelled), the `payment.bitcoin_prepare_voided` intent
/// is emitted, and — past the unreachability grace — a `paykit.void` row
/// is enqueued for the deferred remote void.
async fn void_and_expire_preparing_order(
    pool: &PgPool,
    order_id: Uuid,
    payment_id: Uuid,
    outbox_row_id: i64,
    now: DateTime<Utc>,
    enqueue_void_retry: bool,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    lock_order_payment(&mut tx, order_id).await?;
    let buyer_pubky: (String,) = sqlx::query_as("SELECT buyer_pubky FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
    expire_held_order(
        &mut tx,
        order_id,
        payment_id,
        "awaiting_entitlement",
        &buyer_pubky.0,
        now,
    )
    .await?;
    let (revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET revision = revision + 1, paykit_activation_state = 'voided', \
         paykit_request_state = NULL, updated_at = $2 \
         WHERE id = $1 AND paykit_activation_state = 'preparing' RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    crate::paykit_attempts::record_released_attempt(&mut tx, order_id, now).await?;
    let event_id = crate::executor::insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order_id),
        revision,
        SYSTEM_ACTOR,
        "payment.bitcoin_prepare_voided",
        now,
    )
    .await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        "bitcoin_prepare_voided",
        &buyer_pubky.0,
        SYSTEM_ACTOR,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await?;
    if enqueue_void_retry {
        let pin: (Option<Uuid>, Option<String>, Option<String>, Option<String>, String) = sqlx::query_as(
            "SELECT paykit_invoice_id, paykit_api, paykit_stack_id, paykit_stack_endpoint, seller_pubky \
             FROM orders WHERE id = $1",
        )
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
        let (Some(invoice_id), Some(api), stack_id, endpoint, creator) = pin else {
            anyhow::bail!("preparing order {order_id} is missing its paykit invoice");
        };
        let pin = match (api.as_str(), stack_id, endpoint) {
            ("fork", Some(stack_id), Some(stack_endpoint)) => {
                crate::handlers::cancellation::PaykitVoidPin::Fork {
                    invoice_id,
                    stack_id,
                    stack_endpoint,
                }
            }
            ("upstream", None, None) => crate::handlers::cancellation::PaykitVoidPin::Upstream {
                invoice_id,
                creator,
            },
            _ => anyhow::bail!("preparing order {order_id} has invalid paykit lifecycle pins"),
        };
        crate::handlers::cancellation::enqueue_paykit_void(
            &mut tx,
            event_id,
            order_id,
            &pin,
            "hold_expired",
            now,
        )
        .await?;
    }
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(outbox_row_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// RFC3339-shaped prefix (`YYYY-MM-DDTHH:MM:SS`). Used only to over-include
/// candidates in SQL; due-ness and validity are decided in Rust so a
/// malformed `shipment` timestamp cannot `::timestamptz`-abort the batch.
const SHIPMENT_INSTANT_PREFIX: &str = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}";

fn parse_shipment_instant(raw: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw?)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

struct DeliverySweepClaim {
    claimed: u64,
    processed: u64,
}

/// One claimed `delivered` order for the auto-complete sweep, with both
/// candidate delivery instants (shipment JSON for shipped orders, the
/// handover row for pickups) coalesced by the claim query.
#[derive(Debug, sqlx::FromRow)]
struct DeliveredOrderClaim {
    id: Uuid,
    buyer_pubky: String,
    seller_pubky: String,
    fulfillment: String,
    delivered_at: Option<String>,
    handover_at: Option<DateTime<Utc>>,
    /// The order's own period (`orders.auto_complete_days`), else the
    /// configured default.
    auto_complete_days: i64,
}

/// Marks `shipped` orders `delivered` once the order's own delivery period
/// (`orders.delivery_assume_days`, frozen from the seller's shop record at
/// the order's first payment lock point; `default_assume_days` when none was
/// captured) has elapsed since the ship timestamp — the `delivery_assume` server trigger on the
/// machine's shipped → delivered edge, standing in for the carrier
/// tracking feed this service deliberately does not have. The update is a
/// compare-and-swap on `state = 'shipped'`, so a double-claim (lease
/// violation or a racing pass) transitions the order exactly once; the
/// `delivery_assumed` flag tells the projection — and the buyer — the
/// delivery was assumed, not confirmed. Both participants are notified;
/// the event is attributed to the system actor, never a peer.
///
/// The same period also ends a paid manual digital order the seller never
/// marked delivered, counted from the payment (see
/// `assume_due_digital_deliveries_batch`).
///
/// Claims are bounded (`batch_size` per inner transaction, up to
/// `max_batches` per call) so a backlog cannot hold row locks on every due
/// order at once. Malformed `shipped_at` values are skipped and logged by
/// order id (no payload/PII) so they cannot abort the rest of the batch.
pub async fn assume_due_deliveries(
    pool: &PgPool,
    now: DateTime<Utc>,
    default_assume_days: i64,
    batch_size: i64,
    max_batches: u32,
) -> anyhow::Result<u64> {
    let mut assumed = 0u64;
    for _ in 0..max_batches {
        let pass = assume_due_deliveries_batch(pool, now, default_assume_days, batch_size).await?;
        assumed += pass.processed;
        if pass.claimed < batch_size as u64 {
            break;
        }
    }
    for _ in 0..max_batches {
        let pass =
            assume_due_digital_deliveries_batch(pool, now, default_assume_days, batch_size).await?;
        assumed += pass.processed;
        if pass.claimed < batch_size as u64 {
            break;
        }
    }
    Ok(assumed)
}

/// Assumes delivery of paid digital orders the seller never marked
/// delivered: a manual ("I'll email it" / "I'll send it in messages") order
/// that sits in `paid` past its frozen delivered-assumption period, counted
/// from the receipt's issue instant (the payment), moves `paid → delivered`
/// exactly as a shipped order does after shipment. Instant-only digital
/// orders are delivered at their release and never wait here, and an order
/// whose instant lines still wait for the seller's release
/// (`DIGITAL_RELEASE=seller`) is never assumed delivered: the buyer has
/// nothing to open until the seller releases it. An open
/// cancellation request (`cancel_requested`) takes the order out of the
/// sweep by construction, and so does a PayPal reversal outstanding on the
/// order (`payment_reversed_at`): the seller cannot read the address while
/// the money is taken back, so the clock is only honored once it is
/// restored (the period has usually elapsed by then, so the next pass
/// assumes delivery). The assumption stamps `digital_delivered_at` (the
/// auto-complete clock) and `delivery_assumed`, so the buyer sees it was
/// assumed, and the later `delivered → completed` edge starts the sealed
/// delivery-email retention clock.
async fn assume_due_digital_deliveries_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    default_assume_days: i64,
    batch_size: i64,
) -> anyhow::Result<DeliverySweepClaim> {
    let mut tx = pool.begin().await?;
    let due: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT o.id, o.buyer_pubky, o.seller_pubky \
         FROM orders o JOIN receipts r ON r.order_id = o.id \
         WHERE o.state = 'paid' AND o.fulfillment = 'digital' \
         AND o.payment_reversed_at IS NULL \
         AND (o.digital_release IS DISTINCT FROM 'seller' OR o.digital_released_at IS NOT NULL) \
         AND r.issued_at <= $1 - interval '1 day' * COALESCE(o.delivery_assume_days::bigint, $2) \
         ORDER BY o.id LIMIT $3 FOR UPDATE OF o SKIP LOCKED",
    )
    .bind(now)
    .bind(default_assume_days)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let claimed = due.len() as u64;
    let mut processed = 0u64;
    for (order_id, buyer_pubky, seller_pubky) in due {
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "paid",
            "delivered"
        ));
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE orders SET revision = revision + 1, state = 'delivered', \
             digital_delivered_at = $2, delivery_assumed = TRUE, updated_at = $2 \
             WHERE id = $1 AND state = 'paid' RETURNING revision",
        )
        .bind(order_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        let aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &aggregate_id,
            revision,
            SYSTEM_ACTOR,
            "fulfillment.digital_delivered",
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivery_assumed",
            &buyer_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivered",
            &seller_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        tracing::info!(order_id = %order_id, "assumed digital delivery on server time");
        processed += 1;
    }
    tx.commit().await?;
    Ok(DeliverySweepClaim { claimed, processed })
}

async fn assume_due_deliveries_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    default_assume_days: i64,
    batch_size: i64,
) -> anyhow::Result<DeliverySweepClaim> {
    let mut tx = pool.begin().await?;
    // Each order is due `COALESCE(orders.delivery_assume_days, default)` days
    // after shipment: the period frozen on the order when its payment
    // started, else the configured default. The per-order cutoff is built
    // in SQL (as the same `YYYY-MM-DDTHH:MM:SS` text the shipment instant
    // is compared against) so the batch limit never fills with orders that
    // are not due yet.
    let due: Vec<(Uuid, Value, String, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT id, shipment, buyer_pubky, seller_pubky, shipment->>'shipped_at', \
                COALESCE(delivery_assume_days::bigint, $4) \
         FROM orders \
         WHERE state = 'shipped' AND fulfillment <> 'digital' \
         AND (shipment->>'shipped_at' IS NULL \
              OR shipment->>'shipped_at' !~ $2 \
              OR left(shipment->>'shipped_at', 19) <= to_char( \
                   ($1::timestamptz - interval '1 day' * COALESCE(delivery_assume_days::bigint, $4)) \
                   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS')) \
         ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED",
    )
    .bind(now)
    .bind(SHIPMENT_INSTANT_PREFIX)
    .bind(batch_size)
    .bind(default_assume_days)
    .fetch_all(&mut *tx)
    .await?;

    let claimed = due.len() as u64;
    let mut processed = 0u64;
    for (order_id, shipment, buyer_pubky, seller_pubky, shipped_at, assume_days) in due {
        let Some(shipped_at) = parse_shipment_instant(shipped_at.as_deref()) else {
            tracing::warn!(
                order_id = %order_id,
                "skipping shipped order with malformed shipment timestamp"
            );
            continue;
        };
        if shipped_at > now - chrono::Duration::days(assume_days) {
            continue;
        }
        let mut delivered = shipment;
        delivered["state"] = serde_json::json!("delivered");
        // `delivered_at` is the server instant of the assumption, not
        // `shipped_at + assume_days`. This is a system event (no carrier
        // tracking feed, not peer-attested): the auto-complete clock must
        // start when the service marked delivery, not when the parcel
        // hypothetically arrived.
        delivered["delivered_at"] = serde_json::json!(crate::clock::format_timestamp(now));
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "shipped",
            "delivered"
        ));
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE orders SET revision = revision + 1, state = 'delivered', shipment = $2, \
             delivery_assumed = TRUE, updated_at = $3 \
             WHERE id = $1 AND state = 'shipped' RETURNING revision",
        )
        .bind(order_id)
        .bind(&delivered)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        let aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &aggregate_id,
            revision,
            SYSTEM_ACTOR,
            "fulfillment.delivered",
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivery_assumed",
            &buyer_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivered",
            &seller_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        tracing::info!(order_id = %order_id, "assumed delivery on server time");
        processed += 1;
    }
    tx.commit().await?;
    Ok(DeliverySweepClaim { claimed, processed })
}

/// Completes `delivered` orders once the order's own completion period
/// (`orders.auto_complete_days`, frozen like the delivery period;
/// `default_auto_complete_days` when none was captured) has elapsed since
/// the delivery timestamp — the `order_auto_complete` server trigger
/// on the machine's delivered → completed edge. An open return or cancel
/// request blocks completion by construction: `return_requested`,
/// `return_approved`, `return_received`, and `cancel_requested` are their
/// own order states, so such an order never matches the `state =
/// 'delivered'` claim or the compare-and-swap. The event is attributed to
/// the system actor, never a peer.
pub async fn complete_due_delivered_orders(
    pool: &PgPool,
    now: DateTime<Utc>,
    default_auto_complete_days: i64,
    batch_size: i64,
    max_batches: u32,
) -> anyhow::Result<u64> {
    let mut completed = 0u64;
    for _ in 0..max_batches {
        let pass =
            complete_due_delivered_orders_batch(pool, now, default_auto_complete_days, batch_size)
                .await?;
        completed += pass.processed;
        if pass.claimed < batch_size as u64 {
            break;
        }
    }
    Ok(completed)
}

async fn complete_due_delivered_orders_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    default_auto_complete_days: i64,
    batch_size: i64,
) -> anyhow::Result<DeliverySweepClaim> {
    let mut tx = pool.begin().await?;
    // Each order completes `COALESCE(orders.auto_complete_days, default)`
    // days after delivery: the period frozen on the order when its payment
    // started, else the configured default. The per-order cutoff is built
    // in SQL so the batch limit never fills with orders that are not due.
    //
    // The delivery instant COALESCES the two sources (§A6): the handover
    // record's server instant for pickup orders, the shipment
    // `delivered_at` for shipped ones. The join locks `FOR UPDATE OF o`
    // (orders only — never the handover row), so the coalescing read cannot
    // deadlock against concurrent order writers.
    let due: Vec<DeliveredOrderClaim> = sqlx::query_as(
        "SELECT o.id, o.buyer_pubky, o.seller_pubky, o.fulfillment, \
                    o.shipment->>'delivered_at' AS delivered_at, \
                    CASE WHEN o.fulfillment = 'digital' THEN o.digital_delivered_at \
                         ELSE h.confirmed_at END AS handover_at, \
                    COALESCE(o.auto_complete_days::bigint, $2) AS auto_complete_days \
             FROM orders o LEFT JOIN pickup_handovers h ON h.order_id = o.id \
             WHERE o.state = 'delivered' \
             AND ( \
               (o.fulfillment = 'pickup' AND (h.confirmed_at IS NULL \
                  OR h.confirmed_at <= $1 - interval '1 day' * COALESCE(o.auto_complete_days::bigint, $2))) \
               OR (o.fulfillment = 'digital' \
                   AND (o.digital_delivered_at IS NULL \
                  OR o.digital_delivered_at <= $1 - interval '1 day' * COALESCE(o.auto_complete_days::bigint, $2))) \
               OR (o.fulfillment NOT IN ('pickup', 'digital') AND ( \
                    o.shipment->>'delivered_at' IS NULL \
                    OR o.shipment->>'delivered_at' !~ $3 \
                    OR left(o.shipment->>'delivered_at', 19) <= to_char( \
                         ($1 - interval '1 day' * COALESCE(o.auto_complete_days::bigint, $2)) \
                         AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS'))) \
             ) \
             ORDER BY o.id LIMIT $4 FOR UPDATE OF o SKIP LOCKED",
    )
    .bind(now)
    .bind(default_auto_complete_days)
    .bind(SHIPMENT_INSTANT_PREFIX)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let claimed = due.len() as u64;
    let mut processed = 0u64;
    for claim in due {
        let DeliveredOrderClaim {
            id: order_id,
            buyer_pubky,
            seller_pubky,
            fulfillment,
            delivered_at: shipment_delivered_at,
            handover_at,
            auto_complete_days,
        } = claim;
        let cutoff = now - chrono::Duration::days(auto_complete_days);
        if fulfillment == "pickup" || fulfillment == "digital" {
            // A delivered pickup order always carries its handover row, and a
            // delivered digital order its `digital_delivered_at`
            // (written in the confirm transaction). A missing row is an
            // anomaly to skip — never the warn-and-skip-forever loop the
            // shipment-only sweep would have emitted for pickup rows (§A6).
            let Some(delivered_at) = handover_at else {
                continue;
            };
            if delivered_at > cutoff {
                continue;
            }
        } else {
            let Some(delivered_at) = parse_shipment_instant(shipment_delivered_at.as_deref())
            else {
                tracing::warn!(
                    order_id = %order_id,
                    "skipping delivered order with malformed shipment timestamp"
                );
                continue;
            };
            if delivered_at > cutoff {
                continue;
            }
        }
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "delivered",
            "completed"
        ));
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE orders SET revision = revision + 1, state = 'completed', updated_at = $2 \
             WHERE id = $1 AND state = 'delivered' RETURNING revision",
        )
        .bind(order_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        let aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &aggregate_id,
            revision,
            SYSTEM_ACTOR,
            "order.completed",
            now,
        )
        .await?;
        for recipient in [&buyer_pubky, &seller_pubky] {
            insert_notification_intent(
                &mut tx,
                event_id,
                "order_completed",
                recipient,
                SYSTEM_ACTOR,
                &aggregate_id,
                None,
                now,
            )
            .await?;
        }
        tracing::info!(order_id = %order_id, "auto-completed order on server time");
        processed += 1;
    }
    tx.commit().await?;
    Ok(DeliverySweepClaim { claimed, processed })
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct WorkerSummary {
    pub reservations_expired: u64,
    pub drops_transitioned: u64,
    pub offers_expired: u64,
    pub auctions_started: u64,
    pub auctions_closed: u64,
    pub auction_close_failures: u64,
    pub outbox_delivered: u64,
    pub locks_completions_applied: u64,
    pub locks_snapshots_purged: u64,
    pub paykit_payments_applied: u64,
    pub payment_windows_expired: u64,
    pub stat_attestations_signed: u64,
    pub deliveries_assumed: u64,
    pub orders_auto_completed: u64,
    pub pickup_rows_resealed: u64,
    pub pickup_snapshots_purged: u64,
    pub pickup_versions_purged: u64,
    pub digital_rows_resealed: u64,
    pub priv_keys_resealed: u64,
    pub delivery_emails_purged: u64,
    pub delivery_addresses_purged: u64,
    pub read_rate_buckets_purged: u64,
    pub seller_windows_routed: u64,
    pub manual_review_sla_alerts: u64,
    pub manual_reviews_abandoned: u64,
    pub resolve_rows_delivered: u64,
    pub fx_samples_accepted: u64,
    pub refusal_audit_rows_purged: u64,
    pub listings_tombstoned: u64,
    pub reviews_released: u64,
    pub attestor_records_published: u64,
}

/// Samples the bounded sole source once per server-time bucket, one sample
/// per fiat currency from a single fetch. The unique `(currency, bucket)`
/// constraint is the concurrency cap; invalid samples never reach the table,
/// and each currency's sample is checked against that currency's own
/// persisted median. The endpoint and ticker contract are pinned in `fx.rs`.
/// Returns the number of samples accepted across all currencies.
pub async fn sample_fx_rate(
    pool: &PgPool,
    feed_url: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let snapshot = match fx::fetch_snapshot(feed_url, now).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(reason = ?error, "FX reference sample rejected");
            return Ok(0);
        }
    };
    let bucket_seconds = now.timestamp() - now.timestamp().rem_euclid(FX_SAMPLE_INTERVAL_SECONDS);
    let bucket = DateTime::from_timestamp(bucket_seconds, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid FX sample bucket"))?;
    let mut accepted = 0;
    for (currency, sample) in snapshot.samples() {
        let sample = match sample {
            Ok(sample) => sample,
            Err(error) => {
                tracing::warn!(
                    currency = currency.code(),
                    reason = ?error,
                    "FX reference sample rejected"
                );
                continue;
            }
        };
        let reference: Vec<(String,)> = sqlx::query_as(
            "SELECT rate::text FROM fx_rate_samples \
             WHERE currency = $1 AND accepted_at >= $2",
        )
        .bind(currency.code())
        .bind(fx::reference_cutoff(now))
        .fetch_all(pool)
        .await?;
        if let Some(median) = fx::median(
            reference
                .into_iter()
                .filter_map(|(rate,)| fx::Decimal::parse(&rate))
                .collect(),
        ) {
            if !fx::within_deviation(&sample.rate, &median) {
                tracing::warn!(
                    currency = currency.code(),
                    reason = "deviation_exceeded",
                    "FX reference sample rejected"
                );
                continue;
            }
        }
        accepted += sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ($1, $2, $3, $4, $5, 'blocktank') \
             ON CONFLICT (currency, sample_bucket) DO NOTHING",
        )
        .bind(currency.code())
        .bind(
            sqlx::types::BigDecimal::from_str(&sample.rate.to_string_value())
                .expect("validated FX rate is a decimal"),
        )
        .bind(sample.fetched_at)
        .bind(now)
        .bind(bucket)
        .execute(pool)
        .await?
        .rows_affected();
    }
    sqlx::query(
        "DELETE FROM fx_rate_samples \
         WHERE accepted_at < $1 - interval '3660 seconds'",
    )
    .bind(now)
    .execute(pool)
    .await?;
    Ok(accepted)
}

/// One worker pass: for each task, take the lease, drain, release. Tasks
/// whose lease is held by another live instance are skipped. A drain error
/// propagates only after the release, so a failing task never squats on
/// its lease until expiry. Panics are NOT caught (there is no
/// catch_unwind): a panicking task takes the pass down and leaves its
/// lease to expire on its own.
pub async fn run_once(
    state: &AppState,
    holder: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<WorkerSummary> {
    let lease_seconds = state.config.worker_lease_seconds;
    let mut summary = WorkerSummary::default();

    if let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    {
        state
            .payment_availability
            .rail_health(
                state.clock.as_ref(),
                state.config.paykit_poll_seconds,
                state.config.paykit_rail_stale_seconds,
                || async { paykit.rail_health().await },
            )
            .await;
        state
            .payment_availability
            .evict_stale(now, state.config.paykit_rail_stale_seconds)
            .await;
    }

    if try_acquire_lease(
        &state.pool,
        TASK_RESERVATION_EXPIRY,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = expiry::expire_due_reservations(&state.pool, now).await;
        release_lease(&state.pool, TASK_RESERVATION_EXPIRY, holder, now).await?;
        summary.reservations_expired = result?;
    }
    if try_acquire_lease(
        &state.pool,
        TASK_DROP_TRANSITIONS,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = transition_due_drops(&state.pool, now).await;
        release_lease(&state.pool, TASK_DROP_TRANSITIONS, holder, now).await?;
        summary.drops_transitioned = result?;
    }
    if try_acquire_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now, lease_seconds).await? {
        let result = expire_due_offers(&state.pool, now).await;
        release_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now).await?;
        summary.offers_expired = result?;
    }
    if try_acquire_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now, lease_seconds).await? {
        let result = async {
            let started = start_due_auctions(&state.pool, now).await?;
            let closed = close_due_auctions(&state.pool, state.homeserver.as_deref(), now).await?;
            anyhow::Ok((started, closed))
        }
        .await;
        release_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now).await?;
        let (started, result) = result?;
        summary.auctions_started = started;
        summary.auctions_closed = result.closed;
        summary.auction_close_failures = result.failed;
    }
    if try_acquire_lease(&state.pool, TASK_OUTBOX, holder, now, lease_seconds).await? {
        let paykit = state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        let result = drain_outbox(&state.pool, paykit, now, lease_seconds).await;
        release_lease(&state.pool, TASK_OUTBOX, holder, now).await?;
        summary.outbox_delivered = result?;
    }
    // Verification runs only when the deployment has Locks configured (fail
    // closed); the window sweep is a marketplace-time transition and always
    // runs, so correlations registered before a config change still expire.
    if let Some(locks) = &state.locks {
        if try_acquire_lease(
            &state.pool,
            TASK_LOCKS_VERIFICATION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = verify_due_locks_lifecycles(state, locks, now).await;
            // The checkout-snapshot retention purge runs UNDER the same
            // lease — never after it is released — so concurrent
            // instances cannot purge simultaneously. Bounded at 500 rows
            // per pass, oldest first: a backlog drains incrementally
            // without delaying lifecycle work.
            let purge = crate::locks::purge_terminal_locks_checkout_snapshots(
                &state.pool,
                now,
                state.config.locks_snapshot_retention_days,
            )
            .await;
            release_lease(&state.pool, TASK_LOCKS_VERIFICATION, holder, now).await?;
            summary.locks_completions_applied = result?;
            summary.locks_snapshots_purged = purge?;
        }
    }
    // Paykit verification runs only when the deployment carries the signed
    // Paykit client (fail closed).
    if let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    {
        if try_acquire_lease(
            &state.pool,
            TASK_PAYKIT_VERIFICATION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = verify_due_paykit_payments(state, paykit, now).await;
            release_lease(&state.pool, TASK_PAYKIT_VERIFICATION, holder, now).await?;
            summary.paykit_payments_applied = result?;
        }
    }
    if try_acquire_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now, lease_seconds).await? {
        let result = expire_due_payment_windows(state, now).await;
        release_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now).await?;
        summary.payment_windows_expired = result?;
    }
    if try_acquire_lease(&state.pool, TASK_FX_SAMPLER, holder, now, lease_seconds).await? {
        let result = sample_fx_rate(&state.pool, &state.config.fx_feed_url, now).await;
        release_lease(&state.pool, TASK_FX_SAMPLER, holder, now).await?;
        summary.fx_samples_accepted = result?;
    }
    if let Some(retention_pool) = &state.refusal_audit_retention_pool {
        if try_acquire_lease(
            &state.pool,
            TASK_REFUSAL_AUDIT_RETENTION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = crate::refusal_audit::purge_once(retention_pool, now).await;
            release_lease(&state.pool, TASK_REFUSAL_AUDIT_RETENTION, holder, now).await?;
            match result {
                Ok(purged) => {
                    summary.refusal_audit_rows_purged = purged as u64;
                    if let Some(audit) = &state.refusal_audit {
                        audit.record_purge_success(purged as u64);
                    }
                }
                Err(_) => {
                    if let Some(audit) = &state.refusal_audit {
                        audit.record_purge_failure();
                    }
                    tracing::warn!("refusal audit retention pass failed; domain work continues");
                }
            }
        }
    }
    // The shared_manual seller-confirmation window (§B.8.8): elapsed
    // windows route to manual_review with the hold preserved.
    if try_acquire_lease(
        &state.pool,
        TASK_SELLER_CONFIRMATION_WINDOW,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = crate::bitcoin_review::route_due_seller_confirmation_windows(state, now).await;
        release_lease(&state.pool, TASK_SELLER_CONFIRMATION_WINDOW, holder, now).await?;
        summary.seller_windows_routed = result?;
    }
    // The manual-review watch: two-business-day SLA alerts, then the
    // seven-day inactivity abandonment (same CAS as the seller's resolve).
    if try_acquire_lease(
        &state.pool,
        TASK_MANUAL_REVIEW_WATCH,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = crate::bitcoin_review::watch_manual_reviews(state, now).await;
        release_lease(&state.pool, TASK_MANUAL_REVIEW_WATCH, holder, now).await?;
        let (sla_alerts, abandoned) = result?;
        summary.manual_review_sla_alerts = sla_alerts;
        summary.manual_reviews_abandoned = abandoned;
    }
    // The pinned paykit.resolve delivery arm (§B.8.8 mapping + deadline).
    if let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    {
        if try_acquire_lease(
            &state.pool,
            TASK_PAYKIT_RESOLVE_DELIVERY,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = crate::resolve_delivery::deliver_due_resolve_rows(
                state,
                paykit,
                now,
                lease_seconds,
            )
            .await;
            release_lease(&state.pool, TASK_PAYKIT_RESOLVE_DELIVERY, holder, now).await?;
            summary.resolve_rows_delivered = result?;
        }
    }
    // Post-purchase liveness is server time (no carrier tracking feed):
    // assume delivery, then auto-complete, after the periods the seller set
    // (captured per order); DELIVERY_ASSUME_DAYS / AUTO_COMPLETE_DAYS apply
    // only where the seller left one unset. A return/cancel request that is
    // open takes the order out of the sweep.
    if try_acquire_lease(
        &state.pool,
        TASK_DELIVERY_AUTOCOMPLETE,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = async {
            summary.deliveries_assumed = assume_due_deliveries(
                &state.pool,
                now,
                state.config.delivery_assume_days,
                state.config.delivery_sweep_batch_size,
                DELIVERY_SWEEP_MAX_BATCHES,
            )
            .await?;
            summary.orders_auto_completed = complete_due_delivered_orders(
                &state.pool,
                now,
                state.config.auto_complete_days,
                state.config.delivery_sweep_batch_size,
                DELIVERY_SWEEP_MAX_BATCHES,
            )
            .await?;
            anyhow::Ok(())
        }
        .await;
        release_lease(&state.pool, TASK_DELIVERY_AUTOCOMPLETE, holder, now).await?;
        result?;
    }
    // Double-blind reviews whose window has ended publish on server time. The
    // pass runs whether or not REVIEW_BLIND_WINDOW_DAYS is still set: an order
    // that already carries a window is released when it ends.
    if try_acquire_lease(&state.pool, TASK_REVIEW_RELEASE, holder, now, lease_seconds).await? {
        let result = crate::review_release::release_due_orders(
            &state.pool,
            now,
            state.config.delivery_sweep_batch_size,
            DELIVERY_SWEEP_MAX_BATCHES,
        )
        .await;
        release_lease(&state.pool, TASK_REVIEW_RELEASE, holder, now).await?;
        summary.reviews_released = result?;
    }
    // The attestor's own records (profile, refund and return annotations). A
    // failed pass is logged inside and retried under each path's backoff, so it
    // never holds up the tasks after it.
    if state.attestor_publisher.is_some() {
        summary.attestor_records_published =
            crate::attestor_publish::run_pass(state, holder, now).await;
    }
    // Pickup maintenance runs only when the sealing keys are configured:
    // the retention purge of pinned versions/snapshots whose referencing
    // orders went terminal, and — while a previous key is configured — the
    // dual-key re-seal job across BOTH sealed families (§A1/§A3).
    if let Some(pickup) = &state.pickup {
        if try_acquire_lease(
            &state.pool,
            TASK_PICKUP_RETENTION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = crate::pickup::purge_terminal_pickup_retention(
                &state.pool,
                now,
                state.config.pickup_dispute_retention_days,
            )
            .await;
            release_lease(&state.pool, TASK_PICKUP_RETENTION, holder, now).await?;
            let (snapshots_purged, versions_purged) = result?;
            summary.pickup_snapshots_purged = snapshots_purged;
            summary.pickup_versions_purged = versions_purged;
        }
        if pickup.has_previous()
            && try_acquire_lease(&state.pool, TASK_PICKUP_RESEAL, holder, now, lease_seconds)
                .await?
        {
            let result = crate::pickup::reseal_previous_key_batch(&state.pool, pickup, now).await;
            release_lease(&state.pool, TASK_PICKUP_RESEAL, holder, now).await?;
            // A failed re-seal pass (a permanently unopenable row) is a
            // PER-TASK failure: it is logged with its unopenable count and
            // the tick continues — it must not skip the remaining tasks or
            // discard the tick summary on every pass. The pass retries on
            // the next tick; only the OTHER tasks' failures fail run_once.
            match result {
                Ok(progress) => {
                    summary.pickup_rows_resealed =
                        progress.details_resealed + progress.snapshots_resealed;
                    if progress.remaining_under_previous == 0 && summary.pickup_rows_resealed > 0 {
                        tracing::info!(
                            resealed = summary.pickup_rows_resealed,
                            "pickup key rotation complete: zero rows remain under the previous \
                             key across both sealed families"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "pickup re-seal pass failed; continuing with the remaining worker tasks"
                    );
                }
            }
        }
    }
    // While a previous digital delivery key is configured, one re-seal pass
    // per tick across all three sealed families (digital delivery design
    // §7). A failed pass is logged and retried next tick, as pickup's is.
    // The buyer delivery email purge (§4.3 step 7). It runs whether or not
    // the key is configured: deleting ciphertext needs no key.
    if try_acquire_lease(
        &state.pool,
        TASK_DELIVERY_EMAIL_PURGE,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = crate::handlers::digital_manual::purge_delivery_emails(
            &state.pool,
            now,
            state.config.buyer_email_retention_days,
            state.config.buyer_email_unpaid_retention_days,
        )
        .await;
        release_lease(&state.pool, TASK_DELIVERY_EMAIL_PURGE, holder, now).await?;
        summary.delivery_emails_purged = result?;
    }
    // The delivery address leaves the service when the order ends: sealed
    // envelopes and legacy plaintext alike. It needs no key.
    if try_acquire_lease(
        &state.pool,
        TASK_DELIVERY_ADDRESS_PURGE,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result =
            crate::handlers::delivery_envelope::purge_final_delivery_addresses(&state.pool).await;
        release_lease(&state.pool, TASK_DELIVERY_ADDRESS_PURGE, holder, now).await?;
        summary.delivery_addresses_purged = result?;
    }
    // Download-rate buckets idle past READ_RATE_LIMIT_IDLE. Deleting one
    // changes no later read: it has refilled to a full bucket, which is
    // what the next read seeds.
    if try_acquire_lease(
        &state.pool,
        TASK_READ_RATE_PURGE,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = crate::handlers::digital_orders::purge_idle_read_buckets(
            &state.pool,
            now,
            crate::handlers::digital_orders::READ_RATE_LIMIT_PURGE_BATCH,
            crate::handlers::digital_orders::READ_RATE_LIMIT_PURGE_MAX_BATCHES,
        )
        .await;
        release_lease(&state.pool, TASK_READ_RATE_PURGE, holder, now).await?;
        summary.read_rate_buckets_purged = result?;
    }
    if let Some(digital) = &state.digital {
        if digital.has_previous()
            && try_acquire_lease(&state.pool, TASK_DIGITAL_RESEAL, holder, now, lease_seconds)
                .await?
        {
            let result = crate::digital::reseal_previous_key_batch(&state.pool, digital).await;
            release_lease(&state.pool, TASK_DIGITAL_RESEAL, holder, now).await?;
            match result {
                Ok(progress) => {
                    summary.digital_rows_resealed = progress.versions_resealed
                        + progress.pins_resealed
                        + progress.emails_resealed;
                    if progress.remaining_under_previous == 0 && summary.digital_rows_resealed > 0 {
                        tracing::info!(
                            resealed = summary.digital_rows_resealed,
                            "digital delivery key rotation complete: zero rows remain under the \
                             previous key across all three sealed families"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "digital delivery re-seal pass failed; continuing with the remaining \
                         worker tasks"
                    );
                }
            }
        }
    }
    // While a previous priv data key sealing key is configured, one re-seal
    // pass per tick over `user_priv_keys`. A failed pass is logged and
    // retried next tick, as the digital delivery pass is.
    if let Some(priv_keys) = &state.priv_keys {
        if priv_keys.has_previous()
            && try_acquire_lease(
                &state.pool,
                TASK_PRIV_KEY_RESEAL,
                holder,
                now,
                lease_seconds,
            )
            .await?
        {
            let result =
                crate::priv_keys::reseal_previous_key_batch(&state.pool, priv_keys, now).await;
            release_lease(&state.pool, TASK_PRIV_KEY_RESEAL, holder, now).await?;
            match result {
                Ok(progress) => {
                    summary.priv_keys_resealed = progress.resealed;
                    if progress.remaining_under_previous == 0 && progress.resealed > 0 {
                        tracing::info!(
                            resealed = progress.resealed,
                            "priv data key rotation complete: zero rows remain under the \
                             previous key"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "priv data key re-seal pass failed; continuing with the remaining \
                         worker tasks"
                    );
                }
            }
        }
    }
    // Weekly seller stat attestations (ratified D3) run only when the
    // deployment holds the attestor key: unsigned stats would be worthless.
    if let Some(attestor) = &state.attestor {
        if try_acquire_lease(
            &state.pool,
            TASK_STAT_ATTESTATIONS,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = generate_due_stat_attestations(&state.pool, attestor, now).await;
            release_lease(&state.pool, TASK_STAT_ATTESTATIONS, holder, now).await?;
            summary.stat_attestations_signed = result?;
        }
    }
    // Follows sellers' homeserver deletes into listing tombstones. Last in
    // the pass because it waits on the network; a failed pass is logged and
    // retried next tick.
    if let Some(homeserver) = state.homeserver.as_deref() {
        // The lease is taken at the current time, not the pass start: the
        // tasks above may have run for a while, and the follower's writes
        // check the lease against the clock.
        // Both deadlines count from before the acquisition, so time spent
        // waiting on the lease row comes out of the pass, not on top of
        // the lease.
        let started = tokio::time::Instant::now();
        let deadline = started
            + std::time::Duration::from_millis(state.config.listing_deletion_pass_budget_ms);
        let lease_deadline =
            started + std::time::Duration::from_secs(u64::try_from(lease_seconds).unwrap_or(0));
        let lease_now = state.clock.now();
        if let Some(fence) = crate::listing_deletion::acquire_follower_lease(
            &state.pool,
            holder,
            lease_now,
            lease_seconds,
            deadline,
        )
        .await?
        {
            let pass = crate::listing_deletion::FollowerPass {
                pool: &state.pool,
                homeserver,
                clock: state.clock.as_ref(),
                holder,
                fence,
                deadline,
                lease_deadline,
            };
            let result = crate::listing_deletion::follow_homeserver_deletions(&pass).await;
            pass.release_lease().await?;
            match result {
                Ok(tombstoned) => summary.listings_tombstoned = tombstoned,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "listing deletion follower pass failed; retrying next tick"
                    );
                }
            }
        }
    }
    Ok(summary)
}

/// One row of the per-order event aggregate the stat computation reads.
#[derive(Debug, sqlx::FromRow)]
struct SellerOrderStats {
    paid_at: Option<DateTime<Utc>>,
    shipped_at: Option<DateTime<Utc>>,
    delivered_at: Option<DateTime<Utc>>,
    cancelled: Option<bool>,
    /// A refund was recorded and the order is still `refunded_external`: a
    /// canceled PayPal reversal that reopened the order is not a refund.
    refunded: Option<bool>,
    /// The order's terminal cancel was a buyer-protection exit (§A3):
    /// `order.cancelled_terms_change` excludes the WHOLE order from
    /// `terminated_badly`, including any `refund.recorded_external` leg.
    terms_change_cancelled: Option<bool>,
    /// The order completed on server time (the `order_auto_complete`
    /// sweep), as opposed to a buyer review.
    auto_completed: Option<bool>,
    /// The buyer opened a cancel or return request on the order.
    buyer_disputed: Option<bool>,
    fulfillment: String,
    /// Who confirmed the pickup handover (`buyer` | `seller`), when one
    /// exists; NULL for shipped orders and unconfirmed pickups.
    handover_confirmed_by: Option<String>,
}

/// Computes and signs the weekly per-seller stat attestations (ratified D3:
/// median time-to-ship, completion rate — banded and
/// per-mille, never raw amounts). A seller is due when they have at least
/// one delivered order in the trailing window and no attestation newer than
/// the weekly cadence. Rows are stored for the Phase 3 attestor-homeserver
/// publisher; nothing is public yet.
pub async fn generate_due_stat_attestations(
    pool: &PgPool,
    attestor: &crate::attestor::Attestor,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let window_start = now - chrono::Duration::days(STAT_ATTESTATION_WINDOW_DAYS);
    let stale_before = now - chrono::Duration::days(STAT_ATTESTATION_INTERVAL_DAYS);

    let due_sellers: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT o.seller_pubky \
         FROM events e JOIN orders o ON e.aggregate_id = 'order:' || o.id::text \
         WHERE e.kind = 'fulfillment.delivered' AND e.occurred_at >= $1 AND e.occurred_at <= $2 \
           AND NOT EXISTS ( \
             SELECT 1 FROM seller_stat_attestations s \
             WHERE s.seller_pubky = o.seller_pubky AND s.created_at > $3)",
    )
    .bind(window_start)
    .bind(now)
    .bind(stale_before)
    .fetch_all(pool)
    .await?;

    let mut signed = 0u64;
    for (seller_pubky,) in due_sellers {
        // `receipt.issued` is the paid marker on the ORDER aggregate: it is
        // written in the same transaction as the payment confirmation,
        // exactly once per order (payment.confirmed lives on the payment
        // aggregate and is not visible in this per-order join).
        let per_order: Vec<SellerOrderStats> = sqlx::query_as(
            "SELECT \
               MIN(CASE WHEN e.kind = 'receipt.issued' THEN e.occurred_at END) AS paid_at, \
               MIN(CASE WHEN e.kind = 'fulfillment.shipped' THEN e.occurred_at END) AS shipped_at, \
               MIN(CASE WHEN e.kind = 'fulfillment.delivered' THEN e.occurred_at END) AS delivered_at, \
               BOOL_OR(e.kind = 'order.cancelled') AS cancelled, \
               (BOOL_OR(e.kind = 'refund.recorded_external') \
                AND o.state = 'refunded_external') AS refunded, \
               BOOL_OR(e.kind = 'order.cancelled_terms_change') AS terms_change_cancelled, \
               BOOL_OR(e.kind = 'order.completed') AS auto_completed, \
               BOOL_OR(e.kind IN ('order.cancel_requested', 'order.cancelled', \
                                  'order.cancelled_terms_change', 'return.requested')) \
                 AS buyer_disputed, \
               o.fulfillment AS fulfillment, \
               (SELECT h.confirmed_by FROM pickup_handovers h WHERE h.order_id = o.id) \
                 AS handover_confirmed_by \
             FROM orders o JOIN events e ON e.aggregate_id = 'order:' || o.id::text \
             WHERE o.seller_pubky = $1 AND e.occurred_at >= $2 AND e.occurred_at <= $3 \
             GROUP BY o.id",
        )
        .bind(&seller_pubky)
        .bind(window_start)
        .bind(now)
        .fetch_all(pool)
        .await?;

        // The confirming-actor rule (§A6): a pickup completion counts only
        // when the BUYER confirmed the handover, or when the order
        // auto-completed with no buyer cancel or return in the window. A
        // seller-unilateral confirm is never reputation-positive on its
        // own — the seller is paid at payment time, so a self-confirm that
        // also minted completion reputation would pay a fraudulent seller
        // twice. Shipped orders count on delivery, as today.
        let completed = per_order
            .iter()
            .filter(|order| {
                if order.delivered_at.is_none() {
                    return false;
                }
                if order.fulfillment != "pickup" {
                    return true;
                }
                order.handover_confirmed_by.as_deref() == Some("buyer")
                    || (order.auto_completed.unwrap_or(false)
                        && !order.buyer_disputed.unwrap_or(false))
            })
            .count() as i64;
        if completed < 1 {
            continue;
        }
        // `order.cancelled_terms_change` excludes the WHOLE order from
        // `terminated_badly` (§A3): a buyer can never ding the seller's
        // completion rate by exercising a buyer-protection exit, even when
        // a refund was recorded on the same order.
        let terminated_badly = per_order
            .iter()
            .filter(|order| {
                (order.cancelled.unwrap_or(false) || order.refunded.unwrap_or(false))
                    && !order.terms_change_cancelled.unwrap_or(false)
            })
            .count() as i64;
        let mut ship_hours: Vec<i64> = per_order
            .iter()
            .filter_map(|order| match (order.paid_at, order.shipped_at) {
                (Some(paid), Some(shipped)) if shipped >= paid => {
                    Some((shipped - paid).num_hours())
                }
                _ => None,
            })
            .collect();
        ship_hours.sort_unstable();
        let median_ship_hours = median(&ship_hours);

        let completion_rate_permille = completed * 1_000 / (completed + terminated_badly);
        let period_from = window_start.format("%Y-%m-%d").to_string();
        let period_to = now.format("%Y-%m-%d").to_string();

        // Banded count and per-mille rates, never raw counts or amounts
        // (design §7.2): exact GMV/volume stays private while remaining
        // rankable.
        let body = serde_json::json!({
            "v": 1,
            "attestor": attestor.pubky(),
            "seller": seller_pubky,
            "period": { "from": period_from, "to": period_to },
            "ordersCompletedBand": completed.ilog10().to_string(),
            "medianTimeToShipHours": median_ship_hours,
            "completionRatePermille": completion_rate_permille,
        });
        let jws = attestor.sign_seller_stats(&body);
        let inserted = sqlx::query(
            "INSERT INTO seller_stat_attestations \
             (id, seller_pubky, period_from, period_to, body, jws, created_at) \
             VALUES ($1, $2, $3::date, $4::date, $5, $6, $7) \
             ON CONFLICT (seller_pubky, period_to) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(&seller_pubky)
        .bind(&period_from)
        .bind(&period_to)
        .bind(&body)
        .bind(&jws)
        .bind(now)
        .execute(pool)
        .await?;
        signed += inserted.rows_affected();
    }
    Ok(signed)
}

/// Median of a sorted slice, `None` when empty (an honest absence beats a
/// fabricated zero).
fn median(sorted: &[i64]) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2
    } else {
        sorted[middle]
    })
}

/// Spawns the periodic worker runtime used by the production binary. Each
/// process gets a unique holder id; multiple instances coordinate through
/// the lease table.
pub fn spawn(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let holder = Uuid::new_v4();
        let interval = std::time::Duration::from_secs(state.config.worker_interval_seconds);
        loop {
            tokio::time::sleep(interval).await;
            let now = state.clock.now();
            match run_once(&state, holder, now).await {
                Ok(summary) => {
                    if summary != WorkerSummary::default() {
                        tracing::info!(
                            reservations_expired = summary.reservations_expired,
                            drops_transitioned = summary.drops_transitioned,
                            offers_expired = summary.offers_expired,
                            auctions_started = summary.auctions_started,
                            auctions_closed = summary.auctions_closed,
                            auction_close_failures = summary.auction_close_failures,
                            outbox_delivered = summary.outbox_delivered,
                            locks_completions_applied = summary.locks_completions_applied,
                            locks_snapshots_purged = summary.locks_snapshots_purged,
                            paykit_payments_applied = summary.paykit_payments_applied,
                            payment_windows_expired = summary.payment_windows_expired,
                            stat_attestations_signed = summary.stat_attestations_signed,
                            deliveries_assumed = summary.deliveries_assumed,
                            orders_auto_completed = summary.orders_auto_completed,
                            pickup_rows_resealed = summary.pickup_rows_resealed,
                            pickup_snapshots_purged = summary.pickup_snapshots_purged,
                            pickup_versions_purged = summary.pickup_versions_purged,
                            seller_windows_routed = summary.seller_windows_routed,
                            manual_review_sla_alerts = summary.manual_review_sla_alerts,
                            manual_reviews_abandoned = summary.manual_reviews_abandoned,
                            resolve_rows_delivered = summary.resolve_rows_delivered,
                            fx_samples_accepted = summary.fx_samples_accepted,
                            refusal_audit_rows_purged = summary.refusal_audit_rows_purged,
                            read_rate_buckets_purged = summary.read_rate_buckets_purged,
                            delivery_addresses_purged = summary.delivery_addresses_purged,
                            listings_tombstoned = summary.listings_tombstoned,
                            reviews_released = summary.reviews_released,
                            "worker pass completed"
                        );
                    }
                }
                Err(error) => tracing::error!(error = %error, "worker pass failed"),
            }
        }
    })
}
