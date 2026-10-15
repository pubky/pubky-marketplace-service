//! Server-time reservation expiry (ADR-0019: "inventory reservations and
//! expiration" belong to this service; server time decides deadlines).
//!
//! A background worker sweeps active reservations whose `expires_at` has
//! passed, marks them expired, returns the held quantity to the listing, and
//! appends an `inventory.reservation_expired` event (traceable through the
//! original reserve command id).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
struct DueReservation {
    id: Uuid,
    listing_aggregate_id: String,
    buyer_pubky: String,
    quantity: i64,
    drop_aggregate_id: Option<String>,
    variant_id: Option<String>,
}

/// Expires every active reservation due at `now`. Returns the number of
/// reservations expired. Safe to run concurrently: due rows are locked with
/// `FOR UPDATE SKIP LOCKED` and the status flip is guarded by `status =
/// 'active'`.
pub async fn expire_due_reservations(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    // The exclusion: an auction winner's reservation whose order is awaiting
    // seller confirmation or sits in Paykit manual review is PRESERVED —
    // the buyer demonstrably paid on chain, and the stock stays held until
    // the seller resolves or the seven-day reaper abandons (§B.8.8: routing
    // to manual_review never releases the hold). Scoped to the Paykit
    // bitcoin rail; every other reservation sweeps exactly as before.
    let due: Vec<DueReservation> = sqlx::query_as(
        "SELECT id, listing_aggregate_id, buyer_pubky, quantity, drop_aggregate_id, variant_id \
         FROM reservations \
         WHERE status = 'active' AND expires_at <= $1 \
         AND offer_award_id IS NULL \
         AND NOT EXISTS (\
             SELECT 1 FROM orders o JOIN payments p ON p.order_id = o.id \
             WHERE o.auction_aggregate_id = reservations.listing_aggregate_id \
             AND o.buyer_pubky = reservations.buyer_pubky \
             AND o.payment_method IN ('bitcoin', 'paykit') AND p.adapter = 'paykit' \
             AND (o.paykit_request_state = 'awaiting_seller_confirmation' \
                  OR p.state = 'manual_review')) \
         ORDER BY expires_at FOR UPDATE SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for reservation in due {
        let flipped = sqlx::query(
            "UPDATE reservations SET status = 'expired', updated_at = $2 \
             WHERE id = $1 AND status = 'active'",
        )
        .bind(reservation.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if flipped.rows_affected() != 1 {
            continue;
        }

        // A drop-stamped hold credits its drop first (drop lock before the
        // listing lock, the shared order): while live the unit restocks; an
        // ended drop keeps honest books but nothing reopens.
        if let Some(drop_aggregate_id) = &reservation.drop_aggregate_id {
            if !crate::handlers::drops::credit_drop_release(
                &mut tx,
                drop_aggregate_id,
                &reservation.buyer_pubky,
                reservation.quantity,
                now,
            )
            .await?
            {
                anyhow::bail!(
                    "expired reservation {} could not credit drop {drop_aggregate_id}",
                    reservation.id
                );
            }
        }

        let rows =
            crate::variant_stock::lock_rows(&mut tx, &reservation.listing_aggregate_id).await?;
        let row = crate::variant_stock::resolve_held(&rows, reservation.variant_id.as_deref())
            .map_err(|refusal| {
                anyhow::anyhow!(
                    "expired reservation {} names no variant row of its listing: {refusal:?}",
                    reservation.id
                )
            })?;
        if !crate::variant_stock::release(
            &mut tx,
            &reservation.listing_aggregate_id,
            &row.variant_id,
            reservation.quantity,
            crate::variant_stock::Held::Reserved,
        )
        .await?
        {
            anyhow::bail!(
                "expired reservation {} is not held by its listing",
                reservation.id
            );
        }
        let new_revision: (i64,) = sqlx::query_as(&format!(
            "UPDATE listings SET server_revision = server_revision + 1, state = {}, \
             updated_at = $2 WHERE aggregate_id = $1 RETURNING server_revision",
            crate::variant_stock::STATE_FROM_TOTALS_SQL
        ))
        .bind(&reservation.listing_aggregate_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
             occurred_at) VALUES ($1, $2, $3, $4, $5, 'inventory.reservation_expired', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(reservation.id)
        .bind(&reservation.listing_aggregate_id)
        .bind(new_revision.0)
        .bind(&reservation.buyer_pubky)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tracing::info!(
            reservation_id = %reservation.id,
            aggregate_id = %reservation.listing_aggregate_id,
            "expired reservation and released inventory"
        );
        expired += 1;
    }
    tx.commit().await?;
    Ok(expired)
}
