//! Exclusive inventory holds: a payment lock point parks a unit.
//!
//! Ordinary `checkout.create` does **not** hold stock. The first acquire for
//! a buy-now order is the payment-method bind (`hold_source=bind`). Locks
//! registration and sandbox first-advance acquire (or re-arm) the same way
//! — they never double-decrement. `CHECKOUT_HOLD_WINDOW_SECONDS` is the
//! unbound idle cancel, not an inventory hold.
//!
//! - `POST /v0/orders/{id}/payment-method` — first ordinary acquire;
//!   fiat `FIAT_PAYMENT_WINDOW_SECONDS` (default 600), bitcoin
//!   `BITCOIN_PAYMENT_WINDOW_SECONDS` (default 1800, also Paykit
//!   `expires_at`);
//! - `payment.register_locks` — window `LOCKS_PAYMENT_WINDOW_SECONDS`;
//! - `payment.sandbox_advance` leaving `awaiting_entitlement` — window
//!   `SANDBOX_PAYMENT_WINDOW_SECONDS`.
//!
//! Auction orders are excluded: their hold is the winning `reservations`
//! row. Once that reservation is swept to `expired`, the payment-window
//! worker cancels the unpaid order without a second release. Drop-bound checkout keeps lock-at-claim and arms
//! `DROP_CLAIM_WINDOW_SECONDS` at checkout.
//!
//! The payment-window worker ([`crate::workers::expire_due_payment_windows`])
//! releases a lapsed hold, expires the payment, and cancels the order. An
//! unbound leftover (`stock_held=false`, no payment method) is cancelled
//! after `CHECKOUT_HOLD_WINDOW_SECONDS` with no inventory movement.

use chrono::{DateTime, Utc};
use marketplace_domain::state_machines::{can_transition, listing_machine};
use marketplace_domain::ErrorCode;
use sqlx::{Postgres, Transaction};

use crate::handlers::{fetch_listing_for_update, lock_listings_in_order, order_line_listings};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::CommandFailure;
use crate::variant_stock;

/// Refusal copy for a lock point that finds the stock already gone, pinned
/// by the client contract tests.
pub const SOLD_OUT_BEFORE_PAYMENT: &str = "The listing sold out before this payment started.";

/// Refusal copy when another buyer's live payment holds the unit. Sibling
/// bind (and checkout against an already-reserved listing) return this as
/// `INVALID_STATE` 409.
pub const HOLDING_COPY: &str = "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks.";

/// Refusal copy when the seller deleted the listing (or re-created it) after
/// this order was placed: the unpaid order can no longer take stock.
pub const LISTING_REMOVED_COPY: &str =
    "The seller removed this listing before this payment started.";

/// Historical `hold_source` written by #50 at `checkout.create`. Ordinary
/// create no longer writes it; in-flight rows may still carry it until TTL.
#[allow(dead_code)]
pub const HOLD_SOURCE_CHECKOUT: &str = "checkout";
pub const HOLD_SOURCE_LOCKS: &str = "locks";
pub const HOLD_SOURCE_BIND: &str = "bind";
pub const HOLD_SOURCE_SANDBOX: &str = "sandbox";
pub const HOLD_SOURCE_DROP_CLAIM: &str = "drop_claim";

/// Which listing quantity column an order's lines currently occupy:
/// `reserved` before payment confirmation, `sold` after it.
#[derive(Clone, Copy)]
pub(crate) enum HeldQuantity {
    Reserved,
    Sold,
}

impl HeldQuantity {
    fn held(self) -> variant_stock::Held {
        match self {
            HeldQuantity::Reserved => variant_stock::Held::Reserved,
            HeldQuantity::Sold => variant_stock::Held::Sold,
        }
    }
}

/// Moves each order line's quantity from the held column back to available,
/// on the variant row the line names, under the row's quantity-balance
/// constraint. The held-quantity guard is a compare-and-swap against the
/// ledger: nothing else can remove this order's contribution, so a shortfall
/// is an invariant violation, not a race to tolerate.
pub(crate) async fn release_lines(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    held: HeldQuantity,
    now: DateTime<Utc>,
) -> Result<Result<(), CommandFailure>, sqlx::Error> {
    lock_listings_in_order(tx, order_line_listings(order)).await?;
    let lines = order.lines.as_array().expect("order lines are an array");
    for line in lines {
        let aggregate_id = line["listing_aggregate_id"]
            .as_str()
            .expect("order line carries its listing aggregate id");
        let quantity = line["quantity"]
            .as_i64()
            .expect("order line carries its quantity");
        let Some(listing) = fetch_listing_for_update(tx, aggregate_id).await? else {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "An order line's listing is missing.",
            )));
        };
        debug_assert!(can_transition(
            &listing_machine(),
            &listing.state,
            "available"
        ));
        let rows = variant_stock::lock_rows(tx, aggregate_id).await?;
        let row = match variant_stock::resolve_held(&rows, line["variant_id"].as_str()) {
            Ok(row) => row,
            Err(refusal) => return Ok(Err(refusal.held_failure())),
        };
        if !variant_stock::release(tx, aggregate_id, &row.variant_id, quantity, held.held()).await?
        {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "The inventory held by this order is no longer accounted to its listing.",
            )));
        }
        sqlx::query(&format!(
            "UPDATE listings SET server_revision = server_revision + 1, \
             state = {}, updated_at = $2 WHERE aggregate_id = $1",
            variant_stock::STATE_FROM_TOTALS_SQL
        ))
        .bind(aggregate_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    }
    Ok(Ok(()))
}

/// Acquires (or re-arms) the order's inventory hold inside the caller's
/// transaction. Returns the order with its hold columns current.
///
/// - An auction order is untouched: its hold is the winning reservation.
/// - An order that is no longer `pending_payment` refuses the lock point —
///   a cancelled or already-paid order must never grab stock.
/// - An order that already holds stock never double-decrements; it re-arms
///   `hold_expires_at` and `hold_source` to this lock point's window.
/// - Otherwise each line atomically moves `available → reserved` under the
///   listing row lock (`INSUFFICIENT_INVENTORY` with
///   [`SOLD_OUT_BEFORE_PAYMENT`] when the stock is gone) and the hold
///   window arms at `now + window_seconds`.
pub async fn acquire_payment_hold(
    tx: &mut Transaction<'_, Postgres>,
    order: OrderRow,
    window_seconds: i64,
    hold_source: &str,
    now: DateTime<Utc>,
) -> Result<Result<OrderRow, CommandFailure>, sqlx::Error> {
    if order.auction_aggregate_id.is_some() {
        return Ok(Ok(order));
    }
    if order.state != "pending_payment" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only an order pending payment can start a payment.",
        )));
    }
    let hold_expires_at = now + chrono::Duration::seconds(window_seconds);
    if order.stock_held {
        let rearmed: OrderRow = sqlx::query_as(&format!(
            "UPDATE orders SET hold_expires_at = $2, hold_source = $3, updated_at = $4 \
             WHERE id = $1 RETURNING {ORDER_COLUMNS}"
        ))
        .bind(order.id)
        .bind(hold_expires_at)
        .bind(hold_source)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        return Ok(Ok(rearmed));
    }

    lock_listings_in_order(tx, order_line_listings(&order)).await?;
    let lines = order.lines.as_array().expect("order lines are an array");
    for line in lines {
        let aggregate_id = line["listing_aggregate_id"]
            .as_str()
            .expect("order line carries its listing aggregate id");
        let quantity = line["quantity"]
            .as_i64()
            .expect("order line carries its quantity");
        let Some(listing) = fetch_listing_for_update(tx, aggregate_id).await? else {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "An order line's listing is missing.",
            )));
        };
        if !listing.accepts_commitment_created_at(order.created_at) {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::NotFound,
                ErrorCode::NotFound,
                LISTING_REMOVED_COPY,
            )));
        }
        let rows = variant_stock::lock_rows(tx, aggregate_id).await?;
        let row = match variant_stock::resolve(&rows, line["variant_id"].as_str()) {
            Ok(row) => row,
            Err(refusal) => return Ok(Err(refusal.failure())),
        };
        let keyed = variant_stock::tracks_variants(&rows);
        if variant_stock::shortfall(row, quantity).is_some() {
            let held_by_another = if keyed {
                row.reserved_quantity > 0
            } else {
                listing.state == "reserved" || listing.reserved_quantity > 0
            };
            if held_by_another {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    HOLDING_COPY,
                )));
            }
            let refusal = CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InsufficientInventory,
                ErrorCode::InsufficientInventory,
                SOLD_OUT_BEFORE_PAYMENT,
            );
            return Ok(Err(if keyed {
                refusal.with_reason(variant_stock::REASON_VARIANT_SOLD_OUT)
            } else {
                refusal
            }));
        }
        let new_state = variant_stock::state_after_claim(
            listing.available_quantity,
            quantity,
            listing.made_to_order,
        );
        debug_assert!(can_transition(
            &listing_machine(),
            &listing.state,
            new_state
        ));
        if !variant_stock::reserve(tx, aggregate_id, &row.variant_id, quantity).await? {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InsufficientInventory,
                ErrorCode::InsufficientInventory,
                SOLD_OUT_BEFORE_PAYMENT,
            )));
        }
        sqlx::query(
            "UPDATE listings SET server_revision = server_revision + 1, state = $2, \
             updated_at = $3 WHERE aggregate_id = $1",
        )
        .bind(aggregate_id)
        .bind(new_state)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    }

    let held: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET stock_held = true, hold_expires_at = $2, hold_source = $3, \
         updated_at = $4 WHERE id = $1 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(hold_expires_at)
    .bind(hold_source)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Ok(held))
}
