//! Per-variant stock: the rows behind a listing's quantity columns.
//!
//! `listing_variant_inventory` (migration 0070) holds one row per variant
//! with the same balance rule as `listings` (available + reserved + sold =
//! total). A listing that sells one variant keeps a single IMPLICIT row
//! (`variant_id = ''`) equal to its own totals; a listing whose record has two
//! or more enabled variants keeps one row per variant, keyed by the record's
//! variant id, and tracks stock per variant.
//!
//! The listing's quantity columns are the sum of its rows. Database triggers
//! keep them equal in the same statement as every row change, so the code
//! here moves stock between the available, reserved and sold columns of ONE
//! row and never writes a listing total itself. Every caller takes the
//! listing row lock before it touches a row ([`lock_rows`] does so), the one
//! lock order the whole service uses, so row updates and the listing
//! roll-up cannot deadlock against a path that locks the listing first.
//!
//! A MADE-TO-ORDER row (migration 0073) sells without a stock count: its
//! store does not track the variant's inventory or keeps selling at zero.
//! Its `available_quantity` is always 0. A claim adds its units to the row's
//! total as it reserves them and a release takes them out again, so the
//! balance rule holds and no unit is invented; every other move (reserved to
//! sold, a hold's expiry, a cancel) is the same as on a tracked row. Only
//! [`set_made_to_order`] sets or clears the flag.

use std::collections::{HashMap, HashSet};

use marketplace_domain::commands::VariantStock;
use marketplace_domain::ErrorCode;
use sqlx::{FromRow, Postgres, Transaction};

use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;

/// The `variant_id` of the row of a listing that sells one variant. Never a
/// valid record variant id.
pub const IMPLICIT_VARIANT_ID: &str = "";

pub const REASON_VARIANT_REQUIRED: &str = "variant_required";
pub const REASON_UNKNOWN_VARIANT: &str = "unknown_variant";
pub const REASON_VARIANT_UNAVAILABLE: &str = "variant_unavailable";
pub const REASON_VARIANT_SOLD_OUT: &str = "variant_sold_out";
pub const REASON_VARIANT_BELOW_COMMITTED: &str = "variant_below_committed";

#[derive(Debug, Clone, FromRow)]
pub struct VariantRow {
    pub variant_id: String,
    pub sku: Option<String>,
    pub enabled: bool,
    pub total_quantity: i64,
    pub available_quantity: i64,
    pub reserved_quantity: i64,
    pub sold_quantity: i64,
    pub made_to_order: bool,
}

impl VariantRow {
    pub fn is_implicit(&self) -> bool {
        self.variant_id == IMPLICIT_VARIANT_ID
    }

    pub fn committed(&self) -> i64 {
        self.reserved_quantity + self.sold_quantity
    }

    /// A buyer can order this row now: it is made to order, or has units.
    pub fn orderable(&self) -> bool {
        self.made_to_order || self.available_quantity > 0
    }
}

const ROW_COLUMNS: &str = "variant_id, sku, enabled, total_quantity, available_quantity, \
     reserved_quantity, sold_quantity, made_to_order";

/// The listing state its quantities imply: `available` while anything can
/// be ordered (units on sale, or a made-to-order variant), `reserved` while
/// only held units remain, `sold` otherwise.
pub fn listing_state(available: i64, reserved: i64, made_to_order: bool) -> &'static str {
    if available > 0 || made_to_order {
        "available"
    } else if reserved > 0 {
        "reserved"
    } else {
        "sold"
    }
}

/// The listing state after a new claim of `quantity` units, from the
/// listing's totals before it: a made-to-order listing stays `available`.
pub fn state_after_claim(
    available_before: i64,
    quantity: i64,
    made_to_order: bool,
) -> &'static str {
    if available_before == quantity && !made_to_order {
        "reserved"
    } else {
        "available"
    }
}

/// The listing's rows, locked. The listing row is locked first.
pub async fn lock_rows(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Vec<VariantRow>, sqlx::Error> {
    sqlx::query("SELECT 1 FROM listings WHERE aggregate_id = $1 FOR UPDATE")
        .bind(aggregate_id)
        .fetch_optional(&mut **tx)
        .await?;
    sqlx::query_as(&format!(
        "SELECT {ROW_COLUMNS} FROM listing_variant_inventory \
         WHERE listing_aggregate_id = $1 ORDER BY position, variant_id FOR UPDATE"
    ))
    .bind(aggregate_id)
    .fetch_all(&mut **tx)
    .await
}

/// True when the listing tracks stock per variant (has a keyed row).
pub fn tracks_variants(rows: &[VariantRow]) -> bool {
    rows.iter().any(|row| !row.is_implicit())
}

/// Why a line could not be matched to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantRefusal {
    /// The listing tracks stock per variant and the line names none (and
    /// more than one variant is on offer).
    Required,
    /// The named variant is not a variant of the listing.
    Unknown,
    /// The named variant exists but the seller no longer offers it.
    Unavailable,
    /// The listing has no stock rows at all.
    Missing,
}

impl VariantRefusal {
    pub fn failure(self) -> CommandFailure {
        match self {
            VariantRefusal::Required => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "Choose a variant: this listing tracks its stock per variant.",
                REASON_VARIANT_REQUIRED,
            ),
            VariantRefusal::Unknown => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "The chosen variant is not offered by this listing.",
                REASON_UNKNOWN_VARIANT,
            ),
            VariantRefusal::Unavailable => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The chosen variant is no longer offered.",
                REASON_VARIANT_UNAVAILABLE,
            ),
            VariantRefusal::Missing => CommandFailure::refused(
                RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "The listing's variant stock is missing.",
            ),
        }
    }

    /// The refusal for a line that already holds stock, where a variant that
    /// is not on the listing means the books no longer add up.
    pub fn held_failure(self) -> CommandFailure {
        CommandFailure::refused(
            RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "The inventory held by this order is no longer accounted to its listing.",
        )
    }
}

/// The row a NEW claim on stock (checkout, a hold, a reserve, an accepted
/// offer) takes its units from. A listing that sells one variant answers
/// with its implicit row whatever variant the line names (the name is the
/// buyer's snapshot, as before per-variant stock). A listing that tracks
/// variants needs the named, offered variant; a line that names none takes
/// the sole offered variant when there is exactly one.
pub fn resolve<'a>(
    rows: &'a [VariantRow],
    requested: Option<&str>,
) -> Result<&'a VariantRow, VariantRefusal> {
    if rows.is_empty() {
        return Err(VariantRefusal::Missing);
    }
    if !tracks_variants(rows) {
        return Ok(&rows[0]);
    }
    match requested {
        Some(id) => {
            let row = rows
                .iter()
                .find(|row| row.variant_id == id)
                .ok_or(VariantRefusal::Unknown)?;
            if row.enabled {
                Ok(row)
            } else {
                Err(VariantRefusal::Unavailable)
            }
        }
        None => {
            let mut offered = rows.iter().filter(|row| row.enabled);
            match (offered.next(), offered.next()) {
                (Some(only), None) => Ok(only),
                _ => Err(VariantRefusal::Required),
            }
        }
    }
}

/// The row stock a line ALREADY holds sits on, whether or not the seller
/// still offers the variant.
pub fn resolve_held<'a>(
    rows: &'a [VariantRow],
    requested: Option<&str>,
) -> Result<&'a VariantRow, VariantRefusal> {
    if rows.is_empty() {
        return Err(VariantRefusal::Missing);
    }
    if !tracks_variants(rows) {
        return Ok(&rows[0]);
    }
    let id = requested.ok_or(VariantRefusal::Required)?;
    rows.iter()
        .find(|row| row.variant_id == id)
        .ok_or(VariantRefusal::Unknown)
}

/// How a row falls short of a requested quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shortfall {
    /// Nothing is available and another buyer's payment holds units.
    Held,
    /// Nothing is available and nothing is held.
    SoldOut,
    /// Some, but fewer than asked, are available.
    Insufficient,
}

pub fn shortfall(row: &VariantRow, quantity: i64) -> Option<Shortfall> {
    if row.made_to_order || row.available_quantity >= quantity {
        None
    } else if row.available_quantity > 0 {
        Some(Shortfall::Insufficient)
    } else if row.reserved_quantity > 0 {
        Some(Shortfall::Held)
    } else {
        Some(Shortfall::SoldOut)
    }
}

/// Which row column units leave when they go back on sale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    Reserved,
    Sold,
}

impl Held {
    fn column(self) -> &'static str {
        match self {
            Held::Reserved => "reserved_quantity",
            Held::Sold => "sold_quantity",
        }
    }
}

/// Available -> reserved on one row, guarded by the available count (the
/// compare-and-swap; the CHECK constraints keep stock non-negative even
/// without it). False when the units are not there. A made-to-order row has
/// no count to take from: the units join its total as they are reserved.
pub async fn reserve(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variant_id: &str,
    quantity: i64,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        "UPDATE listing_variant_inventory SET \
         available_quantity = available_quantity - CASE WHEN made_to_order THEN 0 ELSE $3 END, \
         total_quantity = total_quantity + CASE WHEN made_to_order THEN $3 ELSE 0 END, \
         reserved_quantity = reserved_quantity + $3, updated_at = now() \
         WHERE listing_aggregate_id = $1 AND variant_id = $2 \
           AND (made_to_order OR available_quantity >= $3)",
    )
    .bind(aggregate_id)
    .bind(variant_id)
    .bind(quantity)
    .execute(&mut **tx)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// Reserved -> sold on one row (payment confirmation). False when the units
/// are not reserved there.
pub async fn confirm_sale(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variant_id: &str,
    quantity: i64,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        "UPDATE listing_variant_inventory SET \
         reserved_quantity = reserved_quantity - $3, \
         sold_quantity = sold_quantity + $3, updated_at = now() \
         WHERE listing_aggregate_id = $1 AND variant_id = $2 AND reserved_quantity >= $3",
    )
    .bind(aggregate_id)
    .bind(variant_id)
    .bind(quantity)
    .execute(&mut **tx)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// Reserved or sold -> back on sale, on one row. The held-quantity guard is a
/// compare-and-swap against the ledger: nothing else removes a claim's
/// contribution, so a shortfall is an invariant violation. A variant the
/// seller has since withdrawn takes the units out of its total instead of
/// putting them on sale, so a withdrawn size never shows stock; so does a
/// made-to-order variant, which has no count to return them to.
pub async fn release(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variant_id: &str,
    quantity: i64,
    held: Held,
) -> Result<bool, sqlx::Error> {
    let column = held.column();
    let updated = sqlx::query(&format!(
        "UPDATE listing_variant_inventory SET \
         available_quantity = available_quantity + \
             CASE WHEN enabled AND NOT made_to_order THEN $3 ELSE 0 END, \
         total_quantity = total_quantity - \
             CASE WHEN enabled AND NOT made_to_order THEN 0 ELSE $3 END, \
         {column} = {column} - $3, updated_at = now() \
         WHERE listing_aggregate_id = $1 AND variant_id = $2 AND {column} >= $3",
    ))
    .bind(aggregate_id)
    .bind(variant_id)
    .bind(quantity)
    .execute(&mut **tx)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// The listing state its (already rolled-up) quantities imply, as
/// [`listing_state`] does.
pub const STATE_FROM_TOTALS_SQL: &str =
    "CASE WHEN available_quantity > 0 OR made_to_order THEN 'available' \
     WHEN reserved_quantity > 0 THEN 'reserved' ELSE 'sold' END";

/// One variant's stock after a record edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRow {
    pub variant_id: String,
    pub sku: Option<String>,
    pub enabled: bool,
    pub position: i32,
    pub total: i64,
    pub available: i64,
    pub reserved: i64,
    pub sold: i64,
    /// The row stays (or, converting from a made-to-order implicit row,
    /// starts) made to order: its record quantity is not a count, so
    /// `total` is its committed units and `available` 0.
    pub made_to_order: bool,
}

/// What a registration or sync does to the listing's stock rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StockPlan {
    /// One implicit row holding the payload's quantity (the listing sells
    /// one variant, as before per-variant stock).
    Implicit,
    /// The listing already tracks variants and the record lists no enabled
    /// variant with a usable id (or a client that predates variants sent
    /// none): the rows stay as they are, so a payload that says nothing about
    /// variants never withdraws a listing's sizes.
    Keep,
    /// One row per variant. `convert` is true when the listing's implicit row
    /// is replaced by the keyed rows.
    PerVariant {
        rows: Vec<PlannedRow>,
        convert: bool,
    },
}

/// A record edit that would take a variant below the units already reserved
/// or sold of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BelowCommitted {
    pub variant_id: String,
}

impl BelowCommitted {
    pub fn failure(&self, current_revision: i64) -> CommandFailure {
        CommandFailure::refused_with_revision(
            RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "Variant quantity cannot fall below committed inventory.",
            current_revision,
        )
        .with_reason(REASON_VARIANT_BELOW_COMMITTED)
    }
}

/// Why a listing that should track stock per variant still sells from one
/// shared total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferReason {
    /// Units are reserved. A hold made before per-variant stock names no
    /// variant, so it cannot be attributed; the listing converts at a sync
    /// after the holds settle.
    OpenHolds,
    /// Units were sold that the lines of the listing's paid orders do not
    /// attribute exactly to the record's variants.
    UnattributableSales,
    /// The record has not changed since the listing last synced it, but the
    /// listing's total is no longer what the record says: the seller moved it
    /// with `inventory.adjust` while conversion waited. A listing-total
    /// adjustment names no variant, so its units cannot be placed on one;
    /// converting from the record would create or destroy them. The next
    /// record edit (the record is the authority then) converts it.
    AdjustedTotal,
    /// The record gives a variant fewer units than the paid orders attribute
    /// to it. Converting would drop it below its sold units; the seller has to
    /// republish the record.
    BelowCommitted,
}

impl DeferReason {
    pub fn as_str(self) -> &'static str {
        match self {
            DeferReason::OpenHolds => "open_holds",
            DeferReason::UnattributableSales => "unattributable_sales",
            DeferReason::AdjustedTotal => "adjusted_total",
            DeferReason::BelowCommitted => "below_committed",
        }
    }
}

/// Sold units attributable to each variant for a listing whose implicit row
/// is being replaced by keyed rows, or why they cannot be attributed. Units
/// sold are attributed from the lines of the listing's paid orders.
pub async fn attribute_sold(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    implicit: &VariantRow,
    offered: &HashSet<&str>,
) -> Result<Result<HashMap<String, i64>, DeferReason>, sqlx::Error> {
    if implicit.reserved_quantity != 0 {
        return Ok(Err(DeferReason::OpenHolds));
    }
    if implicit.sold_quantity == 0 {
        return Ok(Ok(HashMap::new()));
    }
    let sold: Vec<(Option<String>, i64)> = sqlx::query_as(
        "SELECT line->>'variant_id', SUM((line->>'quantity')::bigint)::bigint \
         FROM orders o, jsonb_array_elements(o.lines) AS line \
         WHERE line->>'listing_aggregate_id' = $1 \
           AND o.auction_aggregate_id IS NULL \
           AND o.state NOT IN ('pending_payment', 'cancelled') \
         GROUP BY 1",
    )
    .bind(aggregate_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut attributed = HashMap::new();
    let mut total = 0i64;
    for (variant_id, quantity) in sold {
        let Some(variant_id) = variant_id.filter(|id| offered.contains(id.as_str())) else {
            return Ok(Err(DeferReason::UnattributableSales));
        };
        total += quantity;
        attributed.insert(variant_id, quantity);
    }
    Ok(if total == implicit.sold_quantity {
        Ok(attributed)
    } else {
        Err(DeferReason::UnattributableSales)
    })
}

/// The state a marker reason reports: `blocked` when the record itself has to
/// change before the listing can convert, `deferred` when time or nothing but
/// the next sync is needed.
pub fn conversion_state(reason: &str) -> &'static str {
    if reason == DeferReason::BelowCommitted.as_str() {
        "blocked"
    } else {
        "deferred"
    }
}

/// Marks a listing the record wants per-variant but that cannot convert yet:
/// one row per listing in `listing_variant_conversion_blocks`, which the
/// operator sweep reads, and a warn log the first time and whenever the reason
/// changes. Every transaction that finds the listing still blocked counts one
/// attempt, however many times it plans.
async fn record_deferral(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    reason: DeferReason,
    implicit: &VariantRow,
    enabled_variants: usize,
) -> Result<(), sqlx::Error> {
    let previous: Option<String> = sqlx::query_scalar(
        "SELECT reason FROM listing_variant_conversion_blocks WHERE listing_aggregate_id = $1",
    )
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await?;
    let attempts: i32 = sqlx::query_scalar(
        "INSERT INTO listing_variant_conversion_blocks \
         (listing_aggregate_id, reason, enabled_variants, reserved_quantity, sold_quantity) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (listing_aggregate_id) DO UPDATE SET reason = EXCLUDED.reason, \
            enabled_variants = EXCLUDED.enabled_variants, \
            reserved_quantity = EXCLUDED.reserved_quantity, \
            sold_quantity = EXCLUDED.sold_quantity, \
            attempts = CASE WHEN listing_variant_conversion_blocks.last_attempt_at = now() \
                THEN listing_variant_conversion_blocks.attempts \
                ELSE listing_variant_conversion_blocks.attempts + 1 END, \
            last_attempt_at = now() \
         RETURNING attempts",
    )
    .bind(aggregate_id)
    .bind(reason.as_str())
    .bind(i32::try_from(enabled_variants).unwrap_or(i32::MAX))
    .bind(implicit.reserved_quantity)
    .bind(implicit.sold_quantity)
    .fetch_one(&mut **tx)
    .await?;
    if previous.as_deref() != Some(reason.as_str()) {
        tracing::warn!(
            target: "variant_conversion",
            aggregate_id,
            reason = reason.as_str(),
            enabled_variants,
            reserved_quantity = implicit.reserved_quantity,
            sold_quantity = implicit.sold_quantity,
            attempts = attempts,
            "listing cannot convert to per-variant stock yet and keeps one shared total"
        );
    }
    Ok(())
}

async fn clear_deferral(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM listing_variant_conversion_blocks WHERE listing_aggregate_id = $1")
        .bind(aggregate_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// What a record of the revision the listing already holds may convert the
/// listing's total to. A sync of an unchanged record is not an edit, so the
/// listing's own total stays the truth: conversion goes ahead only while that
/// total is still what the record says (the sum of its enabled variants, or of
/// all its variants for a listing registered before disabled stock was left
/// out of the total). Anything else is a seller's adjustment and defers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnchangedRecord {
    pub enabled_sum: i64,
    pub all_variants_sum: i64,
}

/// Plans the rows for a record's enabled variants against the rows the
/// listing has now. Pure apart from the sold-unit attribution a conversion
/// needs. `unchanged` is `Some` when the record is at the revision the listing
/// already holds, `None` when the record is a new edit and is the authority.
pub async fn plan(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variants: &[VariantStock],
    rows: &[VariantRow],
    new_listing: bool,
    current_revision: i64,
    unchanged: Option<UnchangedRecord>,
) -> Result<Result<StockPlan, CommandFailure>, sqlx::Error> {
    let keyed = tracks_variants(rows);
    if !keyed && variants.len() < 2 {
        if !new_listing {
            clear_deferral(tx, aggregate_id).await?;
        }
        return Ok(Ok(StockPlan::Implicit));
    }
    if keyed && variants.is_empty() {
        return Ok(Ok(StockPlan::Keep));
    }

    let mut convert = false;
    let mut attribution: HashMap<String, i64> = HashMap::new();
    let mut converting_from: Option<&VariantRow> = None;
    if !keyed && !new_listing {
        let Some(implicit) = rows.iter().find(|row| row.is_implicit()) else {
            return Ok(Ok(StockPlan::Implicit));
        };
        let offered: HashSet<&str> = variants.iter().map(|v| v.id.as_str()).collect();
        match attribute_sold(tx, aggregate_id, implicit, &offered).await? {
            Ok(sold) => attribution = sold,
            Err(reason) => {
                record_deferral(tx, aggregate_id, reason, implicit, variants.len()).await?;
                return Ok(Ok(StockPlan::Implicit));
            }
        }
        // A made-to-order total is its committed units, never a count the
        // seller adjusted, so it does not hold a conversion back.
        if !implicit.made_to_order
            && unchanged.is_some_and(|record| {
                implicit.total_quantity != record.enabled_sum
                    && implicit.total_quantity != record.all_variants_sum
            })
        {
            record_deferral(
                tx,
                aggregate_id,
                DeferReason::AdjustedTotal,
                implicit,
                variants.len(),
            )
            .await?;
            return Ok(Ok(StockPlan::Implicit));
        }
        convert = true;
        converting_from = Some(implicit);
    }

    let mut planned = Vec::with_capacity(variants.len() + rows.len());
    let offered: HashSet<&str> = variants.iter().map(|v| v.id.as_str()).collect();
    // Converting a made-to-order implicit row keeps every new size made to
    // order: which sizes the store tracks is the connector's to say, on the
    // next stock change or poll, and until then none of them is sold out.
    let inherit_made_to_order = converting_from.is_some_and(|implicit| implicit.made_to_order);
    for (position, variant) in variants.iter().enumerate() {
        let (reserved, sold, made_to_order) =
            match rows.iter().find(|row| row.variant_id == variant.id) {
                Some(row) => (row.reserved_quantity, row.sold_quantity, row.made_to_order),
                None => (
                    0,
                    attribution.get(&variant.id).copied().unwrap_or(0),
                    inherit_made_to_order,
                ),
            };
        if made_to_order {
            planned.push(PlannedRow {
                variant_id: variant.id.clone(),
                sku: variant.sku.clone(),
                enabled: true,
                position: i32::try_from(position).unwrap_or(i32::MAX),
                total: reserved + sold,
                available: 0,
                reserved,
                sold,
                made_to_order: true,
            });
            continue;
        }
        if variant.quantity < reserved + sold {
            // A conversion that cannot be written stays marked: the marker is
            // never cleared while the listing is unconvertible.
            if let Some(implicit) = converting_from {
                record_deferral(
                    tx,
                    aggregate_id,
                    DeferReason::BelowCommitted,
                    implicit,
                    variants.len(),
                )
                .await?;
            }
            return Ok(Err(BelowCommitted {
                variant_id: variant.id.clone(),
            }
            .failure(current_revision)));
        }
        planned.push(PlannedRow {
            variant_id: variant.id.clone(),
            sku: variant.sku.clone(),
            enabled: true,
            position: i32::try_from(position).unwrap_or(i32::MAX),
            total: variant.quantity,
            available: variant.quantity - reserved - sold,
            reserved,
            sold,
            made_to_order: false,
        });
    }
    let offered_count = variants.len();
    if !convert {
        for (offset, row) in rows
            .iter()
            .filter(|row| !row.is_implicit() && !offered.contains(row.variant_id.as_str()))
            .enumerate()
        {
            planned.push(PlannedRow {
                variant_id: row.variant_id.clone(),
                sku: row.sku.clone(),
                enabled: false,
                position: i32::try_from(offered_count + offset).unwrap_or(i32::MAX),
                total: row.committed(),
                available: 0,
                reserved: row.reserved_quantity,
                sold: row.sold_quantity,
                made_to_order: row.made_to_order,
            });
        }
    }
    if converting_from.is_some() {
        clear_deferral(tx, aggregate_id).await?;
    }
    Ok(Ok(StockPlan::PerVariant {
        rows: planned,
        convert,
    }))
}

/// Writes a per-variant plan: keyed rows are inserted or updated, then an
/// implicit row being replaced is deleted, so the listing's rolled-up totals
/// never dip below its committed units in between.
pub async fn write_plan(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    rows: &[PlannedRow],
    delete_implicit: bool,
) -> Result<(), sqlx::Error> {
    for row in rows {
        sqlx::query(
            "INSERT INTO listing_variant_inventory \
             (listing_aggregate_id, variant_id, position, sku, enabled, total_quantity, \
              available_quantity, reserved_quantity, sold_quantity, made_to_order, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now()) \
             ON CONFLICT (listing_aggregate_id, variant_id) DO UPDATE SET \
                position = EXCLUDED.position, sku = EXCLUDED.sku, enabled = EXCLUDED.enabled, \
                total_quantity = EXCLUDED.total_quantity, \
                available_quantity = EXCLUDED.available_quantity, \
                reserved_quantity = EXCLUDED.reserved_quantity, \
                sold_quantity = EXCLUDED.sold_quantity, \
                made_to_order = EXCLUDED.made_to_order, updated_at = now()",
        )
        .bind(aggregate_id)
        .bind(&row.variant_id)
        .bind(row.position)
        .bind(&row.sku)
        .bind(row.enabled)
        .bind(row.total)
        .bind(row.available)
        .bind(row.reserved)
        .bind(row.sold)
        .bind(row.made_to_order)
        .execute(&mut **tx)
        .await?;
    }
    if delete_implicit {
        sqlx::query(
            "DELETE FROM listing_variant_inventory \
             WHERE listing_aggregate_id = $1 AND variant_id = $2",
        )
        .bind(aggregate_id)
        .bind(IMPLICIT_VARIANT_ID)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, enabled: bool, available: i64, reserved: i64, sold: i64) -> VariantRow {
        VariantRow {
            variant_id: id.to_string(),
            sku: None,
            enabled,
            total_quantity: available + reserved + sold,
            available_quantity: available,
            reserved_quantity: reserved,
            sold_quantity: sold,
            made_to_order: false,
        }
    }

    #[test]
    fn a_listing_that_sells_one_variant_answers_with_its_implicit_row_for_any_name() {
        let rows = [row(IMPLICIT_VARIANT_ID, true, 3, 0, 0)];
        for requested in [None, Some("anything"), Some("size-m")] {
            let picked = resolve(&rows, requested).expect("implicit row");
            assert!(picked.is_implicit());
            let held = resolve_held(&rows, requested).expect("implicit row");
            assert!(held.is_implicit());
        }
    }

    #[test]
    fn a_keyed_listing_needs_the_named_offered_variant() {
        let rows = [row("s", true, 0, 0, 2), row("m", true, 4, 0, 0)];
        assert_eq!(resolve(&rows, Some("m")).unwrap().variant_id, "m");
        assert_eq!(resolve(&rows, Some("s")).unwrap().variant_id, "s");
        assert_eq!(resolve(&rows, None).unwrap_err(), VariantRefusal::Required);
        assert_eq!(
            resolve(&rows, Some("xl")).unwrap_err(),
            VariantRefusal::Unknown
        );
    }

    #[test]
    fn a_withdrawn_variant_is_not_offered_but_its_holds_still_resolve() {
        let rows = [row("s", false, 0, 1, 1), row("m", true, 4, 0, 0)];
        assert_eq!(
            resolve(&rows, Some("s")).unwrap_err(),
            VariantRefusal::Unavailable
        );
        assert_eq!(resolve_held(&rows, Some("s")).unwrap().variant_id, "s");
        assert_eq!(
            resolve_held(&rows, None).unwrap_err(),
            VariantRefusal::Required
        );
    }

    #[test]
    fn a_line_without_a_variant_takes_the_sole_offered_variant() {
        let rows = [row("s", false, 0, 0, 1), row("m", true, 4, 0, 0)];
        assert_eq!(resolve(&rows, None).unwrap().variant_id, "m");
    }

    #[test]
    fn shortfall_tells_held_from_sold_out_from_partial() {
        assert_eq!(shortfall(&row("m", true, 2, 0, 0), 2), None);
        assert_eq!(
            shortfall(&row("m", true, 1, 0, 0), 2),
            Some(Shortfall::Insufficient)
        );
        assert_eq!(
            shortfall(&row("m", true, 0, 1, 0), 1),
            Some(Shortfall::Held)
        );
        assert_eq!(
            shortfall(&row("m", true, 0, 0, 3), 1),
            Some(Shortfall::SoldOut)
        );
    }
}
