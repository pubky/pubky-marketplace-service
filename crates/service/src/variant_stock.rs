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
}

impl VariantRow {
    pub fn is_implicit(&self) -> bool {
        self.variant_id == IMPLICIT_VARIANT_ID
    }

    pub fn committed(&self) -> i64 {
        self.reserved_quantity + self.sold_quantity
    }
}

const ROW_COLUMNS: &str = "variant_id, sku, enabled, total_quantity, available_quantity, \
     reserved_quantity, sold_quantity";

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
    if row.available_quantity >= quantity {
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
/// without it). False when the units are not there.
pub async fn reserve(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variant_id: &str,
    quantity: i64,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        "UPDATE listing_variant_inventory SET \
         available_quantity = available_quantity - $3, \
         reserved_quantity = reserved_quantity + $3, updated_at = now() \
         WHERE listing_aggregate_id = $1 AND variant_id = $2 AND available_quantity >= $3",
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
/// putting them on sale, so a withdrawn size never shows stock.
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
         available_quantity = available_quantity + CASE WHEN enabled THEN $3 ELSE 0 END, \
         total_quantity = total_quantity - CASE WHEN enabled THEN 0 ELSE $3 END, \
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

/// The listing state its (already rolled-up) quantities imply.
pub const STATE_FROM_TOTALS_SQL: &str = "CASE WHEN available_quantity > 0 THEN 'available' \
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
}

/// What a registration or sync does to the listing's stock rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StockPlan {
    /// One implicit row holding the payload's quantity (the listing sells
    /// one variant, as before per-variant stock).
    Implicit,
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

/// Committed units attributable to each variant for a listing whose implicit
/// row is being replaced by keyed rows, or `None` when they cannot be
/// attributed exactly. Units sold are attributed from the lines of the
/// listing's paid orders; units reserved cannot be (a hold made before
/// per-variant stock names no variant), so a listing with open holds is
/// converted at a later edit, once they have settled.
pub async fn attribute_sold(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    implicit: &VariantRow,
    offered: &HashSet<&str>,
) -> Result<Option<HashMap<String, i64>>, sqlx::Error> {
    if implicit.reserved_quantity != 0 {
        return Ok(None);
    }
    if implicit.sold_quantity == 0 {
        return Ok(Some(HashMap::new()));
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
            return Ok(None);
        };
        total += quantity;
        attributed.insert(variant_id, quantity);
    }
    Ok((total == implicit.sold_quantity).then_some(attributed))
}

/// Plans the rows for a record's enabled variants against the rows the
/// listing has now. Pure apart from the sold-unit attribution a conversion
/// needs.
pub async fn plan(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    variants: &[VariantStock],
    rows: &[VariantRow],
    new_listing: bool,
    current_revision: i64,
) -> Result<Result<StockPlan, CommandFailure>, sqlx::Error> {
    let keyed = tracks_variants(rows);
    if !keyed && variants.len() < 2 {
        return Ok(Ok(StockPlan::Implicit));
    }

    let mut convert = false;
    let mut attribution: HashMap<String, i64> = HashMap::new();
    if !keyed && !new_listing {
        let Some(implicit) = rows.iter().find(|row| row.is_implicit()) else {
            return Ok(Ok(StockPlan::Implicit));
        };
        let offered: HashSet<&str> = variants.iter().map(|v| v.id.as_str()).collect();
        match attribute_sold(tx, aggregate_id, implicit, &offered).await? {
            Some(sold) => {
                attribution = sold;
                convert = true;
            }
            None => return Ok(Ok(StockPlan::Implicit)),
        }
    }

    let mut planned = Vec::with_capacity(variants.len() + rows.len());
    let offered: HashSet<&str> = variants.iter().map(|v| v.id.as_str()).collect();
    for (position, variant) in variants.iter().enumerate() {
        let (reserved, sold) = match rows.iter().find(|row| row.variant_id == variant.id) {
            Some(row) => (row.reserved_quantity, row.sold_quantity),
            None => (0, attribution.get(&variant.id).copied().unwrap_or(0)),
        };
        if variant.quantity < reserved + sold {
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
            });
        }
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
              available_quantity, reserved_quantity, sold_quantity, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now()) \
             ON CONFLICT (listing_aggregate_id, variant_id) DO UPDATE SET \
                position = EXCLUDED.position, sku = EXCLUDED.sku, enabled = EXCLUDED.enabled, \
                total_quantity = EXCLUDED.total_quantity, \
                available_quantity = EXCLUDED.available_quantity, \
                reserved_quantity = EXCLUDED.reserved_quantity, \
                sold_quantity = EXCLUDED.sold_quantity, updated_at = now()",
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
