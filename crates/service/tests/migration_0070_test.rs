//! Migration 0070: per-variant listing inventory. The backfill gives every
//! existing listing one implicit row equal to its totals, the file is
//! directly rerunnable, and the triggers keep a listing's quantity columns
//! equal to the sum of its rows whichever side a writer changes, including a
//! binary from before the migration that writes the listing totals.

use std::borrow::Cow;

use sqlx::migrate::Migrator;
use sqlx::PgPool;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0070: &str = include_str!("../migrations/0070_variant_inventory.sql");

fn through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}

async fn seed_listing(pool: &PgPool, aggregate: &str, sale_format: &str, quantities: Quantities) {
    let (total, available, reserved, sold) = quantities;
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, shipping_minor, sale_format, fulfillment_methods, updated_at) \
         VALUES ($1, 'seller', $1, 'Boots', 1, repeat('a', 64), 1, 'available', $2, $3, $4, $5, \
         12500, 'USD', 2, 0, $6, ARRAY['shipping'], now())",
    )
    .bind(aggregate)
    .bind(total)
    .bind(available)
    .bind(reserved)
    .bind(sold)
    .bind(sale_format)
    .execute(pool)
    .await
    .expect("seed listing");
}

type RowFacts = (String, bool, i64, i64, i64, i64);
/// `(total, available, reserved, sold)`.
type Quantities = (i64, i64, i64, i64);

async fn rows(pool: &PgPool, aggregate: &str) -> Vec<RowFacts> {
    sqlx::query_as(
        "SELECT variant_id, enabled, total_quantity, available_quantity, reserved_quantity, \
         sold_quantity FROM listing_variant_inventory WHERE listing_aggregate_id = $1 \
         ORDER BY position, variant_id",
    )
    .bind(aggregate)
    .fetch_all(pool)
    .await
    .expect("rows")
}

async fn listing_totals(pool: &PgPool, aggregate: &str) -> (i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT total_quantity, available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(aggregate)
    .fetch_one(pool)
    .await
    .expect("listing totals")
}

async fn insert_row(
    pool: &PgPool,
    aggregate: &str,
    variant: &str,
    position: i32,
    quantities: (i64, i64, i64, i64),
) -> Result<(), sqlx::Error> {
    let (total, available, reserved, sold) = quantities;
    sqlx::query(
        "INSERT INTO listing_variant_inventory (listing_aggregate_id, variant_id, position, \
         total_quantity, available_quantity, reserved_quantity, sold_quantity) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(aggregate)
    .bind(variant)
    .bind(position)
    .bind(total)
    .bind(available)
    .bind(reserved)
    .bind(sold)
    .execute(pool)
    .await
    .map(|_| ())
}

/// Replaces a listing's implicit row with keyed rows the way a registration
/// does: keyed rows first, then the implicit row.
async fn make_keyed(pool: &PgPool, aggregate: &str, keyed: &[(&str, Quantities)]) {
    for (position, (variant, quantities)) in keyed.iter().enumerate() {
        insert_row(pool, aggregate, variant, position as i32, *quantities)
            .await
            .expect("keyed row");
    }
    sqlx::query(
        "DELETE FROM listing_variant_inventory WHERE listing_aggregate_id = $1 AND variant_id = ''",
    )
    .bind(aggregate)
    .execute(pool)
    .await
    .expect("drop implicit row");
}

#[sqlx::test(migrations = false)]
async fn the_backfill_gives_each_existing_listing_one_implicit_row_equal_to_its_totals(
    pool: PgPool,
) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(69)
        .run(&pool)
        .await
        .expect("production 0001..0069 catalog applies");
    seed_listing(&pool, "listing:fixed", "fixed_price", (6, 2, 1, 3)).await;
    seed_listing(&pool, "listing:sold_out", "fixed_price", (2, 0, 0, 2)).await;
    seed_listing(&pool, "listing:auction", "auction", (1, 1, 0, 0)).await;

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0070 applies to the production catalog");

    assert_eq!(
        rows(&pool, "listing:fixed").await,
        [("".to_string(), true, 6, 2, 1, 3)]
    );
    assert_eq!(
        rows(&pool, "listing:sold_out").await,
        [("".to_string(), true, 2, 0, 0, 2)]
    );
    assert_eq!(
        rows(&pool, "listing:auction").await,
        [("".to_string(), true, 1, 1, 0, 0)]
    );
    let availability: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM listings WHERE variant_availability IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        availability, 0,
        "single-variant listings project no variants"
    );
    assert_eq!(
        listing_totals(&pool, "listing:fixed").await,
        (6, 2, 1, 3),
        "the backfill leaves the listing untouched"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_is_directly_rerunnable_and_never_re_adds_an_implicit_row(pool: PgPool) {
    seed_listing(&pool, "listing:keyed", "fixed_price", (5, 5, 0, 0)).await;
    seed_listing(&pool, "listing:plain", "fixed_price", (3, 3, 0, 0)).await;
    make_keyed(
        &pool,
        "listing:keyed",
        &[("size_s", (2, 2, 0, 0)), ("size_m", (3, 3, 0, 0))],
    )
    .await;
    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION_0070)
            .execute(&pool)
            .await
            .expect("0070 must be directly rerunnable");
    }
    assert_eq!(
        rows(&pool, "listing:keyed").await,
        [
            ("size_s".to_string(), true, 2, 2, 0, 0),
            ("size_m".to_string(), true, 3, 3, 0, 0),
        ]
    );
    assert_eq!(rows(&pool, "listing:plain").await.len(), 1);
    assert_eq!(listing_totals(&pool, "listing:keyed").await, (5, 5, 0, 0));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_new_listing_has_an_implicit_row_from_the_moment_it_exists(pool: PgPool) {
    seed_listing(&pool, "listing:new", "fixed_price", (4, 3, 1, 0)).await;
    assert_eq!(
        rows(&pool, "listing:new").await,
        [("".to_string(), true, 4, 3, 1, 0)]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_row_change_rolls_the_listing_totals_and_availability_up(pool: PgPool) {
    seed_listing(&pool, "listing:rollup", "fixed_price", (5, 5, 0, 0)).await;
    make_keyed(
        &pool,
        "listing:rollup",
        &[("size_s", (2, 2, 0, 0)), ("size_m", (3, 3, 0, 0))],
    )
    .await;
    assert_eq!(listing_totals(&pool, "listing:rollup").await, (5, 5, 0, 0));
    let availability: serde_json::Value = sqlx::query_scalar(
        "SELECT variant_availability FROM listings WHERE aggregate_id = 'listing:rollup'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        availability,
        serde_json::json!([
            { "variant_id": "size_s", "available_quantity": 2, "sold_out": false },
            { "variant_id": "size_m", "available_quantity": 3, "sold_out": false },
        ])
    );

    sqlx::query(
        "UPDATE listing_variant_inventory SET available_quantity = 0, reserved_quantity = 1, \
         sold_quantity = 1 WHERE listing_aggregate_id = 'listing:rollup' AND variant_id = 'size_s'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(listing_totals(&pool, "listing:rollup").await, (5, 3, 1, 1));
    let availability: serde_json::Value = sqlx::query_scalar(
        "SELECT variant_availability FROM listings WHERE aggregate_id = 'listing:rollup'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        availability[0],
        serde_json::json!({ "variant_id": "size_s", "available_quantity": 0, "sold_out": true })
    );

    sqlx::query(
        "UPDATE listing_variant_inventory SET enabled = false, total_quantity = 2, \
         available_quantity = 0 WHERE listing_aggregate_id = 'listing:rollup' \
         AND variant_id = 'size_s'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let availability: serde_json::Value = sqlx::query_scalar(
        "SELECT variant_availability FROM listings WHERE aggregate_id = 'listing:rollup'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        availability.as_array().map(Vec::len),
        Some(1),
        "a withdrawn variant is not projected"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_binary_from_before_the_migration_can_still_write_a_single_variant_listing(pool: PgPool) {
    seed_listing(&pool, "listing:old", "fixed_price", (5, 5, 0, 0)).await;
    // What the previous binary issues to hold one unit: totals only.
    sqlx::query(
        "UPDATE listings SET available_quantity = available_quantity - 1, \
         reserved_quantity = reserved_quantity + 1 WHERE aggregate_id = 'listing:old'",
    )
    .execute(&pool)
    .await
    .expect("an old-binary hold");
    assert_eq!(
        rows(&pool, "listing:old").await,
        [("".to_string(), true, 5, 4, 1, 0)],
        "the implicit row follows the totals"
    );
    sqlx::query(
        "UPDATE listings SET total_quantity = 8, available_quantity = 7 \
         WHERE aggregate_id = 'listing:old'",
    )
    .execute(&pool)
    .await
    .expect("an old-binary edit");
    assert_eq!(
        rows(&pool, "listing:old").await,
        [("".to_string(), true, 8, 7, 1, 0)]
    );
    assert_eq!(listing_totals(&pool, "listing:old").await, (8, 7, 1, 0));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_binary_from_before_the_migration_cannot_sell_a_variant_listing(pool: PgPool) {
    seed_listing(&pool, "listing:keyed", "fixed_price", (5, 5, 0, 0)).await;
    make_keyed(
        &pool,
        "listing:keyed",
        &[("size_s", (2, 2, 0, 0)), ("size_m", (3, 3, 0, 0))],
    )
    .await;
    let refused = sqlx::query(
        "UPDATE listings SET available_quantity = available_quantity - 1, \
         reserved_quantity = reserved_quantity + 1 WHERE aggregate_id = 'listing:keyed'",
    )
    .execute(&pool)
    .await
    .expect_err("a totals write on a variant listing is refused");
    assert!(
        refused.to_string().contains("tracks stock per variant"),
        "{refused}"
    );
    assert_eq!(listing_totals(&pool, "listing:keyed").await, (5, 5, 0, 0));

    // Writes that leave the quantities alone are not affected.
    sqlx::query("UPDATE listings SET state = 'available' WHERE aggregate_id = 'listing:keyed'")
        .execute(&pool)
        .await
        .expect("a state write");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_row_keeps_the_balance_rule_and_the_implicit_shape(pool: PgPool) {
    seed_listing(&pool, "listing:rules", "fixed_price", (2, 2, 0, 0)).await;
    assert!(
        insert_row(&pool, "listing:rules", "size_s", 0, (3, 1, 1, 0))
            .await
            .is_err(),
        "available + reserved + sold must equal total"
    );
    assert!(
        insert_row(&pool, "listing:rules", "size_s", 0, (1, -1, 1, 1))
            .await
            .is_err(),
        "no negative quantity"
    );
    assert!(
        sqlx::query(
            "UPDATE listing_variant_inventory SET sku = 'X' \
             WHERE listing_aggregate_id = 'listing:rules' AND variant_id = ''",
        )
        .execute(&pool)
        .await
        .is_err(),
        "the implicit row carries no sku"
    );
    sqlx::query("DELETE FROM listings WHERE aggregate_id = 'listing:rules'")
        .execute(&pool)
        .await
        .expect("a listing deletes with its rows");
    assert!(rows(&pool, "listing:rules").await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_conversion_marker_is_constrained_rerunnable_and_leaves_with_its_listing(pool: PgPool) {
    seed_listing(&pool, "listing:blocked", "fixed_price", (5, 4, 1, 0)).await;
    let mark = |reason: &'static str, variants: i32, reserved: i64, sold: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO listing_variant_conversion_blocks \
                 (listing_aggregate_id, reason, enabled_variants, reserved_quantity, sold_quantity) \
                 VALUES ('listing:blocked', $1, $2, $3, $4) \
                 ON CONFLICT (listing_aggregate_id) DO UPDATE SET reason = EXCLUDED.reason",
            )
            .bind(reason)
            .bind(variants)
            .bind(reserved)
            .bind(sold)
            .execute(&pool)
            .await
        }
    };
    assert!(
        mark("because", 2, 1, 0).await.is_err(),
        "only the two named reasons"
    );
    assert!(
        mark("open_holds", 1, 1, 0).await.is_err(),
        "a marker needs two variants"
    );
    assert!(mark("open_holds", 2, -1, 0).await.is_err());
    mark("open_holds", 2, 1, 0).await.expect("a valid marker");
    sqlx::raw_sql(MIGRATION_0070)
        .execute(&pool)
        .await
        .expect("a rerun keeps the marker");
    let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listing_variant_conversion_blocks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(kept, 1);
    sqlx::query("DELETE FROM listings WHERE aggregate_id = 'listing:blocked'")
        .execute(&pool)
        .await
        .expect("the listing deletes with its marker");
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listing_variant_conversion_blocks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
}
