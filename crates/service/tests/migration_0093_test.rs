//! Migration 0093: `orders_external_refund_within_total` admits the sats an
//! overpaid Bitcoin order received (`paykit_observed_sats`), over a database
//! that already holds refunds. A refund of the observed amount above the
//! payable is refused before the migration and recorded after it, anything
//! above every bound is still refused, every existing refund still satisfies
//! the constraint, and the file reruns as a no-op.

use std::borrow::Cow;

use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0093: &str = include_str!("../migrations/0093_bitcoin_refund_received_sats.sql");

const CONSTRAINT: &str = "orders_external_refund_within_total";
const PAYABLE_SATS: i64 = 51_637;
const OBSERVED_SATS: i64 = 53_000;

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

/// A cancelled 137.00 USD order paid in Bitcoin, overpaid by 1,363 sats.
async fn seed_overpaid_bitcoin_order(pool: &PgPool) -> Uuid {
    let order_id = Uuid::new_v4();
    let lines = json!([{ "listing_aggregate_id": "listing:seller_boots_01", "quantity": 1 }]);
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, payment_method, paykit_total_sats, \
         paykit_observed_sats, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 4, 'cancelled', $2, NULL, 12500, 1200, 13700, 'USD', 2, \
         1, $3, 'paykit', $4, $5, now(), now())",
    )
    .bind(order_id)
    .bind(lines)
    .bind(Uuid::new_v4())
    .bind(PAYABLE_SATS)
    .bind(OBSERVED_SATS)
    .execute(pool)
    .await
    .expect("seed order");
    order_id
}

async fn record_refund(
    pool: &PgPool,
    order_id: Uuid,
    amount_minor: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE orders SET state = 'refunded_external', external_refund = jsonb_build_object(\
         'amount_minor', $2::bigint, 'transaction_id', 'bitcoin-refund-1', \
         'recorded_at', '2026-10-29T12:00:00.000Z') WHERE id = $1",
    )
    .bind(order_id)
    .bind(amount_minor)
    .execute(pool)
    .await
    .map(|_| ())
}

fn constraint_violation(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|error| error.constraint().map(str::to_string))
}

async fn refunded(pool: &PgPool, order_id: Uuid) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT (external_refund->>'amount_minor')::bigint FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order")
}

#[sqlx::test(migrations = false)]
async fn the_sats_an_overpaid_order_received_are_refundable_after_the_migration(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(90).run(&pool).await.expect("0001..0090 apply");

    let existing = seed_overpaid_bitcoin_order(&pool).await;
    record_refund(&pool, existing, PAYABLE_SATS)
        .await
        .expect("the payable is refundable before 0093");
    let overpaid = seed_overpaid_bitcoin_order(&pool).await;
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect_err("the observed amount above the payable is refused before 0093");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));

    ALL_MIGRATIONS.run(&pool).await.expect("0093 applies");

    assert_eq!(
        refunded(&pool, existing).await,
        Some(PAYABLE_SATS),
        "an existing refund survives"
    );
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS + 1)
        .await
        .expect_err("above everything the order received is still refused");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));
    record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect("the observed amount is refundable after 0093");
    assert_eq!(refunded(&pool, overpaid).await, Some(OBSERVED_SATS));
    let refused = record_refund(&pool, existing, 0)
        .await
        .expect_err("an empty refund is still refused");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));

    sqlx::query(&format!(
        "ALTER TABLE orders VALIDATE CONSTRAINT {CONSTRAINT}"
    ))
    .execute(&pool)
    .await
    .expect("every existing refund satisfies the widened bound");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_the_widened_bound(pool: PgPool) {
    let overpaid = seed_overpaid_bitcoin_order(&pool).await;
    record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect("the observed amount is refundable");

    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0093)
        .execute(&mut *tx)
        .await
        .expect("0093 must be directly rerunnable");
    tx.commit().await.expect("commit");

    assert_eq!(refunded(&pool, overpaid).await, Some(OBSERVED_SATS));
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS + 1)
        .await
        .expect_err("still bounded");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));
}
