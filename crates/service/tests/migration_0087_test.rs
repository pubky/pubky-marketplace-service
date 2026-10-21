//! Migrations 0087 and 0088: the double-blind review window over a database
//! that already holds reviewed orders. Reviews written before the window
//! existed were public from the first, so their orders are released as of
//! their first review; an order nobody reviewed is untouched, no order gets
//! a window, the file reruns as a no-op, and a binary from before 0087 still
//! inserts reviews during a rolling deploy.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0087: &str = include_str!("../migrations/0087_review_blind_window.sql");

const BUYER: &str = "buyer";
const SELLER: &str = "seller";

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

async fn seed_order(pool: &PgPool) -> Uuid {
    let order_id = Uuid::new_v4();
    let lines = json!([
        { "listing_aggregate_id": format!("listing:{SELLER}_boots_01"), "quantity": 1 },
    ]);
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, $2, $3, 6, 'completed', $4, NULL, 12500, 1200, 13700, 'USD', 2, 1, $5, \
         now(), now())",
    )
    .bind(order_id)
    .bind(BUYER)
    .bind(SELLER)
    .bind(lines)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .expect("seed order");
    order_id
}

async fn insert_review(
    pool: &PgPool,
    order_id: Uuid,
    role: &str,
    created_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let (reviewer, subject) = if role == "buyer" {
        (BUYER, SELLER)
    } else {
        (SELLER, BUYER)
    };
    sqlx::query(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         listing_aggregate_id, rating, text, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 5, 'Fine.', $7, $7)",
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(reviewer)
    .bind(role)
    .bind(subject)
    .bind(format!("listing:{SELLER}_boots_01"))
    .bind(created_at)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn window_columns(
    pool: &PgPool,
    order_id: Uuid,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    sqlx::query_as("SELECT review_window_ends_at, reviews_released_at FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("window columns")
}

#[sqlx::test(migrations = false)]
async fn reviewed_orders_are_released_as_of_their_first_review(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(86).run(&pool).await.expect("0001..0086 apply");

    let first: DateTime<Utc> = "2026-06-01T10:00:00Z".parse().expect("timestamp");
    let second: DateTime<Utc> = "2026-06-03T10:00:00Z".parse().expect("timestamp");
    let buyer_only = seed_order(&pool).await;
    insert_review(&pool, buyer_only, "buyer", first)
        .await
        .expect("buyer review");
    let both = seed_order(&pool).await;
    insert_review(&pool, both, "seller", second)
        .await
        .expect("seller review");
    insert_review(&pool, both, "buyer", first)
        .await
        .expect("buyer review");
    let unreviewed = seed_order(&pool).await;

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0087 and 0088 apply over reviewed orders");

    assert_eq!(window_columns(&pool, buyer_only).await, (None, Some(first)));
    assert_eq!(window_columns(&pool, both).await, (None, Some(first)));
    assert_eq!(window_columns(&pool, unreviewed).await, (None, None));
    let withheld: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM orders WHERE review_window_ends_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("windows");
    assert_eq!(withheld, 0, "no existing order gets a window");

    // The scan index exists, is valid and holds only withheld orders.
    let index: (bool, Option<String>) = sqlx::query_as(
        "SELECT i.indisvalid, pg_get_expr(i.indpred, i.indrelid) \
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE c.relname = 'orders_review_release_due_idx'",
    )
    .fetch_one(&pool)
    .await
    .expect("index");
    assert!(index.0, "the concurrent build left a valid index");
    let predicate = index.1.expect("partial index");
    assert!(predicate.contains("review_window_ends_at IS NOT NULL"));
    assert!(predicate.contains("reviews_released_at IS NULL"));

    // A binary from before 0087 reviews without touching the new columns;
    // such an order is never withheld.
    let rolling = seed_order(&pool).await;
    insert_review(&pool, rolling, "buyer", first)
        .await
        .expect("an old binary's review");
    assert_eq!(window_columns(&pool, rolling).await, (None, None));
    let withheld: bool = sqlx::query_scalar(&format!(
        "SELECT withheld FROM (SELECT {}, 1 AS one FROM reviews WHERE order_id = $1) AS r",
        marketplace_service::handlers::REVIEW_COLUMNS
    ))
    .bind(rolling)
    .fetch_one(&pool)
    .await
    .expect("withheld flag");
    assert!(!withheld);

    // The file reruns as a no-op: nothing is released twice or moved.
    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0087)
        .execute(&mut *tx)
        .await
        .expect("0087 must be directly rerunnable");
    tx.commit().await.expect("commit");
    assert_eq!(window_columns(&pool, buyer_only).await, (None, Some(first)));
    assert_eq!(window_columns(&pool, both).await, (None, Some(first)));
    assert_eq!(window_columns(&pool, unreviewed).await, (None, None));
}
