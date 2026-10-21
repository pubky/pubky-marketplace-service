//! Migration 0089: `reviews.public_removed_at` over a database that already
//! holds reviews, and the `withdraw_review` refusal-audit command kind. Every
//! existing review reads as published (the column is NULL), the file reruns
//! as a no-op that keeps a recorded withdrawal, a binary from before 0089
//! still inserts reviews during a rolling deploy, and a catalog whose id 42
//! names another kind stops the migration.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0089: &str = include_str!("../migrations/0089_review_withdrawal.sql");

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

async fn insert_review(pool: &PgPool, order_id: Uuid, role: &str) -> Uuid {
    let (reviewer, subject) = if role == "buyer" {
        (BUYER, SELLER)
    } else {
        (SELLER, BUYER)
    };
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         listing_aggregate_id, rating, text, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 5, 'Fine.', now(), now())",
    )
    .bind(id)
    .bind(order_id)
    .bind(reviewer)
    .bind(role)
    .bind(subject)
    .bind(format!("listing:{SELLER}_boots_01"))
    .execute(pool)
    .await
    .expect("insert review");
    id
}

async fn removed_at(pool: &PgPool, review_id: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT public_removed_at FROM reviews WHERE id = $1")
        .bind(review_id)
        .fetch_one(pool)
        .await
        .expect("public_removed_at")
}

#[sqlx::test(migrations = false)]
async fn existing_reviews_stay_published_and_the_file_reruns(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(88).run(&pool).await.expect("0001..0088 apply");

    let reviewed = seed_order(&pool).await;
    let buyer_review = insert_review(&pool, reviewed, "buyer").await;
    let seller_review = insert_review(&pool, reviewed, "seller").await;
    let unreviewed = seed_order(&pool).await;

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0089 applies over reviewed orders");

    assert_eq!(removed_at(&pool, buyer_review).await, None);
    assert_eq!(removed_at(&pool, seller_review).await, None);
    let column: (String, String, Option<String>) = sqlx::query_as(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'reviews' AND column_name = 'public_removed_at'",
    )
    .fetch_one(&pool)
    .await
    .expect("column");
    assert_eq!(
        column,
        (
            "timestamp with time zone".to_string(),
            "YES".to_string(),
            None
        ),
        "nullable with no default, so the ALTER rewrites nothing"
    );
    let untouched: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reviews WHERE order_id = $1")
        .bind(unreviewed)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(untouched, 0);

    // A binary from before 0089 inserts reviews without naming the column.
    let rolling = seed_order(&pool).await;
    let old_binary_review = insert_review(&pool, rolling, "buyer").await;
    assert_eq!(removed_at(&pool, old_binary_review).await, None);

    // A recorded withdrawal survives a rerun of the file.
    let withdrawn_at: DateTime<Utc> = "2026-10-21T10:00:00Z".parse().expect("timestamp");
    sqlx::query("UPDATE reviews SET public_removed_at = $2 WHERE id = $1")
        .bind(buyer_review)
        .bind(withdrawn_at)
        .execute(&pool)
        .await
        .expect("withdraw");
    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0089)
        .execute(&mut *tx)
        .await
        .expect("0089 must be directly rerunnable");
    tx.commit().await.expect("commit");
    assert_eq!(removed_at(&pool, buyer_review).await, Some(withdrawn_at));
    assert_eq!(removed_at(&pool, seller_review).await, None);

    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 42 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("command kinds");
    assert_eq!(kinds, vec![(42, "withdraw_review".to_string())]);
}

/// A catalog whose id 42 already names another kind must stop the migration.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0089_refuses_a_catalog_where_id_42_names_another_kind(pool: PgPool) {
    let mut conn = pool.acquire().await.expect("connection");
    sqlx::raw_sql(
        "CREATE SCHEMA scratch; \
         CREATE TABLE scratch.command_refusal_command_kinds \
           (id SMALLINT PRIMARY KEY, name TEXT NOT NULL); \
         CREATE TABLE scratch.reviews (public_removed_at TIMESTAMPTZ); \
         INSERT INTO scratch.command_refusal_command_kinds VALUES (42, 'something_else'); \
         SET search_path = scratch",
    )
    .execute(&mut *conn)
    .await
    .expect("scratch catalog");
    let error = sqlx::raw_sql(MIGRATION_0089)
        .execute(&mut *conn)
        .await
        .expect_err("a divergent id 42 must fail the migration");
    assert!(
        error.to_string().contains("id 42 is not withdraw_review"),
        "{error}"
    );
}
