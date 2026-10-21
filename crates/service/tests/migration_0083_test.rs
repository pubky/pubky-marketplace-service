//! Migrations 0083 to 0086: per-item review keys over a database that
//! already holds reviews and attestations. Existing rows are keyed to the
//! order's first line (the only listing the service attested), an
//! attestation's listing is the one its claims bind, the append-only rule
//! survives the backfill, a binary from before 0083 still inserts, and the
//! transactional files are directly rerunnable.

use std::borrow::Cow;

use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0083: &str = include_str!("../migrations/0083_review_items.sql");
const MIGRATION_0086: &str = include_str!("../migrations/0086_review_item_keys.sql");

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

fn aggregate(listing_id: &str) -> String {
    format!("listing:{SELLER}_{listing_id}")
}

fn uri(listing_id: &str) -> String {
    format!("pubky://{SELLER}/pub/pubky.app/marketplace/v1/listings/{listing_id}")
}

/// A completed two-line order as a binary from before 0083 left it.
async fn seed_order(pool: &PgPool) -> Uuid {
    let order_id = Uuid::new_v4();
    let lines = json!([
        { "listing_aggregate_id": aggregate("boots_01"), "quantity": 1 },
        { "listing_aggregate_id": aggregate("laces_02"), "quantity": 1 },
    ]);
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, $2, $3, 6, 'completed', $4, NULL, 25000, 1200, 26200, 'USD', 2, 1, $5, \
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

/// A review row with the columns a binary from before 0083 writes.
async fn insert_legacy_review(
    pool: &PgPool,
    order_id: Uuid,
    reviewer: &str,
    role: &str,
) -> Result<(), sqlx::Error> {
    let subject = if role == "buyer" { SELLER } else { BUYER };
    sqlx::query(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         rating, text, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, 5, 'Fine.', now(), now())",
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(reviewer)
    .bind(role)
    .bind(subject)
    .execute(pool)
    .await
    .map(|_| ())
}

/// An attestation row with the columns a binary from before 0083 writes.
async fn insert_legacy_attestation(
    pool: &PgPool,
    order_id: Uuid,
    reviewer: &str,
    listing_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO review_attestations (order_id, reviewer_pubky, order_ref, jws, claims, \
         issued_at) VALUES ($1, $2, $3, 'h.p.s', $4, now())",
    )
    .bind(order_id)
    .bind(reviewer)
    .bind("ab".repeat(32))
    .bind(json!({ "v": 1, "sub": reviewer, "listing": uri(listing_id), "order_ref": "ab".repeat(32) }))
    .execute(pool)
    .await
    .map(|_| ())
}

async fn review_keys(pool: &PgPool, order_id: Uuid) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT reviewer_role, listing_aggregate_id FROM reviews WHERE order_id = $1 \
         ORDER BY reviewer_role, listing_aggregate_id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("review keys")
}

async fn attestation_keys(pool: &PgPool, order_id: Uuid) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT reviewer_pubky, listing_uri FROM review_attestations WHERE order_id = $1 \
         ORDER BY reviewer_pubky, listing_uri",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("attestation keys")
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

#[sqlx::test(migrations = false)]
async fn existing_reviews_and_attestations_are_keyed_to_the_first_line(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(82).run(&pool).await.expect("0001..0082 apply");
    let order_id = seed_order(&pool).await;
    insert_legacy_review(&pool, order_id, BUYER, "buyer")
        .await
        .expect("legacy buyer review");
    insert_legacy_review(&pool, order_id, SELLER, "seller")
        .await
        .expect("legacy seller review");
    insert_legacy_attestation(&pool, order_id, BUYER, "boots_01")
        .await
        .expect("legacy buyer attestation");
    insert_legacy_attestation(&pool, order_id, SELLER, "boots_01")
        .await
        .expect("legacy seller attestation");

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0083..0086 apply over existing rows");

    let first_line = aggregate("boots_01");
    assert_eq!(
        review_keys(&pool, order_id).await,
        vec![
            ("buyer".to_string(), first_line.clone()),
            ("seller".to_string(), first_line.clone()),
        ]
    );
    let ratings: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM reviews WHERE shipping IS NOT NULL OR communication IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("ratings");
    assert_eq!(ratings.0, 0, "existing reviews carry no seller ratings");
    assert_eq!(
        attestation_keys(&pool, order_id).await,
        vec![
            (BUYER.to_string(), uri("boots_01")),
            (SELLER.to_string(), uri("boots_01")),
        ]
    );

    // The keys moved to the per-listing indexes.
    let constraints: Vec<(String, String)> = sqlx::query_as(
        "SELECT conname::text, contype::text FROM pg_constraint \
         WHERE conrelid IN ('reviews'::regclass, 'review_attestations'::regclass) \
           AND contype IN ('u', 'p') ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("constraints");
    assert_eq!(
        constraints,
        vec![
            ("review_attestations_item_key".to_string(), "p".to_string()),
            (
                "reviews_one_per_order_role_listing".to_string(),
                "u".to_string()
            ),
            ("reviews_pkey".to_string(), "p".to_string()),
        ]
    );

    // Attestations stay append-only after the backfill lifted the trigger.
    for statement in [
        "UPDATE review_attestations SET jws = 'x.y.z' WHERE order_id = $1",
        "DELETE FROM review_attestations WHERE order_id = $1",
    ] {
        assert!(
            sqlx::query(statement)
                .bind(order_id)
                .execute(&pool)
                .await
                .is_err(),
            "{statement} must be refused"
        );
    }

    // A binary from before 0083 keeps inserting during a rolling deploy:
    // its rows are keyed to the first line, so its duplicate is refused by
    // the new key.
    let error = insert_legacy_review(&pool, order_id, BUYER, "buyer")
        .await
        .expect_err("a second first-line buyer review");
    assert!(is_unique_violation(&error), "{error}");
    let error = insert_legacy_attestation(&pool, order_id, BUYER, "boots_01")
        .await
        .expect_err("a second first-line attestation");
    assert!(is_unique_violation(&error), "{error}");
    let other_order = seed_order(&pool).await;
    insert_legacy_review(&pool, other_order, BUYER, "buyer")
        .await
        .expect("an old binary's review");
    insert_legacy_attestation(&pool, other_order, BUYER, "boots_01")
        .await
        .expect("an old binary's attestation");
    assert_eq!(
        review_keys(&pool, other_order).await,
        vec![("buyer".to_string(), first_line)]
    );
    assert_eq!(
        attestation_keys(&pool, other_order).await,
        vec![(BUYER.to_string(), uri("boots_01"))]
    );

    // The buyer may now review the second line, with its own attestation.
    sqlx::query(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         listing_aggregate_id, rating, text, shipping, communication, created_at, updated_at) \
         VALUES ($1, $2, $3, 'buyer', $4, $5, 4, '', 5, 4, now(), now())",
    )
    .bind(Uuid::new_v4())
    .bind(order_id)
    .bind(BUYER)
    .bind(SELLER)
    .bind(aggregate("laces_02"))
    .execute(&pool)
    .await
    .expect("a second item review");
    insert_legacy_attestation(&pool, order_id, BUYER, "laces_02")
        .await
        .expect("a second item attestation");

    // The column must name the listing the claims bind; seller ratings are
    // the buyer's and 1 to 5.
    let mismatch = sqlx::query(
        "INSERT INTO review_attestations (order_id, reviewer_pubky, listing_uri, order_ref, \
         jws, claims, issued_at) VALUES ($1, $2, $3, 'r', 'h.p.s', $4, now())",
    )
    .bind(other_order)
    .bind(SELLER)
    .bind(uri("laces_02"))
    .bind(json!({ "listing": uri("boots_01") }))
    .execute(&pool)
    .await;
    assert!(mismatch.is_err(), "listing_uri must match the claims");
    for (role, shipping, communication) in [
        ("seller", Some(5), Some(5)),
        ("buyer", Some(6), Some(5)),
        ("buyer", Some(5), None),
    ] {
        let refused = sqlx::query(
            "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
             listing_aggregate_id, rating, text, shipping, communication, created_at, updated_at) \
             VALUES ($1, $2, 'x', $3, 'y', $4, 4, '', $5, $6, now(), now())",
        )
        .bind(Uuid::new_v4())
        .bind(other_order)
        .bind(role)
        .bind(aggregate("laces_02"))
        .bind(shipping)
        .bind(communication)
        .execute(&pool)
        .await;
        assert!(
            refused.is_err(),
            "{role} {shipping:?} {communication:?} must be refused"
        );
    }

    // The transactional files rerun as no-ops.
    let before = (
        review_keys(&pool, order_id).await,
        attestation_keys(&pool, order_id).await,
    );
    for (name, sql) in [("0083", MIGRATION_0083), ("0086", MIGRATION_0086)] {
        let mut tx = pool.begin().await.expect("tx");
        sqlx::raw_sql(sql)
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|error| panic!("{name} must be directly rerunnable: {error}"));
        tx.commit().await.expect("commit");
    }
    assert_eq!(
        (
            review_keys(&pool, order_id).await,
            attestation_keys(&pool, order_id).await
        ),
        before
    );
}
