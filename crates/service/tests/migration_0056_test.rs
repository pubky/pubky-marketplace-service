//! 0056: per-order delivery periods and an optional guarantee policy
//! version. A row that predates the file keeps its values across a rerun and
//! reads as "no seller terms captured"; the two periods are bounded to 1-60
//! days and may stay NULL; `guarantee_policy_version` accepts NULL. The
//! service paths are exercised in `seller_delivery_terms_test.rs`.

use sqlx::PgPool;
use uuid::Uuid;

async fn seed_order(pool: &PgPool, guarantee_policy_version: Option<i32>) -> Uuid {
    let order_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 1, 'pending_payment', '[]', '{}', 100, 10, 110, 'EUR', \
         2, $2, $3, now(), now())",
    )
    .bind(order_id)
    .bind(guarantee_policy_version)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .expect("seed order");
    order_id
}

async fn set_days(pool: &PgPool, column: &str, order_id: Uuid, days: i32) -> Result<(), String> {
    sqlx::query(&format!("UPDATE orders SET {column} = $2 WHERE id = $1"))
        .bind(order_id)
        .bind(days)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0056_bounds_the_periods_and_frees_the_guarantee_version(pool: PgPool) {
    let legacy = seed_order(&pool, Some(1)).await;

    sqlx::raw_sql(include_str!("../migrations/0056_order_delivery_terms.sql"))
        .execute(&pool)
        .await
        .expect("0056 must be directly rerunnable");

    // A row from before the migration keeps its version and has no terms.
    let (delivery, complete, captured, version): (Option<i32>, Option<i32>, bool, Option<i32>) =
        sqlx::query_as(
            "SELECT delivery_assume_days, auto_complete_days, \
                    delivery_terms_captured_at IS NOT NULL, guarantee_policy_version \
             FROM orders WHERE id = $1",
        )
        .bind(legacy)
        .fetch_one(&pool)
        .await
        .expect("legacy row");
    assert_eq!(
        (delivery, complete, captured, version),
        (None, None, false, Some(1))
    );

    // The version is optional for new orders.
    let without_version = seed_order(&pool, None).await;
    let version: Option<i32> =
        sqlx::query_scalar("SELECT guarantee_policy_version FROM orders WHERE id = $1")
            .bind(without_version)
            .fetch_one(&pool)
            .await
            .expect("row without a version");
    assert_eq!(version, None);

    // The periods accept 1 and 60 and refuse anything outside.
    for column in ["delivery_assume_days", "auto_complete_days"] {
        for accepted in [1, 60] {
            set_days(&pool, column, legacy, accepted)
                .await
                .unwrap_or_else(|error| panic!("{column}={accepted} refused: {error}"));
        }
        for refused in [0, 61, -1] {
            assert!(
                set_days(&pool, column, legacy, refused).await.is_err(),
                "{column}={refused} must violate the range check"
            );
        }
    }
}
