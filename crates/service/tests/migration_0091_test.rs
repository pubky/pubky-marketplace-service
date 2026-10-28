//! Migration 0091: who marks an order paid (`orders.payment_confirmation`)
//! and the PayPal hint (`orders.gateway_payment_seen_at`). Both are nullable,
//! the mode is bounded to its two values, and the file reruns as a no-op that
//! keeps what was frozen.

use sqlx::PgPool;

mod common;

const MIGRATION_0091: &str = include_str!("../migrations/0091_seller_confirmed_payment.sql");

async fn order_id(pool: &PgPool) -> uuid::Uuid {
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let buyer = common::new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    uuid::Uuid::parse_str(&order.order_id).expect("order uuid")
}

async fn set_mode(pool: &PgPool, id: uuid::Uuid, mode: Option<&str>) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE orders SET payment_confirmation = $2 WHERE id = $1")
        .bind(id)
        .bind(mode)
        .execute(pool)
        .await
        .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_mode_is_one_of_two_values_or_null(pool: PgPool) {
    let id = order_id(&pool).await;
    let (mode, seen): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT payment_confirmation, gateway_payment_seen_at FROM orders WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .expect("order row");
    assert_eq!((mode, seen), (None, None), "an unbound order has neither");
    for mode in [Some("seller"), Some("automatic"), None] {
        set_mode(&pool, id, mode).await.expect("allowed value");
    }
    for mode in ["service", "Seller", ""] {
        let refused = set_mode(&pool, id, Some(mode)).await.expect_err(mode);
        assert_eq!(
            refused
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("orders_payment_confirmation_values"),
            "{mode:?}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_what_was_frozen(pool: PgPool) {
    let id = order_id(&pool).await;
    set_mode(&pool, id, Some("seller")).await.expect("seller");
    sqlx::query("UPDATE orders SET gateway_payment_seen_at = now() WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("hint");
    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0091)
        .execute(&mut *tx)
        .await
        .expect("0091 must be directly rerunnable");
    tx.commit().await.expect("commit");
    let (mode, seen): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT payment_confirmation, gateway_payment_seen_at FROM orders WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .expect("order row");
    assert_eq!(mode.as_deref(), Some("seller"));
    assert!(seen.is_some());
}
