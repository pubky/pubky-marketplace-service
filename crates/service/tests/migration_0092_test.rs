//! Migration 0092: who releases an order's instant digital lines
//! (`orders.digital_release`), when (`orders.digital_released_at`), and the
//! `release_digital` refusal-audit command kind. The mode is bounded to its
//! two values with NULL valid, and the file reruns as a no-op that keeps what
//! was frozen.

use sqlx::PgPool;

mod common;

const MIGRATION_0092: &str = include_str!("../migrations/0092_digital_release.sql");

async fn order_id(pool: &PgPool) -> uuid::Uuid {
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let buyer = common::new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    uuid::Uuid::parse_str(&order.order_id).expect("order uuid")
}

async fn set_mode(pool: &PgPool, id: uuid::Uuid, mode: Option<&str>) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE orders SET digital_release = $2 WHERE id = $1")
        .bind(id)
        .bind(mode)
        .execute(pool)
        .await
        .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_mode_is_one_of_two_values_or_null_and_the_command_kind_is_catalogued(pool: PgPool) {
    let id = order_id(&pool).await;
    let (mode, released): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT digital_release, digital_released_at FROM orders WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert_eq!(
        (mode, released),
        (None, None),
        "an unpaid order has neither"
    );
    for mode in [Some("seller"), Some("payment"), None] {
        set_mode(&pool, id, mode).await.expect("allowed value");
    }
    for mode in ["automatic", "Seller", ""] {
        let refused = set_mode(&pool, id, Some(mode)).await.expect_err(mode);
        assert_eq!(
            refused
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("orders_digital_release_values"),
            "{mode:?}"
        );
    }
    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 43 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("command kinds");
    assert_eq!(kinds, vec![(43, "release_digital".to_string())]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_what_was_frozen(pool: PgPool) {
    let id = order_id(&pool).await;
    set_mode(&pool, id, Some("seller")).await.expect("seller");
    sqlx::query("UPDATE orders SET digital_released_at = now() WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("released");
    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0092)
        .execute(&mut *tx)
        .await
        .expect("0092 must be directly rerunnable");
    tx.commit().await.expect("commit");
    let (mode, released): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT digital_release, digital_released_at FROM orders WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert_eq!(mode.as_deref(), Some("seller"));
    assert!(released.is_some());
}
