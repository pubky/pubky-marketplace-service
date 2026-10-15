//! Migration 0071: `fulfillment.update_tracking` joins the closed
//! refusal-audit command-kind catalog. Directly rerunnable.

mod common;

use sqlx::PgPool;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0071_adds_the_command_kind_and_is_rerunnable(pool: PgPool) {
    sqlx::raw_sql(include_str!(
        "../migrations/0071_update_tracking_command_kind.sql"
    ))
    .execute(&pool)
    .await
    .expect("0071 must be directly rerunnable");

    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 39 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("command kinds");
    assert_eq!(
        kinds,
        vec![
            (39, "confirm_refund_destination".to_string()),
            (40, "update_tracking".to_string()),
        ]
    );
}
