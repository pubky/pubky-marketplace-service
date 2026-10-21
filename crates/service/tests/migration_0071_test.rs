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
        "SELECT id, name FROM command_refusal_command_kinds WHERE id BETWEEN 39 AND 40 ORDER BY id",
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

/// A catalog whose id 40 already names another kind must stop the migration:
/// `ON CONFLICT DO NOTHING` alone would keep the divergent row. The scratch
/// schema shadows the real table for this one connection.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0071_refuses_a_catalog_where_id_40_names_another_kind(pool: PgPool) {
    let mut conn = pool.acquire().await.expect("connection");
    sqlx::raw_sql(
        "CREATE SCHEMA scratch; \
         CREATE TABLE scratch.command_refusal_command_kinds \
           (id SMALLINT PRIMARY KEY, name TEXT NOT NULL); \
         INSERT INTO scratch.command_refusal_command_kinds VALUES (40, 'something_else'); \
         SET search_path = scratch",
    )
    .execute(&mut *conn)
    .await
    .expect("scratch catalog");
    let error = sqlx::raw_sql(include_str!(
        "../migrations/0071_update_tracking_command_kind.sql"
    ))
    .execute(&mut *conn)
    .await
    .expect_err("a divergent id 40 must fail the migration");
    assert!(
        error.to_string().contains("id 40 is not update_tracking"),
        "{error}"
    );
}
