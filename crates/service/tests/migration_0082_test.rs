//! Migration 0082: `return.cancel` joins the closed refusal-audit
//! command-kind catalog. Directly rerunnable.

mod common;

use sqlx::PgPool;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0082_adds_the_command_kind_and_is_rerunnable(pool: PgPool) {
    sqlx::raw_sql(include_str!(
        "../migrations/0082_cancel_return_command_kind.sql"
    ))
    .execute(&pool)
    .await
    .expect("0082 must be directly rerunnable");

    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 41 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("command kinds");
    assert_eq!(kinds, vec![(41, "cancel_return".to_string())]);
}

/// A catalog whose id 41 already names another kind must stop the migration.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0082_refuses_a_catalog_where_id_41_names_another_kind(pool: PgPool) {
    let mut conn = pool.acquire().await.expect("connection");
    sqlx::raw_sql(
        "CREATE SCHEMA scratch; \
         CREATE TABLE scratch.command_refusal_command_kinds \
           (id SMALLINT PRIMARY KEY, name TEXT NOT NULL); \
         INSERT INTO scratch.command_refusal_command_kinds VALUES (41, 'something_else'); \
         SET search_path = scratch",
    )
    .execute(&mut *conn)
    .await
    .expect("scratch catalog");
    let error = sqlx::raw_sql(include_str!(
        "../migrations/0082_cancel_return_command_kind.sql"
    ))
    .execute(&mut *conn)
    .await
    .expect_err("a divergent id 41 must fail the migration");
    assert!(
        error.to_string().contains("id 41 is not cancel_return"),
        "{error}"
    );
}
