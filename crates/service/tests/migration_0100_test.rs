//! Migration 0100: single-seller mode's refusal kind, route surface and route
//! command kind join the closed refusal-audit catalogs. Directly rerunnable.

mod common;

use sqlx::PgPool;

const MIGRATION: &str = include_str!("../migrations/0100_single_seller_refusals.sql");

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0100_adds_the_catalog_rows_and_is_rerunnable(pool: PgPool) {
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0100 must be directly rerunnable");
    for (table, id, name) in [
        ("command_refusal_kinds", 41, "seller_not_served"),
        ("command_refusal_surface_kinds", 3, "seller_route"),
        ("command_refusal_command_kinds", 44, "seller_route"),
    ] {
        let rows: Vec<(i16, String)> = sqlx::query_as(&format!(
            "SELECT id, name FROM {table} WHERE id >= $1 ORDER BY id"
        ))
        .bind(id)
        .fetch_all(&pool)
        .await
        .expect("catalog rows");
        assert_eq!(rows, vec![(id, name.to_string())], "{table}");
    }
}

/// A catalog whose new id already names something else stops the migration.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0100_refuses_a_catalog_where_an_id_names_something_else(pool: PgPool) {
    for (table, id, message) in [
        (
            "command_refusal_kinds",
            41,
            "command_refusal_kinds id 41 is not seller_not_served",
        ),
        (
            "command_refusal_surface_kinds",
            3,
            "command_refusal_surface_kinds id 3 is not seller_route",
        ),
        (
            "command_refusal_command_kinds",
            44,
            "command_refusal_command_kinds id 44 is not seller_route",
        ),
    ] {
        let mut conn = pool.acquire().await.expect("connection");
        let mut setup =
            String::from("DROP SCHEMA IF EXISTS scratch CASCADE; CREATE SCHEMA scratch;");
        for scratch in [
            "command_refusal_kinds",
            "command_refusal_surface_kinds",
            "command_refusal_command_kinds",
        ] {
            setup.push_str(&format!(
                " CREATE TABLE scratch.{scratch} (id SMALLINT PRIMARY KEY, name TEXT NOT NULL);"
            ));
        }
        setup.push_str(&format!(
            " INSERT INTO scratch.{table} VALUES ({id}, 'something_else');"
        ));
        setup.push_str(" SET search_path = scratch");
        sqlx::raw_sql(&setup)
            .execute(&mut *conn)
            .await
            .expect("scratch catalogs");
        let error = sqlx::raw_sql(MIGRATION)
            .execute(&mut *conn)
            .await
            .expect_err("a divergent id must fail the migration");
        assert!(error.to_string().contains(message), "{table}: {error}");
        sqlx::raw_sql("SET search_path = public")
            .execute(&mut *conn)
            .await
            .expect("reset search path");
    }
}
