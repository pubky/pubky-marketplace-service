//! Migration 0102: one permanent store seller binding, with release authority
//! available only as an exact transaction-local setting. Directly rerunnable.

mod common;

use sqlx::PgPool;

const MIGRATION: &str = include_str!("../migrations/0102_store_seller_binding.sql");

fn seller(byte: u8) -> String {
    marketplace_domain::pubky::encode_pubky(&[byte; 32])
}

async fn insert(pool: &PgPool, seller: &str) {
    sqlx::query(
        "INSERT INTO store_seller_binding (singleton, seller_pubky, bound_by) \
         VALUES (TRUE, $1, 'env')",
    )
    .bind(seller)
    .execute(pool)
    .await
    .expect("binding insert");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0102_is_rerunnable_and_enforces_one_valid_pubky(pool: PgPool) {
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0102 must be directly rerunnable");
    for invalid in ["", "not-a-pubky", &"y".repeat(51), &"y".repeat(53)] {
        let error = sqlx::query(
            "INSERT INTO store_seller_binding (singleton, seller_pubky, bound_by) \
             VALUES (TRUE, $1, 'env')",
        )
        .bind(invalid)
        .execute(&pool)
        .await
        .expect_err("an invalid or empty pubky is rejected");
        assert!(
            error.to_string().contains("store_seller_binding"),
            "{error}"
        );
    }
    insert(&pool, &seller(1)).await;
    let error = sqlx::query(
        "INSERT INTO store_seller_binding (singleton, seller_pubky, bound_by) \
         VALUES (TRUE, $1, 'claim')",
    )
    .bind(seller(2))
    .execute(&pool)
    .await
    .expect_err("a second row is impossible");
    assert!(error.to_string().contains("duplicate key"), "{error}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0102_release_trigger_is_null_safe_and_exact(pool: PgPool) {
    for operation in ["UPDATE", "DELETE", "TRUNCATE"] {
        let statement = match operation {
            "UPDATE" => "UPDATE store_seller_binding SET service_commit = 'test'",
            "DELETE" => "DELETE FROM store_seller_binding",
            "TRUNCATE" => "TRUNCATE store_seller_binding",
            _ => unreachable!(),
        };
        for setting in [None, Some("off"), Some("")] {
            insert(&pool, &seller(3)).await;
            let mut tx = pool.begin().await.expect("transaction");
            if let Some(value) = setting {
                sqlx::query("SELECT set_config('app.allow_store_binding_release', $1, true)")
                    .bind(value)
                    .execute(&mut *tx)
                    .await
                    .expect("release setting");
            }
            let error = sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .expect_err("release without authority is blocked")
                .to_string();
            assert!(
                error.contains("store seller binding is permanent"),
                "{error}"
            );
            tx.rollback().await.expect("rollback restores the binding");

            let mut release = pool.begin().await.expect("release transaction");
            sqlx::query("SELECT set_config('app.allow_store_binding_release', 'on', true)")
                .execute(&mut *release)
                .await
                .expect("release authority");
            sqlx::query(statement)
                .execute(&mut *release)
                .await
                .expect("release authority allows the operation");
            sqlx::query("SELECT set_config('app.allow_store_binding_release', 'on', true)")
                .execute(&mut *release)
                .await
                .expect("release authority remains transaction-local");
            sqlx::query("DELETE FROM store_seller_binding")
                .execute(&mut *release)
                .await
                .expect("fixture cleanup");
            release.commit().await.expect("release commits");
        }
    }
    insert(&pool, &seller(4)).await;
    let mut tx = pool.begin().await.expect("transaction");
    sqlx::query("SELECT set_config('app.allow_store_binding_release', 'on', true)")
        .execute(&mut *tx)
        .await
        .expect("release authority");
    sqlx::query("DELETE FROM store_seller_binding")
        .execute(&mut *tx)
        .await
        .expect("authorized delete");
    tx.rollback().await.expect("rollback");
    sqlx::query("DELETE FROM store_seller_binding")
        .execute(&pool)
        .await
        .expect_err("the setting does not leak out of its transaction");
}
