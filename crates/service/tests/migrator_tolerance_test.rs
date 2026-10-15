//! Startup migrations against a ledger that is ahead of this build: a newer
//! (or branch) image migrated first. The build must still start, so an image
//! rollback works, while a version missing from inside its own range still
//! refuses startup and checksums of known versions are still verified.

use sqlx::PgPool;

/// `TEST_MIGRATOR` wraps the catalog in test-only lock versions (0 and
/// 9_000_000_001); remove them so the ledger is what production holds.
async fn production_ledger(pool: &PgPool) {
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (0, 9000000001)")
        .execute(pool)
        .await
        .expect("drop test-only ledger rows");
}

async fn record(pool: &PgPool, version: i64) {
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES ($1, 'from another build', true, '\\x00', 0)",
    )
    .bind(version)
    .execute(pool)
    .await
    .expect("record a foreign migration");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_build_starts_against_a_ledger_with_newer_migrations(pool: PgPool) {
    production_ledger(&pool).await;
    marketplace_service::migrations::run(&pool)
        .await
        .expect("an up-to-date ledger starts");

    record(&pool, 9_999).await;
    marketplace_service::migrations::run(&pool)
        .await
        .expect("a newer migration from another build is tolerated");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_version_missing_from_inside_the_build_still_refuses_startup(pool: PgPool) {
    production_ledger(&pool).await;
    record(&pool, 0).await;
    let error = marketplace_service::migrations::run(&pool)
        .await
        .expect_err("an unknown version below the newest is refused");
    assert!(
        error.to_string().contains("missing from this build"),
        "{error}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checksums_of_known_versions_are_still_verified(pool: PgPool) {
    production_ledger(&pool).await;
    record(&pool, 9_999).await;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = '\\x00' WHERE version = 1")
        .execute(&pool)
        .await
        .expect("corrupt a checksum");
    let error = marketplace_service::migrations::run(&pool)
        .await
        .expect_err("a changed applied migration is refused");
    assert!(error.to_string().contains("previously applied"), "{error}");
}
