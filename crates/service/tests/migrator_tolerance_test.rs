//! Startup migrations against a ledger that is ahead of this build: a newer
//! (or branch) image migrated first. The build must still start, so an image
//! rollback works, while a version missing from inside its own range still
//! refuses startup and checksums of known versions are still verified.

use std::time::Duration;

use sqlx::migrate::Migrate;
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

/// Rolling deploy: another replica holds the migration lock and commits a
/// newer version while this build waits for it. The tolerance decision is
/// taken under the lock, so this build still starts.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_newer_version_committed_while_waiting_for_the_lock_is_tolerated(pool: PgPool) {
    production_ledger(&pool).await;
    let mut other_replica = pool.acquire().await.expect("other replica connection");
    Migrate::lock(&mut *other_replica)
        .await
        .expect("other replica takes the migration lock");

    let starting = tokio::spawn({
        let pool = pool.clone();
        async move { marketplace_service::migrations::run(&pool).await }
    });
    let waiting = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
                   AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";
    let mut blocked = false;
    for _ in 0..500 {
        let count: i64 = sqlx::query_scalar(waiting)
            .fetch_one(&mut *other_replica)
            .await
            .expect("lock waiters");
        if count > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        blocked,
        "the starting build must wait for the migration lock"
    );

    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (9999, 'from the other replica', true, '\\x00', 0)",
    )
    .execute(&mut *other_replica)
    .await
    .expect("other replica commits a newer migration");
    Migrate::unlock(&mut *other_replica)
        .await
        .expect("other replica releases the lock");

    starting
        .await
        .expect("startup task")
        .expect("a version committed during the wait is tolerated");
}

/// Whether any session (including an idle pooled connection a failed run
/// returned) still holds an advisory lock in this test database.
async fn migration_lock_is_free(pool: &PgPool) -> bool {
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(pool)
    .await
    .expect("advisory locks");
    held == 0
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_migration_run_leaves_no_migration_lock_held(pool: PgPool) {
    production_ledger(&pool).await;
    // A dirty row makes sqlx's run_direct fail after taking its own lock level.
    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 60")
        .execute(&pool)
        .await
        .expect("mark a migration dirty");
    marketplace_service::migrations::run(&pool)
        .await
        .expect_err("a dirty ledger refuses startup");
    assert!(
        migration_lock_is_free(&pool).await,
        "a failed run inside run_direct must release the migration lock"
    );

    sqlx::query("UPDATE _sqlx_migrations SET success = true WHERE version = 60")
        .execute(&pool)
        .await
        .expect("clean the ledger");
    record(&pool, 0).await;
    marketplace_service::migrations::run(&pool)
        .await
        .expect_err("an unknown version below the newest refuses startup");
    assert!(
        migration_lock_is_free(&pool).await,
        "a refusal before run_direct must release the migration lock"
    );
}
