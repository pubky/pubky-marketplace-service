//! Test-only migrator. Migration 0032 is byte-immutable and fail-closed on
//! `marketplace_refusal_audit_retention CONNECTION LIMIT 1`. Migration 0037
//! raises that cluster-global limit to 2, so the next `#[sqlx::test]` database
//! would replay 0032 against LIMIT 2 and abort unless the limit is restored
//! before 0032 runs.
//!
//! Roles and role memberships (`pg_authid`, `pg_auth_members`) are
//! cluster-global, whereas PostgreSQL advisory locks are scoped to a single
//! database and do not serialize two separate `#[sqlx::test]` databases.
//! Instead, this migrator wraps only the `0032..=0037` cluster-role window in
//! one explicit transaction holding `EXCLUSIVE` locks on the shared catalogs
//! `pg_catalog.pg_authid` and `pg_catalog.pg_auth_members` (`database = 0` in
//! `pg_locks`). That serializes cluster-role setup across all databases,
//! threads, and binaries without blocking concurrent `SELECT`, `CREATE
//! DATABASE`, or `DROP DATABASE`, while migrations outside `0032..=0037`
//! (including `CREATE INDEX CONCURRENTLY`) run in full parallelism.
//!
//! Original production checksums are preserved verbatim so ledger checks
//! match `sqlx::migrate!("./migrations")`. Test-only marker versions `0` and
//! `9_000_000_001` stay in `_sqlx_migrations` for tests that strip them. If a
//! migration inside `0032..=0037` fails, dropping the connection rolls back
//! the transaction and immediately releases the shared catalog locks.

use std::borrow::Cow;
use std::sync::LazyLock;

use sqlx::migrate::{Migration, MigrationType, Migrator};

const ROLE_WINDOW_START: i64 = 32;
const ROLE_WINDOW_END: i64 = 37;

const ROLE_LOCK_AND_RESTORE_PRELUDE: &str = "\
BEGIN;
LOCK TABLE pg_catalog.pg_authid, pg_catalog.pg_auth_members IN EXCLUSIVE MODE;
DO $restore$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'marketplace_refusal_audit_retention'
  ) THEN
    ALTER ROLE marketplace_refusal_audit_retention CONNECTION LIMIT 1;
  END IF;
END $restore$;
";

pub static TEST_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let all = sqlx::migrate!("./migrations");
    let prelude = Migration::new(
        0,
        Cow::Borrowed("restore_0032_retention_connlimit"),
        MigrationType::Simple,
        Cow::Borrowed("SELECT 1;"),
        true,
    );
    let release = Migration::new(
        9_000_000_001,
        Cow::Borrowed("release_test_migration_lock"),
        MigrationType::Simple,
        Cow::Borrowed("SELECT 1;"),
        true,
    );

    let mut saw_start = false;
    let mut saw_end = false;
    let mut migrations = Vec::with_capacity(all.migrations.len() + 2);
    migrations.push(prelude);

    for migration in all.migrations.iter() {
        let mut wrapped = migration.clone();
        if (ROLE_WINDOW_START..=ROLE_WINDOW_END).contains(&wrapped.version) {
            assert!(
                !wrapped.no_tx,
                "migration {} inside 0032..=0037 role lock window must be transactional",
                wrapped.version
            );
            wrapped.no_tx = true;
            if wrapped.version == ROLE_WINDOW_START {
                saw_start = true;
                wrapped.sql = Cow::Owned(format!("{ROLE_LOCK_AND_RESTORE_PRELUDE}{}", wrapped.sql));
            } else if wrapped.version == ROLE_WINDOW_END {
                saw_end = true;
                wrapped.sql = Cow::Owned(format!("{}\nCOMMIT;", wrapped.sql));
            }
        }
        migrations.push(wrapped);
    }

    assert!(
        saw_start && saw_end,
        "TEST_MIGRATOR requires migrations {ROLE_WINDOW_START} and {ROLE_WINDOW_END}"
    );
    migrations.push(release);

    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: all.ignore_missing,
        locking: all.locking,
        no_tx: all.no_tx,
    }
});

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::PgPool;

    use super::*;

    #[test]
    fn test_migrator_preserves_production_checksums_and_wraps_role_window() {
        let production = sqlx::migrate!("./migrations");
        assert_eq!(
            TEST_MIGRATOR.migrations.len(),
            production.migrations.len() + 2
        );
        assert_eq!(TEST_MIGRATOR.migrations.first().unwrap().version, 0);
        assert_eq!(
            TEST_MIGRATOR.migrations.last().unwrap().version,
            9_000_000_001
        );

        for (wrapped, original) in TEST_MIGRATOR.migrations[1..TEST_MIGRATOR.migrations.len() - 1]
            .iter()
            .zip(production.migrations.iter())
        {
            assert_eq!(wrapped.version, original.version);
            assert_eq!(
                wrapped.checksum.as_ref(),
                original.checksum.as_ref(),
                "migration {} checksum must match production",
                original.version
            );
            if (ROLE_WINDOW_START..=ROLE_WINDOW_END).contains(&wrapped.version) {
                assert!(wrapped.no_tx);
            } else {
                assert_eq!(wrapped.no_tx, original.no_tx);
            }
        }
    }

    #[sqlx::test(migrations = false)]
    async fn parallel_test_migrations_do_not_race_cluster_roles(admin: PgPool) {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let base_opts = PgConnectOptions::from_str(&database_url).expect("valid DATABASE_URL");

        let run_id = uuid::Uuid::new_v4().simple().to_string();
        let db_names: Vec<String> = (0..8)
            .map(|i| format!("_test_migrator_race_{}_{i}", &run_id[..12]))
            .collect();

        for db in &db_names {
            sqlx::query(&format!("CREATE DATABASE \"{db}\""))
                .execute(&admin)
                .await
                .expect("create temporary test database");
        }

        let mut tasks = tokio::task::JoinSet::new();
        for db in db_names.clone() {
            let opts = base_opts.clone().database(&db);
            tasks.spawn(async move {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(opts)
                    .await
                    .expect("connect to temp database");
                let res = TEST_MIGRATOR.run(&pool).await;
                pool.close().await;
                (db, res)
            });
        }

        let mut errors = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let (db, res) = joined.expect("migration task did not panic");
            if let Err(err) = res {
                errors.push(format!("{db}: {err}"));
            }
        }

        for db in &db_names {
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{db}\""))
                .execute(&admin)
                .await;
        }

        assert!(
            errors.is_empty(),
            "parallel TEST_MIGRATOR runs raced:\n{}",
            errors.join("\n")
        );

        let conn_limit: i32 = sqlx::query_scalar(
            "SELECT rolconnlimit FROM pg_catalog.pg_roles \
             WHERE rolname = 'marketplace_refusal_audit_retention'",
        )
        .fetch_one(&admin)
        .await
        .expect("retention role exists");
        assert_eq!(conn_limit, 2);
    }
}
