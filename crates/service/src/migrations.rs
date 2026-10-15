//! Startup migrations.
//!
//! 0032 transfers the refusal-audit catalog to the NOLOGIN
//! `marketplace_refusal_audit_owner` and revokes it from the migration login.
//! 0040 and 0043 append to that catalog without taking the role back, so a
//! least-authority (non-superuser) migration login is refused. Both files are
//! applied in production and immutable, so the login borrows the owner role
//! for the run instead, the same way 0032's own rerun bootstrap does. A
//! superuser needs no membership and is left untouched.
//!
//! An applied version this build does not know is tolerated only when it is
//! newer than every migration in the build: a newer image (or a branch image
//! on staging) migrated first, and this one must still start so a rollback
//! works. Migrations are additive, so the older code runs against the newer
//! schema. An unknown version at or below the build's newest one means a file
//! was removed or renumbered, and startup is refused as before. Checksums of
//! known versions are still verified.

use std::borrow::Cow;

use sqlx::migrate::{Migrate, Migrator};
use sqlx::{PgConnection, PgPool};

const OWNER_CREATED_BY: i64 = 32;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn run(pool: &PgPool) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    // sqlx's migration lock is held across the ledger read too, so a replica
    // migrating concurrently cannot commit a newer version between this
    // build's tolerance decision and its run. Postgres session advisory locks
    // stack, so `run_direct` taking the same lock inside is fine.
    Migrate::lock(&mut *connection).await?;
    let result = match run_locked(&mut connection).await {
        Ok(()) => Migrate::unlock(&mut *connection)
            .await
            .map_err(anyhow::Error::from),
        Err(error) => Err(error),
    };
    if result.is_err() {
        // A failed `run_direct` returns before releasing its own level of the
        // lock, so one unlock is not enough. Closing the session releases
        // every level instead of returning a locked connection to the pool.
        if let Err(error) = connection.close().await {
            tracing::warn!(error = %error, "failed to close the migration connection");
        }
    }
    result
}

async fn run_locked(connection: &mut PgConnection) -> anyhow::Result<()> {
    let applied = applied_versions(&mut *connection).await?;
    let migrator = tolerating_newer(&MIGRATOR, &applied)?;
    let superuser: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")
            .fetch_one(&mut *connection)
            .await?;
    if superuser || !pending(&migrator, &applied) {
        return Ok(migrator.run_direct(&mut *connection).await?);
    }
    if !owner_exists(&mut *connection).await? {
        through(&migrator, OWNER_CREATED_BY)
            .run_direct(&mut *connection)
            .await?;
    }
    sqlx::query("GRANT marketplace_refusal_audit_owner TO CURRENT_USER")
        .execute(&mut *connection)
        .await?;
    let migrated = migrator.run_direct(&mut *connection).await;
    let revoked = sqlx::query("REVOKE marketplace_refusal_audit_owner FROM CURRENT_USER")
        .execute(&mut *connection)
        .await;
    migrated?;
    revoked?;
    Ok(())
}

/// Successfully applied versions; empty before the ledger exists.
async fn applied_versions(connection: &mut PgConnection) -> sqlx::Result<Vec<i64>> {
    let ledger: Option<String> = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::text")
        .fetch_one(&mut *connection)
        .await?;
    if ledger.is_none() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
        .fetch_all(&mut *connection)
        .await
}

fn pending(migrator: &Migrator, applied: &[i64]) -> bool {
    migrator
        .iter()
        .any(|migration| !applied.contains(&migration.version))
}

/// `migrator` with `ignore_missing` set when every applied version it lacks
/// is newer than its newest migration; an error naming any other unknown
/// applied version.
pub fn tolerating_newer(migrator: &Migrator, applied: &[i64]) -> anyhow::Result<Migrator> {
    let known: Vec<i64> = migrator.iter().map(|migration| migration.version).collect();
    let newest = known.iter().copied().max().unwrap_or(0);
    let (newer, removed): (Vec<i64>, Vec<i64>) = applied
        .iter()
        .copied()
        .filter(|version| !known.contains(version))
        .partition(|version| *version > newest);
    if !removed.is_empty() {
        anyhow::bail!(
            "applied migrations {removed:?} are missing from this build \
             (removed or renumbered); refusing to start"
        );
    }
    if !newer.is_empty() {
        tracing::warn!(
            versions = ?newer,
            newest_known = newest,
            "database has migrations newer than this build; starting without them"
        );
    }
    Ok(Migrator {
        migrations: migrator.migrations.clone(),
        ignore_missing: migrator.ignore_missing || !newer.is_empty(),
        locking: migrator.locking,
        no_tx: migrator.no_tx,
    })
}

async fn owner_exists(connection: &mut PgConnection) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_owner')",
    )
    .fetch_one(connection)
    .await
}

fn through(migrator: &Migrator, version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            migrator
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: migrator.ignore_missing,
        locking: migrator.locking,
        no_tx: migrator.no_tx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::migrate::{Migration, MigrationType};

    fn migrator(versions: &[i64]) -> Migrator {
        Migrator {
            migrations: Cow::Owned(
                versions
                    .iter()
                    .map(|version| {
                        Migration::new(
                            *version,
                            Cow::Owned(format!("m{version}")),
                            MigrationType::Simple,
                            Cow::Borrowed("SELECT 1"),
                            false,
                        )
                    })
                    .collect(),
            ),
            ignore_missing: false,
            locking: true,
            no_tx: false,
        }
    }

    #[test]
    fn a_ledger_this_build_fully_knows_keeps_missing_versions_fatal() {
        let tolerant = tolerating_newer(&migrator(&[1, 2, 3]), &[1, 2]).expect("known ledger");
        assert!(!tolerant.ignore_missing);
        assert_eq!(tolerant.iter().count(), 3);
    }

    #[test]
    fn versions_newer_than_the_build_are_tolerated() {
        let tolerant =
            tolerating_newer(&migrator(&[1, 2, 3]), &[1, 2, 3, 72]).expect("newer ledger");
        assert!(tolerant.ignore_missing);
    }

    #[test]
    fn an_unknown_version_inside_the_build_range_is_refused() {
        let build = migrator(&[1, 2, 3, 30]);
        // 25 was renumbered or removed; 0 is below the newest. Even with a
        // newer version present, either one refuses startup.
        for ledger in [vec![1, 2, 25], vec![0, 1, 2], vec![1, 2, 25, 72]] {
            let error = tolerating_newer(&build, &ledger)
                .err()
                .unwrap_or_else(|| panic!("{ledger:?} must be refused"));
            assert!(
                error.to_string().contains("missing from this build"),
                "{error}"
            );
        }
    }

    #[test]
    fn an_empty_ledger_is_a_fresh_database() {
        assert!(
            !tolerating_newer(&migrator(&[1]), &[])
                .expect("fresh")
                .ignore_missing
        );
    }
}
