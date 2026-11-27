//! Operator-only release of a wrong-identity binding while the store is empty.

use sqlx::{PgPool, Postgres, Transaction};

use crate::store_seller::{other_seller_rows_on, take_binding_lock};

/// A release that passed every check and deleted its binding inside an open
/// transaction. The shared advisory lock stays held until `commit` or drop.
pub struct PendingRelease<'a> {
    tx: Transaction<'a, Postgres>,
    seller_pubky: String,
    seller_prefix: String,
}

impl PendingRelease<'_> {
    pub async fn commit(self) -> anyhow::Result<String> {
        self.tx.commit().await?;
        tracing::info!(seller = self.seller_prefix, "store.binding.released");
        Ok(self.seller_pubky)
    }
}

/// Runs every release check and the delete, leaving the transaction open so
/// callers can prove boot and release lock ordering. Ordinary callers should
/// use [`release`], which immediately commits the returned pending release.
pub async fn begin_release<'a>(
    pool: &'a PgPool,
    confirm: &str,
) -> anyhow::Result<PendingRelease<'a>> {
    if confirm.is_empty() {
        anyhow::bail!("release --confirm requires a pubky or its first 8 characters");
    }
    let mut tx = pool.begin().await?;
    take_binding_lock(&mut tx).await?;
    let another_connection: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity \
         WHERE datname = current_database() AND usename = current_user \
         AND pid <> pg_backend_pid())",
    )
    .fetch_one(&mut *tx)
    .await?;
    if another_connection {
        anyhow::bail!(
            "another service connection is open; stop the service and connector worker before release"
        );
    }
    let binding = sqlx::query_as::<_, crate::store_seller::BindingRow>(
        "SELECT seller_pubky, bound_by, claimed_at, claim_session_id, service_commit \
         FROM store_seller_binding WHERE singleton = TRUE FOR UPDATE",
    )
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("the store has no seller binding to release"))?;
    let prefix = crate::logging::actor_prefix(&binding.seller_pubky);
    if confirm != binding.seller_pubky && confirm != prefix {
        anyhow::bail!("--confirm does not match the current store binding");
    }
    let rows = other_seller_rows_on(&mut tx, None).await?;
    if !rows.is_empty() {
        anyhow::bail!(
            "store binding cannot be released while the database holds seller rows ({}: {}); \
             use a fresh database",
            rows.summary(),
            rows.seller_sample()
        );
    }
    sqlx::query("SELECT set_config('marketplace.release_store_binding', 'on', true)")
        .execute(&mut *tx)
        .await?;
    let deleted: Option<String> = sqlx::query_scalar(
        "DELETE FROM store_seller_binding WHERE singleton = TRUE RETURNING seller_pubky",
    )
    .fetch_optional(&mut *tx)
    .await?;
    if deleted.as_deref() != Some(binding.seller_pubky.as_str()) {
        anyhow::bail!("store binding changed during release");
    }
    Ok(PendingRelease {
        tx,
        seller_pubky: binding.seller_pubky,
        seller_prefix: prefix,
    })
}

/// Release runs in one transaction, with the shared lock taken before the
/// activity check. Any live service connection therefore refuses the release.
pub async fn release(pool: &PgPool, confirm: &str) -> anyhow::Result<String> {
    begin_release(pool, confirm).await?.commit().await
}
