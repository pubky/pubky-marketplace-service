//! Commerce connectors: the framework that lets a seller's Shopify or
//! WooCommerce store stay in step with their Pubky listings and orders.
//!
//! The framework holds everything a store system does not know about
//! Pubky: sealed store credentials, the connection's address-recipient key,
//! the listing <-> product mapping, an idempotent order-push ledger, the
//! durable cursor over the service's event feed, inbound webhook dedupe and
//! the seller-authenticated routes. A Shopify store is connected through an app
//! the merchant creates in their own account: the service keeps that app's
//! client id and secret sealed per connection and exposes per-connection URLs
//! for the merchant to configure. A provider (Shopify, WooCommerce)
//! implements [`provider::ConnectorProvider`] and is registered in the
//! [`provider::ProviderRegistry`]; nothing else changes.
//!
//! Two processes use it. `marketplace-service` serves the `/v1/connectors`
//! routes. The `connector-worker` binary runs [`worker::run_forever`]. Both
//! see the same tables. Everything is off unless `CONNECTORS_ENABLED` is
//! true: [`crate::AppState::connectors`] is `None`, the routes answer
//! `connectors_disabled`, and the worker refuses to start.
//!
//! Money never moves through a store. Orders reach the store already paid,
//! with no buyer identity, and a cancel made in the store raises an alert
//! instead of a refund. A delivery address reaches the store only when the
//! buyer opted in at checkout, and is deleted once the store order exists.

pub mod config;
pub mod inbound;
pub mod keys;
pub mod ledger;
pub mod order_push;
pub mod outbox;
pub mod provider;
pub mod providers;
pub mod recipient;
pub mod routes;
pub mod stock;
pub mod store;
pub mod variant_stock;
pub mod worker;

use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

pub use config::{ConnectorSettings, MAX_CATALOG_PAGES_PER_POLL};
pub use keys::ConnectorKeys;
pub use provider::{ConnectorProvider, ProviderKind, ProviderRegistry};

/// What a process needs to serve or run connectors.
pub struct ConnectorRuntime {
    pub keys: Arc<ConnectorKeys>,
    pub providers: ProviderRegistry,
    pub settings: ConnectorSettings,
    /// Where per-variant store stock lands; skips it until the service has
    /// per-variant inventory ([`stock::VariantInventory`]).
    pub variant_inventory: Arc<dyn stock::VariantInventory>,
}

impl ConnectorRuntime {
    pub fn new(
        keys: Arc<ConnectorKeys>,
        providers: ProviderRegistry,
        settings: ConnectorSettings,
    ) -> Self {
        Self {
            keys,
            providers,
            settings,
            variant_inventory: Arc::new(stock::NoVariantInventory),
        }
    }

    pub fn with_variant_inventory(mut self, inventory: Arc<dyn stock::VariantInventory>) -> Self {
        self.variant_inventory = inventory;
        self
    }

    /// The merchant's own app credentials for a connection, opened for one
    /// call. A public-app connection ([`provider::AppKind::Public`]) is a
    /// reserved slot with no implementation: it has no app to open.
    pub async fn app_for(
        &self,
        pool: &PgPool,
        connection: &store::ConnectionRow,
    ) -> Result<Option<provider::AppContext>, store::OpenError> {
        match connection.app_kind() {
            provider::AppKind::Custom => store::open_app(pool, &self.keys, connection).await,
            provider::AppKind::None | provider::AppKind::Public => Ok(None),
        }
    }

    /// Opens a connection's store credential and attaches its own app.
    pub async fn open(
        &self,
        pool: &PgPool,
        connection: &store::ConnectionRow,
    ) -> Result<provider::ConnectionContext, store::OpenError> {
        let mut context = store::open_connection(pool, &self.keys, connection).await?;
        context.app = self.app_for(pool, connection).await?;
        Ok(context)
    }
}

/// The runtime from the environment, or `None` when `CONNECTORS_ENABLED` is
/// off. Enabled without `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY` is a startup
/// error: a connector with nowhere safe to keep a credential must not run.
pub fn runtime_from_env(
    providers: ProviderRegistry,
    keys: impl FnOnce() -> anyhow::Result<Option<Arc<ConnectorKeys>>>,
) -> anyhow::Result<Option<Arc<ConnectorRuntime>>> {
    let Some(settings) = ConnectorSettings::from_env()? else {
        return Ok(None);
    };
    let Some(keys) = keys()? else {
        anyhow::bail!(
            "{} is required when {} is true",
            keys::ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY,
            config::ENV_CONNECTORS_ENABLED
        );
    };
    Ok(Some(Arc::new(
        ConnectorRuntime::new(keys, providers, settings)
            .with_variant_inventory(Arc::new(variant_stock::ServiceVariantInventory)),
    )))
}

/// The providers this build knows, configured from the environment. This is
/// the one place a provider registers itself: the service and the worker both
/// call it, so they always serve the same set.
pub fn providers_from_env() -> anyhow::Result<ProviderRegistry> {
    Ok(ProviderRegistry::new().with(std::sync::Arc::new(
        providers::woocommerce::WooCommerceProvider::from_env(),
    )))
}

/// A strictly valid version 8 UUID derived from a seed: the same seed gives
/// the same id, so a replayed step presents the same idempotency key.
pub fn deterministic_uuid(seed: &str) -> Uuid {
    let digest = Sha256::digest(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}

#[derive(Clone, Copy)]
enum SealedTable {
    Credential,
    AppCredential,
    RecipientKey,
}

struct SealedRow {
    table: SealedTable,
    connection_id: Uuid,
    aad: Vec<u8>,
    sealed: Vec<u8>,
}

/// Every sealed value the connector family holds: store credentials, merchant
/// app credentials and recipient secrets.
async fn sealed_rows(pool: &PgPool) -> Result<Vec<SealedRow>, sqlx::Error> {
    let credentials: Vec<(Uuid, String, String, String, Vec<u8>)> = sqlx::query_as(
        "SELECT c.connection_id, k.seller_pubky, k.provider, k.shop, c.sealed \
         FROM connector_credentials c JOIN connector_connections k ON k.id = c.connection_id \
         ORDER BY c.connection_id",
    )
    .fetch_all(pool)
    .await?;
    let recipients: Vec<(Uuid, String, String, String, String, Vec<u8>)> = sqlx::query_as(
        "SELECT r.connection_id, k.seller_pubky, k.provider, k.shop, r.key_id, r.sealed_secret \
         FROM connector_recipient_keys r JOIN connector_connections k ON k.id = r.connection_id \
         ORDER BY r.connection_id",
    )
    .fetch_all(pool)
    .await?;
    let apps: Vec<(Uuid, String, String, String, Vec<u8>)> = sqlx::query_as(
        "SELECT a.connection_id, k.seller_pubky, k.provider, k.shop, a.sealed \
         FROM connector_app_credentials a JOIN connector_connections k ON k.id = a.connection_id \
         ORDER BY a.connection_id",
    )
    .fetch_all(pool)
    .await?;
    let mut rows: Vec<SealedRow> = credentials
        .into_iter()
        .map(
            |(connection_id, seller, provider, shop, sealed)| SealedRow {
                table: SealedTable::Credential,
                connection_id,
                aad: keys::credential_aad(&seller, &provider, &shop),
                sealed,
            },
        )
        .collect();
    rows.extend(
        apps.into_iter().map(
            |(connection_id, seller, provider, shop, sealed)| SealedRow {
                table: SealedTable::AppCredential,
                connection_id,
                aad: keys::app_credential_aad(&seller, &provider, &shop, &connection_id),
                sealed,
            },
        ),
    );
    rows.extend(recipients.into_iter().map(
        |(connection_id, seller, provider, shop, key_id, sealed)| SealedRow {
            table: SealedTable::RecipientKey,
            connection_id,
            aad: keys::recipient_key_aad(&seller, &provider, &shop, &key_id),
            sealed,
        },
    ));
    Ok(rows)
}

/// The boot check, run after migrations: every stored credential and
/// recipient key must open under the configured keys (current or previous),
/// and sealed values may not exist without a key. They are few, so every
/// row is checked. Returns how many sealed values were checked.
pub async fn assert_credentials_coherent(
    pool: &PgPool,
    keys: Option<&ConnectorKeys>,
) -> anyhow::Result<u64> {
    let rows = sealed_rows(pool).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let Some(keys) = keys else {
        anyhow::bail!(
            "sealed connector credentials exist but {} is not configured",
            keys::ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY
        );
    };
    for row in &rows {
        if keys.open(&row.aad, &row.sealed).is_err() {
            anyhow::bail!(
                "sealed connector credentials do not open under the configured {} \
                 (current or previous)",
                keys::ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY
            );
        }
    }
    Ok(rows.len() as u64)
}

/// Progress of one re-seal pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResealProgress {
    pub resealed: u64,
    pub skipped_changed: u64,
    pub remaining_under_previous: u64,
}

/// Moves every sealed value still under the previous key to the current key.
/// A write lands only if the row still holds the ciphertext the pass read, so
/// a value replaced meanwhile is never overwritten with an older one. A value
/// that opens under neither key fails the pass after the rest were rotated.
pub async fn reseal_previous_key(
    pool: &PgPool,
    keys: &ConnectorKeys,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<ResealProgress> {
    let mut progress = ResealProgress::default();
    if !keys.has_previous() {
        return Ok(progress);
    }
    let mut unopenable = 0u64;
    for row in sealed_rows(pool).await? {
        if keys.opens_under_current(&row.aad, &row.sealed) {
            continue;
        }
        let Ok(mut plaintext) = keys.open_under_previous(&row.aad, &row.sealed) else {
            unopenable += 1;
            tracing::error!(
                connection_id = %row.connection_id,
                "connector sealed value opens under neither key; continuing"
            );
            continue;
        };
        let resealed = keys.seal(&row.aad, &plaintext);
        plaintext.fill(0);
        let statement = match row.table {
            SealedTable::Credential => {
                "UPDATE connector_credentials SET sealed = $3, updated_at = $4 \
                 WHERE connection_id = $1 AND sealed = $2"
            }
            SealedTable::AppCredential => {
                "UPDATE connector_app_credentials SET sealed = $3, updated_at = $4 \
                 WHERE connection_id = $1 AND sealed = $2"
            }
            SealedTable::RecipientKey => {
                "UPDATE connector_recipient_keys SET sealed_secret = $3, updated_at = $4 \
                 WHERE connection_id = $1 AND sealed_secret = $2"
            }
        };
        let written = sqlx::query(statement)
            .bind(row.connection_id)
            .bind(&row.sealed)
            .bind(&resealed)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();
        if written == 1 {
            progress.resealed += 1;
        } else {
            progress.skipped_changed += 1;
        }
    }
    if unopenable > 0 {
        anyhow::bail!(
            "{unopenable} sealed connector value(s) open under neither the current nor \
             the previous key (their connection ids were logged); the rest of the pass completed"
        );
    }
    progress.remaining_under_previous = sealed_rows(pool)
        .await?
        .iter()
        .filter(|row| !keys.opens_under_current(&row.aad, &row.sealed))
        .count() as u64;
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_uuid_is_a_valid_v8_uuid_and_stable() {
        let first = deterministic_uuid("seed");
        assert_eq!(first, deterministic_uuid("seed"));
        assert_ne!(first, deterministic_uuid("other"));
        assert_eq!(first.get_version_num(), 8);
        let text = first.to_string();
        assert!(matches!(text.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
    }
}
