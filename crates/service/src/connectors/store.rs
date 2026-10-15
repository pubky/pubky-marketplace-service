//! Connections, sealed credentials and recipient keys, mappings, product
//! selection and seller alerts.
//!
//! Every read that returns a connection to a caller filters by the owning
//! seller in SQL, so another seller's connection id behaves exactly like an
//! unknown id. The credential column is read in one place
//! ([`open_connection`]) and its plaintext only ever reaches a provider call.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

use super::config::MAX_CONNECTIONS_PER_SELLER;
use super::keys::{credential_aad, recipient_key_aad, ConnectorKeys};
use super::provider::{ConnectionContext, CredentialSecret, ProviderKind};
use super::recipient::NewRecipientKey;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ConnectionRow {
    pub id: Uuid,
    pub seller_pubky: String,
    pub provider: String,
    pub shop: String,
    pub app_key: Option<String>,
    pub status: String,
    pub scopes: Vec<String>,
    pub connect_state_hash: Option<Vec<u8>>,
    pub connect_expires_at: Option<DateTime<Utc>>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const CONNECTION_COLUMNS: &str = "id, seller_pubky, provider, shop, app_key, status, scopes, \
     connect_state_hash, connect_expires_at, connected_at, last_error, last_error_at, \
     created_at, updated_at";

impl ConnectionRow {
    pub fn provider_kind(&self) -> Option<ProviderKind> {
        ProviderKind::parse(&self.provider)
    }
}

pub fn hash_state(state: &str) -> Vec<u8> {
    Sha256::digest(state.as_bytes()).to_vec()
}

#[derive(Debug)]
pub enum ConnectError {
    AlreadyConnected,
    LimitReached,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for ConnectError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Starts (or restarts) a pending connect for `(seller, provider, shop)`. A
/// shop the seller disconnected, left pending, or whose credential the store
/// rejected is reused; a live connection is refused.
#[allow(clippy::too_many_arguments)]
pub async fn begin_connect(
    pool: &PgPool,
    now: DateTime<Utc>,
    seller_pubky: &str,
    provider: ProviderKind,
    shop: &str,
    app_key: Option<&str>,
    state_hash: &[u8],
    expires_at: DateTime<Utc>,
) -> Result<ConnectionRow, ConnectError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-seller|' || $1, 0))")
        .bind(seller_pubky)
        .execute(&mut *tx)
        .await?;
    let existing: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT id, status FROM connector_connections \
         WHERE seller_pubky = $1 AND provider = $2 AND shop = $3 FOR UPDATE",
    )
    .bind(seller_pubky)
    .bind(provider.as_str())
    .bind(shop)
    .fetch_optional(&mut *tx)
    .await?;
    if matches!(&existing, Some((_, status)) if status == "connected") {
        return Err(ConnectError::AlreadyConnected);
    }
    if existing.is_none() {
        let held: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM connector_connections \
             WHERE seller_pubky = $1 AND status <> 'disconnected'",
        )
        .bind(seller_pubky)
        .fetch_one(&mut *tx)
        .await?;
        if held >= MAX_CONNECTIONS_PER_SELLER {
            return Err(ConnectError::LimitReached);
        }
    }
    let row: ConnectionRow = match existing {
        Some((id, _)) => {
            sqlx::query("DELETE FROM connector_credentials WHERE connection_id = $1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM connector_recipient_keys WHERE connection_id = $1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query_as(&format!(
                "UPDATE connector_connections SET status = 'pending', app_key = $5, \
                 connect_state_hash = $2, connect_expires_at = $3, connected_at = NULL, \
                 last_error = NULL, last_error_at = NULL, scopes = '{{}}', updated_at = $4 \
                 WHERE id = $1 RETURNING {CONNECTION_COLUMNS}"
            ))
            .bind(id)
            .bind(state_hash)
            .bind(expires_at)
            .bind(now)
            .bind(app_key)
            .fetch_one(&mut *tx)
            .await?
        }
        None => {
            sqlx::query_as(&format!(
                "INSERT INTO connector_connections \
                 (id, seller_pubky, provider, shop, app_key, status, connect_state_hash, \
                  connect_expires_at, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $8, $8) \
                 RETURNING {CONNECTION_COLUMNS}"
            ))
            .bind(Uuid::new_v4())
            .bind(seller_pubky)
            .bind(provider.as_str())
            .bind(shop)
            .bind(app_key)
            .bind(state_hash)
            .bind(expires_at)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    Ok(row)
}

/// The pending connect a callback's state value names, if it has not
/// expired. The state is single use: [`complete_connect`] clears it.
pub async fn pending_by_state(
    pool: &PgPool,
    provider: ProviderKind,
    state: &str,
    now: DateTime<Utc>,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE connect_state_hash = $1 AND provider = $2 AND status = 'pending' \
           AND connect_expires_at > $3"
    ))
    .bind(hash_state(state))
    .bind(provider.as_str())
    .bind(now)
    .fetch_optional(pool)
    .await
}

/// Unexpired pending connects for a store, newest first. A store's launch
/// matches exactly one; more than one is ambiguous and refused.
pub async fn pending_for_shop(
    pool: &PgPool,
    provider: ProviderKind,
    shop: &str,
    now: DateTime<Utc>,
) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE provider = $1 AND shop = $2 AND status = 'pending' AND connect_expires_at > $3 \
         ORDER BY created_at DESC, id"
    ))
    .bind(provider.as_str())
    .bind(shop)
    .bind(now)
    .fetch_all(pool)
    .await
}

/// A verified launch binds the pending connect to the app that was
/// installed and replaces its single-use state with the one sent to the
/// store. The expiry stays what it was when the seller asked.
pub async fn bind_launch(
    pool: &PgPool,
    id: Uuid,
    app_key: &str,
    state_hash: &[u8],
    now: DateTime<Utc>,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "UPDATE connector_connections SET app_key = $2, connect_state_hash = $3, updated_at = $4 \
         WHERE id = $1 AND status = 'pending' AND connect_expires_at > $4 \
         RETURNING {CONNECTION_COLUMNS}"
    ))
    .bind(id)
    .bind(app_key)
    .bind(state_hash)
    .bind(now)
    .fetch_optional(pool)
    .await
}

#[derive(Debug)]
pub enum CompleteError {
    /// The state was already used or the connection changed meanwhile.
    Stale,
    /// Another seller holds a live connection to this store.
    ShopInUse,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for CompleteError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Seals the credential, stores a new recipient key and marks the connection
/// connected, in one transaction that also consumes the state value.
pub async fn complete_connect(
    pool: &PgPool,
    keys: &ConnectorKeys,
    now: DateTime<Utc>,
    pending: &ConnectionRow,
    secret: &CredentialSecret,
    scopes: &[String],
    recipient: &NewRecipientKey,
) -> Result<ConnectionRow, CompleteError> {
    let aad = credential_aad(&pending.seller_pubky, &pending.provider, &pending.shop);
    let sealed = keys.seal(&aad, secret.expose());
    let key_aad = recipient_key_aad(
        &pending.seller_pubky,
        &pending.provider,
        &pending.shop,
        &recipient.key_id,
    );
    let sealed_secret = keys.seal(&key_aad, recipient.secret());
    let mut tx = pool.begin().await?;
    let claimed = sqlx::query_as::<_, ConnectionRow>(&format!(
        "UPDATE connector_connections SET status = 'connected', connect_state_hash = NULL, \
         connect_expires_at = NULL, scopes = $3, connected_at = $4, last_error = NULL, \
         last_error_at = NULL, updated_at = $4 \
         WHERE id = $1 AND status = 'pending' AND connect_state_hash = $2 \
         RETURNING {CONNECTION_COLUMNS}"
    ))
    .bind(pending.id)
    .bind(pending.connect_state_hash.as_deref())
    .bind(scopes)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await;
    let claimed = match claimed {
        Ok(claimed) => claimed,
        Err(sqlx::Error::Database(database)) if database.is_unique_violation() => {
            return Err(CompleteError::ShopInUse);
        }
        Err(error) => return Err(error.into()),
    };
    let Some(connected) = claimed else {
        return Err(CompleteError::Stale);
    };
    sqlx::query(
        "INSERT INTO connector_credentials (connection_id, sealed, created_at, updated_at) \
         VALUES ($1, $2, $3, $3) \
         ON CONFLICT (connection_id) DO UPDATE SET sealed = EXCLUDED.sealed, \
             updated_at = EXCLUDED.updated_at",
    )
    .bind(connected.id)
    .bind(&sealed)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO connector_recipient_keys \
         (connection_id, key_id, public_key, sealed_secret, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $5) \
         ON CONFLICT (connection_id) DO UPDATE SET key_id = EXCLUDED.key_id, \
             public_key = EXCLUDED.public_key, sealed_secret = EXCLUDED.sealed_secret, \
             updated_at = EXCLUDED.updated_at",
    )
    .bind(connected.id)
    .bind(&recipient.key_id)
    .bind(&recipient.public_key[..])
    .bind(&sealed_secret)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(connected)
}

pub async fn get_owned(
    pool: &PgPool,
    seller_pubky: &str,
    id: Uuid,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE id = $1 AND seller_pubky = $2"
    ))
    .bind(id)
    .bind(seller_pubky)
    .fetch_optional(pool)
    .await
}

pub async fn get(pool: &PgPool, id: Uuid) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn list_for_seller(
    pool: &PgPool,
    seller_pubky: &str,
) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE seller_pubky = $1 AND status <> 'disconnected' ORDER BY created_at, id"
    ))
    .bind(seller_pubky)
    .fetch_all(pool)
    .await
}

/// The live connection a webhook for `(provider, shop)` belongs to.
pub async fn live_for_shop(
    pool: &PgPool,
    provider: ProviderKind,
    shop: &str,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE provider = $1 AND shop = $2 AND status IN ('connected', 'error')"
    ))
    .bind(provider.as_str())
    .bind(shop)
    .fetch_optional(pool)
    .await
}

/// The newest connection of any state for a store: the home of a
/// compliance webhook that arrives after the store disconnected.
pub async fn latest_for_shop(
    pool: &PgPool,
    provider: ProviderKind,
    shop: &str,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE provider = $1 AND shop = $2 AND status <> 'pending' \
         ORDER BY updated_at DESC, id LIMIT 1"
    ))
    .bind(provider.as_str())
    .bind(shop)
    .fetch_optional(pool)
    .await
}

/// Connected stores whose stock the poller should read.
pub async fn connected_with_mappings(pool: &PgPool) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections c \
         WHERE c.status = 'connected' \
           AND EXISTS (SELECT 1 FROM connector_variant_map m WHERE m.connection_id = c.id) \
         ORDER BY c.id"
    ))
    .fetch_all(pool)
    .await
}

#[derive(Debug)]
pub enum OpenError {
    /// The connection has no credential (pending or disconnected).
    Unavailable,
    /// The stored credential opens under neither configured key.
    Unreadable,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for OpenError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Opens the connection's credential for one provider call. The provider
/// app is added by [`super::ConnectorRuntime::open`].
pub async fn open_connection(
    pool: &PgPool,
    keys: &ConnectorKeys,
    row: &ConnectionRow,
) -> Result<ConnectionContext, OpenError> {
    if !matches!(row.status.as_str(), "connected" | "error") {
        return Err(OpenError::Unavailable);
    }
    let sealed: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT sealed FROM connector_credentials WHERE connection_id = $1")
            .bind(row.id)
            .fetch_optional(pool)
            .await?;
    let Some(sealed) = sealed else {
        return Err(OpenError::Unavailable);
    };
    let aad = credential_aad(&row.seller_pubky, &row.provider, &row.shop);
    let opened = keys
        .open(&aad, &sealed)
        .map_err(|_| OpenError::Unreadable)?;
    Ok(ConnectionContext {
        connection_id: row.id,
        seller_pubky: row.seller_pubky.clone(),
        shop: row.shop.clone(),
        scopes: row.scopes.clone(),
        credentials: CredentialSecret::new(opened),
        app: None,
    })
}

/// The connection's recipient key: its id and public half, for the public
/// connector status and for checkout to match an envelope against.
pub async fn recipient_public(
    pool: &PgPool,
    connection_id: Uuid,
) -> Result<Option<(String, Vec<u8>)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT key_id, public_key FROM connector_recipient_keys WHERE connection_id = $1",
    )
    .bind(connection_id)
    .fetch_optional(pool)
    .await
}

/// Opens the recipient secret of the connection whose key id the order's
/// envelope names. Only the worker, while pushing an opted-in order, calls
/// this.
pub async fn open_recipient_secret(
    pool: &PgPool,
    keys: &ConnectorKeys,
    row: &ConnectionRow,
    key_id: &str,
) -> Result<Option<[u8; 32]>, OpenError> {
    let sealed: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT sealed_secret FROM connector_recipient_keys \
         WHERE connection_id = $1 AND key_id = $2",
    )
    .bind(row.id)
    .bind(key_id)
    .fetch_optional(pool)
    .await?;
    let Some(sealed) = sealed else {
        return Ok(None);
    };
    let aad = recipient_key_aad(&row.seller_pubky, &row.provider, &row.shop, key_id);
    let mut opened = keys
        .open(&aad, &sealed)
        .map_err(|_| OpenError::Unreadable)?;
    let secret = <[u8; 32]>::try_from(opened.as_slice()).map_err(|_| OpenError::Unreadable);
    opened.fill(0);
    secret.map(Some)
}

/// One store a buyer may share an address with, for the public status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicStore {
    pub provider: String,
    pub shop: String,
    pub key_id: String,
    pub public_key: Vec<u8>,
}

/// The seller's connected stores that can receive an opted-in address.
pub async fn public_stores(
    pool: &PgPool,
    seller_pubky: &str,
) -> Result<Vec<PublicStore>, sqlx::Error> {
    let rows: Vec<(String, String, String, Vec<u8>)> = sqlx::query_as(
        "SELECT c.provider, c.shop, k.key_id, k.public_key \
         FROM connector_connections c JOIN connector_recipient_keys k ON k.connection_id = c.id \
         WHERE c.seller_pubky = $1 AND c.status = 'connected' ORDER BY c.created_at, c.id",
    )
    .bind(seller_pubky)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(provider, shop, key_id, public_key)| PublicStore {
            provider,
            shop,
            key_id,
            public_key,
        })
        .collect())
}

/// Deletes the credential, the recipient key, the mappings and the product
/// selection and marks the connection disconnected. Idempotent.
pub async fn disconnect(pool: &PgPool, id: Uuid, now: DateTime<Utc>) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    for table in [
        "connector_credentials",
        "connector_recipient_keys",
        "connector_variant_map",
        "connector_product_selection",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE connection_id = $1"))
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    let changed = sqlx::query(
        "UPDATE connector_connections SET status = 'disconnected', connect_state_hash = NULL, \
         connect_expires_at = NULL, updated_at = $2 \
         WHERE id = $1 AND status <> 'disconnected'",
    )
    .bind(id)
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(changed > 0)
}

/// Records why a connection is unhealthy. A message is a provider error
/// code or a short fixed phrase, never provider response text.
pub async fn mark_error(
    pool: &PgPool,
    id: Uuid,
    message: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE connector_connections SET status = 'error', last_error = $2, \
         last_error_at = $3, updated_at = $3 WHERE id = $1 AND status IN ('connected', 'error')",
    )
    .bind(id)
    .bind(truncate(message, 500))
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn truncate(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

// === Mappings and product selection ==========================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct MappingRow {
    pub listing_id: String,
    pub variant_id: String,
    pub provider_product_id: String,
    pub provider_variant_id: String,
    pub sku: Option<String>,
}

pub async fn list_mappings(
    pool: &PgPool,
    connection_id: Uuid,
) -> Result<Vec<MappingRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT listing_id, variant_id, provider_product_id, provider_variant_id, sku \
         FROM connector_variant_map WHERE connection_id = $1 \
         ORDER BY listing_id, variant_id",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await
}

#[derive(Debug)]
pub enum MappingError {
    /// A listing id the seller does not own (or that is deleted).
    UnknownListing(String),
    /// A uniqueness violation: the pair is already mapped to something else.
    Conflict,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for MappingError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Replaces the connection's whole mapping set in one transaction. A product
/// that has a mapping is always selected, so the selection gains every mapped
/// product. The caller has already rejected duplicates inside the request;
/// the unique constraints still stand behind it for a concurrent writer.
pub async fn replace_mappings(
    pool: &PgPool,
    connection: &ConnectionRow,
    mappings: &[MappingRow],
    now: DateTime<Utc>,
) -> Result<(), MappingError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-map|' || $1::text, 0))")
        .bind(connection.id)
        .execute(&mut *tx)
        .await?;
    let still_live: Option<String> = sqlx::query_scalar(
        "SELECT status FROM connector_connections WHERE id = $1 AND seller_pubky = $2 FOR UPDATE",
    )
    .bind(connection.id)
    .bind(&connection.seller_pubky)
    .fetch_optional(&mut *tx)
    .await?;
    if !matches!(still_live.as_deref(), Some("connected" | "error")) {
        return Err(MappingError::Database(sqlx::Error::RowNotFound));
    }
    let listing_ids: Vec<String> = mappings
        .iter()
        .map(|mapping| mapping.listing_id.clone())
        .collect();
    let owned: Vec<String> = sqlx::query_scalar(
        "SELECT listing_id FROM listings \
         WHERE seller_pubky = $1 AND listing_id = ANY($2) AND deleted_at IS NULL",
    )
    .bind(&connection.seller_pubky)
    .bind(&listing_ids)
    .fetch_all(&mut *tx)
    .await?;
    if let Some(unknown) = listing_ids.iter().find(|id| !owned.contains(id)) {
        return Err(MappingError::UnknownListing(unknown.clone()));
    }
    sqlx::query("DELETE FROM connector_variant_map WHERE connection_id = $1")
        .bind(connection.id)
        .execute(&mut *tx)
        .await?;
    for mapping in mappings {
        let inserted = sqlx::query(
            "INSERT INTO connector_variant_map \
             (id, connection_id, listing_id, variant_id, provider_product_id, \
              provider_variant_id, sku, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(connection.id)
        .bind(&mapping.listing_id)
        .bind(&mapping.variant_id)
        .bind(&mapping.provider_product_id)
        .bind(&mapping.provider_variant_id)
        .bind(&mapping.sku)
        .bind(now)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(database)) if database.is_unique_violation() => {
                return Err(MappingError::Conflict);
            }
            Err(error) => return Err(error.into()),
        }
    }
    sqlx::query(
        "INSERT INTO connector_product_selection (connection_id, provider_product_id, selected_at) \
         SELECT DISTINCT $1, provider_product_id, $2 FROM connector_variant_map \
         WHERE connection_id = $1 ON CONFLICT DO NOTHING",
    )
    .bind(connection.id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn selected_products(
    pool: &PgPool,
    connection_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT provider_product_id FROM connector_product_selection \
         WHERE connection_id = $1 ORDER BY provider_product_id",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await
}

#[derive(Debug)]
pub enum SelectionError {
    /// A mapped product cannot be unselected until its mappings are removed.
    MappedProduct(String),
    Database(sqlx::Error),
}

impl From<sqlx::Error> for SelectionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Replaces the connection's selection of store products to publish. A
/// product that still has a mapping cannot be dropped from the selection.
pub async fn replace_selection(
    pool: &PgPool,
    connection: &ConnectionRow,
    product_ids: &[String],
    now: DateTime<Utc>,
) -> Result<(), SelectionError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-map|' || $1::text, 0))")
        .bind(connection.id)
        .execute(&mut *tx)
        .await?;
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM connector_connections WHERE id = $1 AND seller_pubky = $2 FOR UPDATE",
    )
    .bind(connection.id)
    .bind(&connection.seller_pubky)
    .fetch_optional(&mut *tx)
    .await?;
    if !matches!(status.as_deref(), Some("connected" | "error")) {
        return Err(SelectionError::Database(sqlx::Error::RowNotFound));
    }
    let blocked: Option<String> = sqlx::query_scalar(
        "SELECT provider_product_id FROM connector_variant_map \
         WHERE connection_id = $1 AND NOT (provider_product_id = ANY($2)) LIMIT 1",
    )
    .bind(connection.id)
    .bind(product_ids)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(product) = blocked {
        return Err(SelectionError::MappedProduct(product));
    }
    sqlx::query("DELETE FROM connector_product_selection WHERE connection_id = $1")
        .bind(connection.id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO connector_product_selection (connection_id, provider_product_id, selected_at) \
         SELECT $1, product, $3 FROM UNNEST($2::text[]) AS product",
    )
    .bind(connection.id)
    .bind(product_ids)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn mapping_for_provider_variant(
    pool: &PgPool,
    connection_id: Uuid,
    provider_product_id: &str,
    provider_variant_id: &str,
) -> Result<Option<MappingRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT listing_id, variant_id, provider_product_id, provider_variant_id, sku \
         FROM connector_variant_map \
         WHERE connection_id = $1 AND provider_product_id = $2 AND provider_variant_id = $3",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(provider_variant_id)
    .fetch_optional(pool)
    .await
}

pub async fn mapping_for_pubky_line(
    pool: &PgPool,
    connection_id: Uuid,
    listing_id: &str,
    variant_id: &str,
) -> Result<Option<MappingRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT listing_id, variant_id, provider_product_id, provider_variant_id, sku \
         FROM connector_variant_map \
         WHERE connection_id = $1 AND listing_id = $2 AND variant_id = $3",
    )
    .bind(connection_id)
    .bind(listing_id)
    .bind(variant_id)
    .fetch_optional(pool)
    .await
}

pub async fn mapping_count_for_listing(
    pool: &PgPool,
    connection_id: Uuid,
    listing_id: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM connector_variant_map WHERE connection_id = $1 AND listing_id = $2",
    )
    .bind(connection_id)
    .bind(listing_id)
    .fetch_one(pool)
    .await
}

// === Alerts ==================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertKind {
    PushQuarantined,
    ActionQuarantined,
    MerchantCancelled,
    MerchantRefunded,
    StoreUninstalled,
    CredentialsRejected,
    InboundFailed,
}

impl AlertKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertKind::PushQuarantined => "push_quarantined",
            AlertKind::ActionQuarantined => "action_quarantined",
            AlertKind::MerchantCancelled => "merchant_cancelled",
            AlertKind::MerchantRefunded => "merchant_refunded",
            AlertKind::StoreUninstalled => "store_uninstalled",
            AlertKind::CredentialsRejected => "credentials_rejected",
            AlertKind::InboundFailed => "inbound_failed",
        }
    }
}

/// Raises an alert once per `dedupe_key`; a repeat is a no-op.
#[allow(clippy::too_many_arguments)]
pub async fn raise_alert<'e, E>(
    executor: E,
    now: DateTime<Utc>,
    connection: &ConnectionRow,
    kind: AlertKind,
    pubky_order_id: Option<Uuid>,
    detail: &str,
    dedupe_key: &str,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    sqlx::query(
        "INSERT INTO connector_alerts \
         (id, seller_pubky, connection_id, kind, pubky_order_id, detail, dedupe_key, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(&connection.seller_pubky)
    .bind(connection.id)
    .bind(kind.as_str())
    .bind(pubky_order_id)
    .bind(truncate(detail, 500))
    .bind(truncate(dedupe_key, 300))
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AlertRow {
    pub id: Uuid,
    pub connection_id: Uuid,
    pub kind: String,
    pub pubky_order_id: Option<Uuid>,
    pub detail: String,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

pub async fn list_alerts(
    pool: &PgPool,
    seller_pubky: &str,
    unresolved_only: bool,
    limit: i64,
) -> Result<Vec<AlertRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, connection_id, kind, pubky_order_id, detail, created_at, resolved_at \
         FROM connector_alerts \
         WHERE seller_pubky = $1 AND (NOT $2 OR resolved_at IS NULL) \
         ORDER BY created_at DESC, id LIMIT $3",
    )
    .bind(seller_pubky)
    .bind(unresolved_only)
    .bind(limit)
    .fetch_all(pool)
    .await
}

pub async fn resolve_alert(
    pool: &PgPool,
    seller_pubky: &str,
    id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE connector_alerts SET resolved_at = COALESCE(resolved_at, $3) \
         WHERE id = $1 AND seller_pubky = $2",
    )
    .bind(id)
    .bind(seller_pubky)
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed > 0)
}
