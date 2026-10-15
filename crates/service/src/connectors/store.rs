//! Connections, sealed credentials and recipient keys, a merchant's own app
//! credentials, mappings, product selection and seller alerts.
//!
//! Every read that returns a connection to a caller filters by the owning
//! seller in SQL, so another seller's connection id behaves exactly like an
//! unknown id. The credential columns are read in two places
//! ([`open_connection`], [`open_app`]) and their plaintext only ever reaches a
//! provider call.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

use super::config::MAX_CONNECTIONS_PER_SELLER;
use super::keys::{app_credential_aad, credential_aad, recipient_key_aad, ConnectorKeys};
use super::provider::{AppContext, AppKind, ConnectionContext, CredentialSecret, ProviderKind};
use super::recipient::NewRecipientKey;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ConnectionRow {
    pub id: Uuid,
    pub seller_pubky: String,
    pub provider: String,
    pub shop: String,
    pub app_kind: String,
    pub variant_mode: String,
    pub status: String,
    pub scopes: Vec<String>,
    pub missing_scopes: Vec<String>,
    pub connect_state_hash: Option<Vec<u8>>,
    pub state_expires_at: Option<DateTime<Utc>>,
    pub connect_expires_at: Option<DateTime<Utc>>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const CONNECTION_COLUMNS: &str = "id, seller_pubky, provider, shop, app_kind, variant_mode, status, \
     scopes, missing_scopes, connect_state_hash, state_expires_at, connect_expires_at, connected_at, \
     last_error, last_error_at, created_at, updated_at";

/// The states in which a connection holds an access credential for the store.
pub const LIVE_STATES: &str = "'connected', 'needs_attention', 'error'";

impl ConnectionRow {
    pub fn provider_kind(&self) -> Option<ProviderKind> {
        ProviderKind::parse(&self.provider)
    }

    pub fn app_kind(&self) -> AppKind {
        AppKind::parse(&self.app_kind).unwrap_or(AppKind::None)
    }

    pub fn is_split(&self) -> bool {
        self.variant_mode == "split"
    }

    pub fn is_live(&self) -> bool {
        matches!(
            self.status.as_str(),
            "connected" | "needs_attention" | "error"
        )
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

/// Creates the draft connection for `(seller, provider, shop)`, or returns
/// the seller's existing one so the URLs already configured at the store keep
/// working. An unexpired draft keeps whatever was saved on it and only has its
/// expiry (and, for WooCommerce, its nonce) renewed. A connection the seller
/// disconnected, that expired, or whose credential the store rejected starts
/// a fresh draft on the same id; its store access secret is deleted, and its
/// app credentials survive only when the pair is still usable. A connected
/// store is refused. Expired drafts of the seller are deleted first, so they
/// never count toward the limit.
#[allow(clippy::too_many_arguments)]
pub async fn begin_draft(
    pool: &PgPool,
    now: DateTime<Utc>,
    seller_pubky: &str,
    provider: ProviderKind,
    shop: &str,
    app_kind: AppKind,
    state_hash: Option<&[u8]>,
    expires_at: DateTime<Utc>,
) -> Result<ConnectionRow, ConnectError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-seller|' || $1, 0))")
        .bind(seller_pubky)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "DELETE FROM connector_connections \
         WHERE seller_pubky = $1 AND status = 'draft' AND connect_expires_at <= $2",
    )
    .bind(seller_pubky)
    .bind(now)
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
    if matches!(&existing, Some((_, status)) if matches!(status.as_str(), "connected" | "needs_attention"))
    {
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
        Some((id, status)) => {
            if status != "draft" {
                sqlx::query("DELETE FROM connector_credentials WHERE connection_id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM connector_recipient_keys WHERE connection_id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(
                    "UPDATE connector_app_credentials SET retain_until = NULL WHERE connection_id = $1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query_as(&format!(
                "UPDATE connector_connections SET status = 'draft', app_kind = $5, \
                 connect_state_hash = $2, state_expires_at = CASE WHEN $2::bytea IS NULL THEN NULL ELSE $3 END, \
                 connect_expires_at = $3, connected_at = NULL, last_error = NULL, \
                 last_error_at = NULL, scopes = '{{}}', missing_scopes = '{{}}', updated_at = $4 \
                 WHERE id = $1 RETURNING {CONNECTION_COLUMNS}"
            ))
            .bind(id)
            .bind(state_hash)
            .bind(expires_at)
            .bind(now)
            .bind(app_kind.as_str())
            .fetch_one(&mut *tx)
            .await?
        }
        None => {
            sqlx::query_as(&format!(
                "INSERT INTO connector_connections \
                 (id, seller_pubky, provider, shop, app_kind, status, connect_state_hash, \
                  state_expires_at, connect_expires_at, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, 'draft', $6, \
                         CASE WHEN $6::bytea IS NULL THEN NULL ELSE $7 END, $7, $8, $8) \
                 RETURNING {CONNECTION_COLUMNS}"
            ))
            .bind(Uuid::new_v4())
            .bind(seller_pubky)
            .bind(provider.as_str())
            .bind(shop)
            .bind(app_kind.as_str())
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

/// The connection a callback's state value names, if the state has not
/// expired. The state is single use: [`complete_connect`] clears it.
pub async fn pending_by_state(
    pool: &PgPool,
    provider: ProviderKind,
    state: &str,
    now: DateTime<Utc>,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE connect_state_hash = $1 AND provider = $2 \
           AND status IN ('draft', 'needs_attention', 'error') AND state_expires_at > $3"
    ))
    .bind(hash_state(state))
    .bind(provider.as_str())
    .bind(now)
    .fetch_optional(pool)
    .await
}

/// A verified launch replaces the connection's single-use state with the one
/// sent to the store. Only a live draft, or a connection that needs another
/// install, accepts one.
pub async fn bind_launch(
    pool: &PgPool,
    id: Uuid,
    state_hash: &[u8],
    state_expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "UPDATE connector_connections SET connect_state_hash = $2, state_expires_at = $3, \
         updated_at = $4 \
         WHERE id = $1 AND ((status = 'draft' AND connect_expires_at > $4) \
                            OR status IN ('needs_attention', 'error')) \
         RETURNING {CONNECTION_COLUMNS}"
    ))
    .bind(id)
    .bind(state_hash)
    .bind(state_expires_at)
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

/// Seals the store credential, stores a new recipient key and marks the
/// connection `connected` (or `needs_attention` with `missing_scopes`), in one
/// transaction that also consumes the state value.
#[allow(clippy::too_many_arguments)]
pub async fn complete_connect(
    pool: &PgPool,
    keys: &ConnectorKeys,
    now: DateTime<Utc>,
    pending: &ConnectionRow,
    secret: &CredentialSecret,
    scopes: &[String],
    missing_scopes: &[String],
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
    let status = if missing_scopes.is_empty() {
        "connected"
    } else {
        "needs_attention"
    };
    let mut tx = pool.begin().await?;
    let claimed = sqlx::query_as::<_, ConnectionRow>(&format!(
        "UPDATE connector_connections SET status = $5, connect_state_hash = NULL, \
         state_expires_at = NULL, connect_expires_at = NULL, scopes = $3, missing_scopes = $6, \
         connected_at = $4, last_error = NULL, last_error_at = NULL, updated_at = $4 \
         WHERE id = $1 AND status IN ('draft', 'needs_attention', 'error') \
           AND connect_state_hash = $2 AND (status <> 'draft' OR connect_expires_at > $4) \
         RETURNING {CONNECTION_COLUMNS}"
    ))
    .bind(pending.id)
    .bind(pending.connect_state_hash.as_deref())
    .bind(scopes)
    .bind(now)
    .bind(status)
    .bind(missing_scopes)
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

/// The install completed but something still needs the seller (a webhook
/// subscription that could not be registered).
pub async fn set_attention(
    pool: &PgPool,
    id: Uuid,
    message: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE connector_connections SET status = 'needs_attention', last_error = $2, \
         last_error_at = $3, updated_at = $3 WHERE id = $1 AND status IN ('connected', 'needs_attention')",
    )
    .bind(id)
    .bind(truncate(message, 500))
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

// === A merchant's own app credentials ========================================

#[derive(Debug)]
pub enum SaveAppError {
    /// The connection is not one that takes a merchant's app credentials, or
    /// it is gone, expired or disconnected.
    NotAvailable,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for SaveAppError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Seals the merchant's client id and secret for this connection, replacing
/// any pair already saved. Allowed on an unexpired draft and on a live
/// connection ("replace credentials").
pub async fn save_app_credentials(
    pool: &PgPool,
    keys: &ConnectorKeys,
    now: DateTime<Utc>,
    connection: &ConnectionRow,
    client_id: &str,
    client_secret: &str,
) -> Result<(), SaveAppError> {
    let plaintext = serde_json::to_vec(&json!({
        "client_id": client_id,
        "client_secret": client_secret,
    }))
    .expect("app credentials serialize");
    let aad = app_credential_aad(
        &connection.seller_pubky,
        &connection.provider,
        &connection.shop,
        &connection.id,
    );
    let sealed = keys.seal(&aad, &plaintext);
    let mut tx = pool.begin().await?;
    let usable: Option<(String, String)> = sqlx::query_as(
        "SELECT status, app_kind FROM connector_connections \
         WHERE id = $1 AND seller_pubky = $2 \
           AND (status <> 'draft' OR connect_expires_at > $3) FOR UPDATE",
    )
    .bind(connection.id)
    .bind(&connection.seller_pubky)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((status, app_kind)) = usable else {
        return Err(SaveAppError::NotAvailable);
    };
    if status == "disconnected" || app_kind != AppKind::Custom.as_str() {
        return Err(SaveAppError::NotAvailable);
    }
    sqlx::query(
        "INSERT INTO connector_app_credentials (connection_id, sealed, retain_until, created_at, updated_at) \
         VALUES ($1, $2, NULL, $3, $3) \
         ON CONFLICT (connection_id) DO UPDATE SET sealed = EXCLUDED.sealed, retain_until = NULL, \
             updated_at = EXCLUDED.updated_at",
    )
    .bind(connection.id)
    .bind(&sealed)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn has_app_credentials(pool: &PgPool, connection_id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM connector_app_credentials WHERE connection_id = $1)",
    )
    .bind(connection_id)
    .fetch_one(pool)
    .await
}

/// Opens the connection's own app credentials, whatever the connection's
/// state: a webhook that follows an uninstall still needs them. `None` when
/// none are saved (or they were deleted).
pub async fn open_app(
    pool: &PgPool,
    keys: &ConnectorKeys,
    row: &ConnectionRow,
) -> Result<Option<AppContext>, OpenError> {
    let sealed: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT sealed FROM connector_app_credentials WHERE connection_id = $1")
            .bind(row.id)
            .fetch_optional(pool)
            .await?;
    let Some(sealed) = sealed else {
        return Ok(None);
    };
    let aad = app_credential_aad(&row.seller_pubky, &row.provider, &row.shop, &row.id);
    let mut opened = keys
        .open(&aad, &sealed)
        .map_err(|_| OpenError::Unreadable)?;
    #[derive(serde::Deserialize)]
    struct Pair {
        client_id: String,
        client_secret: String,
    }
    let parsed = serde_json::from_slice::<Pair>(&opened);
    opened.fill(0);
    let pair = parsed.map_err(|_| OpenError::Unreadable)?;
    Ok(Some(AppContext {
        connection_id: row.id,
        client_id: pair.client_id,
        client_secret: CredentialSecret::new(pair.client_secret.into_bytes()),
    }))
}

pub async fn delete_app_credentials(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM connector_app_credentials WHERE connection_id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PurgeReport {
    pub drafts: u64,
    pub app_secrets: u64,
}

/// Deletes drafts that expired (with everything saved on them) and app
/// credentials whose retention after a disconnect or uninstall ended.
pub async fn purge_expired(pool: &PgPool, now: DateTime<Utc>) -> Result<PurgeReport, sqlx::Error> {
    let drafts = sqlx::query(
        "DELETE FROM connector_connections WHERE status = 'draft' AND connect_expires_at <= $1",
    )
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    let app_secrets = sqlx::query(
        "DELETE FROM connector_app_credentials WHERE retain_until IS NOT NULL AND retain_until <= $1",
    )
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(PurgeReport {
        drafts,
        app_secrets,
    })
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

/// The seller's connections that are not gone: drafts that have not expired
/// and every live one.
pub async fn list_for_seller(
    pool: &PgPool,
    seller_pubky: &str,
    now: DateTime<Utc>,
) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONNECTION_COLUMNS} FROM connector_connections \
         WHERE seller_pubky = $1 AND status <> 'disconnected' \
           AND (status <> 'draft' OR connect_expires_at > $2) ORDER BY created_at, id"
    ))
    .bind(seller_pubky)
    .bind(now)
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
         WHERE provider = $1 AND shop = $2 AND status IN ({LIVE_STATES})"
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
         WHERE c.status IN ('connected', 'needs_attention') \
           AND EXISTS (SELECT 1 FROM connector_variant_map m WHERE m.connection_id = c.id) \
         ORDER BY c.id"
    ))
    .fetch_all(pool)
    .await
}

#[derive(Debug)]
pub enum OpenError {
    /// The connection has no credential (draft or disconnected).
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

/// Opens the connection's store credential for one provider call. The app
/// (a merchant's own client id and secret) is added by
/// [`super::ConnectorRuntime::open`].
pub async fn open_connection(
    pool: &PgPool,
    keys: &ConnectorKeys,
    row: &ConnectionRow,
) -> Result<ConnectionContext, OpenError> {
    if !row.is_live() {
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

/// Deletes the store credential, the recipient key, the mappings and the
/// product selection and marks the connection disconnected. Idempotent.
///
/// The merchant's own app credentials are deleted at once when the
/// connection never completed an install (nothing can arrive for it), and
/// otherwise kept until `retain_app_secret_until`: Shopify sends its privacy
/// webhooks to the connection's URL after an uninstall and they can only be
/// verified with that secret. `shop/redact` ends the retention early.
pub async fn disconnect(
    pool: &PgPool,
    id: Uuid,
    now: DateTime<Utc>,
    retain_app_secret_until: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
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
    let installed: Option<bool> = sqlx::query_scalar(
        "SELECT connected_at IS NOT NULL FROM connector_connections WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    if installed == Some(true) {
        sqlx::query(
            "UPDATE connector_app_credentials SET \
             retain_until = COALESCE(retain_until, $2) WHERE connection_id = $1",
        )
        .bind(id)
        .bind(retain_app_secret_until)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query("DELETE FROM connector_app_credentials WHERE connection_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    let changed = sqlx::query(
        "UPDATE connector_connections SET status = 'disconnected', connect_state_hash = NULL, \
         state_expires_at = NULL, connect_expires_at = NULL, updated_at = $2 \
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
    sqlx::query(&format!(
        "UPDATE connector_connections SET status = 'error', last_error = $2, \
         last_error_at = $3, updated_at = $3 WHERE id = $1 AND status IN ({LIVE_STATES})"
    ))
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

#[derive(Debug)]
pub enum VariantModeError {
    /// Switching to `split` while a listing is mapped to more than one store
    /// variant.
    SharedListing(String),
    Database(sqlx::Error),
}

impl From<sqlx::Error> for VariantModeError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Sets how the connection's store variants relate to Pubky listings
/// (`native` or `split`). `split` is refused while a listing is mapped to
/// more than one store variant.
pub async fn set_variant_mode(
    pool: &PgPool,
    connection: &ConnectionRow,
    split: bool,
    now: DateTime<Utc>,
) -> Result<ConnectionRow, VariantModeError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-map|' || $1::text, 0))")
        .bind(connection.id)
        .execute(&mut *tx)
        .await?;
    if split {
        let shared: Option<String> = sqlx::query_scalar(
            "SELECT listing_id FROM connector_variant_map WHERE connection_id = $1 \
             GROUP BY listing_id HAVING COUNT(*) > 1 LIMIT 1",
        )
        .bind(connection.id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(listing_id) = shared {
            return Err(VariantModeError::SharedListing(listing_id));
        }
    }
    let row: ConnectionRow = sqlx::query_as(&format!(
        "UPDATE connector_connections SET variant_mode = $2, updated_at = $3 \
         WHERE id = $1 AND seller_pubky = $4 RETURNING {CONNECTION_COLUMNS}"
    ))
    .bind(connection.id)
    .bind(if split { "split" } else { "native" })
    .bind(now)
    .bind(&connection.seller_pubky)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
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
    /// In `split` mode each store variant has its own listing, so a listing
    /// appears in the mapping once.
    SharedListing(String),
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
    let still_live: Option<(String, String)> = sqlx::query_as(
        "SELECT status, variant_mode FROM connector_connections \
         WHERE id = $1 AND seller_pubky = $2 FOR UPDATE",
    )
    .bind(connection.id)
    .bind(&connection.seller_pubky)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((status, variant_mode)) = still_live else {
        return Err(MappingError::Database(sqlx::Error::RowNotFound));
    };
    if !matches!(status.as_str(), "connected" | "needs_attention" | "error") {
        return Err(MappingError::Database(sqlx::Error::RowNotFound));
    }
    if variant_mode == "split" {
        let mut seen = std::collections::HashSet::new();
        if let Some(shared) = mappings
            .iter()
            .find(|mapping| !seen.insert(mapping.listing_id.as_str()))
        {
            return Err(MappingError::SharedListing(shared.listing_id.clone()));
        }
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
    if !matches!(
        status.as_deref(),
        Some("connected" | "needs_attention" | "error")
    ) {
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
    /// A refund already mirrored to the store was reversed or reduced in
    /// Pubky. Informational: the seller corrects the store; there is nothing
    /// to retry.
    RefundReversedAfterMirror,
    /// Switching to split variant mode unmapped listings that combined several
    /// store variants; the seller re-imports or ends them.
    ListingsUnmapped,
    /// A store stock change named a variant the service cannot address: the
    /// mapping is stale. Permanent until the seller re-maps.
    VariantMappingError,
    /// The store order's total is not what the buyer paid.
    PushTotalMismatch,
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
            AlertKind::RefundReversedAfterMirror => "refund_reversed_after_mirror",
            AlertKind::ListingsUnmapped => "listings_unmapped",
            AlertKind::VariantMappingError => "variant_mapping_error",
            AlertKind::PushTotalMismatch => "push_total_mismatch",
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
    /// The Pubky order's state now, to say which existing flow applies.
    pub order_state: Option<String>,
    pub order_fulfillment: Option<String>,
    pub refund_recorded: Option<bool>,
}

pub async fn list_alerts(
    pool: &PgPool,
    seller_pubky: &str,
    unresolved_only: bool,
    limit: i64,
) -> Result<Vec<AlertRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT a.id, a.connection_id, a.kind, a.pubky_order_id, a.detail, a.created_at, \
                a.resolved_at, o.state AS order_state, o.fulfillment AS order_fulfillment, \
                (o.external_refund IS NOT NULL) AS refund_recorded \
         FROM connector_alerts a LEFT JOIN orders o ON o.id = a.pubky_order_id \
         WHERE a.seller_pubky = $1 AND (NOT $2 OR a.resolved_at IS NULL) \
         ORDER BY a.created_at DESC, a.id LIMIT $3",
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
