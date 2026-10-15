//! `PUT /v1/connectors/{id}/woocommerce/keys` and `PUT /v1/connectors/{id}/credentials`
//! with `{consumer_key, consumer_secret}` (the Shop's credentials form): the
//! pasted-keys way to connect a WooCommerce store, for merchants whose site
//! blocks the one-click flow. Both end in [`connect_with_keys`].
//!
//! This file is compiled as a child module of `connectors::routes` (see the
//! `#[path]` declaration at the end of that file) so it shares the route
//! helpers (`respond`, `error`, `owned`, `connected_json`, ...) without
//! widening their visibility. It is the only WooCommerce-specific route.
//!
//! The seller is signed in and names their own pending connection; the key
//! pair travels over TLS, is verified against the store, and is sealed. It is
//! never logged and never returned.

use super::*;

use crate::connectors::provider::ConnectCompleted;
use crate::connectors::providers::woocommerce::creds::ApiKeys;

#[derive(Deserialize)]
pub struct KeysRequest {
    consumer_key: String,
    consumer_secret: String,
}

impl std::fmt::Debug for KeysRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KeysRequest(<redacted>)")
    }
}

pub async fn put_keys(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<KeysRequest>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    connect_with_keys(
        &state,
        runtime,
        row,
        request.consumer_key,
        request.consumer_secret,
    )
    .await
}

/// Connects the seller's own draft with a pasted key pair, or replaces the key
/// pair of a connection that is already connected ("replace credentials").
pub async fn connect_with_keys(
    state: &AppState,
    runtime: &std::sync::Arc<ConnectorRuntime>,
    row: ConnectionRow,
    consumer_key: String,
    consumer_secret: String,
) -> Response {
    if runtime.providers.get(ProviderKind::Woocommerce).is_none() {
        return provider_unavailable();
    }
    let now = state.clock.now();
    let still_draft = row.status == "draft"
        && row.connect_state_hash.is_some()
        && row.connect_expires_at.is_some_and(|expires| expires > now);
    let replaceable = matches!(
        row.status.as_str(),
        "connected" | "needs_attention" | "error"
    );
    if row.provider != ProviderKind::Woocommerce.as_str() || !(still_draft || replaceable) {
        return error(
            StatusCode::CONFLICT,
            "not_pending",
            "This connection is not waiting for keys. Start a new WooCommerce connection first.",
        );
    }
    let keys = match ApiKeys::parse(&consumer_key, &consumer_secret) {
        Ok(keys) => keys,
        Err(ProviderError::Invalid(message)) => {
            return error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_keys", &message)
        }
        Err(other) => return provider_failure(StatusCode::BAD_GATEWAY, other),
    };
    if replaceable {
        return replace_keys(state, runtime, row, keys).await;
    }
    let completed = match verify_keys(runtime, &row, &keys, false).await {
        Ok(completed) => completed,
        Err(response) => return response,
    };
    let provider = runtime
        .providers
        .get(ProviderKind::Woocommerce)
        .expect("checked above");
    let missing: Vec<String> = provider
        .required_scopes()
        .into_iter()
        .filter(|scope| !completed.scopes.contains(scope))
        .collect();
    let recipient_key = recipient::generate();
    match store::complete_connect(
        &state.pool,
        &runtime.keys,
        state.clock.now(),
        &row,
        &completed.credentials,
        &completed.scopes,
        &missing,
        &recipient_key,
    )
    .await
    {
        Ok(connected) => {
            // Stored; now the store's webhooks, at this connection's own URL.
            let url = runtime.settings.woocommerce_webhook_url(connected.id);
            match register_webhooks(state, runtime, provider, &connected, &url).await {
                Ok(connected) => connected_json(state, runtime, &connected).await,
                Err(response) => response,
            }
        }
        Err(CompleteError::ShopInUse) => error(
            StatusCode::CONFLICT,
            "shop_in_use",
            "Another seller already has this store connected.",
        ),
        Err(CompleteError::Stale) => error(
            StatusCode::CONFLICT,
            "not_pending",
            "This connection is no longer waiting for keys.",
        ),
        Err(CompleteError::Database(cause)) => internal("connection completion", &cause),
    }
}

/// Verifies a key pair against the connection's store, exactly as connecting
/// does (the read that decides Basic or query-string auth, the write-access
/// check, a fresh per-connection webhook secret), and returns the credential to
/// seal. Nothing is changed on the store or on our side: the webhooks are
/// registered after the credential is stored. `rotate` adds the storefront
/// probe: the store must still answer at the address the connection is for. A
/// refusal is typed.
async fn verify_keys(
    runtime: &std::sync::Arc<ConnectorRuntime>,
    row: &ConnectionRow,
    keys: &ApiKeys,
    rotate: bool,
) -> Result<ConnectCompleted, Response> {
    let provider = runtime
        .providers
        .get(ProviderKind::Woocommerce)
        .ok_or_else(provider_unavailable)?;
    let body = serde_json::to_vec(&json!({
        "consumer_key": keys.key,
        "consumer_secret": keys.secret,
        "rotate": rotate,
    }))
    .expect("keys serialize");
    let complete = CompleteRequest {
        pending: PendingConnection {
            connection_id: row.id,
            seller_pubky: row.seller_pubky.clone(),
            shop: row.shop.clone(),
            state: String::new(),
        },
        app: None,
        callback: CallbackParams {
            body,
            ..CallbackParams::default()
        },
        webhook_url: runtime.settings.woocommerce_webhook_url(row.id),
    };
    match call(runtime, provider.complete_connect(&complete)).await {
        Ok(completed) => Ok(completed),
        Err(ProviderError::Rejected { code, .. }) if code == "credentials_rejected" => Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "credentials_rejected",
            "The store does not accept this consumer key and secret.",
        )),
        Err(ProviderError::Unauthorized) => Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "credentials_rejected",
            "The store does not accept this consumer key and secret.",
        )),
        Err(ProviderError::Rejected { code, .. }) if code == "keys_read_only" => Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "keys_read_only",
            "These keys cannot create webhooks or orders. Create a key with Read/Write permissions.",
        )),
        Err(ProviderError::Rejected { code, message }) if code == "wrong_store" => Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "wrong_store",
            &message,
        )),
        Err(failure) => Err(provider_failure(StatusCode::BAD_GATEWAY, failure)),
    }
}

/// "Replace credentials" on a connected (or needs-attention, or rejected)
/// store, in three steps and no lock held while the store is called:
///
/// 1. the connection's stored credential is read (no lock);
/// 2. the new pair is verified against the store (probe, auth read, write
///    check), which changes nothing;
/// 3. one short transaction locks the connection, checks it is still live and
///    that the credential is still the one read in step 1, and swaps it.
///
/// If the connection was disconnected or its credential replaced by someone
/// else in between, step 3 writes nothing and answers 409: a disconnected
/// connection never gets keys back, and two replacements never both apply.
/// Only after the swap are the store's webhooks registered at the connection's
/// own URL with the new secret (and settled against the database, see
/// `register_webhooks`). A refusal at steps 1 or 2 (wrong keys, keys for
/// another store, a read-only key, the store unreachable) leaves the old keys,
/// secret, webhooks and status as they were. A failure at the last step leaves
/// the connection `needs_attention` (`webhook_registration_failed`) with the
/// new keys; replacing the keys again repeats it. The old key pair is not
/// revoked on the store (WooCommerce has no call for it): the merchant deletes
/// it under WooCommerce > Settings > Advanced > REST API.
async fn replace_keys(
    state: &AppState,
    runtime: &std::sync::Arc<ConnectorRuntime>,
    row: ConnectionRow,
    keys: ApiKeys,
) -> Response {
    let provider = runtime
        .providers
        .get(ProviderKind::Woocommerce)
        .expect("checked by the caller");
    let held: Result<Option<(Vec<u8>,)>, sqlx::Error> = sqlx::query_as(
        "SELECT k.sealed FROM connector_credentials k \
         JOIN connector_connections c ON c.id = k.connection_id \
         WHERE c.id = $1 AND c.status IN ('connected', 'needs_attention', 'error')",
    )
    .bind(row.id)
    .fetch_optional(&state.pool)
    .await;
    let held = match held {
        Ok(Some((sealed,))) => sealed,
        Ok(None) => {
            return error(
                StatusCode::CONFLICT,
                "not_pending",
                "This connection is no longer connected.",
            )
        }
        Err(cause) => return internal("credential replacement", &cause),
    };
    let completed = match verify_keys(runtime, &row, &keys, true).await {
        Ok(completed) => completed,
        Err(response) => return response,
    };
    let missing: Vec<String> = provider
        .required_scopes()
        .into_iter()
        .filter(|scope| !completed.scopes.contains(scope))
        .collect();
    let now = state.clock.now();
    let aad = crate::connectors::keys::credential_aad(&row.seller_pubky, &row.provider, &row.shop);
    let sealed = runtime.keys.seal(&aad, completed.credentials.expose());
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(cause) => return internal("credential replacement", &cause),
    };
    let locked: Result<Option<(String,)>, sqlx::Error> = sqlx::query_as(
        "SELECT status FROM connector_connections \
         WHERE id = $1 AND status IN ('connected', 'needs_attention', 'error') FOR UPDATE",
    )
    .bind(row.id)
    .fetch_optional(&mut *tx)
    .await;
    match locked {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error(
                StatusCode::CONFLICT,
                "not_pending",
                "This connection is no longer connected.",
            )
        }
        Err(cause) => return internal("credential replacement", &cause),
    }
    let swapped = sqlx::query(
        "UPDATE connector_credentials SET sealed = $2, updated_at = $3 \
         WHERE connection_id = $1 AND sealed = $4",
    )
    .bind(row.id)
    .bind(&sealed)
    .bind(now)
    .bind(&held)
    .execute(&mut *tx)
    .await;
    match swapped {
        Ok(done) if done.rows_affected() == 1 => {}
        Ok(_) => {
            return error(
                StatusCode::CONFLICT,
                "credentials_changed",
                "The connection's keys were replaced while these were being checked. Check the connection and try again.",
            )
        }
        Err(cause) => return internal("credential replacement", &cause),
    }
    let updated: Result<ConnectionRow, sqlx::Error> = sqlx::query_as(&format!(
        "UPDATE connector_connections SET status = $4, scopes = $2, missing_scopes = $3, \
         last_error = NULL, last_error_at = NULL, updated_at = $5 \
         WHERE id = $1 RETURNING {}",
        store::CONNECTION_COLUMNS
    ))
    .bind(row.id)
    .bind(&completed.scopes)
    .bind(&missing)
    .bind(if missing.is_empty() {
        "connected"
    } else {
        "needs_attention"
    })
    .bind(now)
    .fetch_one(&mut *tx)
    .await;
    let connected = match updated {
        Ok(connected) => connected,
        Err(cause) => return internal("credential replacement", &cause),
    };
    if let Err(cause) = tx.commit().await {
        return internal("credential replacement", &cause);
    }
    let url = runtime.settings.woocommerce_webhook_url(connected.id);
    match register_webhooks(state, runtime, provider, &connected, &url).await {
        Ok(connected) => connected_json(state, runtime, &connected).await,
        Err(response) => response,
    }
}
