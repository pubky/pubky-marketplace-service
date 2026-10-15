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
        Ok(connected) => connected_json(state, runtime, &connected).await,
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
/// does (the read that decides Basic or query-string auth, the webhooks
/// registered with a fresh per-connection secret), and returns the credential to
/// seal. `rotate` adds the storefront probe: the store must still answer at the
/// address the connection is for. A refusal is typed and nothing was changed on
/// our side.
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
        webhook_url: runtime.settings.woocommerce_webhook_url(),
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
/// store: verify the new pair, then swap it for the old one in one
/// transaction.
///
/// The connection's row is locked for the whole call, so two replacements of
/// the same store run one after the other and the webhooks the store ends up
/// with always match the secret that is stored. A refusal (wrong keys, keys
/// for another store, a read-only key, the store unreachable) returns before
/// anything is written and leaves the old keys, secret and status as they were;
/// the webhooks are swapped create-first, so even a failure while registering
/// leaves the old ones working. The old key pair is not revoked on the store
/// (WooCommerce has no call for it): the merchant deletes it under WooCommerce
/// > Settings > Advanced > REST API.
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
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(cause) => return internal("credential replacement", &cause),
    };
    let locked: Option<(String,)> = match sqlx::query_as(
        "SELECT status FROM connector_connections \
         WHERE id = $1 AND status IN ('connected', 'needs_attention', 'error') FOR UPDATE",
    )
    .bind(row.id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(locked) => locked,
        Err(cause) => return internal("credential replacement", &cause),
    };
    if locked.is_none() {
        return error(
            StatusCode::CONFLICT,
            "not_pending",
            "This connection is no longer connected.",
        );
    }
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
    let written = sqlx::query(
        "INSERT INTO connector_credentials (connection_id, sealed, created_at, updated_at) \
         VALUES ($1, $2, $3, $3) \
         ON CONFLICT (connection_id) DO UPDATE SET sealed = EXCLUDED.sealed, \
             updated_at = EXCLUDED.updated_at",
    )
    .bind(row.id)
    .bind(&sealed)
    .bind(now)
    .execute(&mut *tx)
    .await;
    if let Err(cause) = written {
        return internal("credential replacement", &cause);
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
    connected_json(state, runtime, &connected).await
}
