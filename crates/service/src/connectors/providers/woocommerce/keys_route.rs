//! `PUT /v1/connectors/{id}/woocommerce/keys`: the pasted-keys way to connect a
//! WooCommerce store, for merchants whose site blocks the one-click flow.
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
    let Some(provider) = runtime.providers.get(ProviderKind::Woocommerce) else {
        return provider_unavailable();
    };
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    let now = state.clock.now();
    let still_draft = row.status == "draft"
        && row.connect_state_hash.is_some()
        && row.connect_expires_at.is_some_and(|expires| expires > now);
    if row.provider != ProviderKind::Woocommerce.as_str() || !still_draft {
        return error(
            StatusCode::CONFLICT,
            "not_pending",
            "This connection is not waiting for keys. Start a new WooCommerce connection first.",
        );
    }
    let keys = match ApiKeys::parse(&request.consumer_key, &request.consumer_secret) {
        Ok(keys) => keys,
        Err(ProviderError::Invalid(message)) => {
            return error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_keys", &message)
        }
        Err(other) => return provider_failure(StatusCode::BAD_GATEWAY, other),
    };
    let body = serde_json::to_vec(&json!({
        "consumer_key": keys.key,
        "consumer_secret": keys.secret,
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
    let completed = match call(runtime, provider.complete_connect(&complete)).await {
        Ok(completed) => completed,
        Err(ProviderError::Rejected { code, .. }) if code == "credentials_rejected" => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "credentials_rejected",
                "The store does not accept this consumer key and secret.",
            )
        }
        Err(ProviderError::Unauthorized) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "credentials_rejected",
                "The store does not accept this consumer key and secret.",
            )
        }
        Err(ProviderError::Rejected { code, .. }) if code == "keys_read_only" => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "keys_read_only",
                "These keys cannot create webhooks or orders. Create a key with Read/Write permissions.",
            )
        }
        Err(failure) => return provider_failure(StatusCode::BAD_GATEWAY, failure),
    };
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
        Ok(connected) => connected_json(&state, runtime, &connected).await,
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
