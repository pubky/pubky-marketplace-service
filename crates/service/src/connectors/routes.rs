//! The `/v1/connectors` routes.
//!
//! Seller routes sit behind `auth::require_session`, the same layer as the
//! other seller routes, and act only on connections the session's pubky
//! owns: another seller's connection id answers exactly like an unknown id.
//! The provider routes (a Shopify connection's launch, OAuth callback and
//! webhooks, WooCommerce's callback and webhooks) and the public connector
//! status are unauthenticated by nature. Their authenticity is the provider's
//! signature (launch, webhook), checked with that connection's own app secret
//! for Shopify, or the single-use `state` issued when the install began
//! (callbacks). The paths are fixed because merchants configure their apps
//! with them; a Shopify connection's are keyed by its random, unguessable id:
//!
//! ```text
//! GET  /v1/connectors/shopify/c/{connection_id}/launch
//! GET  /v1/connectors/shopify/c/{connection_id}/oauth/callback
//! POST /v1/connectors/shopify/c/{connection_id}/webhooks
//! POST /v1/connectors/woocommerce/oauth/callback
//! POST /v1/connectors/webhooks/woocommerce
//! ```
//!
//! No response carries a credential, and no handler logs one: the plaintext
//! exists only inside a [`provider::ConnectionContext`] or
//! [`provider::AppContext`] handed to a provider call.

// A handler's early return is the `Response` it would send, so the helpers
// that can refuse return it as their `Err`.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, HashSet};

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::AppState;

use super::config::{
    APP_SECRET_RETENTION_SECONDS, DRAFT_TTL_SECONDS, MAX_MAPPINGS_PER_REQUEST,
    MAX_SELECTED_PRODUCTS, OAUTH_STATE_TTL_SECONDS,
};
use super::inbound::{self, Recorded};
use super::ledger::{self, reason_is_informational, PushView, RetryOutcome};
use super::provider::{
    AppKind, AuthorizeRequest, CallbackParams, CompleteRequest, ConnectRequest, ConnectorProvider,
    PendingConnection, ProviderError, ProviderKind, WebhookContext, WebhookHeaders,
};
use super::recipient;
use super::store::{
    self, AlertRow, CompleteError, ConnectError, ConnectionRow, MappingError, MappingRow,
    OpenError, SaveAppError, SelectionError,
};
use super::ConnectorRuntime;

const DEFAULT_PAGE: i64 = 50;
const MAX_PAGE: i64 = 200;
const MAX_PAGE_TOKEN_BYTES: usize = 2048;
const MAX_FIELD_CHARS: usize = 256;
const RECIPIENT_ALGORITHM: &str = "hpke-x25519-hkdf-sha256-chacha20poly1305";

fn respond(status: StatusCode, body: Value) -> Response {
    let mut response = (status, Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    respond(
        status,
        json!({
            "schema_version": 1,
            "ok": false,
            "error": { "code": code, "message": message },
        }),
    )
}

fn internal(context: &str, cause: &sqlx::Error) -> Response {
    tracing::error!(error = %cause, "connector {context} failed");
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "The request could not be completed.",
    )
}

fn disabled() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "connectors_disabled",
        "Store connectors are not enabled on this deployment.",
    )
}

fn runtime(state: &AppState) -> Result<&std::sync::Arc<ConnectorRuntime>, Response> {
    state.connectors.as_ref().ok_or_else(disabled)
}

fn not_found() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "connection_not_found",
        "The connection was not found.",
    )
}

async fn owned(state: &AppState, actor: &Actor, raw_id: &str) -> Result<ConnectionRow, Response> {
    let Ok(id) = Uuid::parse_str(raw_id) else {
        return Err(not_found());
    };
    match store::get_owned(&state.pool, &actor.0, id).await {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(not_found()),
        Err(cause) => Err(internal("connection lookup", &cause)),
    }
}

fn live(row: &ConnectionRow) -> Result<(), Response> {
    if row.is_live() {
        Ok(())
    } else {
        Err(error(
            StatusCode::CONFLICT,
            "connection_not_connected",
            "The store is not connected.",
        ))
    }
}

fn provider_for<'a>(
    runtime: &'a ConnectorRuntime,
    row: &ConnectionRow,
) -> Result<&'a std::sync::Arc<dyn ConnectorProvider>, Response> {
    row.provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
        .ok_or_else(provider_unavailable)
}

fn provider_unavailable() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "provider_unavailable",
        "That provider is not configured on this deployment.",
    )
}

fn provider_failure(status_for_unknown: StatusCode, failure: ProviderError) -> Response {
    match failure {
        ProviderError::RateLimited { retry_after } => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "provider_rate_limited",
                "The store is rate limiting requests. Try again shortly.",
            );
            let seconds = retry_after.map_or(30, |wait| wait.as_secs().clamp(1, 3600));
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&seconds.to_string()).expect("integer header"),
            );
            response
        }
        ProviderError::Rejected { code, .. } => {
            tracing::info!(code = %code, "provider rejected a connector request");
            error(
                StatusCode::BAD_GATEWAY,
                "provider_rejected",
                "The store refused the request.",
            )
        }
        ProviderError::Unauthorized => error(
            StatusCode::CONFLICT,
            "credentials_rejected",
            "The store no longer accepts the connection. Connect it again.",
        ),
        ProviderError::Invalid(message) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            &store::truncate(&message, 200),
        ),
        ProviderError::Unknown(_) => error(
            status_for_unknown,
            "provider_unavailable",
            "The store could not be reached.",
        ),
    }
}

async fn call<T>(
    runtime: &ConnectorRuntime,
    future: impl std::future::Future<Output = Result<T, ProviderError>>,
) -> Result<T, ProviderError> {
    match tokio::time::timeout(runtime.settings.provider_timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(ProviderError::Unknown("the provider call timed out".into())),
    }
}

async fn open_or_respond(
    state: &AppState,
    runtime: &ConnectorRuntime,
    row: &ConnectionRow,
) -> Result<super::provider::ConnectionContext, Response> {
    match runtime.open(&state.pool, row).await {
        Ok(context) => Ok(context),
        Err(OpenError::Unavailable) => Err(error(
            StatusCode::CONFLICT,
            "connection_not_connected",
            "The store is not connected.",
        )),
        Err(OpenError::Unreadable) => {
            tracing::error!(connection_id = %row.id, "connector credential is unreadable");
            Err(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "credential_unreadable",
                "The stored connection cannot be read. Connect the store again.",
            ))
        }
        Err(OpenError::Database(cause)) => Err(internal("credential read", &cause)),
    }
}

async fn on_unauthorized(state: &AppState, row: &ConnectionRow) {
    let now = state.clock.now();
    if let Err(cause) = store::mark_error(&state.pool, row.id, "credentials_rejected", now).await {
        tracing::error!(error = %cause, "connector could not record a rejected credential");
        return;
    }
    if let Err(cause) = store::raise_alert(
        &state.pool,
        now,
        row,
        store::AlertKind::CredentialsRejected,
        None,
        "The store no longer accepts the connection. Connect the store again.",
        &format!("credentials|{}", row.id),
    )
    .await
    {
        tracing::error!(error = %cause, "connector could not raise a credentials alert");
    }
}

// === JSON shapes =============================================================

fn ts(value: DateTime<Utc>) -> String {
    format_timestamp(value)
}

/// The URLs a merchant configures their store with, built from
/// `CONNECTOR_PUBLIC_BASE_URL`. A Shopify connection's carry its own id.
fn urls_json(runtime: &ConnectorRuntime, row: &ConnectionRow) -> Value {
    match row.provider_kind() {
        Some(ProviderKind::Shopify) => json!({
            "launch": runtime.settings.shopify_launch_url(row.id),
            "oauth_callback": runtime.settings.shopify_callback_url(row.id),
            "webhooks": runtime.settings.shopify_webhook_url(row.id),
        }),
        _ => json!({
            "oauth_callback": runtime.settings.woocommerce_callback_url(),
            "webhooks": runtime.settings.woocommerce_webhook_url(),
        }),
    }
}

struct ConnectionFacts {
    mappings: i64,
    pushes: BTreeMap<String, i64>,
    credentials_saved: bool,
}

fn connection_json(
    runtime: &ConnectorRuntime,
    row: &ConnectionRow,
    facts: &ConnectionFacts,
) -> Value {
    let count = |name: &str| facts.pushes.get(name).copied().unwrap_or(0);
    json!({
        "id": row.id,
        "provider": row.provider,
        "domain": row.shop,
        "app_kind": row.app_kind,
        "status": row.status,
        "scopes": row.scopes,
        "missing_scopes": row.missing_scopes,
        "credentials_saved": facts.credentials_saved,
        "urls": urls_json(runtime, row),
        "expires_at": (row.status == "draft")
            .then(|| row.connect_expires_at.map(ts))
            .flatten(),
        "connected_at": row.connected_at.map(ts),
        "created_at": ts(row.created_at),
        "updated_at": ts(row.updated_at),
        "last_error": row.last_error.as_ref().map(|message| json!({
            "message": message,
            "at": row.last_error_at.map(ts),
        })),
        "mapping_count": facts.mappings,
        "pushes": {
            "pending": count("pending"),
            "sending": count("sending"),
            "created": count("created"),
            "quarantined": count("quarantined"),
        },
    })
}

async fn connection_view(
    state: &AppState,
    runtime: &ConnectorRuntime,
    row: &ConnectionRow,
) -> Result<Value, sqlx::Error> {
    let mappings: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM connector_variant_map WHERE connection_id = $1")
            .bind(row.id)
            .fetch_one(&state.pool)
            .await?;
    let facts = ConnectionFacts {
        mappings,
        pushes: ledger::push_counts(&state.pool, row.id).await?,
        credentials_saved: store::has_app_credentials(&state.pool, row.id).await?,
    };
    Ok(connection_json(runtime, row, &facts))
}

fn push_json(view: &PushView) -> Value {
    let severity = view.quarantine_reason.as_deref().map(|reason| {
        if reason_is_informational(reason) {
            "info"
        } else {
            "action_required"
        }
    });
    json!({
        "id": view.id,
        "pubky_order_id": view.pubky_order_id,
        "provider": view.provider,
        "state": view.state,
        "external_id": view.external_id,
        "quarantine_reason": view.quarantine_reason,
        "quarantine_severity": severity,
        "attempts": view.attempts,
        "next_attempt_at": view.next_attempt_at.map(ts),
        "last_error": view.last_error,
        "created_at": ts(view.created_at),
        "updated_at": ts(view.updated_at),
        "actions": view.actions.iter().map(|action| json!({
            "kind": action.kind,
            "key": action.action_key,
            "state": action.state,
            "attempts": action.attempts,
            "quarantine_reason": action.quarantine_reason,
            "last_error": action.last_error,
            "updated_at": ts(action.updated_at),
        })).collect::<Vec<_>>(),
    })
}

fn mapping_json(mapping: &MappingRow) -> Value {
    json!({
        "listing_id": mapping.listing_id,
        "variant_id": (!mapping.variant_id.is_empty()).then_some(&mapping.variant_id),
        "provider_product_id": mapping.provider_product_id,
        "provider_variant_id":
            (!mapping.provider_variant_id.is_empty()).then_some(&mapping.provider_variant_id),
        "sku": mapping.sku,
    })
}

fn alert_json(alert: &AlertRow) -> Value {
    json!({
        "id": alert.id,
        "connection_id": alert.connection_id,
        "kind": alert.kind,
        "pubky_order_id": alert.pubky_order_id,
        "detail": alert.detail,
        "created_at": ts(alert.created_at),
        "resolved_at": alert.resolved_at.map(ts),
    })
}

// === Seller routes ===========================================================

pub async fn list_connections(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let rows = match store::list_for_seller(&state.pool, &actor.0, state.clock.now()).await {
        Ok(rows) => rows,
        Err(cause) => return internal("connection list", &cause),
    };
    let mut connections = Vec::with_capacity(rows.len());
    for row in &rows {
        match connection_view(&state, runtime, row).await {
            Ok(view) => connections.push(view),
            Err(cause) => return internal("connection summary", &cause),
        }
    }
    respond(
        StatusCode::OK,
        json!({
            "schema_version": 1,
            "kind": "connector_list",
            "connections": connections,
        }),
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartConnectRequest {
    provider: String,
    domain: String,
    /// Reserved: `public` is the slot for Pubky's own Shopify app and is
    /// refused until it exists. Omitted means the provider's own kind.
    #[serde(default)]
    app_kind: Option<String>,
    #[serde(default)]
    params: Option<Value>,
}

/// `POST /v1/connectors`: the signed-in seller starts connecting a store.
///
/// This creates the **draft connection**: a random, unguessable id and an
/// expiry 24 hours out. Asking again for the same store returns the same
/// draft (renewed), so the URLs the merchant already configured keep
/// working. The answer carries the URLs to show (`connection.urls`).
///
/// * Shopify (a merchant's own custom app): `authorize_url` is `null`. The
///   merchant saves the app's client id and secret with
///   `PUT /v1/connectors/{id}/credentials`, then installs from Shopify; the
///   connection's launch URL begins the authorization.
/// * WooCommerce: `authorize_url` is the store's `wc-auth` URL, whose
///   `user_id` is the draft's single-use nonce.
pub async fn start_connect(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(request): Json<StartConnectRequest>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let Some(kind) = ProviderKind::parse(&request.provider) else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_provider",
            "The provider must be shopify or woocommerce.",
        );
    };
    let Some(provider) = runtime.providers.get(kind) else {
        return provider_unavailable();
    };
    let app_kind = provider.app_kind();
    match request.app_kind.as_deref().map(AppKind::parse) {
        None => {}
        Some(Some(AppKind::Public)) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "public_app_unavailable",
                "The Pubky Shopify app is not available yet.",
            )
        }
        Some(Some(requested)) if requested == app_kind => {}
        Some(_) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_app_kind",
                "That app kind does not fit this provider.",
            )
        }
    }
    let params = request.params.unwrap_or_else(|| json!({}));
    if !params.is_object() || params.to_string().len() > 4096 {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            "params must be a small JSON object.",
        );
    }
    let shop = match provider.normalize_shop(&request.domain) {
        Ok(shop) if !shop.is_empty() && shop.len() <= 255 && !shop.contains(['|', '\n', '\r']) => {
            shop
        }
        Err(ProviderError::Invalid(reason)) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_domain",
                &store::truncate(&reason, 200),
            )
        }
        _ => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_domain",
                "The store address is not valid for this provider.",
            )
        }
    };
    // A provider that sends the merchant straight to the store (WooCommerce)
    // carries a nonce in that request; one installed from the store's own
    // admin (Shopify) gets its OAuth state at launch.
    let nonce = (app_kind == AppKind::None).then(|| {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        URL_SAFE_NO_PAD.encode(bytes)
    });
    let now = state.clock.now();
    let row = match store::begin_draft(
        &state.pool,
        now,
        &actor.0,
        kind,
        &shop,
        app_kind,
        nonce.as_deref().map(store::hash_state).as_deref(),
        now + chrono::Duration::seconds(DRAFT_TTL_SECONDS),
    )
    .await
    {
        Ok(row) => row,
        Err(ConnectError::AlreadyConnected) => {
            return error(
                StatusCode::CONFLICT,
                "already_connected",
                "This store is already connected.",
            )
        }
        Err(ConnectError::LimitReached) => {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "connection_limit",
                "The seller connection limit was reached.",
            )
        }
        Err(ConnectError::Database(cause)) => return internal("connect start", &cause),
    };
    let app = match runtime.app_for(&state.pool, &row).await {
        Ok(app) => app,
        Err(OpenError::Database(cause)) => return internal("app credentials read", &cause),
        Err(_) => None,
    };
    let (callback_url, webhook_url) = match kind {
        ProviderKind::Shopify => (
            runtime.settings.shopify_callback_url(row.id),
            runtime.settings.shopify_webhook_url(row.id),
        ),
        ProviderKind::Woocommerce => (
            runtime.settings.woocommerce_callback_url(),
            runtime.settings.woocommerce_webhook_url(),
        ),
    };
    let connect = ConnectRequest {
        connection_id: row.id,
        seller_pubky: actor.0.clone(),
        shop: shop.clone(),
        state: nonce.unwrap_or_default(),
        app,
        callback_url,
        webhook_url,
        params,
    };
    let started = match call(runtime, provider.connect(&connect)).await {
        Ok(started) => started,
        Err(failure) => {
            if let Err(cause) = store::discard_new_draft(&state.pool, row.id).await {
                tracing::warn!(error = %cause, "a refused connect left its draft behind");
            }
            return provider_failure(StatusCode::BAD_GATEWAY, failure);
        }
    };
    let view = match connection_view(&state, runtime, &row).await {
        Ok(view) => view,
        Err(cause) => return internal("connection summary", &cause),
    };
    respond(
        StatusCode::CREATED,
        json!({
            "schema_version": 1,
            "kind": "connector_connect",
            "connection": view,
            "authorize_url": started.authorize_url,
        }),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutCredentialsRequest {
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    /// WooCommerce's pasted REST key pair, routed to the provider's own flow.
    #[serde(default)]
    consumer_key: Option<String>,
    #[serde(default)]
    consumer_secret: Option<String>,
}

impl std::fmt::Debug for PutCredentialsRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PutCredentialsRequest(<redacted>)")
    }
}

fn clean_secret(value: &str) -> bool {
    !value.is_empty()
        && value.chars().count() <= MAX_FIELD_CHARS
        && !value.chars().any(|c| c.is_control() || c.is_whitespace())
}

/// `PUT /v1/connectors/{id}/credentials`: the merchant's own app client id
/// and secret, sealed for this connection. Saving again replaces the pair
/// ("replace credentials"). They verify this connection's launch, OAuth and
/// webhook signatures only; they are never returned, never logged and never
/// written to the homeserver. The answer is the connection with
/// `credentials_saved: true`.
pub async fn put_credentials(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PutCredentialsRequest>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if row.provider_kind() == Some(ProviderKind::Woocommerce)
        && (request.consumer_key.is_some() || request.consumer_secret.is_some())
    {
        return woocommerce_keys::connect_with_keys(
            &state,
            runtime,
            row,
            request.consumer_key.unwrap_or_default(),
            request.consumer_secret.unwrap_or_default(),
        )
        .await;
    }
    let supported = row
        .provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
        .is_some_and(|provider| provider.app_kind() == AppKind::Custom);
    if !supported || row.app_kind() != AppKind::Custom {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "credentials_not_supported",
            "This connection does not take app credentials.",
        );
    }
    if !clean_secret(&request.client_id) || !clean_secret(&request.client_secret) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_credentials",
            "The client id and client secret must be non-empty values without spaces.",
        );
    }
    match store::save_app_credentials(
        &state.pool,
        &runtime.keys,
        state.clock.now(),
        &row,
        &request.client_id,
        &request.client_secret,
    )
    .await
    {
        Ok(()) => match connection_view(&state, runtime, &row).await {
            Ok(view) => respond(
                StatusCode::OK,
                json!({
                    "schema_version": 1,
                    "kind": "connector_credentials",
                    "connection": view,
                }),
            ),
            Err(cause) => internal("connection summary", &cause),
        },
        Err(SaveAppError::NotAvailable) => error(
            StatusCode::CONFLICT,
            "connection_not_available",
            "The draft expired or the connection was disconnected. Start again.",
        ),
        Err(SaveAppError::Database(cause)) => internal("credentials save", &cause),
    }
}

/// `DELETE /v1/connectors/{id}`: asks the provider to revoke (best effort)
/// and deletes the store credential, the recipient key, the mappings and the
/// product selection. A merchant app's secret is deleted at once for a draft
/// that never installed, and otherwise kept for at most 30 days (or until
/// `shop/redact`) so Shopify's privacy webhooks after an uninstall can be
/// verified.
pub async fn disconnect(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if let Some(provider) = row
        .provider_kind()
        .and_then(|kind| runtime.providers.get(kind))
    {
        if let Ok(context) = runtime.open(&state.pool, &row).await {
            if let Err(failure) = call(runtime, provider.disconnect(&context)).await {
                tracing::warn!(
                    connection_id = %row.id,
                    error = %failure,
                    "provider could not revoke a connector credential; deleting ours anyway"
                );
            }
        }
    }
    let now = state.clock.now();
    let retain_until = now + chrono::Duration::seconds(APP_SECRET_RETENTION_SECONDS);
    match store::disconnect(&state.pool, row.id, now, retain_until).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(cause) => internal("disconnect", &cause),
    }
}

#[derive(Debug, Deserialize)]
pub struct CatalogQuery {
    page: Option<String>,
    selected: Option<bool>,
}

/// `GET /v1/connectors/{id}/catalog`: one page of the store's products,
/// each marked with whether the seller selected it for publishing.
/// `?selected=true` keeps only the selected products of the page and
/// `?selected=false` only the others.
pub async fn get_catalog(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Query(query): Query<CatalogQuery>,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if let Err(response) = live(&row) {
        return response;
    }
    if query
        .page
        .as_deref()
        .is_some_and(|page| page.len() > MAX_PAGE_TOKEN_BYTES)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_page",
            "The page token is too long.",
        );
    }
    let provider = match provider_for(runtime, &row) {
        Ok(provider) => provider,
        Err(response) => return response,
    };
    let context = match open_or_respond(&state, runtime, &row).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    let page = match call(
        runtime,
        provider.list_catalog(&context, query.page.as_deref()),
    )
    .await
    {
        Ok(page) => page,
        Err(failure) => {
            if matches!(failure, ProviderError::Unauthorized) {
                on_unauthorized(&state, &row).await;
            }
            return provider_failure(StatusCode::BAD_GATEWAY, failure);
        }
    };
    let selected: HashSet<String> = match store::selected_products(&state.pool, row.id).await {
        Ok(selected) => selected.into_iter().collect(),
        Err(cause) => return internal("selection read", &cause),
    };
    let products: Vec<Value> = page
        .products
        .iter()
        .filter(|product| {
            query
                .selected
                .is_none_or(|wanted| selected.contains(&product.provider_product_id) == wanted)
        })
        .map(|product| {
            let mut value = serde_json::to_value(product).expect("catalog product serializes");
            value["selected"] = json!(selected.contains(&product.provider_product_id));
            value
        })
        .collect();
    respond(
        StatusCode::OK,
        json!({
            "schema_version": 1,
            "kind": "connector_catalog",
            "connection_id": row.id,
            "products": products,
            "next_page": page.next_page,
        }),
    )
}

pub async fn get_mappings(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    match store::list_mappings(&state.pool, row.id).await {
        Ok(mappings) => respond(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "kind": "connector_mappings",
                "connection_id": row.id,
                "mappings": mappings.iter().map(mapping_json).collect::<Vec<_>>(),
            }),
        ),
        Err(cause) => internal("mapping list", &cause),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutMappingsRequest {
    mappings: Vec<MappingInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingInput {
    listing_id: String,
    #[serde(default)]
    variant_id: Option<String>,
    provider_product_id: String,
    #[serde(default)]
    provider_variant_id: Option<String>,
    #[serde(default)]
    sku: Option<String>,
}

fn clean_id(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.chars().count() <= max
        && !value.chars().any(char::is_control)
        && value == value.trim()
}

fn validate_mappings(input: Vec<MappingInput>) -> Result<Vec<MappingRow>, &'static str> {
    if input.len() > MAX_MAPPINGS_PER_REQUEST {
        return Err("too_many_mappings");
    }
    let mut rows = Vec::with_capacity(input.len());
    let mut pubky_side = HashSet::new();
    let mut provider_side = HashSet::new();
    for mapping in input {
        let variant_id = mapping.variant_id.unwrap_or_default();
        let provider_variant_id = mapping.provider_variant_id.unwrap_or_default();
        let valid = clean_id(&mapping.listing_id, 128)
            && (variant_id.is_empty() || clean_id(&variant_id, 128))
            && clean_id(&mapping.provider_product_id, MAX_FIELD_CHARS)
            && (provider_variant_id.is_empty() || clean_id(&provider_variant_id, MAX_FIELD_CHARS))
            && mapping.sku.as_deref().is_none_or(|sku| clean_id(sku, 128));
        if !valid {
            return Err("invalid_mapping");
        }
        if !pubky_side.insert((mapping.listing_id.clone(), variant_id.clone()))
            || !provider_side.insert((
                mapping.provider_product_id.clone(),
                provider_variant_id.clone(),
            ))
        {
            return Err("duplicate_mapping");
        }
        rows.push(MappingRow {
            listing_id: mapping.listing_id,
            variant_id,
            provider_product_id: mapping.provider_product_id,
            provider_variant_id,
            sku: mapping.sku,
        });
    }
    Ok(rows)
}

/// `PUT /v1/connectors/{id}/mappings`: replaces the whole mapping set. A
/// product with a mapping is always in the product selection.
pub async fn put_mappings(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PutMappingsRequest>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if let Err(response) = live(&row) {
        return response;
    }
    let mappings = match validate_mappings(request.mappings) {
        Ok(mappings) => mappings,
        Err(code @ "duplicate_mapping") => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                code,
                "A listing variant and a store variant can each be mapped only once.",
            )
        }
        Err(code) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                code,
                "The mappings are not valid.",
            )
        }
    };
    match store::replace_mappings(&state.pool, &row, &mappings, state.clock.now()).await {
        Ok(()) => respond(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "kind": "connector_mappings",
                "connection_id": row.id,
                "mappings": mappings.iter().map(mapping_json).collect::<Vec<_>>(),
            }),
        ),
        Err(MappingError::UnknownListing(listing_id)) => respond(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({
                "schema_version": 1,
                "ok": false,
                "error": {
                    "code": "listing_not_found",
                    "message": "A mapped listing is not one of your listings.",
                    "listing_id": listing_id,
                },
            }),
        ),
        Err(MappingError::Conflict) => error(
            StatusCode::CONFLICT,
            "mapping_conflict",
            "The mappings changed while they were being saved. Try again.",
        ),
        Err(MappingError::Database(sqlx::Error::RowNotFound)) => error(
            StatusCode::CONFLICT,
            "connection_not_connected",
            "The store is not connected.",
        ),
        Err(MappingError::Database(cause)) => internal("mapping save", &cause),
    }
}

fn selection_json(connection: Uuid, product_ids: &[String]) -> Value {
    json!({
        "schema_version": 1,
        "kind": "connector_selection",
        "connection_id": connection,
        "product_ids": product_ids,
    })
}

pub async fn get_selection(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    match store::selected_products(&state.pool, row.id).await {
        Ok(products) => respond(StatusCode::OK, selection_json(row.id, &products)),
        Err(cause) => internal("selection read", &cause),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutSelectionRequest {
    product_ids: Vec<String>,
}

/// `PUT /v1/connectors/{id}/selection`: replaces the set of store products
/// the seller chose to publish on the Shop. A product that still has a
/// mapping cannot be dropped (`product_mapped`).
pub async fn put_selection(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PutSelectionRequest>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if let Err(response) = live(&row) {
        return response;
    }
    let ids = request.product_ids;
    if ids.len() > MAX_SELECTED_PRODUCTS || ids.iter().any(|id| !clean_id(id, MAX_FIELD_CHARS)) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_selection",
            "The product selection is not valid.",
        );
    }
    let unique: HashSet<&String> = ids.iter().collect();
    if unique.len() != ids.len() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "duplicate_product",
            "A product can be selected only once.",
        );
    }
    match store::replace_selection(&state.pool, &row, &ids, state.clock.now()).await {
        Ok(()) => {
            let mut sorted = ids;
            sorted.sort();
            respond(StatusCode::OK, selection_json(row.id, &sorted))
        }
        Err(SelectionError::MappedProduct(product)) => respond(
            StatusCode::CONFLICT,
            json!({
                "schema_version": 1,
                "ok": false,
                "error": {
                    "code": "product_mapped",
                    "message": "A product with a mapping cannot be unselected. Remove its mappings first.",
                    "product_id": product,
                },
            }),
        ),
        Err(SelectionError::Database(sqlx::Error::RowNotFound)) => error(
            StatusCode::CONFLICT,
            "connection_not_connected",
            "The store is not connected.",
        ),
        Err(SelectionError::Database(cause)) => internal("selection save", &cause),
    }
}

#[derive(Debug, Deserialize)]
pub struct PushesQuery {
    state: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

fn encode_push_cursor(at: DateTime<Utc>, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}:{id}", at.timestamp_micros()))
}

fn decode_push_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let (micros, id) = text.split_once(':')?;
    let at = Utc.timestamp_micros(micros.parse().ok()?).single()?;
    Some((at, Uuid::parse_str(id).ok()?))
}

pub async fn list_pushes(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
    Query(query): Query<PushesQuery>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    let limit = query.limit.unwrap_or(DEFAULT_PAGE);
    if !(1..=MAX_PAGE).contains(&limit) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_limit",
            "The page limit is invalid.",
        );
    }
    if let Some(filter) = query.state.as_deref() {
        if !matches!(filter, "pending" | "sending" | "created" | "quarantined") {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_state",
                "The state filter is invalid.",
            );
        }
    }
    let after = match query.cursor.as_deref().map(decode_push_cursor) {
        None => None,
        Some(Some(position)) => Some(position),
        Some(None) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_cursor",
                "The cursor is invalid.",
            )
        }
    };
    match ledger::list_pushes(&state.pool, row.id, query.state.as_deref(), after, limit).await {
        Ok((pushes, more)) => {
            let next_cursor = pushes
                .last()
                .filter(|_| more)
                .map(|last| encode_push_cursor(last.created_at, last.id));
            respond(
                StatusCode::OK,
                json!({
                    "schema_version": 1,
                    "kind": "connector_pushes",
                    "connection_id": row.id,
                    "pushes": pushes.iter().map(push_json).collect::<Vec<_>>(),
                    "next_cursor": next_cursor,
                }),
            )
        }
        Err(cause) => internal("push list", &cause),
    }
}

pub async fn retry_push(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path((id, order)): Path<(String, String)>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let row = match owned(&state, &actor, &id).await {
        Ok(row) => row,
        Err(response) => return response,
    };
    if let Err(response) = live(&row) {
        return response;
    }
    let Ok(order_id) = Uuid::parse_str(&order) else {
        return error(
            StatusCode::NOT_FOUND,
            "push_not_found",
            "There is no push for that order.",
        );
    };
    match ledger::retry_push(&state.pool, row.id, order_id, state.clock.now()).await {
        Ok(RetryOutcome::Queued(view)) => respond(
            StatusCode::ACCEPTED,
            json!({
                "schema_version": 1,
                "kind": "connector_push",
                "push": push_json(&view),
            }),
        ),
        Ok(RetryOutcome::NotFound) => error(
            StatusCode::NOT_FOUND,
            "push_not_found",
            "There is no push for that order.",
        ),
        Ok(RetryOutcome::NotQuarantined(current)) => respond(
            StatusCode::CONFLICT,
            json!({
                "schema_version": 1,
                "ok": false,
                "error": {
                    "code": if current == "created" { "already_created" } else { "not_retryable" },
                    "message": "Only a quarantined push can be retried.",
                    "state": current,
                },
            }),
        ),
        Err(cause) => internal("push retry", &cause),
    }
}

#[derive(Debug, Deserialize)]
pub struct AlertsQuery {
    unresolved: Option<bool>,
    limit: Option<i64>,
}

pub async fn list_alerts(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(query): Query<AlertsQuery>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let limit = query.limit.unwrap_or(DEFAULT_PAGE);
    if !(1..=MAX_PAGE).contains(&limit) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_limit",
            "The page limit is invalid.",
        );
    }
    match store::list_alerts(
        &state.pool,
        &actor.0,
        query.unresolved.unwrap_or(false),
        limit,
    )
    .await
    {
        Ok(alerts) => respond(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "kind": "connector_alerts",
                "alerts": alerts.iter().map(alert_json).collect::<Vec<_>>(),
            }),
        ),
        Err(cause) => internal("alert list", &cause),
    }
}

pub async fn resolve_alert(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = runtime(&state) {
        return response;
    }
    let missing = || {
        error(
            StatusCode::NOT_FOUND,
            "alert_not_found",
            "The alert was not found.",
        )
    };
    let Ok(id) = Uuid::parse_str(&id) else {
        return missing();
    };
    match store::resolve_alert(&state.pool, &actor.0, id, state.clock.now()).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => missing(),
        Err(cause) => internal("alert resolve", &cause),
    }
}

// === Public: connector status ================================================

/// `GET /v0/sellers/{pubky}/connector-status`: the stores a buyer may share
/// an address with, and each store's recipient public key. Public like the
/// seller's payment config: it names no credential and no private data. It
/// answers an empty list when connectors are off or the seller has none.
pub async fn connector_status(
    State(state): State<AppState>,
    Path(seller_pubky): Path<String>,
) -> Response {
    let stores = if state.connectors.is_some() && seller_pubky.len() <= 128 {
        match store::public_stores(&state.pool, &seller_pubky).await {
            Ok(stores) => stores,
            Err(cause) => return internal("connector status", &cause),
        }
    } else {
        Vec::new()
    };
    let body = json!({
        "schema_version": 1,
        "kind": "connector_status",
        "seller_pubky": seller_pubky,
        "stores": stores.iter().map(|store| json!({
            "provider": store.provider,
            "domain": store.shop,
            "address_sharing": {
                "recipient_key": {
                    "key_id": store.key_id,
                    "public_key": URL_SAFE_NO_PAD.encode(&store.public_key),
                    "algorithm": RECIPIENT_ALGORITHM,
                    "envelope_context": recipient::ENVELOPE_CONTEXT,
                    "envelope_bytes": recipient::ENVELOPE_BYTES,
                },
            },
        })).collect::<Vec<_>>(),
    });
    let mut response = (StatusCode::OK, Json(body)).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30"),
    );
    response
}

// === Provider routes =========================================================

fn headers_of(headers: &HeaderMap) -> WebhookHeaders {
    WebhookHeaders::from_pairs(headers.iter().filter_map(|(name, value)| {
        value
            .to_str()
            .ok()
            .map(|value| (name.as_str().to_string(), value.to_string()))
    }))
}

fn invalid_webhook() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "invalid_webhook",
        "The webhook could not be verified.",
    )
}

/// `POST /v1/connectors/webhooks/woocommerce`: a WooCommerce store's webhook,
/// found by the shop it names and verified with the store's own secret.
/// Unknown store, bad signature and unreadable credential all answer the
/// same 401, so the route does not reveal which stores are connected.
pub async fn receive_woocommerce_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let kind = ProviderKind::Woocommerce;
    let Some(provider) = runtime.providers.get(kind) else {
        return provider_unavailable();
    };
    let headers = headers_of(&headers);
    let Ok(shop) = provider.webhook_shop(&headers, &body) else {
        return invalid_webhook();
    };
    let connection = match store::live_for_shop(&state.pool, kind, &shop).await {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            // No connected store by that name. The one request that is still
            // welcome is one that needs no store: WooCommerce's unsigned ping,
            // which it repeats until answered 200. The provider decides.
            let unconnected = WebhookContext {
                app: None,
                connection: None,
            };
            return match provider.verify_webhook(&unconnected, &body, &headers) {
                Ok(_) => respond(
                    StatusCode::OK,
                    json!({ "schema_version": 1, "status": "ignored" }),
                ),
                Err(_) => invalid_webhook(),
            };
        }
        Err(cause) => return internal("webhook connection lookup", &cause),
    };
    accept_webhook(&state, runtime, provider, connection, &headers, &body).await
}

/// `POST /v1/connectors/shopify/c/{connection_id}/webhooks`: a webhook from
/// the merchant's own Shopify app, including the privacy topics. It is
/// verified with that connection's own app secret, which is kept for a while
/// after an uninstall for exactly these topics. An unknown or draft
/// connection, a missing secret, a different shop and a bad signature all
/// answer the same 401.
pub async fn receive_shopify_webhook(
    State(state): State<AppState>,
    Path(connection_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let Some(provider) = runtime.providers.get(ProviderKind::Shopify) else {
        return provider_unavailable();
    };
    let connection = match shopify_connection(&state, &connection_id).await {
        Some(Ok(connection)) => connection,
        Some(Err(response)) => return response,
        None => return invalid_webhook(),
    };
    if connection.status == "draft" {
        return invalid_webhook();
    }
    let headers = headers_of(&headers);
    match provider.webhook_shop(&headers, &body) {
        Ok(shop) if shop == connection.shop => {}
        _ => return invalid_webhook(),
    }
    accept_webhook(&state, runtime, provider, connection, &headers, &body).await
}

/// The Shopify connection a path names: one of this deployment's own
/// custom-app connections, in any state.
async fn shopify_connection(
    state: &AppState,
    raw_id: &str,
) -> Option<Result<ConnectionRow, Response>> {
    let id = Uuid::parse_str(raw_id).ok()?;
    match store::get(&state.pool, id).await {
        Ok(Some(row))
            if row.provider_kind() == Some(ProviderKind::Shopify)
                && row.app_kind() == AppKind::Custom =>
        {
            Some(Ok(row))
        }
        Ok(_) => None,
        Err(cause) => Some(Err(internal("connection lookup", &cause))),
    }
}

async fn accept_webhook(
    state: &AppState,
    runtime: &ConnectorRuntime,
    provider: &std::sync::Arc<dyn ConnectorProvider>,
    connection: ConnectionRow,
    headers: &WebhookHeaders,
    body: &Bytes,
) -> Response {
    let app = match runtime.app_for(&state.pool, &connection).await {
        Ok(app) => app,
        Err(OpenError::Unreadable) => {
            tracing::error!(
                connection_id = %connection.id,
                "connector app credential is unreadable; webhook refused"
            );
            return invalid_webhook();
        }
        Err(OpenError::Unavailable) => None,
        Err(OpenError::Database(cause)) => return internal("webhook app read", &cause),
    };
    let context = if connection.is_live() {
        match store::open_connection(&state.pool, &runtime.keys, &connection).await {
            Ok(opened) => Some(opened),
            Err(OpenError::Unavailable) => None,
            Err(OpenError::Unreadable) => {
                tracing::error!(
                    connection_id = %connection.id,
                    "connector credential is unreadable; webhook refused"
                );
                return invalid_webhook();
            }
            Err(OpenError::Database(cause)) => return internal("webhook credential read", &cause),
        }
    } else {
        None
    };
    let webhook_context = WebhookContext {
        app,
        connection: context,
    };
    let verified = match provider.verify_webhook(&webhook_context, body, headers) {
        Ok(verified) => verified,
        Err(_) => return invalid_webhook(),
    };
    let event = match provider.parse_inbound(&verified, body) {
        Ok(event) => event,
        Err(failure) => {
            tracing::warn!(
                connection_id = %connection.id,
                error = %failure,
                "a verified connector webhook could not be parsed"
            );
            return respond(
                StatusCode::OK,
                json!({ "schema_version": 1, "status": "ignored" }),
            );
        }
    };
    match inbound::record(
        &state.pool,
        state.clock.now(),
        &connection,
        &verified,
        &event,
    )
    .await
    {
        Ok(Recorded::New(_)) => respond(
            StatusCode::OK,
            json!({ "schema_version": 1, "status": "accepted" }),
        ),
        Ok(Recorded::Duplicate) => respond(
            StatusCode::OK,
            json!({ "schema_version": 1, "status": "duplicate" }),
        ),
        Err(cause) => internal("webhook record", &cause),
    }
}

/// Where a browser request ends: a redirect to the Shop's connection page
/// (`CONNECTOR_RETURN_URL`) with a `status` and either a `code` or the
/// `connector` id, or JSON when no return URL is configured.
fn browser_outcome(
    runtime: &ConnectorRuntime,
    pairs: &[(&str, &str)],
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    if let Some(target) = &runtime.settings.return_url {
        if let Ok(mut url) = url::Url::parse(target) {
            {
                let mut query = url.query_pairs_mut();
                for (name, value) in pairs {
                    query.append_pair(name, value);
                }
            }
            return Redirect::to(url.as_str()).into_response();
        }
    }
    error(status, code, message)
}

fn browser_error(
    runtime: &ConnectorRuntime,
    redirect: bool,
    connection: Option<Uuid>,
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    if redirect {
        let id = connection.map(|id| id.to_string());
        let mut pairs = vec![("status", "error"), ("code", code)];
        if let Some(id) = &id {
            pairs.push(("connector", id));
        }
        browser_outcome(runtime, &pairs, status, code, message)
    } else {
        error(status, code, message)
    }
}

/// `GET /v1/connectors/shopify/c/{connection_id}/launch`: the merchant's app
/// URL for this connection, where Shopify sends the merchant after Install
/// and every time the app is opened.
///
/// The launch is matched by the connection id in the path, verified with that
/// connection's own app secret, and must name the connection's store. A draft
/// or a connection that needs another install is sent on to authorize; one
/// that is already connected is sent back to the Shop's status page.
pub async fn shopify_launch(
    State(state): State<AppState>,
    Path(connection_id): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let Some(provider) = runtime.providers.get(ProviderKind::Shopify) else {
        return provider_unavailable();
    };
    let connection = match shopify_connection(&state, &connection_id).await {
        Some(Ok(connection)) => connection,
        Some(Err(response)) => return response,
        None => return not_found(),
    };
    let now = state.clock.now();
    let gone = connection.status == "disconnected"
        || (connection.status == "draft"
            && connection
                .connect_expires_at
                .is_none_or(|expiry| expiry <= now));
    if gone {
        return browser_error(
            runtime,
            true,
            Some(connection.id),
            StatusCode::GONE,
            "draft_expired",
            "This connection expired or was disconnected. Start again from the Shop.",
        );
    }
    let app = match runtime.app_for(&state.pool, &connection).await {
        Ok(Some(app)) => app,
        Ok(None) => {
            return browser_error(
                runtime,
                true,
                Some(connection.id),
                StatusCode::CONFLICT,
                "credentials_missing",
                "Save the app's client id and secret in the Shop first.",
            )
        }
        Err(OpenError::Database(cause)) => return internal("launch app read", &cause),
        Err(_) => {
            tracing::error!(connection_id = %connection.id, "connector app credential is unreadable");
            return browser_error(
                runtime,
                true,
                Some(connection.id),
                StatusCode::CONFLICT,
                "credentials_unreadable",
                "The saved app credentials cannot be read. Save them again in the Shop.",
            );
        }
    };
    let params = callback_params(raw_query, &headers, &Bytes::new());
    let shop = match provider.verify_launch(&app, &params) {
        Ok(shop) => shop,
        Err(_) => {
            return error(
                StatusCode::UNAUTHORIZED,
                "invalid_launch",
                "The launch request could not be verified.",
            )
        }
    };
    if shop != connection.shop {
        return browser_error(
            runtime,
            true,
            Some(connection.id),
            StatusCode::CONFLICT,
            "shop_mismatch",
            "The app was installed on a different store than this connection's.",
        );
    }
    if connection.status == "connected" {
        if runtime.settings.return_url.is_some() {
            return browser_outcome(
                runtime,
                &[
                    ("status", "connected"),
                    ("connector", &connection.id.to_string()),
                ],
                StatusCode::OK,
                "connected",
                "",
            );
        }
        return connected_json(&state, runtime, &connection).await;
    }
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let authorize_state = URL_SAFE_NO_PAD.encode(nonce);
    match store::bind_launch(
        &state.pool,
        connection.id,
        &store::hash_state(&authorize_state),
        now + chrono::Duration::seconds(OAUTH_STATE_TTL_SECONDS),
        now,
    )
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return browser_error(
                runtime,
                true,
                Some(connection.id),
                StatusCode::BAD_REQUEST,
                "invalid_state",
                "The connection request has expired.",
            )
        }
        Err(cause) => return internal("launch bind", &cause),
    }
    let authorize = AuthorizeRequest {
        app,
        shop,
        state: authorize_state,
        callback_url: runtime.settings.shopify_callback_url(connection.id),
    };
    match provider.authorize_url(&authorize) {
        Ok(url) => Redirect::to(&url).into_response(),
        Err(failure) => provider_failure(StatusCode::BAD_GATEWAY, failure),
    }
}

fn callback_params(raw_query: Option<String>, headers: &HeaderMap, body: &Bytes) -> CallbackParams {
    let raw_query = raw_query.unwrap_or_default();
    let query: BTreeMap<String, String> = url::form_urlencoded::parse(raw_query.as_bytes())
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    CallbackParams {
        raw_query,
        query,
        headers: headers_of(headers),
        body: body.to_vec(),
    }
}

/// `GET /v1/connectors/shopify/c/{connection_id}/oauth/callback`.
pub async fn shopify_callback(
    State(state): State<AppState>,
    Path(connection_id): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Ok(id) = Uuid::parse_str(&connection_id) else {
        return not_found();
    };
    let params = callback_params(raw_query, &headers, &Bytes::new());
    complete_callback(&state, ProviderKind::Shopify, Some(id), true, params).await
}

/// `POST /v1/connectors/woocommerce/oauth/callback`: WooCommerce posts the
/// store's keys here, with `user_id` set to the draft's nonce.
pub async fn woocommerce_callback(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = callback_params(raw_query, &headers, &body);
    complete_callback(&state, ProviderKind::Woocommerce, None, false, params).await
}

async fn complete_callback(
    state: &AppState,
    kind: ProviderKind,
    path_connection: Option<Uuid>,
    redirect: bool,
    params: CallbackParams,
) -> Response {
    let runtime = match runtime(state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let Some(provider) = runtime.providers.get(kind) else {
        return provider_unavailable();
    };
    let invalid = |connection: Option<Uuid>| {
        browser_error(
            runtime,
            redirect,
            connection,
            StatusCode::BAD_REQUEST,
            "invalid_state",
            "The connection request is not valid or has expired.",
        )
    };
    let Ok(connect_state) = provider.callback_state(&params) else {
        return invalid(path_connection);
    };
    let now = state.clock.now();
    let pending = match store::pending_by_state(&state.pool, kind, &connect_state, now).await {
        Ok(Some(pending)) => pending,
        Ok(None) => return invalid(path_connection),
        Err(cause) => return internal("callback lookup", &cause),
    };
    if path_connection.is_some_and(|id| id != pending.id) {
        return invalid(path_connection);
    }
    let app = match runtime.app_for(&state.pool, &pending).await {
        Ok(app) => app,
        Err(OpenError::Database(cause)) => return internal("callback app read", &cause),
        Err(_) => None,
    };
    if provider.app_kind() == AppKind::Custom && app.is_none() {
        return browser_error(
            runtime,
            redirect,
            Some(pending.id),
            StatusCode::CONFLICT,
            "credentials_missing",
            "Save the app's client id and secret in the Shop first.",
        );
    }
    let webhook_url = match kind {
        ProviderKind::Shopify => runtime.settings.shopify_webhook_url(pending.id),
        ProviderKind::Woocommerce => runtime.settings.woocommerce_webhook_url(),
    };
    let request = CompleteRequest {
        pending: PendingConnection {
            connection_id: pending.id,
            seller_pubky: pending.seller_pubky.clone(),
            shop: pending.shop.clone(),
            state: connect_state,
        },
        app,
        callback: params,
        webhook_url: webhook_url.clone(),
    };
    let completed = match call(runtime, provider.complete_connect(&request)).await {
        Ok(completed) => completed,
        Err(failure) => {
            let (status, code) = match &failure {
                ProviderError::RateLimited { .. } | ProviderError::Unknown(_) => {
                    (StatusCode::BAD_GATEWAY, "provider_unavailable")
                }
                _ => (StatusCode::BAD_REQUEST, "connect_rejected"),
            };
            tracing::info!(
                connection_id = %pending.id,
                outcome = code,
                "provider did not complete a connection"
            );
            return browser_error(
                runtime,
                redirect,
                Some(pending.id),
                status,
                code,
                "The store did not complete the connection.",
            );
        }
    };
    let missing: Vec<String> = provider
        .required_scopes()
        .into_iter()
        .filter(|scope| !completed.scopes.contains(scope))
        .collect();
    let recipient_key = recipient::generate();
    let connected = match store::complete_connect(
        &state.pool,
        &runtime.keys,
        state.clock.now(),
        &pending,
        &completed.credentials,
        &completed.scopes,
        &missing,
        &recipient_key,
    )
    .await
    {
        Ok(connected) => connected,
        Err(CompleteError::ShopInUse) => {
            return browser_error(
                runtime,
                redirect,
                Some(pending.id),
                StatusCode::CONFLICT,
                "shop_in_use",
                "Another seller already has this store connected.",
            )
        }
        Err(CompleteError::Stale) => return invalid(path_connection),
        Err(CompleteError::Database(cause)) => return internal("connection completion", &cause),
    };
    // The install is stored; now the provider subscribes the store's webhooks
    // to this connection's URL. A failure leaves the connection needing the
    // seller rather than undoing the install.
    let connected =
        match register_webhooks(state, runtime, provider, &connected, &webhook_url).await {
            Ok(row) => row,
            Err(response) => return response,
        };
    if redirect && runtime.settings.return_url.is_some() {
        return browser_outcome(
            runtime,
            &[
                ("status", connected.status.as_str()),
                ("connector", &connected.id.to_string()),
            ],
            StatusCode::OK,
            "connected",
            "",
        );
    }
    connected_json(state, runtime, &connected).await
}

async fn register_webhooks(
    state: &AppState,
    runtime: &ConnectorRuntime,
    provider: &std::sync::Arc<dyn ConnectorProvider>,
    connected: &ConnectionRow,
    webhook_url: &str,
) -> Result<ConnectionRow, Response> {
    let context = match runtime.open(&state.pool, connected).await {
        Ok(context) => context,
        Err(OpenError::Database(cause)) => return Err(internal("webhook registration", &cause)),
        Err(_) => return Ok(connected.clone()),
    };
    match call(runtime, provider.register_webhooks(&context, webhook_url)).await {
        Ok(()) => Ok(connected.clone()),
        Err(failure) => {
            tracing::warn!(
                connection_id = %connected.id,
                error = %failure,
                "the provider could not register the store's webhooks"
            );
            store::set_attention(
                &state.pool,
                connected.id,
                "webhook_registration_failed",
                state.clock.now(),
            )
            .await
            .map_err(|cause| internal("webhook registration", &cause))?;
            store::get(&state.pool, connected.id)
                .await
                .map_err(|cause| internal("webhook registration", &cause))?
                .ok_or_else(not_found)
        }
    }
}

async fn connected_json(
    state: &AppState,
    runtime: &ConnectorRuntime,
    connected: &ConnectionRow,
) -> Response {
    match connection_view(state, runtime, connected).await {
        Ok(view) => respond(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "kind": "connector_connected",
                "connection": view,
            }),
        ),
        Err(cause) => internal("connection summary", &cause),
    }
}

#[path = "providers/woocommerce/keys_route.rs"]
pub mod woocommerce_keys;
