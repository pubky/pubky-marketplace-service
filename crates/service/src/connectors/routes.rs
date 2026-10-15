//! The `/v1/connectors` routes.
//!
//! Seller routes sit behind `auth::require_session`, the same layer as the
//! other seller routes, and act only on connections the session's pubky
//! owns: another seller's connection id answers exactly like an unknown id.
//! The provider routes (Shopify launch and OAuth callback, WooCommerce
//! callback, webhooks) and the public connector status are unauthenticated by
//! nature; their authenticity is the provider's signature (launch, webhook)
//! or the single-use `state` issued when the seller asked to connect
//! (callbacks). The paths are fixed because merchants' provider apps point at
//! them:
//!
//! ```text
//! GET  /v1/connectors/shopify/apps/{app_key}/launch
//! GET  /v1/connectors/shopify/apps/{app_key}/oauth/callback
//! POST /v1/connectors/woocommerce/oauth/callback
//! POST /v1/connectors/webhooks/shopify/{app_key}
//! POST /v1/connectors/webhooks/woocommerce
//! ```
//!
//! No response carries a credential, and no handler logs one: the plaintext
//! exists only inside a [`provider::ConnectionContext`] handed to a provider
//! call.

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

use super::apps::valid_app_key;
use super::config::{CONNECT_STATE_TTL_SECONDS, MAX_MAPPINGS_PER_REQUEST, MAX_SELECTED_PRODUCTS};
use super::inbound::{self, Recorded};
use super::ledger::{self, reason_is_informational, PushView, RetryOutcome};
use super::provider::{
    AuthorizeRequest, CallbackParams, CompleteRequest, ConnectRequest, ConnectorProvider,
    PendingConnection, ProviderError, ProviderKind, WebhookContext, WebhookHeaders,
};
use super::recipient;
use super::store::{
    self, AlertRow, CompleteError, ConnectError, ConnectionRow, MappingError, MappingRow,
    OpenError, SelectionError,
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
    if matches!(row.status.as_str(), "connected" | "error") {
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

fn unknown_app() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "unknown_app",
        "That app is not configured on this deployment.",
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

fn connection_json(row: &ConnectionRow, mappings: i64, pushes: &BTreeMap<String, i64>) -> Value {
    let count = |name: &str| pushes.get(name).copied().unwrap_or(0);
    json!({
        "id": row.id,
        "provider": row.provider,
        "domain": row.shop,
        "app_key": row.app_key,
        "status": row.status,
        "scopes": row.scopes,
        "expires_at": (row.status == "pending")
            .then(|| row.connect_expires_at.map(ts))
            .flatten(),
        "connected_at": row.connected_at.map(ts),
        "created_at": ts(row.created_at),
        "updated_at": ts(row.updated_at),
        "last_error": row.last_error.as_ref().map(|message| json!({
            "message": message,
            "at": row.last_error_at.map(ts),
        })),
        "mapping_count": mappings,
        "pushes": {
            "pending": count("pending"),
            "sending": count("sending"),
            "created": count("created"),
            "quarantined": count("quarantined"),
        },
    })
}

async fn connection_view(state: &AppState, row: &ConnectionRow) -> Result<Value, sqlx::Error> {
    let mappings: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM connector_variant_map WHERE connection_id = $1")
            .bind(row.id)
            .fetch_one(&state.pool)
            .await?;
    let pushes = ledger::push_counts(&state.pool, row.id).await?;
    Ok(connection_json(row, mappings, &pushes))
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
    if let Err(response) = runtime(&state) {
        return response;
    }
    let rows = match store::list_for_seller(&state.pool, &actor.0).await {
        Ok(rows) => rows,
        Err(cause) => return internal("connection list", &cause),
    };
    let mut connections = Vec::with_capacity(rows.len());
    for row in &rows {
        match connection_view(&state, row).await {
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
    #[serde(default)]
    app_key: Option<String>,
    #[serde(default)]
    params: Option<Value>,
}

/// `POST /v1/connectors`: the signed-in seller asks to connect a store. This
/// creates the pending connect (a one-time nonce, 30 minutes). WooCommerce's
/// answer carries the `authorize_url` to open; a Shopify store is installed
/// from its own admin (the app's App URL), so `authorize_url` is `null` and
/// the launch matches this pending connect by domain.
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
    let params = request.params.unwrap_or_else(|| json!({}));
    if !params.is_object() || params.to_string().len() > 4096 {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            "params must be a small JSON object.",
        );
    }
    let app_key = match request.app_key.as_deref() {
        None => None,
        Some(key) if valid_app_key(key) && runtime.apps.context(kind, key).is_some() => Some(key),
        Some(_) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unknown_app",
                "That app is not configured for this provider.",
            )
        }
    };
    let shop = match provider.normalize_shop(&request.domain) {
        Ok(shop) if !shop.is_empty() && shop.len() <= 255 && !shop.contains(['|', '\n', '\r']) => {
            shop
        }
        _ => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_domain",
                "The store address is not valid for this provider.",
            )
        }
    };
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let connect_state = URL_SAFE_NO_PAD.encode(nonce);
    let now = state.clock.now();
    let row = match store::begin_connect(
        &state.pool,
        now,
        &actor.0,
        kind,
        &shop,
        app_key,
        &store::hash_state(&connect_state),
        now + chrono::Duration::seconds(CONNECT_STATE_TTL_SECONDS),
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
    let connect = ConnectRequest {
        connection_id: row.id,
        seller_pubky: actor.0.clone(),
        shop: shop.clone(),
        state: connect_state,
        app: app_key.and_then(|key| runtime.apps.context(kind, key)),
        callback_url: runtime.settings.callback_url(kind.as_str(), app_key),
        webhook_url: runtime.settings.webhook_url(kind.as_str(), app_key),
        params,
    };
    let started = match call(runtime, provider.connect(&connect)).await {
        Ok(started) => started,
        Err(failure) => return provider_failure(StatusCode::BAD_GATEWAY, failure),
    };
    let view = match connection_view(&state, &row).await {
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
    match store::disconnect(&state.pool, row.id, state.clock.now()).await {
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

/// `POST /v1/connectors/webhooks/{provider}` (WooCommerce) and
/// `POST /v1/connectors/webhooks/{provider}/{app_key}` (Shopify): a store's
/// webhook. Unknown store, bad signature and unreadable credential all
/// answer the same 401, so the route does not reveal which stores are
/// connected. A store that already disconnected is still recognised so its
/// privacy webhooks are honoured; the provider verifies those with the app
/// secret alone.
pub async fn receive_webhook(
    State(state): State<AppState>,
    Path(provider_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    webhook_inner(&state, &provider_name, None, &headers, &body).await
}

pub async fn receive_app_webhook(
    State(state): State<AppState>,
    Path((provider_name, app_key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    webhook_inner(&state, &provider_name, Some(app_key), &headers, &body).await
}

async fn webhook_inner(
    state: &AppState,
    provider_name: &str,
    app_key: Option<String>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response {
    let runtime = match runtime(state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let Some(kind) = ProviderKind::parse(provider_name) else {
        return provider_unavailable();
    };
    let Some(provider) = runtime.providers.get(kind) else {
        return provider_unavailable();
    };
    let app = match app_key.as_deref() {
        None => None,
        Some(key) => match runtime.apps.context(kind, key) {
            Some(app) => Some(app),
            None => return unknown_app(),
        },
    };
    let headers = headers_of(headers);
    let Ok(shop) = provider.webhook_shop(&headers, body) else {
        return invalid_webhook();
    };
    let live_connection = match store::live_for_shop(&state.pool, kind, &shop).await {
        Ok(connection) => connection,
        Err(cause) => return internal("webhook connection lookup", &cause),
    };
    let connection = match live_connection {
        Some(connection) => Some(connection),
        None => match store::latest_for_shop(&state.pool, kind, &shop).await {
            Ok(connection) => connection,
            Err(cause) => return internal("webhook connection lookup", &cause),
        },
    };
    if let (Some(connection), Some(key)) = (&connection, app_key.as_deref()) {
        if connection
            .app_key
            .as_deref()
            .is_some_and(|bound| bound != key)
        {
            return invalid_webhook();
        }
    }
    let context = match &connection {
        Some(connection) if matches!(connection.status.as_str(), "connected" | "error") => {
            match runtime.open(&state.pool, connection).await {
                Ok(mut opened) => {
                    opened.app = None;
                    Some(opened)
                }
                Err(OpenError::Unavailable) => None,
                Err(OpenError::Unreadable) => {
                    tracing::error!(
                        connection_id = %connection.id,
                        "connector credential is unreadable; webhook refused"
                    );
                    return invalid_webhook();
                }
                Err(OpenError::Database(cause)) => {
                    return internal("webhook credential read", &cause)
                }
            }
        }
        _ => None,
    };
    let webhook_context = WebhookContext {
        app,
        connection: context,
    };
    let verified = match provider.verify_webhook(&webhook_context, body, &headers) {
        Ok(verified) => verified,
        Err(_) => return invalid_webhook(),
    };
    let Some(connection) = connection else {
        return respond(
            StatusCode::OK,
            json!({ "schema_version": 1, "status": "ignored" }),
        );
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
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    if redirect {
        browser_outcome(
            runtime,
            &[("status", "error"), ("code", code)],
            status,
            code,
            message,
        )
    } else {
        error(status, code, message)
    }
}

/// `GET /v1/connectors/shopify/apps/{app_key}/launch`: the app's App URL,
/// where the store sends the merchant after Install. Verifies the launch
/// signature with the app's secret, matches the pending connect for that
/// domain, and sends the merchant on to authorize. An install with no pending
/// connect goes to the Shop's connect page with the domain prefilled.
pub async fn shopify_launch(
    State(state): State<AppState>,
    Path(app_key): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let runtime = match runtime(&state) {
        Ok(runtime) => runtime,
        Err(response) => return response,
    };
    let kind = ProviderKind::Shopify;
    let Some(provider) = runtime.providers.get(kind) else {
        return provider_unavailable();
    };
    let Some(app) = runtime.apps.context(kind, &app_key) else {
        return unknown_app();
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
    let now = state.clock.now();
    let pending = match store::pending_for_shop(&state.pool, kind, &shop, now).await {
        Ok(pending) => pending,
        Err(cause) => return internal("launch lookup", &cause),
    };
    let pending = match pending.as_slice() {
        [] => {
            return browser_outcome(
                runtime,
                &[
                    ("status", "install_unclaimed"),
                    ("provider", kind.as_str()),
                    ("domain", &shop),
                ],
                StatusCode::NOT_FOUND,
                "no_pending_connect",
                "No seller asked to connect this store. Connect it from the Shop first.",
            )
        }
        [only] => only,
        _ => {
            return browser_error(
                runtime,
                true,
                StatusCode::CONFLICT,
                "ambiguous_connect",
                "More than one seller asked to connect this store.",
            )
        }
    };
    if pending
        .app_key
        .as_deref()
        .is_some_and(|bound| bound != app_key)
    {
        return browser_error(
            runtime,
            true,
            StatusCode::CONFLICT,
            "app_mismatch",
            "This connection was started for a different app.",
        );
    }
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let authorize_state = URL_SAFE_NO_PAD.encode(nonce);
    match store::bind_launch(
        &state.pool,
        pending.id,
        &app_key,
        &store::hash_state(&authorize_state),
        now,
    )
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return browser_error(
                runtime,
                true,
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
        callback_url: runtime.settings.callback_url(kind.as_str(), Some(&app_key)),
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

/// `GET /v1/connectors/shopify/apps/{app_key}/oauth/callback`.
pub async fn shopify_callback(
    State(state): State<AppState>,
    Path(app_key): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let params = callback_params(raw_query, &headers, &Bytes::new());
    complete_callback(&state, ProviderKind::Shopify, Some(app_key), true, params).await
}

/// `POST /v1/connectors/woocommerce/oauth/callback`: WooCommerce posts the
/// store's keys here, with `user_id` set to the pending connect's nonce.
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
    app_key: Option<String>,
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
    let app = match app_key.as_deref() {
        None => None,
        Some(key) => match runtime.apps.context(kind, key) {
            Some(app) => Some(app),
            None => return unknown_app(),
        },
    };
    let invalid = || {
        browser_error(
            runtime,
            redirect,
            StatusCode::BAD_REQUEST,
            "invalid_state",
            "The connection request is not valid or has expired.",
        )
    };
    let Ok(connect_state) = provider.callback_state(&params) else {
        return invalid();
    };
    let now = state.clock.now();
    let pending = match store::pending_by_state(&state.pool, kind, &connect_state, now).await {
        Ok(Some(pending)) => pending,
        Ok(None) => return invalid(),
        Err(cause) => return internal("callback lookup", &cause),
    };
    if pending.app_key.as_deref() != app_key.as_deref() {
        return invalid();
    }
    let request = CompleteRequest {
        pending: PendingConnection {
            connection_id: pending.id,
            seller_pubky: pending.seller_pubky.clone(),
            shop: pending.shop.clone(),
            state: connect_state,
        },
        app,
        callback: params,
        webhook_url: runtime
            .settings
            .webhook_url(kind.as_str(), app_key.as_deref()),
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
                status,
                code,
                "The store did not complete the connection.",
            );
        }
    };
    let recipient_key = recipient::generate();
    match store::complete_connect(
        &state.pool,
        &runtime.keys,
        state.clock.now(),
        &pending,
        &completed.credentials,
        &completed.scopes,
        &recipient_key,
    )
    .await
    {
        Ok(connected) => {
            if redirect && runtime.settings.return_url.is_some() {
                return browser_outcome(
                    runtime,
                    &[
                        ("status", "connected"),
                        ("connector", &connected.id.to_string()),
                    ],
                    StatusCode::OK,
                    "connected",
                    "",
                );
            }
            connected_json(state, &connected).await
        }
        Err(CompleteError::ShopInUse) => browser_error(
            runtime,
            redirect,
            StatusCode::CONFLICT,
            "shop_in_use",
            "Another seller already has this store connected.",
        ),
        Err(CompleteError::Stale) => invalid(),
        Err(CompleteError::Database(cause)) => internal("connection completion", &cause),
    }
}

async fn connected_json(state: &AppState, connected: &ConnectionRow) -> Response {
    match connection_view(state, connected).await {
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
