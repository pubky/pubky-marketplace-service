//! The Shopify provider: a store connected through a custom app the merchant
//! created in their own Shopify Dev Dashboard, on the GraphQL Admin API
//! [`client::API_VERSION`].
//!
//! * **Install.** The merchant pastes the app's client id and secret into the
//!   Shop (the framework seals them per connection) and sets the app's App URL
//!   to the connection's launch URL. Shopify calls it with a signed query; the
//!   provider verifies the signature with that connection's own secret and
//!   redirects to Shopify's authorize page. The callback is verified the same
//!   way and the authorization code is exchanged for an offline access token,
//!   which the framework seals. A grant missing any of
//!   [`REQUIRED_SCOPES`] puts the connection in `needs_attention`.
//! * **Webhooks.** After an install the five operational topics are
//!   subscribed through the API ([`webhooks::SUBSCRIBED_TOPICS`]); the three
//!   privacy topics are configured in the app ([`webhooks::PRIVACY_TOPICS`]).
//!   Deliveries are verified with the connection's app secret.
//! * **Catalog, orders, refunds** are documented in [`catalog`] and
//!   [`orders`]. Every query and mutation is a `.graphql` file under
//!   `graphql/` that `tests/shopify_graphql_test.rs` validates against
//!   Shopify's published schema.

pub mod auth;
pub mod catalog;
pub mod client;
pub mod documents;
pub mod money;
pub mod orders;
pub mod webhooks;

use async_trait::async_trait;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::connectors::provider::{
    AppContext, AppKind, AuthorizeRequest, CallbackParams, CancelOrder, CatalogPage,
    CompleteRequest, ConnectCompleted, ConnectRequest, ConnectStart, ConnectionCheck,
    ConnectionContext, ConnectorProvider, CredentialSecret, ExternalOrder, InboundEvent, OrderPush,
    ProviderError, ProviderKind, RefundRecord, ResolvedStock, VerifiedWebhook, WebhookContext,
    WebhookHeaders,
};
use client::{Client, Origin};

/// What a connection needs from the store.
pub const REQUIRED_SCOPES: [&str; 5] = [
    "read_products",
    "read_inventory",
    "read_locations",
    "read_fulfillments",
    "write_orders",
];

pub struct ShopifyProvider {
    client: Client,
}

impl ShopifyProvider {
    pub fn new() -> Self {
        Self::with_origin(Origin::Shopify)
    }

    /// A provider that sends every request to `origin`; production uses
    /// [`Origin::Shopify`].
    pub fn with_origin(origin: Origin) -> Self {
        Self {
            client: Client::new(origin),
        }
    }
}

impl Default for ShopifyProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn token(connection: &ConnectionContext) -> Result<&str, ProviderError> {
    std::str::from_utf8(connection.credentials.expose())
        .map_err(|_| ProviderError::Invalid("the stored access token is unreadable".into()))
}

fn app_secret(app: &AppContext) -> &[u8] {
    app.client_secret.expose()
}

fn now_seconds() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Verifies a signed launch or callback query: signature, freshness and the
/// shop it names. Returns the canonical shop.
fn verify_signed_query(
    app: &AppContext,
    request: &CallbackParams,
) -> Result<String, ProviderError> {
    if !auth::verify_query_hmac(app_secret(app), &request.raw_query) {
        return Err(ProviderError::Invalid(
            "the request signature is wrong".into(),
        ));
    }
    if !auth::timestamp_fresh(&request.raw_query, now_seconds()) {
        return Err(ProviderError::Invalid("the request is too old".into()));
    }
    let shop = request
        .query
        .get("shop")
        .ok_or_else(|| ProviderError::Invalid("the request names no shop".into()))?;
    auth::normalize_shop(shop)
}

fn split_scopes(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(str::to_string)
        .collect()
}

#[async_trait]
impl ConnectorProvider for ShopifyProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Shopify
    }

    fn normalize_shop(&self, raw: &str) -> Result<String, ProviderError> {
        auth::normalize_shop(raw)
    }

    fn app_kind(&self) -> AppKind {
        AppKind::Custom
    }

    fn required_scopes(&self) -> Vec<String> {
        REQUIRED_SCOPES
            .iter()
            .map(|scope| scope.to_string())
            .collect()
    }

    async fn connect(&self, _request: &ConnectRequest) -> Result<ConnectStart, ProviderError> {
        Ok(ConnectStart {
            authorize_url: None,
        })
    }

    fn verify_launch(
        &self,
        app: &AppContext,
        launch: &CallbackParams,
    ) -> Result<String, ProviderError> {
        verify_signed_query(app, launch)
    }

    fn authorize_url(&self, request: &AuthorizeRequest) -> Result<String, ProviderError> {
        let shop = auth::normalize_shop(&request.shop)?;
        let mut url = url::Url::parse(&format!("https://{shop}/admin/oauth/authorize"))
            .map_err(|_| ProviderError::Invalid("not a store address".into()))?;
        url.query_pairs_mut()
            .append_pair("client_id", &request.app.client_id)
            .append_pair("scope", &REQUIRED_SCOPES.join(","))
            .append_pair("redirect_uri", &request.callback_url)
            .append_pair("state", &request.state);
        Ok(url.into())
    }

    fn callback_state(&self, callback: &CallbackParams) -> Result<String, ProviderError> {
        callback
            .query
            .get("state")
            .filter(|state| !state.is_empty())
            .cloned()
            .ok_or_else(|| ProviderError::Invalid("the callback carries no state".into()))
    }

    async fn complete_connect(
        &self,
        request: &CompleteRequest,
    ) -> Result<ConnectCompleted, ProviderError> {
        let app = request.app.as_ref().ok_or_else(|| {
            ProviderError::Invalid("the connection has no app credentials".into())
        })?;
        let shop = verify_signed_query(app, &request.callback)?;
        if shop != request.pending.shop {
            return Err(ProviderError::Invalid(
                "the callback is for a different store".into(),
            ));
        }
        let code = request
            .callback
            .query
            .get("code")
            .filter(|code| !code.is_empty())
            .ok_or_else(|| ProviderError::Invalid("the callback carries no code".into()))?;
        let response = self
            .client
            .http()
            .post(self.client.token_endpoint(&shop))
            .json(&json!({
                "client_id": app.client_id,
                "client_secret": String::from_utf8_lossy(app_secret(app)),
                "code": code,
            }))
            .send()
            .await
            .map_err(|error| {
                ProviderError::Unknown(format!("token exchange failed: {}", error.without_url()))
            })?;
        let status = response.status();
        if status.is_server_error() {
            return Err(ProviderError::Unknown(format!(
                "token exchange answered {status}"
            )));
        }
        if !status.is_success() {
            return Err(ProviderError::Rejected {
                code: "token_exchange_refused".into(),
                message: format!("Shopify refused the authorization code ({status})."),
            });
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| ProviderError::Unknown("token exchange returned no JSON".into()))?;
        let access_token = body
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| ProviderError::Unknown("token exchange returned no token".into()))?;
        Ok(ConnectCompleted {
            credentials: CredentialSecret::new(access_token.as_bytes().to_vec()),
            scopes: split_scopes(
                body.get("scope")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
        })
    }

    async fn register_webhooks(
        &self,
        connection: &ConnectionContext,
        webhook_url: &str,
    ) -> Result<(), ProviderError> {
        webhooks::register(
            &self.client,
            &connection.shop,
            token(connection)?,
            webhook_url,
        )
        .await
    }

    async fn disconnect(&self, connection: &ConnectionContext) -> Result<(), ProviderError> {
        let data = self
            .client
            .execute(
                &connection.shop,
                token(connection)?,
                documents::APP_UNINSTALL,
                json!({}),
            )
            .await?;
        match data
            .pointer("/appUninstall/userErrors")
            .and_then(Value::as_array)
        {
            Some(errors) if !errors.is_empty() => Err(ProviderError::Rejected {
                code: "uninstall_refused".into(),
                message: "Shopify did not uninstall the app.".into(),
            }),
            _ => Ok(()),
        }
    }

    async fn list_catalog(
        &self,
        connection: &ConnectionContext,
        page: Option<&str>,
    ) -> Result<CatalogPage, ProviderError> {
        catalog::page(&self.client, &connection.shop, token(connection)?, page).await
    }

    async fn resolve_inventory(
        &self,
        connection: &ConnectionContext,
        inventory_item_id: &str,
    ) -> Result<Option<ResolvedStock>, ProviderError> {
        catalog::resolve(
            &self.client,
            &connection.shop,
            token(connection)?,
            inventory_item_id,
        )
        .await
    }

    async fn check_connection(
        &self,
        connection: &ConnectionContext,
    ) -> Result<ConnectionCheck, ProviderError> {
        let token = token(connection)?;
        let data = self
            .client
            .execute(&connection.shop, token, documents::SHOP_OVERVIEW, json!({}))
            .await?;
        let granted_scopes = data
            .pointer("/currentAppInstallation/accessScopes")
            .and_then(Value::as_array)
            .map(|scopes| {
                scopes
                    .iter()
                    .filter_map(|scope| scope.get("handle")?.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let location_count = data
            .pointer("/locations/nodes")
            .and_then(Value::as_array)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter(|node| node.get("isActive").and_then(Value::as_bool) == Some(true))
                    .count() as i64
            })
            .unwrap_or_default();
        let catalog_readable =
            match catalog::page(&self.client, &connection.shop, token, None).await {
                Ok(_) => true,
                Err(ProviderError::Rejected { .. }) => false,
                Err(other) => return Err(other),
            };
        Ok(ConnectionCheck {
            shop_name: data
                .pointer("/shop/name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            granted_scopes,
            location_count,
            catalog_readable,
        })
    }

    async fn push_order(
        &self,
        connection: &ConnectionContext,
        order: &OrderPush,
    ) -> Result<ExternalOrder, ProviderError> {
        orders::push(&self.client, &connection.shop, token(connection)?, order).await
    }

    async fn find_pushed_order(
        &self,
        connection: &ConnectionContext,
        pubky_order_id: Uuid,
    ) -> Result<Option<ExternalOrder>, ProviderError> {
        orders::find(
            &self.client,
            &connection.shop,
            token(connection)?,
            pubky_order_id,
        )
        .await
    }

    async fn cancel_order(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        cancel: &CancelOrder,
    ) -> Result<(), ProviderError> {
        orders::cancel(
            &self.client,
            &connection.shop,
            token(connection)?,
            order,
            cancel,
        )
        .await
    }

    async fn record_refund(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        refund: &RefundRecord,
    ) -> Result<(), ProviderError> {
        orders::refund(
            &self.client,
            &connection.shop,
            token(connection)?,
            order,
            refund,
        )
        .await
    }

    fn webhook_shop(&self, headers: &WebhookHeaders, body: &[u8]) -> Result<String, ProviderError> {
        webhooks::shop_of(headers, body)
    }

    fn verify_webhook(
        &self,
        context: &WebhookContext,
        body: &[u8],
        headers: &WebhookHeaders,
    ) -> Result<VerifiedWebhook, ProviderError> {
        webhooks::verify(context.app.as_ref(), body, headers)
    }

    fn parse_inbound(
        &self,
        webhook: &VerifiedWebhook,
        body: &[u8],
    ) -> Result<InboundEvent, ProviderError> {
        webhooks::parse(webhook, body)
    }
}
