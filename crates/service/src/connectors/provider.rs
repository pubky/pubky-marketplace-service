//! The provider contract the Shopify and WooCommerce providers plug into.
//!
//! The framework (ledger, outbox consumer, routes, sealing) never knows a
//! store system. A provider turns the normalised types below into its own
//! API calls and back. Providers hold no state of their own: every call that
//! needs the merchant's secret receives it in a [`ConnectionContext`], opened
//! by the framework from `connector_credentials` for that one call, and every
//! call that needs the app's identity receives an [`AppContext`]: for a
//! Shopify custom app, the merchant's own client id and secret, sealed per
//! connection when they pasted them into the Shop.
//!
//! Outcome rules every implementation must honour (the ledger relies on
//! them):
//!
//! * [`ProviderError::Rejected`] and [`ProviderError::RateLimited`] mean the
//!   store did **not** apply the request. The framework may retry (rate
//!   limit) or quarantine (rejection) without looking the order up.
//! * [`ProviderError::Unknown`] means the request may or may not have been
//!   applied (timeout, connection reset, 5xx). Before the framework pushes
//!   the same order again it calls [`ConnectorProvider::find_pushed_order`],
//!   which must therefore be reliable (it may not answer from a search index
//!   that lags a create).
//! * `cancel_order` and `record_refund` are retried after an unknown outcome
//!   without a lookup, so they must be idempotent in the store (a
//!   deterministic idempotency key derived from the Pubky ids they receive,
//!   or a lookup of their own).
//! * No method may log, return or store the credential bytes or an app
//!   secret.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Shopify,
    Woocommerce,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProviderKind::Shopify => "shopify",
            ProviderKind::Woocommerce => "woocommerce",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "shopify" => Some(ProviderKind::Shopify),
            "woocommerce" => Some(ProviderKind::Woocommerce),
            _ => None,
        }
    }
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A secret (an OAuth token, an API key pair, an app secret) as bytes or
/// text. Opaque to the framework, never serialised, never printed, zeroed
/// when dropped.
pub struct CredentialSecret(Vec<u8>);

impl CredentialSecret {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for CredentialSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialSecret(<redacted>)")
    }
}

impl Drop for CredentialSecret {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// The kind of app a connection is installed through.
///
/// * `None`: no app; the provider authenticates with the store's own keys
///   (WooCommerce).
/// * `Custom`: the merchant created their own app (a Shopify Dev Dashboard
///   custom app) and gave the service its client id and secret. Both live
///   sealed per connection and verify that connection's signatures only.
/// * `Public`: Pubky's own public app, whose credentials will be the only
///   Shopify values in the deployment's environment
///   (`SHOPIFY_PUBLIC_APP_CLIENT_ID`, `SHOPIFY_PUBLIC_APP_CLIENT_SECRET`).
///   Reserved: the type, the column and the environment names exist, nothing
///   creates or reads a public-app connection yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppKind {
    None,
    Custom,
    Public,
}

impl AppKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AppKind::None => "none",
            AppKind::Custom => "custom",
            AppKind::Public => "public",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "none" => Some(AppKind::None),
            "custom" => Some(AppKind::Custom),
            "public" => Some(AppKind::Public),
            _ => None,
        }
    }
}

/// An app's client identity for one connection. The secret verifies launch,
/// OAuth and webhook signatures; it is opened from its sealed row for one call
/// and never printed.
pub struct AppContext {
    pub connection_id: Uuid,
    pub client_id: String,
    pub client_secret: CredentialSecret,
}

impl std::fmt::Debug for AppContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppContext")
            .field("connection_id", &self.connection_id)
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .finish()
    }
}

/// One connected store, with its opened credential, for the duration of a
/// provider call.
#[derive(Debug)]
pub struct ConnectionContext {
    pub connection_id: Uuid,
    pub seller_pubky: String,
    /// Canonical shop identifier ([`ConnectorProvider::normalize_shop`]).
    pub shop: String,
    pub scopes: Vec<String>,
    pub credentials: CredentialSecret,
    /// The provider app the store installed; `None` for a provider without
    /// apps (WooCommerce).
    pub app: Option<AppContext>,
}

/// Input of [`ConnectorProvider::connect`].
#[derive(Debug)]
pub struct ConnectRequest {
    pub connection_id: Uuid,
    pub seller_pubky: String,
    pub shop: String,
    /// The single-use nonce of the draft connection. A provider that sends the
    /// merchant straight to the store (WooCommerce) puts it in `user_id`; the
    /// framework stores only its hash.
    pub state: String,
    /// The connection's app, when its credentials were already saved.
    pub app: Option<AppContext>,
    pub callback_url: String,
    pub webhook_url: String,
    /// Provider-specific fields the Shop sent with the connect request.
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectStart {
    /// Where the merchant's browser goes to approve the connection. `None`
    /// when the merchant installs from the store's side (Shopify: the app's
    /// App URL starts the authorization).
    pub authorize_url: Option<String>,
}

/// What a launch, callback or other browser/server request carried, as
/// received.
#[derive(Debug, Default)]
pub struct CallbackParams {
    pub raw_query: String,
    pub query: BTreeMap<String, String>,
    pub headers: WebhookHeaders,
    pub body: Vec<u8>,
}

/// The draft connection a callback completes.
#[derive(Debug)]
pub struct PendingConnection {
    pub connection_id: Uuid,
    pub seller_pubky: String,
    pub shop: String,
    pub state: String,
}

#[derive(Debug)]
pub struct CompleteRequest {
    pub pending: PendingConnection,
    pub app: Option<AppContext>,
    pub callback: CallbackParams,
    pub webhook_url: String,
}

/// Input of [`ConnectorProvider::authorize_url`], after a verified launch.
#[derive(Debug)]
pub struct AuthorizeRequest {
    pub app: AppContext,
    pub shop: String,
    /// A fresh single-use value for the OAuth `state`.
    pub state: String,
    pub callback_url: String,
}

#[derive(Debug)]
pub struct ConnectCompleted {
    pub credentials: CredentialSecret,
    pub scopes: Vec<String>,
}

/// Headers of an inbound request, names lower-cased.
#[derive(Debug, Default, Clone)]
pub struct WebhookHeaders(BTreeMap<String, String>);

impl WebhookHeaders {
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        Self(
            pairs
                .into_iter()
                .map(|(name, value)| (name.as_ref().to_ascii_lowercase(), value.into()))
                .collect(),
        )
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(&name.to_ascii_lowercase()).map(String::as_str)
    }
}

/// What a provider may use to verify a webhook: the app (Shopify signs with
/// the connection's app secret) and, while the store is connected, its opened
/// credential (WooCommerce signs with a per-store secret). A store that
/// already disconnected has no credential, but Shopify's compliance webhooks
/// still arrive for it.
#[derive(Debug)]
pub struct WebhookContext {
    pub app: Option<AppContext>,
    pub connection: Option<ConnectionContext>,
}

/// A webhook whose signature the provider verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedWebhook {
    /// The provider's own delivery or event id. The inbound table is unique
    /// on it per connection, which is the webhook dedupe.
    pub event_id: String,
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Price {
    pub amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogOption {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogOptionValue {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogVariant {
    pub provider_variant_id: String,
    pub sku: Option<String>,
    pub title: Option<String>,
    pub option_values: Vec<CatalogOptionValue>,
    pub price: Price,
    /// Units the store can sell; `None` when the store does not track stock
    /// for this variant.
    pub stock: Option<i64>,
    /// The store sells the variant without a stock limit: it does not track
    /// its inventory, or keeps selling at zero (print-on-demand, made to
    /// order). Pubky then sells it as made to order, whatever `stock` says.
    #[serde(default)]
    pub made_to_order: bool,
    pub image_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogProduct {
    pub provider_product_id: String,
    pub title: String,
    pub description: Option<String>,
    pub url: Option<String>,
    pub options: Vec<CatalogOption>,
    pub image_urls: Vec<String>,
    pub variants: Vec<CatalogVariant>,
    /// The store's category names for the product, plain text.
    #[serde(default)]
    pub categories: Vec<String>,
    /// Descriptive attributes that do not pick a variant (material, brand),
    /// as name and values. The variant-picking ones are `options`.
    #[serde(default)]
    pub attributes: Vec<CatalogOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage {
    pub products: Vec<CatalogProduct>,
    /// Opaque cursor for the next page; `None` on the last page.
    pub next_page: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPushLine {
    pub provider_product_id: String,
    pub provider_variant_id: String,
    pub sku: Option<String>,
    pub title: String,
    pub quantity: i64,
    pub unit_price_minor: i64,
}

/// The delivery address a buyer chose to share with the store. Same fields as
/// the sealed delivery address. Held in memory for one push and never logged
/// (the `Debug` form hides it).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShippingAddress {
    pub name: String,
    pub line1: String,
    #[serde(default)]
    pub line2: String,
    pub city: String,
    #[serde(default)]
    pub region: String,
    #[serde(rename = "postalCode", alias = "postal_code")]
    pub postal_code: String,
    #[serde(rename = "countryCode", alias = "country_code")]
    pub country_code: String,
    /// The buyer's phone, when they gave one for the courier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
}

impl std::fmt::Debug for ShippingAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ShippingAddress(<hidden>)")
    }
}

/// A paid Pubky order as the store sees it. Carries no buyer identity. It
/// carries a delivery address only when the buyer opted in at checkout to
/// share it with this store (`shipping_address`); otherwise the store order
/// names only the Pubky order id and the merchant copies the address from
/// the Shop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPush {
    pub pubky_order_id: Uuid,
    pub currency: String,
    pub exponent: i32,
    pub lines: Vec<OrderPushLine>,
    pub subtotal_minor: i64,
    pub shipping_minor: i64,
    pub total_minor: i64,
    /// The rail the buyer paid on (`bitcoin`, `paykit`, `usdt`, `paypal`,
    /// `sandbox`, or `unknown`), for the store's manual payment record.
    pub payment_method: String,
    pub paid_at: DateTime<Utc>,
    pub shipping_address: Option<ShippingAddress>,
}

/// The store-side order a push created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalOrder {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrder {
    pub pubky_order_id: Uuid,
    pub reason: String,
}

/// A refund Pubky recorded, as the framework hands it to a provider: the
/// amount **still to mirror**.
///
/// Every amount is in the order currency's minor units (`order_currency`,
/// `order_exponent`), whatever rail the buyer paid on; the framework has
/// already converted a refund recorded in another unit (see
/// `connectors::refund`) and a provider never converts. The framework also
/// compares the recorded refund total with what the store already shows
/// refunded ([`ConnectorProvider::refunded_total`]) and calls
/// `record_refund` only with the difference, never taking the store's refunds
/// above its payment, so a repeat or a later, larger record never refunds the
/// same money twice and the provider does no arithmetic of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefundRecord {
    /// Deterministic per Pubky refund; the provider derives its idempotency
    /// key from it (and the amount).
    pub refund_id: String,
    pub pubky_order_id: Uuid,
    /// The amount to record in the store now: the recorded refund total minus
    /// what the store already shows refunded. Always positive.
    pub amount_minor: i64,
    /// What Pubky's books show refunded in total, in the same units.
    pub recorded_total_minor: i64,
    pub transaction_id: Option<String>,
    pub order_total_minor: i64,
    pub order_currency: String,
    pub order_exponent: i32,
    /// True when the recorded total covers the whole order
    /// (`recorded_total_minor >= order_total_minor`).
    pub full: bool,
    /// How the recorded amount relates to what the buyer paid (the
    /// conversion the order's own quote gave, when the refund was recorded in
    /// another unit), for the note a provider puts on the store refund.
    pub basis: Option<String>,
}

/// What a store shows for an order's payment and refunds, in one currency's
/// minor units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRefundTotals {
    /// The store order's currency code.
    pub currency: String,
    /// What the store order was paid.
    pub paid_minor: i64,
    /// What the store already shows refunded, in total.
    pub refunded_minor: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRef {
    pub external_order_id: String,
    /// Present when the store order itself names the Pubky order (a tag or a
    /// note attribute the push wrote).
    pub pubky_order_id: Option<Uuid>,
}

/// Shopify's mandatory privacy webhooks. The service keeps no store customer
/// data, so a data request or a customer redaction has nothing to return or
/// erase; a shop redaction deletes whatever connector data remains for the
/// store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceTopic {
    CustomerDataRequest,
    CustomerRedact,
    ShopRedact,
}

/// An amount a store refunded, in one currency's minor units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefundAmount {
    pub currency: String,
    pub amount_minor: i64,
}

/// What a provider wrote on a store refund it created for a Pubky refund, read
/// back from the store's refund webhook: the Pubky refund id
/// ([`RefundRecord::refund_id`]) and the amount it created, so an echo is
/// matched on identity **and** amount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PubkyRefundMark {
    pub refund_id: String,
    pub amount: RefundAmount,
}

/// A store variant's current sellable stock, found from an inventory item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStock {
    pub provider_product_id: String,
    pub provider_variant_id: String,
    /// Units the store can sell, summed over its locations; `None` when the
    /// store does not track this variant's stock.
    pub available: Option<i64>,
    /// The store sells the variant without a stock limit (see
    /// [`CatalogVariant::made_to_order`]).
    pub made_to_order: bool,
}

/// What a live check of a connection found, for the Shop's status page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionCheck {
    pub shop_name: String,
    pub granted_scopes: Vec<String>,
    pub location_count: i64,
    pub catalog_readable: bool,
}

/// What an inbound webhook means to Pubky.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InboundEvent {
    /// The store's sellable stock for a variant. Applied only to mapped
    /// variants, through `inventory.adjust`.
    StockLevel {
        provider_product_id: String,
        provider_variant_id: String,
        available: i64,
    },
    /// The store sells the variant without a stock limit now (see
    /// [`CatalogVariant::made_to_order`]). Applied only to mapped variants:
    /// the Pubky variant becomes made to order until a later `StockLevel`
    /// gives it a count again.
    MadeToOrder {
        provider_product_id: String,
        provider_variant_id: String,
    },
    /// A store inventory item changed. The webhook names the item, not the
    /// variant or the total, so the framework asks the provider
    /// ([`ConnectorProvider::resolve_inventory`]) which variant it is and
    /// what the store can now sell, then applies it like a `StockLevel`.
    InventoryChanged { inventory_item_id: String },
    /// A shipment for an order the push created. `tracking_number` is empty
    /// when the store fulfilled the order without one (carrier `Other` then):
    /// the Pubky order is still marked shipped, and the number is added later
    /// by a [`InboundEvent::TrackingUpdated`].
    Fulfillment {
        order: OrderRef,
        carrier: String,
        tracking_number: String,
    },
    /// Tracking added or changed in the store after the shipment. Syncs to the
    /// Pubky order through `fulfillment.update_tracking`.
    TrackingUpdated {
        order: OrderRef,
        carrier: String,
        tracking_number: String,
    },
    /// The merchant cancelled the order in the store. Raises a seller alert,
    /// never a refund.
    OrderCancelled {
        order: OrderRef,
        reason: Option<String>,
    },
    /// The merchant refunded the order in the store. Raises a seller alert,
    /// never a refund.
    Refund {
        order: OrderRef,
        amount_minor: Option<i64>,
        currency: Option<String>,
    },
    /// The same event for a provider that can say more: what the store
    /// refunded **per currency**, and the mark of a refund this service
    /// itself created there ([`PubkyRefundMark`]), which is what lets the
    /// framework tell an echo of its own mirrored refund from a refund the
    /// merchant made. Raises a seller alert, never a refund.
    StoreRefund {
        order: OrderRef,
        amounts: Vec<RefundAmount>,
        mark: Option<PubkyRefundMark>,
    },
    /// The merchant removed the app. The credential is deleted.
    AppUninstalled,
    /// One of the provider's mandatory privacy topics.
    Compliance { topic: ComplianceTopic },
    /// A topic the connector does not act on.
    Ignored { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The store refused the request; nothing was applied. `code` is a short
    /// machine value the Shop can show (`unmapped_variant`, `out_of_stock`,
    /// `validation`, ...).
    #[error("provider rejected the request: {code}")]
    Rejected { code: String, message: String },
    /// The store is throttling; nothing was applied.
    #[error("provider is rate limiting")]
    RateLimited { retry_after: Option<Duration> },
    /// The store no longer accepts the credential (revoked, expired,
    /// uninstalled).
    #[error("provider rejected the credential")]
    Unauthorized,
    /// The request may or may not have been applied.
    #[error("provider outcome unknown: {0}")]
    Unknown(String),
    /// The input to the provider was invalid (a bad shop, a bad signature, a
    /// body that does not parse). Nothing was sent.
    #[error("invalid input: {0}")]
    Invalid(String),
}

#[async_trait]
pub trait ConnectorProvider: Send + Sync + 'static {
    fn kind(&self) -> ProviderKind;

    /// Validates a merchant-entered shop (a `*.myshopify.com` domain, a site
    /// URL) and returns the canonical form used as the connection key and in
    /// the credential's associated data. Must never contain `|`.
    fn normalize_shop(&self, raw: &str) -> Result<String, ProviderError>;

    /// Whether a connection of this provider is installed through an app the
    /// merchant owns ([`AppKind::Custom`]) or none ([`AppKind::None`]).
    fn app_kind(&self) -> AppKind {
        AppKind::None
    }

    /// The scopes a connection needs. A grant missing any of them puts the
    /// connection in `needs_attention` with the missing names.
    fn required_scopes(&self) -> Vec<String> {
        Vec::new()
    }

    /// How the merchant installs the app on their store, for the Shop to show
    /// next to the connection's URLs: a JSON object (`method`, `where`,
    /// `app_url`, `redirect_url`, `note`) or `None` for a provider that needs
    /// no merchant-side install (WooCommerce sends the merchant straight to the
    /// store). The service builds no install link when the provider's own
    /// documentation does not confirm that one would install the app.
    fn install_guidance(&self, _app_url: &str, _redirect_url: &str) -> Option<serde_json::Value> {
        None
    }

    /// Starts a connection after the seller's connect request. Returns where
    /// the merchant approves it, or `None` when the install starts from the
    /// store (see [`Self::verify_launch`]).
    async fn connect(&self, request: &ConnectRequest) -> Result<ConnectStart, ProviderError>;

    /// Verifies the signature of the store's launch request to the
    /// connection's App URL (`GET /v1/connectors/shopify/c/{connection_id}/launch`)
    /// with the connection's own app secret, and returns the canonical shop
    /// it names (the framework then checks it is the connection's shop).
    /// Providers without an install-from-store flow keep the default.
    fn verify_launch(
        &self,
        _app: &AppContext,
        _launch: &CallbackParams,
    ) -> Result<String, ProviderError> {
        Err(ProviderError::Invalid(
            "this provider has no launch flow".into(),
        ))
    }

    /// The URL the merchant's browser is sent to after a verified launch that
    /// matched the connection.
    fn authorize_url(&self, _request: &AuthorizeRequest) -> Result<String, ProviderError> {
        Err(ProviderError::Invalid(
            "this provider has no launch flow".into(),
        ))
    }

    /// Extracts the single-use `state` value from a callback so the
    /// framework can find the draft connection. No authentication happens
    /// here.
    fn callback_state(&self, callback: &CallbackParams) -> Result<String, ProviderError>;

    /// Finishes a connection from the provider's callback: verifies it and
    /// obtains the credential. The framework seals and stores the result.
    async fn complete_connect(
        &self,
        request: &CompleteRequest,
    ) -> Result<ConnectCompleted, ProviderError>;

    /// Called once after an install completed and its credential is stored:
    /// subscribe the store's webhooks (stock, fulfilments, cancels, refunds,
    /// uninstall) to `webhook_url`, the connection's own webhooks URL. A
    /// failure leaves the connection in `needs_attention`; the next install
    /// runs it again. Providers that register nothing keep the default.
    async fn register_webhooks(
        &self,
        _connection: &ConnectionContext,
        _webhook_url: &str,
    ) -> Result<(), ProviderError> {
        Ok(())
    }

    /// Revokes the credential at the store, best effort. The framework
    /// deletes its own copy whatever this returns.
    async fn disconnect(&self, connection: &ConnectionContext) -> Result<(), ProviderError>;

    /// One page of the store's catalog, normalised.
    async fn list_catalog(
        &self,
        connection: &ConnectionContext,
        page: Option<&str>,
    ) -> Result<CatalogPage, ProviderError>;

    /// The variant, and its total sellable stock, behind a store inventory
    /// item (see [`InboundEvent::InventoryChanged`]). `Ok(None)` when the
    /// item is not a product variant. Providers whose stock webhooks already
    /// name the variant keep the default.
    async fn resolve_inventory(
        &self,
        _connection: &ConnectionContext,
        _inventory_item_id: &str,
    ) -> Result<Option<ResolvedStock>, ProviderError> {
        Ok(None)
    }

    /// A live look at the connection for the Shop's status page: the store's
    /// name, the scopes it granted, how many locations it has and whether a
    /// catalog read works. Providers without one keep the default.
    async fn check_connection(
        &self,
        _connection: &ConnectionContext,
    ) -> Result<ConnectionCheck, ProviderError> {
        Err(ProviderError::Invalid(
            "this provider has no connection check".into(),
        ))
    }

    /// Creates the store order. Called only after the ledger row is
    /// `sending`.
    async fn push_order(
        &self,
        connection: &ConnectionContext,
        order: &OrderPush,
    ) -> Result<ExternalOrder, ProviderError>;

    /// Called once after a push created the store order: a provider that can
    /// read the order back says whether it is what the buyer paid (the total,
    /// and no tax the store added). `Ok(Some(message))` is a sentence for the
    /// seller, who gets an alert; the order stays created. Providers that do
    /// not check keep the default. An error is logged and ignored: the order
    /// exists either way.
    async fn verify_pushed_order(
        &self,
        _connection: &ConnectionContext,
        _created: &ExternalOrder,
        _pushed: &OrderPush,
    ) -> Result<Option<String>, ProviderError> {
        Ok(None)
    }

    /// Looks up an order a push may already have created, by the Pubky order
    /// id the push stamped on it.
    async fn find_pushed_order(
        &self,
        connection: &ConnectionContext,
        pubky_order_id: Uuid,
    ) -> Result<Option<ExternalOrder>, ProviderError>;

    /// Cancels a pushed order. The money never moved through the store, so
    /// no store refund is made.
    async fn cancel_order(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        cancel: &CancelOrder,
    ) -> Result<(), ProviderError>;

    /// What the store shows for the order's payment and refunds, read fresh
    /// from the store (a refund the merchant made there counts). The
    /// framework works out how much of a Pubky refund is still to mirror from
    /// this; it never asks a provider to refund more than the payment.
    async fn refunded_total(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
    ) -> Result<StoreRefundTotals, ProviderError>;

    /// Records a refund Pubky made so the store's books match. `refund`
    /// carries only the amount still to mirror (see [`RefundRecord`]).
    async fn record_refund(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        refund: &RefundRecord,
    ) -> Result<(), ProviderError>;

    /// Reads which shop an inbound webhook claims to come from, so the
    /// framework can find that connection. The claim is not trusted until
    /// [`Self::verify_webhook`] accepts the request.
    fn webhook_shop(&self, headers: &WebhookHeaders, body: &[u8]) -> Result<String, ProviderError>;

    /// Verifies the webhook's signature over the raw body, in constant time.
    fn verify_webhook(
        &self,
        context: &WebhookContext,
        body: &[u8],
        headers: &WebhookHeaders,
    ) -> Result<VerifiedWebhook, ProviderError>;

    /// Turns a verified webhook into what it means to Pubky.
    fn parse_inbound(
        &self,
        webhook: &VerifiedWebhook,
        body: &[u8],
    ) -> Result<InboundEvent, ProviderError>;
}

/// The providers a process has configured. Empty when none is configured:
/// every connector route then answers `provider_unavailable`.
#[derive(Clone, Default)]
pub struct ProviderRegistry {
    providers: BTreeMap<ProviderKind, Arc<dyn ConnectorProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, provider: Arc<dyn ConnectorProvider>) -> Self {
        self.providers.insert(provider.kind(), provider);
        self
    }

    pub fn get(&self, kind: ProviderKind) -> Option<&Arc<dyn ConnectorProvider>> {
        self.providers.get(&kind)
    }

    pub fn kinds(&self) -> Vec<ProviderKind> {
        self.providers.keys().copied().collect()
    }
}
