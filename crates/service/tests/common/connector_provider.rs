//! A real, in-memory implementation of `ConnectorProvider` for the connector
//! tests, plus the harness around it. Lives in the test crate only: the
//! service ships no fake provider.
//!
//! The "store" is a `TestStore` the tests hold a handle to. It records every
//! call, keeps the orders a push created, and can be scripted to fail the
//! way a real store does: create the order and then time out, time out
//! without creating, throttle, refuse, or reject the credential. A gate lets
//! a test hold a push open mid-flight to interleave a second worker.
//!
//! Two kinds are registered, shaped like the real ones:
//! * the Shopify-kind provider installs from the store (launch, then OAuth
//!   callback) and signs webhooks with the app's client secret;
//! * the WooCommerce-kind provider sends the merchant to the store with the
//!   pending-connect nonce as `user_id`, is called back with a POST, and
//!   signs webhooks with a per-store secret held in the credential.
//!
//! Wire formats of the fake store:
//! * credential blob: JSON `{"token": ..., "webhook_secret": ...}`;
//! * launch: query `shop`, `hmac` = hex HMAC-SHA256 of `shop=<shop>` under
//!   the app's client secret;
//! * webhook headers `x-test-shop`, `x-test-event-id`, `x-test-topic`, and
//!   `x-test-signature` = hex HMAC-SHA256 of the raw body;
//! * callback: query `state` and `code` (`good` completes the connection), or
//!   a JSON body `{"user_id": <nonce>, "consumer_key": ...}`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use marketplace_service::connectors::keys::ConnectorKeys;
use marketplace_service::connectors::provider::*;
use marketplace_service::connectors::{AppRegistry, ConnectorRuntime, ConnectorSettings};
use marketplace_service::http::build_router;
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use tokio::sync::Notify;
use uuid::Uuid;

use super::{TestActor, TestApp};

pub const TEST_TOKEN: &str = "shpat_test_credential_value_0123456789";
pub const TEST_WEBHOOK_SECRET: &str = "whsec_test_webhook_secret_value";
pub const TEST_APP_SECRET: &str = "shpss_test_app_client_secret_value";
pub const CONNECTOR_KEY: &str = "5555555555555555555555555555555555555555555555555555555555555555";
pub const CONNECTOR_PREVIOUS_KEY: &str =
    "6666666666666666666666666666666666666666666666666666666666666666";
pub const SHOP: &str = "dezentral.test-store.example";
pub const APP_KEY: &str = "john";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteOrder {
    pub id: String,
    pub pubky_order_id: Uuid,
    pub line_count: usize,
    pub total_minor: i64,
    pub payment_method: String,
    pub cancelled: bool,
    pub refunds: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum PushScript {
    CreateThenUnknown,
    UnknownWithoutCreate,
    RateLimited(Option<Duration>),
    Rejected(&'static str),
    Unauthorized,
}

#[derive(Default)]
pub struct Gate {
    pub entered: Notify,
    pub release: Notify,
}

#[derive(Default)]
pub struct TestStore {
    pub orders: Mutex<Vec<RemoteOrder>>,
    pub push_calls: AtomicUsize,
    pub find_calls: AtomicUsize,
    pub cancel_calls: AtomicUsize,
    pub refund_calls: AtomicUsize,
    pub catalog_calls: AtomicUsize,
    pub disconnect_calls: AtomicUsize,
    pub script: Mutex<VecDeque<PushScript>>,
    pub gate: Mutex<Option<Arc<Gate>>>,
    pub push_delay: Mutex<Duration>,
    pub catalog: Mutex<Vec<CatalogProduct>>,
    pub reject_credentials: AtomicBool,
    pub cancel_unknown_once: AtomicBool,
    pub last_order_push: Mutex<Option<OrderPush>>,
    pub last_refund: Mutex<Option<RefundRecord>>,
    pub last_app: Mutex<Option<String>>,
}

impl TestStore {
    pub fn remote_orders(&self) -> Vec<RemoteOrder> {
        self.orders.lock().expect("orders lock").clone()
    }

    pub fn pushes(&self) -> usize {
        self.push_calls.load(Ordering::SeqCst)
    }

    pub fn script(&self, step: PushScript) {
        self.script.lock().expect("script lock").push_back(step);
    }

    pub fn open_gate(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.gate.lock().expect("gate lock") = Some(gate.clone());
        gate
    }

    /// An order that exists in the store as if an earlier, unacknowledged
    /// push had created it.
    pub fn plant_order(&self, pubky_order_id: Uuid, external_id: &str) {
        self.orders.lock().expect("orders lock").push(RemoteOrder {
            id: external_id.to_string(),
            pubky_order_id,
            line_count: 1,
            total_minor: 0,
            payment_method: "planted".to_string(),
            cancelled: false,
            refunds: Vec::new(),
        });
    }

    pub fn set_catalog(&self, products: Vec<CatalogProduct>) {
        *self.catalog.lock().expect("catalog lock") = products;
    }

    fn create(&self, order: &OrderPush) -> ExternalOrder {
        let mut orders = self.orders.lock().expect("orders lock");
        let id = format!("ext-{}", orders.len() + 1);
        orders.push(RemoteOrder {
            id: id.clone(),
            pubky_order_id: order.pubky_order_id,
            line_count: order.lines.len(),
            total_minor: order.total_minor,
            payment_method: order.payment_method.clone(),
            cancelled: false,
            refunds: Vec::new(),
        });
        ExternalOrder { id }
    }
}

pub struct TestProvider {
    kind: ProviderKind,
    pub store: Arc<TestStore>,
}

impl TestProvider {
    pub fn new(kind: ProviderKind) -> Self {
        Self {
            kind,
            store: Arc::new(TestStore::default()),
        }
    }
}

#[derive(serde::Deserialize)]
struct Blob {
    token: String,
    webhook_secret: String,
}

fn blob(connection: &ConnectionContext) -> Result<Blob, ProviderError> {
    serde_json::from_slice(connection.credentials.expose())
        .map_err(|_| ProviderError::Invalid("unreadable credential".into()))
}

pub fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

pub fn launch_query(shop: &str, secret: &str) -> String {
    format!(
        "shop={shop}&hmac={}",
        sign(secret, format!("shop={shop}").as_bytes())
    )
}

fn verify_signature(secret: &[u8], body: &[u8], presented: &[u8]) -> Result<(), ProviderError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(body);
    mac.verify_slice(presented)
        .map_err(|_| ProviderError::Invalid("bad signature".into()))
}

#[async_trait]
impl ConnectorProvider for TestProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn normalize_shop(&self, raw: &str) -> Result<String, ProviderError> {
        let shop = raw.trim().to_ascii_lowercase();
        let valid = (3..=100).contains(&shop.len())
            && shop
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'));
        if valid {
            Ok(shop)
        } else {
            Err(ProviderError::Invalid("not a valid shop".into()))
        }
    }

    async fn connect(&self, request: &ConnectRequest) -> Result<ConnectStart, ProviderError> {
        Ok(ConnectStart {
            authorize_url: (self.kind == ProviderKind::Woocommerce).then(|| {
                format!(
                    "https://{}/wc-auth/v1/authorize?user_id={}&callback_url={}",
                    request.shop, request.state, request.callback_url
                )
            }),
        })
    }

    fn verify_launch(
        &self,
        app: &AppContext,
        launch: &CallbackParams,
    ) -> Result<String, ProviderError> {
        let shop = launch
            .query
            .get("shop")
            .ok_or_else(|| ProviderError::Invalid("no shop".into()))?;
        let presented = launch
            .query
            .get("hmac")
            .and_then(|value| hex::decode(value).ok())
            .ok_or_else(|| ProviderError::Invalid("no hmac".into()))?;
        verify_signature(
            app.client_secret.expose(),
            format!("shop={shop}").as_bytes(),
            &presented,
        )?;
        self.normalize_shop(shop)
    }

    fn authorize_url(&self, request: &AuthorizeRequest) -> Result<String, ProviderError> {
        *self.store.last_app.lock().expect("app lock") = Some(request.app.app_key.clone());
        Ok(format!(
            "https://{}/admin/oauth/authorize?client_id={}&state={}&redirect_uri={}",
            request.shop, request.app.client_id, request.state, request.callback_url
        ))
    }

    fn callback_state(&self, callback: &CallbackParams) -> Result<String, ProviderError> {
        if let Some(state) = callback.query.get("state") {
            return Ok(state.clone());
        }
        serde_json::from_slice::<Value>(&callback.body)
            .ok()
            .and_then(|body| {
                body.get("user_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| ProviderError::Invalid("no state".into()))
    }

    async fn complete_connect(
        &self,
        request: &CompleteRequest,
    ) -> Result<ConnectCompleted, ProviderError> {
        let by_code = request.callback.query.get("code").map(String::as_str) == Some("good");
        let by_keys = serde_json::from_slice::<Value>(&request.callback.body)
            .ok()
            .is_some_and(|body| body.get("consumer_key").is_some());
        if !by_code && !by_keys {
            return Err(ProviderError::Rejected {
                code: "bad_code".into(),
                message: "the code was refused".into(),
            });
        }
        *self.store.last_app.lock().expect("app lock") =
            request.app.as_ref().map(|app| app.app_key.clone());
        Ok(ConnectCompleted {
            credentials: CredentialSecret::new(
                serde_json::to_vec(&json!({
                    "token": TEST_TOKEN,
                    "webhook_secret": TEST_WEBHOOK_SECRET,
                }))
                .expect("blob serializes"),
            ),
            scopes: vec!["read_products".into(), "write_orders".into()],
        })
    }

    async fn disconnect(&self, _connection: &ConnectionContext) -> Result<(), ProviderError> {
        self.store.disconnect_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn list_catalog(
        &self,
        connection: &ConnectionContext,
        page: Option<&str>,
    ) -> Result<CatalogPage, ProviderError> {
        self.store.catalog_calls.fetch_add(1, Ordering::SeqCst);
        blob(connection)?;
        if self.store.reject_credentials.load(Ordering::SeqCst) {
            return Err(ProviderError::Unauthorized);
        }
        let products = self.store.catalog.lock().expect("catalog lock").clone();
        let index: usize = page.and_then(|page| page.parse().ok()).unwrap_or(0);
        let next_page = (index + 1 < products.len()).then(|| (index + 1).to_string());
        Ok(CatalogPage {
            products: products.get(index).cloned().into_iter().collect(),
            next_page,
        })
    }

    async fn push_order(
        &self,
        connection: &ConnectionContext,
        order: &OrderPush,
    ) -> Result<ExternalOrder, ProviderError> {
        self.store.push_calls.fetch_add(1, Ordering::SeqCst);
        *self.store.last_order_push.lock().expect("push lock") = Some(order.clone());
        *self.store.last_app.lock().expect("app lock") =
            connection.app.as_ref().map(|app| app.app_key.clone());
        blob(connection)?;
        let gate = self.store.gate.lock().expect("gate lock").clone();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let delay = *self.store.push_delay.lock().expect("delay lock");
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let step = self.store.script.lock().expect("script lock").pop_front();
        match step {
            None => Ok(self.store.create(order)),
            Some(PushScript::CreateThenUnknown) => {
                self.store.create(order);
                Err(ProviderError::Unknown(
                    "connection reset after write".into(),
                ))
            }
            Some(PushScript::UnknownWithoutCreate) => {
                Err(ProviderError::Unknown("connection refused".into()))
            }
            Some(PushScript::RateLimited(retry_after)) => {
                Err(ProviderError::RateLimited { retry_after })
            }
            Some(PushScript::Rejected(code)) => Err(ProviderError::Rejected {
                code: code.into(),
                message: "the store refused the order".into(),
            }),
            Some(PushScript::Unauthorized) => Err(ProviderError::Unauthorized),
        }
    }

    async fn find_pushed_order(
        &self,
        connection: &ConnectionContext,
        pubky_order_id: Uuid,
    ) -> Result<Option<ExternalOrder>, ProviderError> {
        self.store.find_calls.fetch_add(1, Ordering::SeqCst);
        blob(connection)?;
        Ok(self
            .store
            .orders
            .lock()
            .expect("orders lock")
            .iter()
            .find(|order| order.pubky_order_id == pubky_order_id)
            .map(|order| ExternalOrder {
                id: order.id.clone(),
            }))
    }

    async fn cancel_order(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        _cancel: &CancelOrder,
    ) -> Result<(), ProviderError> {
        self.store.cancel_calls.fetch_add(1, Ordering::SeqCst);
        blob(connection)?;
        if self.store.cancel_unknown_once.swap(false, Ordering::SeqCst) {
            return Err(ProviderError::Unknown("timeout".into()));
        }
        let mut orders = self.store.orders.lock().expect("orders lock");
        if let Some(remote) = orders.iter_mut().find(|remote| remote.id == order.id) {
            remote.cancelled = true;
        }
        Ok(())
    }

    async fn record_refund(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        refund: &RefundRecord,
    ) -> Result<(), ProviderError> {
        self.store.refund_calls.fetch_add(1, Ordering::SeqCst);
        blob(connection)?;
        *self.store.last_refund.lock().expect("refund lock") = Some(refund.clone());
        let mut orders = self.store.orders.lock().expect("orders lock");
        if let Some(remote) = orders.iter_mut().find(|remote| remote.id == order.id) {
            if !remote.refunds.contains(&refund.refund_id) {
                remote.refunds.push(refund.refund_id.clone());
            }
        }
        Ok(())
    }

    fn webhook_shop(
        &self,
        headers: &WebhookHeaders,
        _body: &[u8],
    ) -> Result<String, ProviderError> {
        headers
            .get("x-test-shop")
            .map(str::to_string)
            .ok_or_else(|| ProviderError::Invalid("no shop header".into()))
    }

    fn verify_webhook(
        &self,
        context: &WebhookContext,
        body: &[u8],
        headers: &WebhookHeaders,
    ) -> Result<VerifiedWebhook, ProviderError> {
        let secret: Vec<u8> = match (&context.app, &context.connection) {
            (Some(app), _) => app.client_secret.expose().to_vec(),
            (None, Some(connection)) => blob(connection)?.webhook_secret.into_bytes(),
            (None, None) => return Err(ProviderError::Invalid("nothing to verify with".into())),
        };
        let presented = headers
            .get("x-test-signature")
            .and_then(|value| hex::decode(value).ok())
            .ok_or_else(|| ProviderError::Invalid("no signature".into()))?;
        verify_signature(&secret, body, &presented)?;
        Ok(VerifiedWebhook {
            event_id: headers
                .get("x-test-event-id")
                .ok_or_else(|| ProviderError::Invalid("no event id".into()))?
                .to_string(),
            topic: headers.get("x-test-topic").unwrap_or("unknown").to_string(),
        })
    }

    fn parse_inbound(
        &self,
        _webhook: &VerifiedWebhook,
        body: &[u8],
    ) -> Result<InboundEvent, ProviderError> {
        let value: Value =
            serde_json::from_slice(body).map_err(|_| ProviderError::Invalid("not json".into()))?;
        let text = |name: &str| {
            value
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let order = || OrderRef {
            external_order_id: text("order"),
            pubky_order_id: None,
        };
        Ok(match text("type").as_str() {
            "stock" => InboundEvent::StockLevel {
                provider_product_id: text("product"),
                provider_variant_id: text("variant"),
                available: value.get("available").and_then(Value::as_i64).unwrap_or(0),
            },
            "fulfillment" => InboundEvent::Fulfillment {
                order: order(),
                carrier: text("carrier"),
                tracking_number: text("tracking"),
            },
            "cancelled" => InboundEvent::OrderCancelled {
                order: order(),
                reason: None,
            },
            "refund" => InboundEvent::Refund {
                order: order(),
                amount_minor: value.get("amount_minor").and_then(Value::as_i64),
                currency: None,
            },
            "uninstalled" => InboundEvent::AppUninstalled,
            "data_request" => InboundEvent::Compliance {
                topic: ComplianceTopic::CustomerDataRequest,
            },
            "customer_redact" => InboundEvent::Compliance {
                topic: ComplianceTopic::CustomerRedact,
            },
            "shop_redact" => InboundEvent::Compliance {
                topic: ComplianceTopic::ShopRedact,
            },
            other => InboundEvent::Ignored {
                reason: format!("topic {other}"),
            },
        })
    }
}

// === Harness =================================================================

pub fn test_keys() -> Arc<ConnectorKeys> {
    Arc::new(ConnectorKeys::from_hex(CONNECTOR_KEY, None).expect("test connector keys"))
}

pub fn test_settings() -> ConnectorSettings {
    let mut settings = ConnectorSettings::new("https://service.test").expect("settings");
    settings.provider_timeout = Duration::from_secs(2);
    settings.lease = Duration::from_secs(60);
    settings.backoff_base = Duration::from_secs(10);
    settings.backoff_max = Duration::from_secs(600);
    settings.max_attempts = 5;
    settings
}

pub fn test_apps() -> AppRegistry {
    AppRegistry::new()
        .with(
            ProviderKind::Shopify,
            APP_KEY,
            "client-john",
            TEST_APP_SECRET,
        )
        .with(
            ProviderKind::Shopify,
            "pilot-1",
            "client-pilot",
            "pilot-secret",
        )
}

pub struct Stores {
    pub shopify: Arc<TestStore>,
    pub woocommerce: Arc<TestStore>,
}

/// The app with connectors enabled, a Shopify-kind test provider and two
/// configured Shopify apps (`john`, `pilot-1`).
pub async fn connector_app(pool: PgPool) -> (TestApp, Arc<TestStore>) {
    connector_app_with(pool, test_keys(), test_settings()).await
}

pub async fn connector_app_with(
    pool: PgPool,
    keys: Arc<ConnectorKeys>,
    settings: ConnectorSettings,
) -> (TestApp, Arc<TestStore>) {
    let (app, stores) = connector_app_full(pool, keys, settings).await;
    (app, stores.shopify)
}

/// Both provider kinds registered.
pub async fn connector_app_full(
    pool: PgPool,
    keys: Arc<ConnectorKeys>,
    settings: ConnectorSettings,
) -> (TestApp, Stores) {
    let shopify = Arc::new(TestProvider::new(ProviderKind::Shopify));
    let woocommerce = Arc::new(TestProvider::new(ProviderKind::Woocommerce));
    let stores = Stores {
        shopify: shopify.store.clone(),
        woocommerce: woocommerce.store.clone(),
    };
    let runtime = Arc::new(
        ConnectorRuntime::new(
            keys,
            ProviderRegistry::new().with(shopify).with(woocommerce),
            settings,
        )
        .with_apps(test_apps()),
    );
    let base = super::test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(runtime));
    (rebuild(base, state), stores)
}

pub fn rebuild(base: TestApp, state: marketplace_service::AppState) -> TestApp {
    TestApp {
        router: build_router(state.clone()).layer(axum::extract::connect_info::MockConnectInfo(
            "127.0.0.1:41000"
                .parse::<std::net::SocketAddr>()
                .expect("test peer address"),
        )),
        pool: base.pool,
        clock: base.clock,
        state,
    }
}

pub async fn raw(
    router: axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> (axum::http::StatusCode, axum::http::HeaderMap, Vec<u8>) {
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;
    let mut request = axum::http::Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = router
        .oneshot(
            request
                .body(axum::body::Body::from(body))
                .expect("request builds"),
        )
        .await
        .expect("request executes");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes()
        .to_vec();
    (status, headers, bytes)
}

pub fn as_json(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(bytes).expect("body is JSON")
    }
}

pub async fn api(
    app: &TestApp,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &Value,
) -> (axum::http::StatusCode, Value) {
    super::send(app.router.clone(), method, uri, token, body).await
}

pub fn query_param(url: &str, name: &str) -> String {
    url::Url::parse(url)
        .expect("url parses")
        .query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
        .unwrap_or_else(|| panic!("{name} in {url}"))
}

/// The seller asks to connect `shop`. Returns the pending connection id.
pub async fn start_connect(app: &TestApp, seller: &TestActor, shop: &str) -> Uuid {
    let (status, body) = api(
        app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "shopify", "domain": shop }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "connect: {body}");
    assert_eq!(body["connection"]["status"], "pending");
    body["connection"]["id"]
        .as_str()
        .expect("connection id")
        .parse()
        .expect("uuid")
}

/// The store's launch to the app's App URL. Returns the status and the
/// `Location` it redirected to, if any.
pub async fn launch(
    app: &TestApp,
    app_key: &str,
    shop: &str,
    secret: &str,
) -> (axum::http::StatusCode, Option<String>, Value) {
    let (status, headers, body) = raw(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/connectors/shopify/apps/{app_key}/launch?{}",
            launch_query(shop, secret)
        ),
        &[],
        Vec::new(),
    )
    .await;
    let location = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    (status, location, as_json(&body))
}

/// The full Shopify install: the seller's connect request, the store's
/// launch, and the OAuth callback with the good code.
pub async fn connect_shop(app: &TestApp, seller: &TestActor, shop: &str) -> Uuid {
    let id = start_connect(app, seller, shop).await;
    let (status, location, body) = launch(app, APP_KEY, shop, TEST_APP_SECRET).await;
    assert_eq!(status, axum::http::StatusCode::SEE_OTHER, "launch: {body}");
    let state = query_param(&location.expect("launch redirects"), "state");
    let (status, headers, bytes) = raw(
        app.router.clone(),
        "GET",
        &format!("/v1/connectors/shopify/apps/{APP_KEY}/oauth/callback?state={state}&code=good"),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "callback: {:?} {}",
        headers,
        String::from_utf8_lossy(&bytes)
    );
    id
}

pub async fn map_listing(
    app: &TestApp,
    seller: &TestActor,
    connection: Uuid,
    listing_id: &str,
    product: &str,
    variant: &str,
) {
    let (status, body) = api(
        app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [{
            "listing_id": listing_id,
            "provider_product_id": product,
            "provider_variant_id": variant,
            "sku": "BOOT-42",
        }] }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "map: {body}");
}

/// A Shopify-style webhook, signed with the first app's client secret.
pub async fn webhook(
    app: &TestApp,
    shop: &str,
    event_id: &str,
    topic: &str,
    body: &Value,
) -> (axum::http::StatusCode, Value) {
    let bytes = serde_json::to_vec(body).expect("webhook body serializes");
    signed_webhook(app, shop, event_id, topic, bytes, TEST_APP_SECRET).await
}

pub async fn signed_webhook(
    app: &TestApp,
    shop: &str,
    event_id: &str,
    topic: &str,
    bytes: Vec<u8>,
    secret: &str,
) -> (axum::http::StatusCode, Value) {
    post_signed(
        app,
        &format!("/v1/connectors/webhooks/shopify/{APP_KEY}"),
        shop,
        event_id,
        topic,
        bytes,
        secret,
    )
    .await
}

pub async fn post_signed(
    app: &TestApp,
    path: &str,
    shop: &str,
    event_id: &str,
    topic: &str,
    bytes: Vec<u8>,
    secret: &str,
) -> (axum::http::StatusCode, Value) {
    let signature = sign(secret, &bytes);
    let (status, _, response) = raw(
        app.router.clone(),
        "POST",
        path,
        &[
            ("content-type", "application/json"),
            ("x-test-shop", shop),
            ("x-test-event-id", event_id),
            ("x-test-topic", topic),
            ("x-test-signature", &signature),
        ],
        bytes,
    )
    .await;
    (status, as_json(&response))
}

pub async fn push_state(
    pool: &PgPool,
    order_id: &str,
) -> (String, Option<String>, Option<String>, i32) {
    sqlx::query_as(
        "SELECT state, external_id, quarantine_reason, attempts FROM connector_order_push \
         WHERE pubky_order_id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("push row exists")
}
