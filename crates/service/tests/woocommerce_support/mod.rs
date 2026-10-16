//! Shared harness for the WooCommerce provider tests: a replay of exchanges
//! captured from a real WooCommerce, the driver that records them, and the
//! flows both run.
#![allow(dead_code)]

pub mod flows;
pub mod live;
pub mod replay;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use hmac::{Hmac, Mac};
use marketplace_service::connectors::provider::{
    CallbackParams, CompleteRequest, ConnectRequest, ConnectionContext, ConnectorProvider,
    CredentialSecret, PendingConnection, ProviderKind, ProviderRegistry,
};
use marketplace_service::connectors::providers::woocommerce::{WooCommerceProvider, WooOptions};
use marketplace_service::connectors::{ConnectorRuntime, ConnectorSettings};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::connector_provider::{raw, rebuild, test_keys};
use crate::common::{test_app, TestApp};

use self::live::{ca_certificate, LiveStack};
use self::replay::{Delivery, Replay};

pub const RETURN_URL: &str = "https://shop.test/marketplace/connections";
/// The origin the store is told to call. While recording, WooCommerce (in
/// Docker) reaches the receiver on the VM host through this name; the replay
/// uses the same value so the recorded registration bodies match.
pub const PUBLIC_BASE: &str = "https://host.docker.internal:18443";
pub const CALLBACK_URL: &str =
    "https://host.docker.internal:18443/v1/connectors/woocommerce/oauth/callback";
pub const WEBHOOK_PATH_BASE: &str = "/v1/connectors/webhooks/woocommerce/c";

/// Each connection's own webhook URL, as the service builds it from the public
/// base.
pub fn webhook_url(connection_id: Uuid) -> String {
    format!("{PUBLIC_BASE}{WEBHOOK_PATH_BASE}/{connection_id}")
}

pub fn webhook_path(connection_id: Uuid) -> String {
    format!("{WEBHOOK_PATH_BASE}/{connection_id}")
}

/// Where the store side of a scenario comes from: the live Docker store (when
/// recording) or the replay of what it answered.
pub struct StoreSide {
    pub scenario: String,
    pub replay: Option<Replay>,
    pub live: Option<LiveStack>,
}

impl StoreSide {
    /// Starts a scenario. `WOO_LIVE=1` runs against the Docker store and
    /// labels the capture; otherwise the scenario's recorded exchanges are
    /// replayed.
    pub async fn open(scenario: &str) -> Self {
        Self::open_with(scenario, true).await
    }

    /// `rewrite_origin: false` keeps the recorded origin in replayed answers
    /// (see [`Replay::start_with`]).
    pub async fn open_with(scenario: &str, rewrite_origin: bool) -> Self {
        match LiveStack::from_env() {
            Some(live) => {
                live.label(scenario).await;
                live.wipe_webhooks().await;
                Self {
                    scenario: scenario.to_string(),
                    replay: None,
                    live: Some(live),
                }
            }
            None => Self {
                scenario: scenario.to_string(),
                replay: Some(Replay::start_with(&[scenario], rewrite_origin).await),
                live: None,
            },
        }
    }

    pub fn is_live(&self) -> bool {
        self.live.is_some()
    }

    /// The store address as a seller would type it.
    pub fn site(&self) -> String {
        match (&self.live, &self.replay) {
            (Some(live), _) => live.site(),
            (_, Some(replay)) => replay.site(),
            _ => unreachable!("a side is live or replayed"),
        }
    }

    pub fn provider(&self, page_size: u32) -> WooCommerceProvider {
        let options = if self.is_live() {
            WooOptions::local_testing(false, vec![ca_certificate()])
        } else {
            WooOptions::local_testing(true, Vec::new())
        }
        .with_catalog_page_size(page_size);
        WooCommerceProvider::with_options(options, Some(RETURN_URL.to_string()))
    }

    /// A provider whose requests give up after `timeout`, to be held up by
    /// [`Self::delay_next`].
    pub fn provider_with_timeout(&self, timeout: Duration) -> WooCommerceProvider {
        let mut options = if self.is_live() {
            WooOptions::local_testing(false, vec![ca_certificate()])
        } else {
            WooOptions::local_testing(true, Vec::new())
        }
        .with_catalog_page_size(7);
        options.request_timeout = timeout;
        WooCommerceProvider::with_options(options, Some(RETURN_URL.to_string()))
    }

    /// The store answers the next create of `kind` only after `delay`,
    /// applying it late: `order`, `refund`, `probe` (the write-access check a
    /// connect makes) or `register` (the first of our webhooks being created).
    /// Live through the test WordPress plugin, replayed by holding the
    /// recorded answer.
    pub fn delay_next(&self, kind: &str, delay: Duration) {
        match (&self.live, &self.replay) {
            (Some(live), _) => {
                live.wp(&[
                    "option",
                    "update",
                    &format!("pubky_test_{kind}_delay"),
                    &delay.as_secs().to_string(),
                ]);
            }
            (_, Some(replay)) => match kind {
                "refund" => replay.hold_next("POST", "/refunds", delay),
                "probe" => replay.hold_next_with(
                    "POST",
                    "/wc/v3/webhooks",
                    Some("pubky.access_check"),
                    delay,
                ),
                "register" => {
                    replay.hold_next_with("POST", "/wc/v3/webhooks", Some("product.updated"), delay)
                }
                _ => replay.hold_next("POST", "/wc/v3/orders", delay),
            },
            _ => unreachable!("a side is live or replayed"),
        }
    }

    /// Waits until a request held by [`Self::delay_next`] has reached the
    /// store (`kind` as there). Replayed, that is the `nth` request of its kind
    /// seen so far.
    pub async fn wait_until_held(&self, kind: &str, nth: usize) {
        for _ in 0..200 {
            let started = match (&self.live, &self.replay) {
                (Some(live), _) => {
                    live.rest("GET", &format!("/pubky-test/v1/delay/{kind}"), None)
                        .await
                        == json!(0)
                }
                (_, Some(replay)) => {
                    let topic = if kind == "probe" {
                        "pubky.access_check"
                    } else {
                        "product.updated"
                    };
                    replay
                        .seen()
                        .iter()
                        .filter(|seen| {
                            seen.method == "POST"
                                && seen.path == "/wp-json/wc/v3/webhooks"
                                && seen
                                    .body
                                    .as_ref()
                                    .is_some_and(|body| body["topic"] == topic)
                        })
                        .count()
                        >= nth
                }
                _ => unreachable!("a side is live or replayed"),
            };
            if started {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the held {kind} request never reached the store");
    }

    /// The store refuses the next order cancellation with a server error, once
    /// (live through the test plugin; replayed because the recorded answer is
    /// the 500).
    pub fn fail_next_cancel(&self) {
        if let Some(live) = &self.live {
            live.wp(&["option", "update", "pubky_test_cancel_fail", "1"]);
        }
    }

    /// Lets the held request land. Live: waits it out. Replay: nothing to wait for.
    pub async fn let_it_land(&self, delay: Duration) {
        if self.is_live() {
            tokio::time::sleep(delay + Duration::from_secs(1)).await;
        }
    }

    pub fn live(&self) -> &LiveStack {
        self.live
            .as_ref()
            .expect("this step runs against the live store only")
    }

    /// The merchant approving the wc-auth request: the body WooCommerce posts
    /// to the callback, with the nonce of this connect as `user_id`.
    pub fn approve(&self, authorize_url: &str, name: &str) -> Vec<u8> {
        let nonce = url::Url::parse(authorize_url)
            .expect("authorize url")
            .query_pairs()
            .find(|(key, _)| key == "user_id")
            .map(|(_, value)| value.into_owned())
            .expect("user_id in the authorize url");
        match &self.live {
            Some(live) => {
                let body = live.approve(authorize_url);
                live.save_callback(&format!("{}.{name}", self.scenario), &body);
                body
            }
            None => {
                let recorded = Delivery::load(&format!("{}.{name}", self.scenario));
                let mut value: Value =
                    serde_json::from_slice(&recorded.body()).expect("callback JSON");
                value["user_id"] = json!(nonce);
                serde_json::to_vec(&value).expect("json")
            }
        }
    }

    /// A webhook delivery the store sends after `action`. Live: performs the
    /// merchant's action, lets the store deliver, picks the delivery the
    /// predicate accepts and saves it as a fixture. Replay: the fixture.
    pub async fn deliver(
        &self,
        name: &str,
        secret: &str,
        action: impl std::future::Future<Output = ()>,
        wanted: impl Fn(&str, &Value) -> bool,
    ) -> Delivery {
        let full = format!("{}.{name}", self.scenario);
        match &self.live {
            Some(live) => {
                let mark = live.receiver_mark();
                action.await;
                live.flush_webhooks();
                let delivery = live
                    .deliveries_since(mark)
                    .into_iter()
                    .rev()
                    .find(|delivery| {
                        if !signed_with(delivery, secret) {
                            return false;
                        }
                        let topic = delivery
                            .headers
                            .get("x-wc-webhook-topic")
                            .cloned()
                            .unwrap_or_default();
                        let body: Value =
                            serde_json::from_slice(&delivery.body()).unwrap_or(Value::Null);
                        wanted(&topic, &body)
                    })
                    .unwrap_or_else(|| panic!("the store delivered no webhook for {name}"));
                live.save_delivery(&full, &delivery, Some(secret));
                delivery
            }
            None => {
                drop(action);
                let recorded = Delivery::load(&full);
                let replay = self.replay.as_ref().expect("replay");
                resign(&recorded, secret, &format!("http://{}/", replay.site()))
            }
        }
    }
}

pub fn settings() -> ConnectorSettings {
    let mut settings = ConnectorSettings::new(PUBLIC_BASE).expect("settings");
    settings.provider_timeout = Duration::from_secs(20);
    settings.lease = Duration::from_secs(60);
    settings.backoff_base = Duration::from_secs(10);
    settings.backoff_max = Duration::from_secs(600);
    settings.max_attempts = 5;
    settings
}

/// The service with connectors enabled and the real WooCommerce provider.
pub async fn woo_app(pool: PgPool, provider: WooCommerceProvider) -> TestApp {
    let runtime = Arc::new(ConnectorRuntime::new(
        test_keys(),
        ProviderRegistry::new().with(Arc::new(provider)),
        settings(),
    ));
    let base = test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(runtime));
    rebuild(base, state)
}

/// Whether the store signed this delivery with `secret`, that is, whether it
/// was sent by one of this connection's webhooks.
fn signed_with(delivery: &Delivery, secret: &str) -> bool {
    use base64::Engine;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(&delivery.body());
    let expected = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    delivery
        .headers
        .get("x-wc-webhook-signature")
        .is_some_and(|signature| *signature == expected)
}

/// A recorded delivery made ready to post to this run: addressed from this
/// replay's store (the `X-WC-Webhook-Source` the connection is found by) and
/// signed again with the secret this run registered, because the recorded
/// signature belonged to the recording run's secret. The body is untouched.
pub fn resign(delivery: &Delivery, secret: &str, source: &str) -> Delivery {
    use base64::Engine;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(&delivery.body());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let mut resigned = delivery.clone();
    resigned
        .headers
        .insert("x-wc-webhook-signature".into(), signature);
    resigned
        .headers
        .insert("x-wc-webhook-source".into(), source.to_string());
    resigned
}

/// Posts a delivery to the newest live WooCommerce connection's webhook route
/// as its store would.
pub async fn post_delivery(app: &TestApp, delivery: &Delivery) -> (StatusCode, Value) {
    let connection: Uuid = sqlx::query_scalar(
        "SELECT id FROM connector_connections WHERE provider = 'woocommerce' \
         AND status IN ('connected', 'needs_attention', 'error') \
         ORDER BY connected_at DESC LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .expect("a live WooCommerce connection");
    post_delivery_to(app, connection, delivery).await
}

/// Posts a delivery to one connection's webhook route.
pub async fn post_delivery_to(
    app: &TestApp,
    connection: Uuid,
    delivery: &Delivery,
) -> (StatusCode, Value) {
    let pairs: Vec<(&str, &str)> = delivery
        .headers
        .iter()
        .filter(|(name, _)| name.as_str() != "user-agent")
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let (status, _, body) = raw(
        app.router.clone(),
        "POST",
        &webhook_path(connection),
        &pairs,
        delivery.body(),
    )
    .await;
    (status, crate::common::connector_provider::as_json(&body))
}

/// A connected context built straight from the provider, for the tests that
/// need no database.
pub async fn connect_context(
    side: &StoreSide,
    provider: &WooCommerceProvider,
    name: &str,
) -> (ConnectionContext, String) {
    let nonce = format!("nonce-{}", Uuid::new_v4().simple());
    let request_id = Uuid::new_v4();
    let request = ConnectRequest {
        connection_id: request_id,
        seller_pubky: "seller".into(),
        shop: provider.normalize_shop(&side.site()).expect("shop"),
        state: nonce,
        app: None,
        callback_url: CALLBACK_URL.into(),
        webhook_url: webhook_url(request_id),
        params: json!({}),
    };
    let started = provider.connect(&request).await.expect("connect");
    let authorize_url = started
        .authorize_url
        .expect("woocommerce authorizes at the store");
    let body = side.approve(&authorize_url, name);
    let completed = provider
        .complete_connect(&CompleteRequest {
            pending: PendingConnection {
                connection_id: request.connection_id,
                seller_pubky: request.seller_pubky.clone(),
                shop: request.shop.clone(),
                state: request.state.clone(),
            },
            app: None,
            callback: CallbackParams {
                body,
                ..CallbackParams::default()
            },
            webhook_url: webhook_url(request_id),
        })
        .await
        .expect("complete connect");
    let secret = secret_of(&completed.credentials);
    let context = ConnectionContext {
        connection_id: request.connection_id,
        seller_pubky: request.seller_pubky,
        shop: request.shop,
        scopes: completed.scopes,
        credentials: completed.credentials,
        app: None,
    };
    // What the framework does once the credential is stored.
    provider
        .register_webhooks(&context, &webhook_url(request_id))
        .await
        .expect("register webhooks");
    (context, secret)
}

pub fn secret_of(credentials: &CredentialSecret) -> String {
    marketplace_service::connectors::providers::woocommerce::creds::StoredCredentials::open(
        credentials,
    )
    .expect("credentials open")
    .webhook_secret
}

pub fn kind() -> ProviderKind {
    ProviderKind::Woocommerce
}
