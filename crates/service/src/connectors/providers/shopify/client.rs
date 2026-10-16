//! Shopify's Admin GraphQL API over HTTPS, with its cost-based throttle.
//!
//! Every request is a `POST https://{shop}/admin/api/2026-10/graphql.json`
//! with the store's access token. Nothing here knows an order or a product.
//!
//! Outcome mapping follows the provider contract in `provider.rs`:
//!
//! * a throttled call (`THROTTLED` in the GraphQL errors, or HTTP 429) was
//!   not run, so it is [`ProviderError::RateLimited`], even for a mutation;
//!   the client waits and retries it a couple of times itself before it
//!   hands that back to the framework;
//! * a response that never arrived or that is a 5xx may have been applied:
//!   [`ProviderError::Unknown`];
//! * 401/402/403/404/423 mean the store no longer accepts the app's access:
//!   [`ProviderError::Unauthorized`];
//! * GraphQL errors (a missing scope, a query over the cost cap, a bad
//!   variable) were refused before anything ran: [`ProviderError::Rejected`].
//!
//! Shopify's leaky bucket (`extensions.cost.throttleStatus`) is remembered
//! per shop, and a call waits for the bucket before it is sent when the last
//! answer left it nearly empty.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::connectors::provider::ProviderError;

/// The Admin API version every document is written and validated against.
pub const API_VERSION: &str = "2026-10";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A THROTTLED answer is retried this many times inside one call.
const INLINE_RETRIES: u32 = 2;
/// The longest the client sleeps between two attempts of one call.
const MAX_INLINE_WAIT: Duration = Duration::from_secs(5);
/// Below this many points left in the bucket, the next call waits first.
const PACE_FLOOR: f64 = 150.0;
const DEFAULT_RESTORE_RATE: f64 = 50.0;

/// Where requests for a shop go. [`Origin::Shopify`] is the shop's own
/// `https://{shop}` host; [`Origin::Fixed`] sends every shop to one base URL
/// (an integration test's stand-in for Shopify, never set in production).
#[derive(Debug, Clone)]
pub enum Origin {
    Shopify,
    Fixed(String),
}

impl Origin {
    fn base(&self, shop: &str) -> String {
        match self {
            Origin::Shopify => format!("https://{shop}"),
            Origin::Fixed(base) => base.trim_end_matches('/').to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    available: f64,
    maximum: f64,
    restore_rate: f64,
    at: Instant,
}

impl Bucket {
    fn estimate(&self, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        (self.available + self.restore_rate * elapsed).min(self.maximum)
    }
}

pub struct Client {
    http: reqwest::Client,
    origin: Origin,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl Client {
    pub fn new(origin: Origin) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            // A redirect would carry the access token to another host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("the static reqwest client configuration is valid");
        Self {
            http,
            origin,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    pub fn endpoint(&self, shop: &str) -> String {
        format!(
            "{}/admin/api/{API_VERSION}/graphql.json",
            self.origin.base(shop)
        )
    }

    pub fn token_endpoint(&self, shop: &str) -> String {
        format!("{}/admin/oauth/access_token", self.origin.base(shop))
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Runs one document and returns its `data`. A document that returned
    /// GraphQL errors is an error here; a mutation's own `userErrors` are in
    /// the returned data for the caller to read.
    pub async fn execute(
        &self,
        shop: &str,
        token: &str,
        document: &str,
        variables: Value,
    ) -> Result<Value, ProviderError> {
        let mut attempt = 0;
        loop {
            self.pace(shop).await;
            match self.send(shop, token, document, &variables).await {
                Err(ProviderError::RateLimited { retry_after }) if attempt < INLINE_RETRIES => {
                    attempt += 1;
                    let wait = retry_after
                        .unwrap_or(Duration::from_secs(1))
                        .min(MAX_INLINE_WAIT);
                    tokio::time::sleep(wait).await;
                }
                other => return other,
            }
        }
    }

    async fn pace(&self, shop: &str) {
        let wait = {
            let buckets = self.buckets.lock().expect("throttle state is not poisoned");
            buckets.get(shop).and_then(|bucket| {
                let estimate = bucket.estimate(Instant::now());
                let floor = PACE_FLOOR.min(bucket.maximum);
                (estimate < floor).then(|| {
                    Duration::from_secs_f64((floor - estimate) / bucket.restore_rate.max(1.0))
                        .min(MAX_INLINE_WAIT)
                })
            })
        };
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
    }

    fn remember(&self, shop: &str, status: &Value) {
        let number = |name: &str| status.get(name).and_then(Value::as_f64);
        let (Some(available), Some(maximum)) =
            (number("currentlyAvailable"), number("maximumAvailable"))
        else {
            return;
        };
        let bucket = Bucket {
            available,
            maximum,
            restore_rate: number("restoreRate").unwrap_or(DEFAULT_RESTORE_RATE),
            at: Instant::now(),
        };
        self.buckets
            .lock()
            .expect("throttle state is not poisoned")
            .insert(shop.to_string(), bucket);
    }

    async fn send(
        &self,
        shop: &str,
        token: &str,
        document: &str,
        variables: &Value,
    ) -> Result<Value, ProviderError> {
        let response = self
            .http
            .post(self.endpoint(shop))
            .header("X-Shopify-Access-Token", token)
            .header("Accept", "application/json")
            .json(&json!({ "query": document, "variables": variables }))
            .send()
            .await
            .map_err(|error| ProviderError::Unknown(describe(error)))?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
            .map(Duration::from_secs_f64);
        match status.as_u16() {
            429 => return Err(ProviderError::RateLimited { retry_after }),
            401 | 402 | 403 | 404 | 423 => return Err(ProviderError::Unauthorized),
            code if status.is_server_error() => {
                return Err(ProviderError::Unknown(format!("shopify answered {code}")));
            }
            code if !status.is_success() => {
                return Err(ProviderError::Rejected {
                    code: format!("http_{code}"),
                    message: format!("shopify answered {code}"),
                });
            }
            _ => {}
        }
        let body: Value = response
            .json()
            .await
            .map_err(|error| ProviderError::Unknown(describe(error)))?;
        if let Some(throttle) = body.pointer("/extensions/cost/throttleStatus") {
            self.remember(shop, throttle);
        }
        match body.get("errors") {
            Some(errors) if errors.as_array().is_none_or(|list| !list.is_empty()) => {
                Err(graphql_error(errors, &body))
            }
            _ => body
                .get("data")
                .cloned()
                .filter(|data| !data.is_null())
                .ok_or_else(|| ProviderError::Unknown("shopify returned no data".into())),
        }
    }
}

fn describe(error: reqwest::Error) -> String {
    let kind = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_decode() {
        "decode"
    } else {
        "request"
    };
    format!("shopify {kind} error: {}", error.without_url())
}

fn graphql_error(errors: &Value, body: &Value) -> ProviderError {
    let first = errors.as_array().and_then(|list| list.first());
    let code = first
        .and_then(|error| error.pointer("/extensions/code"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = first
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("shopify returned an error")
        .chars()
        .take(300)
        .collect::<String>();
    match code {
        "THROTTLED" => ProviderError::RateLimited {
            retry_after: throttle_wait(body),
        },
        "ACCESS_DENIED" => ProviderError::Rejected {
            code: "access_denied".into(),
            message,
        },
        "MAX_COST_EXCEEDED" => ProviderError::Rejected {
            code: "query_too_costly".into(),
            message,
        },
        "INTERNAL_SERVER_ERROR" | "SHOP_INACTIVE" => ProviderError::Unknown(message),
        _ => ProviderError::Rejected {
            code: "graphql_error".into(),
            message,
        },
    }
}

/// How long until the bucket holds what the throttled query asked for.
fn throttle_wait(body: &Value) -> Option<Duration> {
    let cost = body.pointer("/extensions/cost")?;
    let requested = cost.get("requestedQueryCost")?.as_f64()?;
    let status = cost.get("throttleStatus")?;
    let available = status.get("currentlyAvailable")?.as_f64()?;
    let rate = status.get("restoreRate")?.as_f64()?;
    (rate > 0.0 && requested > available)
        .then(|| Duration::from_secs_f64((requested - available) / rate))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_throttled_answer_says_how_long_to_wait() {
        let body = json!({
            "errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
            "extensions": {"cost": {
                "requestedQueryCost": 500,
                "throttleStatus": {"maximumAvailable": 1000.0, "currentlyAvailable": 100.0, "restoreRate": 50.0}
            }}
        });
        match graphql_error(&body["errors"], &body) {
            ProviderError::RateLimited { retry_after } => {
                assert_eq!(retry_after, Some(Duration::from_secs(8)));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn graphql_errors_map_to_the_provider_contract() {
        let errors = |code: &str| json!([{"message": "m", "extensions": {"code": code}}]);
        let map = |code: &str| graphql_error(&errors(code), &json!({}));
        assert!(matches!(
            map("ACCESS_DENIED"),
            ProviderError::Rejected { code, .. } if code == "access_denied"
        ));
        assert!(matches!(
            map("MAX_COST_EXCEEDED"),
            ProviderError::Rejected { code, .. } if code == "query_too_costly"
        ));
        assert!(matches!(
            map("INTERNAL_SERVER_ERROR"),
            ProviderError::Unknown(_)
        ));
        assert!(matches!(
            map("SOMETHING_ELSE"),
            ProviderError::Rejected { code, .. } if code == "graphql_error"
        ));
        assert!(matches!(
            map("THROTTLED"),
            ProviderError::RateLimited { retry_after: None }
        ));
    }

    #[test]
    fn the_bucket_refills_at_its_restore_rate_up_to_its_maximum() {
        let now = Instant::now();
        let bucket = Bucket {
            available: 100.0,
            maximum: 1000.0,
            restore_rate: 50.0,
            at: now,
        };
        assert!((bucket.estimate(now + Duration::from_secs(2)) - 200.0).abs() < 1e-6);
        assert!((bucket.estimate(now + Duration::from_secs(600)) - 1000.0).abs() < 1e-6);
    }

    #[test]
    fn endpoints_follow_the_origin() {
        let shopify = Client::new(Origin::Shopify);
        assert_eq!(
            shopify.endpoint("a.myshopify.com"),
            "https://a.myshopify.com/admin/api/2026-10/graphql.json"
        );
        assert_eq!(
            shopify.token_endpoint("a.myshopify.com"),
            "https://a.myshopify.com/admin/oauth/access_token"
        );
        let fixed = Client::new(Origin::Fixed("http://127.0.0.1:9/".into()));
        assert_eq!(
            fixed.endpoint("a.myshopify.com"),
            "http://127.0.0.1:9/admin/api/2026-10/graphql.json"
        );
    }
}
