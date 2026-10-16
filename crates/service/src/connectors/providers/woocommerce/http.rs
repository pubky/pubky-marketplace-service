//! The HTTP layer every WooCommerce call goes through.
//!
//! One request builds one client, pinned to the addresses the host resolved to
//! and checked to be public, so a merchant cannot point the service at its own
//! network and a DNS answer cannot change between the check and the
//! connection. Redirects are never followed (a redirect would carry the
//! credential to another host). The response body is capped. In query-string
//! mode the credential is in the URL, so a transport error is never printed
//! with its URL.

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, RETRY_AFTER};
use reqwest::Method;
use serde_json::Value;

use crate::connectors::provider::ProviderError;

use super::creds::ApiKeys;
use super::site::{is_public_ip, Site, SitePolicy};

const MAX_BODY_BYTES: usize = 12 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct WooOptions {
    pub policy: SitePolicy,
    pub extra_roots: Vec<reqwest::Certificate>,
    pub request_timeout: Duration,
    /// Products per catalog request (WooCommerce allows up to 100).
    pub catalog_page_size: u32,
    /// Items per request when a list is read in full (webhooks, an order's
    /// refunds, order search); every such read follows the pages.
    pub list_page_size: u32,
}

impl Default for WooOptions {
    fn default() -> Self {
        Self {
            policy: SitePolicy::default(),
            extra_roots: Vec::new(),
            request_timeout: Duration::from_secs(25),
            catalog_page_size: 50,
            list_page_size: 100,
        }
    }
}

impl WooOptions {
    /// For tests only: allows loopback addresses and, when `plain_http`,
    /// unencrypted HTTP (a replay server in the test process), and trusts the
    /// given extra CA certificates (the local CA of the Docker WooCommerce).
    /// Nothing reads this from the environment, so a deployment cannot reach
    /// it.
    pub fn local_testing(plain_http: bool, extra_roots: Vec<reqwest::Certificate>) -> Self {
        Self {
            policy: SitePolicy {
                plain_http,
                allow_private_hosts: true,
            },
            extra_roots,
            request_timeout: Duration::from_secs(25),
            catalog_page_size: 50,
            list_page_size: 100,
        }
    }

    pub fn with_list_page_size(mut self, size: u32) -> Self {
        self.list_page_size = size.clamp(1, 100);
        self
    }

    pub fn with_catalog_page_size(mut self, size: u32) -> Self {
        self.catalog_page_size = size.clamp(1, 100);
        self
    }
}

#[derive(Clone, Copy)]
pub enum Auth<'a> {
    None,
    Basic(&'a ApiKeys),
    Query(&'a ApiKeys),
}

pub struct Reply {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub fn json(&self) -> Result<Value, ProviderError> {
        serde_json::from_slice(&self.body).map_err(|_| {
            ProviderError::Unknown("the store answered with something that is not JSON".into())
        })
    }

    /// WooCommerce's machine error code, if the body is its error shape.
    pub fn error_code(&self) -> Option<String> {
        let value: Value = serde_json::from_slice(&self.body).ok()?;
        value.get("code")?.as_str().map(str::to_string)
    }

    pub fn error_message(&self) -> String {
        serde_json::from_slice::<Value>(&self.body)
            .ok()
            .and_then(|value| value.get("message")?.as_str().map(str::to_string))
            .map(|message| crate::connectors::store::truncate(&message, 200))
            .unwrap_or_default()
    }

    /// WooCommerce answers a write with a read-only key by 401, saying so; an
    /// unknown key gets the same status with another message.
    fn is_read_only_key(&self) -> bool {
        self.error_code().as_deref() == Some("woocommerce_rest_authentication_error")
            && self.error_message().contains("write permissions")
    }

    /// `Ok(self)` for a 2xx; otherwise the framework's error for it.
    pub fn checked(self) -> Result<Self, ProviderError> {
        match self.status {
            200..=299 => Ok(self),
            401 if self.is_read_only_key() => Err(ProviderError::Rejected {
                code: "keys_read_only".into(),
                message: self.error_message(),
            }),
            401 => Err(ProviderError::Unauthorized),
            429 => Err(ProviderError::RateLimited {
                retry_after: self
                    .header(RETRY_AFTER.as_str())
                    .and_then(|value| value.trim().parse::<u64>().ok())
                    .map(Duration::from_secs),
            }),
            500..=599 => Err(ProviderError::Unknown(format!(
                "the store answered {}",
                self.status
            ))),
            status => Err(ProviderError::Rejected {
                code: self
                    .error_code()
                    .map(|code| short_code(&code))
                    .unwrap_or_else(|| {
                        if status == 403 {
                            "forbidden".into()
                        } else {
                            format!("http_{status}")
                        }
                    }),
                message: self.error_message(),
            }),
        }
    }
}

/// `woocommerce_rest_invalid_product_id` is a code a merchant could read, but
/// the reason shown for a quarantined order is short: drop the common prefix.
fn short_code(code: &str) -> String {
    code.trim_start_matches("woocommerce_rest_")
        .trim_start_matches("woocommerce_")
        .trim_start_matches("rest_")
        .to_string()
}

pub struct WooHttp {
    options: WooOptions,
}

impl WooHttp {
    pub fn new(options: WooOptions) -> Self {
        Self { options }
    }

    pub fn policy(&self) -> SitePolicy {
        self.options.policy
    }

    pub fn plain_http(&self) -> bool {
        self.options.policy.plain_http
    }

    pub fn list_page_size(&self) -> u32 {
        self.options.list_page_size.clamp(1, 100)
    }

    pub fn catalog_page_size(&self) -> u32 {
        self.options.catalog_page_size.clamp(1, 100)
    }

    async fn client(&self, site: &Site) -> Result<reqwest::Client, ProviderError> {
        let port = site
            .port()
            .unwrap_or(if self.plain_http() { 80 } else { 443 });
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(self.options.request_timeout)
            .https_only(!self.plain_http())
            .user_agent("PubkyShop-Connector/1 (+https://pubky.app)");
        for root in &self.options.extra_roots {
            builder = builder.add_root_certificate(root.clone());
        }
        if site.host().parse::<std::net::IpAddr>().is_err() {
            let addresses: Vec<SocketAddr> = tokio::net::lookup_host((site.host(), port))
                .await
                .map_err(|_| ProviderError::Unknown("the store's address did not resolve".into()))?
                .collect();
            if addresses.is_empty() {
                return Err(ProviderError::Unknown(
                    "the store's address did not resolve".into(),
                ));
            }
            if !self.options.policy.allow_private_hosts
                && !addresses.iter().all(|address| is_public_ip(address.ip()))
            {
                return Err(ProviderError::Invalid(
                    "The store's address points at a private network, which cannot be connected."
                        .into(),
                ));
            }
            builder = builder.resolve_to_addrs(site.host(), &addresses);
        } else {
            let ip: std::net::IpAddr = site.host().parse().expect("checked above");
            if !self.options.policy.allow_private_hosts && !is_public_ip(ip) {
                return Err(ProviderError::Invalid(
                    "The store's address points at a private network, which cannot be connected."
                        .into(),
                ));
            }
        }
        builder
            .build()
            .map_err(|_| ProviderError::Unknown("the HTTP client could not be built".into()))
    }

    /// One request, any status returned as a [`Reply`]. Only a transport
    /// failure is an error here. `path` starts with `/` and is relative to the
    /// site (`/wp-json/wc/v3/orders`).
    pub async fn send(
        &self,
        site: &Site,
        auth: Auth<'_>,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Reply, ProviderError> {
        let client = self.client(site).await?;
        let mut request = client.request(method, site.url(self.plain_http(), path));
        if !query.is_empty() {
            request = request.query(query);
        }
        match auth {
            Auth::None => {}
            Auth::Basic(keys) => {
                use base64::Engine;
                let token = base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", keys.key, keys.secret));
                let mut value = HeaderValue::from_str(&format!("Basic {token}"))
                    .map_err(|_| ProviderError::Invalid("the API key is not valid".into()))?;
                value.set_sensitive(true);
                request = request.header(AUTHORIZATION, value);
            }
            Auth::Query(keys) => {
                request = request.query(&[
                    ("consumer_key", keys.key.as_str()),
                    ("consumer_secret", keys.secret.as_str()),
                ]);
            }
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let mut response = request.send().await.map_err(transport_error)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let mut collected = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if collected.len() + chunk.len() > MAX_BODY_BYTES {
                        return Err(ProviderError::Unknown(
                            "the store's answer was too large".into(),
                        ));
                    }
                    collected.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(error) => return Err(transport_error(error)),
            }
        }
        Ok(Reply {
            status,
            headers,
            body: collected,
        })
    }
}

/// A transport failure, described without the URL (which carries the
/// credential in query-string mode).
fn transport_error(error: reqwest::Error) -> ProviderError {
    let error = error.without_url();
    let mut chain = String::new();
    let mut source: Option<&dyn std::error::Error> = Some(&error);
    while let Some(current) = source {
        chain.push_str(&current.to_string().to_ascii_lowercase());
        chain.push(' ');
        source = current.source();
    }
    if chain.contains("certificate")
        || chain.contains("invalid peer")
        || chain.contains("unknownissuer")
    {
        return ProviderError::Invalid(
            "The store's HTTPS certificate is not valid. Fix the certificate (an expired or \
             self-signed certificate is not accepted) and try again."
                .into(),
        );
    }
    if error.is_timeout() {
        return ProviderError::Unknown("the store did not answer in time".into());
    }
    ProviderError::Unknown("the store could not be reached".into())
}
