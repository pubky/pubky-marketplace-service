//! The WooCommerce provider: WooCommerce REST v3 over HTTPS, one connection
//! per store.
//!
//! Connecting:
//!
//! * **One click.** The Shop opens `{site}/wc-auth/v1/authorize` with
//!   `app_name=Pubky Shop`, `scope=read_write`, `user_id` set to the pending
//!   connect's single-use nonce, the Shop's connection page as `return_url`
//!   and this service's `POST /v1/connectors/woocommerce/oauth/callback` as
//!   `callback_url`. The merchant approves in WordPress; WooCommerce posts the
//!   key pair to the callback, where it is verified against the store and
//!   sealed.
//! * **Pasted keys.** The seller posts a consumer key and secret to
//!   `PUT /v1/connectors/{id}/woocommerce/keys` (see [`keys_route`]); the same
//!   verification and sealing follow.
//!
//! After either, a harmless authenticated read decides how the key pair is
//! sent. WooCommerce reads HTTP Basic credentials from the `Authorization`
//! header; some hosts drop that header before PHP sees it, which looks exactly
//! like a wrong key. When Basic is refused and the same key works in the query
//! string (which WooCommerce accepts over HTTPS), that is recorded on the
//! connection and used from then on. Plain-HTTP sites are refused outright.
//!
//! The sealed credential is a small JSON document ([`creds`]) holding the key
//! pair, the auth mode and the per-connection webhook secret, which is
//! generated here and registered with the store's three webhooks
//! (`product.updated`, `order.updated`, `order.deleted`) at connect time.

pub mod api;
pub mod catalog;
pub mod creds;
pub mod http;
pub mod money;
pub mod orders;
pub mod site;
pub mod text;
pub mod webhooks;

use async_trait::async_trait;
use rand::RngCore;
use reqwest::Method;
use serde_json::Value;

use crate::connectors::config::ENV_CONNECTOR_RETURN_URL;
use crate::connectors::provider::{
    CallbackParams, CancelOrder, CatalogPage, CompleteRequest, ConnectCompleted, ConnectRequest,
    ConnectStart, ConnectionContext, ConnectorProvider, ExternalOrder, InboundEvent, OrderPush,
    ProviderError, ProviderKind, RefundRecord, StoreRefundTotals, VerifiedWebhook, WebhookContext,
    WebhookHeaders,
};

use self::api::{Session, REST_ROOT};
use self::creds::{ApiKeys, AuthMode, StoredCredentials};
pub use self::http::WooOptions;
use self::http::{Auth, WooHttp};
use self::site::Site;

pub const APP_NAME: &str = "Pubky Shop";
pub const SCOPE: &str = "read_write";
const VARIATIONS_PER_PAGE: u32 = 100;
const MAX_VARIATION_PAGES: u32 = 10;
const PRODUCT_FIELDS: &str =
    "id,name,type,status,permalink,description,short_description,sku,price,\
manage_stock,stock_quantity,stock_status,images,categories,attributes,variations,parent_id";
const VARIATION_FIELDS: &str =
    "id,status,sku,price,manage_stock,stock_quantity,stock_status,attributes,image";

pub struct WooCommerceProvider {
    pub(crate) http: WooHttp,
    return_url: Option<String>,
}

impl WooCommerceProvider {
    /// Production configuration: HTTPS only, public hosts only. The Shop's
    /// connection page comes from `CONNECTOR_RETURN_URL`.
    pub fn from_env() -> Self {
        let return_url = std::env::var(ENV_CONNECTOR_RETURN_URL)
            .ok()
            .filter(|value| !value.is_empty());
        Self::with_options(WooOptions::default(), return_url)
    }

    pub fn with_options(options: WooOptions, return_url: Option<String>) -> Self {
        Self {
            http: WooHttp::new(options),
            return_url,
        }
    }

    fn policy(&self) -> site::SitePolicy {
        self.http.policy()
    }

    fn site(&self, shop: &str) -> Result<Site, ProviderError> {
        Site::parse(shop, self.policy())
    }

    /// The Shop's origin, for the link in an order's customer note.
    fn shop_origin(&self) -> Option<String> {
        let url = url::Url::parse(self.return_url.as_deref()?).ok()?;
        Some(url.origin().ascii_serialization()).filter(|origin| origin != "null")
    }

    fn return_url_for(&self, params: &Value) -> Result<String, ProviderError> {
        let candidate = self
            .return_url
            .clone()
            .or_else(|| params.get("return_url")?.as_str().map(str::to_string))
            .ok_or_else(|| {
                ProviderError::Invalid("A return_url is needed to send the merchant back.".into())
            })?;
        let url = url::Url::parse(&candidate)
            .map_err(|_| ProviderError::Invalid("The return_url is not a valid URL.".into()))?;
        let secure = url.scheme() == "https" || (self.http.plain_http() && url.scheme() == "http");
        if !secure || url.host_str().is_none() || candidate.len() > 500 {
            return Err(ProviderError::Invalid(
                "The return_url must be an https URL.".into(),
            ));
        }
        Ok(candidate)
    }

    /// What the site must show before a merchant is sent to approve access:
    /// reachable over HTTPS with a valid certificate, WordPress with pretty
    /// permalinks (the `wc-auth` endpoint needs the rewrite rules), WooCommerce
    /// active, and the address WordPress itself reports.
    async fn check_storefront(&self, site: &Site, rotating: bool) -> Result<(), ProviderError> {
        let index = self
            .http
            .send(site, Auth::None, Method::GET, "/wp-json/", &[], None)
            .await?;
        let namespaces = |reply: &http::Reply| -> Option<Value> {
            let value = reply.json().ok()?;
            value.get("namespaces")?.is_array().then_some(value)
        };
        match index.status {
            200 => {}
            401 | 403 => {
                return Err(ProviderError::Invalid(
                    "The store's REST API is blocked (a security plugin or the host is refusing \
                     /wp-json/). Allow the WordPress REST API and try again."
                        .into(),
                ))
            }
            500..=599 => {
                return Err(ProviderError::Unknown(format!(
                    "the store answered {}",
                    index.status
                )))
            }
            status => {
                // With plain permalinks WordPress does not serve /wp-json/ (it
                // 404s or redirects it to itself), but the same REST API answers
                // at /?rest_route=/ . WooCommerce's one-click flow needs the
                // pretty URLs, so say what to change.
                let plain = self
                    .http
                    .send(
                        site,
                        Auth::None,
                        Method::GET,
                        "/",
                        &[("rest_route", "/".into())],
                        None,
                    )
                    .await?;
                if plain.status == 200 && namespaces(&plain).is_some() {
                    return Err(ProviderError::Invalid(
                        "WordPress is using plain permalinks. In WordPress go to Settings > \
                         Permalinks, choose \"Post name\" and save; WooCommerce's one-click \
                         connection needs it."
                            .into(),
                    ));
                }
                let target = [&index, &plain]
                    .into_iter()
                    .filter(|reply| matches!(reply.status, 301 | 302 | 303 | 307 | 308))
                    .find_map(|reply| {
                        url::Url::parse(reply.header("location")?).ok().map(|url| {
                            match url.port() {
                                Some(port) => {
                                    format!("{}:{port}", url.host_str().unwrap_or_default())
                                }
                                None => url.host_str().unwrap_or_default().to_string(),
                            }
                        })
                    });
                return Err(ProviderError::Invalid(match (status, target) {
                    (301 | 302 | 303 | 307 | 308, Some(host)) => format!(
                        "The store redirects to {host}. Use that address (and HTTPS) as the store address."
                    ),
                    (301 | 302 | 303 | 307 | 308, None) => "The store address redirects elsewhere. Use the final address (HTTPS) as the store address.".into(),
                    _ => "The WordPress REST API was not found at this address. Check the store address \
                          and that WordPress is installed there."
                        .into(),
                }));
            }
        }
        let Some(root) = namespaces(&index) else {
            return Err(ProviderError::Invalid(
                "The address answered, but not with the WordPress REST API. Check the store address."
                    .into(),
            ));
        };
        let has_woocommerce = root["namespaces"]
            .as_array()
            .is_some_and(|all| all.iter().any(|name| name.as_str() == Some("wc/v3")));
        if !has_woocommerce {
            return Err(ProviderError::Invalid(
                "WooCommerce was not found on this site (its REST API is missing). Install and \
                 activate WooCommerce, or check that its REST API is not disabled."
                    .into(),
            ));
        }
        if let Some(home) = root.get("home").and_then(Value::as_str) {
            if let Ok(reported) = Site::parse(home, self.policy()) {
                if reported.canonical() != site.canonical() {
                    let message = format!(
                        "WordPress reports this store's address as {}. Use exactly that address.",
                        reported.canonical()
                    );
                    // Replacing the keys of a connected store: keys for the store at
                    // this address are not keys for the store the connection is for.
                    return Err(if rotating {
                        ProviderError::Rejected {
                            code: "wrong_store".into(),
                            message,
                        }
                    } else {
                        ProviderError::Invalid(message)
                    });
                }
            }
        }
        Ok(())
    }

    async fn read_currency(
        &self,
        site: &Site,
        auth: Auth<'_>,
    ) -> Result<http::Reply, ProviderError> {
        self.http
            .send(
                site,
                auth,
                Method::GET,
                &format!("{REST_ROOT}/settings/general/woocommerce_currency"),
                &[("_fields", "id,value".to_string())],
                None,
            )
            .await
    }

    /// Decides how the key pair is sent, by a harmless authenticated read
    /// (the store currency). Basic first; if the store answers 401 the
    /// `Authorization` header may have been dropped, so the same key is tried
    /// in the query string. Only when both are refused are the keys wrong.
    async fn detect_auth(&self, site: &Site, keys: &ApiKeys) -> Result<AuthMode, ProviderError> {
        let basic = self.read_currency(site, Auth::Basic(keys)).await?;
        if basic.status == 200 {
            return Ok(AuthMode::Basic);
        }
        if basic.status != 401 {
            basic.checked()?;
            return Err(ProviderError::Unknown(
                "unexpected answer to the credential check".into(),
            ));
        }
        let query = self.read_currency(site, Auth::Query(keys)).await?;
        if query.status == 200 {
            return Ok(AuthMode::Query);
        }
        if query.status == 401 {
            return Err(ProviderError::Rejected {
                code: "credentials_rejected".into(),
                message: "The store does not accept this consumer key and secret.".into(),
            });
        }
        query.checked()?;
        Err(ProviderError::Unknown(
            "unexpected answer to the credential check".into(),
        ))
    }

    /// Verifies a key pair against the store, registers our webhooks with a
    /// fresh secret and returns the credential to seal. Shared by the
    /// `wc-auth` callback and the pasted-keys route.
    pub async fn adopt_keys(
        &self,
        shop: &str,
        webhook_url: &str,
        keys: ApiKeys,
    ) -> Result<ConnectCompleted, ProviderError> {
        let site = self.site(shop)?;
        let mode = self.detect_auth(&site, &keys).await?;
        let mut secret = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut secret);
        let webhook_secret = hex::encode(secret);
        let session = Session {
            site,
            keys,
            mode,
            webhook_secret,
        };
        webhooks::register(
            &session.api(&self.http),
            webhook_url,
            &session.webhook_secret,
        )
        .await?;
        let stored = StoredCredentials::new(
            session.keys.key.clone(),
            session.keys.secret.clone(),
            session.mode,
            session.webhook_secret.clone(),
        );
        Ok(ConnectCompleted {
            credentials: stored.into_secret(),
            scopes: vec![SCOPE.to_string()],
        })
    }

    async fn store_currency(&self, session: &Session) -> Result<String, ProviderError> {
        let reply = session
            .api(&self.http)
            .get(
                "/settings/general/woocommerce_currency",
                &[("_fields", "id,value".into())],
            )
            .await?
            .json()?;
        reply
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .filter(|code| code.len() == 3 && code.bytes().all(|b| b.is_ascii_uppercase()))
            .ok_or_else(|| ProviderError::Unknown("the store has no readable currency".into()))
    }

    async fn variations_of(
        &self,
        session: &Session,
        product_id: i64,
    ) -> Result<Vec<Value>, ProviderError> {
        let mut all = Vec::new();
        for page in 1..=MAX_VARIATION_PAGES {
            let reply = session
                .api(&self.http)
                .get(
                    &format!("/products/{product_id}/variations"),
                    &[
                        ("per_page", VARIATIONS_PER_PAGE.to_string()),
                        ("page", page.to_string()),
                        ("orderby", "id".into()),
                        ("order", "asc".into()),
                        ("_fields", VARIATION_FIELDS.into()),
                    ],
                )
                .await?;
            let pages: u32 = reply
                .header("x-wp-totalpages")
                .and_then(|value| value.parse().ok())
                .unwrap_or(1);
            if let Value::Array(items) = reply.json()? {
                all.extend(items);
            }
            if page >= pages {
                break;
            }
        }
        all.sort_by_key(|variation| catalog::id_of(variation).unwrap_or(i64::MAX));
        Ok(all)
    }
}

fn callback_fields(callback: &CallbackParams) -> Result<Value, ProviderError> {
    let from_json = serde_json::from_slice::<Value>(&callback.body)
        .ok()
        .filter(Value::is_object);
    if let Some(object) = from_json {
        return Ok(object);
    }
    let mut fields = serde_json::Map::new();
    for (name, value) in url::form_urlencoded::parse(&callback.body) {
        fields.insert(name.into_owned(), Value::String(value.into_owned()));
    }
    for (name, value) in &callback.query {
        fields
            .entry(name.clone())
            .or_insert_with(|| Value::String(value.clone()));
    }
    if fields.is_empty() {
        return Err(ProviderError::Invalid(
            "the callback carried no data".into(),
        ));
    }
    Ok(Value::Object(fields))
}

fn field(fields: &Value, name: &str) -> Option<String> {
    match fields.get(name)? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

#[async_trait]
impl ConnectorProvider for WooCommerceProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Woocommerce
    }

    fn normalize_shop(&self, raw: &str) -> Result<String, ProviderError> {
        Ok(self.site(raw)?.canonical())
    }

    fn required_scopes(&self) -> Vec<String> {
        vec![SCOPE.to_string()]
    }

    async fn connect(&self, request: &ConnectRequest) -> Result<ConnectStart, ProviderError> {
        let site = self.site(&request.shop)?;
        let callback_ok = request.callback_url.starts_with("https://")
            || (self.http.plain_http() && request.callback_url.starts_with("http://"));
        if !callback_ok {
            return Err(ProviderError::Invalid(
                "WooCommerce only posts its keys to an https callback; this deployment's public \
                 base URL is not https."
                    .into(),
            ));
        }
        let return_url = self.return_url_for(&request.params)?;
        self.check_storefront(&site, false).await?;
        let mut authorize =
            url::Url::parse(&site.url(self.http.plain_http(), "/wc-auth/v1/authorize"))
                .map_err(|_| ProviderError::Invalid("The store address is not valid.".into()))?;
        authorize
            .query_pairs_mut()
            .append_pair("app_name", APP_NAME)
            .append_pair("scope", SCOPE)
            .append_pair("user_id", &request.state)
            .append_pair("return_url", &return_url)
            .append_pair("callback_url", &request.callback_url);
        Ok(ConnectStart {
            authorize_url: Some(authorize.into()),
        })
    }

    fn callback_state(&self, callback: &CallbackParams) -> Result<String, ProviderError> {
        let fields = callback_fields(callback)?;
        field(&fields, "user_id")
            .filter(|state| !state.is_empty() && state.len() <= 256)
            .ok_or_else(|| ProviderError::Invalid("the callback has no user_id".into()))
    }

    async fn complete_connect(
        &self,
        request: &CompleteRequest,
    ) -> Result<ConnectCompleted, ProviderError> {
        let fields = callback_fields(&request.callback)?;
        if let Some(permissions) = field(&fields, "key_permissions") {
            if permissions != SCOPE {
                return Err(ProviderError::Rejected {
                    code: "keys_not_read_write".into(),
                    message: "WooCommerce granted a key without Read/Write permission.".into(),
                });
            }
        }
        let keys = ApiKeys::parse(
            &field(&fields, "consumer_key").unwrap_or_default(),
            &field(&fields, "consumer_secret").unwrap_or_default(),
        )?;
        // The pasted-keys route replacing a connected store's keys marks its
        // request, and the store is probed again first: reachable over HTTPS,
        // WooCommerce there, and at the address the connection is for.
        if fields.get("rotate").and_then(Value::as_bool) == Some(true) {
            self.check_storefront(&self.site(&request.pending.shop)?, true)
                .await?;
        }
        self.adopt_keys(&request.pending.shop, &request.webhook_url, keys)
            .await
    }

    async fn disconnect(&self, connection: &ConnectionContext) -> Result<(), ProviderError> {
        let session = Session::open(self, connection)?;
        webhooks::unregister(&session, &self.http).await.map(|_| ())
    }

    async fn list_catalog(
        &self,
        connection: &ConnectionContext,
        page: Option<&str>,
    ) -> Result<CatalogPage, ProviderError> {
        let number: u32 = match page {
            None => 1,
            Some(token) => token
                .strip_prefix('p')
                .and_then(|digits| digits.parse().ok())
                .filter(|n| (1..=100_000).contains(n))
                .ok_or_else(|| ProviderError::Invalid("invalid page".into()))?,
        };
        let session = Session::open(self, connection)?;
        let currency = self.store_currency(&session).await?;
        let exponent = money::exponent_for(&currency);
        let reply = session
            .api(&self.http)
            .get(
                "/products",
                &[
                    ("per_page", self.http.catalog_page_size().to_string()),
                    ("page", number.to_string()),
                    ("orderby", "id".into()),
                    ("order", "asc".into()),
                    ("_fields", PRODUCT_FIELDS.into()),
                ],
            )
            .await?;
        let total_pages: u32 = reply
            .header("x-wp-totalpages")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let Value::Array(items) = reply.json()? else {
            return Err(ProviderError::Unknown(
                "the product list is not an array".into(),
            ));
        };
        let mut products = Vec::with_capacity(items.len());
        for item in &items {
            if !catalog::is_sellable_type(item) {
                continue;
            }
            let variations = if catalog::is_variable(item) {
                match catalog::id_of(item) {
                    Some(id) => self.variations_of(&session, id).await?,
                    None => continue,
                }
            } else {
                Vec::new()
            };
            if let Some(product) =
                catalog::normalize_product(item, &variations, &currency, exponent)
            {
                products.push(product);
            }
        }
        Ok(CatalogPage {
            products,
            next_page: (number < total_pages).then(|| format!("p{}", number + 1)),
        })
    }

    async fn push_order(
        &self,
        connection: &ConnectionContext,
        order: &OrderPush,
    ) -> Result<ExternalOrder, ProviderError> {
        let session = Session::open(self, connection)?;
        orders::push(
            &session.api(&self.http),
            order,
            self.shop_origin().as_deref(),
        )
        .await
    }

    async fn find_pushed_order(
        &self,
        connection: &ConnectionContext,
        pubky_order_id: uuid::Uuid,
    ) -> Result<Option<ExternalOrder>, ProviderError> {
        let session = Session::open(self, connection)?;
        orders::find(&session.api(&self.http), pubky_order_id).await
    }

    async fn cancel_order(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        cancel: &CancelOrder,
    ) -> Result<(), ProviderError> {
        let session = Session::open(self, connection)?;
        orders::cancel(&session.api(&self.http), order, cancel).await
    }

    async fn refunded_total(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
    ) -> Result<StoreRefundTotals, ProviderError> {
        let session = Session::open(self, connection)?;
        orders::refunded_total(&session.api(&self.http), order).await
    }

    async fn record_refund(
        &self,
        connection: &ConnectionContext,
        order: &ExternalOrder,
        refund: &RefundRecord,
    ) -> Result<(), ProviderError> {
        let session = Session::open(self, connection)?;
        orders::record_refund(&session.api(&self.http), order, refund).await
    }

    fn webhook_shop(&self, headers: &WebhookHeaders, body: &[u8]) -> Result<String, ProviderError> {
        webhooks::shop_of(headers, body, self.policy())
    }

    fn verify_webhook(
        &self,
        context: &WebhookContext,
        body: &[u8],
        headers: &WebhookHeaders,
    ) -> Result<VerifiedWebhook, ProviderError> {
        webhooks::verify(context, body, headers)
    }

    fn parse_inbound(
        &self,
        webhook: &VerifiedWebhook,
        body: &[u8],
    ) -> Result<InboundEvent, ProviderError> {
        webhooks::parse(webhook, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn provider(return_url: Option<&str>) -> WooCommerceProvider {
        WooCommerceProvider::with_options(WooOptions::default(), return_url.map(str::to_string))
    }

    #[test]
    fn the_configured_return_url_wins_over_the_request() {
        let configured = provider(Some("https://shop.pubky.app/marketplace/connections"));
        let url = configured
            .return_url_for(&json!({"return_url": "https://elsewhere.example/"}))
            .unwrap();
        assert_eq!(url, "https://shop.pubky.app/marketplace/connections");
        assert_eq!(
            configured.shop_origin().as_deref(),
            Some("https://shop.pubky.app")
        );
    }

    #[test]
    fn without_configuration_the_request_names_the_return_page() {
        let open = provider(None);
        assert_eq!(
            open.return_url_for(&json!({"return_url": "https://shop.pubky.app/connections"}))
                .unwrap(),
            "https://shop.pubky.app/connections"
        );
        assert_eq!(open.shop_origin(), None);
        for bad in [
            json!({}),
            json!({"return_url": "javascript:alert(1)"}),
            json!({"return_url": "nonsense"}),
        ] {
            assert!(
                matches!(open.return_url_for(&bad), Err(ProviderError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_callback_is_read_from_json_a_form_or_the_query() {
        let json_body = CallbackParams {
            body: serde_json::to_vec(&json!({"user_id": "n1", "consumer_key": "ck_x"})).unwrap(),
            ..CallbackParams::default()
        };
        let fields = callback_fields(&json_body).unwrap();
        assert_eq!(field(&fields, "user_id").as_deref(), Some("n1"));
        assert_eq!(provider(None).callback_state(&json_body).unwrap(), "n1");

        let form = CallbackParams {
            body: b"user_id=n2&consumer_key=ck_y&key_id=7".to_vec(),
            ..CallbackParams::default()
        };
        let fields = callback_fields(&form).unwrap();
        assert_eq!(field(&fields, "consumer_key").as_deref(), Some("ck_y"));
        assert_eq!(field(&fields, "key_id").as_deref(), Some("7"));

        let query = CallbackParams {
            query: [("user_id".to_string(), "n3".to_string())]
                .into_iter()
                .collect(),
            ..CallbackParams::default()
        };
        assert_eq!(provider(None).callback_state(&query).unwrap(), "n3");

        assert!(provider(None)
            .callback_state(&CallbackParams::default())
            .is_err());
        let blank = CallbackParams {
            body: serde_json::to_vec(&json!({"user_id": ""})).unwrap(),
            ..CallbackParams::default()
        };
        assert!(provider(None).callback_state(&blank).is_err());
    }
}
