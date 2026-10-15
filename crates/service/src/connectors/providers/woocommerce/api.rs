//! A WooCommerce REST v3 session: the site, the key pair and how it is sent.

use reqwest::Method;
use serde_json::Value;

use crate::connectors::provider::{ConnectionContext, ProviderError};

use super::creds::{ApiKeys, AuthMode, StoredCredentials};
use super::http::{Auth, Reply, WooHttp};
use super::site::Site;
use super::WooCommerceProvider;

pub const REST_ROOT: &str = "/wp-json/wc/v3";
/// The most pages [`Api::list_all`] reads. A list longer than this many
/// pages is an error, not a silent truncation.
pub const MAX_LIST_PAGES: u32 = 50;

/// A connection's opened credential and site, for one provider call.
pub struct Session {
    pub site: Site,
    pub keys: ApiKeys,
    pub mode: AuthMode,
    pub webhook_secret: String,
}

impl Session {
    pub fn open(
        provider: &WooCommerceProvider,
        connection: &ConnectionContext,
    ) -> Result<Self, ProviderError> {
        let stored = StoredCredentials::open(&connection.credentials)?;
        let site = Site::parse(&connection.shop, provider.http.policy())
            .map_err(|_| ProviderError::Unauthorized)?;
        Ok(Self {
            keys: ApiKeys::from(&stored),
            mode: stored.auth,
            webhook_secret: stored.webhook_secret,
            site,
        })
    }

    pub fn api<'a>(&'a self, http: &'a WooHttp) -> Api<'a> {
        Api {
            http,
            site: &self.site,
            auth: match self.mode {
                AuthMode::Basic => Auth::Basic(&self.keys),
                AuthMode::Query => Auth::Query(&self.keys),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub struct Api<'a> {
    pub http: &'a WooHttp,
    pub site: &'a Site,
    pub auth: Auth<'a>,
}

impl Api<'_> {
    async fn call(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Reply, ProviderError> {
        self.http
            .send(
                self.site,
                self.auth,
                method,
                &format!("{REST_ROOT}{endpoint}"),
                query,
                body,
            )
            .await?
            .checked()
    }

    pub async fn get(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Reply, ProviderError> {
        self.call(Method::GET, endpoint, query, None).await
    }

    /// Every item of a list endpoint, following its pages (`per_page` is the
    /// provider's list page size). Stops at the last page the store reports,
    /// or at a short page when it reports none, and refuses a list of more than
    /// [`MAX_LIST_PAGES`] pages instead of acting on part of it.
    pub async fn list_all(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<Value>, ProviderError> {
        let per_page = self.http.list_page_size() as usize;
        let mut all = Vec::new();
        for page in 1..=MAX_LIST_PAGES {
            let mut paged = query.to_vec();
            paged.push(("per_page", per_page.to_string()));
            paged.push(("page", page.to_string()));
            let reply = self.get(endpoint, &paged).await?;
            let total_pages: Option<u32> = reply
                .header("x-wp-totalpages")
                .and_then(|value| value.trim().parse().ok());
            let Value::Array(items) = reply.json()? else {
                return Err(ProviderError::Unknown(format!(
                    "the store's {endpoint} list is not an array"
                )));
            };
            let received = items.len();
            all.extend(items);
            let finished = received == 0
                || match total_pages {
                    Some(total) => page >= total,
                    None => received < per_page,
                };
            if finished {
                return Ok(all);
            }
        }
        Err(ProviderError::Rejected {
            code: "list_too_long".into(),
            message: format!("The store's {endpoint} list is longer than {MAX_LIST_PAGES} pages."),
        })
    }

    /// A read whose 404 is an answer ("it is gone"), not a failure.
    pub async fn get_optional(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Option<Reply>, ProviderError> {
        let reply = self
            .http
            .send(
                self.site,
                self.auth,
                Method::GET,
                &format!("{REST_ROOT}{endpoint}"),
                query,
                None,
            )
            .await?;
        if reply.status == 404 {
            return Ok(None);
        }
        reply.checked().map(Some)
    }

    pub async fn post(&self, endpoint: &str, body: &Value) -> Result<Reply, ProviderError> {
        self.call(Method::POST, endpoint, &[], Some(body)).await
    }

    pub async fn put(&self, endpoint: &str, body: &Value) -> Result<Reply, ProviderError> {
        self.call(Method::PUT, endpoint, &[], Some(body)).await
    }

    pub async fn delete(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Reply, ProviderError> {
        self.call(Method::DELETE, endpoint, query, None).await
    }

    /// A delete whose 404 means it is already gone (`false`), not a failure.
    pub async fn delete_if_present(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<bool, ProviderError> {
        let reply = self
            .http
            .send(
                self.site,
                self.auth,
                Method::DELETE,
                &format!("{REST_ROOT}{endpoint}"),
                query,
                None,
            )
            .await?;
        if reply.status == 404 {
            return Ok(false);
        }
        reply.checked().map(|_| true)
    }
}
