//! A WooCommerce REST v3 session: the site, the key pair and how it is sent.

use reqwest::Method;
use serde_json::Value;

use crate::connectors::provider::{ConnectionContext, ProviderError};

use super::creds::{ApiKeys, AuthMode, StoredCredentials};
use super::http::{Auth, Reply, WooHttp};
use super::site::Site;
use super::WooCommerceProvider;

pub const REST_ROOT: &str = "/wp-json/wc/v3";

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
}
