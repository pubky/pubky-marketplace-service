//! Connector settings from the environment. Everything is off unless
//! `CONNECTORS_ENABLED` is true, so a deployment that never sets it behaves
//! exactly as before.

use std::time::Duration;

use uuid::Uuid;

pub const ENV_CONNECTORS_ENABLED: &str = "CONNECTORS_ENABLED";
pub const ENV_CONNECTOR_PUBLIC_BASE_URL: &str = "CONNECTOR_PUBLIC_BASE_URL";
pub const ENV_CONNECTOR_RETURN_URL: &str = "CONNECTOR_RETURN_URL";
/// Reserved for Pubky's own public Shopify app, the one app type whose
/// credentials will live in the environment. Nothing reads them yet.
pub const ENV_SHOPIFY_PUBLIC_APP_CLIENT_ID: &str = "SHOPIFY_PUBLIC_APP_CLIENT_ID";
pub const ENV_SHOPIFY_PUBLIC_APP_CLIENT_SECRET: &str = "SHOPIFY_PUBLIC_APP_CLIENT_SECRET";
pub const ENV_CONNECTOR_PUSH_LEASE_SECONDS: &str = "CONNECTOR_PUSH_LEASE_SECONDS";
pub const ENV_CONNECTOR_PROVIDER_TIMEOUT_SECONDS: &str = "CONNECTOR_PROVIDER_TIMEOUT_SECONDS";
pub const ENV_CONNECTOR_MAX_ATTEMPTS: &str = "CONNECTOR_MAX_ATTEMPTS";
pub const ENV_CONNECTOR_BACKOFF_BASE_SECONDS: &str = "CONNECTOR_BACKOFF_BASE_SECONDS";
pub const ENV_CONNECTOR_BACKOFF_MAX_SECONDS: &str = "CONNECTOR_BACKOFF_MAX_SECONDS";
pub const ENV_CONNECTOR_STOCK_POLL_SECONDS: &str = "CONNECTOR_STOCK_POLL_SECONDS";
pub const ENV_CONNECTOR_WORKER_TICK_MILLIS: &str = "CONNECTOR_WORKER_TICK_MILLIS";

pub const MAX_CONNECTIONS_PER_SELLER: i64 = 5;
pub const MAX_MAPPINGS_PER_REQUEST: usize = 5_000;
pub const MAX_SELECTED_PRODUCTS: usize = 5_000;
/// A draft connection lives 24 hours from the seller's request; a draft that
/// is never completed is deleted with whatever was saved on it.
pub const DRAFT_TTL_SECONDS: i64 = 24 * 3600;
/// A single-use OAuth `state` lives 15 minutes from the launch that issued it
/// (a WooCommerce nonce lives as long as its draft).
pub const OAUTH_STATE_TTL_SECONDS: i64 = 900;
/// A merchant app's secret outlives a disconnect or an uninstall by at most
/// this long, so Shopify's privacy webhooks that follow an uninstall can still
/// be verified. `shop/redact` ends it sooner.
pub const APP_SECRET_RETENTION_SECONDS: i64 = 30 * 24 * 3600;
pub const WEBHOOK_BODY_LIMIT: usize = 256 * 1024;
pub const CALLBACK_BODY_LIMIT: usize = 64 * 1024;
pub const MAX_CATALOG_PAGES_PER_POLL: usize = 50;

#[derive(Debug, Clone)]
pub struct ConnectorSettings {
    /// `CONNECTOR_PUBLIC_BASE_URL`, without a trailing slash: the origin
    /// providers call back and deliver webhooks to.
    pub public_base_url: String,
    /// Where the browser goes after a launch or callback
    /// (`CONNECTOR_RETURN_URL`, the Shop's connection page); the routes
    /// answer JSON when unset.
    pub return_url: Option<String>,
    /// How long a claimed push or action is owned before another worker may
    /// treat its outcome as unknown. Always longer than twice the provider
    /// timeout, so a slow call is dead before its lease lapses.
    pub lease: Duration,
    pub provider_timeout: Duration,
    pub max_attempts: i32,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub stock_poll_interval: Duration,
    pub tick: Duration,
}

impl ConnectorSettings {
    pub fn new(public_base_url: &str) -> anyhow::Result<Self> {
        let url = url::Url::parse(public_base_url).map_err(|_| {
            anyhow::anyhow!("{ENV_CONNECTOR_PUBLIC_BASE_URL} must be an absolute URL")
        })?;
        if !matches!(url.scheme(), "https" | "http") || url.host_str().is_none() {
            anyhow::bail!("{ENV_CONNECTOR_PUBLIC_BASE_URL} must be an http(s) URL");
        }
        Ok(Self {
            public_base_url: public_base_url.trim_end_matches('/').to_string(),
            return_url: None,
            lease: Duration::from_secs(300),
            provider_timeout: Duration::from_secs(30),
            max_attempts: 12,
            backoff_base: Duration::from_secs(30),
            backoff_max: Duration::from_secs(3600),
            stock_poll_interval: Duration::from_secs(900),
            tick: Duration::from_millis(2000),
        })
    }

    /// `None` when `CONNECTORS_ENABLED` is unset or false. When enabled, the
    /// public base URL is required and every tuning value is range checked.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let enabled = match std::env::var(ENV_CONNECTORS_ENABLED) {
            Ok(raw) => parse_flag(&raw)?,
            Err(_) => false,
        };
        if !enabled {
            return Ok(None);
        }
        let base = std::env::var(ENV_CONNECTOR_PUBLIC_BASE_URL).map_err(|_| {
            anyhow::anyhow!(
                "{ENV_CONNECTOR_PUBLIC_BASE_URL} is required when connectors are enabled"
            )
        })?;
        let mut settings = Self::new(&base)?;
        settings.return_url = std::env::var(ENV_CONNECTOR_RETURN_URL)
            .ok()
            .filter(|value| !value.is_empty());
        if let Some(return_url) = &settings.return_url {
            url::Url::parse(return_url).map_err(|_| {
                anyhow::anyhow!("{ENV_CONNECTOR_RETURN_URL} must be an absolute URL")
            })?;
        }
        settings.lease = seconds(ENV_CONNECTOR_PUSH_LEASE_SECONDS, 300, 60, 3600)?;
        settings.provider_timeout = seconds(ENV_CONNECTOR_PROVIDER_TIMEOUT_SECONDS, 30, 1, 120)?;
        settings.max_attempts = bounded(ENV_CONNECTOR_MAX_ATTEMPTS, 12, 1, 100)? as i32;
        settings.backoff_base = seconds(ENV_CONNECTOR_BACKOFF_BASE_SECONDS, 30, 1, 3600)?;
        settings.backoff_max = seconds(ENV_CONNECTOR_BACKOFF_MAX_SECONDS, 3600, 1, 86_400)?;
        settings.stock_poll_interval = seconds(ENV_CONNECTOR_STOCK_POLL_SECONDS, 900, 60, 86_400)?;
        settings.tick = Duration::from_millis(bounded(
            ENV_CONNECTOR_WORKER_TICK_MILLIS,
            2000,
            100,
            60_000,
        )?);
        settings.validate()?;
        Ok(Some(settings))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.lease <= self.provider_timeout * 2 {
            anyhow::bail!(
                "{ENV_CONNECTOR_PUSH_LEASE_SECONDS} must exceed twice \
                 {ENV_CONNECTOR_PROVIDER_TIMEOUT_SECONDS}"
            );
        }
        if self.backoff_max < self.backoff_base {
            anyhow::bail!(
                "{ENV_CONNECTOR_BACKOFF_MAX_SECONDS} must not be below \
                 {ENV_CONNECTOR_BACKOFF_BASE_SECONDS}"
            );
        }
        Ok(())
    }

    /// A Shopify connection's own URLs, shown to the merchant to configure
    /// their app: `launch` (App URL), `oauth/callback` (allowed redirection
    /// URL) and `webhooks`. The connection id is random and unguessable.
    pub fn shopify_launch_url(&self, connection_id: Uuid) -> String {
        format!(
            "{}/v1/connectors/shopify/c/{connection_id}/launch",
            self.public_base_url
        )
    }

    pub fn shopify_callback_url(&self, connection_id: Uuid) -> String {
        format!(
            "{}/v1/connectors/shopify/c/{connection_id}/oauth/callback",
            self.public_base_url
        )
    }

    pub fn shopify_webhook_url(&self, connection_id: Uuid) -> String {
        format!(
            "{}/v1/connectors/shopify/c/{connection_id}/webhooks",
            self.public_base_url
        )
    }

    /// WooCommerce's fixed callback URL (one for the deployment: the draft is
    /// found by the nonce in `user_id`) and each connection's own webhook URL.
    pub fn woocommerce_callback_url(&self) -> String {
        format!(
            "{}/v1/connectors/woocommerce/oauth/callback",
            self.public_base_url
        )
    }

    pub fn woocommerce_webhook_url(&self, connection_id: Uuid) -> String {
        format!(
            "{}/v1/connectors/webhooks/woocommerce/c/{connection_id}",
            self.public_base_url
        )
    }

    /// Delay before the next attempt after `attempts` failed ones:
    /// exponential from the base, capped.
    pub fn backoff(&self, attempts: i32) -> Duration {
        let exponent = attempts.saturating_sub(1).clamp(0, 20) as u32;
        let seconds = self
            .backoff_base
            .as_secs()
            .saturating_mul(1u64 << exponent)
            .min(self.backoff_max.as_secs());
        Duration::from_secs(seconds)
    }
}

fn parse_flag(raw: &str) -> anyhow::Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "" | "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{ENV_CONNECTORS_ENABLED} must be true or false"),
    }
}

fn bounded(name: &str, default: u64, minimum: u64, maximum: u64) -> anyhow::Result<u64> {
    let value = match std::env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?,
        Err(_) => default,
    };
    if !(minimum..=maximum).contains(&value) {
        anyhow::bail!("{name} must be between {minimum} and {maximum}");
    }
    Ok(value)
}

fn seconds(name: &str, default: u64, minimum: u64, maximum: u64) -> anyhow::Result<Duration> {
    bounded(name, default, minimum, maximum).map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_the_base_and_caps() {
        let settings = ConnectorSettings::new("https://service.example").expect("settings");
        assert_eq!(settings.backoff(1), Duration::from_secs(30));
        assert_eq!(settings.backoff(2), Duration::from_secs(60));
        assert_eq!(settings.backoff(3), Duration::from_secs(120));
        assert_eq!(settings.backoff(40), Duration::from_secs(3600));
        assert_eq!(settings.backoff(0), Duration::from_secs(30));
    }

    #[test]
    fn urls_follow_the_fixed_route_table() {
        assert!(ConnectorSettings::new("not a url").is_err());
        assert!(ConnectorSettings::new("ftp://service.example").is_err());
        let settings = ConnectorSettings::new("https://service.example/").expect("settings");
        let id = Uuid::nil();
        assert_eq!(
            settings.shopify_launch_url(id),
            "https://service.example/v1/connectors/shopify/c/00000000-0000-0000-0000-000000000000/launch"
        );
        assert_eq!(
            settings.shopify_callback_url(id),
            "https://service.example/v1/connectors/shopify/c/00000000-0000-0000-0000-000000000000/oauth/callback"
        );
        assert_eq!(
            settings.shopify_webhook_url(id),
            "https://service.example/v1/connectors/shopify/c/00000000-0000-0000-0000-000000000000/webhooks"
        );
        assert_eq!(
            settings.woocommerce_callback_url(),
            "https://service.example/v1/connectors/woocommerce/oauth/callback"
        );
        assert_eq!(
            settings.woocommerce_webhook_url(id),
            "https://service.example/v1/connectors/webhooks/woocommerce/c/00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn a_draft_lives_a_day_and_an_oauth_state_a_quarter_hour() {
        assert_eq!(DRAFT_TTL_SECONDS, 24 * 3600);
        assert_eq!(OAUTH_STATE_TTL_SECONDS, 15 * 60);
        assert_eq!(APP_SECRET_RETENTION_SECONDS, 30 * 24 * 3600);
    }

    #[test]
    fn lease_must_outlive_two_provider_calls() {
        let mut settings = ConnectorSettings::new("https://service.example").expect("settings");
        settings.validate().expect("defaults are coherent");
        settings.lease = Duration::from_secs(50);
        assert!(settings.validate().is_err());
    }

    #[test]
    fn flag_parsing_is_strict() {
        assert!(parse_flag("true").expect("true"));
        assert!(parse_flag("1").expect("1"));
        assert!(!parse_flag("false").expect("false"));
        assert!(!parse_flag("").expect("empty"));
        assert!(parse_flag("maybe").is_err());
    }
}
