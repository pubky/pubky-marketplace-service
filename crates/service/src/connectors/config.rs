//! Connector settings from the environment. Everything is off unless
//! `CONNECTORS_ENABLED` is true, so a deployment that never sets it behaves
//! exactly as before.

use std::time::Duration;

pub const ENV_CONNECTORS_ENABLED: &str = "CONNECTORS_ENABLED";
pub const ENV_CONNECTOR_PUBLIC_BASE_URL: &str = "CONNECTOR_PUBLIC_BASE_URL";
pub const ENV_CONNECTOR_RETURN_URL: &str = "CONNECTOR_RETURN_URL";
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
/// A pending connect lives 30 minutes from the seller's request.
pub const CONNECT_STATE_TTL_SECONDS: i64 = 1_800;
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

    /// The Shopify webhook URL of an app, as configured in the Dev Dashboard.
    pub fn webhook_url(&self, provider: &str, app_key: Option<&str>) -> String {
        match app_key {
            Some(app_key) => format!(
                "{}/v1/connectors/webhooks/{provider}/{app_key}",
                self.public_base_url
            ),
            None => format!("{}/v1/connectors/webhooks/{provider}", self.public_base_url),
        }
    }

    /// The URL the store calls back to: the app's OAuth callback for Shopify,
    /// the WooCommerce `callback_url`.
    pub fn callback_url(&self, provider: &str, app_key: Option<&str>) -> String {
        match app_key {
            Some(app_key) => format!(
                "{}/v1/connectors/{provider}/apps/{app_key}/oauth/callback",
                self.public_base_url
            ),
            None => format!(
                "{}/v1/connectors/{provider}/oauth/callback",
                self.public_base_url
            ),
        }
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
        assert_eq!(
            settings.callback_url("shopify", Some("john")),
            "https://service.example/v1/connectors/shopify/apps/john/oauth/callback"
        );
        assert_eq!(
            settings.callback_url("woocommerce", None),
            "https://service.example/v1/connectors/woocommerce/oauth/callback"
        );
        assert_eq!(
            settings.webhook_url("shopify", Some("john")),
            "https://service.example/v1/connectors/webhooks/shopify/john"
        );
        assert_eq!(
            settings.webhook_url("woocommerce", None),
            "https://service.example/v1/connectors/webhooks/woocommerce"
        );
    }

    #[test]
    fn a_pending_connect_lives_thirty_minutes() {
        assert_eq!(CONNECT_STATE_TTL_SECONDS, 30 * 60);
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
