//! What the framework seals for a WooCommerce connection: the REST key pair,
//! how it authenticates against this particular host, and the secret that
//! signs the store's webhooks to us.

use serde::{Deserialize, Serialize};

use crate::connectors::provider::{CredentialSecret, ProviderError};

const VERSION: u8 = 1;

/// How the key pair reaches WooCommerce. Basic auth is the normal way; a host
/// that drops the `Authorization` header before PHP sees it needs the key pair
/// in the query string instead (over HTTPS, which WooCommerce supports).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Basic,
    Query,
}

#[derive(Serialize, Deserialize)]
pub struct StoredCredentials {
    v: u8,
    pub consumer_key: String,
    pub consumer_secret: String,
    pub auth: AuthMode,
    pub webhook_secret: String,
}

impl std::fmt::Debug for StoredCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StoredCredentials(<redacted>)")
    }
}

impl StoredCredentials {
    pub fn new(
        consumer_key: String,
        consumer_secret: String,
        auth: AuthMode,
        webhook_secret: String,
    ) -> Self {
        Self {
            v: VERSION,
            consumer_key,
            consumer_secret,
            auth,
            webhook_secret,
        }
    }

    pub fn into_secret(self) -> CredentialSecret {
        CredentialSecret::new(serde_json::to_vec(&self).expect("credentials serialize"))
    }

    pub fn open(secret: &CredentialSecret) -> Result<Self, ProviderError> {
        let parsed: Self =
            serde_json::from_slice(secret.expose()).map_err(|_| ProviderError::Unauthorized)?;
        if parsed.v != VERSION
            || parsed.consumer_key.is_empty()
            || parsed.consumer_secret.is_empty()
        {
            return Err(ProviderError::Unauthorized);
        }
        Ok(parsed)
    }
}

/// A REST key pair as pasted or posted. Printed as nothing.
#[derive(Clone)]
pub struct ApiKeys {
    pub key: String,
    pub secret: String,
}

impl std::fmt::Debug for ApiKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ApiKeys(<redacted>)")
    }
}

impl ApiKeys {
    /// `ck_` and `cs_` keys are 43 characters of letters, digits and
    /// underscores. A looser shape is refused here so nothing odd reaches a
    /// header or a query string.
    pub fn parse(key: &str, secret: &str) -> Result<Self, ProviderError> {
        let well_formed = |value: &str, prefix: &str| {
            value.starts_with(prefix)
                && (10..=100).contains(&value.len())
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        };
        if !well_formed(key, "ck_") || !well_formed(secret, "cs_") {
            return Err(ProviderError::Invalid(
                "The consumer key must start with ck_ and the consumer secret with cs_."
                    .to_string(),
            ));
        }
        Ok(Self {
            key: key.to_string(),
            secret: secret.to_string(),
        })
    }
}

impl From<&StoredCredentials> for ApiKeys {
    fn from(stored: &StoredCredentials) -> Self {
        Self {
            key: stored.consumer_key.clone(),
            secret: stored.consumer_secret.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> StoredCredentials {
        StoredCredentials::new(
            "ck_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            "cs_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            AuthMode::Query,
            "w".repeat(64),
        )
    }

    #[test]
    fn credentials_round_trip_and_never_print() {
        let sealed = keys().into_secret();
        let opened = StoredCredentials::open(&sealed).unwrap();
        assert_eq!(opened.auth, AuthMode::Query);
        assert_eq!(opened.webhook_secret.len(), 64);
        let printed = format!("{opened:?} {sealed:?} {:?}", ApiKeys::from(&opened));
        assert!(!printed.contains("ck_aaaa"));
        assert!(!printed.contains("cs_bbbb"));
    }

    #[test]
    fn garbage_credentials_read_as_revoked() {
        for bytes in [&b""[..], b"{}", b"not json", br#"{"v":2}"#] {
            let secret = CredentialSecret::new(bytes.to_vec());
            assert!(matches!(
                StoredCredentials::open(&secret),
                Err(ProviderError::Unauthorized)
            ));
        }
    }

    #[test]
    fn key_shapes_are_checked() {
        assert!(ApiKeys::parse("ck_abc123DEF456", "cs_abc123DEF456").is_ok());
        for (key, secret) in [
            ("abc", "cs_abc123DEF456"),
            ("ck_abc123DEF456", "ck_abc123DEF456"),
            ("ck_abc 123 456", "cs_abc123DEF456"),
            ("ck_a&b=c123456", "cs_abc123DEF456"),
            ("ck_", "cs_"),
        ] {
            assert!(ApiKeys::parse(key, secret).is_err(), "{key} {secret}");
        }
    }
}
