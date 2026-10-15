//! The provider apps a deployment is configured with.
//!
//! Interim Shopify custom-distribution apps are one per merchant store, and
//! the public app joins them later, so a deployment holds several. They are
//! named in `SHOPIFY_APPS` (a comma-separated list of keys), and each key has
//! `SHOPIFY_APP_<KEY>_CLIENT_ID` and `SHOPIFY_APP_<KEY>_CLIENT_SECRET`, where
//! `<KEY>` is the key upper-cased with `-` as `_`. A key is 1 to 32 characters
//! of `a-z`, `0-9` and `-`, starting with a letter or digit; it appears in
//! the app's URLs (`/v1/connectors/shopify/apps/{app_key}/launch`) and on the
//! connection (`app_key`). A connection's signature checks use its own app's
//! secret.
//!
//! The framework resolves the app and hands it to the provider as an
//! [`AppContext`], so a provider keeps no configuration of its own.

use std::collections::BTreeMap;

use super::provider::{AppContext, CredentialSecret, ProviderKind};

pub const ENV_SHOPIFY_APPS: &str = "SHOPIFY_APPS";

struct App {
    client_id: String,
    client_secret: String,
}

/// Which apps exist per provider. Secrets are held here and copied into an
/// [`AppContext`] only for one call.
#[derive(Default)]
pub struct AppRegistry {
    apps: BTreeMap<(ProviderKind, String), App>,
}

impl std::fmt::Debug for AppRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppRegistry")
            .field(
                "apps",
                &self
                    .apps
                    .keys()
                    .map(|(kind, key)| format!("{kind}/{key}"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

pub fn valid_app_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    (1..=32).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn env_prefix(app_key: &str) -> String {
    format!(
        "SHOPIFY_APP_{}",
        app_key.to_ascii_uppercase().replace('-', "_")
    )
}

impl AppRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(
        mut self,
        kind: ProviderKind,
        app_key: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Self {
        self.apps.insert(
            (kind, app_key.to_string()),
            App {
                client_id: client_id.to_string(),
                client_secret: client_secret.to_string(),
            },
        );
        self
    }

    /// Reads `SHOPIFY_APPS` and each listed app's id and secret. Missing
    /// values, a malformed key or two keys that map to the same variable
    /// name are startup errors.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let mut registry = Self::new();
        let Some(list) = lookup(ENV_SHOPIFY_APPS) else {
            return Ok(registry);
        };
        let mut prefixes = std::collections::BTreeSet::new();
        for key in list.split(',').map(str::trim).filter(|key| !key.is_empty()) {
            if !valid_app_key(key) {
                anyhow::bail!(
                    "{ENV_SHOPIFY_APPS} lists {key:?}: a key is 1 to 32 characters of a-z, 0-9 \
                     and -"
                );
            }
            let prefix = env_prefix(key);
            if !prefixes.insert(prefix.clone()) {
                anyhow::bail!("{ENV_SHOPIFY_APPS} lists two keys that share {prefix}_*");
            }
            let id_name = format!("{prefix}_CLIENT_ID");
            let secret_name = format!("{prefix}_CLIENT_SECRET");
            let client_id = lookup(&id_name)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("{id_name} is required for the {key} app"))?;
            let client_secret = lookup(&secret_name)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("{secret_name} is required for the {key} app"))?;
            registry = registry.with(
                ProviderKind::Shopify,
                key,
                client_id.trim(),
                client_secret.trim(),
            );
        }
        Ok(registry)
    }

    pub fn keys(&self, kind: ProviderKind) -> Vec<String> {
        self.apps
            .keys()
            .filter(|(candidate, _)| *candidate == kind)
            .map(|(_, key)| key.clone())
            .collect()
    }

    pub fn has_apps(&self, kind: ProviderKind) -> bool {
        self.apps.keys().any(|(candidate, _)| *candidate == kind)
    }

    pub fn context(&self, kind: ProviderKind, app_key: &str) -> Option<AppContext> {
        self.apps
            .get(&(kind, app_key.to_string()))
            .map(|app| AppContext {
                app_key: app_key.to_string(),
                client_id: app.client_id.clone(),
                client_secret: CredentialSecret::new(app.client_secret.clone().into_bytes()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn apps_are_read_from_the_documented_names() {
        let registry = AppRegistry::from_lookup(lookup(&[
            ("SHOPIFY_APPS", "john, pilot-1"),
            ("SHOPIFY_APP_JOHN_CLIENT_ID", "id-john"),
            ("SHOPIFY_APP_JOHN_CLIENT_SECRET", "secret-john"),
            ("SHOPIFY_APP_PILOT_1_CLIENT_ID", "id-pilot"),
            ("SHOPIFY_APP_PILOT_1_CLIENT_SECRET", "secret-pilot"),
        ]))
        .expect("apps parse");
        assert_eq!(
            registry.keys(ProviderKind::Shopify),
            vec!["john", "pilot-1"]
        );
        assert!(registry.keys(ProviderKind::Woocommerce).is_empty());
        let app = registry
            .context(ProviderKind::Shopify, "pilot-1")
            .expect("app");
        assert_eq!(app.client_id, "id-pilot");
        assert_eq!(app.client_secret.expose(), b"secret-pilot");
        assert!(registry.context(ProviderKind::Shopify, "nobody").is_none());
        let shown = format!("{registry:?} {app:?}");
        assert!(!shown.contains("secret-"), "{shown}");
    }

    #[test]
    fn missing_values_and_bad_keys_are_startup_errors() {
        assert!(AppRegistry::from_lookup(lookup(&[]))
            .expect("none")
            .keys(ProviderKind::Shopify)
            .is_empty());
        for pairs in [
            &[("SHOPIFY_APPS", "john")][..],
            &[
                ("SHOPIFY_APPS", "john"),
                ("SHOPIFY_APP_JOHN_CLIENT_ID", "x"),
            ][..],
            &[("SHOPIFY_APPS", "Bad Key")][..],
            &[("SHOPIFY_APPS", "-lead")][..],
        ] {
            let pairs: &'static [(&'static str, &'static str)] =
                Box::leak(pairs.to_vec().into_boxed_slice());
            assert!(
                AppRegistry::from_lookup(lookup(pairs)).is_err(),
                "{pairs:?}"
            );
        }
        let colliding: &'static [(&'static str, &'static str)] = &[
            ("SHOPIFY_APPS", "a-b,a_b"),
            ("SHOPIFY_APP_A_B_CLIENT_ID", "x"),
            ("SHOPIFY_APP_A_B_CLIENT_SECRET", "y"),
        ];
        assert!(AppRegistry::from_lookup(lookup(colliding)).is_err());
    }

    #[test]
    fn app_keys_are_url_safe() {
        assert!(valid_app_key("john"));
        assert!(valid_app_key("pilot-1"));
        assert!(!valid_app_key(""));
        assert!(!valid_app_key("Upper"));
        assert!(!valid_app_key("a/b"));
        assert!(!valid_app_key(&"a".repeat(33)));
    }
}
