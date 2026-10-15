//! The sealing keys for merchant store credentials and connector recipient
//! keys.
//!
//! One rotating family (`CONNECTOR_CREDENTIAL_ENCRYPTION_KEY`, with
//! `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS` for rotation) over the
//! shared [`crate::seal`] construction. Associated data binds each sealed
//! value to its owner so a ciphertext cannot be moved to another connection:
//!
//! * store credential (token, API keys): `connector-cred|v1|{seller}|{provider}|{shop}`
//! * a merchant's own app credentials (client id and secret), one pair per
//!   connection: `connector-cred|v1|{seller}|{provider}|{shop}|{connection_id}`
//! * recipient key: `connector-recipient|v1|{seller}|{provider}|{shop}|{key_id}`
//!
//! None of the fields can contain `|`: a pubky is z-base-32, the provider is
//! a fixed word, the key id is hex, and the shop is validated before it is
//! stored (`connector_connections_shop_shape`).

use std::sync::Arc;

use crate::digital::{DigitalKeys, ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY};
use crate::locks::{LocksRuntime, ENV_BUNDLE_ENCRYPTION_KEY, ENV_LOOKUP_HMAC_KEY};
use crate::pickup::{PickupKeys, ENV_PICKUP_DETAILS_ENCRYPTION_KEY};
use crate::priv_keys::{PrivKeys, ENV_PRIV_DATA_KEY_ENCRYPTION_KEY};
use crate::rotating_keys::{KeyFamily, RotatingKeys};

pub const ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY: &str = "CONNECTOR_CREDENTIAL_ENCRYPTION_KEY";
pub const ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS: &str =
    "CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS";

pub struct ConnectorCredentialFamily;

impl KeyFamily for ConnectorCredentialFamily {
    const CURRENT_ENV: &'static str = ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY;
    const PREVIOUS_ENV: &'static str = ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS;
    const DESCRIPTION: &'static str = "connector credential";
    const DEBUG_NAME: &'static str = "ConnectorKeys";
}

pub type ConnectorKeys = RotatingKeys<ConnectorCredentialFamily>;

pub fn credential_aad(seller_pubky: &str, provider: &str, shop: &str) -> Vec<u8> {
    format!("connector-cred|v1|{seller_pubky}|{provider}|{shop}").into_bytes()
}

pub fn app_credential_aad(
    seller_pubky: &str,
    provider: &str,
    shop: &str,
    connection_id: &uuid::Uuid,
) -> Vec<u8> {
    format!("connector-cred|v1|{seller_pubky}|{provider}|{shop}|{connection_id}").into_bytes()
}

pub fn recipient_key_aad(seller_pubky: &str, provider: &str, shop: &str, key_id: &str) -> Vec<u8> {
    format!("connector-recipient|v1|{seller_pubky}|{provider}|{shop}|{key_id}").into_bytes()
}

/// Builds the keys from the environment. `None` means the family is off. A
/// shared key would let a dump of one family's ciphertext be opened as
/// another's, so both connector keys must differ from every other sealing
/// key the process holds.
pub fn connector_keys_from_env(
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
    digital: Option<&DigitalKeys>,
    priv_keys: Option<&PrivKeys>,
) -> anyhow::Result<Option<Arc<ConnectorKeys>>> {
    let Some(keys) = ConnectorKeys::from_env()? else {
        return Ok(None);
    };
    ensure_distinct_keys(&keys, locks, pickup, digital, priv_keys)?;
    Ok(Some(Arc::new(keys)))
}

pub fn ensure_distinct_keys(
    keys: &ConnectorKeys,
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
    digital: Option<&DigitalKeys>,
    priv_keys: Option<&PrivKeys>,
) -> anyhow::Result<()> {
    for key in keys.key_material() {
        if let Some(locks) = locks {
            if key == locks.keys.encryption_bytes() {
                anyhow::bail!(
                    "{ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY} and {ENV_BUNDLE_ENCRYPTION_KEY} \
                     must be distinct keys"
                );
            }
            if key == locks.keys.lookup_hmac_bytes() {
                anyhow::bail!(
                    "{ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY} and {ENV_LOOKUP_HMAC_KEY} must be \
                     distinct keys"
                );
            }
        }
        if pickup.is_some_and(|other| other.key_material().any(|candidate| candidate == key)) {
            anyhow::bail!(
                "{ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY} and \
                 {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} must be distinct keys"
            );
        }
        if digital.is_some_and(|other| other.key_material().any(|candidate| candidate == key)) {
            anyhow::bail!(
                "{ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY} and \
                 {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} must be distinct keys"
            );
        }
        if priv_keys.is_some_and(|other| other.key_material().any(|candidate| candidate == key)) {
            anyhow::bail!(
                "{ENV_CONNECTOR_CREDENTIAL_ENCRYPTION_KEY} and \
                 {ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} must be distinct keys"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aad_binds_owner_fields_with_distinct_prefixes() {
        let credential =
            String::from_utf8(credential_aad("seller", "shopify", "shop.myshopify.com"))
                .expect("aad is utf-8");
        assert_eq!(
            credential,
            "connector-cred|v1|seller|shopify|shop.myshopify.com"
        );
        let app = String::from_utf8(app_credential_aad(
            "seller",
            "shopify",
            "shop.myshopify.com",
            &uuid::Uuid::nil(),
        ))
        .expect("aad is utf-8");
        assert_eq!(
            app,
            "connector-cred|v1|seller|shopify|shop.myshopify.com|00000000-0000-0000-0000-000000000000"
        );
        assert_ne!(app.as_bytes(), credential.as_bytes());
        let recipient = String::from_utf8(recipient_key_aad(
            "seller",
            "shopify",
            "shop.myshopify.com",
            "00ff",
        ))
        .expect("aad is utf-8");
        assert_eq!(
            recipient,
            "connector-recipient|v1|seller|shopify|shop.myshopify.com|00ff"
        );
        for other in [
            "priv-dek|v1|",
            "digital-version/v1|",
            "digital-pin/v1|",
            "buyer-email/v1|",
        ] {
            assert!(!credential.starts_with(other) && !recipient.starts_with(other));
        }
    }
}
