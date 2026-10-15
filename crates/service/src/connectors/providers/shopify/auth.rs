//! Shopify's signed requests and shop addresses.
//!
//! * Launch (the app's App URL) and the OAuth callback carry an `hmac` query
//!   parameter: the hex HMAC-SHA256, under the app's client secret, of the
//!   other query parameters sorted by name and joined as `key=value` with `&`
//!   (each key and value percent-decoded, then `%` written `%25` and `&`
//!   written `%26`, and `=` in a key `%3D`). A `timestamp` parameter is the
//!   request time.
//! * Webhooks carry `X-Shopify-Hmac-Sha256`: the base64 HMAC-SHA256 of the raw
//!   body under the same secret.
//!
//! Every comparison is constant time (`Mac::verify_slice`).

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::connectors::provider::ProviderError;

/// A signed request older (or newer) than this is refused.
pub const MAX_REQUEST_SKEW_SECONDS: i64 = 3600;

/// Validates a merchant-entered store address and returns the canonical
/// `{handle}.myshopify.com`. Accepts the bare domain, an `https://` URL and the
/// admin URL `admin.shopify.com/store/{handle}`; nothing else, so a request
/// can only ever go to a Shopify host.
pub fn normalize_shop(raw: &str) -> Result<String, ProviderError> {
    let invalid = || ProviderError::Invalid("not a myshopify.com store address".into());
    let mut text = raw.trim().to_ascii_lowercase();
    for prefix in ["https://", "http://"] {
        if let Some(rest) = text.strip_prefix(prefix) {
            text = rest.to_string();
        }
    }
    let host_and_path = text.trim_end_matches('/').to_string();
    let handle = if let Some(rest) = host_and_path.strip_prefix("admin.shopify.com/store/") {
        rest.split(['/', '?', '#']).next().unwrap_or_default().to_string()
    } else {
        let host = host_and_path
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        host.strip_suffix(".myshopify.com")
            .ok_or_else(invalid)?
            .to_string()
    };
    let valid = !handle.is_empty()
        && handle.len() <= 60
        && handle.as_bytes()[0].is_ascii_alphanumeric()
        && handle
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if valid {
        Ok(format!("{handle}.myshopify.com"))
    } else {
        Err(invalid())
    }
}

fn encode_part(text: &str, key: bool) -> String {
    let mut out = text.replace('%', "%25").replace('&', "%26");
    if key {
        out = out.replace('=', "%3D");
    }
    out
}

/// The message Shopify signs for a launch or callback: every query parameter
/// but `hmac`, sorted by key, as `key=value` joined by `&`.
pub fn query_message(raw_query: &str) -> String {
    let mut pairs: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        if key == "hmac" || key == "signature" {
            continue;
        }
        pairs.insert(key.into_owned(), value.into_owned());
    }
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", encode_part(key, true), encode_part(value, false)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Whether `raw_query`'s `hmac` is the signature of its other parameters
/// under `secret`.
pub fn verify_query_hmac(secret: &[u8], raw_query: &str) -> bool {
    let presented = url::form_urlencoded::parse(raw_query.as_bytes())
        .find(|(key, _)| key == "hmac")
        .and_then(|(_, value)| hex::decode(value.as_bytes()).ok());
    let Some(presented) = presented else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(query_message(raw_query).as_bytes());
    mac.verify_slice(&presented).is_ok()
}

/// Whether the request's `timestamp` is within [`MAX_REQUEST_SKEW_SECONDS`]
/// of `now` (Unix seconds).
pub fn timestamp_fresh(raw_query: &str, now: i64) -> bool {
    url::form_urlencoded::parse(raw_query.as_bytes())
        .find(|(key, _)| key == "timestamp")
        .and_then(|(_, value)| value.parse::<i64>().ok())
        .is_some_and(|timestamp| (now - timestamp).abs() <= MAX_REQUEST_SKEW_SECONDS)
}

/// Whether `signature` (base64) is the HMAC-SHA256 of `body` under `secret`.
pub fn verify_webhook_hmac(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let Ok(presented) = STANDARD.decode(signature.trim()) else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(body);
    mac.verify_slice(&presented).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed(secret: &str, query: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("key");
        mac.update(query_message(query).as_bytes());
        format!("{query}&hmac={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn shop_addresses_are_myshopify_hosts_only() {
        for (raw, canonical) in [
            ("Dezentral.myshopify.com", "dezentral.myshopify.com"),
            ("  https://dezentral.myshopify.com/admin ", "dezentral.myshopify.com"),
            ("https://admin.shopify.com/store/dezentral/apps", "dezentral.myshopify.com"),
            ("shop-1.myshopify.com/", "shop-1.myshopify.com"),
        ] {
            assert_eq!(normalize_shop(raw).expect(raw), canonical);
        }
        for bad in [
            "",
            "example.com",
            "evil.com/.myshopify.com",
            "evil.com#.myshopify.com",
            "a.b.myshopify.com",
            "-bad.myshopify.com",
            ".myshopify.com",
            "bad_name.myshopify.com",
            "user@dezentral.myshopify.com",
            "dezentral.myshopify.com:8080",
            "admin.shopify.com/store/",
            "admin.shopify.com/store/Bad Name",
        ] {
            assert!(normalize_shop(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_signed_message_is_sorted_and_escaped_as_shopify_documents() {
        assert_eq!(
            query_message("shop=a.myshopify.com&timestamp=1&hmac=zz&host=YWJj"),
            "host=YWJj&shop=a.myshopify.com&timestamp=1"
        );
        assert_eq!(query_message("a%26b=c%25d&hmac=x"), "a%26b=c%25d");
        assert_eq!(query_message("k=v&hmac=x&signature=y"), "k=v");
    }

    #[test]
    fn a_launch_signature_verifies_only_under_its_secret_and_unaltered() {
        let query = signed("secret", "shop=a.myshopify.com&timestamp=1700000000&host=YWJj");
        assert!(verify_query_hmac(b"secret", &query));
        assert!(!verify_query_hmac(b"other", &query));
        assert!(!verify_query_hmac(b"secret", &query.replace("a.myshopify", "b.myshopify")));
        assert!(!verify_query_hmac(b"secret", "shop=a.myshopify.com&timestamp=1"));
        assert!(!verify_query_hmac(b"secret", "shop=a&hmac=not-hex"));
    }

    #[test]
    fn a_request_must_be_recent() {
        assert!(timestamp_fresh("timestamp=1700000000", 1_700_000_100));
        assert!(!timestamp_fresh("timestamp=1700000000", 1_700_010_000));
        assert!(!timestamp_fresh("timestamp=1700010000", 1_700_000_000));
        assert!(!timestamp_fresh("shop=a", 1_700_000_000));
        assert!(!timestamp_fresh("timestamp=abc", 1_700_000_000));
    }

    #[test]
    fn a_webhook_signature_is_base64_of_the_body_hmac() {
        let body = br#"{"id":1}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").expect("key");
        mac.update(body);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        assert!(verify_webhook_hmac(b"secret", body, &signature));
        assert!(!verify_webhook_hmac(b"other", body, &signature));
        assert!(!verify_webhook_hmac(b"secret", br#"{"id":2}"#, &signature));
        assert!(!verify_webhook_hmac(b"secret", body, "%%%"));
        assert!(!verify_webhook_hmac(b"secret", body, ""));
    }
}
