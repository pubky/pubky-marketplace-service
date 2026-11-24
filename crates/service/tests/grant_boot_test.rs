//! Boot of the grant flow from the environment. One test, because env
//! mutation is process-wide; this binary has no other test.

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ed25519_dalek::SigningKey;
use marketplace_service::grant::GrantRuntime;
use serde_json::json;

const HOMESERVER_PUBKY: &str = "HOMESERVER_PUBKY";
const SESSION_TTL_SECONDS: i64 = 3600;

fn verify_ring(kid: &str, seed: u8) -> String {
    let public_key = SigningKey::from_bytes(&[seed; 32]).verifying_key();
    json!({"active": {"epoch": 1, "kid": kid, "public_key": URL_SAFE_NO_PAD.encode(public_key.as_bytes())}})
        .to_string()
}

fn refusal(expected: &str) {
    let error = GrantRuntime::from_env(SESSION_TTL_SECONDS)
        .expect_err("the grant flow must refuse to boot");
    assert!(
        format!("{error:#}").contains(expected),
        "expected `{expected}`, got `{error:#}`"
    );
}

#[tokio::test]
async fn the_grant_flow_boots_only_with_a_homeserver_pubky() {
    std::env::remove_var("MARKETPLACE_GRANT_FLOW_ENABLED");
    std::env::remove_var(HOMESERVER_PUBKY);
    assert!(
        GrantRuntime::from_env(SESSION_TTL_SECONDS)
            .expect("the flow is off by default")
            .is_none(),
        "with the flow off, HOMESERVER_PUBKY is not read"
    );

    std::env::set_var("MARKETPLACE_GRANT_FLOW_ENABLED", "true");
    std::env::set_var("MARKETPLACE_GRANT_CLIENT_ID", "marketplace.localhost");
    std::env::set_var("MARKETPLACE_GRANT_RELAY_URL", "https://relay.example/inbox");
    std::env::set_var("GRANT_FLOW_ENCRYPTION_KEY_B64", STANDARD.encode([1u8; 32]));
    std::env::set_var("GRANT_FLOW_KEY_EPOCH", "1");
    std::env::set_var("GRANT_RESULT_HMAC_ROOT_B64", STANDARD.encode([2u8; 32]));
    std::env::set_var("GRANT_RESULT_HMAC_KEY_EPOCH", "1");
    std::env::set_var(
        "SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON",
        verify_ring("shop-test-0001", 3),
    );
    std::env::set_var(
        "SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON",
        verify_ring("bff-test-0001", 4),
    );

    let must_be_set = "HOMESERVER_PUBKY must be set when the grant flow is enabled";
    let malformed = "HOMESERVER_PUBKY must be a 52-character z-base-32 pubky";
    refusal(must_be_set);
    for blank in ["", " ", "\n", " \t\r\n "] {
        std::env::set_var(HOMESERVER_PUBKY, blank);
        refusal(must_be_set);
    }
    let pubky = pubky::Keypair::random().public_key().z32();
    for wrong in [
        "not-a-pubky".to_string(),
        pubky[..51].to_string(),
        format!("{pubky}y"),
        pubky.to_uppercase(),
        format!("pubky{pubky}"),
        format!("{} {}", &pubky[..26], &pubky[26..]),
    ] {
        std::env::set_var(HOMESERVER_PUBKY, &wrong);
        refusal(malformed);
    }

    for padded in [
        pubky.clone(),
        format!("{pubky}\n"),
        format!("{pubky}\r\n"),
        format!("  {pubky}\t"),
    ] {
        std::env::set_var(HOMESERVER_PUBKY, &padded);
        assert!(
            GrantRuntime::from_env(SESSION_TTL_SECONDS)
                .unwrap_or_else(|error| panic!("{padded:?} must boot: {error:#}"))
                .is_some(),
            "the grant flow is on"
        );
    }
}
