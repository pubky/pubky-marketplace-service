//! Historical producer captures pin old request bytes, signatures, and
//! provenance. Current prepare behavior is exercised against the
//! signature-verifying producer double because the captured producer predates
//! `description` support and must not be treated as compatible.

mod common;

use base64::Engine;
use common::{paykit_server_67_75, spawn_fake_paykit, TEST_PAYKIT_SIGNING_SEED};
use ed25519_dalek::{Signature, SigningKey, Verifier};
use marketplace_service::payments::{paykit_signature_preimage, PaykitApi, PaykitClient};

const SELLER: &str = "buuk6nr1caaumhms4tph5oa656z9f5k9q867h4u8ze441mpfuhyo";
const BUYER: &str = "7ahm9mquyezn4ny85baxqo43r8epijtwo6h8149yxsm7eiu155uy";

fn captured_signature_verifies(fixture: &paykit_server_67_75::Fixture) -> bool {
    let seed: [u8; 32] = hex::decode(TEST_PAYKIT_SIGNING_SEED)
        .expect("test signing seed decodes")
        .try_into()
        .expect("test signing seed is 32 bytes");
    let signature: [u8; 64] = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&fixture.signature)
        .expect("captured signature is base64url")
        .try_into()
        .expect("captured signature is 64 bytes");
    SigningKey::from_bytes(&seed)
        .verifying_key()
        .verify(
            &paykit_signature_preimage(
                &fixture.method,
                &fixture.path,
                fixture.request_body.as_bytes(),
            ),
            &Signature::from_bytes(&signature),
        )
        .is_ok()
}

#[test]
fn captured_exchanges_pin_routes_auth_headers_and_canonical_bytes() {
    let expected_paths = [
        "/marketplace/payment-requests/prepare",
        "/marketplace/payment-requests/prepare",
        "/marketplace/payment-requests/activate",
        "/marketplace/payment-requests/status",
        "/marketplace/payment-requests/resolve",
        "/marketplace/payment-requests/prepare",
        "/marketplace/payment-requests/void",
    ];
    for ((expected_name, expected_path), fixture) in paykit_server_67_75::FIXTURE_NAMES
        .iter()
        .zip(expected_paths)
        .zip(paykit_server_67_75::load_all())
    {
        assert_eq!(&fixture.name, expected_name);
        assert_eq!(
            fixture.server_revision,
            paykit_server_67_75::SERVER_REVISION
        );
        assert_eq!(fixture.method, "POST", "{} method", fixture.name);
        assert_eq!(fixture.path, expected_path, "{} route", fixture.name);
        assert_eq!(
            fixture.content_type, None,
            "{} request header",
            fixture.name
        );
        assert_eq!(fixture.status, 200, "{} response status", fixture.name);
        assert_eq!(
            fixture.response_content_type.as_deref(),
            Some("application/json"),
            "{} response header",
            fixture.name
        );
        assert!(
            captured_signature_verifies(&fixture),
            "{} captured authentication",
            fixture.name
        );
        let request: serde_json::Value =
            serde_json::from_str(&fixture.request_body).expect("captured request is JSON");
        if fixture.path == "/marketplace/payment-requests/prepare" {
            assert!(
                request.get("description").is_none(),
                "legacy capture predates provider description support"
            );
        }
        assert_eq!(
            serde_json_canonicalizer::to_string(&request).expect("request canonicalizes"),
            fixture.request_body,
            "{} request bytes are canonical",
            fixture.name
        );
        let response: serde_json::Value =
            serde_json::from_str(&fixture.response_body).expect("captured response is JSON");
        response["invoice_id"]
            .as_str()
            .expect("producer response carries invoice_id")
            .parse::<uuid::Uuid>()
            .expect("producer invoice_id is UUID");
    }
}

#[tokio::test]
async fn signed_prepare_capture_contains_stable_order_label() {
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    let client = PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("Paykit client")
        .with_api(PaykitApi::Upstream);
    let order_id = uuid::Uuid::parse_str("8f3a91b2-3333-4333-8333-333333333333").unwrap();

    client
        .create_payment_request(
            SELLER,
            BUYER,
            order_id,
            19,
            50_000,
            chrono::DateTime::UNIX_EPOCH,
            3_600,
        )
        .await
        .expect("signature-verifying capture accepts prepare");

    let calls = paykit.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].body["description"],
        serde_json::json!("Pubky Shop order 8F3A91")
    );
}

#[test]
fn capture_provenance_pins_reviewed_producer_heads() {
    assert_eq!(
        paykit_server_67_75::SERVER_REVISION,
        paykit_server_67_75::PR75_HEAD
    );
    for commit in [
        paykit_server_67_75::SERVER_REVISION,
        paykit_server_67_75::SERVER_TREE,
        paykit_server_67_75::PR67_REVIEWED_HEAD,
    ] {
        assert_eq!(commit.len(), 40);
        assert!(commit.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
