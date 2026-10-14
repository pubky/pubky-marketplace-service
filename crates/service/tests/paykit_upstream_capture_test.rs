//! Historical producer captures pin old request bytes, signatures, response
//! bytes, and provenance. Current prepare behavior is exercised elsewhere
//! against the signature-verifying producer double because these captures
//! predate `description` support and must not be treated as compatible.

mod common;

use base64::Engine;
use common::{paykit_server_67_75, TEST_PAYKIT_SIGNING_SEED};
use ed25519_dalek::{Signature, SigningKey, Verifier};
use marketplace_service::payments::{
    paykit_signature_preimage, PaykitApi, PaykitClient, PaykitDeliveryState, PaykitLifecycleTarget,
    PaykitStatusOutcome,
};

const SELLER: &str = "buuk6nr1caaumhms4tph5oa656z9f5k9q867h4u8ze441mpfuhyo";

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

fn captured_invoice_id(prepare_fixture: &str) -> uuid::Uuid {
    let fixture = paykit_server_67_75::load(prepare_fixture);
    serde_json::from_str::<serde_json::Value>(&fixture.response_body)
        .expect("captured prepare response is JSON")["invoice_id"]
        .as_str()
        .expect("captured prepare response carries invoice_id")
        .parse()
        .expect("captured prepare invoice_id is UUID")
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
async fn captured_producer_lifecycle_responses_replay_through_the_consumer() {
    // Prepare now carries `description`, so its legacy request bytes cannot be
    // replayed. Seed the unchanged commands from the invoice ids in the real
    // prepare response captures, then replay the producer's exact response
    // bytes for activate, status, resolve, and void through current parsers.
    let activate = paykit_server_67_75::load("activate");
    let status = paykit_server_67_75::load("status_active");
    let resolve = paykit_server_67_75::load("resolve_paid_manually");
    let void = paykit_server_67_75::load("void");
    let invoice_id = captured_invoice_id("prepare_new");
    let second_invoice = captured_invoice_id("prepare_for_void");
    let fixtures = vec![activate, status, resolve, void];
    assert!(fixtures
        .iter()
        .all(|fixture| fixture.server_revision == paykit_server_67_75::SERVER_REVISION));
    let replay = paykit_server_67_75::spawn_replay(fixtures).await;
    let client = PaykitClient::new(&replay.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("Paykit client")
        .with_api(PaykitApi::Upstream);
    let target = PaykitLifecycleTarget::Upstream {
        creator: SELLER.to_string(),
    };

    let activated = client
        .activate_payment_request(&target, invoice_id, 50_000, 1)
        .await
        .expect("captured activation parses");
    assert_eq!(activated.invoice_id, invoice_id);
    assert_eq!(activated.total_sats, 50_000);

    let (status, delivery) = client
        .marketplace_payment_status(SELLER, invoice_id, Some(activated.payment_deadline))
        .await;
    assert!(matches!(
        status,
        PaykitStatusOutcome::ActiveUndetected { payment_deadline }
            if payment_deadline == activated.payment_deadline
    ));
    assert_eq!(delivery, Some(PaykitDeliveryState::Pending));

    let resolved = client
        .resolve_payment_request(
            &target,
            invoice_id,
            "paid_manually",
            chrono::DateTime::UNIX_EPOCH,
        )
        .await
        .expect("captured resolve response is reachable");
    assert_eq!(resolved.status, reqwest::StatusCode::OK);
    let resolved = resolved.resolved.expect("captured resolve body parses");
    assert_eq!(resolved.invoice_id, invoice_id);
    assert_eq!(
        resolved.outcome, "paid_manually",
        "captured producer outcome remains typed"
    );

    let voided = client
        .void_payment_request(&target, second_invoice, "not serialized upstream")
        .await
        .expect("captured void response parses");
    assert_eq!(voided.invoice_id, second_invoice);
    assert_eq!(voided.state, "voided");

    replay.assert_drained();
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
