//! The delivery envelope is sealed in the browser by hpke-js and never opened
//! by this service. This test proves the format is plain RFC 9180 HPKE by
//! opening the Shop's known-answer vector with the Rust `hpke` crate, the one
//! pubky-core's homeserver uses (DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
//! ChaCha20-Poly1305, base mode). Any Rust client or the homeserver SDK can
//! therefore open an envelope with no format change.
//!
//! The vector is `src/libs/commerce/delivery-envelope.vectors.json` in
//! pubky/pubky-marketplace, copied byte for byte.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, OpModeR};
use marketplace_domain::commands::{decode_delivery_envelope, DELIVERY_ENVELOPE_BYTES};
use serde_json::Value;

type Kem = X25519HkdfSha256;

const PLAINTEXT_BUCKET: usize = 2048;

fn vector() -> Value {
    serde_json::from_str(include_str!("fixtures/delivery_envelope/vectors.json"))
        .expect("the vector is JSON")
}

fn text<'a>(value: &'a Value, path: &[&str]) -> &'a str {
    path.iter()
        .fold(value, |value, key| &value[*key])
        .as_str()
        .unwrap_or_else(|| panic!("vector field {path:?} is a string"))
}

fn aad(vector: &Value) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}|{}",
        text(vector, &["context"]),
        text(vector, &["binding", "checkoutCommandId"]),
        text(vector, &["binding", "buyerPubky"]),
        text(vector, &["binding", "sellerPubky"]),
        text(vector, &["binding", "keyId"]),
    )
    .into_bytes()
}

fn open_with(vector: &Value, private_key: &str, aad: &[u8], info: &[u8]) -> Option<Vec<u8>> {
    let sk_bytes = URL_SAFE_NO_PAD.decode(private_key).expect("private key");
    let sk = <Kem as KemTrait>::PrivateKey::from_bytes(&sk_bytes).expect("x25519 private key");
    let wire = URL_SAFE_NO_PAD
        .decode(text(vector, &["envelope"]))
        .expect("envelope base64url");
    let (enc, ciphertext) = wire.split_at(32);
    let encapped = <Kem as KemTrait>::EncappedKey::from_bytes(enc).expect("encapsulated key");
    hpke::single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        &sk,
        &encapped,
        info,
        ciphertext,
        aad,
    )
    .ok()
}

fn unpad(padded: &[u8]) -> &[u8] {
    let marker = padded
        .iter()
        .rposition(|byte| *byte != 0)
        .expect("padding marker present");
    assert_eq!(padded[marker], 0x80, "padding marker is 0x80");
    &padded[..marker]
}

#[test]
fn the_vector_is_a_valid_service_envelope() {
    let vector = vector();
    let envelope = text(&vector, &["envelope"]);
    let bytes = decode_delivery_envelope(envelope).expect("the service accepts the vector shape");
    assert_eq!(bytes.len(), DELIVERY_ENVELOPE_BYTES);
}

#[test]
fn the_rust_hpke_crate_opens_the_hpke_js_vector() {
    let vector = vector();
    let padded = open_with(
        &vector,
        text(&vector, &["privateKey"]),
        &aad(&vector),
        text(&vector, &["context"]).as_bytes(),
    )
    .expect("the Rust crate opens the hpke-js envelope");
    assert_eq!(padded.len(), PLAINTEXT_BUCKET);
    let address: Value = serde_json::from_slice(unpad(&padded)).expect("padded JSON address");
    assert_eq!(address, vector["address"]);
    assert_eq!(address["name"], "Ada Buyer");
}

#[test]
fn the_rust_hpke_crate_refuses_a_tampered_or_rebound_envelope() {
    let vector = vector();
    let private_key = text(&vector, &["privateKey"]);
    let info = text(&vector, &["context"]).as_bytes().to_vec();
    let good_aad = aad(&vector);

    for rebound in [
        (
            "seller",
            "ssssssssssssssssssssssssssssssssssssssssssssssssssss",
            "tttttttttttttttttttttttttttttttttttttttttttttttttttt",
        ),
        (
            "buyer",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccc",
        ),
        (
            "command",
            "5b0c2f3e-8a4d-4c1e-9f6a-7d2b1e0a3c44",
            "5b0c2f3e-8a4d-4c1e-9f6a-7d2b1e0a3c45",
        ),
        (
            "key",
            "7bc99de1c2bee33ae26aecb31f5ece87",
            "7bc99de1c2bee33ae26aecb31f5ece88",
        ),
    ] {
        let wrong = String::from_utf8(good_aad.clone())
            .expect("aad is UTF-8")
            .replace(rebound.1, rebound.2)
            .into_bytes();
        assert_ne!(
            wrong, good_aad,
            "the {} substitution changed the AAD",
            rebound.0
        );
        assert!(
            open_with(&vector, private_key, &wrong, &info).is_none(),
            "an envelope replayed onto another {} must not open",
            rebound.0
        );
    }

    assert!(
        open_with(
            &vector,
            private_key,
            &good_aad,
            b"pubky-marketplace/other/v1"
        )
        .is_none(),
        "a different HPKE info string must not open"
    );

    let other = URL_SAFE_NO_PAD.encode([0x42u8; 32]);
    assert!(
        open_with(&vector, &other, &good_aad, &info).is_none(),
        "another recipient key must not open"
    );

    let mut tampered = vector.clone();
    let mut wire = URL_SAFE_NO_PAD
        .decode(text(&vector, &["envelope"]))
        .expect("envelope");
    wire[100] ^= 1;
    tampered["envelope"] = Value::String(URL_SAFE_NO_PAD.encode(wire));
    assert!(
        open_with(&tampered, private_key, &good_aad, &info).is_none(),
        "a flipped ciphertext bit must not open"
    );
}
