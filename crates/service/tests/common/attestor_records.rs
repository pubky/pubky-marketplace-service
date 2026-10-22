//! The index's way of loading an attestor record, written against the rules
//! and not against `marketplace_service::attestor_records`: the record at a
//! `pubky://{owner}/{path}` URI is accepted only when its detached-payload JWS
//! verifies with the owner's key over the canonical record, and an
//! annotation's `orderRef` equals its path ID.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::{json, Value};

pub const PROFILE_TYP: &str = "pubky-attestor-profile+v1";
pub const ANNOTATION_TYP: &str = "pubky-attestor-annotation+v1";
pub const PROFILE_PATH: &str = "/pub/pubky.app/marketplace/v1/attestor/profile";
pub const ANNOTATIONS_PREFIX: &str = "/pub/pubky.app/marketplace/v1/attestor/annotations/";

pub fn decode_pubky(pubky: &str) -> [u8; 32] {
    const ALPHABET: &str = "ybndrfg8ejkmcpqxot1uwisza345h769";
    let mut accumulator: u64 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::new();
    for c in pubky.chars() {
        let value = ALPHABET.find(c).expect("z-base-32 char") as u64;
        accumulator = (accumulator << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }
    out.truncate(32);
    out.try_into().expect("32 bytes")
}

fn canonical(value: &Value) -> String {
    serde_json_canonicalizer::to_string(value).expect("canonical json")
}

/// Verifies `body` as the record published at `path` on `owner`'s homeserver
/// and returns it parsed.
pub fn verify_record(owner: &str, path: &str, body: &[u8]) -> Result<Value, String> {
    let record: Value = serde_json::from_slice(body).map_err(|error| error.to_string())?;
    let typ = if path == PROFILE_PATH {
        PROFILE_TYP
    } else if let Some(order_ref) = path.strip_prefix(ANNOTATIONS_PREFIX) {
        if record["orderRef"] != json!(order_ref) {
            return Err("orderRef does not match the annotation path ID".into());
        }
        ANNOTATION_TYP
    } else {
        return Err(format!("{path} is not an attestor record path"));
    };
    let proof = record["proof"].as_str().ok_or("no proof")?;
    let (header_b64, signature_b64) = proof.split_once("..").ok_or("proof is not detached")?;
    let header: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    if header != json!({ "alg": "EdDSA", "typ": typ }) {
        return Err(format!("unexpected proof header {header}"));
    }
    let mut unsigned = record.clone();
    unsigned.as_object_mut().expect("object").remove("proof");
    let payload_b64 = URL_SAFE_NO_PAD.encode(canonical(&unsigned).as_bytes());
    let signature = Signature::from_slice(
        &URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    VerifyingKey::from_bytes(&decode_pubky(owner))
        .map_err(|error| error.to_string())?
        .verify(format!("{header_b64}.{payload_b64}").as_bytes(), &signature)
        .map_err(|error| error.to_string())?;
    if canonical(&record).as_bytes() != body {
        return Err("the stored bytes are not the canonical record".into());
    }
    Ok(record)
}
