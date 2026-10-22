//! The attestor records the service writes are the records `pubky-app-specs`
//! signs and the index verifies. The specs crate cannot be linked here (see the
//! `attestor_records` module), so this holds the module to the crate's output:
//! the specs test vector byte for byte, and every case in `cases.json`, which
//! `tools/specs-parity` checks against the crate itself.

mod common;

use common::attestor_records::{
    verify_record, ANNOTATIONS_PREFIX, PROFILE_PATH as PROFILE_FILE_PATH,
};
use marketplace_service::attestor_records::{
    annotation_path, Annotation, Outcome, OutcomeKind, Profile, PROFILE_PATH,
};
use serde_json::{json, Value};

const VECTOR: &str = include_str!("fixtures/attestor_records/specs-vector-v1.json");
const CASES: &str = include_str!("fixtures/attestor_records/cases.json");

fn secret(hex_secret: &str) -> [u8; 32] {
    let mut out = [0_u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex_secret[index * 2..index * 2 + 2], 16).expect("hex");
    }
    out
}

fn canonical(value: &Value) -> String {
    serde_json_canonicalizer::to_string(value).expect("canonical")
}

#[test]
fn the_specs_profile_vector_is_reproduced_byte_for_byte() {
    let vector: Value = serde_json::from_str(VECTOR).expect("vector");
    let key = secret(vector["attestorSecretKeyHex"].as_str().expect("key"));
    let entry = &vector["profile"];
    let unsigned: Profile = serde_json::from_value(entry["record"].clone()).expect("profile");

    let signed = unsigned.sign(&key).expect("the vector profile signs");

    assert_eq!(
        signed.canonical_body().expect("body"),
        entry["canonicalBody"].as_str().expect("canonicalBody")
    );
    assert_eq!(signed.proof, entry["proof"].as_str().expect("proof"));
    assert_eq!(
        signed.to_canonical_json().expect("json"),
        canonical(&entry["record"])
    );
    let owner = vector["attestorPubky"].as_str().expect("pubky");
    assert_eq!(
        entry["uri"].as_str().expect("uri"),
        format!("pubky://{owner}{PROFILE_PATH}")
    );
}

#[test]
fn the_specs_annotation_vector_is_reproduced_byte_for_byte() {
    let vector: Value = serde_json::from_str(VECTOR).expect("vector");
    let key = secret(vector["attestorSecretKeyHex"].as_str().expect("key"));
    let entry = &vector["annotation"];
    let unsigned: Annotation = serde_json::from_value(entry["record"].clone()).expect("annotation");
    let order_ref = unsigned.order_ref.clone();

    let signed = unsigned.sign(&key).expect("the vector annotation signs");

    assert_eq!(
        signed.canonical_body().expect("body"),
        entry["canonicalBody"].as_str().expect("canonicalBody")
    );
    assert_eq!(signed.proof, entry["proof"].as_str().expect("proof"));
    assert_eq!(
        signed.to_canonical_json().expect("json"),
        canonical(&entry["record"])
    );
    let owner = vector["attestorPubky"].as_str().expect("pubky");
    assert_eq!(
        entry["uri"].as_str().expect("uri"),
        format!("pubky://{owner}{}", annotation_path(&order_ref))
    );
}

#[test]
fn the_vector_records_verify_the_way_the_index_verifies_them() {
    let vector: Value = serde_json::from_str(VECTOR).expect("vector");
    let owner = vector["attestorPubky"].as_str().expect("pubky");
    for member in ["profile", "annotation"] {
        let entry = &vector[member];
        let uri = entry["uri"].as_str().expect("uri");
        let path = &uri[uri.find("/pub/").expect("path")..];
        let body = canonical(&entry["record"]);
        verify_record(owner, path, body.as_bytes())
            .unwrap_or_else(|error| panic!("{member}: {error}"));
    }
}

/// Signs a case record the way the service does; `Err` when the module
/// refuses it, as a record that does not even parse is refused.
fn service_sign(kind: &str, record: &Value, key: &[u8; 32]) -> Result<(String, String), String> {
    if kind == "profile" {
        let record: Profile = serde_json::from_value(record.clone()).map_err(|e| e.to_string())?;
        let signed = record.sign(key).map_err(|e| e.to_string())?;
        let path = PROFILE_PATH.to_string();
        Ok((path, signed.to_canonical_json().map_err(|e| e.to_string())?))
    } else {
        let record: Annotation =
            serde_json::from_value(record.clone()).map_err(|e| e.to_string())?;
        let signed = record.sign(key).map_err(|e| e.to_string())?;
        let path = annotation_path(&signed.order_ref);
        Ok((path, signed.to_canonical_json().map_err(|e| e.to_string())?))
    }
}

#[test]
fn every_parity_case_signs_to_the_specs_bytes_or_is_refused() {
    let cases: Value = serde_json::from_str(CASES).expect("cases");
    let key = secret(cases["secretKeyHex"].as_str().expect("key"));
    let owner = cases["attestorPubky"].as_str().expect("pubky");
    let (mut accepted, mut refused) = (0, 0);
    for (kind, group) in [("profile", "profiles"), ("annotation", "annotations")] {
        for case in cases[group].as_array().expect("cases") {
            let name = case["name"].as_str().expect("name");
            let result = service_sign(kind, &case["record"], &key);
            match case["outcome"].as_str().expect("outcome") {
                "ok" => {
                    let (path, body) =
                        result.unwrap_or_else(|error| panic!("{kind} {name:?} refused: {error}"));
                    assert_eq!(
                        body,
                        case["expected"].as_str().expect("expected"),
                        "{kind} {name:?} differs from the specs record"
                    );
                    verify_record(owner, &path, body.as_bytes())
                        .unwrap_or_else(|error| panic!("{kind} {name:?}: {error}"));
                    accepted += 1;
                }
                "reject" => {
                    assert!(result.is_err(), "{kind} {name:?} was signed");
                    refused += 1;
                }
                other => panic!("{kind} {name:?}: outcome {other:?}"),
            }
        }
    }
    assert!(
        accepted >= 15 && refused >= 30,
        "{accepted} accepted, {refused} refused"
    );
}

fn annotation(revision: i64, outcomes: Vec<Outcome>) -> Annotation {
    Annotation {
        order_ref: "ab".repeat(32),
        outcomes,
        revision,
        proof: String::new(),
    }
}

fn refunded(on: &str) -> Outcome {
    Outcome {
        kind: OutcomeKind::Refunded,
        on: on.to_string(),
    }
}

#[test]
fn the_content_hash_ignores_the_revision_and_the_proof_and_not_the_outcomes() {
    let key = [7_u8; 32];
    let first = annotation(1, vec![refunded("2026-10-14")]);
    let later = annotation(9, vec![refunded("2026-10-14")]);
    let signed = later.clone().sign(&key).expect("signs");
    assert_eq!(first.content_hash().unwrap(), later.content_hash().unwrap());
    assert_eq!(
        first.content_hash().unwrap(),
        signed.content_hash().unwrap()
    );
    assert_ne!(
        first.content_hash().unwrap(),
        annotation(1, vec![refunded("2026-10-15")])
            .content_hash()
            .unwrap()
    );
    assert_ne!(
        first.content_hash().unwrap(),
        annotation(1, vec![]).content_hash().unwrap()
    );
}

#[test]
fn a_tampered_or_misplaced_record_does_not_verify() {
    let cases: Value = serde_json::from_str(CASES).expect("cases");
    let key = secret(cases["secretKeyHex"].as_str().expect("key"));
    let owner = cases["attestorPubky"].as_str().expect("pubky");
    let signed = annotation(2, vec![refunded("2026-10-14")])
        .sign(&key)
        .expect("signs");
    let path = annotation_path(&signed.order_ref);
    let body = signed.to_canonical_json().expect("json");
    verify_record(owner, &path, body.as_bytes()).expect("the untouched record verifies");

    let other_owner = "9wm1eqn4wddis385xdgsym7b5ge9549zpcjhmdszymicopxj6acy";
    assert!(verify_record(other_owner, &path, body.as_bytes()).is_err());

    let mut forged: Value = serde_json::from_str(&body).unwrap();
    forged["outcomes"] = json!([]);
    assert!(verify_record(owner, &path, canonical(&forged).as_bytes()).is_err());
    let mut raised: Value = serde_json::from_str(&body).unwrap();
    raised["revision"] = json!(3);
    assert!(verify_record(owner, &path, canonical(&raised).as_bytes()).is_err());

    let elsewhere = format!("{ANNOTATIONS_PREFIX}{}", "cd".repeat(32));
    assert!(verify_record(owner, &elsewhere, body.as_bytes()).is_err());
    assert!(verify_record(owner, PROFILE_FILE_PATH, body.as_bytes()).is_err());
}

#[test]
fn signing_is_deterministic() {
    let key = [7_u8; 32];
    let first = annotation(1, vec![refunded("2026-10-14")])
        .sign(&key)
        .unwrap();
    let again = annotation(1, vec![refunded("2026-10-14")])
        .sign(&key)
        .unwrap();
    assert_eq!(first.proof, again.proof);
    assert_eq!(
        first.to_canonical_json().unwrap(),
        again.to_canonical_json().unwrap()
    );
}
