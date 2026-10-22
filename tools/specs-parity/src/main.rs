//! Parity between the service's attestor records and `pubky-app-specs`.
//!
//! Reads `crates/service/tests/fixtures/attestor_records/cases.json` and, for
//! every case, signs the record with the specs crate: a case marked `ok` must
//! sign, produce the `expected` canonical record and verify through
//! `from_verified_json` the way the index loads it; a case marked `reject` must
//! be refused. The service's `attestor_records_test` runs the same file against
//! its own module, so the two stay byte for byte equal.
//!
//! `BLESS=1 cargo run` writes the `expected` records from the specs crate into
//! the file after a deliberate change of the pinned specs revision.

use std::path::PathBuf;
use std::process::ExitCode;

use pubky_app_specs::{PubkyAppAttestorAnnotation, PubkyAppAttestorProfile};
use serde_json::Value;

const PROFILE_PATH: &str = "/pub/pubky.app/marketplace/v1/attestor/profile";
const ANNOTATION_PATH: &str = "/pub/pubky.app/marketplace/v1/attestor/annotations/";

fn cases_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/service/tests/fixtures/attestor_records/cases.json")
}

fn decode_secret(hex: &str) -> [u8; 32] {
    assert_eq!(hex.len(), 64, "secretKeyHex is 32 bytes of hex");
    let mut secret = [0_u8; 32];
    for (index, byte) in secret.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).expect("hex digit");
    }
    secret
}

/// The canonical record the specs crate signs for `record`, or why it refuses.
fn specs_sign(kind: &str, record: &Value, secret: &[u8; 32]) -> Result<String, String> {
    match kind {
        "profile" => {
            let record: PubkyAppAttestorProfile =
                serde_json::from_value(record.clone()).map_err(|error| error.to_string())?;
            record.sign(secret)?.to_canonical_json()
        }
        _ => {
            let record: PubkyAppAttestorAnnotation =
                serde_json::from_value(record.clone()).map_err(|error| error.to_string())?;
            record.sign(secret)?.to_canonical_json()
        }
    }
}

fn uri(kind: &str, owner: &str, record: &Value) -> String {
    match kind {
        "profile" => format!("pubky://{owner}{PROFILE_PATH}"),
        _ => format!(
            "pubky://{owner}{ANNOTATION_PATH}{}",
            record["orderRef"].as_str().expect("orderRef")
        ),
    }
}

fn verifies(kind: &str, uri: &str, body: &str) -> Result<(), String> {
    match kind {
        "profile" => PubkyAppAttestorProfile::from_verified_json(uri, body.as_bytes()).map(|_| ()),
        _ => PubkyAppAttestorAnnotation::from_verified_json(uri, body.as_bytes()).map(|_| ()),
    }
}

fn main() -> ExitCode {
    let path = cases_path();
    let bless = std::env::var_os("BLESS").is_some();
    let mut file: Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("cases.json")).expect("json");
    let secret = decode_secret(file["secretKeyHex"].as_str().expect("secretKeyHex"));
    let owner = file["attestorPubky"]
        .as_str()
        .expect("attestorPubky")
        .to_string();
    let other_owner = "9wm1eqn4wddis385xdgsym7b5ge9549zpcjhmdszymicopxj6acy";

    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0_usize;
    for (kind, group) in [("profile", "profiles"), ("annotation", "annotations")] {
        let cases = file[group].as_array_mut().expect("cases");
        for case in cases.iter_mut() {
            checked += 1;
            let name = case["name"].as_str().expect("name").to_string();
            let record = case["record"].clone();
            let signed = specs_sign(kind, &record, &secret);
            match (case["outcome"].as_str().expect("outcome"), signed) {
                ("reject", Ok(_)) => failures.push(format!("{kind} {name:?}: specs signed it")),
                ("reject", Err(_)) => {}
                ("ok", Err(error)) => {
                    failures.push(format!("{kind} {name:?}: specs refused it: {error}"));
                }
                ("ok", Ok(body)) => {
                    let location = uri(kind, &owner, &record);
                    if let Err(error) = verifies(kind, &location, &body) {
                        failures.push(format!("{kind} {name:?}: does not verify: {error}"));
                    }
                    if verifies(kind, &uri(kind, other_owner, &record), &body).is_ok() {
                        failures.push(format!(
                            "{kind} {name:?}: verifies under another owner's URI"
                        ));
                    }
                    if bless {
                        case["expected"] = Value::String(body);
                    } else if case["expected"].as_str() != Some(body.as_str()) {
                        failures.push(format!(
                            "{kind} {name:?}: expected differs from the specs record: {body}"
                        ));
                    }
                }
                (other, _) => failures.push(format!("{kind} {name:?}: outcome {other:?}")),
            }
        }
    }

    let vector_path = path.with_file_name("specs-vector-v1.json");
    let vector: Value =
        serde_json::from_str(&std::fs::read_to_string(&vector_path).expect("vector"))
            .expect("json");
    for (kind, member) in [("profile", "profile"), ("annotation", "annotation")] {
        let entry = &vector[member];
        let body = serde_json::to_string(&entry["record"]).expect("record");
        let location = entry["uri"].as_str().expect("uri");
        match verifies(kind, location, &body) {
            Ok(()) => {}
            Err(error) => failures.push(format!("specs vector {member}: {error}")),
        }
    }

    if bless && failures.is_empty() {
        let mut text = serde_json::to_string_pretty(&file).expect("serialize");
        text.push('\n');
        std::fs::write(&path, text).expect("write cases.json");
        println!("blessed {checked} cases");
        return ExitCode::SUCCESS;
    }
    if failures.is_empty() {
        println!("specs parity: {checked} cases agree with pubky-app-specs");
        ExitCode::SUCCESS
    } else {
        for failure in &failures {
            eprintln!("{failure}");
        }
        ExitCode::FAILURE
    }
}
