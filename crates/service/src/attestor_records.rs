//! The two records the attestor publishes on its own homeserver: its profile
//! and one annotation per attested order, each with a `proof` the index and the
//! Shop verify offline.
//!
//! The record shapes, their validation and the proof are those of
//! `pubky-app-specs` `marketplace_attestor.rs` (tag `v0.6.2-marketplace.16`,
//! commit `5ef6e31e89530a58e66044a08fb6baf704adabc7`), which the index verifies
//! them with. The service cannot link that crate: it depends on `pubky` 0.9,
//! whose `pkarr` pins a pre-release `ed25519` that conflicts with the
//! `ed25519-dalek` 3 this service's `pubky` 0.16.1 pulls in, so no single
//! dependency graph holds both. This module is therefore a port, and it is held
//! to the crate by `tools/specs-parity`, which signs and verifies the fixture
//! inputs in `tests/fixtures/attestor_records/cases.json` with the real crate,
//! while `attestor_records_test` makes this module reproduce the same bytes. The
//! specs' own test vector is replayed byte for byte too.
//!
//! The proof is a compact JWS with a detached payload (RFC 7515 Appendix F,
//! `alg: EdDSA` per RFC 8037):
//!
//! - header: exactly `{"alg":"EdDSA","typ":<typ>}`;
//! - payload: the record without its `proof`, serialized per RFC 8785;
//! - `base64url(header) || ".." || base64url(signature)`, the signature over
//!   `base64url(header) || "." || base64url(payload)`.
//!
//! The verification key is the pubky that owns the record's URI, so a copy on
//! another homeserver fails, and an annotation's `orderRef` equals its path ID.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use marketplace_domain::pubky::is_valid_pubky;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// JOSE `typ` of an attestor profile proof.
pub const ATTESTOR_PROFILE_TYP: &str = "pubky-attestor-profile+v1";
/// JOSE `typ` of an attestor annotation proof.
pub const ATTESTOR_ANNOTATION_TYP: &str = "pubky-attestor-annotation+v1";
/// `issuancePolicy.amountBand` of an attestor that adds the order total's band
/// only when both trade sides consent.
pub const AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT: &str = "both-sides consent";
/// `issuancePolicy.priceDecade` of an attestor that signs every per-item
/// attestation's `price_decade`.
pub const PRICE_DECADE_POLICY_ALWAYS: &str = "always";
/// `issuancePolicy.priceDecade` of an attestor that signs none.
pub const PRICE_DECADE_POLICY_NEVER: &str = "never";

/// Where the profile lives on the attestor's homeserver.
pub const PROFILE_PATH: &str = "/pub/pubky.app/marketplace/v1/attestor/profile";
const ANNOTATIONS_PATH: &str = "/pub/pubky.app/marketplace/v1/attestor/annotations/";

const MAX_OPERATOR_NAME_LENGTH: usize = 100;
const MAX_URL_LENGTH: usize = 2_048;
const MAX_PROFILE_KEYS: usize = 16;
const MAX_POLICY_VALUE_LENGTH: usize = 64;
const MAX_EDIT_WINDOW_HOURS: i64 = 8_760;
const MAX_BLIND_WINDOW_DAYS: i64 = 365;
/// Most outcomes an annotation may hold.
pub const MAX_ANNOTATION_OUTCOMES: usize = 16;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Where one order's annotation lives on the attestor's homeserver.
pub fn annotation_path(order_ref: &str) -> String {
    format!("{ANNOTATIONS_PATH}{order_ref}")
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("{0}")]
pub struct RecordError(pub String);

fn invalid(message: impl Into<String>) -> RecordError {
    RecordError(format!("Validation Error: {}", message.into()))
}

/// Who operates the attestor key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Operator {
    pub name: String,
    pub url: String,
    pub policy_url: String,
}

/// One key the attestor has issued with. A verifier may only narrow its own
/// trust window from this entry; it never adds keys from a profile.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProfileKey {
    pub pubky: String,
    pub valid_from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compromised_from: Option<String>,
}

/// How the attestor issues purchase attestations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IssuancePolicy {
    pub edit_window_hours: i64,
    pub blind_window_days: i64,
    pub amount_band: String,
    pub price_decade: String,
    /// The service attests every order whatever its value, so this is always
    /// `null`; the field is always serialized.
    #[serde(default)]
    pub min_order_value: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub operator: Operator,
    pub keys: Vec<ProfileKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successor: Option<String>,
    pub issuance_policy: IssuancePolicy,
    #[serde(default)]
    pub proof: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeKind {
    Refunded,
    Returned,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    pub kind: OutcomeKind,
    /// Day the outcome happened, `YYYY-MM-DD`.
    pub on: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Annotation {
    pub order_ref: String,
    pub outcomes: Vec<Outcome>,
    pub revision: i64,
    #[serde(default)]
    pub proof: String,
}

impl Profile {
    /// Validates the record and sets `proof`, signed with the attestor's
    /// Ed25519 secret key.
    pub fn sign(mut self, secret_key: &[u8; 32]) -> Result<Self, RecordError> {
        self.proof = String::new();
        self.validate_fields()?;
        self.proof = sign_proof(ATTESTOR_PROFILE_TYP, &to_object(&self)?, secret_key)?;
        Ok(self)
    }

    /// The canonical body the proof covers.
    pub fn canonical_body(&self) -> Result<String, RecordError> {
        canonical_body(&to_object(self)?)
    }

    /// The whole record, `proof` included, in canonical form: the bytes the
    /// publisher writes.
    pub fn to_canonical_json(&self) -> Result<String, RecordError> {
        canonical_json(&to_object(self)?)
    }

    /// BLAKE3 of the canonical body: equal for equal content, whoever signs.
    pub fn content_hash(&self) -> Result<String, RecordError> {
        Ok(blake3::hash(self.canonical_body()?.as_bytes())
            .to_hex()
            .to_string())
    }

    pub fn validate_fields(&self) -> Result<(), RecordError> {
        let name_length = self.operator.name.chars().count();
        if !(1..=MAX_OPERATOR_NAME_LENGTH).contains(&name_length)
            || self.operator.name.trim() != self.operator.name
        {
            return Err(invalid(format!(
                "operator.name must be 1-{MAX_OPERATOR_NAME_LENGTH} characters without surrounding whitespace"
            )));
        }
        validate_https_url(&self.operator.url, "operator.url")?;
        validate_https_url(&self.operator.policy_url, "operator.policyUrl")?;

        if !(1..=MAX_PROFILE_KEYS).contains(&self.keys.len()) {
            return Err(invalid(format!(
                "keys must hold 1-{MAX_PROFILE_KEYS} entries"
            )));
        }
        for (index, key) in self.keys.iter().enumerate() {
            validate_pubky(&key.pubky, "keys.pubky")?;
            if self.keys[..index]
                .iter()
                .any(|other| other.pubky == key.pubky)
            {
                return Err(invalid("keys must not repeat a pubky"));
            }
            let from = parse_rfc3339_millis(&key.valid_from)
                .map_err(|e| invalid(format!("keys.validFrom {e}")))?;
            if let Some(until) = &key.valid_until {
                let until = parse_rfc3339_millis(until)
                    .map_err(|e| invalid(format!("keys.validUntil {e}")))?;
                if until <= from {
                    return Err(invalid("keys.validUntil must follow validFrom"));
                }
            }
            if let Some(compromised) = &key.compromised_from {
                let compromised = parse_rfc3339_millis(compromised)
                    .map_err(|e| invalid(format!("keys.compromisedFrom {e}")))?;
                if compromised < from {
                    return Err(invalid("keys.compromisedFrom must not precede validFrom"));
                }
            }
        }
        if let Some(successor) = &self.successor {
            validate_pubky(successor, "successor")?;
        }

        let policy = &self.issuance_policy;
        if !(0..=MAX_EDIT_WINDOW_HOURS).contains(&policy.edit_window_hours) {
            return Err(invalid(format!(
                "issuancePolicy.editWindowHours must be 0-{MAX_EDIT_WINDOW_HOURS}"
            )));
        }
        if !(0..=MAX_BLIND_WINDOW_DAYS).contains(&policy.blind_window_days) {
            return Err(invalid(format!(
                "issuancePolicy.blindWindowDays must be 0-{MAX_BLIND_WINDOW_DAYS}"
            )));
        }
        validate_policy_value(&policy.amount_band, "issuancePolicy.amountBand")?;
        validate_policy_value(&policy.price_decade, "issuancePolicy.priceDecade")?;
        if policy.min_order_value.is_some() {
            return Err(invalid(
                "issuancePolicy.minOrderValue is not issued by this service",
            ));
        }
        Ok(())
    }
}

impl Annotation {
    /// Validates the record and sets `proof`, signed with the attestor's
    /// Ed25519 secret key.
    pub fn sign(mut self, secret_key: &[u8; 32]) -> Result<Self, RecordError> {
        self.proof = String::new();
        self.validate_fields()?;
        self.proof = sign_proof(ATTESTOR_ANNOTATION_TYP, &to_object(&self)?, secret_key)?;
        Ok(self)
    }

    pub fn canonical_body(&self) -> Result<String, RecordError> {
        canonical_body(&to_object(self)?)
    }

    pub fn to_canonical_json(&self) -> Result<String, RecordError> {
        canonical_json(&to_object(self)?)
    }

    /// BLAKE3 of the order ref and its outcomes, without the revision: equal
    /// for equal content, so a change of outcomes is what moves the revision.
    pub fn content_hash(&self) -> Result<String, RecordError> {
        let content = json!({ "orderRef": self.order_ref, "outcomes": self.outcomes });
        Ok(blake3::hash(canonical_json(&content)?.as_bytes())
            .to_hex()
            .to_string())
    }

    pub fn validate_fields(&self) -> Result<(), RecordError> {
        if self.order_ref.len() != 64
            || !self
                .order_ref
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        {
            return Err(invalid(
                "orderRef must be a lowercase 64-character BLAKE3 hex hash",
            ));
        }
        if self.outcomes.len() > MAX_ANNOTATION_OUTCOMES {
            return Err(invalid(format!(
                "outcomes must hold at most {MAX_ANNOTATION_OUTCOMES} entries"
            )));
        }
        if let Some(outcome) = self.outcomes.iter().find(|o| !is_civil_date(&o.on)) {
            return Err(invalid(format!(
                "outcomes.on must be a YYYY-MM-DD date, got {:?}",
                outcome.on
            )));
        }
        if !(1..=MAX_SAFE_INTEGER).contains(&self.revision) {
            return Err(invalid("revision must be a positive safe integer"));
        }
        Ok(())
    }
}

fn validate_pubky(value: &str, field: &str) -> Result<(), RecordError> {
    if !is_valid_pubky(value) {
        return Err(invalid(format!(
            "{field} must be a 52-character z-base-32 pubky"
        )));
    }
    Ok(())
}

fn validate_https_url(value: &str, field: &str) -> Result<(), RecordError> {
    let error = || {
        invalid(format!(
            "{field} must be an https URL of at most {MAX_URL_LENGTH} characters"
        ))
    };
    if value.chars().count() > MAX_URL_LENGTH {
        return Err(error());
    }
    let parsed = url::Url::parse(value).map_err(|_| error())?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(error());
    }
    Ok(())
}

fn validate_policy_value(value: &str, field: &str) -> Result<(), RecordError> {
    let length = value.chars().count();
    if !(1..=MAX_POLICY_VALUE_LENGTH).contains(&length) || value.trim() != value {
        return Err(invalid(format!(
            "{field} must be 1-{MAX_POLICY_VALUE_LENGTH} characters without surrounding whitespace"
        )));
    }
    Ok(())
}

fn to_object<T: Serialize>(record: &T) -> Result<Value, RecordError> {
    serde_json::to_value(record).map_err(|e| RecordError(format!("Proof Error: {e}")))
}

/// The record's JSON object without `proof`, in canonical form.
fn canonical_body(record: &Value) -> Result<String, RecordError> {
    let Value::Object(members) = record else {
        return Err(RecordError(
            "Proof Error: record must be a JSON object".into(),
        ));
    };
    let mut body = members.clone();
    body.remove("proof");
    canonical_json(&Value::Object(body))
}

/// RFC 8785 serialization. The records hold only strings, integers, booleans,
/// null, arrays and objects, so it is members sorted by key, no whitespace.
fn canonical_json(value: &Value) -> Result<String, RecordError> {
    serde_json_canonicalizer::to_string(value).map_err(|e| RecordError(format!("Proof Error: {e}")))
}

fn sign_proof(typ: &str, record: &Value, secret_key: &[u8; 32]) -> Result<String, RecordError> {
    let header = json!({ "alg": "EdDSA", "typ": typ });
    let header_b64 = URL_SAFE_NO_PAD.encode(canonical_json(&header)?.as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(canonical_body(record)?.as_bytes());
    let signature =
        SigningKey::from_bytes(secret_key).sign(format!("{header_b64}.{payload_b64}").as_bytes());
    Ok(format!(
        "{header_b64}..{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

/// True for a real `YYYY-MM-DD` calendar date.
pub fn is_civil_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = &bytes[range];
        if !slice.iter().all(u8::is_ascii_digit) {
            return None;
        }
        Some(
            slice
                .iter()
                .fold(0i64, |acc, b| acc * 10 + i64::from(b - b'0')),
        )
    };
    let (Some(year), Some(month), Some(day)) = (digits(0..4), digits(5..7), digits(8..10)) else {
        return false;
    };
    (1..=12).contains(&month) && (1..=days_in_month(year, month)).contains(&day)
}

/// Milliseconds since the epoch of an RFC 3339 datetime in the layout the
/// specs accept: `YYYY-MM-DDTHH:MM:SS`, up to nine fractional digits, then `Z`
/// or `±HH:MM`, nothing else.
fn parse_rfc3339_millis(value: &str) -> Result<i64, String> {
    let bytes = value.as_bytes();
    let error = "must be an ISO-8601 datetime with offset".to_string();

    let digits = |range: std::ops::Range<usize>| -> Result<i64, String> {
        let slice = bytes.get(range).ok_or_else(|| error.clone())?;
        if !slice.iter().all(u8::is_ascii_digit) {
            return Err(error.clone());
        }
        Ok(slice
            .iter()
            .fold(0i64, |acc, b| acc * 10 + i64::from(b - b'0')))
    };

    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(error);
    }
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;

    if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
        return Err(error);
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(error);
    }

    let mut cursor = 19;
    let mut millis_fraction = 0i64;
    if bytes.get(cursor) == Some(&b'.') {
        let fraction_start = cursor + 1;
        let mut fraction_end = fraction_start;
        while bytes.get(fraction_end).is_some_and(u8::is_ascii_digit) {
            fraction_end += 1;
        }
        if fraction_end == fraction_start || fraction_end - fraction_start > 9 {
            return Err(error);
        }
        for (index, byte) in bytes[fraction_start..fraction_end.min(fraction_start + 3)]
            .iter()
            .enumerate()
        {
            millis_fraction += i64::from(byte - b'0') * 10i64.pow(2 - index as u32);
        }
        cursor = fraction_end;
    }

    let offset_seconds = match bytes.get(cursor) {
        Some(b'Z') if cursor + 1 == bytes.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            if cursor + 6 != bytes.len() || bytes[cursor + 3] != b':' {
                return Err(error);
            }
            let offset_hour = digits(cursor + 1..cursor + 3)?;
            let offset_minute = digits(cursor + 4..cursor + 6)?;
            if offset_hour > 23 || offset_minute > 59 {
                return Err(error);
            }
            let seconds = offset_hour * 3600 + offset_minute * 60;
            if *sign == b'+' {
                seconds
            } else {
                -seconds
            }
        }
        _ => return Err(error),
    };

    let days = days_from_civil(year, month, day);
    let epoch_seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    Ok(epoch_seconds * 1_000 + millis_fraction)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        _ => 28,
    }
}

/// Days since the UNIX epoch for a civil date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let adjusted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
