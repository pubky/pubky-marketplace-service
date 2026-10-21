//! The marketplace attestor identity (ADR 0024, ratified D6).
//!
//! The attestor is a Pubky identity: an Ed25519 key whose z-base-32 encoding
//! is the attestor's pubky. It signs two kinds of compact JWS (RFC 7515,
//! `alg: EdDSA` per RFC 8037):
//!
//! - **Purchase attestations** issued inside the `review.create`
//!   transaction and embedded by the client in the published review record's
//!   `eligibilityAttestation` field. Durable, no expiry; verifiable offline
//!   by decoding `iss` from z-base-32 (the pubky *is* the verification key).
//! - **Seller stat attestations** computed weekly from the private order
//!   book (D3: median time-to-ship, completion rate) and
//!   signed for later publication on the attestor's own homeserver
//!   (publication is Phase 3 of the trust & reputation plan; this service
//!   currently signs and stores them).
//!
//! Claim privacy follows ADR 0019 §8 verbatim: no exact amounts, no
//! addresses, no payment identifiers, no bundle ids. `order_ref` is an
//! attestor-salted Blake3 hash of the private order UUID — nobody without
//! the salt can link it back to an order. `completed_on` is day-granularity.
//! The optional `amount_band` log-decade band is emitted only under
//! both-sides consent (ratified D2): the seller's standing preference AND
//! the reviewer's per-review opt-in. `price_decade`, the log decade of the
//! bound listing's unit price paid, is always emitted (reviews decision
//! RV1): it discloses only the order of magnitude the public listing price
//! already shows.
//!
//! Key custody (ratified D6): the secret lives in the `ATTESTOR_SECRET_KEY`
//! environment variable (KMS/env-held, same process as the service), to be
//! revisited before real funds move. The salt (`ATTESTOR_ORDER_SALT`) must
//! stay stable for the life of the attestor identity — changing it silently
//! unlinks every previously issued `order_ref` from its annotations.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
use marketplace_domain::pubky::encode_pubky;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::clock::format_timestamp;

pub const ENV_ATTESTOR_SECRET_KEY: &str = "ATTESTOR_SECRET_KEY";
pub const ENV_ATTESTOR_ORDER_SALT: &str = "ATTESTOR_ORDER_SALT";

/// JOSE `typ` of a v1 purchase attestation (spec fork: normative reference
/// in `pubky-app-specs` `marketplace_attestation.rs`).
pub const PURCHASE_ATTESTATION_TYP: &str = "pubky-purchase-attestation+v1";
/// JOSE `typ` of a v1 seller stat attestation.
pub const SELLER_STATS_TYP: &str = "pubky-seller-stats+v1";
/// JOSE `typ` of a v1 order receipt attestation ("credible exit for
/// orders"): the compact JWS a participant embeds in the portable receipt
/// record published on their own homeserver, verifiable against the
/// attestor pubky alone after this operator disappears.
pub const RECEIPT_ATTESTATION_TYP: &str = "pubky-order-receipt+v1";
pub const RECEIPT_ATTESTATION_V2_TYP: &str = "pubky-order-receipt+v2";
/// JOSE `typ` of a v1 drop edition attestation (ADR-0026 layer 2): the
/// compact JWS attesting which numbered edition of a drop a paid order
/// received, verifiable against the attestor pubky alone.
pub const DROP_EDITION_ATTESTATION_TYP: &str = "pubky-drop-edition+v1";

pub struct Attestor {
    signing_key: SigningKey,
    pubky: String,
    order_salt: [u8; 32],
}

/// A freshly issued purchase attestation: the compact JWS plus its claims
/// (returned in the command result and stored for idempotent re-fetch).
#[derive(Debug, Clone)]
pub struct IssuedAttestation {
    pub jws: String,
    pub claims: Value,
    pub order_ref: String,
}

/// A freshly issued receipt attestation: the compact JWS plus its claims.
/// Never stored — every claim derives from stored rows, so re-issuance is
/// deterministic and the fetch endpoint re-signs on demand.
#[derive(Debug, Clone)]
pub struct IssuedReceiptAttestation {
    pub jws: String,
    pub claims: Value,
}

/// Claims of a v1 receipt attestation. The field order is normative: the
/// specs-fork verifier checks the serialized payload byte-for-byte, so this
/// struct's declaration order (which `serde_json` preserves) is the wire
/// contract.
#[derive(Debug, Clone, Serialize)]
struct ReceiptAttestationClaims {
    v: i64,
    iss: String,
    buyer: String,
    seller: String,
    order: String,
    receipt: String,
    total_minor: i64,
    currency: String,
    exponent: i64,
    paid_at: String,
    iat: i64,
}

#[derive(Debug, Clone, Serialize)]
struct ReceiptAttestationClaimsV2 {
    v: i64,
    iss: String,
    buyer: String,
    seller: String,
    order: String,
    receipt: String,
    settlement_total: Value,
    merchandise_total: Value,
    paid_at: String,
    iat: i64,
}

/// A freshly issued drop edition attestation: the compact JWS plus its
/// claims. Like the receipt attestation, never stored — every claim derives
/// from stored rows, so re-issuance is deterministic.
#[derive(Debug, Clone)]
pub struct IssuedDropEditionAttestation {
    pub jws: String,
    pub claims: Value,
}

/// Claims of a v1 drop edition attestation. The field order is normative:
/// the specs-fork verifier for `pubky-drop-edition+v1` checks the
/// serialized payload byte-for-byte, so this struct's declaration order
/// (which `serde_json` preserves) is the wire contract.
#[derive(Debug, Clone, Serialize)]
struct DropEditionAttestationClaims {
    v: i64,
    iss: String,
    buyer: String,
    seller: String,
    drop: String,
    edition: i64,
    of: i64,
    receipt: String,
    iat: i64,
}

impl Attestor {
    #[allow(clippy::too_many_arguments)]
    pub fn issue_receipt_attestation_v2(
        &self,
        order_id: Uuid,
        receipt_id: Uuid,
        buyer_pubky: &str,
        seller_pubky: &str,
        settlement_total: Value,
        merchandise_total: Value,
        paid_at: DateTime<Utc>,
    ) -> IssuedReceiptAttestation {
        let claims = ReceiptAttestationClaimsV2 {
            v: 2,
            iss: self.pubky.clone(),
            buyer: buyer_pubky.to_string(),
            seller: seller_pubky.to_string(),
            order: order_id.to_string(),
            receipt: receipt_id.to_string(),
            settlement_total,
            merchandise_total,
            paid_at: format_timestamp(paid_at),
            iat: paid_at.timestamp(),
        };
        let claims = serde_json::to_value(claims).expect("claims serialize infallibly");
        let jws = self.sign_compact(RECEIPT_ATTESTATION_V2_TYP, &claims);
        IssuedReceiptAttestation { jws, claims }
    }
    /// Builds the attestor from `ATTESTOR_SECRET_KEY` and
    /// `ATTESTOR_ORDER_SALT` (both 64 hex chars). Fail closed on partial
    /// configuration: either both are set or attestation support is off.
    pub fn from_env() -> anyhow::Result<Option<Arc<Attestor>>> {
        let secret = std::env::var(ENV_ATTESTOR_SECRET_KEY).ok();
        let salt = std::env::var(ENV_ATTESTOR_ORDER_SALT).ok();
        match (secret, salt) {
            (None, None) => Ok(None),
            (Some(secret), Some(salt)) => Ok(Some(Arc::new(Self::from_hex(&secret, &salt)?))),
            _ => anyhow::bail!(
                "Attestation is partially configured: set both \
                 {ENV_ATTESTOR_SECRET_KEY} and {ENV_ATTESTOR_ORDER_SALT}, or neither"
            ),
        }
    }

    pub fn from_hex(secret_hex: &str, salt_hex: &str) -> anyhow::Result<Self> {
        let secret: [u8; 32] = hex::decode(secret_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                anyhow::anyhow!("{ENV_ATTESTOR_SECRET_KEY} must be 64 hex characters")
            })?;
        let order_salt: [u8; 32] = hex::decode(salt_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                anyhow::anyhow!("{ENV_ATTESTOR_ORDER_SALT} must be 64 hex characters")
            })?;
        let signing_key = SigningKey::from_bytes(&secret);
        let pubky = encode_pubky(signing_key.verifying_key().as_bytes());
        Ok(Self {
            signing_key,
            pubky,
            order_salt,
        })
    }

    /// The attestor's pubky: the z-base-32 encoding of its Ed25519
    /// verification key. Verifiers decode this to check signatures — no key
    /// server involved.
    pub fn pubky(&self) -> &str {
        &self.pubky
    }

    /// The opaque, attestor-salted reference for an order: lowercase hex
    /// Blake3 of the order UUID bytes concatenated with the private salt.
    /// Lets annotations and repeat-purchase attestations reference an order
    /// without exposing service identifiers (ADR 0019 §8).
    pub fn order_ref(&self, order_id: Uuid) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(order_id.as_bytes());
        hasher.update(&self.order_salt);
        hasher.finalize().to_hex().to_string()
    }

    /// Issues the durable purchase attestation for one (order, reviewer)
    /// pair. `role` is the record-vocabulary direction
    /// (`buyer_reviewing_seller` / `seller_reviewing_buyer`); `amount_band`
    /// must already have passed the D2 both-sides consent gate;
    /// `price_decade` is [`price_decade`] of the listing's unit price paid.
    #[allow(clippy::too_many_arguments)]
    pub fn issue_purchase_attestation(
        &self,
        order_id: Uuid,
        reviewer_pubky: &str,
        counterparty_pubky: &str,
        role: &str,
        listing_uri: &str,
        completed_on: &str,
        amount_band: Option<String>,
        price_decade: Option<String>,
        now: DateTime<Utc>,
    ) -> IssuedAttestation {
        let order_ref = self.order_ref(order_id);
        // Claim order matches the design doc example (§5.3); serde_json is
        // built with preserve_order, so serialization is stable.
        let mut claims = json!({
            "v": 1,
            "iss": self.pubky,
            "sub": reviewer_pubky,
            "cpk": counterparty_pubky,
            "role": role,
            "listing": listing_uri,
            "order_ref": order_ref,
            "completed_on": completed_on,
        });
        if let Some(band) = amount_band {
            claims["amount_band"] = json!(band);
        }
        if let Some(decade) = price_decade {
            claims["price_decade"] = json!(decade);
        }
        claims["iat"] = json!(now.timestamp());
        let jws = self.sign_compact(PURCHASE_ATTESTATION_TYP, &claims);
        IssuedAttestation {
            jws,
            claims,
            order_ref,
        }
    }

    /// Signs a seller stat attestation body (already banded/per-mille per
    /// design §7.2) as a compact JWS.
    pub fn sign_seller_stats(&self, body: &Value) -> String {
        self.sign_compact(SELLER_STATS_TYP, body)
    }

    /// Issues the receipt attestation for one paid order ("credible exit
    /// for orders"): a compact JWS attesting the receipt's facts so the
    /// portable receipt document a participant publishes on their own
    /// homeserver stays verifiable offline. `paid_at` is the receipt's
    /// stored creation instant — never the current time — so repeated
    /// issuance returns the byte-identical JWS.
    #[allow(clippy::too_many_arguments)]
    pub fn issue_receipt_attestation(
        &self,
        order_id: Uuid,
        receipt_id: Uuid,
        buyer_pubky: &str,
        seller_pubky: &str,
        total_minor: i64,
        currency: &str,
        exponent: i64,
        paid_at: DateTime<Utc>,
    ) -> IssuedReceiptAttestation {
        let claims = ReceiptAttestationClaims {
            v: 1,
            iss: self.pubky.clone(),
            buyer: buyer_pubky.to_string(),
            seller: seller_pubky.to_string(),
            order: order_id.to_string(),
            receipt: receipt_id.to_string(),
            total_minor,
            currency: currency.to_string(),
            exponent,
            paid_at: format_timestamp(paid_at),
            iat: paid_at.timestamp(),
        };
        let claims = serde_json::to_value(claims).expect("claims serialize infallibly");
        let jws = self.sign_compact(RECEIPT_ATTESTATION_TYP, &claims);
        IssuedReceiptAttestation { jws, claims }
    }

    /// Issues the drop edition attestation for one paid drop order: a
    /// compact JWS attesting that the order's buyer received `edition` of
    /// `of` units of the seller's drop. `drop` is the DROP ID (the seller's
    /// record identifier), never the aggregate id. `issued_at` is the
    /// receipt's stored creation instant — never the current time — so
    /// repeated issuance returns the byte-identical JWS.
    #[allow(clippy::too_many_arguments)]
    pub fn issue_drop_edition_attestation(
        &self,
        receipt_id: Uuid,
        buyer_pubky: &str,
        seller_pubky: &str,
        drop_id: &str,
        edition: i64,
        of: i64,
        issued_at: DateTime<Utc>,
    ) -> IssuedDropEditionAttestation {
        let claims = DropEditionAttestationClaims {
            v: 1,
            iss: self.pubky.clone(),
            buyer: buyer_pubky.to_string(),
            seller: seller_pubky.to_string(),
            drop: drop_id.to_string(),
            edition,
            of,
            receipt: receipt_id.to_string(),
            iat: issued_at.timestamp(),
        };
        let claims = serde_json::to_value(claims).expect("claims serialize infallibly");
        let jws = self.sign_compact(DROP_EDITION_ATTESTATION_TYP, &claims);
        IssuedDropEditionAttestation { jws, claims }
    }

    fn sign_compact(&self, typ: &str, payload: &Value) -> String {
        let header = json!({ "alg": "EdDSA", "typ": typ });
        let header_b64 = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&header).expect("header serializes infallibly"));
        let payload_b64 = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(payload).expect("claims serialize infallibly"));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = self.signing_key.sign(signing_input.as_bytes());
        format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        )
    }
}

/// Maps the service's stored reviewer role (`buyer` / `seller`) to the
/// public record vocabulary the attestation claims use.
pub fn claim_role(reviewer_role: &str) -> &'static str {
    if reviewer_role == "buyer" {
        "buyer_reviewing_seller"
    } else {
        "seller_reviewing_buyer"
    }
}

/// The log-decade amount band for an order total in minor units, e.g.
/// 850_000 sats -> `SAT:5`. `None` for non-positive totals (nothing honest
/// to band).
pub fn amount_band(currency: &str, total_minor: i64) -> Option<String> {
    if total_minor < 1 {
        return None;
    }
    Some(format!("{currency}:{}", total_minor.ilog10()))
}

/// The specs price weight table's minor units (`pubky-app-specs`
/// `PRICE_WEIGHT_CURRENCIES`): the exponent each currency is decaded at.
pub const PRICE_DECADE_EXPONENTS: &[(&str, i64)] = &[
    ("USD", 2),
    ("EUR", 2),
    ("GBP", 2),
    ("CHF", 2),
    ("CAD", 2),
    ("AUD", 2),
    ("JPY", 0),
    ("BTC", 8),
    ("SAT", 0),
];

/// i64 minor units cap a decade at 18, the most the claim may carry.
const MAX_PRICE_DECADE: i64 = 18;

/// The `price_decade` claim for a unit price of `unit_price_minor` at
/// `exponent` decimal places of `currency`, exactly as the specs helper
/// `price_decade(currency, exponent, unit_price_minor)` computes it, e.g.
/// `("USD", 2, 4_999)` -> `USD:3` and `("BTC", 8, 25_000)` -> `BTC:4`.
///
/// The currency code is kept as written (`BTC` is never rewritten to
/// `SAT`). A table currency is decaded in the table's minor unit, so a price
/// at another exponent shifts the decade by `table exponent - exponent`; a
/// currency outside the table keeps its own minor-unit decade. Decades above
/// 18 are written as 18. `None`, and no claim, for a non-positive price or
/// one below a single table minor unit.
pub fn price_decade(currency: &str, exponent: i64, unit_price_minor: i64) -> Option<String> {
    if unit_price_minor < 1 {
        return None;
    }
    let shift = PRICE_DECADE_EXPONENTS
        .iter()
        .find(|(code, _)| *code == currency)
        .map_or(0, |(_, table_exponent)| table_exponent - exponent);
    let decade = i64::from(unit_price_minor.ilog10()) + shift;
    if decade < 0 {
        return None;
    }
    Some(format!("{currency}:{}", decade.min(MAX_PRICE_DECADE)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    const SECRET: &str = "0707070707070707070707070707070707070707070707070707070707070707";
    const SALT: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn attestor() -> Attestor {
        Attestor::from_hex(SECRET, SALT).expect("test attestor builds")
    }

    /// Decodes a z-base-32 pubky back to key bytes (verifier-side recipe).
    fn decode_pubky(pubky: &str) -> [u8; 32] {
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

    #[test]
    fn pubky_round_trips_to_the_verifying_key() {
        let attestor = attestor();
        assert_eq!(attestor.pubky().len(), 52);
        let decoded = decode_pubky(attestor.pubky());
        assert_eq!(&decoded, attestor.signing_key.verifying_key().as_bytes());
    }

    #[test]
    fn order_ref_is_deterministic_and_salted() {
        let attestor = attestor();
        let order = Uuid::from_u128(42);
        assert_eq!(attestor.order_ref(order), attestor.order_ref(order));
        assert_eq!(attestor.order_ref(order).len(), 64);

        let other_salt = Attestor::from_hex(
            SECRET,
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        assert_ne!(attestor.order_ref(order), other_salt.order_ref(order));
    }

    #[test]
    fn issued_attestation_verifies_with_the_attestor_pubky() {
        let attestor = attestor();
        let now = "2026-08-21T12:00:00Z".parse().unwrap();
        let issued = attestor.issue_purchase_attestation(
            Uuid::from_u128(7),
            &"o".repeat(52),
            &"p".repeat(52),
            "buyer_reviewing_seller",
            "pubky://ppppp/pub/pubky.app/marketplace/v1/listings/x",
            "2026-08-20",
            Some("SAT:5".to_string()),
            Some("SAT:4".to_string()),
            now,
        );
        let keys: Vec<&str> = issued
            .claims
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "v",
                "iss",
                "sub",
                "cpk",
                "role",
                "listing",
                "order_ref",
                "completed_on",
                "amount_band",
                "price_decade",
                "iat"
            ]
        );
        let mut parts = issued.jws.split('.');
        let header_b64 = parts.next().unwrap();
        let payload_b64 = parts.next().unwrap();
        let signature_b64 = parts.next().unwrap();
        assert!(parts.next().is_none());

        let header: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header_b64).unwrap()).unwrap();
        assert_eq!(header["alg"], "EdDSA");
        assert_eq!(header["typ"], PURCHASE_ATTESTATION_TYP);

        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload_b64).unwrap()).unwrap();
        assert_eq!(claims, issued.claims);
        assert_eq!(claims["order_ref"], json!(issued.order_ref));

        // Verifier recipe: decode iss -> Ed25519 key -> verify.
        let key = VerifyingKey::from_bytes(&decode_pubky(claims["iss"].as_str().unwrap()))
            .expect("valid key");
        let signature = Signature::from_bytes(
            &URL_SAFE_NO_PAD
                .decode(signature_b64)
                .unwrap()
                .try_into()
                .expect("64 bytes"),
        );
        key.verify(format!("{header_b64}.{payload_b64}").as_bytes(), &signature)
            .expect("signature verifies");
    }

    #[test]
    fn receipt_attestation_is_deterministic_with_the_exact_wire_shape() {
        let attestor = attestor();
        let paid_at = "2026-08-19T22:00:00Z".parse().unwrap();
        let issue = || {
            attestor.issue_receipt_attestation(
                Uuid::from_u128(7),
                Uuid::from_u128(9),
                &"b".repeat(52),
                &"s".repeat(52),
                14_796,
                "USD",
                2,
                paid_at,
            )
        };
        let issued = issue();
        if std::env::var_os("UPDATE_RECEIPT_SAMPLES").is_some() {
            let write_sample = |name: &str, issued: IssuedReceiptAttestation| {
                let sample = json!({
                    "receipt_attestation": {
                        "jws": issued.jws,
                        "claims": issued.claims,
                    },
                    "attestor_pubky": attestor.pubky(),
                });
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../contracts/samples")
                    .join(format!("receipt-attestation-{name}.json"));
                std::fs::write(
                    path,
                    serde_json::to_vec_pretty(&sample).expect("sample serializes"),
                )
                .expect("sample writes");
            };
            write_sample("v1", issued.clone());
            write_sample(
                "v2-bitcoin",
                attestor.issue_receipt_attestation_v2(
                    Uuid::parse_str("97c2ff18-cf54-4727-bbc4-4463df5c5084").unwrap(),
                    Uuid::parse_str("de7fa583-bbc3-415d-96c6-d6a54a185221").unwrap(),
                    "hnhabjbjbjjxzbcif7stmqi9p76qubombhsujfrgezkmfgqydm8o",
                    "obh36yni5ctgxac4wwicubxj9hdksx5aqrmsqb8undokqqekhpfy",
                    json!({"amount_minor": 51_637, "currency": "SAT", "exponent": 0}),
                    json!({"amount_minor": 13_700, "currency": "USD", "exponent": 2}),
                    paid_at,
                ),
            );
            write_sample(
                "v2-same-currency",
                attestor.issue_receipt_attestation_v2(
                    Uuid::parse_str("7bdd4ba2-ac6f-4370-a6ef-8edd469b28d2").unwrap(),
                    Uuid::parse_str("e5752e58-9b32-40e8-86c3-2a80c908a955").unwrap(),
                    "6m3819xgydzdrc1hihdj9r9k45zze5qjbzcs5mh98sp5abi9x87y",
                    "ddkhtthrpa7m94apwy6437ddz47a61jpyq9n5zo8kdozqbu4phuy",
                    json!({"amount_minor": 13_700, "currency": "USD", "exponent": 2}),
                    json!({"amount_minor": 13_700, "currency": "USD", "exponent": 2}),
                    paid_at,
                ),
            );
        }
        // Deterministic: no `now` input, so re-issuance is byte-identical.
        assert_eq!(issued.jws, issue().jws);

        let mut parts = issued.jws.split('.');
        let header_b64 = parts.next().unwrap();
        let payload_b64 = parts.next().unwrap();
        let signature_b64 = parts.next().unwrap();
        assert!(parts.next().is_none());

        // The protected header is byte-exact (specs-fork verifier contract).
        assert_eq!(
            String::from_utf8(URL_SAFE_NO_PAD.decode(header_b64).unwrap()).unwrap(),
            r#"{"alg":"EdDSA","typ":"pubky-order-receipt+v1"}"#
        );

        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload_b64).unwrap()).unwrap();
        assert_eq!(claims, issued.claims);
        let keys: Vec<&str> = claims
            .as_object()
            .expect("claims are an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            vec![
                "v",
                "iss",
                "buyer",
                "seller",
                "order",
                "receipt",
                "total_minor",
                "currency",
                "exponent",
                "paid_at",
                "iat"
            ]
        );
        assert_eq!(claims["v"], json!(1));
        assert_eq!(claims["iss"], json!(attestor.pubky()));
        assert_eq!(
            claims["order"],
            json!("00000000-0000-0000-0000-000000000007")
        );
        assert_eq!(
            claims["receipt"],
            json!("00000000-0000-0000-0000-000000000009")
        );
        assert_eq!(claims["total_minor"], json!(14_796));
        assert_eq!(claims["currency"], json!("USD"));
        assert_eq!(claims["exponent"], json!(2));
        assert_eq!(claims["paid_at"], json!("2026-08-19T22:00:00.000Z"));
        assert_eq!(claims["iat"], json!(1_787_176_800));

        let key = VerifyingKey::from_bytes(&decode_pubky(attestor.pubky())).expect("valid key");
        let signature = Signature::from_bytes(
            &URL_SAFE_NO_PAD
                .decode(signature_b64)
                .unwrap()
                .try_into()
                .expect("64 bytes"),
        );
        key.verify(format!("{header_b64}.{payload_b64}").as_bytes(), &signature)
            .expect("signature verifies");
    }

    #[test]
    fn drop_edition_attestation_is_deterministic_with_the_exact_wire_shape() {
        let attestor = attestor();
        let issued_at = "2026-08-19T22:00:00Z".parse().unwrap();
        let issue = || {
            attestor.issue_drop_edition_attestation(
                Uuid::from_u128(9),
                &"b".repeat(52),
                &"s".repeat(52),
                "winter_drop",
                7,
                100,
                issued_at,
            )
        };
        let issued = issue();
        // Deterministic: no `now` input, so re-issuance is byte-identical.
        assert_eq!(issued.jws, issue().jws);

        let mut parts = issued.jws.split('.');
        let header_b64 = parts.next().unwrap();
        let payload_b64 = parts.next().unwrap();
        let signature_b64 = parts.next().unwrap();
        assert!(parts.next().is_none());

        // The protected header is byte-exact (specs-fork verifier contract).
        assert_eq!(
            String::from_utf8(URL_SAFE_NO_PAD.decode(header_b64).unwrap()).unwrap(),
            r#"{"alg":"EdDSA","typ":"pubky-drop-edition+v1"}"#
        );

        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload_b64).unwrap()).unwrap();
        assert_eq!(claims, issued.claims);
        let keys: Vec<&str> = claims
            .as_object()
            .expect("claims are an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            vec!["v", "iss", "buyer", "seller", "drop", "edition", "of", "receipt", "iat"]
        );
        assert_eq!(claims["v"], json!(1));
        assert_eq!(claims["iss"], json!(attestor.pubky()));
        assert_eq!(claims["drop"], json!("winter_drop"));
        assert_eq!(claims["edition"], json!(7));
        assert_eq!(claims["of"], json!(100));
        assert_eq!(
            claims["receipt"],
            json!("00000000-0000-0000-0000-000000000009")
        );
        assert_eq!(claims["iat"], json!(1_787_176_800));

        let key = VerifyingKey::from_bytes(&decode_pubky(attestor.pubky())).expect("valid key");
        let signature = Signature::from_bytes(
            &URL_SAFE_NO_PAD
                .decode(signature_b64)
                .unwrap()
                .try_into()
                .expect("64 bytes"),
        );
        key.verify(format!("{header_b64}.{payload_b64}").as_bytes(), &signature)
            .expect("signature verifies");
    }

    #[test]
    fn amount_band_is_a_log_decade() {
        assert_eq!(amount_band("SAT", 850_000), Some("SAT:5".to_string()));
        assert_eq!(amount_band("USD", 1), Some("USD:0".to_string()));
        assert_eq!(amount_band("USD", 99), Some("USD:1".to_string()));
        assert_eq!(amount_band("USD", 0), None);
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct PriceWeightVector {
        currency: String,
        exponent: i64,
        unit_price_minor: i64,
        price_decade: Option<String>,
        #[allow(dead_code)]
        weight: u8,
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PriceWeightVectors {
        #[allow(dead_code)]
        description: String,
        vectors: Vec<PriceWeightVector>,
    }

    #[test]
    fn price_decade_matches_the_specs_vectors() {
        let fixture: PriceWeightVectors =
            serde_json::from_str(include_str!("../tests/fixtures/price-weight-vectors.json"))
                .expect("vector fixture parses");
        for (code, _) in PRICE_DECADE_EXPONENTS {
            assert!(
                fixture
                    .vectors
                    .iter()
                    .any(|vector| vector.currency == *code),
                "no vector for {code}"
            );
        }
        for vector in &fixture.vectors {
            assert_eq!(
                price_decade(&vector.currency, vector.exponent, vector.unit_price_minor),
                vector.price_decade,
                "{} {} at exponent {}",
                vector.currency,
                vector.unit_price_minor,
                vector.exponent
            );
        }
    }

    #[test]
    fn price_decade_keeps_the_code_and_shifts_to_the_table_unit() {
        assert_eq!(price_decade("BTC", 8, 25_000).as_deref(), Some("BTC:4"));
        assert_eq!(price_decade("SAT", 0, 25_000).as_deref(), Some("SAT:4"));
        assert_eq!(price_decade("BTC", 0, 1).as_deref(), Some("BTC:8"));
        assert_eq!(price_decade("BTC", 11, 999), None);
        assert_eq!(price_decade("USD", 3, 10_000).as_deref(), Some("USD:3"));
        assert_eq!(price_decade("JPY", 2, 45_000).as_deref(), Some("JPY:2"));
        assert_eq!(price_decade("BTC", 0, i64::MAX).as_deref(), Some("BTC:18"));
        assert_eq!(price_decade("XYZ", 5, 1_000).as_deref(), Some("XYZ:3"));
        assert_eq!(price_decade("USD", 2, 0), None);
        assert_eq!(price_decade("USD", 2, -1_000), None);
    }

    #[test]
    fn every_registry_currency_is_decaded_at_its_registry_exponent() {
        use marketplace_domain::currency::{Currency, SUPPORTED_CODES};
        let codes: Vec<&str> = SUPPORTED_CODES.split(", ").collect();
        assert_eq!(codes.len(), PRICE_DECADE_EXPONENTS.len());
        for code in codes {
            let currency = Currency::parse(code).expect(code);
            assert_eq!(
                PRICE_DECADE_EXPONENTS
                    .iter()
                    .find(|(table_code, _)| *table_code == code)
                    .map(|(_, exponent)| *exponent),
                Some(i64::from(currency.exponent())),
                "{code}"
            );
        }
    }

    #[test]
    fn claim_role_maps_the_stored_vocabulary() {
        assert_eq!(claim_role("buyer"), "buyer_reviewing_seller");
        assert_eq!(claim_role("seller"), "seller_reviewing_buyer");
    }

    #[test]
    fn partial_hex_configuration_is_rejected() {
        assert!(Attestor::from_hex("zz", SALT).is_err());
        assert!(Attestor::from_hex(SECRET, "short").is_err());
    }
}
