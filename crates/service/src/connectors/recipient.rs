//! The connector recipient key and the address envelope sealed to it.
//!
//! A buyer who opts in to share a delivery address with a connected store
//! seals it to that connection's recipient key, an X25519 key the service
//! generates when the connection completes, keeps sealed in
//! `connector_recipient_keys` and publishes (public half only) in the
//! seller's connector status. The envelope is the delivery envelope's
//! construction, unchanged except for the recipient and the context string:
//!
//! * RFC 9180 HPKE, base mode, DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
//!   ChaCha20-Poly1305;
//! * `info` = [`ENVELOPE_CONTEXT`];
//! * associated data = `{context}|{checkout_command_id}|{buyer_pubky}|
//!   {seller_pubky}|{key_id}`, with the connector key id;
//! * plaintext = the address as JSON padded to 2,048 bytes with `0x80`
//!   then zeros, so every envelope is 32 + 2,048 + 16 = 2,096 bytes;
//! * wire = encapsulated key (32 bytes) then ciphertext, as unpadded
//!   base64url.
//!
//! The service opens an envelope in one place, [`open_address`], called by
//! the connector worker at push time. The address is never logged and the
//! envelope is deleted when the push is created.

use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, OpModeR, Serializable};
use rand::RngCore;

use super::provider::ShippingAddress;

type Kem = X25519HkdfSha256;

/// The HPKE `info` and the first AAD field of a connector address envelope.
/// The delivery envelope uses `pubky-marketplace/delivery-address/v1`; the
/// distinct context keeps one kind of envelope from opening as the other.
pub const ENVELOPE_CONTEXT: &str = "pubky-marketplace/connector-address/v1";
pub const ENVELOPE_BYTES: usize = 2096;
pub const KEY_ID_CHARS: usize = 32;
const PLAINTEXT_BUCKET: usize = 2048;
const ENCAPSULATED_KEY_BYTES: usize = 32;

/// A freshly generated recipient key. The secret is zeroed when dropped.
pub struct NewRecipientKey {
    pub key_id: String,
    pub public_key: [u8; 32],
    secret: [u8; 32],
}

impl NewRecipientKey {
    pub fn secret(&self) -> &[u8; 32] {
        &self.secret
    }
}

impl std::fmt::Debug for NewRecipientKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewRecipientKey")
            .field("key_id", &self.key_id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl Drop for NewRecipientKey {
    fn drop(&mut self) {
        self.secret.fill(0);
    }
}

pub fn generate() -> NewRecipientKey {
    let mut ikm = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut ikm);
    let (secret, public) = Kem::derive_keypair(&ikm);
    ikm.fill(0);
    let mut key_id = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut key_id);
    NewRecipientKey {
        key_id: hex::encode(key_id),
        public_key: public.to_bytes().into(),
        secret: secret.to_bytes().into(),
    }
}

pub fn envelope_aad(
    checkout_command_id: &str,
    buyer_pubky: &str,
    seller_pubky: &str,
    key_id: &str,
) -> Vec<u8> {
    format!("{ENVELOPE_CONTEXT}|{checkout_command_id}|{buyer_pubky}|{seller_pubky}|{key_id}")
        .into_bytes()
}

#[derive(Debug, PartialEq, Eq)]
pub enum OpenFailure {
    /// The envelope is not the right length, or did not authenticate under
    /// this key and these bindings.
    Unreadable,
    /// It opened, but the plaintext is not a padded address.
    Malformed,
}

/// Opens a connector address envelope with the recipient secret and returns
/// the address. Fails closed on any wrong key, wrong binding or tampering.
pub fn open_address(
    secret: &[u8; 32],
    aad: &[u8],
    wire: &[u8],
) -> Result<ShippingAddress, OpenFailure> {
    if wire.len() != ENVELOPE_BYTES {
        return Err(OpenFailure::Unreadable);
    }
    let private =
        <Kem as KemTrait>::PrivateKey::from_bytes(secret).map_err(|_| OpenFailure::Unreadable)?;
    let (enc, ciphertext) = wire.split_at(ENCAPSULATED_KEY_BYTES);
    let encapped =
        <Kem as KemTrait>::EncappedKey::from_bytes(enc).map_err(|_| OpenFailure::Unreadable)?;
    let padded = hpke::single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        &private,
        &encapped,
        ENVELOPE_CONTEXT.as_bytes(),
        ciphertext,
        aad,
    )
    .map_err(|_| OpenFailure::Unreadable)?;
    if padded.len() != PLAINTEXT_BUCKET {
        return Err(OpenFailure::Malformed);
    }
    let marker = padded
        .iter()
        .rposition(|byte| *byte != 0)
        .ok_or(OpenFailure::Malformed)?;
    if padded[marker] != 0x80 {
        return Err(OpenFailure::Malformed);
    }
    serde_json::from_slice(&padded[..marker]).map_err(|_| OpenFailure::Malformed)
}

#[cfg(test)]
pub mod test_support {
    //! Sealing, for tests only: production never seals a connector envelope
    //! (the buyer's client does).

    use super::*;
    use hpke::OpModeS;

    pub fn seal_address(public_key: &[u8; 32], aad: &[u8], address: &serde_json::Value) -> Vec<u8> {
        let recipient = <Kem as KemTrait>::PublicKey::from_bytes(public_key).expect("public key");
        let mut plaintext = serde_json::to_vec(address).expect("address serializes");
        plaintext.push(0x80);
        plaintext.resize(PLAINTEXT_BUCKET, 0);
        let (encapped, ciphertext) = hpke::single_shot_seal::<ChaCha20Poly1305, HkdfSha256, Kem>(
            &OpModeS::Base,
            &recipient,
            ENVELOPE_CONTEXT.as_bytes(),
            &plaintext,
            aad,
        )
        .expect("seal");
        let mut wire = encapped.to_bytes().to_vec();
        wire.extend_from_slice(&ciphertext);
        wire
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::seal_address;
    use super::*;

    fn address() -> serde_json::Value {
        serde_json::json!({
            "name": "Ada Buyer", "line1": "1 Example Street", "line2": "",
            "city": "Lisbon", "region": "", "postalCode": "1000-001", "countryCode": "PT",
        })
    }

    #[test]
    fn a_sealed_address_opens_only_with_its_key_and_bindings() {
        let key = generate();
        assert_eq!(key.key_id.len(), KEY_ID_CHARS);
        let aad = envelope_aad("cmd-1", "buyer", "seller", &key.key_id);
        let wire = seal_address(&key.public_key, &aad, &address());
        assert_eq!(wire.len(), ENVELOPE_BYTES);
        let opened = open_address(key.secret(), &aad, &wire).expect("opens");
        assert_eq!(opened.name, "Ada Buyer");
        assert_eq!(opened.postal_code, "1000-001");
        assert_eq!(opened.country_code, "PT");
        assert_eq!(format!("{opened:?}"), "ShippingAddress(<hidden>)");

        let other = generate();
        assert_eq!(
            open_address(other.secret(), &aad, &wire),
            Err(OpenFailure::Unreadable)
        );
        for rebound in [
            envelope_aad("cmd-2", "buyer", "seller", &key.key_id),
            envelope_aad("cmd-1", "someone", "seller", &key.key_id),
            envelope_aad("cmd-1", "buyer", "other", &key.key_id),
            envelope_aad("cmd-1", "buyer", "seller", &other.key_id),
        ] {
            assert_eq!(
                open_address(key.secret(), &rebound, &wire),
                Err(OpenFailure::Unreadable)
            );
        }
        let mut tampered = wire.clone();
        tampered[100] ^= 1;
        assert_eq!(
            open_address(key.secret(), &aad, &tampered),
            Err(OpenFailure::Unreadable)
        );
        assert_eq!(
            open_address(key.secret(), &aad, &wire[..100]),
            Err(OpenFailure::Unreadable)
        );
        assert!(format!("{key:?}").contains("<redacted>"));
    }
}
