//! Delivery addresses sealed to the seller (encrypted-delivery-address
//! design §D2, §F).
//!
//! The buyer's client seals the address to the seller's published delivery
//! key (RFC 9180 HPKE) and sends the 2,096-byte envelope. The service checks
//! the shape (length, canonical base64url, key id) and which seller each
//! envelope is for, stores the bytes, and returns them to that seller
//! untouched. It holds no key and has no code path that opens an envelope.
//! Refusal messages never echo an envelope.

use std::collections::{BTreeSet, HashMap};

use marketplace_domain::commands::{
    decode_delivery_envelope, validate_delivery_address, validate_delivery_envelope,
    DeliveryAddress, DeliveryEnvelope,
};
use marketplace_domain::ErrorCode;

use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;

pub const REASON_ENCRYPTED_REQUIRED: &str = "encrypted_delivery_required";
pub const REASON_ENVELOPE_SELLERS: &str = "delivery_envelope_sellers";
pub const REASON_ADDRESS_REQUIRED: &str = "delivery_address_required";
pub const REASON_ADDRESS_NOT_ACCEPTED: &str = "delivery_address_not_accepted";
pub const REASON_ADDRESS_INVALID: &str = "invalid_delivery_address";
pub const REASON_ADDRESS_BOTH: &str = "delivery_address_and_envelope";

/// An envelope ready to bind into the `orders` insert.
#[derive(Clone)]
pub(crate) struct StoredEnvelope {
    pub bytes: Vec<u8>,
    pub key_id: String,
}

impl StoredEnvelope {
    pub(crate) fn from_wire(sealed: &DeliveryEnvelope) -> Option<Self> {
        Some(Self {
            bytes: decode_delivery_envelope(&sealed.envelope)?,
            key_id: sealed.key_id.clone(),
        })
    }
}

pub(crate) fn plaintext_refused() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "This deployment accepts only an encrypted delivery address.",
        REASON_ENCRYPTED_REQUIRED,
    )
}

pub(crate) fn sellers_mismatch() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "Send one delivery envelope for each shipped seller and none for any other seller.",
        REASON_ENVELOPE_SELLERS,
    )
}

fn malformed() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "A delivery envelope is malformed.",
    )
}

/// Matches a checkout's envelopes to the shipped seller groups. Returns the
/// stored envelope per shipped seller; an empty map means the checkout
/// carries a legacy plaintext address (or no shipped group).
pub(crate) fn match_checkout_envelopes(
    plaintext_present: bool,
    envelopes: Option<&[marketplace_domain::commands::SellerDeliveryEnvelope]>,
    shipped_sellers: &BTreeSet<&str>,
    require_encrypted: bool,
) -> Result<HashMap<String, StoredEnvelope>, CommandFailure> {
    if require_encrypted && plaintext_present {
        return Err(plaintext_refused());
    }
    let Some(envelopes) = envelopes else {
        return Ok(HashMap::new());
    };
    if plaintext_present {
        return Err(CommandFailure::refused(
            RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "Send the delivery address or its envelopes, not both.",
        ));
    }
    let supplied: BTreeSet<&str> = envelopes
        .iter()
        .map(|sealed| sealed.seller_pubky.as_str())
        .collect();
    if supplied.len() != envelopes.len() || supplied != *shipped_sellers {
        return Err(sellers_mismatch());
    }
    envelopes
        .iter()
        .map(|sealed| {
            StoredEnvelope::from_wire(&sealed.sealed())
                .map(|stored| (sealed.seller_pubky.clone(), stored))
                .ok_or_else(malformed)
        })
        .collect()
}

pub(crate) fn match_offer_envelope(
    plaintext_present: bool,
    envelope: Option<&DeliveryEnvelope>,
    require_encrypted: bool,
) -> Result<Option<StoredEnvelope>, CommandFailure> {
    if require_encrypted && plaintext_present {
        return Err(plaintext_refused());
    }
    envelope
        .map(|sealed| StoredEnvelope::from_wire(sealed).ok_or_else(malformed))
        .transpose()
}

/// A refusal of the delivery address an auction winner sends with the payment
/// method. The messages are static: they never echo an address or envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WinnerDeliveryRefusal {
    pub code: ErrorCode,
    pub reason: &'static str,
    pub message: &'static str,
}

impl WinnerDeliveryRefusal {
    pub(crate) const REQUIRED: Self = Self {
        code: ErrorCode::InvalidState,
        reason: REASON_ADDRESS_REQUIRED,
        message: "Send a delivery address before choosing a payment method for this auction order.",
    };
    pub(crate) const NOT_ACCEPTED: Self = Self {
        code: ErrorCode::InvalidCommand,
        reason: REASON_ADDRESS_NOT_ACCEPTED,
        message:
            "Only an auction winner's order takes its delivery address with the payment method.",
    };
    const PLAINTEXT: Self = Self {
        code: ErrorCode::InvalidCommand,
        reason: REASON_ENCRYPTED_REQUIRED,
        message: "This deployment accepts only an encrypted delivery address.",
    };
    const BOTH: Self = Self {
        code: ErrorCode::InvalidCommand,
        reason: REASON_ADDRESS_BOTH,
        message: "Send the delivery address or its envelope, not both.",
    };
    const INVALID: Self = Self {
        code: ErrorCode::InvalidCommand,
        reason: REASON_ADDRESS_INVALID,
        message: "The delivery address is invalid.",
    };
}

/// The delivery address an auction winner sends with the payment method,
/// validated and ready to store: one of a plaintext address (legacy) or a
/// sealed envelope.
pub(crate) struct WinnerDelivery {
    pub address: Option<serde_json::Value>,
    pub envelope: Option<StoredEnvelope>,
}

/// An auction order is created at close, so its delivery address arrives
/// with the winner's payment-method bind. Same rules as a checkout: one of
/// address or envelope, never both, and a deployment that requires encrypted
/// addresses refuses a plaintext one.
pub(crate) fn match_winner_delivery(
    address: Option<DeliveryAddress>,
    envelope: Option<&DeliveryEnvelope>,
    require_encrypted: bool,
) -> Result<Option<WinnerDelivery>, WinnerDeliveryRefusal> {
    match (address, envelope) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(WinnerDeliveryRefusal::BOTH),
        (Some(_), None) if require_encrypted => Err(WinnerDeliveryRefusal::PLAINTEXT),
        (Some(mut address), None) => {
            let mut issues = Vec::new();
            validate_delivery_address("address", &mut address, &mut issues);
            if !issues.is_empty() {
                return Err(WinnerDeliveryRefusal::INVALID);
            }
            Ok(Some(WinnerDelivery {
                address: Some(serde_json::to_value(address).expect("address serializes")),
                envelope: None,
            }))
        }
        (None, Some(sealed)) => {
            let mut issues = Vec::new();
            validate_delivery_envelope("envelope", &sealed.key_id, &sealed.envelope, &mut issues);
            if !issues.is_empty() {
                return Err(WinnerDeliveryRefusal::INVALID);
            }
            let stored = StoredEnvelope::from_wire(sealed).ok_or(WinnerDeliveryRefusal::INVALID)?;
            Ok(Some(WinnerDelivery {
                address: None,
                envelope: Some(stored),
            }))
        }
    }
}

/// Rows one purge tick clears; a larger backlog drains over the next ticks.
pub const DELIVERY_ADDRESS_PURGE_BATCH: i64 = 500;

/// Clears the delivery address, plaintext or sealed, from orders in a final
/// state (design §F). The deadline is the final state itself: an order that
/// ends has no further use for the address, and the buyer's copy lives on
/// their own device. Revision and `updated_at` stay as they were, so the
/// purge never races a command that CASes the order.
pub async fn purge_final_delivery_addresses<'e>(
    executor: impl sqlx::PgExecutor<'e>,
) -> Result<u64, sqlx::Error> {
    let purged = sqlx::query(
        "UPDATE orders SET delivery_address = NULL, delivery_address_envelope = NULL, \
         delivery_key_id = NULL \
         WHERE id IN ( \
           SELECT id FROM orders \
           WHERE state IN ('completed', 'cancelled', 'refunded_external', 'closed') \
             AND (delivery_address IS NOT NULL OR delivery_address_envelope IS NOT NULL) \
           ORDER BY ended_at NULLS FIRST, id \
           LIMIT $1 \
           FOR UPDATE SKIP LOCKED)",
    )
    .bind(DELIVERY_ADDRESS_PURGE_BATCH)
    .execute(executor)
    .await?
    .rows_affected();
    Ok(purged)
}
