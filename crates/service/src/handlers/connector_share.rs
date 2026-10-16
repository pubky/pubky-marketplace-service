//! A buyer's opt-in to share the delivery address with the seller's
//! connected store (LG5).
//!
//! Checkout carries, next to the delivery envelope sealed to the seller, a
//! second envelope sealed to the recipient key of the seller's store
//! connection, and the consent the buyer gave (store domain, consent copy
//! version). The service checks that the seller really has that store
//! connected with that key, then keeps the envelope and the consent on the
//! order. It opens the envelope only in the connector worker, for the one
//! push, and deletes it once the store order exists
//! (`connectors::ledger::mark_created`), or when the order reaches a final
//! state (`delivery_envelope::purge_final_delivery_addresses`). Refusal
//! messages never echo an envelope.

use std::collections::{BTreeSet, HashMap};

use marketplace_domain::commands::{decode_delivery_envelope, SellerConnectorShare};
use marketplace_domain::ErrorCode;
use sqlx::{Postgres, Transaction};

use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;

pub const REASON_SHARE_SELLERS: &str = "connector_share_sellers";
pub const REASON_SHARE_UNAVAILABLE: &str = "connector_share_unavailable";

/// An opt-in ready to bind onto the `orders` row.
#[derive(Clone)]
pub(crate) struct StoredShare {
    pub shop_domain: String,
    pub consent_version: String,
    pub key_id: String,
    pub envelope: Vec<u8>,
}

fn refused(message: &'static str, reason: &'static str) -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        message,
        reason,
    )
}

/// Matches a checkout's opt-ins to its shipped sellers and to live store
/// connections. Returns the stored share per opted-in seller.
pub(crate) async fn match_checkout_shares(
    tx: &mut Transaction<'_, Postgres>,
    shares: Option<&[SellerConnectorShare]>,
    shipped_sellers: &BTreeSet<&str>,
) -> Result<Result<HashMap<String, StoredShare>, CommandFailure>, sqlx::Error> {
    let Some(shares) = shares else {
        return Ok(Ok(HashMap::new()));
    };
    let mut matched = HashMap::with_capacity(shares.len());
    for share in shares {
        if !shipped_sellers.contains(share.seller_pubky.as_str()) {
            return Ok(Err(refused(
                "Share an address only with a seller whose order ships.",
                REASON_SHARE_SELLERS,
            )));
        }
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS ( \
                 SELECT 1 FROM connector_connections c \
                 JOIN connector_recipient_keys k ON k.connection_id = c.id \
                 WHERE c.seller_pubky = $1 AND c.shop = $2 AND c.status = 'connected' \
                   AND k.key_id = $3)",
        )
        .bind(&share.seller_pubky)
        .bind(&share.shop_domain)
        .bind(&share.key_id)
        .fetch_one(&mut **tx)
        .await?;
        let envelope = decode_delivery_envelope(&share.envelope);
        let (true, Some(envelope)) = (live, envelope) else {
            return Ok(Err(refused(
                "This seller's store no longer accepts shared addresses. Check out without sharing.",
                REASON_SHARE_UNAVAILABLE,
            )));
        };
        matched.insert(
            share.seller_pubky.clone(),
            StoredShare {
                shop_domain: share.shop_domain.clone(),
                consent_version: share.consent_version.clone(),
                key_id: share.key_id.clone(),
                envelope,
            },
        );
    }
    Ok(Ok(matched))
}
