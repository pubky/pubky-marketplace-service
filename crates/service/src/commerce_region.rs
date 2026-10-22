//! Commerce in a blocked region (`REGION_MODES`, [`crate::region_policy`]).
//!
//! A region the policy marks `blocked` has no new commerce on Pubky Market.
//! This module is the authority for that: the Shop and every other client
//! can be bypassed, the service cannot. It refuses, before the command's
//! transaction and with the same replay rule as the fence and the digital
//! hold (a stored result is answered first):
//!
//! - **Buying:** `checkout.create` (which carries drop claims),
//!   `offer.create`, `offer.checkout` and `auction.place_bid` need the
//!   buyer's `buyer_country`, and are refused with
//!   `COMMERCE_UNAVAILABLE_IN_REGION` (409) when the declared country is
//!   blocked (`buyer_region_blocked`), when the request's own country from
//!   the trusted edge header is blocked (`buyer_ip_blocked`), or when the
//!   edge sent none and unknown origins are blocked (`buyer_region_unknown`).
//!   A buyer cannot dodge a block by declaring Switzerland from a US IP or by
//!   sending no header.
//! - **Starting a payment:** `payment.prepare_locks` and the HTTP payment
//!   method bind check the buyer's request against the country the buyer
//!   declared at checkout, and the seller's shop country.
//! - **Selling:** `listing.register`, `listing.sync`, `drop.sync` and the
//!   seller's payment setup are refused with `SELLER_UNAVAILABLE_IN_REGION`
//!   when the seller's shop country is blocked (`seller_region_blocked`) or,
//!   for the seller's own interactive request, when its IP is blocked
//!   (`seller_ip_blocked`). Automation (`listings/sync-many`) has no browser
//!   IP: only the shop country applies. A `listing.sync` that takes a
//!   listing down (the record is gone, or is paused, ended or removed) is
//!   never refused.
//! - **Every buyer command on a blocked seller's listing** is refused with
//!   `SELLER_UNAVAILABLE_IN_REGION` (`seller_region_blocked`): see
//!   [`crate::region_fence::gate`], which reads the shop record.
//!
//! Nothing here touches what finishes an order: webhooks and IPN, Paykit
//! callbacks, payment confirmation, refunds, cancellation, returns,
//! fulfillment, delivery, reviews, reads and takedowns are never gated, so
//! nobody is trapped with money in flight.
//!
//! Unknown origins fail closed for commerce commands only; every other
//! request never reads the region. With no blocked region configured nothing
//! here reads the database or refuses.

use marketplace_domain::commands::{Command, CommandPayload};
use marketplace_domain::ErrorCode;
use serde_json::Value;

use crate::digital_region::EdgeContext;
use crate::homeserver::HomeserverFetchOutcome;
use crate::listing_lifecycle::{lifecycle_of_record, ListingLifecycle};
use crate::refusal_audit::RefusalKind;
use crate::region_fence::{
    missing_country, read_sellers, region_refusal, unavailable, REASON_SELLER_REGION_BLOCKED,
    SELLER_BLOCKED_MESSAGE,
};
use crate::region_policy::{Block, IpBlock};
use crate::result::CommandFailure;
use crate::AppState;

pub const COMMERCE_UNAVAILABLE_MESSAGE: &str =
    "Buying and selling aren't available in your region yet.";
pub const COMMERCE_UNKNOWN_MESSAGE: &str =
    "We can't tell where you are, so buying and selling are paused.";
/// The seller's own request, or the seller's own shop country.
pub const SELLING_UNAVAILABLE_MESSAGE: &str = "Selling isn't available in your region yet.";
pub const REASON_BUYER_REGION_BLOCKED: &str = "buyer_region_blocked";
pub const REASON_BUYER_IP_BLOCKED: &str = "buyer_ip_blocked";
pub const REASON_BUYER_REGION_UNKNOWN: &str = "buyer_region_unknown";
pub const REASON_SELLER_IP_BLOCKED: &str = "seller_ip_blocked";

fn unknown_origin() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::CommerceUnavailableInRegion,
        COMMERCE_UNKNOWN_MESSAGE,
        REASON_BUYER_REGION_UNKNOWN,
    )
}

fn buyer_refusal(block: Block) -> CommandFailure {
    let blocked = |reason| {
        CommandFailure::refused_with_reason(
            RefusalKind::InvalidState,
            ErrorCode::CommerceUnavailableInRegion,
            COMMERCE_UNAVAILABLE_MESSAGE,
            reason,
        )
    };
    match block {
        Block::Declared => blocked(REASON_BUYER_REGION_BLOCKED),
        Block::Ip(IpBlock::Country) => blocked(REASON_BUYER_IP_BLOCKED),
        Block::Ip(IpBlock::Unknown) => unknown_origin(),
    }
}

fn seller_ip_refusal(block: IpBlock) -> CommandFailure {
    match block {
        IpBlock::Country => region_refusal(REASON_SELLER_IP_BLOCKED, SELLING_UNAVAILABLE_MESSAGE),
        IpBlock::Unknown => unknown_origin(),
    }
}

/// The commands this module may refuse; every other command skips it.
pub fn is_gated(payload: &CommandPayload) -> bool {
    matches!(
        payload,
        CommandPayload::CreateCheckout(_)
            | CommandPayload::CreateOffer(_)
            | CommandPayload::OfferCheckout(_)
            | CommandPayload::PlaceBid(_)
            | CommandPayload::PrepareLocks(_)
            | CommandPayload::RegisterListing(_)
            | CommandPayload::SyncListing(_)
            | CommandPayload::SyncDrop(_)
    )
}

/// Runs before the command's transaction, for a command [`is_gated`] names
/// and only while a region is blocked.
pub async fn gate(
    state: &AppState,
    actor: &str,
    command: &Command,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    if !state.config.region_policy().commerce_active() {
        return Ok(());
    }
    match &command.payload {
        CommandPayload::CreateCheckout(_)
        | CommandPayload::CreateOffer(_)
        | CommandPayload::OfferCheckout(_)
        | CommandPayload::PlaceBid(_) => {
            let Some(country) = crate::region_fence::buyer_country_of(&command.payload).flatten()
            else {
                return Err(missing_country());
            };
            buyer_check(state, Some(country), edge)
        }
        CommandPayload::PrepareLocks(payload) => {
            let order: Option<OrderFacts> = sqlx::query_as(
                "SELECT o.seller_pubky, o.buyer_country, o.seller_country FROM payments p \
                 JOIN orders o ON o.id = p.order_id \
                 WHERE p.id = $1 AND o.buyer_pubky = $2",
            )
            .bind(payload.payment_id)
            .bind(actor)
            .fetch_optional(&state.pool)
            .await
            .map_err(|error| {
                tracing::error!(%error, "commerce region gate could not read the order");
                unavailable()
            })?;
            match order {
                Some(order) => start_payment_check(state, order, edge).await,
                None => Ok(()),
            }
        }
        CommandPayload::RegisterListing(payload) => {
            seller_check(state, actor, &payload.seller_pubky, None, edge).await
        }
        CommandPayload::SyncListing(payload) => {
            seller_check(
                state,
                actor,
                &payload.seller_pubky,
                Some(&payload.listing_id),
                edge,
            )
            .await
        }
        CommandPayload::SyncDrop(payload) => {
            seller_check(state, actor, &payload.seller_pubky, None, edge).await
        }
        _ => Ok(()),
    }
}

/// `POST /v0/orders/{id}/payment-method`: the first bind of a method on the
/// buyer's own order starts a payment. A re-bind of the method already bound
/// only reads the order and is never gated.
pub async fn gate_bind(
    state: &AppState,
    actor: &str,
    order_id: uuid::Uuid,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    if !state.config.region_policy().commerce_active() {
        return Ok(());
    }
    let order: Option<OrderFacts> = sqlx::query_as(
        "SELECT seller_pubky, buyer_country, seller_country FROM orders \
         WHERE id = $1 AND buyer_pubky = $2 AND payment_method IS NULL",
    )
    .bind(order_id)
    .bind(actor)
    .fetch_optional(&state.pool)
    .await
    .map_err(|error| {
        tracing::error!(%error, "commerce region gate could not read the order");
        unavailable()
    })?;
    match order {
        Some(order) => start_payment_check(state, order, edge).await,
        None => Ok(()),
    }
}

/// `PUT /v0/sellers/me/payment-config`: the seller's own interactive request.
pub async fn gate_payment_setup(
    state: &AppState,
    actor: &str,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    if !state.config.region_policy().commerce_active() {
        return Ok(());
    }
    seller_check(state, actor, actor, None, edge).await
}

fn buyer_check(
    state: &AppState,
    declared: Option<&str>,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    match state
        .config
        .region_policy()
        .modes()
        .buyer_block(declared, edge.client_country.as_deref())
    {
        Some(block) => Err(buyer_refusal(block)),
        None => Ok(()),
    }
}

/// What an order says about the two countries, as stored.
#[derive(sqlx::FromRow)]
struct OrderFacts {
    seller_pubky: String,
    /// The buyer's declared country at checkout; none for an order created
    /// without one.
    buyer_country: Option<String>,
    /// The seller's shop country, frozen at the order's first payment lock
    /// point; none until then.
    seller_country: Option<String>,
}

/// A payment starts from the country the buyer declared at checkout and the
/// request's own country now. The seller's shop country is the frozen one,
/// else the shop record is read now (fail closed), so a seller who moved to a
/// blocked region after checkout takes no payment.
async fn start_payment_check(
    state: &AppState,
    order: OrderFacts,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    buyer_check(state, order.buyer_country.as_deref(), edge)?;
    let seller_country = match order.seller_country {
        Some(country) => Some(country),
        None => shop_country(state, &order.seller_pubky).await?,
    };
    if state
        .config
        .region_policy()
        .modes()
        .shop_country_blocked(seller_country.as_deref())
    {
        return Err(region_refusal(
            REASON_SELLER_REGION_BLOCKED,
            SELLER_BLOCKED_MESSAGE,
        ));
    }
    Ok(())
}

/// What a fetched listing record says about a `listing.sync`: whether it
/// publishes or updates a live listing. `None` when the homeserver gave no
/// answer.
fn publishes_live_listing(outcome: &HomeserverFetchOutcome) -> Option<bool> {
    match outcome {
        HomeserverFetchOutcome::Found(record) => Some(is_live(record)),
        HomeserverFetchOutcome::NotFound => Some(false),
        HomeserverFetchOutcome::Unavailable => None,
    }
}

fn is_live(record: &Value) -> bool {
    lifecycle_of_record(record) == ListingLifecycle::Active
}

/// Selling: the seller's own request IP (interactive requests only) and the
/// seller's shop country. `listing_id` is set for a `listing.sync`, which a
/// takedown is exempt from.
async fn seller_check(
    state: &AppState,
    actor: &str,
    seller: &str,
    listing_id: Option<&str>,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    let modes = state.config.region_policy().modes();
    let own = actor == seller;
    let ip_block = if edge.interactive && own {
        modes.ip_block(edge.client_country.as_deref())
    } else {
        None
    };
    let refusal = if let Some(block) = ip_block {
        Some(seller_ip_refusal(block))
    } else {
        let shop_country = shop_country(state, seller).await?;
        modes
            .shop_country_blocked(shop_country.as_deref())
            .then(|| {
                if own {
                    region_refusal(REASON_SELLER_REGION_BLOCKED, SELLING_UNAVAILABLE_MESSAGE)
                } else {
                    region_refusal(REASON_SELLER_REGION_BLOCKED, SELLER_BLOCKED_MESSAGE)
                }
            })
    };
    let Some(refusal) = refusal else {
        return Ok(());
    };
    let Some(listing_id) = listing_id else {
        return Err(refusal);
    };
    let Some(homeserver) = state.homeserver.clone() else {
        return Err(unavailable());
    };
    let record = homeserver.fetch_listing(seller, listing_id).await;
    match publishes_live_listing(&record) {
        Some(true) => Err(refusal),
        Some(false) => Ok(()),
        None => Err(unavailable()),
    }
}

/// The country of the seller's shop record; `None` when the seller has no
/// shop record or it carries no usable country. Fails closed when the
/// homeserver cannot answer.
async fn shop_country(state: &AppState, seller: &str) -> Result<Option<String>, CommandFailure> {
    let Some(homeserver) = state.homeserver.clone() else {
        return Err(unavailable());
    };
    let sellers = [seller.to_string()];
    let mut facts = read_sellers(homeserver, &sellers, state.clock.now()).await?;
    Ok(facts.remove(seller).and_then(|facts| facts.country))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parts(failure: &CommandFailure) -> (ErrorCode, Option<&'static str>, &str) {
        (failure.code(), failure.reason(), failure.message())
    }

    #[test]
    fn a_buyer_refusal_names_what_blocked_it() {
        assert_eq!(
            parts(&buyer_refusal(Block::Declared)),
            (
                ErrorCode::CommerceUnavailableInRegion,
                Some("buyer_region_blocked"),
                "Buying and selling aren't available in your region yet."
            )
        );
        assert_eq!(
            parts(&buyer_refusal(Block::Ip(IpBlock::Country))),
            (
                ErrorCode::CommerceUnavailableInRegion,
                Some("buyer_ip_blocked"),
                "Buying and selling aren't available in your region yet."
            )
        );
        assert_eq!(
            parts(&buyer_refusal(Block::Ip(IpBlock::Unknown))),
            (
                ErrorCode::CommerceUnavailableInRegion,
                Some("buyer_region_unknown"),
                "We can't tell where you are, so buying and selling are paused."
            )
        );
    }

    #[test]
    fn a_seller_refusal_reuses_the_seller_code() {
        assert_eq!(
            parts(&seller_ip_refusal(IpBlock::Country)),
            (
                ErrorCode::SellerUnavailableInRegion,
                Some("seller_ip_blocked"),
                "Selling isn't available in your region yet."
            )
        );
        assert_eq!(
            parts(&seller_ip_refusal(IpBlock::Unknown)),
            (
                ErrorCode::CommerceUnavailableInRegion,
                Some("buyer_region_unknown"),
                "We can't tell where you are, so buying and selling are paused."
            )
        );
    }

    #[test]
    fn both_refusals_are_409() {
        for failure in [
            buyer_refusal(Block::Declared),
            seller_ip_refusal(IpBlock::Country),
        ] {
            assert_eq!(failure.http_status(), 409);
        }
    }

    #[test]
    fn only_a_live_record_is_published_by_a_sync() {
        let found = |record| publishes_live_listing(&HomeserverFetchOutcome::Found(record));
        assert_eq!(found(json!({ "state": "active" })), Some(true));
        assert_eq!(
            found(json!({})),
            Some(true),
            "a record without a state is on sale"
        );
        for state in ["paused", "ended", "removed", "other"] {
            assert_eq!(found(json!({ "state": state })), Some(false), "{state}");
        }
        assert_eq!(
            publishes_live_listing(&HomeserverFetchOutcome::NotFound),
            Some(false)
        );
        assert_eq!(
            publishes_live_listing(&HomeserverFetchOutcome::Unavailable),
            None
        );
    }
}
