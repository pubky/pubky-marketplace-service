//! Seller lifecycle gate for the commands that open a new commitment on a
//! listing.
//!
//! The seller pauses, ends or removes a listing by rewriting its record on
//! their homeserver (`state` in
//! `/pub/pubky.app/marketplace/v1/listings/{listing_id}`). The listing
//! aggregate tracks stock only, so before the service accepts
//! `checkout.create`, `offer.create`, `offer.counter`, `offer.accept` or
//! `auction.place_bid` it reads each listing's current record and refuses
//! with `LISTING_NOT_ACTIVE` unless the record says `active`. The `reason` is
//! `listing_paused`, `listing_ended` or `listing_removed` for those states
//! (a record the homeserver no longer has is `listing_removed`) and
//! `listing_not_active` for any other value.
//!
//! The record is the seller's own authority over whether the listing is on
//! sale, so the gate fails closed: a homeserver that cannot answer refuses
//! with the retriable `UPSTREAM_UNAVAILABLE` rather than selling something
//! the seller may have withdrawn. Nothing is written by a refused command,
//! and the buyer can retry once the homeserver recovers.
//!
//! A record without a `state` field predates the field and is on sale.
//!
//! `offer.checkout`, the payment-method bind and `payment.prepare_locks`
//! continue a commitment made while the listing was on sale, so they are not
//! gated: a seller who pauses a listing to stop new buyers does not strand
//! the buyer already checking out.

use std::sync::Arc;

use marketplace_domain::commands::CommandPayload;
use marketplace_domain::ErrorCode;
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;

pub const LISTING_NOT_ACTIVE_MESSAGE: &str = "The seller is not selling this listing right now.";
pub const REASON_PAUSED: &str = "listing_paused";
pub const REASON_ENDED: &str = "listing_ended";
pub const REASON_REMOVED: &str = "listing_removed";
pub const REASON_NOT_ACTIVE: &str = "listing_not_active";

const UNAVAILABLE_MESSAGE: &str =
    "The seller's homeserver could not be reached. Try again shortly.";

/// A checkout carries at most 50 lines; this bounds how many record reads
/// one command has in flight against the homeserver at once.
const MAX_CONCURRENT_RECORD_READS: usize = 8;

/// What the seller's current record says about selling the listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingLifecycle {
    Active,
    /// Not on sale; carries the stable `error.reason`.
    Inactive(&'static str),
    /// The homeserver gave no usable answer.
    Unavailable,
}

/// Reads the `state` of a fetched listing record.
pub fn lifecycle_of_record(record: &Value) -> ListingLifecycle {
    let Some(record) = record.as_object() else {
        return ListingLifecycle::Inactive(REASON_NOT_ACTIVE);
    };
    match record.get("state") {
        None | Some(Value::Null) => ListingLifecycle::Active,
        Some(Value::String(state)) => match state.as_str() {
            "active" => ListingLifecycle::Active,
            "paused" => ListingLifecycle::Inactive(REASON_PAUSED),
            "ended" => ListingLifecycle::Inactive(REASON_ENDED),
            "removed" => ListingLifecycle::Inactive(REASON_REMOVED),
            _ => ListingLifecycle::Inactive(REASON_NOT_ACTIVE),
        },
        Some(_) => ListingLifecycle::Inactive(REASON_NOT_ACTIVE),
    }
}

pub fn lifecycle_of_fetch(outcome: &HomeserverFetchOutcome) -> ListingLifecycle {
    match outcome {
        HomeserverFetchOutcome::Found(record) => lifecycle_of_record(record),
        HomeserverFetchOutcome::NotFound => ListingLifecycle::Inactive(REASON_REMOVED),
        HomeserverFetchOutcome::Unavailable => ListingLifecycle::Unavailable,
    }
}

/// The commands that open a new commitment on a listing.
pub fn opens_a_commitment(payload: &CommandPayload) -> bool {
    matches!(
        payload,
        CommandPayload::CreateCheckout(_)
            | CommandPayload::CreateOffer(_)
            | CommandPayload::CounterOffer(_)
            | CommandPayload::AcceptOffer(_)
            | CommandPayload::PlaceBid(_)
    )
}

pub fn not_active(reason: &'static str) -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::ListingNotActive,
        LISTING_NOT_ACTIVE_MESSAGE,
        reason,
    )
}

fn unavailable() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::UpstreamUnavailable,
        ErrorCode::UpstreamUnavailable,
        UNAVAILABLE_MESSAGE,
    )
}

/// Reads every listing's record and returns the refusal, if any. A listing
/// that is not on sale wins over an unreachable one (it is definitive), and
/// among those the first listing in `listings` order decides the reason.
pub async fn check(
    homeserver: Arc<dyn HomeserverListingClient>,
    listings: &[(String, String)],
) -> Option<CommandFailure> {
    let lifecycles = read_lifecycles(homeserver, listings).await;
    if let Some(reason) = lifecycles.iter().find_map(|lifecycle| match lifecycle {
        ListingLifecycle::Inactive(reason) => Some(*reason),
        _ => None,
    }) {
        return Some(not_active(reason));
    }
    if lifecycles.contains(&ListingLifecycle::Unavailable) {
        return Some(unavailable());
    }
    None
}

async fn read_lifecycles(
    homeserver: Arc<dyn HomeserverListingClient>,
    listings: &[(String, String)],
) -> Vec<ListingLifecycle> {
    if let [(seller_pubky, listing_id)] = listings {
        return vec![lifecycle_of_fetch(
            &homeserver.fetch_listing(seller_pubky, listing_id).await,
        )];
    }
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_RECORD_READS));
    let mut reads = JoinSet::new();
    for (index, (seller_pubky, listing_id)) in listings.iter().cloned().enumerate() {
        let homeserver = homeserver.clone();
        let permits = permits.clone();
        reads.spawn(async move {
            let _permit = permits.acquire_owned().await;
            let outcome = homeserver.fetch_listing(&seller_pubky, &listing_id).await;
            (index, lifecycle_of_fetch(&outcome))
        });
    }
    let mut lifecycles = vec![ListingLifecycle::Unavailable; listings.len()];
    while let Some(read) = reads.join_next().await {
        match read {
            Ok((index, lifecycle)) => lifecycles[index] = lifecycle,
            Err(_) => tracing::error!("listing lifecycle record read task failed"),
        }
    }
    lifecycles
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;

    #[test]
    fn only_an_active_or_stateless_record_is_on_sale() {
        assert_eq!(
            lifecycle_of_record(&json!({"state": "active"})),
            ListingLifecycle::Active
        );
        assert_eq!(lifecycle_of_record(&json!({})), ListingLifecycle::Active);
        assert_eq!(
            lifecycle_of_record(&json!({"state": null})),
            ListingLifecycle::Active
        );
        for (state, reason) in [
            (json!("paused"), REASON_PAUSED),
            (json!("ended"), REASON_ENDED),
            (json!("removed"), REASON_REMOVED),
            (json!("draft"), REASON_NOT_ACTIVE),
            (json!("Active"), REASON_NOT_ACTIVE),
            (json!(true), REASON_NOT_ACTIVE),
            (json!({"value": "active"}), REASON_NOT_ACTIVE),
        ] {
            assert_eq!(
                lifecycle_of_record(&json!({ "state": state })),
                ListingLifecycle::Inactive(reason),
                "{state}"
            );
        }
        assert_eq!(
            lifecycle_of_record(&json!(["active"])),
            ListingLifecycle::Inactive(REASON_NOT_ACTIVE)
        );
    }

    #[test]
    fn a_missing_record_is_removed_and_an_outage_is_unavailable() {
        assert_eq!(
            lifecycle_of_fetch(&HomeserverFetchOutcome::NotFound),
            ListingLifecycle::Inactive(REASON_REMOVED)
        );
        assert_eq!(
            lifecycle_of_fetch(&HomeserverFetchOutcome::Unavailable),
            ListingLifecycle::Unavailable
        );
    }

    struct Records(HashMap<String, HomeserverFetchOutcome>);

    impl HomeserverListingClient for Records {
        fn fetch_listing<'a>(
            &'a self,
            _seller_pubky: &'a str,
            listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            Box::pin(async move {
                self.0
                    .get(listing_id)
                    .cloned()
                    .unwrap_or(HomeserverFetchOutcome::NotFound)
            })
        }

        fn fetch_drop<'a>(
            &'a self,
            _seller_pubky: &'a str,
            _drop_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            Box::pin(async { HomeserverFetchOutcome::NotFound })
        }
    }

    fn records(entries: &[(&str, HomeserverFetchOutcome)]) -> Arc<dyn HomeserverListingClient> {
        Arc::new(Records(
            entries
                .iter()
                .map(|(id, outcome)| (id.to_string(), outcome.clone()))
                .collect(),
        ))
    }

    fn listings(ids: &[&str]) -> Vec<(String, String)> {
        ids.iter()
            .map(|id| ("seller".to_string(), id.to_string()))
            .collect()
    }

    fn found(state: &str) -> HomeserverFetchOutcome {
        HomeserverFetchOutcome::Found(json!({ "state": state }))
    }

    fn reason(failure: &CommandFailure) -> Value {
        failure.body()["error"]["reason"].clone()
    }

    #[tokio::test]
    async fn active_listings_pass() {
        let homeserver = records(&[("a", found("active")), ("b", found("active"))]);
        assert!(check(homeserver.clone(), &listings(&["a"])).await.is_none());
        assert!(check(homeserver, &listings(&["a", "b"])).await.is_none());
    }

    #[tokio::test]
    async fn the_first_inactive_listing_decides_the_reason_over_an_outage() {
        let homeserver = records(&[
            ("a", found("active")),
            ("b", HomeserverFetchOutcome::Unavailable),
            ("c", found("ended")),
            ("d", found("paused")),
        ]);
        let failure = check(homeserver, &listings(&["a", "b", "c", "d"]))
            .await
            .expect("an inactive listing refuses");
        assert_eq!(failure.code(), ErrorCode::ListingNotActive);
        assert_eq!(failure.http_status(), 409);
        assert_eq!(reason(&failure), json!(REASON_ENDED));
    }

    #[tokio::test]
    async fn an_unreachable_record_fails_closed() {
        let homeserver = records(&[
            ("a", found("active")),
            ("b", HomeserverFetchOutcome::Unavailable),
        ]);
        let failure = check(homeserver, &listings(&["a", "b"]))
            .await
            .expect("an outage refuses");
        assert_eq!(failure.code(), ErrorCode::UpstreamUnavailable);
        assert_eq!(failure.http_status(), 503);
    }

    #[tokio::test]
    async fn every_line_of_a_large_cart_is_read() {
        let mut entries: Vec<(String, HomeserverFetchOutcome)> = (0..50)
            .map(|index| (format!("l{index}"), found("active")))
            .collect();
        entries[49].1 = found("paused");
        let homeserver: Arc<dyn HomeserverListingClient> =
            Arc::new(Records(entries.iter().cloned().collect()));
        let ids: Vec<String> = entries.iter().map(|(id, _)| id.clone()).collect();
        let cart: Vec<(String, String)> = ids
            .iter()
            .map(|id| ("seller".to_string(), id.clone()))
            .collect();
        let failure = check(homeserver, &cart)
            .await
            .expect("the last line refuses");
        assert_eq!(reason(&failure), json!(REASON_PAUSED));
    }
}
