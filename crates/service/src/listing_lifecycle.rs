//! Seller lifecycle gate for the commands that sell a listing.
//!
//! The seller pauses, ends or removes a listing by rewriting its record on
//! their homeserver (`state` in
//! `/pub/pubky.app/marketplace/v1/listings/{listing_id}`). The listing
//! aggregate tracks stock only, so before the service accepts a command that
//! sells a listing it reads each listing's current record. The `reason` of a
//! `LISTING_NOT_ACTIVE` refusal is `listing_paused`, `listing_ended` or
//! `listing_removed` for those states (a record the homeserver no longer has
//! is `listing_removed`) and `listing_not_active` for any other value. A
//! record without a `state` field predates the field and is on sale.
//!
//! [`Gate::OnSale`] guards a new commitment (`checkout.create`,
//! `offer.create`, `offer.counter`, `offer.accept`, `auction.place_bid`):
//! anything but `active` refuses. [`Gate::NotRemoved`] guards a continuation
//! of a commitment made while the listing was on sale (`offer.checkout`,
//! the first `payment.prepare_locks`, `auction.close` and the payment-method
//! bind): only a removal refuses, because a seller who removed the listing
//! cannot fulfil it and no payment may start, while a pause or an end does
//! not strand a buyer already checking out.
//!
//! The record is the seller's own authority, so the gate fails closed: a
//! homeserver that cannot answer, or does not answer within
//! [`CHECK_TIMEOUT`] for the whole command, refuses with the retriable
//! `UPSTREAM_UNAVAILABLE`. Nothing is written by a refused command.

use std::sync::Arc;
use std::time::Duration;

use marketplace_domain::commands::CommandPayload;
use marketplace_domain::ErrorCode;
use serde_json::Value;
use sqlx::PgPool;
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

pub const UNAVAILABLE_MESSAGE: &str =
    "The seller's homeserver could not be reached. Try again shortly.";

/// A checkout carries at most 50 lines; this bounds how many record reads
/// one command has in flight against the homeserver at once.
const MAX_CONCURRENT_RECORD_READS: usize = 8;

/// The longest one command waits for all of its record reads, so a
/// homeserver that accepts connections and never answers cannot hold a
/// checkout for several per-request timeouts in a row.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(12);

/// What the seller's current record says about selling the listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingLifecycle {
    Active,
    /// Not on sale; carries the stable `error.reason`.
    Inactive(&'static str),
    /// The homeserver gave no usable answer.
    Unavailable,
}

/// Which records refuse a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// A new commitment: anything but an `active` record refuses.
    OnSale,
    /// A continuation of an existing commitment: only a removed record
    /// refuses.
    NotRemoved,
}

impl Gate {
    fn refuses(self, reason: &str) -> bool {
        match self {
            Gate::OnSale => true,
            Gate::NotRemoved => reason == REASON_REMOVED,
        }
    }
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

/// The gate a command passes, or `None` for a command that sells nothing.
pub fn gate_for(payload: &CommandPayload) -> Option<Gate> {
    match payload {
        CommandPayload::CreateCheckout(_)
        | CommandPayload::CreateOffer(_)
        | CommandPayload::CounterOffer(_)
        | CommandPayload::AcceptOffer(_)
        | CommandPayload::PlaceBid(_) => Some(Gate::OnSale),
        CommandPayload::OfferCheckout(_)
        | CommandPayload::PrepareLocks(_)
        | CommandPayload::CloseAuction(_) => Some(Gate::NotRemoved),
        _ => None,
    }
}

/// The live auction an `auction.close` command addresses, as
/// `(seller_pubky, listing_id)`; empty for anything else.
pub async fn auction_being_closed(
    pool: &PgPool,
    aggregate_id: &str,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT seller_pubky, listing_id FROM listings \
         WHERE aggregate_id = $1 AND sale_format = 'auction' AND deleted_at IS NULL",
    )
    .bind(aggregate_id)
    .fetch_all(pool)
    .await
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

/// Reads every distinct listing's record and returns the refusal, if any. A
/// refusing record wins over an unreachable one (it is definitive), and
/// among those the first listing in `listings` order decides the reason.
pub async fn check(
    homeserver: Arc<dyn HomeserverListingClient>,
    listings: &[(String, String)],
    gate: Gate,
) -> Option<CommandFailure> {
    let mut distinct: Vec<(String, String)> = Vec::with_capacity(listings.len());
    for listing in listings {
        if !distinct.contains(listing) {
            distinct.push(listing.clone());
        }
    }
    if distinct.is_empty() {
        return None;
    }
    let Ok(lifecycles) =
        tokio::time::timeout(CHECK_TIMEOUT, read_lifecycles(homeserver, &distinct)).await
    else {
        tracing::warn!(
            listings = distinct.len(),
            "listing lifecycle record reads timed out"
        );
        return Some(unavailable());
    };
    if let Some(reason) = lifecycles.iter().find_map(|lifecycle| match lifecycle {
        ListingLifecycle::Inactive(reason) if gate.refuses(reason) => Some(*reason),
        _ => None,
    }) {
        return Some(not_active(reason));
    }
    if lifecycles.contains(&ListingLifecycle::Unavailable) {
        return Some(unavailable());
    }
    None
}

/// Whether one listing's record says the seller removed it, within
/// [`CHECK_TIMEOUT`]; `None` when the homeserver gave no usable answer.
pub async fn is_removed(
    homeserver: &dyn HomeserverListingClient,
    seller_pubky: &str,
    listing_id: &str,
) -> Option<bool> {
    let outcome = tokio::time::timeout(
        CHECK_TIMEOUT,
        homeserver.fetch_listing(seller_pubky, listing_id),
    )
    .await
    .ok()?;
    match lifecycle_of_fetch(&outcome) {
        ListingLifecycle::Unavailable => None,
        lifecycle => Some(lifecycle == ListingLifecycle::Inactive(REASON_REMOVED)),
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Answers listing ids from the map, counts reads, and never answers
    /// the ids in `hung`.
    #[derive(Default)]
    struct Records {
        records: HashMap<String, HomeserverFetchOutcome>,
        hung: Vec<String>,
        reads: AtomicUsize,
    }

    impl Records {
        fn new(records: HashMap<String, HomeserverFetchOutcome>) -> Self {
            Self {
                records,
                ..Self::default()
            }
        }
    }

    impl HomeserverListingClient for Records {
        fn fetch_listing<'a>(
            &'a self,
            _seller_pubky: &'a str,
            listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            Box::pin(async move {
                self.reads.fetch_add(1, Ordering::SeqCst);
                if self.hung.iter().any(|id| id == listing_id) {
                    std::future::pending::<()>().await;
                }
                self.records
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
        Arc::new(Records::new(
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
        assert!(check(homeserver.clone(), &listings(&["a"]), Gate::OnSale)
            .await
            .is_none());
        assert!(check(homeserver, &listings(&["a", "b"]), Gate::OnSale)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn the_first_inactive_listing_decides_the_reason_over_an_outage() {
        let homeserver = records(&[
            ("a", found("active")),
            ("b", HomeserverFetchOutcome::Unavailable),
            ("c", found("ended")),
            ("d", found("paused")),
        ]);
        let failure = check(homeserver, &listings(&["a", "b", "c", "d"]), Gate::OnSale)
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
        let failure = check(homeserver, &listings(&["a", "b"]), Gate::OnSale)
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
            Arc::new(Records::new(entries.iter().cloned().collect()));
        let ids: Vec<String> = entries.iter().map(|(id, _)| id.clone()).collect();
        let cart: Vec<(String, String)> = ids
            .iter()
            .map(|id| ("seller".to_string(), id.clone()))
            .collect();
        let failure = check(homeserver, &cart, Gate::OnSale)
            .await
            .expect("the last line refuses");
        assert_eq!(reason(&failure), json!(REASON_PAUSED));
    }

    #[tokio::test]
    async fn a_continuation_refuses_only_a_removal() {
        let homeserver = records(&[
            ("paused", found("paused")),
            ("ended", found("ended")),
            ("draft", found("draft")),
            ("removed", found("removed")),
        ]);
        assert!(check(
            homeserver.clone(),
            &listings(&["paused", "ended", "draft"]),
            Gate::NotRemoved
        )
        .await
        .is_none());
        for ids in [&["paused", "removed"][..], &["missing"][..]] {
            let failure = check(homeserver.clone(), &listings(ids), Gate::NotRemoved)
                .await
                .expect("a removal refuses");
            assert_eq!(failure.code(), ErrorCode::ListingNotActive);
            assert_eq!(reason(&failure), json!(REASON_REMOVED));
        }
        let outage = records(&[("a", HomeserverFetchOutcome::Unavailable)]);
        let failure = check(outage, &listings(&["a"]), Gate::NotRemoved)
            .await
            .expect("an outage refuses a continuation too");
        assert_eq!(failure.code(), ErrorCode::UpstreamUnavailable);
    }

    #[tokio::test]
    async fn each_listing_is_read_once() {
        let homeserver = Arc::new(Records::new(
            [("a", found("active")), ("b", found("active"))]
                .into_iter()
                .map(|(id, outcome)| (id.to_string(), outcome))
                .collect(),
        ));
        let cart = listings(&["a", "b", "a", "a", "b"]);
        assert!(check(homeserver.clone(), &cart, Gate::OnSale)
            .await
            .is_none());
        assert_eq!(homeserver.reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_homeserver_is_cut_off_by_the_overall_timeout() {
        let homeserver = Arc::new(Records {
            records: [("a".to_string(), found("active"))].into_iter().collect(),
            hung: vec!["b".to_string()],
            ..Records::default()
        });
        let started = tokio::time::Instant::now();
        let failure = check(homeserver, &listings(&["a", "b"]), Gate::OnSale)
            .await
            .expect("a hung read refuses");
        assert_eq!(failure.code(), ErrorCode::UpstreamUnavailable);
        assert_eq!(failure.http_status(), 503);
        assert_eq!(started.elapsed(), CHECK_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_removal_read_that_hangs_is_unknown() {
        let homeserver = Records {
            hung: vec!["a".to_string()],
            ..Records::default()
        };
        assert_eq!(is_removed(&homeserver, "seller", "a").await, None);
        let homeserver = records(&[("a", found("removed")), ("b", found("paused"))]);
        assert_eq!(
            is_removed(homeserver.as_ref(), "seller", "a").await,
            Some(true)
        );
        assert_eq!(
            is_removed(homeserver.as_ref(), "seller", "b").await,
            Some(false)
        );
        assert_eq!(
            is_removed(homeserver.as_ref(), "seller", "gone").await,
            Some(true)
        );
    }
}
