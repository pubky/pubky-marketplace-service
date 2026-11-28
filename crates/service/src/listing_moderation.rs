//! Moderation gate for the commands and requests that sell a listing.
//!
//! The marketplace index (Nexus) applies pubky.app's moderation: a listing
//! the moderator tagged, or whose seller the moderator tagged, is out of the
//! index. The index reports that state at
//! `GET {NEXUS_URL}/v0/listing/{seller}/{listing_id}/moderation`. Before the
//! service accepts a checkout, a bid or an offer on a listing it asks, and
//! refuses with `LISTING_MODERATED` when the answer is moderated. An order
//! created before the listing was moderated is gated again where its payment
//! starts: the payment-method bind and the Locks payment preparation.
//!
//! The index is read, never trusted for anything else: it is not the
//! transaction authority (ADR 0019), so an unreachable or unusable index is
//! not a reason to stop selling. The gate fails open on an outage and says so
//! at ERROR level with an `ALERT` prefix, with one exception that keeps a
//! moderated listing from becoming sellable by an outage: a listing the gate
//! has seen moderated stays refused until the index answers that it is clear.
//!
//! Only `Moderated` answers survive outages; `Clear` answers are cached in a
//! short bounded in-memory LRU (`DEFAULT_CLEAR_TTL`, 15 s) so bursts of orders
//! on the same listing do not hammer Nexus serially on every call. `Clear`
//! entries are bypassed and invalidated immediately whenever Nexus reports
//! `Moderated` or `Unavailable`, and `Unavailable` is never cached so an
//! outage still fails open per call while known moderated listings stay
//! refused.

use std::collections::HashSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use marketplace_domain::commands::{Command, CommandPayload};
use serde::Deserialize;
use sqlx::PgPool;
use url::Url;
use uuid::Uuid;

/// The index base URL. Required: the service refuses to boot without it, so
/// a deployment never sells without a moderation check by omission.
pub const ENV_NEXUS_URL: &str = "NEXUS_URL";

const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a "moderated" answer is served without asking the index again.
pub const DEFAULT_MODERATED_TTL: Duration = Duration::from_secs(300);

/// How long a "clear" answer is served without asking the index again.
pub const DEFAULT_CLEAR_TTL: Duration = Duration::from_secs(15);

/// Upper bound on remembered moderated listings. A moderated listing past it
/// is still refused: the index is asked every time.
const MODERATED_CACHE_CAPACITY: usize = 10_000;

/// Upper bound on cached clear listings.
const CLEAR_CACHE_CAPACITY: usize = 10_000;

/// Maximum concurrent moderation lookups for one multi-item checkout or bind.
const MAX_CONCURRENT_LOOKUPS: usize = 8;

/// What the index said about one listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModerationLookup {
    Clear,
    Moderated,
    /// No usable answer: transport failure, timeout, a non-success status or
    /// a body without the `moderated` field.
    Unavailable,
}

/// The index read. The trait exists so integration tests can stand in a
/// local double; production only constructs [`HttpListingModerationClient`].
pub trait ListingModerationClient: Send + Sync + 'static {
    fn lookup<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>>;
}

#[derive(Debug, Deserialize)]
struct ModerationBody {
    moderated: Option<bool>,
}

pub struct HttpListingModerationClient {
    base_url: Url,
    http: reqwest::Client,
}

impl HttpListingModerationClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let mut base_url = Url::parse(base_url.trim())
            .map_err(|_| anyhow::anyhow!("{ENV_NEXUS_URL} must be an http(s) URL"))?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.host_str().is_none() {
            anyhow::bail!("{ENV_NEXUS_URL} must be an http(s) URL");
        }
        base_url.set_query(None);
        base_url.set_fragment(None);
        tracing::info!(
            host = base_url.host_str().unwrap_or_default(),
            "index client configured"
        );
        let http = reqwest::Client::builder()
            .timeout(LOOKUP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { base_url, http })
    }

    fn moderation_url(&self, seller_pubky: &str, listing_id: &str) -> Option<Url> {
        let mut url = self.base_url.clone();
        url.path_segments_mut().ok()?.pop_if_empty().extend([
            "v0",
            "listing",
            seller_pubky,
            listing_id,
            "moderation",
        ]);
        Some(url)
    }
}

impl ListingModerationClient for HttpListingModerationClient {
    fn lookup<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
        Box::pin(async move {
            let Some(url) = self.moderation_url(seller_pubky, listing_id) else {
                return ModerationLookup::Unavailable;
            };
            let response = match self.http.get(url).send().await {
                Ok(response) => response,
                Err(_) => {
                    tracing::warn!("index moderation lookup transport failure");
                    return ModerationLookup::Unavailable;
                }
            };
            let status = response.status();
            if !status.is_success() {
                tracing::warn!(status = %status, "index moderation lookup rejected");
                return ModerationLookup::Unavailable;
            }
            match response.json::<ModerationBody>().await {
                Ok(ModerationBody {
                    moderated: Some(true),
                }) => ModerationLookup::Moderated,
                Ok(ModerationBody {
                    moderated: Some(false),
                }) => ModerationLookup::Clear,
                _ => {
                    tracing::warn!("index moderation lookup returned an unusable body");
                    ModerationLookup::Unavailable
                }
            }
        })
    }
}

/// Builds the production client from the environment.
pub fn client_from_env() -> anyhow::Result<HttpListingModerationClient> {
    let url =
        std::env::var(ENV_NEXUS_URL).map_err(|_| anyhow::anyhow!("{ENV_NEXUS_URL} must be set"))?;
    HttpListingModerationClient::new(&url)
}

/// The message of the refusal.
pub const LISTING_MODERATED_MESSAGE: &str =
    "This listing was removed by moderation and cannot be bought, bid on or offered on.";

/// One listing a command would sell, bid on or offer on.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct SoldListing {
    pub seller_pubky: String,
    pub listing_id: String,
    /// False for a tombstoned listing, whose handler answers `NOT_FOUND`.
    pub live: bool,
    /// Seller's configured shipping profile id on this listing row, reused by
    /// zone-priced shipping pre-tx reads to avoid a duplicate `listings` query.
    pub shipping_profile_id: Option<String>,
}

/// Every listing a command would sell, bid on or offer on; empty for any
/// other command and for a listing that is not registered (the handler
/// refuses those itself).
///
/// The commands are `checkout.create`, `offer.checkout`, `offer.create`,
/// `offer.counter`, `offer.accept`, `auction.place_bid`, and
/// `payment.prepare_locks`, which starts a Locks payment on an existing
/// order. Only the first preparation is gated: once the payment is pinned to
/// Locks, `payment.register_locks` and any later preparation stay open so
/// money already in flight is never stranded. Withdrawing or rejecting an
/// offer and closing an auction stay open: they wind a negotiation down and
/// sell nothing.
pub async fn listings_a_command_sells(
    pool: &PgPool,
    actor: &str,
    command: &Command,
) -> Result<Vec<SoldListing>, sqlx::Error> {
    match &command.payload {
        CommandPayload::CreateCheckout(payload) => {
            let aggregate_ids: Vec<&str> = payload
                .lines
                .iter()
                .map(|line| line.listing_aggregate_id.as_str())
                .collect();
            sqlx::query_as(
                "SELECT seller_pubky, listing_id, deleted_at IS NULL AS live, \
                        shipping_profile_id \
                 FROM listings \
                 WHERE aggregate_id = ANY($1) ORDER BY aggregate_id",
            )
            .bind(&aggregate_ids)
            .fetch_all(pool)
            .await
        }
        CommandPayload::CreateOffer(_) | CommandPayload::PlaceBid(_) => {
            sqlx::query_as(
                "SELECT seller_pubky, listing_id, deleted_at IS NULL AS live, \
                        shipping_profile_id \
                 FROM listings \
                 WHERE aggregate_id = $1",
            )
            .bind(&command.aggregate_id)
            .fetch_all(pool)
            .await
        }
        CommandPayload::CounterOffer(_)
        | CommandPayload::AcceptOffer(_)
        | CommandPayload::OfferCheckout(_) => {
            sqlx::query_as(
                "SELECT l.seller_pubky, l.listing_id, l.deleted_at IS NULL AS live, \
                        l.shipping_profile_id \
                 FROM offers o \
                 JOIN listings l ON l.aggregate_id = o.listing_aggregate_id \
                 WHERE o.aggregate_id = $1",
            )
            .bind(&command.aggregate_id)
            .fetch_all(pool)
            .await
        }
        CommandPayload::PrepareLocks(payload) => {
            sqlx::query_as(
                "SELECT DISTINCT l.seller_pubky, l.listing_id, l.deleted_at IS NULL AS live, \
                        l.shipping_profile_id \
                 FROM payments p \
                 JOIN orders o ON o.id = p.order_id \
                 CROSS JOIN LATERAL jsonb_array_elements(o.lines) AS line \
                 JOIN listings l ON l.aggregate_id = line ->> 'listing_aggregate_id' \
                 WHERE p.id = $1 AND p.buyer_pubky = $2 AND o.state = 'pending_payment' \
                   AND p.state = 'awaiting_entitlement' AND p.adapter <> 'locks' \
                 ORDER BY l.seller_pubky, l.listing_id",
            )
            .bind(payload.payment_id)
            .bind(actor)
            .fetch_all(pool)
            .await
        }
        _ => Ok(Vec::new()),
    }
}

/// Every listing on an order that is about to have a payment method bound,
/// for the buyer's own order only.
/// Empty when the order is not the buyer's, is no longer awaiting a payment
/// method, or already has one (a re-bind of the bound method sells nothing
/// new), so the existing bind answers those cases itself.
pub async fn listings_a_bind_sells(
    pool: &PgPool,
    order_id: Uuid,
    buyer_pubky: &str,
) -> Result<Vec<SoldListing>, sqlx::Error> {
    sqlx::query_as(
        "SELECT DISTINCT l.seller_pubky, l.listing_id, l.deleted_at IS NULL AS live, \
                l.shipping_profile_id \
         FROM orders o \
         CROSS JOIN LATERAL jsonb_array_elements(o.lines) AS line \
         JOIN listings l ON l.aggregate_id = line ->> 'listing_aggregate_id' \
         WHERE o.id = $1 AND o.buyer_pubky = $2 \
           AND o.state = 'pending_payment' AND o.payment_method IS NULL \
         ORDER BY l.seller_pubky, l.listing_id",
    )
    .bind(order_id)
    .bind(buyer_pubky)
    .fetch_all(pool)
    .await
}

/// The decision for one listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModerationVerdict {
    /// The listing, or its seller, is moderated: refuse.
    Moderated,
    /// Not moderated, or the index could not say and the gate failed open.
    Allowed,
}

#[derive(Debug, Clone, Copy)]
struct ModeratedEntry {
    at: Instant,
    /// Monotonic sequence stamped when this `Moderated` verdict was recorded.
    /// Any in-flight `Clear` lookup that started at an earlier sequence cannot
    /// erase this entry or populate the `Clear` cache.
    recorded_seq: u64,
}

struct ModerationCache {
    /// `(seller_pubky, listing_id)` to the instant and sequence the index last
    /// said moderated. An entry past its TTL is not served as an answer, but
    /// it still refuses while the index is unavailable.
    moderated: lru::LruCache<(String, String), ModeratedEntry>,
    /// Highest `recorded_seq` of any `Moderated` entry evicted from `moderated`
    /// due to LRU capacity. A delayed `Clear` lookup whose `started_seq` is
    /// older than this watermark must not populate the 15 s `Clear` cache.
    evicted_moderated_max_seq: u64,
    /// Short bounded LRU of recently observed `Clear` listings, bypassed and
    /// invalidated on any `Moderated` or `Unavailable` response.
    clear: lru::LruCache<(String, String), Instant>,
}

pub struct ListingModerationGate {
    client: Arc<dyn ListingModerationClient>,
    moderated_ttl: Duration,
    clear_ttl: Duration,
    seq: AtomicU64,
    cache: Mutex<ModerationCache>,
}

impl ListingModerationGate {
    pub fn new(client: Arc<dyn ListingModerationClient>) -> Self {
        Self::with_ttls(client, DEFAULT_MODERATED_TTL, DEFAULT_CLEAR_TTL)
    }

    pub fn with_moderated_ttl(
        client: Arc<dyn ListingModerationClient>,
        moderated_ttl: Duration,
    ) -> Self {
        Self::with_ttls(client, moderated_ttl, Duration::ZERO)
    }

    pub fn with_ttls(
        client: Arc<dyn ListingModerationClient>,
        moderated_ttl: Duration,
        clear_ttl: Duration,
    ) -> Self {
        Self::with_capacities(
            client,
            moderated_ttl,
            clear_ttl,
            MODERATED_CACHE_CAPACITY,
            CLEAR_CACHE_CAPACITY,
        )
    }

    fn with_capacities(
        client: Arc<dyn ListingModerationClient>,
        moderated_ttl: Duration,
        clear_ttl: Duration,
        moderated_capacity: usize,
        clear_capacity: usize,
    ) -> Self {
        Self {
            client,
            moderated_ttl,
            clear_ttl,
            seq: AtomicU64::new(1),
            cache: Mutex::new(ModerationCache {
                moderated: lru::LruCache::new(
                    NonZeroUsize::new(moderated_capacity).expect("non-zero capacity"),
                ),
                evicted_moderated_max_seq: 0,
                clear: lru::LruCache::new(
                    NonZeroUsize::new(clear_capacity).expect("non-zero capacity"),
                ),
            }),
        }
    }

    /// Checks a batch of listings concurrently (bounded and deduplicated),
    /// returning `true` if any listing is moderated.
    pub async fn any_moderated(self: &Arc<Self>, listings: &[SoldListing]) -> bool {
        if listings.is_empty() {
            return false;
        }
        if listings.len() == 1 {
            return self
                .check(&listings[0].seller_pubky, &listings[0].listing_id)
                .await
                == ModerationVerdict::Moderated;
        }
        let mut seen = HashSet::with_capacity(listings.len());
        let unique: Vec<(String, String)> = listings
            .iter()
            .filter_map(|listing| {
                let pair = (listing.seller_pubky.clone(), listing.listing_id.clone());
                seen.insert(pair.clone()).then_some(pair)
            })
            .collect();
        let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_LOOKUPS));
        let mut tasks = tokio::task::JoinSet::new();
        for (seller, listing_id) in unique {
            let gate = Arc::clone(self);
            let permits = Arc::clone(&permits);
            tasks.spawn(async move {
                let _permit = permits.acquire_owned().await.ok();
                gate.check(&seller, &listing_id).await
            });
        }
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(ModerationVerdict::Moderated) => {
                    tasks.abort_all();
                    return true;
                }
                Ok(ModerationVerdict::Allowed) => {}
                Err(error) => {
                    tracing::error!(
                        %error,
                        "ALERT listing moderation lookup task failed; failing open for this task"
                    );
                }
            }
        }
        false
    }

    pub async fn check(&self, seller_pubky: &str, listing_id: &str) -> ModerationVerdict {
        let key = (seller_pubky.to_string(), listing_id.to_string());
        let remembered = {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let remembered = cache.moderated.get(&key).copied();
            if remembered.is_some_and(|entry| entry.at.elapsed() < self.moderated_ttl) {
                return ModerationVerdict::Moderated;
            }
            if !self.clear_ttl.is_zero()
                && remembered.is_none()
                && cache
                    .clear
                    .get(&key)
                    .is_some_and(|at| at.elapsed() < self.clear_ttl)
            {
                return ModerationVerdict::Allowed;
            }
            remembered
        };

        let started_seq = self.seq.fetch_add(1, Ordering::AcqRel);
        match self.client.lookup(seller_pubky, listing_id).await {
            ModerationLookup::Moderated => {
                let mut cache = self
                    .cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let recorded_seq = self.seq.fetch_add(1, Ordering::AcqRel);
                cache.clear.pop(&key);
                if let Some((evicted_key, evicted_entry)) = cache.moderated.push(
                    key.clone(),
                    ModeratedEntry {
                        at: Instant::now(),
                        recorded_seq,
                    },
                ) {
                    if evicted_key != key {
                        cache.evicted_moderated_max_seq = cache
                            .evicted_moderated_max_seq
                            .max(evicted_entry.recorded_seq);
                    }
                }
                ModerationVerdict::Moderated
            }
            ModerationLookup::Clear => {
                let mut cache = self
                    .cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                // Fence out delayed Clear completions that started before a
                // Moderated verdict was recorded for this listing so an
                // out-of-order response can never erase Moderated or poison
                // the Clear cache.
                if cache
                    .moderated
                    .peek(&key)
                    .is_some_and(|entry| started_seq < entry.recorded_seq)
                {
                    cache.clear.pop(&key);
                    return ModerationVerdict::Moderated;
                }
                cache.moderated.pop(&key);
                // If an older Moderated entry was evicted from the bounded LRU
                // after this lookup started, skip caching Clear so a stalled
                // response cannot hide a newer moderation for 15 s.
                if !self.clear_ttl.is_zero() && started_seq >= cache.evicted_moderated_max_seq {
                    cache.clear.put(key, Instant::now());
                }
                ModerationVerdict::Allowed
            }
            ModerationLookup::Unavailable => {
                let mut cache = self
                    .cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                cache.clear.pop(&key);
                let still_remembered = remembered.is_some() || cache.moderated.peek(&key).is_some();
                drop(cache);
                if still_remembered {
                    tracing::error!(
                        seller_prefix = %seller_pubky.get(..8).unwrap_or(seller_pubky),
                        "ALERT the index could not confirm a moderated listing is still moderated; \
                         refusing it on the last moderated answer"
                    );
                    ModerationVerdict::Moderated
                } else {
                    tracing::error!(
                        seller_prefix = %seller_pubky.get(..8).unwrap_or(seller_pubky),
                        "ALERT the index moderation lookup is unavailable; allowing the command \
                         without a moderation check"
                    );
                    ModerationVerdict::Allowed
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scripted {
        answers: Mutex<Vec<ModerationLookup>>,
        calls: AtomicUsize,
    }

    impl Scripted {
        fn new(answers: Vec<ModerationLookup>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(answers),
                calls: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ListingModerationClient for Scripted {
        fn lookup<'a>(
            &'a self,
            _seller_pubky: &'a str,
            _listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let mut answers = self.answers.lock().unwrap();
                if answers.len() > 1 {
                    answers.remove(0)
                } else {
                    answers[0]
                }
            })
        }
    }

    #[tokio::test]
    async fn a_clear_answer_is_cached_for_clear_ttl_and_invalidated_on_moderation() {
        let client = Scripted::new(vec![ModerationLookup::Clear, ModerationLookup::Moderated]);
        let gate = ListingModerationGate::with_ttls(
            client.clone(),
            DEFAULT_MODERATED_TTL,
            Duration::from_millis(80),
        );
        for _ in 0..3 {
            assert_eq!(
                gate.check("seller", "boots").await,
                ModerationVerdict::Allowed
            );
        }
        assert_eq!(client.calls(), 1, "clear burst hits Nexus once inside TTL");

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(client.calls(), 2, "re-checks after clear TTL expires");
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(client.calls(), 2, "moderated verdict stays cached");
    }

    #[tokio::test]
    async fn an_unavailable_answer_is_never_cached_as_clear() {
        let client = Scripted::new(vec![
            ModerationLookup::Unavailable,
            ModerationLookup::Moderated,
        ]);
        let gate = ListingModerationGate::new(client.clone());
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Allowed
        );
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(
            client.calls(),
            2,
            "unavailable fails open once without caching clear"
        );
    }

    #[tokio::test]
    async fn a_moderated_answer_is_served_from_the_cache_until_it_expires() {
        let client = Scripted::new(vec![ModerationLookup::Moderated]);
        let gate =
            ListingModerationGate::with_moderated_ttl(client.clone(), Duration::from_millis(80));
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(client.calls(), 1);

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(client.calls(), 2);
    }

    #[tokio::test]
    async fn lifting_a_moderation_takes_effect_after_the_cached_answer_expires() {
        let client = Scripted::new(vec![ModerationLookup::Moderated, ModerationLookup::Clear]);
        let gate =
            ListingModerationGate::with_moderated_ttl(client.clone(), Duration::from_millis(60));
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Allowed
        );
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Allowed
        );
        assert_eq!(client.calls(), 3);
    }

    #[tokio::test]
    async fn an_outage_keeps_a_known_moderated_listing_refused_even_after_the_ttl() {
        let client = Scripted::new(vec![
            ModerationLookup::Moderated,
            ModerationLookup::Unavailable,
        ]);
        let gate =
            ListingModerationGate::with_moderated_ttl(client.clone(), Duration::from_millis(40));
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
    }

    #[tokio::test]
    async fn an_outage_allows_a_listing_nothing_is_known_about() {
        let client = Scripted::new(vec![ModerationLookup::Unavailable]);
        let gate = ListingModerationGate::new(client.clone());
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Allowed
        );
        assert_eq!(client.calls(), 1);
    }

    #[tokio::test]
    async fn the_cache_is_per_listing() {
        let client = Scripted::new(vec![ModerationLookup::Moderated, ModerationLookup::Clear]);
        let gate = ListingModerationGate::new(client.clone());
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(
            gate.check("seller", "hat").await,
            ModerationVerdict::Allowed
        );
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(client.calls(), 2);
    }

    #[test]
    fn the_url_must_be_http_or_https() {
        assert!(HttpListingModerationClient::new("https://nexus.example").is_ok());
        assert!(HttpListingModerationClient::new("http://127.0.0.1:8080/").is_ok());
        assert!(HttpListingModerationClient::new("nexus.example").is_err());
        assert!(HttpListingModerationClient::new("ftp://nexus.example").is_err());
        assert!(HttpListingModerationClient::new("").is_err());
    }

    #[test]
    fn the_lookup_path_is_built_from_escaped_segments() {
        let client = HttpListingModerationClient::new("https://nexus.example/base/").unwrap();
        let url = client.moderation_url("seller", "a/b c").unwrap();
        assert_eq!(
            url.as_str(),
            "https://nexus.example/base/v0/listing/seller/a%2Fb%20c/moderation"
        );
    }

    #[tokio::test]
    async fn delayed_clear_cannot_overwrite_newer_moderated_verdict() {
        struct DelayedClearClient {
            calls: AtomicUsize,
            clear_entered: Arc<tokio::sync::Notify>,
            release_clear: Arc<tokio::sync::Notify>,
        }

        impl ListingModerationClient for DelayedClearClient {
            fn lookup<'a>(
                &'a self,
                _seller_pubky: &'a str,
                _listing_id: &'a str,
            ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
                Box::pin(async move {
                    let call = self.calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        self.clear_entered.notify_one();
                        self.release_clear.notified().await;
                        ModerationLookup::Clear
                    } else {
                        ModerationLookup::Moderated
                    }
                })
            }
        }

        let clear_entered = Arc::new(tokio::sync::Notify::new());
        let release_clear = Arc::new(tokio::sync::Notify::new());
        let client = Arc::new(DelayedClearClient {
            calls: AtomicUsize::new(0),
            clear_entered: Arc::clone(&clear_entered),
            release_clear: Arc::clone(&release_clear),
        });
        let gate = Arc::new(ListingModerationGate::with_ttls(
            client.clone(),
            Duration::from_secs(60),
            Duration::from_secs(15),
        ));

        // Lookup A starts first and stalls before returning Clear.
        let gate_a = Arc::clone(&gate);
        let lookup_a = tokio::spawn(async move { gate_a.check("seller", "boots").await });
        clear_entered.notified().await;

        // Lookup B observes Moderated while A is stalled.
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );

        // Release A's delayed Clear response: it must NOT erase Moderated or
        // populate the 15s Clear cache.
        release_clear.notify_one();
        assert_eq!(
            lookup_a.await.expect("lookup A finishes"),
            ModerationVerdict::Moderated
        );
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated,
            "subsequent check still observes cached Moderated verdict"
        );
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            2,
            "cached Moderated answered the follow-up without hitting Nexus"
        );
    }

    #[tokio::test]
    async fn any_moderated_logs_alert_on_panicked_task_and_enforces_concurrency_cap() {
        struct ConcurrencyClient {
            in_flight: AtomicUsize,
            max_in_flight: AtomicUsize,
            panic_on: Option<&'static str>,
            moderated_on: Option<&'static str>,
        }

        impl ListingModerationClient for ConcurrencyClient {
            fn lookup<'a>(
                &'a self,
                _seller_pubky: &'a str,
                listing_id: &'a str,
            ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
                let listing_id = listing_id.to_owned();
                Box::pin(async move {
                    if self.panic_on == Some(listing_id.as_str()) {
                        panic!("simulated lookup task panic");
                    }
                    let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.max_in_flight.fetch_max(active, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    self.in_flight.fetch_sub(1, Ordering::SeqCst);
                    if self.moderated_on == Some(listing_id.as_str()) {
                        ModerationLookup::Moderated
                    } else {
                        ModerationLookup::Clear
                    }
                })
            }
        }

        // 1. Concurrency cap (MAX_CONCURRENT_LOOKUPS = 8) across 20 listings.
        let bounded = Arc::new(ConcurrencyClient {
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            panic_on: None,
            moderated_on: None,
        });
        let gate = Arc::new(ListingModerationGate::with_moderated_ttl(
            bounded.clone(),
            Duration::ZERO,
        ));
        let listings: Vec<SoldListing> = (0..20)
            .map(|index| SoldListing {
                seller_pubky: "seller".to_string(),
                listing_id: format!("item_{index}"),
                live: true,
                shipping_profile_id: None,
            })
            .collect();
        assert!(!gate.any_moderated(&listings).await);
        let observed_max = bounded.max_in_flight.load(Ordering::SeqCst);
        assert!(
            observed_max > 1 && observed_max <= MAX_CONCURRENT_LOOKUPS,
            "expected concurrency in 2..={MAX_CONCURRENT_LOOKUPS}, observed {observed_max}"
        );

        // 2. Panicked lookup task logs ALERT and fails open while still
        // allowing a peer Moderated lookup in the same batch to refuse.
        #[derive(Clone, Default)]
        struct LogBuf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for LogBuf {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
            type Writer = LogBuf;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let logs = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let panicking = Arc::new(ConcurrencyClient {
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            panic_on: Some("boom"),
            moderated_on: None,
        });
        let panic_gate = Arc::new(ListingModerationGate::with_moderated_ttl(
            panicking,
            Duration::ZERO,
        ));
        let batch = vec![
            SoldListing {
                seller_pubky: "seller".to_string(),
                listing_id: "boom".to_string(),
                live: true,
                shipping_profile_id: None,
            },
            SoldListing {
                seller_pubky: "seller".to_string(),
                listing_id: "ok".to_string(),
                live: true,
                shipping_profile_id: None,
            },
        ];
        assert!(!panic_gate.any_moderated(&batch).await);
        let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            captured.contains("ALERT listing moderation lookup task failed"),
            "expected ALERT log on JoinError, got:\n{captured}"
        );
    }

    #[tokio::test]
    async fn delayed_clear_after_moderated_eviction_does_not_populate_clear_cache() {
        struct EvictionRaceClient {
            calls: AtomicUsize,
            clear_entered: Arc<tokio::sync::Notify>,
            release_clear: Arc<tokio::sync::Notify>,
        }

        impl ListingModerationClient for EvictionRaceClient {
            fn lookup<'a>(
                &'a self,
                _seller_pubky: &'a str,
                listing_id: &'a str,
            ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
                let listing_id = listing_id.to_owned();
                Box::pin(async move {
                    let call = self.calls.fetch_add(1, Ordering::SeqCst);
                    if listing_id == "boots" && call == 0 {
                        self.clear_entered.notify_one();
                        self.release_clear.notified().await;
                        ModerationLookup::Clear
                    } else {
                        ModerationLookup::Moderated
                    }
                })
            }
        }

        let clear_entered = Arc::new(tokio::sync::Notify::new());
        let release_clear = Arc::new(tokio::sync::Notify::new());
        let client = Arc::new(EvictionRaceClient {
            calls: AtomicUsize::new(0),
            clear_entered: Arc::clone(&clear_entered),
            release_clear: Arc::clone(&release_clear),
        });
        // Capacity 2 for the moderated LRU so two newer moderated listings
        // evict the earlier moderated entry for "boots".
        let gate = Arc::new(ListingModerationGate::with_capacities(
            client.clone(),
            Duration::from_secs(60),
            Duration::from_secs(15),
            2,
            16,
        ));

        // 1. Lookup A for "boots" starts first (earliest started_seq) and stalls.
        let gate_a = Arc::clone(&gate);
        let lookup_a = tokio::spawn(async move { gate_a.check("seller", "boots").await });
        clear_entered.notified().await;

        // 2. While A is stalled, "boots" is observed Moderated, and then two
        //    other moderated listings ("hat", "coat") evict "boots" from the
        //    2-entry moderated LRU, advancing evicted_moderated_max_seq past
        //    A's started_seq.
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(
            gate.check("seller", "hat").await,
            ModerationVerdict::Moderated
        );
        assert_eq!(
            gate.check("seller", "coat").await,
            ModerationVerdict::Moderated
        );

        // 3. Release A's delayed Clear completion. Even though "boots" was
        //    evicted from cache.moderated, started_seq < evicted_moderated_max_seq
        //    must prevent A from populating the 15 s Clear cache.
        release_clear.notify_one();
        let _ = lookup_a.await.expect("lookup A finishes");

        // 4. The next check for "boots" must hit Nexus (not serve a stale 15 s
        //    cached Clear) and immediately observe Moderated.
        assert_eq!(
            gate.check("seller", "boots").await,
            ModerationVerdict::Moderated,
            "evicted moderation must not be hidden behind a stale Clear cache entry"
        );
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            5,
            "follow-up check bypassed clear cache and re-queried Nexus"
        );
    }
}
