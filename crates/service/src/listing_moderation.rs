//! Moderation gate for the commands that sell a listing.
//!
//! The marketplace index (Nexus) applies pubky.app's moderation: a listing
//! the moderator tagged, or whose seller the moderator tagged, is out of the
//! index. The index reports that state at
//! `GET {NEXUS_URL}/v0/listing/{seller}/{listing_id}/moderation`. Before the
//! service accepts a checkout, a bid or an offer on a listing it asks, and
//! refuses with `LISTING_MODERATED` when the answer is moderated.
//!
//! The index is read, never trusted for anything else: it is not the
//! transaction authority (ADR 0019), so an unreachable or unusable index is
//! not a reason to stop selling. The gate fails open on an outage and says so
//! at ERROR level with an `ALERT` prefix, with one exception that keeps a
//! moderated listing from becoming sellable by an outage: a listing the gate
//! has seen moderated stays refused until the index answers that it is clear.
//!
//! Only "moderated" answers are cached, never "clear" ones, so lifting a
//! moderation takes effect on the next command after the cached answer
//! expires and a moderation placed meanwhile is never hidden by a cache.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use marketplace_domain::commands::{Command, CommandPayload};
use serde::Deserialize;
use sqlx::PgPool;
use url::Url;

/// The index base URL. Required: the service refuses to boot without it, so
/// a deployment never sells without a moderation check by omission.
pub const ENV_NEXUS_URL: &str = "NEXUS_URL";

const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a "moderated" answer is served without asking the index again.
pub const DEFAULT_MODERATED_TTL: Duration = Duration::from_secs(300);

/// Upper bound on remembered moderated listings. A moderated listing past it
/// is still refused: the index is asked every time.
const MODERATED_CACHE_CAPACITY: usize = 10_000;

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

/// The `(seller_pubky, listing_id)` of every listing a command would sell,
/// bid on or offer on; empty for any other command and for a listing that is
/// not registered (the handler refuses those itself).
///
/// The commands are `checkout.create`, `offer.checkout`, `offer.create`,
/// `offer.counter`, `offer.accept` and `auction.place_bid`. Withdrawing or
/// rejecting an offer and closing an auction stay open: they wind a
/// negotiation down and sell nothing.
pub async fn listings_a_command_sells(
    pool: &PgPool,
    command: &Command,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    match &command.payload {
        CommandPayload::CreateCheckout(payload) => {
            let aggregate_ids: Vec<&str> = payload
                .lines
                .iter()
                .map(|line| line.listing_aggregate_id.as_str())
                .collect();
            sqlx::query_as(
                "SELECT seller_pubky, listing_id FROM listings \
                 WHERE aggregate_id = ANY($1) ORDER BY aggregate_id",
            )
            .bind(&aggregate_ids)
            .fetch_all(pool)
            .await
        }
        CommandPayload::CreateOffer(_) | CommandPayload::PlaceBid(_) => {
            sqlx::query_as("SELECT seller_pubky, listing_id FROM listings WHERE aggregate_id = $1")
                .bind(&command.aggregate_id)
                .fetch_all(pool)
                .await
        }
        CommandPayload::CounterOffer(_)
        | CommandPayload::AcceptOffer(_)
        | CommandPayload::OfferCheckout(_) => {
            sqlx::query_as(
                "SELECT l.seller_pubky, l.listing_id FROM offers o \
                 JOIN listings l ON l.aggregate_id = o.listing_aggregate_id \
                 WHERE o.aggregate_id = $1",
            )
            .bind(&command.aggregate_id)
            .fetch_all(pool)
            .await
        }
        _ => Ok(Vec::new()),
    }
}

/// The decision for one listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModerationVerdict {
    /// The listing, or its seller, is moderated: refuse.
    Moderated,
    /// Not moderated, or the index could not say and the gate failed open.
    Allowed,
}

pub struct ListingModerationGate {
    client: Arc<dyn ListingModerationClient>,
    moderated_ttl: Duration,
    /// `(seller_pubky, listing_id)` to the instant the index last said
    /// moderated. An entry past its TTL is not served as an answer, but it
    /// still refuses while the index is unavailable.
    moderated: Mutex<HashMap<(String, String), Instant>>,
}

impl ListingModerationGate {
    pub fn new(client: Arc<dyn ListingModerationClient>) -> Self {
        Self::with_moderated_ttl(client, DEFAULT_MODERATED_TTL)
    }

    pub fn with_moderated_ttl(
        client: Arc<dyn ListingModerationClient>,
        moderated_ttl: Duration,
    ) -> Self {
        Self {
            client,
            moderated_ttl,
            moderated: Mutex::new(HashMap::new()),
        }
    }

    pub async fn check(&self, seller_pubky: &str, listing_id: &str) -> ModerationVerdict {
        let key = (seller_pubky.to_string(), listing_id.to_string());
        let remembered = self.remembered(&key);
        if remembered.is_some_and(|at| at.elapsed() < self.moderated_ttl) {
            return ModerationVerdict::Moderated;
        }

        match self.client.lookup(seller_pubky, listing_id).await {
            ModerationLookup::Moderated => {
                self.remember(key);
                ModerationVerdict::Moderated
            }
            ModerationLookup::Clear => {
                self.forget(&key);
                ModerationVerdict::Allowed
            }
            ModerationLookup::Unavailable if remembered.is_some() => {
                tracing::error!(
                    seller_prefix = %seller_pubky.get(..8).unwrap_or(seller_pubky),
                    "ALERT the index could not confirm a moderated listing is still moderated; \
                     refusing it on the last moderated answer"
                );
                ModerationVerdict::Moderated
            }
            ModerationLookup::Unavailable => {
                tracing::error!(
                    seller_prefix = %seller_pubky.get(..8).unwrap_or(seller_pubky),
                    "ALERT the index moderation lookup is unavailable; allowing the command \
                     without a moderation check"
                );
                ModerationVerdict::Allowed
            }
        }
    }

    fn remembered(&self, key: &(String, String)) -> Option<Instant> {
        self.moderated
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
            .copied()
    }

    fn remember(&self, key: (String, String)) {
        let mut moderated = self
            .moderated
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if moderated.len() >= MODERATED_CACHE_CAPACITY && !moderated.contains_key(&key) {
            let oldest = moderated
                .iter()
                .min_by_key(|(_, at)| **at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                moderated.remove(&oldest);
            }
        }
        moderated.insert(key, Instant::now());
    }

    fn forget(&self, key: &(String, String)) {
        self.moderated
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
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
    async fn a_clear_answer_is_never_cached() {
        let client = Scripted::new(vec![ModerationLookup::Clear]);
        let gate = ListingModerationGate::new(client.clone());
        for _ in 0..3 {
            assert_eq!(
                gate.check("seller", "boots").await,
                ModerationVerdict::Allowed
            );
        }
        assert_eq!(client.calls(), 3);
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
}
