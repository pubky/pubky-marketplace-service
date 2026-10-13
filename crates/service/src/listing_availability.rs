//! Public listing availability (`GET /v0/listings/{seller}/{listing_id}/availability`).
//!
//! The listing projection is a session read, so a viewer whose purchase
//! approval has lapsed cannot see that another buyer holds the item. This
//! route answers that one question without a bearer: the listing's stock
//! state and the server time, nothing else. The only protection it needs is a
//! per-client token bucket, held in memory with salted-hash keys.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;

use axum::http::{HeaderMap, HeaderName};
use chrono::{DateTime, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::address_search::{
    env_u32, resolve_client_address, Bucket, CLIENT_TABLE_CAPACITY, ENV_CLIENT_IP_HEADER,
};

pub const ENV_CLIENT_IP_HEADER_OVERRIDE: &str = "LISTING_AVAILABILITY_CLIENT_IP_HEADER";
pub const ENV_CLIENT_PER_MINUTE: &str = "LISTING_AVAILABILITY_CLIENT_PER_MINUTE";
pub const DEFAULT_CLIENT_PER_MINUTE: u32 = 120;
pub const CLIENT_BURST: f64 = 30.0;
/// How long a shared cache may reuse an answer. Short, because a hold starts
/// and lapses within minutes and the Shop polls on its own cadence.
pub const CACHE_CONTROL_VALUE: &str = "public, max-age=5";

#[derive(Debug)]
pub struct AvailabilityLimiter {
    client_ip_header: Option<HeaderName>,
    salt: [u8; 16],
    per_second: f64,
    clients: Mutex<HashMap<[u8; 16], Bucket>>,
}

impl AvailabilityLimiter {
    pub fn new(client_ip_header: Option<HeaderName>, client_per_minute: u32) -> Self {
        let mut salt = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        Self {
            client_ip_header,
            salt,
            per_second: f64::from(client_per_minute.max(1)) / 60.0,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Reads `LISTING_AVAILABILITY_CLIENT_PER_MINUTE` and the trusted client
    /// address header. The header falls back to the address search proxy's
    /// `ADDRESS_SEARCH_CLIENT_IP_HEADER`, so a deployment behind one proxy
    /// configures it once.
    pub fn from_env() -> anyhow::Result<Self> {
        let header_name = [ENV_CLIENT_IP_HEADER_OVERRIDE, ENV_CLIENT_IP_HEADER]
            .into_iter()
            .find_map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| (name, value))
            });
        let header = match header_name {
            Some((name, value)) => Some(
                HeaderName::from_bytes(value.trim().as_bytes())
                    .map_err(|_| anyhow::anyhow!("{name} is not a header name"))?,
            ),
            None => None,
        };
        let per_minute = env_u32(ENV_CLIENT_PER_MINUTE, DEFAULT_CLIENT_PER_MINUTE)?;
        Ok(Self::new(header, per_minute))
    }

    fn client_key(&self, ip: &str) -> [u8; 16] {
        let mut hasher = Sha256::new();
        hasher.update(self.salt);
        hasher.update(ip.as_bytes());
        let digest = hasher.finalize();
        let mut key = [0u8; 16];
        key.copy_from_slice(&digest[..16]);
        key
    }

    /// Takes one token for this client, or returns the whole seconds until
    /// one is available.
    pub fn admit(
        &self,
        headers: &HeaderMap,
        peer: Option<IpAddr>,
        now: DateTime<Utc>,
    ) -> Result<(), u64> {
        let ip = resolve_client_address(self.client_ip_header.as_ref(), headers, peer);
        let key = self.client_key(&ip);
        let per_second = self.per_second;
        let mut clients = self.clients.lock().expect("availability limiter lock");
        if clients.len() >= CLIENT_TABLE_CAPACITY && !clients.contains_key(&key) {
            clients.retain(|_, bucket| {
                bucket.refill(now, per_second, CLIENT_BURST);
                bucket.tokens() < CLIENT_BURST
            });
            if clients.len() >= CLIENT_TABLE_CAPACITY {
                return Err(60);
            }
        }
        clients
            .entry(key)
            .or_insert_with(|| Bucket::full(CLIENT_BURST, now))
            .take(now, per_second, CLIENT_BURST)
    }
}

impl Default for AvailabilityLimiter {
    fn default() -> Self {
        Self::new(None, DEFAULT_CLIENT_PER_MINUTE)
    }
}
