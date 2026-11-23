//! The `HOMESERVER_RESOLUTION=pkarr` transport.
//!
//! A pubky's Pkarr record names its homeserver, and the homeserver's own
//! record names where to connect to it. Both are published by whoever holds
//! the keys, so any user can point the service's reads at an address of
//! their choosing. This transport therefore:
//!
//! - connects only to globally routable addresses ([`EndpointPolicy`]). The
//!   check runs in the transport's DNS resolvers, on the addresses the
//!   connection is then made to, so a name that resolves differently between
//!   a check and the connect (DNS rebinding) still cannot reach an internal
//!   address. An ICANN endpoint named by an IP literal, which skips the
//!   resolver, is checked before any request is built;
//! - follows no redirect, speaks only HTTPS and uses no proxy;
//! - looks a pubky's homeserver up with the newest record the network holds
//!   whenever its cache holds no fresh one;
//! - bounds each read, from the lookup to the end of the body, by one budget.
//!
//! It replaces the `pubky` SDK's request path (`PubkyHttpClient::request_async`),
//! whose clients follow redirects, allow plain HTTP and connect to whatever
//! addresses a record names, with no setting to change that. Only the SDK's
//! Pkarr client is used. A redirect, scheme and endpoint-address option in the
//! SDK would let this module go.
//!
//! Memory is bounded: Pkarr records sit in one LRU cache
//! ([`super::PkarrResolutionCache`]) and homeserver storage addressing in
//! another ([`ADDRESSING_CACHE_CAPACITY`]). The SDK's per-homeserver transport
//! cache, which never evicts, is not used.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_lite::StreamExt;
use pubky::pkarr::dns::rdata::RData;
use pubky::pkarr::errors::ResolveError;
use pubky::pkarr::extra::endpoints::Endpoint;
use pubky::pkarr::{self, Cache, CacheKey, InMemoryCache, ResolvePolicy, SignedPacket};
use pubky_common::constants::features::PATH_ADDRESSED_STORAGE;
use pubky_reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::time::Instant;

use super::{pubky_host_prefix, HomeserverRefresh, PkarrResolutionCache, Target, Unreached};

/// Bound on a network-only Pkarr lookup, which asks every DHT node near the
/// key for its most recent record instead of stopping at the first answer.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound on one TCP connect, so a homeserver's unreachable direct address
/// leaves the read's budget for its ICANN endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Bound on a homeserver's `/info` read, within the read's budget.
const INFO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INFO_BYTES: usize = 16 * 1024;
/// How long a homeserver's storage addressing is reused before `/info` is
/// read again.
const ADDRESSING_TTL: Duration = Duration::from_secs(60);
/// Homeservers whose storage addressing is kept; the least recently used is
/// evicted first.
const ADDRESSING_CACHE_CAPACITY: usize = 256;

/// Which addresses a read may connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndpointPolicy {
    /// Globally routable addresses only. The only policy a built service has.
    PublicOnly,
    /// Also loopback, where a local test network's homeservers listen.
    #[cfg(any(test, feature = "test-faults"))]
    PublicOrLoopback,
}

impl EndpointPolicy {
    fn allows(self, ip: IpAddr) -> bool {
        match self {
            Self::PublicOnly => is_public(ip),
            #[cfg(any(test, feature = "test-faults"))]
            Self::PublicOrLoopback => ip.is_loopback() || is_public(ip),
        }
    }
}

/// Whether `ip` is a globally routable unicast address. Refused: loopback,
/// private, shared (CGNAT), link-local, unique-local and site-local,
/// multicast, unspecified, broadcast, reserved, documentation and
/// benchmarking ranges, the IETF special-purpose blocks, and IPv6 forms that
/// carry such an IPv4 address (IPv4-mapped, NAT64, 6to4) or any IPv4 address
/// at all (IPv4-compatible, IPv4-translated, Teredo).
pub(super) fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    let refused = a == 0 // "this network"
        || a == 10 // private
        || a == 127 // loopback
        || (a == 100 && (b & 0xc0) == 64) // shared address space, 100.64.0.0/10
        || (a == 169 && b == 254) // link-local
        || (a == 172 && (b & 0xf0) == 16) // private, 172.16.0.0/12
        || (a == 192 && b == 0 && c == 0) // IETF protocol assignments
        || (a == 192 && b == 0 && c == 2) // documentation
        || (a == 192 && b == 88 && c == 99) // 6to4 relay anycast
        || (a == 192 && b == 168) // private
        || (a == 198 && (b & 0xfe) == 18) // benchmarking, 198.18.0.0/15
        || (a == 198 && b == 51 && c == 100) // documentation
        || (a == 203 && b == 0 && c == 113) // documentation
        || a >= 224; // multicast, reserved, broadcast
    !refused
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    let embedded = |high: u16, low: u16| Ipv4Addr::from((u32::from(high) << 16) | u32::from(low));
    // IPv4-mapped, ::ffff:0:0/96.
    if s[..5] == [0; 5] && s[5] == 0xffff {
        return is_public_v4(embedded(s[6], s[7]));
    }
    // NAT64 well-known prefix, 64:ff9b::/96.
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return is_public_v4(embedded(s[6], s[7]));
    }
    // 6to4, 2002::/16.
    if s[0] == 0x2002 {
        return is_public_v4(embedded(s[1], s[2]));
    }
    // Global unicast is 2000::/3; everything else (unspecified, loopback,
    // IPv4-compatible and -translated, unique-local, link-local, site-local,
    // multicast, discard-only, local-use NAT64) is not.
    let global_unicast = (s[0] & 0xe000) == 0x2000;
    // 2001::/23 is IETF special-purpose (Teredo among them) and 2001:db8::/32
    // and 3fff::/20 are documentation.
    let special =
        (s[0] == 0x2001 && (s[1] < 0x0200 || s[1] == 0x0db8)) || (s[0] == 0x3fff && s[1] < 0x1000);
    global_unicast && !special
}

/// No address a read may connect to.
#[derive(Debug)]
struct NoPublicAddress;

impl std::fmt::Display for NoPublicAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no address the endpoint policy allows")
    }
}

impl std::error::Error for NoPublicAddress {}

fn allowed_addrs(addrs: Vec<SocketAddr>, policy: EndpointPolicy) -> Resolving {
    let addrs: Vec<SocketAddr> = addrs
        .into_iter()
        .filter(|addr| policy.allows(addr.ip()))
        .collect();
    Box::pin(async move {
        if addrs.is_empty() {
            return Err(Box::new(NoPublicAddress) as Box<dyn std::error::Error + Send + Sync>);
        }
        Ok(Box::new(addrs.into_iter()) as Addrs)
    })
}

/// Resolves a homeserver key to the addresses its own record lists
/// (endpoints whose target is `.`), keeping those the policy allows.
struct DirectResolver {
    pkarr: pkarr::Client,
    policy: EndpointPolicy,
}

impl Resolve for DirectResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let pkarr = self.pkarr.clone();
        let policy = self.policy;
        let name = name.as_str().to_owned();
        Box::pin(async move {
            if pkarr::PublicKey::try_from(name.as_str()).is_err() {
                return Err(Box::new(NoPublicAddress) as Box<dyn std::error::Error + Send + Sync>);
            }
            let addrs = direct_addrs(&pkarr, &name).await;
            allowed_addrs(addrs, policy).await
        })
    }
}

async fn direct_addrs(pkarr: &pkarr::Client, homeserver: &str) -> Vec<SocketAddr> {
    pkarr
        .resolve_https_endpoints(homeserver)
        .collect::<Vec<Endpoint>>()
        .await
        .iter()
        .filter(|endpoint| endpoint.target() == ".")
        .flat_map(Endpoint::to_socket_addrs)
        .collect()
}

/// Resolves an ICANN domain through the system resolver, keeping the
/// addresses the policy allows.
struct IcannResolver {
    policy: EndpointPolicy,
}

impl Resolve for IcannResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy;
        let name = name.as_str().to_owned();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?
                .collect();
            allowed_addrs(addrs, policy).await
        })
    }
}

/// The TLS configuration of ICANN-domain homeservers: the WebPKI roots and
/// the provider the `pubky` SDK uses for them.
fn icann_tls() -> anyhow::Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tls)
}

fn hardened(builder: pubky_reqwest::ClientBuilder) -> pubky_reqwest::ClientBuilder {
    builder
        .redirect(pubky_reqwest::redirect::Policy::none())
        .https_only(true)
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT)
}

/// Where a homeserver can be reached, from its Pkarr record.
struct Route {
    /// The record lists a direct address the policy allows; the homeserver
    /// is reached at its key, over TLS pinned to that key.
    direct: bool,
    /// ICANN endpoints, in the record's priority order: a domain and its
    /// port (443 when unset).
    icann: Vec<(String, Option<u16>)>,
}

/// What one exchange asks a homeserver for.
enum Exchange<'a> {
    /// A record of `owner`, at an absolute `/pub/…` path, path-addressed
    /// (`/storage/{owner}/pub/…`) or in the legacy form with a `pubky-host`
    /// header.
    Storage {
        owner: &'a str,
        path: &'a str,
        path_addressed: bool,
    },
    Events(&'a [(&'a str, &'a str)]),
    Info,
}

impl Exchange<'_> {
    /// The URL at `base` (`https://host[:port]`) and the `pubky-host` header.
    /// `None` when a storage path does not survive URL parsing unchanged:
    /// a `..` segment, an encoded separator, a query or a fragment in it
    /// could otherwise address a record other than `owner`'s at `path`.
    fn url(&self, base: &str) -> Option<(url::Url, Option<String>)> {
        match self {
            Self::Storage {
                owner,
                path,
                path_addressed,
            } => {
                if !path.starts_with('/') {
                    return None;
                }
                let expected = if *path_addressed {
                    format!("/storage/{owner}{path}")
                } else {
                    (*path).to_owned()
                };
                let url = url::Url::parse(&format!("{base}{expected}")).ok()?;
                if url.path() != expected || url.query().is_some() || url.fragment().is_some() {
                    return None;
                }
                let pubky_host = (!path_addressed).then(|| (*owner).to_owned());
                Some((url, pubky_host))
            }
            Self::Events(query) => {
                let mut url = url::Url::parse(&format!("{base}/events-stream")).ok()?;
                url.query_pairs_mut().extend_pairs(query.iter());
                Some((url, None))
            }
            Self::Info => Some((url::Url::parse(&format!("{base}/info")).ok()?, None)),
        }
    }
}

pub(super) struct PkarrTransport {
    pkarr: pkarr::Client,
    /// The Pkarr client's record cache, read directly to tell a fresh
    /// record from a miss.
    records: Arc<InMemoryCache>,
    record_ttl_seconds: u32,
    direct: pubky_reqwest::Client,
    icann: pubky_reqwest::Client,
    /// Per homeserver key: when its `/info` was read, and whether it
    /// advertises path-addressed storage.
    addressing: Mutex<lru::LruCache<String, (Instant, bool)>>,
    policy: EndpointPolicy,
}

impl PkarrTransport {
    pub(super) fn new(
        mut builder: pubky::PubkyHttpClientBuilder,
        cache: PkarrResolutionCache,
        policy: EndpointPolicy,
    ) -> anyhow::Result<Self> {
        let capacity = NonZeroUsize::new(cache.capacity)
            .ok_or_else(|| anyhow::anyhow!("the Pkarr record cache needs a capacity"))?;
        let records = Arc::new(InMemoryCache::new(capacity));
        let record_ttl_seconds = u32::try_from(cache.ttl.as_secs()).unwrap_or(u32::MAX);
        let shared: Arc<dyn Cache> = records.clone();
        builder.pkarr(|pkarr| {
            pkarr
                .cache(shared)
                .minimum_ttl(record_ttl_seconds)
                .maximum_ttl(record_ttl_seconds)
        });
        let pkarr = builder
            .build()
            .map_err(|error| anyhow::anyhow!("homeserver Pkarr client: {error}"))?
            .pkarr()
            .clone();
        let direct = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(DirectResolver {
                pkarr: pkarr.clone(),
                policy,
            }))
            .use_preconfigured_tls(rustls::ClientConfig::from(pkarr.clone()))
            .build()?;
        let icann = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(IcannResolver { policy }))
            .use_preconfigured_tls(icann_tls()?)
            .build()?;
        let addressing = Mutex::new(lru::LruCache::new(
            NonZeroUsize::new(ADDRESSING_CACHE_CAPACITY).expect("non-zero capacity"),
        ));
        Ok(Self {
            pkarr,
            records,
            record_ttl_seconds,
            direct,
            icann,
            addressing,
            policy,
        })
    }

    /// One `GET` of `target` on `pubky`'s homeserver, bounded by `budget`
    /// from the lookup to the end of the body. Also names the homeserver
    /// that answered. `Err` when the homeserver was not found or offers no
    /// endpoint the policy allows (logged here), or the request failed
    /// before a status arrived (logged by the caller).
    pub(super) async fn get(
        &self,
        pubky: &str,
        target: Target<'_>,
        budget: Duration,
    ) -> Result<(pubky_reqwest::Response, String), Unreached> {
        let deadline = Instant::now() + budget;
        let prefix = pubky_host_prefix(pubky);
        let user = match pkarr::PublicKey::try_from(pubky) {
            Ok(user) if user.to_z32() == pubky => user,
            _ => {
                tracing::warn!(pubky_host_prefix = %prefix, "homeserver resolution refused a malformed pubky");
                return Err(Unreached);
            }
        };
        let failure = match tokio::time::timeout_at(deadline, self.homeserver_of(&user)).await {
            Ok(Ok(homeserver)) => Ok(homeserver),
            Ok(Err(failure)) => Err(failure),
            Err(_) => Err("lookup timed out"),
        };
        let homeserver = match failure {
            Ok(homeserver) => homeserver.to_z32(),
            Err(failure) => {
                tracing::warn!(pubky_host_prefix = %prefix, failure, "homeserver resolution failed");
                return Err(Unreached);
            }
        };
        let route = match tokio::time::timeout_at(deadline, self.route(&homeserver)).await {
            Ok(route) if route.direct || !route.icann.is_empty() => route,
            outcome => {
                tracing::warn!(
                    pubky_host_prefix = %prefix,
                    homeserver_prefix = %pubky_host_prefix(&homeserver),
                    failure = if outcome.is_ok() {
                        "no endpoint at an allowed address"
                    } else {
                        "endpoint lookup timed out"
                    },
                    "homeserver transport could not be resolved"
                );
                return Err(Unreached);
            }
        };
        let exchange = match target {
            Target::Record(path) => Exchange::Storage {
                owner: pubky,
                path,
                path_addressed: self.path_addressed(&homeserver, &route, deadline).await,
            },
            Target::Events(query) => Exchange::Events(query),
        };
        let response = self.send(&homeserver, &route, &exchange, deadline).await?;
        Ok((response, homeserver))
    }

    /// The homeserver `user`'s record names: from a cached record still
    /// within the cache TTL, otherwise from the newest record on the network,
    /// never the first answer, which a DHT node could replay from before the
    /// user moved.
    async fn homeserver_of(
        &self,
        user: &pkarr::PublicKey,
    ) -> Result<pkarr::PublicKey, &'static str> {
        let fresh = self
            .records
            .get(&CacheKey::from(user))
            .filter(|packet| !packet.is_expired(self.record_ttl_seconds, self.record_ttl_seconds));
        let packet = match fresh {
            Some(packet) => packet,
            None => match self.pkarr.resolve(user, ResolvePolicy::NetworkOnly).await {
                Ok(packet) => packet,
                Err(ResolveError::NotFound) => return Err("no homeserver record"),
                Err(_) => return Err("lookup failed"),
            },
        };
        homeserver_named_by(&packet).ok_or("no homeserver record")
    }

    async fn route(&self, homeserver: &str) -> Route {
        let endpoints: Vec<Endpoint> = self
            .pkarr
            .resolve_https_endpoints(homeserver)
            .collect()
            .await;
        let mut route = Route {
            direct: false,
            icann: Vec::new(),
        };
        for endpoint in &endpoints {
            match endpoint.domain() {
                None => {
                    route.direct |= endpoint.target() == "."
                        && endpoint
                            .to_socket_addrs()
                            .iter()
                            .any(|addr| self.policy.allows(addr.ip()));
                }
                Some(domain) => {
                    if domain
                        .parse::<IpAddr>()
                        .is_ok_and(|ip| !self.policy.allows(ip))
                    {
                        continue;
                    }
                    route.icann.push((domain.to_owned(), endpoint.port()));
                }
            }
        }
        route
    }

    /// Whether `homeserver` advertises path-addressed storage in its `/info`,
    /// read at most once per [`ADDRESSING_TTL`]. A homeserver whose `/info`
    /// cannot be read is addressed in the legacy form, which every homeserver
    /// release serves.
    async fn path_addressed(&self, homeserver: &str, route: &Route, deadline: Instant) -> bool {
        if let Some((read_at, supported)) = self
            .addressing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(homeserver)
            .copied()
        {
            if read_at.elapsed() < ADDRESSING_TTL {
                return supported;
            }
        }
        let info_deadline = deadline.min(Instant::now() + INFO_TIMEOUT);
        let supported = match self
            .send(homeserver, route, &Exchange::Info, info_deadline)
            .await
        {
            Ok(response) if response.status().is_success() => {
                advertises_path_addressed_storage(response, info_deadline).await
            }
            _ => false,
        };
        self.addressing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .put(homeserver.to_owned(), (Instant::now(), supported));
        supported
    }

    /// Sends `exchange` to the homeserver's direct addresses, then, if no
    /// connection could be made there, to each ICANN endpoint in turn.
    async fn send(
        &self,
        homeserver: &str,
        route: &Route,
        exchange: &Exchange<'_>,
        deadline: Instant,
    ) -> Result<pubky_reqwest::Response, Unreached> {
        let direct = route
            .direct
            .then(|| (&self.direct, format!("https://{homeserver}")));
        let icann = route.icann.iter().map(|(domain, port)| {
            let base = match port {
                Some(port) => format!("https://{domain}:{port}"),
                None => format!("https://{domain}"),
            };
            (&self.icann, base)
        });
        for (client, base) in direct.into_iter().chain(icann) {
            let Some((url, pubky_host)) = exchange.url(&base) else {
                tracing::warn!(
                    homeserver_prefix = %pubky_host_prefix(homeserver),
                    "homeserver request path is not a URL path"
                );
                return Err(Unreached);
            };
            let mut request = client
                .get(url)
                .timeout(deadline.saturating_duration_since(Instant::now()));
            if let Some(owner) = pubky_host {
                request = request.header("pubky-host", owner);
            }
            match request.send().await {
                Ok(response) => return Ok(response),
                Err(error) if error.is_connect() => continue,
                Err(_) => return Err(Unreached),
            }
        }
        Err(Unreached)
    }

    /// Reads `pubky`'s Pkarr record from the network, past the cache, which
    /// the answer then refreshes, so the reads that follow go to the
    /// homeserver the record names now.
    pub(super) async fn refresh(&self, pubky: &str) -> HomeserverRefresh {
        let prefix = pubky_host_prefix(pubky);
        let user = match pkarr::PublicKey::try_from(pubky) {
            Ok(user) if user.to_z32() == pubky => user,
            _ => {
                tracing::warn!(pubky_host_prefix = %prefix, "homeserver refresh refused a malformed pubky");
                return HomeserverRefresh::Unavailable;
            }
        };
        let lookup = self.pkarr.resolve(&user, ResolvePolicy::NetworkOnly);
        let failure = match tokio::time::timeout(REFRESH_TIMEOUT, lookup).await {
            Ok(Ok(_)) => return HomeserverRefresh::Refreshed,
            Ok(Err(ResolveError::NotFound)) => "no record",
            Ok(Err(_)) => "lookup failed",
            Err(_) => "lookup timed out",
        };
        tracing::warn!(pubky_host_prefix = %prefix, failure, "homeserver refresh failed");
        HomeserverRefresh::Unavailable
    }
}

/// The homeserver key a user's record names in its `_pubky` HTTPS or SVCB
/// record, as the `pubky` SDK reads it.
fn homeserver_named_by(packet: &SignedPacket) -> Option<pkarr::PublicKey> {
    let target = packet
        .resource_records("_pubky")
        .find_map(|record| match &record.rdata {
            RData::SVCB(svcb) => Some(svcb.target.to_string()),
            RData::HTTPS(https) => Some(https.0.target.to_string()),
            _ => None,
        })?;
    pkarr::PublicKey::try_from(target.as_str()).ok()
}

async fn advertises_path_addressed_storage(
    mut response: pubky_reqwest::Response,
    deadline: Instant,
) -> bool {
    #[derive(serde::Deserialize)]
    struct Info {
        features: Vec<String>,
    }
    let mut body = Vec::new();
    let read = async {
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len() + chunk.len() > MAX_INFO_BYTES {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        Some(())
    };
    if !matches!(tokio::time::timeout_at(deadline, read).await, Ok(Some(()))) {
        return false;
    }
    serde_json::from_slice::<Info>(&body).is_ok_and(|info| {
        info.features
            .iter()
            .any(|feature| feature == PATH_ADDRESSED_STORAGE)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public(address: &str) -> bool {
        is_public(address.parse().expect("an IP address"))
    }

    #[test]
    fn public_addresses_are_allowed() {
        for address in [
            "1.1.1.1",
            "8.8.8.8",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.0.1.1",
            "198.17.255.255",
            "198.20.0.0",
            "223.255.255.255",
            "2606:4700:4700::1111",
            "2a00:1450::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
            "2001:200::1",
        ] {
            assert!(public(address), "{address} is public");
        }
    }

    #[test]
    fn internal_and_special_addresses_are_refused() {
        for address in [
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "10.255.255.255",
            "127.0.0.1",
            "127.255.0.1",
            "100.64.0.1",
            "100.127.255.255",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.8",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "239.255.255.250",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::127.0.0.1",
            "::10.0.0.1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:0:10.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::a00:1",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:a00:1::1",
            "100::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "febf::1",
            "fec0::1",
            "ff02::1",
            "2001::1",
            "2001:0:4136:e378::1",
            "2001:db8::1",
            "3fff::1",
            "5f00::1",
        ] {
            assert!(!public(address), "{address} is refused");
        }
    }

    #[test]
    fn the_test_policy_adds_only_loopback() {
        let policy = EndpointPolicy::PublicOrLoopback;
        assert!(policy.allows("127.0.0.1".parse().unwrap()));
        assert!(policy.allows("::1".parse().unwrap()));
        assert!(policy.allows("8.8.8.8".parse().unwrap()));
        for address in [
            "10.0.0.1",
            "192.168.0.1",
            "169.254.169.254",
            "::ffff:10.0.0.1",
        ] {
            assert!(!policy.allows(address.parse().unwrap()), "{address}");
        }
        assert!(!EndpointPolicy::PublicOnly.allows("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn storage_urls_keep_the_owner_and_path_they_were_built_from() {
        let owner = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
        let storage = |path, path_addressed| {
            Exchange::Storage {
                owner,
                path,
                path_addressed,
            }
            .url("https://homeserver.example")
        };
        let (url, host) = storage("/pub/pubky.app/marketplace/v1/listings/a1", true).unwrap();
        assert_eq!(
            url.path(),
            format!("/storage/{owner}/pub/pubky.app/marketplace/v1/listings/a1")
        );
        assert_eq!(host, None);
        let (url, host) = storage("/pub/pubky.app/marketplace/v1/listings/a1", false).unwrap();
        assert_eq!(url.path(), "/pub/pubky.app/marketplace/v1/listings/a1");
        assert_eq!(host.as_deref(), Some(owner));

        for path in [
            "/pub/../../other/pub/x",
            "/pub/%2e%2e/%2e%2e/other/pub/x",
            "/pub/x?user=other",
            "/pub/x#fragment",
            "pub/x",
            "/pub/a b",
        ] {
            assert!(storage(path, true).is_none(), "{path} (path-addressed)");
            assert!(storage(path, false).is_none(), "{path} (legacy)");
        }
    }

    #[tokio::test]
    async fn plain_http_is_refused_before_any_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(IcannResolver {
                policy: EndpointPolicy::PublicOrLoopback,
            }))
            .build()
            .unwrap();
        let error = client
            .get(format!("http://localhost:{port}/pub/x"))
            .send()
            .await
            .expect_err("plain HTTP is refused");
        assert!(!error.is_connect(), "{error}");
        let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(accepted.is_err(), "no connection was made");
    }

    #[tokio::test]
    async fn the_icann_resolver_refuses_internal_answers() {
        let resolved = IcannResolver {
            policy: EndpointPolicy::PublicOnly,
        }
        .resolve("localhost".parse().unwrap())
        .await;
        assert!(resolved.is_err(), "localhost resolves only to loopback");
    }
}
