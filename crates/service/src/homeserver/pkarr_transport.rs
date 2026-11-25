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
//!   address. An ICANN endpoint is classified as URL parsing reads it, so an
//!   IP literal in any form (`127.1`, `2130706433`, `0x7f.0.0.1`), which the
//!   HTTP client connects to without resolving, is checked as that address;
//!   and the host of every URL built is checked again before it is sent;
//! - follows no redirect, speaks only HTTPS and uses no proxy;
//! - reads a pubky's record, and its homeserver's, from the cache while
//!   fresh, otherwise from the newest record the network holds, keeping a
//!   cached record the network has nothing newer than;
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

use pubky::pkarr::dns::rdata::{RData, SVCParam, SVCB};
use pubky::pkarr::errors::ResolveError;
use pubky::pkarr::{self, Cache, CacheKey, InMemoryCache, ResolvePolicy, SignedPacket};
use pubky_common::constants::features::PATH_ADDRESSED_STORAGE;
use pubky_reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::time::Instant;
use url::Host;

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

/// The record cache the Pkarr client and the transport share, in which a
/// record is never replaced by an older one. The check and the write are one
/// step under one lock, whoever writes: the Pkarr client, or a lookup that
/// read the cache before awaiting the network and writes back what it read.
#[derive(Debug, Clone)]
struct MonotonicCache {
    records: InMemoryCache,
    writes: Arc<Mutex<()>>,
}

impl MonotonicCache {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            records: InMemoryCache::new(capacity),
            writes: Arc::new(Mutex::new(())),
        }
    }

    /// Stores `packet` unless the cache holds a more recent record for its
    /// key, and returns the most recent record the cache holds.
    fn keep(&self, packet: SignedPacket) -> SignedPacket {
        let _writing = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
        let key = CacheKey::from(&packet.public_key());
        match self.records.get_read_only(&key) {
            Some(held) if held.more_recent_than(&packet) => held,
            _ => {
                self.records.put(&key, &packet);
                packet
            }
        }
    }
}

impl Cache for MonotonicCache {
    fn capacity(&self) -> usize {
        self.records.capacity()
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn put(&self, _key: &CacheKey, signed_packet: &SignedPacket) {
        self.keep(signed_packet.clone());
    }

    fn get(&self, key: &CacheKey) -> Option<SignedPacket> {
        self.records.get(key)
    }

    fn get_read_only(&self, key: &CacheKey) -> Option<SignedPacket> {
        self.records.get_read_only(key)
    }
}

/// The Pkarr client and the record cache it shares with the transport.
#[derive(Clone)]
struct Records {
    pkarr: pkarr::Client,
    cache: Arc<MonotonicCache>,
    ttl_seconds: u32,
}

impl Records {
    /// `key`'s record: the cached one while it is fresh; otherwise the most
    /// recent record the network holds, never the first answer a DHT node
    /// gives, which could be a record `key` has since replaced.
    async fn current(&self, key: &pkarr::PublicKey) -> Result<SignedPacket, ResolveError> {
        let cached = self.cache.get(&CacheKey::from(key));
        if let Some(packet) = cached
            .as_ref()
            .filter(|packet| !packet.is_expired(self.ttl_seconds, self.ttl_seconds))
        {
            return Ok(packet.clone());
        }
        self.network_record(key, cached).await
    }

    /// `key`'s most recent record on the network, read past the cache.
    async fn latest(&self, key: &pkarr::PublicKey) -> Result<SignedPacket, ResolveError> {
        let cached = self.cache.get(&CacheKey::from(key));
        self.network_record(key, cached).await
    }

    /// The network's answer, or `cached` when that is more recent still: an
    /// expired cached record would otherwise lose to an older network answer
    /// on every read. A cached record the network has nothing newer than is
    /// marked as just seen, so it is fresh again. Either way the result is
    /// the most recent record the cache holds once the lookup is done, so a
    /// record another lookup stored while this one waited is not lost.
    async fn network_record(
        &self,
        key: &pkarr::PublicKey,
        cached: Option<SignedPacket>,
    ) -> Result<SignedPacket, ResolveError> {
        let network = self.pkarr.resolve(key, ResolvePolicy::NetworkOnly).await?;
        Ok(more_recent(&self.cache, cached, network))
    }
}

fn more_recent(
    cache: &MonotonicCache,
    cached: Option<SignedPacket>,
    network: SignedPacket,
) -> SignedPacket {
    let chosen = match cached {
        Some(mut cached) if cached.more_recent_than(&network) => {
            cached.refresh();
            cached
        }
        _ => network,
    };
    cache.keep(chosen)
}

/// Resolves a homeserver key to the addresses its own record lists for
/// itself, keeping those the policy allows.
struct DirectResolver {
    records: Records,
    policy: EndpointPolicy,
}

impl Resolve for DirectResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let records = self.records.clone();
        let policy = self.policy;
        let name = name.as_str().to_owned();
        Box::pin(async move {
            let addrs = match pkarr::PublicKey::try_from(name.as_str()) {
                Ok(key) if key.to_z32() == name => match records.current(&key).await {
                    Ok(packet) => direct_addrs(&packet),
                    Err(_) => Vec::new(),
                },
                _ => Vec::new(),
            };
            allowed_addrs(addrs, policy).await
        })
    }
}

/// The homeserver's HTTPS records at its key, lowest priority value first.
fn https_records(packet: &SignedPacket) -> Vec<&SVCB<'_>> {
    let mut records: Vec<&SVCB<'_>> = packet
        .resource_records("@")
        .filter_map(|record| match &record.rdata {
            RData::HTTPS(https) => Some(&https.0),
            _ => None,
        })
        .collect();
    records.sort_by_key(|svcb| svcb.priority);
    records
}

fn port_of(svcb: &SVCB<'_>) -> Option<u16> {
    svcb.iter_params()
        .find_map(|param| match param {
            SVCParam::Port(port) => Some(*port),
            _ => None,
        })
        .filter(|port| *port > 0)
}

/// Whether an HTTPS record names the homeserver's own addresses (target `.`).
fn targets_itself(svcb: &SVCB<'_>) -> bool {
    let target = svcb.target.to_string();
    target.is_empty() || target == "."
}

/// The addresses a homeserver's record lists for itself: its `A` and `AAAA`
/// records, at the port of each HTTPS record that targets `.` (0, the URL's
/// own port, when unset).
fn direct_addrs(packet: &SignedPacket) -> Vec<SocketAddr> {
    let ips: Vec<IpAddr> = packet
        .resource_records("@")
        .filter_map(|record| match &record.rdata {
            RData::A(a) => Some(IpAddr::V4(Ipv4Addr::from(a.address))),
            RData::AAAA(aaaa) => Some(IpAddr::V6(Ipv6Addr::from(aaaa.address))),
            _ => None,
        })
        .collect();
    let mut addrs = Vec::new();
    for svcb in https_records(packet) {
        if !targets_itself(svcb) {
            continue;
        }
        let port = port_of(svcb).unwrap_or(0);
        for ip in &ips {
            let addr = SocketAddr::new(*ip, port);
            if !addrs.contains(&addr) {
                addrs.push(addr);
            }
        }
    }
    addrs
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
#[derive(Debug, PartialEq, Eq)]
struct Route {
    /// The record lists a direct address the policy allows; the homeserver
    /// is reached at its key, over TLS pinned to that key.
    direct: bool,
    /// ICANN endpoints, in the record's priority order: a host and its port
    /// (443 when unset). A host is a domain exactly as the record wrote it
    /// (lowercase, no trailing dot), or an IP address the policy allows.
    icann: Vec<(Host<String>, Option<u16>)>,
}

impl Route {
    fn of(packet: &SignedPacket, policy: EndpointPolicy) -> Self {
        let direct = direct_addrs(packet)
            .iter()
            .any(|addr| policy.allows(addr.ip()));
        let mut icann = Vec::new();
        for svcb in https_records(packet) {
            if targets_itself(svcb) {
                continue;
            }
            let target = svcb.target.to_string();
            // Another Pkarr key: a delegation, which homeservers do not
            // publish for themselves and which is not followed.
            if pkarr::PublicKey::try_from(target.as_str()).is_ok() {
                continue;
            }
            if let Some(host) = icann_host(&target, policy) {
                icann.push((host, port_of(svcb)));
            }
        }
        Self { direct, icann }
    }
}

/// The host an ICANN target names, as URL parsing reads it. A target that
/// parses as an IP address in any form is that address, kept only if the
/// policy allows it; a domain is kept only if parsing leaves it unchanged
/// apart from case, so the URL's host is the name the record wrote.
fn icann_host(target: &str, policy: EndpointPolicy) -> Option<Host<String>> {
    let name = target.strip_suffix('.').unwrap_or(target);
    match Host::parse(name).ok()? {
        Host::Domain(domain) if domain == name.to_ascii_lowercase() => Some(Host::Domain(domain)),
        Host::Domain(_) => None,
        Host::Ipv4(ip) => policy.allows(IpAddr::V4(ip)).then_some(Host::Ipv4(ip)),
        Host::Ipv6(ip) => policy.allows(IpAddr::V6(ip)).then_some(Host::Ipv6(ip)),
    }
}

/// Whether `url` will connect to `expected`: no user info, and a host equal
/// to it; an IP host must also be one the policy allows.
fn connects_to(url: &url::Url, expected: &Host<String>, policy: EndpointPolicy) -> bool {
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    match (url.host(), expected) {
        (Some(Host::Domain(domain)), Host::Domain(expected)) => domain == expected,
        (Some(Host::Ipv4(ip)), Host::Ipv4(expected)) => {
            ip == *expected && policy.allows(IpAddr::V4(ip))
        }
        (Some(Host::Ipv6(ip)), Host::Ipv6(expected)) => {
            ip == *expected && policy.allows(IpAddr::V6(ip))
        }
        _ => false,
    }
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
    records: Records,
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
        let records = Arc::new(MonotonicCache::new(capacity));
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
        let records = Records {
            pkarr: pkarr.clone(),
            cache: records,
            ttl_seconds: record_ttl_seconds,
        };
        let direct = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(DirectResolver {
                records: records.clone(),
                policy,
            }))
            .use_preconfigured_tls(rustls::ClientConfig::from(pkarr))
            .build()?;
        let icann = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(IcannResolver { policy }))
            .use_preconfigured_tls(icann_tls()?)
            .build()?;
        let addressing = Mutex::new(lru::LruCache::new(
            NonZeroUsize::new(ADDRESSING_CACHE_CAPACITY).expect("non-zero capacity"),
        ));
        Ok(Self {
            records,
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
            Ok(Ok(route)) if route.direct || !route.icann.is_empty() => Ok(route),
            Ok(Ok(_)) => Err("no endpoint at an allowed address"),
            Ok(Err(failure)) => Err(failure),
            Err(_) => Err("endpoint lookup timed out"),
        };
        let route = match route {
            Ok(route) => route,
            Err(failure) => {
                tracing::warn!(
                    pubky_host_prefix = %prefix,
                    homeserver_prefix = %pubky_host_prefix(&homeserver),
                    failure,
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

    /// The homeserver `user`'s current record names ([`Records::current`]).
    async fn homeserver_of(
        &self,
        user: &pkarr::PublicKey,
    ) -> Result<pkarr::PublicKey, &'static str> {
        let packet = self
            .records
            .current(user)
            .await
            .map_err(|error| match error {
                ResolveError::NotFound => "no homeserver record",
                _ => "lookup failed",
            })?;
        homeserver_named_by(&packet).ok_or("no homeserver record")
    }

    /// The endpoints of `homeserver`'s current record ([`Records::current`]),
    /// the same rule as a user's.
    async fn route(&self, homeserver: &str) -> Result<Route, &'static str> {
        let key = pkarr::PublicKey::try_from(homeserver).map_err(|_| "malformed homeserver key")?;
        let packet = self
            .records
            .current(&key)
            .await
            .map_err(|error| match error {
                ResolveError::NotFound => "no endpoint record",
                _ => "endpoint lookup failed",
            })?;
        Ok(Route::of(&packet, self.policy))
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
    /// connection could be made there, to each ICANN endpoint in turn. An
    /// endpoint whose URL would connect anywhere but the host it was built
    /// for is skipped.
    async fn send(
        &self,
        homeserver: &str,
        route: &Route,
        exchange: &Exchange<'_>,
        deadline: Instant,
    ) -> Result<pubky_reqwest::Response, Unreached> {
        let direct = route.direct.then(|| {
            (
                &self.direct,
                Host::Domain(homeserver.to_owned()),
                format!("https://{homeserver}"),
            )
        });
        let icann = route.icann.iter().map(|(host, port)| {
            let base = match port {
                Some(port) => format!("https://{host}:{port}"),
                None => format!("https://{host}"),
            };
            (&self.icann, host.clone(), base)
        });
        for (client, host, base) in direct.into_iter().chain(icann) {
            let Some((url, pubky_host)) = exchange.url(&base) else {
                tracing::warn!(
                    homeserver_prefix = %pubky_host_prefix(homeserver),
                    "homeserver request path is not a URL path"
                );
                return Err(Unreached);
            };
            if !connects_to(&url, &host, self.policy) {
                tracing::warn!(
                    homeserver_prefix = %pubky_host_prefix(homeserver),
                    "homeserver endpoint skipped: its URL names another host"
                );
                continue;
            }
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
    /// then holds the most recent record seen ([`Records::latest`]), so the
    /// reads that follow go to the homeserver the record names now.
    pub(super) async fn refresh(&self, pubky: &str) -> HomeserverRefresh {
        let prefix = pubky_host_prefix(pubky);
        let user = match pkarr::PublicKey::try_from(pubky) {
            Ok(user) if user.to_z32() == pubky => user,
            _ => {
                tracing::warn!(pubky_host_prefix = %prefix, "homeserver refresh refused a malformed pubky");
                return HomeserverRefresh::Unavailable;
            }
        };
        let lookup = self.records.latest(&user);
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

    /// A homeserver record with an HTTPS endpoint (port 6286) per target, in
    /// priority order, and `addresses` at its key. Targets are written
    /// unchecked, as a record read off the network may hold any bytes.
    fn endpoint_record(targets: &[&str], addresses: &[&str]) -> SignedPacket {
        let mut packet = SignedPacket::builder();
        for (priority, target) in (1..).zip(targets) {
            let name = if *target == "." {
                ".".try_into().unwrap()
            } else {
                pkarr::dns::Name::new_unchecked(target)
            };
            let mut svcb = SVCB::new(priority, name);
            svcb.set_port(6286);
            packet = packet.https(".".try_into().unwrap(), svcb, 60);
        }
        for address in addresses {
            packet = packet.address(".".try_into().unwrap(), address.parse().unwrap(), 60);
        }
        packet.sign(&pkarr::Keypair::random()).unwrap()
    }

    #[test]
    fn icann_endpoints_are_classified_as_urls_read_them() {
        let delegated = pkarr::Keypair::random().public_key().to_z32();
        let targets = [
            "127.1",
            "2130706433",
            "0x7f.0.0.1",
            "0177.0.0.1",
            "10.1",
            "169.254.43518",
            "127.0.0.1.",
            "[::1]",
            "[::ffff:127.0.0.1]",
            "user@8.8.8.8",
            "8.8.8.8:80",
            "ex%61mple.com",
            delegated.as_str(),
            "8.8.4.4",
            "134744072",
            "Homeserver.Example.",
            "pubky.example",
        ];
        let record = endpoint_record(&targets, &[]);
        assert_eq!(
            Route::of(&record, EndpointPolicy::PublicOnly),
            Route {
                direct: false,
                icann: vec![
                    (Host::Ipv4(Ipv4Addr::new(8, 8, 4, 4)), Some(6286)),
                    (Host::Ipv4(Ipv4Addr::new(8, 8, 8, 8)), Some(6286)),
                    (Host::Domain("homeserver.example".to_owned()), Some(6286)),
                    (Host::Domain("pubky.example".to_owned()), Some(6286)),
                ],
            }
        );
        // The loopback forms are loopback, not domains: the test seam, which
        // allows loopback, reads each as 127.0.0.1.
        let seam = Route::of(
            &endpoint_record(&["127.1", "2130706433", "0x7f.0.0.1", "127.0.0.1."], &[]),
            EndpointPolicy::PublicOrLoopback,
        );
        assert_eq!(
            seam.icann,
            vec![(Host::Ipv4(Ipv4Addr::LOCALHOST), Some(6286)); 4]
        );
    }

    #[test]
    fn direct_endpoints_use_the_records_own_addresses() {
        let internal = endpoint_record(&["."], &["10.0.0.1", "127.0.0.1"]);
        assert!(!Route::of(&internal, EndpointPolicy::PublicOnly).direct);
        assert!(Route::of(&internal, EndpointPolicy::PublicOrLoopback).direct);

        let mixed = endpoint_record(&[".", "pubky.example"], &["10.0.0.1", "8.8.8.8"]);
        assert_eq!(
            direct_addrs(&mixed),
            vec![
                "10.0.0.1:6286".parse::<SocketAddr>().unwrap(),
                "8.8.8.8:6286".parse().unwrap(),
            ]
        );
        assert_eq!(
            Route::of(&mixed, EndpointPolicy::PublicOnly),
            Route {
                direct: true,
                icann: vec![(Host::Domain("pubky.example".to_owned()), Some(6286))],
            }
        );
    }

    #[test]
    fn a_url_connects_only_to_the_host_it_was_built_for() {
        let url = |url: &str| url::Url::parse(url).unwrap();
        let domain = |name: &str| Host::Domain(name.to_owned());
        let public = EndpointPolicy::PublicOnly;
        assert!(connects_to(
            &url("https://pubky.example:6286/pub/x"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://other.example/pub/x"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://user@pubky.example/pub/x"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://127.1/pub/x"),
            &domain("127.1"),
            public
        ));
        let loopback = Host::Ipv4(Ipv4Addr::LOCALHOST);
        assert!(!connects_to(&url("https://127.1/pub/x"), &loopback, public));
        assert!(connects_to(
            &url("https://127.1/pub/x"),
            &loopback,
            EndpointPolicy::PublicOrLoopback
        ));
        let public_ip = Host::Ipv4(Ipv4Addr::new(8, 8, 8, 8));
        assert!(connects_to(
            &url("https://8.8.8.8/pub/x"),
            &public_ip,
            public
        ));
        assert!(!connects_to(
            &url("https://8.8.4.4/pub/x"),
            &public_ip,
            public
        ));
    }

    fn records_of(keypair: &pkarr::Keypair) -> impl Fn(&str) -> SignedPacket + '_ {
        |target: &str| {
            SignedPacket::builder()
                .https(
                    "_pubky".try_into().unwrap(),
                    SVCB::new(0, target.try_into().unwrap()),
                    60,
                )
                .sign(keypair)
                .unwrap()
        }
    }

    #[test]
    fn a_cached_record_newer_than_the_networks_answer_is_kept() {
        let keypair = pkarr::Keypair::random();
        let key = CacheKey::from(&keypair.public_key());
        let record = records_of(&keypair);
        let older = record("older.example");
        let newer = record("newer.example");
        assert!(newer.more_recent_than(&older));
        let cache = MonotonicCache::new(NonZeroUsize::new(4).unwrap());

        // An expired cached record still beats an older network answer, and
        // is cached again as just seen.
        let mut expired = newer.clone();
        expired.set_last_seen(&pkarr::Timestamp::from(0));
        assert!(expired.is_expired(60, 60));
        let kept = more_recent(&cache, Some(expired), older.clone());
        assert_eq!(kept.timestamp(), newer.timestamp());
        let cached = cache.get(&key).expect("the kept record is cached");
        assert_eq!(cached.timestamp(), newer.timestamp());
        assert!(!cached.is_expired(60, 60));

        // A newer network answer wins and is cached.
        let fresh = MonotonicCache::new(NonZeroUsize::new(4).unwrap());
        let chosen = more_recent(&fresh, Some(older.clone()), newer.clone());
        assert_eq!(chosen.timestamp(), newer.timestamp());
        assert_eq!(
            fresh.get(&key).expect("cached").timestamp(),
            newer.timestamp()
        );
    }

    #[test]
    fn a_record_is_never_replaced_by_an_older_one() {
        let keypair = pkarr::Keypair::random();
        let key = CacheKey::from(&keypair.public_key());
        let record = records_of(&keypair);
        let oldest = record("oldest.example");
        let middle = record("middle.example");
        let newest = record("newest.example");
        assert!(middle.more_recent_than(&oldest) && newest.more_recent_than(&middle));
        let cache = MonotonicCache::new(NonZeroUsize::new(4).unwrap());

        cache.put(&key, &middle);
        cache.put(&key, &newest);
        cache.put(&key, &oldest);
        cache.put(&key, &middle);
        assert_eq!(
            cache.get(&key).expect("cached").timestamp(),
            newest.timestamp(),
            "an older record, written by the Pkarr client or not, does not replace a newer one"
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_lookup_that_waited_does_not_replace_a_record_stored_meanwhile() {
        let keypair = pkarr::Keypair::random();
        let key = CacheKey::from(&keypair.public_key());
        let record = records_of(&keypair);
        let oldest = record("oldest.example");
        let middle = record("middle.example");
        let newest = record("newest.example");
        let cache = MonotonicCache::new(NonZeroUsize::new(4).unwrap());
        cache.put(&key, &middle);

        // This lookup read `middle` from the cache and went to the network,
        // which answers with an older record. Another lookup stores `newest`
        // before this one finishes.
        let mut snapshot = cache.get(&key).expect("cached");
        snapshot.set_last_seen(&pkarr::Timestamp::from(0));
        cache.put(&key, &newest);
        let result = more_recent(&cache, Some(snapshot), oldest);

        assert_eq!(
            result.timestamp(),
            newest.timestamp(),
            "the lookup answers with the most recent record, so the next read does not go back to the homeserver the seller left"
        );
        let held = cache.get(&key).expect("cached");
        assert_eq!(held.timestamp(), newest.timestamp());
        assert!(!held.is_expired(60, 60));
    }
}
