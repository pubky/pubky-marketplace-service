//! Outbound HTTP to a homeserver a Pkarr record names.
//!
//! A pubky's Pkarr record names its homeserver, and the homeserver's own
//! record names where to connect to it. Both are published by whoever holds
//! the keys, so any user can point a request at an address of their choosing.
//! Requests sent through [`HomeserverTransport`] therefore:
//!
//! - connect only to globally routable addresses ([`EndpointPolicy`]). The
//!   check runs in the transport's DNS resolvers, on the addresses the
//!   connection is then made to, so a name that resolves differently between
//!   a check and the connect (DNS rebinding) still cannot reach an internal
//!   address. An ICANN endpoint is classified as URL parsing reads it, so an
//!   IP literal in any form (`127.1`, `2130706433`, `0x7f.0.0.1`), which the
//!   HTTP client connects to without resolving, is checked as that address;
//!   and the host of every URL built is checked again before it is sent;
//! - follow no redirect, speak only HTTPS and use no proxy;
//! - never open a connection only to probe whether an endpoint is reachable;
//! - verify a direct endpoint's TLS key without any record lookup
//!   ([`direct_tls`]): the host a request is addressed to is the key.
//!
//! It replaces the `pubky` SDK's request path, whose clients follow
//! redirects, allow plain HTTP, connect to whatever addresses a record names
//! and TCP-probe a record's direct addresses, with no setting to change that.
//! Only the SDK's Pkarr client is used. A redirect, scheme and
//! endpoint-address option in the SDK would let this module go.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pubky::pkarr::dns::rdata::{RData, SVCParam, SVCB};
use pubky::pkarr::errors::ResolveError;
use pubky::pkarr::{self, ResolvePolicy, SignedPacket};
use pubky_reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::Host;

/// Bound on one TCP connect, so a homeserver's unreachable direct address
/// leaves the request's budget for its ICANN endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Which addresses a request may connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointPolicy {
    /// Globally routable addresses only. The only policy a built service has.
    PublicOnly,
    /// Also loopback, where a local test network's homeservers listen.
    #[cfg(any(test, feature = "test-faults"))]
    PublicOrLoopback,
}

impl EndpointPolicy {
    pub fn allows(self, ip: IpAddr) -> bool {
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
pub fn is_public(ip: IpAddr) -> bool {
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

/// No address a request may connect to.
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

/// A key's current Pkarr record, as the transport reads it: the user's record
/// naming their homeserver, and the homeserver's own naming its endpoints.
pub trait RecordSource: Send + Sync + 'static {
    fn current<'a>(
        &'a self,
        key: &'a pkarr::PublicKey,
    ) -> Pin<Box<dyn Future<Output = Result<SignedPacket, ResolveError>> + Send + 'a>>;
}

/// The Pkarr client's cache while fresh, otherwise the network: the policy
/// the `pubky` SDK resolves homeservers with.
impl RecordSource for pkarr::Client {
    fn current<'a>(
        &'a self,
        key: &'a pkarr::PublicKey,
    ) -> Pin<Box<dyn Future<Output = Result<SignedPacket, ResolveError>> + Send + 'a>> {
        Box::pin(self.resolve(key, ResolvePolicy::CacheFirst))
    }
}

/// Resolves a homeserver key to the addresses its own record lists for
/// itself, keeping those the policy allows.
struct DirectResolver {
    records: Arc<dyn RecordSource>,
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

/// The TLS configuration of direct homeserver endpoints, of this transport
/// and of the `HOMESERVER_RESOLUTION=pkarr` one: the server must present the
/// raw public key the request is addressed to, its own.
pub(crate) fn direct_tls() -> rustls::ClientConfig {
    rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("the provider supports the default protocol versions")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(PinnedKeyVerifier {
        algorithms: rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
    }))
    .with_no_client_auth()
}

/// Accepts a server whose raw public key is the one named by the host of the
/// request, and no other. The Pkarr client's own verifier also accepts keys
/// the host's record delegates to, and finds them by resolving records inside
/// the handshake, blocking the runtime thread that drives it for as long as
/// those lookups take. A homeserver's own key needs no lookup: the host is
/// the key.
#[derive(Debug)]
struct PinnedKeyVerifier {
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl rustls::client::danger::ServerCertVerifier for PinnedKeyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ));
        }
        let not_valid_for_name =
            || rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName);
        let rustls::pki_types::ServerName::DnsName(host) = server_name else {
            return Err(not_valid_for_name());
        };
        let key = pkarr::PublicKey::try_from(host.as_ref()).map_err(|_| not_valid_for_name())?;
        if key.to_public_key_der().as_bytes() == end_entity.as_ref() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verify_tls13_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature_with_raw_key(
            message,
            &rustls::pki_types::SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![rustls::SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
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
pub struct Route {
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

    /// No endpoint the policy allows.
    pub fn is_empty(&self) -> bool {
        !self.direct && self.icann.is_empty()
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

/// Whether a homeserver's record names `host` as one of its endpoints: the
/// homeserver's key, an address it lists for itself, or an ICANN target as
/// URL parsing reads it. A domain's trailing dot is ignored on both sides.
fn names_host(packet: &SignedPacket, homeserver: &pkarr::PublicKey, host: &Host<String>) -> bool {
    let host = match host {
        Host::Domain(domain) => Host::Domain(domain.strip_suffix('.').unwrap_or(domain).to_owned()),
        other => other.clone(),
    };
    let direct = |ip: IpAddr| direct_addrs(packet).iter().any(|addr| addr.ip() == ip);
    let named_directly = match &host {
        Host::Domain(domain) => *domain == homeserver.to_z32(),
        Host::Ipv4(ip) => direct(IpAddr::V4(*ip)),
        Host::Ipv6(ip) => direct(IpAddr::V6(*ip)),
    };
    named_directly
        || https_records(packet)
            .into_iter()
            .filter(|svcb| !targets_itself(svcb))
            .any(|svcb| {
                let target = svcb.target.to_string();
                Host::parse(target.strip_suffix('.').unwrap_or(&target)).as_ref() == Ok(&host)
            })
}

/// What a homeserver's current record says about a host it is expected at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCheck {
    /// The record names the host, and an endpoint the policy allows.
    Named,
    /// The record does not name the host among its endpoints.
    NotNamed,
    /// The record names the host, but no endpoint the policy allows.
    NoAllowedEndpoint,
    /// The homeserver has no record.
    NoRecord,
    /// The record could not be read.
    Unavailable,
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

/// Why [`HomeserverTransport::send`] has no response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsent {
    /// The path is not one URL path, or no endpoint could be connected to.
    Unreachable,
    /// A connection was made, and the request then failed or timed out.
    Failed,
}

pub struct HomeserverTransport {
    records: Arc<dyn RecordSource>,
    direct: pubky_reqwest::Client,
    icann: pubky_reqwest::Client,
    policy: EndpointPolicy,
}

impl HomeserverTransport {
    /// Reads records through `pkarr`'s cache while fresh, otherwise the
    /// network. Direct endpoints' TLS is pinned to the homeserver key
    /// ([`direct_tls`]), checked without reading any record.
    pub fn new(pkarr: pkarr::Client, policy: EndpointPolicy) -> anyhow::Result<Self> {
        Self::with_records(Arc::new(pkarr), policy)
    }

    /// Like [`Self::new`], with the records read from `records`.
    pub fn with_records(
        records: Arc<dyn RecordSource>,
        policy: EndpointPolicy,
    ) -> anyhow::Result<Self> {
        let direct = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(DirectResolver {
                records: records.clone(),
                policy,
            }))
            .use_preconfigured_tls(direct_tls())
            .build()?;
        let icann = hardened(pubky_reqwest::Client::builder())
            .dns_resolver(Arc::new(IcannResolver { policy }))
            .use_preconfigured_tls(icann_tls()?)
            .build()?;
        Ok(Self {
            records,
            direct,
            icann,
            policy,
        })
    }

    /// The homeserver `user`'s current record names. `Ok(None)` when `user`
    /// has no record, or one that names no homeserver key.
    pub async fn homeserver_of(
        &self,
        user: &pkarr::PublicKey,
    ) -> Result<Option<pkarr::PublicKey>, ResolveError> {
        match self.records.current(user).await {
            Ok(packet) => Ok(homeserver_named_by(&packet)),
            Err(ResolveError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The endpoints of `homeserver`'s current record the policy allows.
    pub async fn route(&self, homeserver: &pkarr::PublicKey) -> Result<Route, ResolveError> {
        let packet = self.records.current(homeserver).await?;
        Ok(Route::of(&packet, self.policy))
    }

    /// Whether `homeserver`'s current record names `host`, read without
    /// connecting to any endpoint.
    pub async fn check_record(
        &self,
        homeserver: &pkarr::PublicKey,
        host: &Host<String>,
    ) -> RecordCheck {
        let packet = match self.records.current(homeserver).await {
            Ok(packet) => packet,
            Err(ResolveError::NotFound) => return RecordCheck::NoRecord,
            Err(_) => return RecordCheck::Unavailable,
        };
        if !names_host(&packet, homeserver, host) {
            RecordCheck::NotNamed
        } else if Route::of(&packet, self.policy).is_empty() {
            RecordCheck::NoAllowedEndpoint
        } else {
            RecordCheck::Named
        }
    }

    /// Sends the request `build` makes for `path` (an absolute path, no query
    /// or fragment) to the homeserver's direct addresses, then, if no
    /// connection could be made there, to each ICANN endpoint in turn. An
    /// endpoint whose URL would connect anywhere but the host it was built
    /// for is skipped. `build` may be called once per endpoint tried.
    pub async fn send<F>(
        &self,
        homeserver: &pkarr::PublicKey,
        route: &Route,
        path: &str,
        build: F,
    ) -> Result<pubky_reqwest::Response, Unsent>
    where
        F: Fn(&pubky_reqwest::Client, url::Url) -> pubky_reqwest::RequestBuilder,
    {
        let homeserver = homeserver.to_z32();
        let direct = route.direct.then(|| {
            (
                &self.direct,
                Host::Domain(homeserver.clone()),
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
            let Some(url) = url_path(&base, path) else {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver),
                    "homeserver request path is not a URL path"
                );
                return Err(Unsent::Unreachable);
            };
            if !connects_to(&url, &host, self.policy) {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver),
                    "homeserver endpoint skipped: its URL names another host"
                );
                continue;
            }
            match build(client, url).send().await {
                Ok(response) => return Ok(response),
                Err(error) if error.is_connect() => continue,
                Err(_) => return Err(Unsent::Failed),
            }
        }
        Err(Unsent::Unreachable)
    }
}

/// `base` (`https://host[:port]`) joined with `path`, or `None` when `path`
/// does not survive URL parsing unchanged as a path alone.
fn url_path(base: &str, path: &str) -> Option<url::Url> {
    if !path.starts_with('/') {
        return None;
    }
    let url = url::Url::parse(&format!("{base}{path}")).ok()?;
    (url.path() == path && url.query().is_none() && url.fragment().is_none()).then_some(url)
}

/// The first eight characters of a key, all a log line may carry.
pub fn key_prefix(key: &str) -> &str {
    key.get(..8).unwrap_or(key)
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
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:a00:1::1",
            "2002:a9fe:a9fe::",
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
    fn request_urls_keep_the_path_they_were_built_from() {
        let base = "https://homeserver.example";
        assert_eq!(
            url_path(base, "/auth/grant/session").map(|url| url.to_string()),
            Some("https://homeserver.example/auth/grant/session".to_owned())
        );
        for path in [
            "/auth/../admin",
            "/auth/%2e%2e/admin",
            "/auth/grant/session?x=1",
            "/auth/grant/session#fragment",
            "auth/grant/session",
            "/auth/a b",
            "@127.0.0.1/auth",
        ] {
            assert!(url_path(base, path).is_none(), "{path}");
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
        for url in [
            format!("http://localhost:{port}/auth/grant/session"),
            format!("http://127.0.0.1:{port}/auth/grant/session"),
        ] {
            let error = client
                .post(url)
                .send()
                .await
                .expect_err("plain HTTP is refused");
            assert!(!error.is_connect(), "{error}");
        }
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
            "[::ffff:7f00:1]",
            "[64:ff9b::a9fe:a9fe]",
            "[2002:a9fe:a9fe::]",
            "[fd00::1]",
            "[fe80::1%25eth0]",
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
        let internal = endpoint_record(
            &["."],
            &[
                "10.0.0.1",
                "127.0.0.1",
                "::1",
                "::ffff:127.0.0.1",
                "64:ff9b::7f00:1",
                "2002:7f00:1::",
            ],
        );
        assert!(!Route::of(&internal, EndpointPolicy::PublicOnly).direct);
        assert!(Route::of(&internal, EndpointPolicy::PublicOnly).is_empty());
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
    fn a_record_names_its_key_its_own_addresses_and_its_icann_targets() {
        let record = endpoint_record(
            &[".", "Homeserver.Example.", "2130706433"],
            &["8.8.8.8", "2606:4700::1111"],
        );
        let key = record.public_key();
        let named = |url: &str| {
            let url = url::Url::parse(url).unwrap();
            names_host(&record, &key, &url.host().unwrap().to_owned())
        };
        for url in [
            format!("https://{}", key.to_z32()),
            "https://homeserver.example".to_owned(),
            "https://HOMESERVER.example./pub/".to_owned(),
            "http://homeserver.example:6286".to_owned(),
            "https://127.0.0.1".to_owned(),
            "https://8.8.8.8".to_owned(),
            "https://[2606:4700::1111]".to_owned(),
        ] {
            assert!(named(&url), "{url} is named");
        }
        for url in [
            format!("https://{}", pkarr::Keypair::random().public_key().to_z32()),
            "https://homeserver.example.com".to_owned(),
            "https://www.homeserver.example".to_owned(),
            "https://example".to_owned(),
            "https://8.8.4.4".to_owned(),
            "https://[::1]".to_owned(),
        ] {
            assert!(!named(&url), "{url} is not named");
        }
    }

    #[tokio::test]
    async fn the_direct_resolver_keeps_only_allowed_addresses() {
        let keypair = pkarr::Keypair::random();
        let mut svcb = SVCB::new(1, ".".try_into().unwrap());
        svcb.set_port(6286);
        let mut packet = SignedPacket::builder().https(".".try_into().unwrap(), svcb, 60);
        for address in ["127.0.0.1", "10.0.0.1", "::ffff:169.254.169.254", "8.8.8.8"] {
            packet = packet.address(".".try_into().unwrap(), address.parse().unwrap(), 60);
        }
        let packet = packet.sign(&keypair).unwrap();
        struct Fixed(SignedPacket);
        impl RecordSource for Fixed {
            fn current<'a>(
                &'a self,
                _key: &'a pkarr::PublicKey,
            ) -> Pin<Box<dyn Future<Output = Result<SignedPacket, ResolveError>> + Send + 'a>>
            {
                let packet = self.0.clone();
                Box::pin(async move { Ok(packet) })
            }
        }
        let resolver = DirectResolver {
            records: Arc::new(Fixed(packet)),
            policy: EndpointPolicy::PublicOnly,
        };
        let name = keypair.public_key().to_z32();
        let addrs: Vec<SocketAddr> = resolver
            .resolve(name.parse().unwrap())
            .await
            .expect("one public address")
            .collect();
        assert_eq!(addrs, vec!["8.8.8.8:6286".parse::<SocketAddr>().unwrap()]);
        assert!(
            resolver
                .resolve("localhost".parse().unwrap())
                .await
                .is_err(),
            "a name that is not a key resolves to nothing"
        );
    }

    #[test]
    fn a_url_connects_only_to_the_host_it_was_built_for() {
        let url = |url: &str| url::Url::parse(url).unwrap();
        let domain = |name: &str| Host::Domain(name.to_owned());
        let public = EndpointPolicy::PublicOnly;
        assert!(connects_to(
            &url("https://pubky.example:6286/auth/grant/session"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://other.example/auth/grant/session"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://user@pubky.example/auth/grant/session"),
            &domain("pubky.example"),
            public
        ));
        assert!(!connects_to(
            &url("https://127.1/auth/grant/session"),
            &domain("127.1"),
            public
        ));
        let loopback = Host::Ipv4(Ipv4Addr::LOCALHOST);
        assert!(!connects_to(
            &url("https://127.1/auth/grant/session"),
            &loopback,
            public
        ));
        assert!(connects_to(
            &url("https://127.1/auth/grant/session"),
            &loopback,
            EndpointPolicy::PublicOrLoopback
        ));
        let public_ip = Host::Ipv4(Ipv4Addr::new(8, 8, 8, 8));
        assert!(connects_to(
            &url("https://8.8.8.8/auth/grant/session"),
            &public_ip,
            public
        ));
        assert!(!connects_to(
            &url("https://8.8.4.4/auth/grant/session"),
            &public_ip,
            public
        ));
    }

    #[test]
    fn the_homeserver_is_read_from_the_users_pubky_record() {
        let homeserver = pkarr::Keypair::random().public_key();
        let user = pkarr::Keypair::random();
        let target = homeserver.to_z32();
        let record = SignedPacket::builder()
            .https(
                "_pubky".try_into().unwrap(),
                SVCB::new(0, target.as_str().try_into().unwrap()),
                60,
            )
            .sign(&user)
            .unwrap();
        assert_eq!(homeserver_named_by(&record), Some(homeserver));
        let domain = SignedPacket::builder()
            .https(
                "_pubky".try_into().unwrap(),
                SVCB::new(0, "homeserver.example".try_into().unwrap()),
                60,
            )
            .sign(&user)
            .unwrap();
        assert_eq!(homeserver_named_by(&domain), None);
    }

    fn pinned_verifier() -> PinnedKeyVerifier {
        PinnedKeyVerifier {
            algorithms: rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms,
        }
    }

    fn verified(
        host: &str,
        presented: &pkarr::PublicKey,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
    ) -> bool {
        use rustls::client::danger::ServerCertVerifier;
        let certificate =
            rustls::pki_types::CertificateDer::from(presented.to_public_key_der().to_vec());
        let name = rustls::pki_types::ServerName::try_from(host.to_owned()).expect("server name");
        pinned_verifier()
            .verify_server_cert(
                &certificate,
                intermediates,
                &name,
                &[],
                rustls::pki_types::UnixTime::now(),
            )
            .is_ok()
    }

    #[test]
    fn the_direct_verifier_accepts_only_the_hosts_own_key() {
        let homeserver = pkarr::Keypair::random().public_key();
        let other = pkarr::Keypair::random().public_key();
        let host = homeserver.to_z32();

        assert!(verified(&host, &homeserver, &[]));
        assert!(!verified(&host, &other, &[]));
        assert!(!verified("homeserver.example", &homeserver, &[]));
        assert!(!verified("127.0.0.1", &homeserver, &[]));
        let extra = rustls::pki_types::CertificateDer::from(other.to_public_key_der().to_vec());
        assert!(!verified(&host, &homeserver, &[extra]));
        use rustls::client::danger::ServerCertVerifier;
        assert!(pinned_verifier().requires_raw_public_keys());
        assert_eq!(
            pinned_verifier().supported_verify_schemes(),
            vec![rustls::SignatureScheme::ED25519]
        );
    }

    /// Handshakes of the transport's direct client with a server on
    /// loopback, while every record lookup the Pkarr client makes goes to a
    /// relay that counts it. The Pkarr client caches nothing, so a lookup
    /// during a handshake cannot be answered from memory unseen.
    mod direct_handshakes {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Mutex;

        use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::time::Instant;

        use super::*;

        /// How long the Pkarr client waits for a relay answer. A handshake
        /// that made a lookup at a relay gone silent takes at least this.
        const RELAY_TIMEOUT: Duration = Duration::from_secs(2);

        /// A Pkarr relay on loopback serving the records it was given and
        /// counting the lookups it receives. Once silenced it answers none.
        #[derive(Clone)]
        struct Relay {
            url: String,
            records: Arc<Mutex<HashMap<String, SignedPacket>>>,
            lookups: Arc<AtomicUsize>,
            silent: Arc<AtomicBool>,
        }

        impl Relay {
            async fn start() -> Self {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let relay = Self {
                    url: format!("http://{}", socket.local_addr().unwrap()),
                    records: Arc::default(),
                    lookups: Arc::default(),
                    silent: Arc::default(),
                };
                let serving = relay.clone();
                tokio::spawn(async move {
                    while let Ok((mut stream, _)) = socket.accept().await {
                        let relay = serving.clone();
                        tokio::spawn(async move {
                            let Some(head) = request_head(&mut stream).await else {
                                return;
                            };
                            relay.lookups.fetch_add(1, Ordering::SeqCst);
                            if relay.silent.load(Ordering::SeqCst) {
                                let _ = stream.read(&mut [0u8; 1]).await;
                                return;
                            }
                            let _ = stream.write_all(&relay.answer(&head)).await;
                        });
                    }
                });
                relay
            }

            fn serve(&self, packet: SignedPacket) {
                self.records
                    .lock()
                    .unwrap()
                    .insert(packet.public_key().to_z32(), packet);
            }

            fn answer(&self, head: &str) -> Vec<u8> {
                let path = head.split_whitespace().nth(1).unwrap_or_default();
                let key = path.split('?').next().unwrap_or_default();
                let key = key.rsplit('/').next().unwrap_or_default();
                match self.records.lock().unwrap().get(key) {
                    Some(packet) => {
                        let body = packet.to_relay_payload();
                        let mut answer = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .into_bytes();
                        answer.extend_from_slice(&body);
                        answer
                    }
                    None => {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    }
                }
            }

            fn lookups(&self) -> usize {
                self.lookups.load(Ordering::SeqCst)
            }

            /// A Pkarr client that reads records only from this relay and
            /// caches none.
            fn client(&self) -> pkarr::Client {
                pkarr::Client::builder()
                    .no_default_network()
                    .relays(&[self.url.as_str()])
                    .unwrap()
                    .cache_size(0)
                    .relay_request_timeout(RELAY_TIMEOUT)
                    .build()
                    .unwrap()
            }
        }

        async fn request_head<S: AsyncRead + Unpin>(stream: &mut S) -> Option<String> {
            let mut head = Vec::new();
            let mut buffer = [0u8; 1024];
            while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => return None,
                    Ok(read) => head.extend_from_slice(&buffer[..read]),
                }
            }
            Some(String::from_utf8_lossy(&head).into_owned())
        }

        /// One TLS handshake the server ran: whether it completed, the relay
        /// lookups made while it ran, and how long it took.
        #[derive(Debug, Clone, Copy)]
        struct Handshake {
            completed: bool,
            lookups: usize,
            took: Duration,
        }

        /// A server on loopback presenting `presented`'s raw public key and
        /// answering every request `200`. When `silence` is set, a connection
        /// silences `relay` before the handshake starts.
        async fn pinned_server(
            presented: &pkarr::Keypair,
            relay: &Relay,
            silence: bool,
        ) -> (u16, Arc<Mutex<Vec<Handshake>>>) {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = socket.local_addr().unwrap().port();
            let acceptor =
                tokio_rustls::TlsAcceptor::from(Arc::new(presented.to_rpk_rustls_server_config()));
            let handshakes = Arc::new(Mutex::new(Vec::new()));
            let (relay, recorded) = (relay.clone(), handshakes.clone());
            tokio::spawn(async move {
                while let Ok((stream, _)) = socket.accept().await {
                    if silence {
                        relay.silent.store(true, Ordering::SeqCst);
                    }
                    let (started, before) = (Instant::now(), relay.lookups());
                    let accepted = acceptor.accept(stream).await;
                    recorded.lock().unwrap().push(Handshake {
                        completed: accepted.is_ok(),
                        lookups: relay.lookups() - before,
                        took: started.elapsed(),
                    });
                    let Ok(mut tls) = accepted else {
                        continue;
                    };
                    tokio::spawn(async move {
                        if request_head(&mut tls).await.is_some() {
                            let _ = tls
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                )
                                .await;
                            let _ = tls.shutdown().await;
                        }
                    });
                }
            });
            (port, handshakes)
        }

        /// `homeserver`'s record listing 127.0.0.1 at `port` for itself and,
        /// if given, a delegation to `delegate`.
        fn direct_record(
            homeserver: &pkarr::Keypair,
            port: u16,
            delegate: Option<&pkarr::PublicKey>,
        ) -> SignedPacket {
            let mut direct = SVCB::new(1, ".".try_into().unwrap());
            direct.set_port(port);
            let mut packet = SignedPacket::builder()
                .https(".".try_into().unwrap(), direct, 60)
                .address(".".try_into().unwrap(), Ipv4Addr::LOCALHOST.into(), 60);
            if let Some(delegate) = delegate {
                let target = delegate.to_z32();
                let mut delegated = SVCB::new(2, target.as_str().try_into().unwrap());
                delegated.set_port(port);
                packet = packet.https(".".try_into().unwrap(), delegated, 60);
            }
            packet.sign(homeserver).unwrap()
        }

        /// One `GET` through a transport reading `relay`'s records, to
        /// `homeserver`'s direct endpoint.
        async fn get(
            relay: &Relay,
            homeserver: &pkarr::PublicKey,
        ) -> Result<pubky_reqwest::Response, Unsent> {
            let transport =
                HomeserverTransport::new(relay.client(), EndpointPolicy::PublicOrLoopback).unwrap();
            let route = transport
                .route(homeserver)
                .await
                .expect("the relay serves the homeserver's record");
            assert!(!route.is_empty(), "the record lists a direct endpoint");
            transport
                .send(homeserver, &route, "/pub/x", |client, url| client.get(url))
                .await
        }

        /// The handshakes the server recorded. A refused client can return
        /// before the server reads its alert, so this waits for the first.
        async fn handshakes_of(recorded: &Mutex<Vec<Handshake>>) -> Vec<Handshake> {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let handshakes = recorded.lock().unwrap().clone();
                if !handshakes.is_empty() {
                    return handshakes;
                }
                assert!(Instant::now() < deadline, "the server was reached");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_hosts_own_key_is_accepted_with_no_lookup_while_the_relay_is_silent() {
            let relay = Relay::start().await;
            let homeserver = pkarr::Keypair::random();
            let (port, recorded) = pinned_server(&homeserver, &relay, true).await;
            relay.serve(direct_record(&homeserver, port, None));

            let response = get(&relay, &homeserver.public_key())
                .await
                .expect("the homeserver answers");
            assert_eq!(response.status().as_u16(), 200);
            for handshake in handshakes_of(&recorded).await {
                assert!(handshake.completed, "{handshake:?}");
                assert_eq!(handshake.lookups, 0, "{handshake:?}");
                assert!(handshake.took < RELAY_TIMEOUT, "{handshake:?}");
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn another_key_is_refused_at_once_with_no_lookup() {
            let relay = Relay::start().await;
            let homeserver = pkarr::Keypair::random();
            let impostor = pkarr::Keypair::random();
            let (port, recorded) = pinned_server(&impostor, &relay, true).await;
            relay.serve(direct_record(&homeserver, port, None));

            assert!(get(&relay, &homeserver.public_key()).await.is_err());
            for handshake in handshakes_of(&recorded).await {
                assert!(!handshake.completed, "{handshake:?}");
                assert_eq!(handshake.lookups, 0, "{handshake:?}");
                assert!(handshake.took < RELAY_TIMEOUT, "{handshake:?}");
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_key_the_record_delegates_to_is_refused_with_no_lookup() {
            let relay = Relay::start().await;
            let homeserver = pkarr::Keypair::random();
            let delegate = pkarr::Keypair::random();
            let (port, recorded) = pinned_server(&delegate, &relay, false).await;
            relay.serve(direct_record(
                &homeserver,
                port,
                Some(&delegate.public_key()),
            ));
            relay.serve(direct_record(&delegate, port, None));

            assert!(get(&relay, &homeserver.public_key()).await.is_err());
            for handshake in handshakes_of(&recorded).await {
                assert!(!handshake.completed, "{handshake:?}");
                assert_eq!(handshake.lookups, 0, "{handshake:?}");
            }
        }
    }
}
