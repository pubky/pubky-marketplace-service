//! A store's own domain, resolved to its `*.myshopify.com` domain.
//!
//! A merchant often knows the store as `shop.example`, not
//! `handle.myshopify.com`. Every Shopify storefront answers `GET /meta.json`
//! with the shop's `myshopify_domain` and its primary `domain`. The browser
//! cannot read that across origins, so the service does, under the rules every
//! outbound call on a merchant's behalf follows:
//!
//! * HTTPS only, the standard port, and a public DNS name (never an IP
//!   address, `localhost` or a name without a dot);
//! * every address the name resolves to must be public, and the request is
//!   pinned to those addresses, so a DNS answer cannot change between the check
//!   and the connection;
//! * redirects are followed by hand, at most [`MAX_REDIRECTS`], each hop under
//!   the same rules and only to the same scheme (apex to `www` and back is the
//!   common case); only the new host is used, never the redirect's path;
//! * short timeouts, a small body cap, and at most [`MAX_CONCURRENT`] lookups
//!   at once.
//!
//! The storefront's claim is not trusted on its own: any site can print any
//! `myshopify_domain`. The claimed shop is asked on Shopify's own host
//! (`https://{shop}/meta.json`), and the lookup succeeds only if that answer
//! names the same shop and its primary domain is the host that made the claim
//! (`www.` ignored). A site cannot pass itself off as someone else's store.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Semaphore;

use super::auth;
use super::client::Origin;
use crate::connectors::provider::{ProviderError, ResolvedShop};
use crate::connectors::providers::woocommerce::site::{is_public_ip, Site, SitePolicy};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_BODY_BYTES: usize = 128 * 1024;
pub const MAX_REDIRECTS: usize = 3;
pub const MAX_CONCURRENT: usize = 8;

const NOT_SHOPIFY: &str = "not_a_shopify_store";
const NOT_PRIMARY: &str = "not_primary_domain";

fn invalid(message: &str) -> ProviderError {
    ProviderError::Invalid(message.to_string())
}

fn rejected(code: &str, message: &str) -> ProviderError {
    ProviderError::Rejected {
        code: code.to_string(),
        message: message.to_string(),
    }
}

fn not_shopify(host: &str) -> ProviderError {
    rejected(
        NOT_SHOPIFY,
        &format!(
            "{host} does not look like a Shopify store. Enter the store's \
             your-store.myshopify.com address instead."
        ),
    )
}

/// The lookup's network rules. Production is the default; tests against a
/// loopback stand-in loosen them through [`Resolver::local_testing`].
pub struct Resolver {
    policy: SitePolicy,
    permits: Semaphore,
}

impl Default for Resolver {
    fn default() -> Self {
        Self::new(SitePolicy::default())
    }
}

impl Resolver {
    fn new(policy: SitePolicy) -> Self {
        Self {
            policy,
            permits: Semaphore::new(MAX_CONCURRENT),
        }
    }

    /// For tests only: allows loopback addresses and plain HTTP. Nothing reads
    /// this from the environment, so a deployment cannot reach it.
    pub fn local_testing() -> Self {
        Self::new(SitePolicy {
            plain_http: true,
            allow_private_hosts: true,
        })
    }

    pub async fn resolve(&self, origin: &Origin, raw: &str) -> Result<ResolvedShop, ProviderError> {
        if let Ok(shop) = auth::normalize_shop(raw) {
            return Ok(ResolvedShop { shop, domain: None });
        }
        let site = self.parse(raw)?;
        let Ok(_permit) = self.permits.try_acquire() else {
            return Err(ProviderError::RateLimited {
                retry_after: Some(Duration::from_secs(5)),
            });
        };
        tokio::time::timeout(TOTAL_TIMEOUT, self.lookup(origin, &site))
            .await
            .map_err(|_| ProviderError::Unknown("the store did not answer in time".into()))?
    }

    /// The host of what the merchant typed. A pasted page address keeps its
    /// host and loses its path, query and fragment.
    fn parse(&self, raw: &str) -> Result<Site, ProviderError> {
        let message = "Enter your store address, like your-store.myshopify.com or the \
                       domain your store uses.";
        let cut = raw.find(['?', '#']).map_or(raw, |at| &raw[..at]);
        let site = Site::parse(cut, self.policy).map_err(|_| invalid(message))?;
        let on_a_port = site.port().is_some() && !self.policy.allow_private_hosts;
        let myshopify = site.host().ends_with(".myshopify.com");
        if on_a_port || myshopify || site.host().ends_with(".shopify.com") {
            return Err(invalid(message));
        }
        Ok(site)
    }

    async fn lookup(&self, origin: &Origin, site: &Site) -> Result<ResolvedShop, ProviderError> {
        let plain = self.policy.plain_http;
        let scheme = if plain { "http" } else { "https" };
        let typed = site.host().to_string();
        let port = site.port().map(|p| format!(":{p}")).unwrap_or_default();
        let mut url = format!("{scheme}://{typed}{port}/meta.json");
        let mut host = typed.clone();
        let mut hops = 0;
        let storefront = loop {
            let reply = self.fetch(&url, self.policy).await?;
            match reply.status {
                200 => break reply,
                301 | 302 | 303 | 307 | 308 if hops < MAX_REDIRECTS => {
                    hops += 1;
                    let next = self.redirect_target(&url, reply.location.as_deref())?;
                    host = next.0;
                    url = next.1;
                }
                _ => return Err(not_shopify(&typed)),
            }
        };
        let claim = json_object(&storefront.body).ok_or_else(|| not_shopify(&typed))?;
        let shop = claim
            .get("myshopify_domain")
            .and_then(Value::as_str)
            .and_then(|claimed| auth::normalize_shop(claimed).ok())
            .ok_or_else(|| not_shopify(&typed))?;

        let verify_policy = match origin {
            Origin::Shopify => SitePolicy::default(),
            Origin::Fixed(_) => SitePolicy {
                plain_http: true,
                allow_private_hosts: true,
            },
        };
        let verified = self
            .fetch(&format!("{}/meta.json", origin.base(&shop)), verify_policy)
            .await?;
        if verified.status != 200 {
            return Err(not_shopify(&typed));
        }
        let own = json_object(&verified.body).ok_or_else(|| not_shopify(&typed))?;
        let confirmed = own
            .get("myshopify_domain")
            .and_then(Value::as_str)
            .and_then(|domain| auth::normalize_shop(domain).ok());
        if confirmed.as_deref() != Some(shop.as_str()) {
            return Err(not_shopify(&typed));
        }
        let primary = own
            .get("domain")
            .and_then(Value::as_str)
            .map(bare_host)
            .filter(|domain| !domain.is_empty())
            .ok_or_else(|| not_shopify(&typed))?;
        if !same_site(&primary, &host) {
            return Err(rejected(
                NOT_PRIMARY,
                &format!(
                    "{typed} is not this store's main domain. Enter {primary} or the store's \
                     your-store.myshopify.com address instead."
                ),
            ));
        }
        Ok(ResolvedShop {
            shop,
            domain: Some(typed),
        })
    }

    /// The next `meta.json` URL of a redirect, and its host. Only the host of
    /// the target is used; the scheme and port must stay what they were.
    fn redirect_target(
        &self,
        current: &str,
        location: Option<&str>,
    ) -> Result<(String, String), ProviderError> {
        let unusable = || {
            rejected(
                NOT_SHOPIFY,
                "The store address redirects somewhere that cannot be used. Enter the store's \
                 your-store.myshopify.com address instead.",
            )
        };
        let base = url::Url::parse(current).map_err(|_| unusable())?;
        let target = base
            .join(location.ok_or_else(unusable)?)
            .map_err(|_| unusable())?;
        if target.scheme() != base.scheme()
            || !target.username().is_empty()
            || target.password().is_some()
            || (!self.policy.allow_private_hosts && target.port() != base.port())
        {
            return Err(unusable());
        }
        let host = target
            .host_str()
            .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
            .ok_or_else(unusable)?;
        if !self.policy.allow_private_hosts
            && (host.parse::<IpAddr>().is_ok()
                || host.starts_with('[')
                || !host.contains('.')
                || host.ends_with(".localhost"))
        {
            return Err(unusable());
        }
        let port = target.port().map(|p| format!(":{p}")).unwrap_or_default();
        let next = format!("{}://{host}{port}/meta.json", target.scheme());
        Ok((host, next))
    }

    /// One GET, never redirected, pinned to public addresses, body capped.
    async fn fetch(&self, url: &str, policy: SitePolicy) -> Result<Fetched, ProviderError> {
        let unreachable = || ProviderError::Unknown("the store could not be reached".into());
        let parsed = url::Url::parse(url).map_err(|_| unreachable())?;
        let host = parsed.host_str().ok_or_else(unreachable)?.to_string();
        let port = parsed.port_or_known_default().ok_or_else(unreachable)?;
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .https_only(!policy.plain_http)
            .user_agent("PubkyShop-Connector/1 (+https://pubky.app)");
        match host.parse::<IpAddr>() {
            Ok(ip) => {
                if !policy.allow_private_hosts && !is_public_ip(ip) {
                    return Err(private_network());
                }
            }
            Err(_) => {
                let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map_err(|_| ProviderError::Unknown("the address did not resolve".into()))?
                    .collect();
                ensure_public(&addresses, policy)?;
                builder = builder.resolve_to_addrs(&host, &addresses);
            }
        }
        let client = builder.build().map_err(|_| unreachable())?;
        let mut response = client
            .get(parsed)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    ProviderError::Unknown("the store did not answer in time".into())
                } else {
                    unreachable()
                }
            })?;
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unreachable())? {
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(ProviderError::Unknown(
                    "the store's answer was too large".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Fetched {
            status,
            location,
            body,
        })
    }
}

/// Every address a name resolved to must be public; one private answer among
/// public ones refuses the lookup, so a split DNS reply cannot slip through.
fn ensure_public(addresses: &[SocketAddr], policy: SitePolicy) -> Result<(), ProviderError> {
    if addresses.is_empty() {
        return Err(ProviderError::Unknown("the address did not resolve".into()));
    }
    if !policy.allow_private_hosts && !addresses.iter().all(|address| is_public_ip(address.ip())) {
        return Err(private_network());
    }
    Ok(())
}

fn private_network() -> ProviderError {
    ProviderError::Invalid(
        "That address points at a private network, which cannot be connected.".into(),
    )
}

struct Fetched {
    status: u16,
    location: Option<String>,
    body: Vec<u8>,
}

fn json_object(body: &[u8]) -> Option<serde_json::Map<String, Value>> {
    match serde_json::from_slice(body).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// `https://Shop.Example/path` and `shop.example:443` as `shop.example`.
fn bare_host(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    let rest = lower
        .split_once("://")
        .map_or(lower.as_str(), |(_, rest)| rest);
    let host = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
    let host = host.split(':').next().unwrap_or_default();
    host.trim_end_matches('.').to_string()
}

/// The same site, ignoring case and a leading `www.`.
fn same_site(left: &str, right: &str) -> bool {
    let strip = |host: &str| {
        let lower = host.to_ascii_lowercase();
        lower.strip_prefix("www.").unwrap_or(&lower).to_string()
    };
    !left.is_empty() && strip(left) == strip(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_are_compared_without_scheme_port_case_or_www() {
        assert_eq!(bare_host("https://Shop.Example/"), "shop.example");
        assert_eq!(bare_host("shop.example:443"), "shop.example");
        assert_eq!(bare_host("user@shop.example"), "shop.example");
        assert!(same_site("shop.example", "www.shop.example"));
        assert!(same_site("WWW.shop.example", "shop.example"));
        assert!(!same_site("shop.example", "shop.example.evil.test"));
        assert!(!same_site("shop.example", "other.example"));
        assert!(!same_site("", ""));
    }

    #[test]
    fn a_name_is_refused_when_any_address_it_resolves_to_is_private() {
        let at = |text: &str| -> SocketAddr { text.parse().unwrap() };
        let production = SitePolicy::default();
        assert!(ensure_public(&[at("23.227.38.65:443")], production).is_ok());
        assert!(ensure_public(&[at("[2606:4700::1]:443")], production).is_ok());
        for private in [
            "127.0.0.1:443",
            "10.0.0.1:443",
            "192.168.1.1:443",
            "172.16.0.1:443",
            "169.254.169.254:443",
            "100.64.0.1:443",
            "[::1]:443",
            "[fd00::1]:443",
            "[::ffff:127.0.0.1]:443",
        ] {
            assert!(
                matches!(
                    ensure_public(&[at("23.227.38.65:443"), at(private)], production),
                    Err(ProviderError::Invalid(_))
                ),
                "{private} slipped through"
            );
        }
        assert!(matches!(
            ensure_public(&[], production),
            Err(ProviderError::Unknown(_))
        ));
        let local = SitePolicy {
            plain_http: true,
            allow_private_hosts: true,
        };
        assert!(ensure_public(&[at("127.0.0.1:80")], local).is_ok());
    }

    #[test]
    fn typed_addresses_keep_only_their_host() {
        let resolver = Resolver::default();
        for raw in [
            "shop.example",
            "https://shop.example/",
            "https://shop.example/collections/all?utm=1#top",
            "  SHOP.example  ",
        ] {
            assert_eq!(resolver.parse(raw).unwrap().host(), "shop.example", "{raw}");
        }
    }

    #[test]
    fn addresses_that_cannot_be_a_public_storefront_are_refused() {
        let resolver = Resolver::default();
        for raw in [
            "",
            "localhost",
            "intranet",
            "127.0.0.1",
            "10.0.0.5",
            "[::1]",
            "http://shop.example",
            "ftp://shop.example",
            "user:pass@shop.example",
            "shop.example:8443",
            "a.b.myshopify.com",
            "admin.shopify.com",
            "shop.example|x",
        ] {
            assert!(
                matches!(resolver.parse(raw), Err(ProviderError::Invalid(_))),
                "{raw:?} was accepted"
            );
        }
    }

    #[test]
    fn redirects_keep_scheme_and_port_and_drop_the_path() {
        let resolver = Resolver::default();
        let (host, next) = resolver
            .redirect_target(
                "https://shop.example/meta.json",
                Some("https://www.shop.example/x"),
            )
            .unwrap();
        assert_eq!(host, "www.shop.example");
        assert_eq!(next, "https://www.shop.example/meta.json");
        let (host, _) = resolver
            .redirect_target("https://shop.example/meta.json", Some("/meta.json"))
            .unwrap();
        assert_eq!(host, "shop.example");
        for location in [
            None,
            Some("http://shop.example/"),
            Some("https://127.0.0.1/"),
            Some("https://intranet/"),
            Some("https://shop.example:8443/"),
            Some("https://user@other.example/"),
        ] {
            assert!(
                resolver
                    .redirect_target("https://shop.example/meta.json", location)
                    .is_err(),
                "{location:?} was followed"
            );
        }
    }
}
