//! The merchant's site address: validation, the canonical form used as the
//! connection key, and the guard that keeps the service from being pointed at
//! its own network.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::connectors::provider::ProviderError;

/// What a deployment accepts. Production is the default: HTTPS only, public
/// DNS names only. Tests replaying captured responses against a loopback
/// server loosen it through [`super::WooOptions::local_testing`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SitePolicy {
    pub plain_http: bool,
    pub allow_private_hosts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Site {
    host: String,
    port: Option<u16>,
    /// Empty, or `/subdirectory` without a trailing slash.
    path: String,
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Invalid(message.to_string())
}

impl Site {
    /// Accepts `shop.example`, `shop.example:8443`, `shop.example/store` and
    /// the same with `https://` in front, with or without a trailing slash.
    /// Refuses `http://`, other schemes, credentials, a query or a fragment.
    pub fn parse(raw: &str, policy: SitePolicy) -> Result<Self, ProviderError> {
        let raw = raw.trim();
        if raw.is_empty() || raw.len() > 253 || raw.chars().any(|c| c.is_control() || c == '|') {
            return Err(invalid(
                "Enter the store address, for example shop.example.",
            ));
        }
        let lower = raw.to_ascii_lowercase();
        let rest = if let Some(rest) = lower.strip_prefix("https://") {
            rest
        } else if let Some(rest) = lower.strip_prefix("http://") {
            if !policy.plain_http {
                return Err(invalid(
                    "The store must be served over HTTPS. WooCommerce only creates API keys \
                     for HTTPS sites; enable HTTPS on the store and try again.",
                ));
            }
            rest
        } else if lower.contains("://") {
            return Err(invalid(
                "Enter the store address without a protocol other than https://.",
            ));
        } else {
            lower.as_str()
        };
        let url = url::Url::parse(&format!("https://{rest}"))
            .map_err(|_| invalid("The store address is not a valid web address."))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(invalid(
                "The store address must not contain a username or password.",
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(invalid(
                "Enter the store address without a query or fragment.",
            ));
        }
        let Some(host) = url.host_str() else {
            return Err(invalid("The store address has no host."));
        };
        let host = host.trim_end_matches('.').to_string();
        let is_ip = host.parse::<IpAddr>().is_ok() || host.starts_with('[');
        if !policy.allow_private_hosts {
            if is_ip {
                return Err(invalid(
                    "Use the store's domain name, not an IP address (its HTTPS certificate is \
                     issued for the name).",
                ));
            }
            if !host.contains('.') || host == "localhost" || host.ends_with(".localhost") {
                return Err(invalid("The store address must be a public domain name."));
            }
        }
        let path = url.path().trim_end_matches('/').to_string();
        if path.contains("//") || path.split('/').any(|part| part == ".." || part == ".") {
            return Err(invalid("The store address path is not valid."));
        }
        Ok(Self {
            host,
            port: url.port(),
            path,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// `host[:port][/path]`: the connection's shop key.
    pub fn canonical(&self) -> String {
        let port = self.port.map(|p| format!(":{p}")).unwrap_or_default();
        format!("{}{port}{}", self.host, self.path)
    }

    pub fn origin(&self, plain_http: bool) -> String {
        let scheme = if plain_http { "http" } else { "https" };
        format!("{scheme}://{}", self.canonical())
    }

    /// An absolute URL below the site, `path` starting with `/`.
    pub fn url(&self, plain_http: bool, path: &str) -> String {
        format!("{}{path}", self.origin(plain_http))
    }
}

/// True for an address the service may connect to on a merchant's behalf:
/// public unicast only. Loopback, private, link-local, carrier-grade NAT,
/// documentation, benchmarking, multicast, reserved and unique-local ranges
/// are all refused, so a merchant cannot aim a connection at the service's
/// own network or the cloud metadata address.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 198 && (18..=19).contains(&b))
        || a >= 240
        || a == 0)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let segments = ip.segments();
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x0064 && segments[1] == 0xff9b)
        || segments[0] == 0x0100)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict(raw: &str) -> Result<Site, ProviderError> {
        Site::parse(raw, SitePolicy::default())
    }

    #[test]
    fn canonical_form_drops_scheme_slash_and_case() {
        for raw in [
            "Dezentralshop.ch",
            "https://dezentralshop.ch",
            "https://DEZENTRALSHOP.ch/",
            "  dezentralshop.ch/  ",
        ] {
            assert_eq!(
                strict(raw).unwrap().canonical(),
                "dezentralshop.ch",
                "{raw}"
            );
        }
        assert_eq!(
            strict("https://shop.example:8443/store/")
                .unwrap()
                .canonical(),
            "shop.example:8443/store"
        );
        assert_eq!(
            strict("shop.example:443").unwrap().canonical(),
            "shop.example"
        );
    }

    #[test]
    fn plain_http_is_refused_with_a_reason() {
        let Err(ProviderError::Invalid(message)) = strict("http://dezentralshop.ch") else {
            panic!("http must be refused");
        };
        assert!(message.contains("HTTPS"), "{message}");
    }

    #[test]
    fn refuses_what_is_not_a_public_domain() {
        for bad in [
            "",
            "localhost",
            "intranet",
            "127.0.0.1",
            "https://10.0.0.1/",
            "[::1]",
            "ftp://shop.example",
            "https://user:pass@shop.example",
            "shop.example/?x=1",
            "shop.example/#frag",
            "shop.example/a|b",
            "shop.example//x",
            "shop example",
        ] {
            assert!(strict(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn local_testing_policy_accepts_loopback_and_http() {
        let policy = SitePolicy {
            plain_http: true,
            allow_private_hosts: true,
        };
        assert_eq!(
            Site::parse("http://127.0.0.1:8080", policy)
                .unwrap()
                .canonical(),
            "127.0.0.1:8080"
        );
        assert_eq!(
            Site::parse("localhost:8443", policy).unwrap().canonical(),
            "localhost:8443"
        );
    }

    #[test]
    fn urls_are_built_from_the_canonical_form() {
        let site = strict("https://shop.example/store").unwrap();
        assert_eq!(
            site.url(false, "/wp-json/"),
            "https://shop.example/store/wp-json/"
        );
        assert_eq!(site.url(true, "/x"), "http://shop.example/store/x");
    }

    #[test]
    fn only_public_unicast_addresses_pass_the_guard() {
        for private in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "240.0.0.1",
            "198.18.0.1",
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:10.0.0.1",
            "2001:db8::1",
        ] {
            assert!(!is_public_ip(private.parse().unwrap()), "{private}");
        }
        for public in [
            "8.8.8.8",
            "1.1.1.1",
            "185.199.108.153",
            "2606:4700:4700::1111",
        ] {
            assert!(is_public_ip(public.parse().unwrap()), "{public}");
        }
    }
}
