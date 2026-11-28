//! Production FX reference handling for fiat-at-bind quotes.
//!
//! In marketplace mode the service reads the pinned Blocktank snapshot
//! (`FX_URL`), which carries a `BTC<CCY>` ticker for each of the seven fiat
//! currencies the marketplace prices in (USD, EUR, CHF, GBP, JPY, CAD, AUD).
//! In single-seller mode (`STORE_MODE=single_seller` or `STORE_SELLER_PUBKY`
//! set), the operator may choose
//! a custom Blocktank-format URL via `FX_FEED_URL` or a built-in public
//! exchange ticker adapter (`FX_FEED_PROVIDER`: `blocktank`, `kraken`,
//! `coinbase`, `mempool`, `bitstamp`). Every ticker passes the same guards
//! (unambiguous pair match, staleness, future skew, finite positive rate)
//! before a rate is used, and a quote additionally passes the
//! reference-deviation guard against that currency's own persisted median.
//! The Blocktank wire contract is captured in
//! `tests/fixtures/blocktank_fx_rates_btc.json` on 2026-09-13T16:19:07Z.
//! Paykit receives only the resulting amount; its amount contract is pinned
//! at paykit-server@67a03896b87b3df9bab78aba81ac4cfe93fca5de:
//! paykit-server/src/application/create_payment_request.rs.
//!
//! Rounding rules (all integer arithmetic, all rounding UP, so the buyer
//! pays the exact price or at most one minor unit of the settlement asset
//! more, never less):
//!
//! - fiat to sats: `ceil(total_minor * 10^8 / (10^exponent * BTC<CCY>))`
//!   ([`quote_sats`]), positive satoshis with checked integer arithmetic;
//! - fiat to USD cents (the multi-asset Paykit denomination):
//!   `ceil(total_minor * 100 * BTCUSD / (10^exponent * BTC<CCY>))`
//!   ([`usd_cents_for`]); the cross rate is the ratio of two tickers of the
//!   same snapshot.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use marketplace_domain::currency::Currency;
use serde::Deserialize;
use serde_json::value::RawValue;
use url::{Host, Url};

pub const ENV_FX_FEED_URL: &str = "FX_FEED_URL";
pub const ENV_FX_FEED_PROVIDER: &str = "FX_FEED_PROVIDER";

pub const FX_SOURCE: &str = "blocktank";
pub const FX_URL: &str = "https://api1.blocktank.to/api/fx/rates/btc";
pub const KRAKEN_FX_URL: &str = "https://api.kraken.com/0/public/Trades";
pub const COINBASE_FX_URL: &str = "https://api.coinbase.com/v2/exchange-rates?currency=BTC";
pub const MEMPOOL_FX_URL: &str = "https://mempool.space/api/v1/prices";
pub const BITSTAMP_FX_URL: &str = "https://www.bitstamp.net/api/v2/ticker/";

pub const BITSTAMP_SUPPORTED_CURRENCIES: [Currency; 3] =
    [Currency::Usd, Currency::Eur, Currency::Gbp];

const FIAT_USD_ONLY: [Currency; 1] = [Currency::Usd];
const FIAT_EUR_ONLY: [Currency; 1] = [Currency::Eur];
const FIAT_CHF_ONLY: [Currency; 1] = [Currency::Chf];
const FIAT_GBP_ONLY: [Currency; 1] = [Currency::Gbp];
const FIAT_JPY_ONLY: [Currency; 1] = [Currency::Jpy];
const FIAT_CAD_ONLY: [Currency; 1] = [Currency::Cad];
const FIAT_AUD_ONLY: [Currency; 1] = [Currency::Aud];

const fn fiat_currency_slice(currency: Currency) -> &'static [Currency] {
    match currency {
        Currency::Usd => &FIAT_USD_ONLY,
        Currency::Eur => &FIAT_EUR_ONLY,
        Currency::Chf => &FIAT_CHF_ONLY,
        Currency::Gbp => &FIAT_GBP_ONLY,
        Currency::Jpy => &FIAT_JPY_ONLY,
        Currency::Cad => &FIAT_CAD_ONLY,
        Currency::Aud => &FIAT_AUD_ONLY,
        Currency::Btc | Currency::Sat => &[],
    }
}

pub const MAX_DEVIATION_BPS: u128 = 500;
pub const MAX_RATE_AGE_SECS: i64 = 300;
pub const MAX_FUTURE_SKEW_SECS: i64 = 60;
pub const REFERENCE_WINDOW_SECS: i64 = 3_600;
pub const REFERENCE_MIN_SAMPLES: usize = 3;
pub const MAX_FRACTION_SCALE: u32 = 18;
const MAX_CURRENCY_EXPONENT: u32 = 18;
pub const MAX_FX_RESPONSE_BYTES: usize = 1024 * 1024;

const FX_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const FX_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(3);

/// Built-in FX feed wire formats supported by the sampler and bind-time quote
/// path. Marketplace mode always uses [`Self::Blocktank`] at [`FX_URL`];
/// single-seller mode may pick any provider or supply a custom `FX_FEED_URL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FxFeedProvider {
    #[default]
    Blocktank,
    Kraken,
    Coinbase,
    Mempool,
    Bitstamp,
}

impl FxFeedProvider {
    pub const ALL: [Self; 5] = [
        Self::Blocktank,
        Self::Kraken,
        Self::Coinbase,
        Self::Mempool,
        Self::Bitstamp,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blocktank => "blocktank",
            Self::Kraken => "kraken",
            Self::Coinbase => "coinbase",
            Self::Mempool => "mempool",
            Self::Bitstamp => "bitstamp",
        }
    }

    pub const fn default_url(self) -> &'static str {
        match self {
            Self::Blocktank => FX_URL,
            Self::Kraken => KRAKEN_FX_URL,
            Self::Coinbase => COINBASE_FX_URL,
            Self::Mempool => MEMPOOL_FX_URL,
            Self::Bitstamp => BITSTAMP_FX_URL,
        }
    }

    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim() {
            "blocktank" => Ok(Self::Blocktank),
            "kraken" => Ok(Self::Kraken),
            "coinbase" => Ok(Self::Coinbase),
            "mempool" => Ok(Self::Mempool),
            "bitstamp" => Ok(Self::Bitstamp),
            _ => anyhow::bail!(
                "{ENV_FX_FEED_PROVIDER} must be one of: blocktank, kraken, coinbase, mempool, bitstamp"
            ),
        }
    }

    /// Infers a built-in provider from well-known public exchange API hosts
    /// when `FX_FEED_URL` is configured without an explicit `FX_FEED_PROVIDER`.
    /// Unknown hosts default to the Blocktank JSON contract.
    pub fn infer_from_url(url: &Url) -> Self {
        match normalized_host(url).as_deref() {
            Some("api.kraken.com") => Self::Kraken,
            Some("api.coinbase.com") => Self::Coinbase,
            Some("mempool.space") => Self::Mempool,
            Some("www.bitstamp.net" | "bitstamp.net") => Self::Bitstamp,
            _ => Self::Blocktank,
        }
    }

    /// Fiat currencies quoted directly by this provider. Bitstamp publishes
    /// direct BTC markets only for USD, EUR, and GBP; the other four built-in
    /// providers cover all seven marketplace fiat currencies.
    pub const fn supported_currencies(self) -> &'static [Currency] {
        match self {
            Self::Blocktank | Self::Kraken | Self::Coinbase | Self::Mempool => &Currency::FIAT,
            Self::Bitstamp => &BITSTAMP_SUPPORTED_CURRENCIES,
        }
    }

    /// True when this provider quotes `currency` directly as a fiat FX pair.
    pub fn supports_fiat(self, currency: Currency) -> bool {
        self.supported_currencies().contains(&currency)
    }

    /// True when a listing or order denominated in `currency` is supported
    /// under this provider: every fiat currency the provider quotes, plus
    /// Bitcoin denominations (`BTC` and `SAT`), which do not need a
    /// fiat-to-BTC conversion and require only `USD` when checking USDT.
    pub fn supports_currency(self, currency: Currency) -> bool {
        currency.is_bitcoin() || self.supports_fiat(currency)
    }

    /// Comma-separated list of listing/order currency codes supported under
    /// this provider, for validation refusal messages.
    pub const fn supported_currency_codes(self) -> &'static str {
        match self {
            Self::Blocktank | Self::Kraken | Self::Coinbase | Self::Mempool => {
                marketplace_domain::currency::SUPPORTED_CODES
            }
            Self::Bitstamp => "USD, EUR, GBP, BTC, SAT",
        }
    }
}

/// Human-readable refusal message when `currency_code` is in the marketplace
/// currency registry but unsupported by the configured FX feed provider.
pub fn unsupported_currency_message_from_slice(
    provider: FxFeedProvider,
    supported_fiat: &[Currency],
    currency_code: &str,
) -> String {
    let mut codes: Vec<&str> = supported_fiat.iter().copied().map(Currency::code).collect();
    codes.push(Currency::Btc.code());
    codes.push(Currency::Sat.code());
    format!(
        "Currency {currency_code} is not supported by FX provider `{}`. Supported currencies: {}.",
        provider.as_str(),
        codes.join(", ")
    )
}

pub fn unsupported_currency_message(provider: FxFeedProvider, currency_code: &str) -> String {
    unsupported_currency_message_from_slice(
        provider,
        provider.supported_currencies(),
        currency_code,
    )
}

/// Resolved FX feed provider, endpoint URL, and the fiat currencies that URL
/// quotes (a single-pair Kraken or Bitstamp URL may quote one fiat only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FxFeedConfig {
    pub provider: FxFeedProvider,
    pub url: String,
    supported_fiat: &'static [Currency],
}

impl Default for FxFeedConfig {
    fn default() -> Self {
        Self::pinned_blocktank()
    }
}

impl FxFeedConfig {
    pub fn pinned_blocktank() -> Self {
        Self {
            provider: FxFeedProvider::Blocktank,
            url: FX_URL.to_string(),
            supported_fiat: &Currency::FIAT,
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.provider == FxFeedProvider::Blocktank && self.url == FX_URL
    }

    /// Fiat currencies quoted by this resolved `(provider, url)`.
    pub const fn supported_currencies(&self) -> &'static [Currency] {
        self.supported_fiat
    }

    pub fn supports_fiat(&self, currency: Currency) -> bool {
        self.supported_fiat.contains(&currency)
    }

    pub fn supports_currency(&self, currency: Currency) -> bool {
        currency.is_bitcoin() || self.supports_fiat(currency)
    }

    pub fn supported_currency_codes(&self) -> String {
        let mut codes: Vec<&str> = self
            .supported_fiat
            .iter()
            .copied()
            .map(Currency::code)
            .collect();
        codes.push(Currency::Btc.code());
        codes.push(Currency::Sat.code());
        codes.join(", ")
    }

    pub fn resolve(provider: FxFeedProvider, url: &str) -> anyhow::Result<Self> {
        let parsed = validate_fx_feed_url(url)?;
        validate_provider_url(provider, &parsed)?;
        let supported_fiat = supported_fiat_for(provider, &parsed)?;
        Ok(Self {
            provider,
            url: url.to_string(),
            supported_fiat,
        })
    }
}

/// Maps a Kraken `pair` query value to the fiat currency it quotes.
fn kraken_pair_currency(pair: &str) -> Option<Currency> {
    let pair = pair.trim();
    if pair.is_empty() || pair.contains(',') {
        return None;
    }
    Currency::FIAT.iter().copied().find(|currency| {
        kraken_pair_keys(*currency)
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(pair))
    })
}

/// Fiat currencies the validated `(provider, url)` quotes at bind and sample time.
pub fn supported_fiat_for(
    provider: FxFeedProvider,
    url: &Url,
) -> anyhow::Result<&'static [Currency]> {
    match provider {
        FxFeedProvider::Blocktank | FxFeedProvider::Coinbase | FxFeedProvider::Mempool => {
            Ok(&Currency::FIAT)
        }
        FxFeedProvider::Kraken => {
            let pair_values: Vec<String> = url
                .query_pairs()
                .filter(|(key, _)| key.eq_ignore_ascii_case("pair"))
                .map(|(_, value)| value.into_owned())
                .collect();
            if pair_values.is_empty() {
                Ok(&Currency::FIAT)
            } else if pair_values.len() == 1 {
                let currency = kraken_pair_currency(&pair_values[0]).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} `pair` is not a supported BTC market"
                    )
                })?;
                Ok(fiat_currency_slice(currency))
            } else {
                anyhow::bail!(
                    "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts at most one `pair` query parameter"
                )
            }
        }
        FxFeedProvider::Bitstamp => match classify_bitstamp_ticker_url(url)? {
            BitstampTickerEndpoint::AllPairs => Ok(&BITSTAMP_SUPPORTED_CURRENCIES),
            BitstampTickerEndpoint::SinglePair(currency) => Ok(fiat_currency_slice(currency)),
        },
    }
}

/// Recomputes [`Config::fx_supported_fiat`] from `fx_feed_provider` and
/// `fx_feed_url` after an in-process config override.
pub fn sync_config_fx_admission(config: &mut crate::config::Config) -> anyhow::Result<()> {
    config.fx_supported_fiat =
        FxFeedConfig::resolve(config.fx_feed_provider, &config.fx_feed_url)?.supported_fiat;
    Ok(())
}

/// FX feed admission state copied into command handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FxFeedAdmission {
    pub provider: FxFeedProvider,
    pub supported_fiat: &'static [Currency],
}

impl FxFeedAdmission {
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            provider: config.fx_feed_provider,
            supported_fiat: config.fx_supported_fiat,
        }
    }

    pub fn supports_fiat(self, currency: Currency) -> bool {
        self.supported_fiat.contains(&currency)
    }

    pub fn supports_currency(self, currency: Currency) -> bool {
        currency.is_bitcoin() || self.supports_fiat(currency)
    }

    pub fn supported_currencies(self) -> &'static [Currency] {
        self.supported_fiat
    }

    pub fn supports_all_fiat(self) -> bool {
        Currency::FIAT
            .iter()
            .all(|currency| self.supports_fiat(*currency))
    }
}

/// Boot error when upstream multi-asset Paykit needs a `BTCUSD` leg on every
/// non-USD fiat bind and the configured FX URL quotes only one other fiat.
pub const MULTI_ASSET_REQUIRES_USD_FX: &str =
    "PAYKIT_MARKETPLACE_CONTRACT=multi_asset requires USD in the configured FX feed's supported currencies";

/// Refuses `multi_asset` when the resolved FX feed does not quote USD.
pub fn ensure_multi_asset_fx_admission(
    contract: crate::payment_attempt::MarketplaceContract,
    admission: FxFeedAdmission,
) -> anyhow::Result<()> {
    if contract.is_multi_asset() && !admission.supports_fiat(Currency::Usd) {
        anyhow::bail!(MULTI_ASSET_REQUIRES_USD_FX);
    }
    Ok(())
}

/// Boot error when the effective region policy can run EU C4 annual-limit or
/// private-seller EU re-confirmation checks (which convert non-EUR orders and
/// paid history through `eur_cents_reference`) and the configured FX URL does
/// not quote EUR.
pub const EU_LIMITS_REQUIRE_EUR_FX: &str =
    "EU annual-limit and re-confirmation policy (EU_FENCE_ENABLED=true or a nuanced EU-27 country in REGION_MODES) requires EUR in the configured FX feed's supported currencies";

/// Refuses a deployment whose effective region policy can run EU C4 annual
/// limits or private-seller EU re-confirmation when the resolved FX feed does
/// not quote EUR. Open-only and blocked-only region modes do not run C4 and
/// therefore do not require EUR.
pub fn ensure_region_policy_fx_admission(
    policy: crate::region_policy::RegionPolicy<'_>,
    admission: FxFeedAdmission,
) -> anyhow::Result<()> {
    if policy.eu_limits_active() && !admission.supports_fiat(Currency::Eur) {
        anyhow::bail!(EU_LIMITS_REQUIRE_EUR_FX);
    }
    Ok(())
}

fn is_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip == Ipv4Addr::LOCALHOST,
        Some(Host::Ipv6(ip)) => ip == Ipv6Addr::LOCALHOST,
        None => false,
    }
}

fn is_allowed_fx_ip(ip: IpAddr, allow_loopback: bool) -> bool {
    if allow_loopback {
        ip.is_loopback()
    } else {
        crate::homeserver_transport::is_public(ip)
    }
}

fn validate_fx_feed_url(raw: &str) -> anyhow::Result<Url> {
    if raw.is_empty() || raw.trim() != raw {
        anyhow::bail!("{ENV_FX_FEED_URL} must be an https URL without surrounding whitespace");
    }
    let parsed = Url::parse(raw)
        .map_err(|_| anyhow::anyhow!("{ENV_FX_FEED_URL} must be a valid https URL"))?;
    if parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        anyhow::bail!(
            "{ENV_FX_FEED_URL} must be an https URL with a host and no credentials or fragment"
        );
    }
    let loopback = is_loopback_host(&parsed);
    match parsed.scheme() {
        "https" => {}
        "http" if loopback => {}
        _ => anyhow::bail!(
            "{ENV_FX_FEED_URL} must use https (http is allowed only for loopback 127.0.0.1, \
             localhost, or [::1])"
        ),
    }
    match parsed.host() {
        Some(Host::Ipv4(ip)) if !is_allowed_fx_ip(IpAddr::V4(ip), loopback) => {
            anyhow::bail!(
                "{ENV_FX_FEED_URL} must point to a public address (or loopback for local feeds)"
            );
        }
        Some(Host::Ipv6(ip)) if !is_allowed_fx_ip(IpAddr::V6(ip), loopback) => {
            anyhow::bail!(
                "{ENV_FX_FEED_URL} must point to a public address (or loopback for local feeds)"
            );
        }
        _ => {}
    }
    Ok(parsed)
}

/// Resolves `FX_FEED_PROVIDER` and `FX_FEED_URL` in the deployment's store
/// mode. Marketplace mode stays pinned to Blocktank (`FX_URL`) and refuses
/// custom feeds; single-seller mode honors operator-configured URLs and
/// built-in public exchange ticker adapters.
pub fn fx_feed_from_lookup(
    get: &dyn Fn(&str) -> Option<String>,
    store_mode: &crate::store_seller::StoreMode,
) -> anyhow::Result<FxFeedConfig> {
    let raw_provider = get(ENV_FX_FEED_PROVIDER).filter(|value| !value.is_empty());
    let raw_url = get(ENV_FX_FEED_URL).filter(|value| !value.is_empty());

    let explicit_provider = match raw_provider {
        None => None,
        Some(raw) => Some(FxFeedProvider::parse(&raw)?),
    };
    let explicit_url = match raw_url {
        None => None,
        Some(raw) => Some(validate_fx_feed_url(&raw)?),
    };

    if !store_mode.is_single_seller() {
        if let Some(provider) = explicit_provider {
            if provider != FxFeedProvider::Blocktank {
                anyhow::bail!(
                    "{ENV_FX_FEED_PROVIDER} other than `blocktank` is allowed only in \
                     single-seller mode (STORE_MODE=single_seller or STORE_SELLER_PUBKY set)"
                );
            }
        }
        if let Some(url) = &explicit_url {
            if url.as_str() != FX_URL {
                anyhow::bail!(
                    "{ENV_FX_FEED_URL} other than the pinned Blocktank feed is allowed only in \
                     single-seller mode (STORE_MODE=single_seller or STORE_SELLER_PUBKY set)"
                );
            }
        }
        return Ok(FxFeedConfig::pinned_blocktank());
    }

    let (provider, url) = match (explicit_provider, explicit_url) {
        (None, None) => (FxFeedProvider::Blocktank, FX_URL.to_string()),
        (Some(provider), None) => (provider, provider.default_url().to_string()),
        (None, Some(url)) => {
            let provider = FxFeedProvider::infer_from_url(&url);
            validate_provider_url(provider, &url)?;
            (provider, url.into())
        }
        (Some(provider), Some(url)) => {
            validate_provider_url(provider, &url)?;
            (provider, url.into())
        }
    };
    let parsed = Url::parse(&url)?;
    let supported_fiat = supported_fiat_for(provider, &parsed)?;
    Ok(FxFeedConfig {
        provider,
        url,
        supported_fiat,
    })
}

fn validate_provider_url(provider: FxFeedProvider, url: &Url) -> anyhow::Result<()> {
    match provider {
        FxFeedProvider::Kraken => {
            validate_kraken_trades_path(url)?;
            validate_kraken_trades_query(url)?;
        }
        FxFeedProvider::Bitstamp => {
            classify_bitstamp_ticker_url(url)?;
        }
        FxFeedProvider::Blocktank => {
            validate_blocktank_path(url)?;
            if url.query().is_some_and(|query| !query.is_empty()) {
                anyhow::bail!("Blocktank {ENV_FX_FEED_URL} must not set query parameters");
            }
        }
        FxFeedProvider::Coinbase => {
            validate_coinbase_exchange_rates_path(url)?;
            validate_coinbase_exchange_rates_query(url)?;
        }
        FxFeedProvider::Mempool => {
            validate_mempool_path(url)?;
            if url.query().is_some_and(|query| !query.is_empty()) {
                anyhow::bail!("Mempool {ENV_FX_FEED_URL} must not set query parameters");
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    pub mantissa: u128,
    pub scale: u32,
}

/// Checked 320-bit unsigned integer (little-endian limbs) used for exact
/// rational FX ceiling and cross-rate arithmetic at up to 18 fractional digits
/// without external dependencies or intermediate overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct U320([u64; 5]);

impl Ord for U320 {
    fn cmp(&self, other: &Self) -> Ordering {
        for i in (0..5).rev() {
            match self.0[i].cmp(&other.0[i]) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for U320 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl U320 {
    const ZERO: Self = Self([0; 5]);

    const fn from_u64(value: u64) -> Self {
        Self([value, 0, 0, 0, 0])
    }

    const fn from_u128(value: u128) -> Self {
        Self([value as u64, (value >> 64) as u64, 0, 0, 0])
    }

    const fn is_zero(self) -> bool {
        self.0[0] == 0 && self.0[1] == 0 && self.0[2] == 0 && self.0[3] == 0 && self.0[4] == 0
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        let mut out = [0u64; 5];
        let mut carry = 0u128;
        for (slot, (&a, &b)) in out.iter_mut().zip(self.0.iter().zip(rhs.0.iter())) {
            let sum = u128::from(a) + u128::from(b) + carry;
            *slot = sum as u64;
            carry = sum >> 64;
        }
        (carry == 0).then_some(Self(out))
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        let mut out = [0u64; 5];
        let mut borrow = 0u64;
        for (slot, (&a, &b)) in out.iter_mut().zip(self.0.iter().zip(rhs.0.iter())) {
            let (d1, b1) = a.overflowing_sub(b);
            let (d2, b2) = d1.overflowing_sub(borrow);
            *slot = d2;
            borrow = u64::from(b1 || b2);
        }
        (borrow == 0).then_some(Self(out))
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        let mut acc = [0u64; 5];
        for i in 0..5 {
            let mut carry = 0u128;
            for j in 0..5 {
                let prod = u128::from(self.0[i]) * u128::from(rhs.0[j]);
                if i + j < 5 {
                    let sum = u128::from(acc[i + j]) + prod + carry;
                    acc[i + j] = sum as u64;
                    carry = sum >> 64;
                } else if prod != 0 || carry != 0 {
                    return None;
                }
            }
            if carry != 0 {
                return None;
            }
        }
        Some(Self(acc))
    }

    fn pow10(exp: u32) -> Option<Self> {
        let mut value = Self::from_u64(1);
        let ten = Self::from_u64(10);
        for _ in 0..exp {
            value = value.checked_mul(ten)?;
        }
        Some(value)
    }

    fn shr1(self) -> Self {
        let mut out = [0u64; 5];
        let mut carry = 0u64;
        for i in (0..5).rev() {
            out[i] = (self.0[i] >> 1) | (carry << 63);
            carry = self.0[i] & 1;
        }
        Self(out)
    }

    fn bit(self, idx: usize) -> bool {
        ((self.0[idx / 64] >> (idx % 64)) & 1) != 0
    }

    fn set_bit(&mut self, idx: usize) {
        self.0[idx / 64] |= 1u64 << (idx % 64);
    }

    fn div_rem(self, rhs: Self) -> Option<(Self, Self)> {
        if rhs.is_zero() {
            return None;
        }
        let mut q = Self::ZERO;
        let mut r = Self::ZERO;
        for bit in (0..320).rev() {
            let mut carry = u64::from(self.bit(bit));
            for limb in &mut r.0 {
                let next_carry = *limb >> 63;
                *limb = (*limb << 1) | carry;
                carry = next_carry;
            }
            if carry != 0 || r >= rhs {
                let mut borrow = 0u64;
                for i in 0..5 {
                    let (d1, b1) = r.0[i].overflowing_sub(rhs.0[i]);
                    let (d2, b2) = d1.overflowing_sub(borrow);
                    r.0[i] = d2;
                    borrow = u64::from(b1 || b2);
                }
                q.set_bit(bit);
            }
        }
        Some((q, r))
    }

    fn div_ceil(self, rhs: Self) -> Option<Self> {
        let (q, r) = self.div_rem(rhs)?;
        if r.is_zero() {
            Some(q)
        } else {
            q.checked_add(Self::from_u64(1))
        }
    }

    fn to_u64(self) -> Option<u64> {
        (self.0[1] == 0 && self.0[2] == 0 && self.0[3] == 0 && self.0[4] == 0).then_some(self.0[0])
    }

    fn to_decimal_string(mut self) -> String {
        if self.is_zero() {
            return "0".to_string();
        }
        let mut digits = Vec::new();
        while !self.is_zero() {
            let mut rem = 0u128;
            for i in (0..5).rev() {
                let cur = (rem << 64) | u128::from(self.0[i]);
                self.0[i] = (cur / 10) as u64;
                rem = cur % 10;
            }
            digits.push(b'0' + rem as u8);
        }
        digits.reverse();
        String::from_utf8(digits).expect("ASCII decimal digits")
    }
}

impl Decimal {
    pub fn parse(value: &str) -> Option<Self> {
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        if whole.is_empty()
            || whole.len() > 38
            || !whole.chars().all(|c| c.is_ascii_digit())
            || !fraction.chars().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        // The fraction is capped at MAX_FRACTION_SCALE (18 digits) so exact
        // exchange precision is preserved while keeping widening and U320
        // powers bounded. Trailing zeros past MAX_FRACTION_SCALE (for example
        // from base-10000 PostgreSQL NUMERIC decoding) are stripped losslessly;
        // any non-zero digit past MAX_FRACTION_SCALE fails closed.
        let max_frac = MAX_FRACTION_SCALE as usize;
        let kept_fraction = if fraction.len() > max_frac {
            if fraction[max_frac..].bytes().any(|b| b != b'0') {
                return None;
            }
            &fraction[..max_frac]
        } else {
            fraction
        };
        let digits = format!("{whole}{kept_fraction}");
        let mantissa = digits.parse().ok()?;
        Some(Self {
            mantissa,
            scale: kept_fraction.len().try_into().ok()?,
        })
    }

    /// Parses an unsigned decimal string from a public exchange feed without
    /// ever silently truncating non-zero digits. Up to [`MAX_FRACTION_SCALE`]
    /// (18) fractional digits are kept losslessly; trailing zeros past
    /// [`MAX_FRACTION_SCALE`] are stripped cleanly, and any non-zero digit
    /// past [`MAX_FRACTION_SCALE`] fails closed (`None`).
    fn parse_exchange_value(value: &str) -> Option<Self> {
        if value.len() > 64 {
            return None;
        }
        Self::parse(value)
    }

    /// The mantissa widened to `scale` fractional digits, checked: an
    /// over-wide value returns `None` so callers fail closed instead of
    /// wrapping.
    fn widened(&self, scale: u32) -> Option<u128> {
        let factor = 10u128.checked_pow(scale.checked_sub(self.scale)?)?;
        self.mantissa.checked_mul(factor)
    }

    fn cmp(&self, other: &Self) -> Option<Ordering> {
        let scale = self.scale.max(other.scale);
        let left = self.widened(scale)?;
        let right = other.widened(scale)?;
        Some(left.cmp(&right))
    }

    /// True when the rate is strictly positive and finite within checked
    /// widening precision (`MAX_FRACTION_SCALE`). Replaces static market
    /// price ceilings and floors; staleness and multi-sample deviation still
    /// guard every quote.
    pub fn is_valid_rate(&self) -> bool {
        self.mantissa > 0
            && self.scale <= MAX_FRACTION_SCALE
            && self.widened(MAX_FRACTION_SCALE).is_some()
    }

    pub fn to_string_value(&self) -> String {
        if self.scale == 0 {
            return self.mantissa.to_string();
        }
        let scale = self.scale as usize;
        let digits = format!("{:0width$}", self.mantissa, width = scale + 1);
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    }
}

/// Parses an unsigned decimal from a validated JSON raw token (string or
/// number) directly from its lexical digits without an `f64` intermediate.
fn parse_raw_decimal(raw: &str) -> Option<Decimal> {
    let token = raw.trim();
    if token.starts_with('"') {
        let decoded: String = serde_json::from_str(token).ok()?;
        return Decimal::parse_exchange_value(&decoded);
    }
    Decimal::parse_exchange_value(token)
}

/// Parses an integer Unix timestamp (seconds) from a validated JSON raw token
/// (string or integer number) without floating-point conversion.
fn parse_raw_unix_seconds(raw: &str) -> Option<i64> {
    let token = raw.trim();
    if token.starts_with('"') {
        let decoded: String = serde_json::from_str(token).ok()?;
        return decoded.parse::<i64>().ok();
    }
    token.parse::<i64>().ok()
}

/// Parses an integer or fractional Unix timestamp in seconds (such as Kraken
/// trade execution time `1795829462.0336654`) directly from a validated JSON
/// raw token into an exact UTC timestamp without floating-point arithmetic.
fn parse_raw_unix_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    let token = raw.trim();
    let owned;
    let text = if token.starts_with('"') {
        owned = serde_json::from_str::<String>(token).ok()?;
        owned.trim()
    } else {
        token
    };
    if text.is_empty() || text.len() > 32 {
        return None;
    }
    let (secs_str, frac_str) = match text.split_once('.') {
        Some((secs, frac)) => {
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (secs, frac)
        }
        None => (text, ""),
    };
    if secs_str.is_empty() || !secs_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let secs: i64 = secs_str.parse().ok()?;
    let nanos: u32 = if frac_str.is_empty() {
        0
    } else {
        let mut padded = [b'0'; 9];
        for (idx, byte) in frac_str.bytes().take(9).enumerate() {
            padded[idx] = byte;
        }
        std::str::from_utf8(&padded).ok()?.parse().ok()?
    };
    DateTime::from_timestamp(secs, nanos)
}

fn check_freshness(updated_at: DateTime<Utc>, observed_at: DateTime<Utc>) -> Result<(), FxError> {
    let age = observed_at.signed_duration_since(updated_at);
    if age > Duration::seconds(MAX_RATE_AGE_SECS) {
        return Err(FxError::Stale);
    }
    if age < -Duration::seconds(MAX_FUTURE_SKEW_SECS) {
        return Err(FxError::Future);
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Feed {
    tickers: Vec<Ticker>,
}

#[derive(Debug, Deserialize)]
struct Ticker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
    base: String,
    quote: String,
    #[serde(rename = "lastUpdatedAt")]
    last_updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateSample {
    pub rate: Decimal,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FxError {
    Malformed,
    MissingReference,
    Stale,
    Future,
    OutOfBounds,
    DeviationExceeded,
    Overflow,
    BelowMinimum,
    UnsupportedCurrency,
}

/// Every ticker of one feed response, each fiat currency validated on its
/// own: a stale or missing JPY ticker never blocks a USD quote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FxSnapshot {
    supported_fiat: &'static [Currency],
    samples: Vec<(Currency, Result<RateSample, FxError>)>,
}

impl FxSnapshot {
    /// The guarded rate sample of `currency`: `UnsupportedCurrency` when the
    /// feed URL does not quote `currency`, `Malformed` when the feed
    /// carried no (or more than one) ticker for a supported currency, or the
    /// ticker's own refusal when it failed a guard.
    pub fn sample(&self, currency: Currency) -> Result<RateSample, FxError> {
        if let Some((_, sample)) = self
            .samples
            .iter()
            .find(|(candidate, _)| *candidate == currency)
        {
            return sample.clone();
        }
        if currency.is_fiat() && !self.supported_fiat.contains(&currency) {
            return Err(FxError::UnsupportedCurrency);
        }
        Err(FxError::Malformed)
    }

    /// Every supported fiat currency with its validation outcome.
    pub fn samples(&self) -> impl Iterator<Item = (Currency, &Result<RateSample, FxError>)> {
        self.samples
            .iter()
            .map(|(currency, sample)| (*currency, sample))
    }
}

fn parse_blocktank_ticker(
    feed: &Feed,
    currency: Currency,
    observed_at: DateTime<Utc>,
) -> Result<RateSample, FxError> {
    let symbol = currency.feed_symbol().ok_or(FxError::Malformed)?;
    let ticker = feed
        .tickers
        .iter()
        .filter(|ticker| {
            ticker.symbol == symbol && ticker.base == "BTC" && ticker.quote == currency.code()
        })
        .collect::<Vec<_>>();
    if ticker.len() != 1 {
        return Err(FxError::Malformed);
    }
    let ticker = ticker[0];
    let updated_at =
        DateTime::from_timestamp_millis(ticker.last_updated_at).ok_or(FxError::Malformed)?;
    check_freshness(updated_at, observed_at)?;
    let rate = Decimal::parse(&ticker.last_price).ok_or(FxError::Malformed)?;
    if !rate.is_valid_rate() {
        return Err(FxError::OutOfBounds);
    }
    Ok(RateSample {
        rate,
        fetched_at: updated_at,
    })
}

fn kraken_primary_pair(currency: Currency) -> Option<&'static str> {
    Some(match currency {
        Currency::Usd => "XXBTZUSD",
        Currency::Eur => "XXBTZEUR",
        Currency::Chf => "XBTCHF",
        Currency::Gbp => "XXBTZGBP",
        Currency::Jpy => "XXBTZJPY",
        Currency::Cad => "XXBTZCAD",
        Currency::Aud => "XBTAUD",
        Currency::Btc | Currency::Sat => return None,
    })
}

fn kraken_pair_keys(currency: Currency) -> &'static [&'static str] {
    match currency {
        Currency::Usd => &["XXBTZUSD", "XBTUSD"],
        Currency::Eur => &["XXBTZEUR", "XBTEUR"],
        Currency::Chf => &["XBTCHF", "XXBTZCHF"],
        Currency::Gbp => &["XXBTZGBP", "XBTGBP"],
        Currency::Jpy => &["XXBTZJPY", "XBTJPY"],
        Currency::Cad => &["XXBTZCAD", "XBTCAD"],
        Currency::Aud => &["XBTAUD", "XXBTZAUD"],
        Currency::Btc | Currency::Sat => &[],
    }
}

/// JSON object map that rejects duplicate keys fail-closed instead of silently
/// keeping the last value.
#[derive(Debug)]
struct UniqueRawObject(HashMap<String, Box<RawValue>>);

impl<'de> Deserialize<'de> for UniqueRawObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueRawObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for UniqueRawObjectVisitor {
            type Value = UniqueRawObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object without duplicate keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((key, value)) = access.next_entry::<String, Box<RawValue>>()? {
                    if map.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                    map.insert(key, value);
                }
                Ok(UniqueRawObject(map))
            }
        }

        deserializer.deserialize_map(UniqueRawObjectVisitor)
    }
}

/// Bitstamp array item object that tracks duplicate keys and every `"pair"`
/// value seen so unrelated malformed or duplicate-key altcoins remain isolated,
/// while any matched `BTC/<CCY>` entry with a duplicate key fails closed.
#[derive(Debug)]
struct BitstampArrayObject {
    fields: HashMap<String, Box<RawValue>>,
    pairs: Vec<String>,
    has_duplicate_key: bool,
}

impl<'de> Deserialize<'de> for BitstampArrayObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BitstampArrayObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for BitstampArrayObjectVisitor {
            type Value = BitstampArrayObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Bitstamp market JSON object")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut fields = HashMap::with_capacity(access.size_hint().unwrap_or(0));
                let mut pairs = Vec::new();
                let mut has_duplicate_key = false;
                while let Some((key, value)) = access.next_entry::<String, Box<RawValue>>()? {
                    if key == "pair" {
                        if let Ok(pair) = serde_json::from_str::<String>(value.get()) {
                            pairs.push(pair);
                        }
                    }
                    if fields.contains_key(&key) {
                        has_duplicate_key = true;
                    }
                    fields.insert(key, value);
                }
                Ok(BitstampArrayObject {
                    fields,
                    pairs,
                    has_duplicate_key,
                })
            }
        }

        deserializer.deserialize_map(BitstampArrayObjectVisitor)
    }
}

fn kraken_canonical_pair_url(base: &Url, pair: &str) -> Url {
    let mut url = base.clone();
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.append_pair("pair", pair);
        query.append_pair("count", "1");
    }
    url
}

fn kraken_pair_url(base: &Url, currency: Currency) -> Result<Url, FxError> {
    let pair = kraken_primary_pair(currency).ok_or(FxError::Malformed)?;
    Ok(kraken_canonical_pair_url(base, pair))
}

#[derive(Debug, Deserialize)]
struct KrakenTradesEnvelope {
    error: Vec<Box<RawValue>>,
    result: Option<UniqueRawObject>,
}

fn parse_kraken_envelope(body: &[u8]) -> Result<HashMap<String, Box<RawValue>>, FxError> {
    let payload: KrakenTradesEnvelope =
        serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    if !payload.error.is_empty() {
        return Err(FxError::Malformed);
    }
    payload.result.map(|obj| obj.0).ok_or(FxError::Malformed)
}

fn parse_kraken_trade_entry(fields: &[Box<RawValue>]) -> Result<(Decimal, DateTime<Utc>), FxError> {
    if fields.len() < 3 {
        return Err(FxError::Malformed);
    }
    let rate = parse_raw_decimal(fields[0].get()).ok_or(FxError::Malformed)?;
    let trade_ts = parse_raw_unix_timestamp(fields[2].get()).ok_or(FxError::Malformed)?;
    Ok((rate, trade_ts))
}

fn parse_kraken_pair_trades(
    result: &HashMap<String, Box<RawValue>>,
    currency: Currency,
    observed_at: DateTime<Utc>,
) -> Result<RateSample, FxError> {
    let keys = kraken_pair_keys(currency);
    let matched: Vec<&RawValue> = result
        .iter()
        .filter(|(key, _)| keys.contains(&key.as_str()))
        .map(|(_, value)| value.as_ref())
        .collect();
    if matched.len() != 1 {
        return Err(FxError::Malformed);
    }
    let trades: Vec<Vec<Box<RawValue>>> =
        serde_json::from_str(matched[0].get()).map_err(|_| FxError::Malformed)?;
    if trades.is_empty() {
        return Err(FxError::Malformed);
    }
    let mut latest: Option<(Decimal, DateTime<Utc>)> = None;
    for entry in &trades {
        let (rate, trade_ts) = parse_kraken_trade_entry(entry)?;
        if let Some((_, prev_ts)) = latest {
            if trade_ts < prev_ts {
                return Err(FxError::Malformed);
            }
        }
        latest = Some((rate, trade_ts));
    }
    let (rate, trade_ts) = latest.ok_or(FxError::Malformed)?;
    check_freshness(trade_ts, observed_at)?;
    if !rate.is_valid_rate() {
        return Err(FxError::OutOfBounds);
    }
    Ok(RateSample {
        rate,
        fetched_at: trade_ts,
    })
}

fn parse_kraken_snapshot(
    body: &[u8],
    observed_at: DateTime<Utc>,
    supported_fiat: &'static [Currency],
) -> Result<FxSnapshot, FxError> {
    let result = parse_kraken_envelope(body)?;
    let samples = supported_fiat
        .iter()
        .copied()
        .map(|currency| {
            (
                currency,
                parse_kraken_pair_trades(&result, currency, observed_at),
            )
        })
        .collect();
    Ok(FxSnapshot {
        supported_fiat,
        samples,
    })
}

#[derive(Debug, Deserialize)]
struct CoinbaseEnvelope {
    data: CoinbaseData,
}

#[derive(Debug, Deserialize)]
struct CoinbaseData {
    currency: Option<String>,
    base: Option<String>,
    rates: Option<UniqueRawObject>,
    amount: Option<Box<RawValue>>,
}

fn parse_coinbase_snapshot(
    body: &[u8],
    observed_at: DateTime<Utc>,
    header_date: Option<DateTime<Utc>>,
    supported_fiat: &'static [Currency],
) -> Result<FxSnapshot, FxError> {
    let payload: CoinbaseEnvelope = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    let data = payload.data;
    let updated_at = header_date.ok_or(FxError::Malformed)?;
    check_freshness(updated_at, observed_at)?;

    if let Some(UniqueRawObject(rates)) = data.rates {
        if data.currency.as_deref() != Some("BTC") {
            return Err(FxError::Malformed);
        }
        let samples = supported_fiat
            .iter()
            .copied()
            .map(|currency| {
                let sample = match rates
                    .get(currency.code())
                    .and_then(|raw| parse_raw_decimal(raw.get()))
                {
                    None => Err(FxError::Malformed),
                    Some(rate) if !rate.is_valid_rate() => Err(FxError::OutOfBounds),
                    Some(rate) => Ok(RateSample {
                        rate,
                        fetched_at: updated_at,
                    }),
                };
                (currency, sample)
            })
            .collect();
        return Ok(FxSnapshot {
            supported_fiat,
            samples,
        });
    }

    if data.base.as_deref() == Some("BTC") {
        let quote_code = data.currency.as_deref().ok_or(FxError::Malformed)?;
        let amount_value = data.amount.as_deref().ok_or(FxError::Malformed)?;
        let samples = supported_fiat
            .iter()
            .copied()
            .map(|currency| {
                let sample = if currency.code() == quote_code {
                    match parse_raw_decimal(amount_value.get()) {
                        None => Err(FxError::Malformed),
                        Some(rate) if !rate.is_valid_rate() => Err(FxError::OutOfBounds),
                        Some(rate) => Ok(RateSample {
                            rate,
                            fetched_at: updated_at,
                        }),
                    }
                } else {
                    Err(FxError::Malformed)
                };
                (currency, sample)
            })
            .collect();
        return Ok(FxSnapshot {
            supported_fiat,
            samples,
        });
    }

    Err(FxError::Malformed)
}

fn parse_mempool_snapshot(
    body: &[u8],
    observed_at: DateTime<Utc>,
    supported_fiat: &'static [Currency],
) -> Result<FxSnapshot, FxError> {
    let UniqueRawObject(object) = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    let timestamp = object
        .get("time")
        .and_then(|raw| parse_raw_unix_seconds(raw.get()))
        .ok_or(FxError::Malformed)?;
    let updated_at = DateTime::from_timestamp(timestamp, 0).ok_or(FxError::Malformed)?;
    check_freshness(updated_at, observed_at)?;

    let samples = supported_fiat
        .iter()
        .copied()
        .map(|currency| {
            let sample = match object
                .get(currency.code())
                .and_then(|raw| parse_raw_decimal(raw.get()))
            {
                None => Err(FxError::Malformed),
                Some(rate) if !rate.is_valid_rate() => Err(FxError::OutOfBounds),
                Some(rate) => Ok(RateSample {
                    rate,
                    fetched_at: updated_at,
                }),
            };
            (currency, sample)
        })
        .collect();
    Ok(FxSnapshot {
        supported_fiat,
        samples,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitstampTickerEndpoint {
    AllPairs,
    SinglePair(Currency),
}

fn classify_bitstamp_ticker_url(url: &Url) -> Result<BitstampTickerEndpoint, anyhow::Error> {
    if url.query().is_some_and(|query| !query.is_empty()) {
        anyhow::bail!("Bitstamp {ENV_FX_FEED_URL} must not set query parameters");
    }
    let segments: Vec<&str> = url
        .path_segments()
        .ok_or_else(|| anyhow::anyhow!("Bitstamp {ENV_FX_FEED_URL} must be a valid URL path"))?
        .filter(|segment| !segment.is_empty())
        .collect();
    match segments.as_slice() {
        ["api", "v2", "ticker"] => Ok(BitstampTickerEndpoint::AllPairs),
        ["api", "v2", "ticker", pair] => {
            let pair = pair.to_ascii_lowercase();
            if let Some(quote) = pair.strip_prefix("btc") {
                if let Some(currency) =
                    Currency::parse(&quote.to_ascii_uppercase()).filter(|c| c.is_fiat())
                {
                    if BITSTAMP_SUPPORTED_CURRENCIES.contains(&currency) {
                        return Ok(BitstampTickerEndpoint::SinglePair(currency));
                    }
                    anyhow::bail!(
                        "Bitstamp does not publish a BTC/{} market; supported Bitstamp fiat \
                         currencies are USD, EUR, and GBP",
                        currency.code()
                    );
                }
            }
            anyhow::bail!(
                "Bitstamp {ENV_FX_FEED_URL} must use /api/v2/ticker, /api/v2/ticker/, or \
                 /api/v2/ticker/btcusd/, /api/v2/ticker/btceur/, or /api/v2/ticker/btcgbp/"
            );
        }
        _ => anyhow::bail!(
            "Bitstamp {ENV_FX_FEED_URL} must use /api/v2/ticker, /api/v2/ticker/, or \
             /api/v2/ticker/btcusd/, /api/v2/ticker/btceur/, or /api/v2/ticker/btcgbp/"
        ),
    }
}

fn normalized_host(url: &Url) -> Option<String> {
    url.host_str()
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
}

fn is_official_blocktank_host(url: &Url) -> bool {
    normalized_host(url).as_deref() == Some("api1.blocktank.to")
}

fn is_official_coinbase_host(url: &Url) -> bool {
    normalized_host(url).as_deref() == Some("api.coinbase.com")
}

fn is_official_mempool_host(url: &Url) -> bool {
    normalized_host(url).as_deref() == Some("mempool.space")
}

fn fx_url_path_segments(url: &Url) -> Vec<&str> {
    url.path_segments()
        .map(|segments| segments.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default()
}

fn validate_blocktank_path(url: &Url) -> anyhow::Result<()> {
    if is_official_blocktank_host(url) {
        let segments = fx_url_path_segments(url);
        if segments != ["api", "fx", "rates", "btc"] {
            anyhow::bail!("Blocktank {ENV_FX_FEED_URL} must use the /api/fx/rates/btc endpoint");
        }
    }
    Ok(())
}

fn validate_mempool_path(url: &Url) -> anyhow::Result<()> {
    if is_official_mempool_host(url) || is_loopback_host(url) {
        let segments = fx_url_path_segments(url);
        if segments != ["api", "v1", "prices"] {
            anyhow::bail!("Mempool {ENV_FX_FEED_URL} must use the /api/v1/prices endpoint");
        }
    }
    Ok(())
}

fn validate_kraken_trades_query(url: &Url) -> anyhow::Result<()> {
    let mut saw_pair = false;
    let mut saw_count = false;
    for (key, value) in url.query_pairs() {
        if key.is_empty() || value.is_empty() {
            anyhow::bail!(
                "Kraken /0/public/Trades {ENV_FX_FEED_URL} query keys and values must be non-empty"
            );
        }
        match key.as_ref() {
            "pair" => {
                if saw_pair {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts at most one `pair` query \
                         parameter (omit `pair` to query all supported pairs)"
                    );
                }
                saw_pair = true;
                if value.contains(',') {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts at most one non-empty \
                         `pair` query parameter (omit `pair` to query all supported pairs)"
                    );
                }
                if kraken_pair_currency(&value).is_none() {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} `pair` is not a supported BTC market"
                    );
                }
            }
            "count" => {
                if saw_count {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts at most one `count` query \
                         parameter and its value must be `1`"
                    );
                }
                saw_count = true;
                if value != "1" {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts only `count=1` when `count` \
                         is set"
                    );
                }
            }
            _ => {
                if key.eq_ignore_ascii_case("since") {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} must not set `since` (historical \
                         pagination does not return the most recent trade)"
                    );
                }
                if key.eq_ignore_ascii_case("pair") || key.eq_ignore_ascii_case("count") {
                    anyhow::bail!(
                        "Kraken /0/public/Trades {ENV_FX_FEED_URL} query keys must use canonical \
                         lowercase `pair` and `count`"
                    );
                }
                anyhow::bail!(
                    "Kraken /0/public/Trades {ENV_FX_FEED_URL} accepts only `pair` and optional \
                     `count=1` query parameters"
                );
            }
        }
    }
    Ok(())
}

fn validate_coinbase_exchange_rates_query(url: &Url) -> anyhow::Result<()> {
    let mut saw_currency = false;
    for (key, value) in url.query_pairs() {
        if key.is_empty() || value.is_empty() {
            anyhow::bail!("Coinbase {ENV_FX_FEED_URL} query keys and values must be non-empty");
        }
        if key == "currency" {
            if saw_currency {
                anyhow::bail!(
                    "Coinbase {ENV_FX_FEED_URL} accepts at most one `currency` query parameter"
                );
            }
            saw_currency = true;
            if value != "BTC" {
                anyhow::bail!(
                    "Coinbase {ENV_FX_FEED_URL} requires `currency=BTC` on api.coinbase.com \
                     (custom hosts may omit the query when they default to BTC)"
                );
            }
        } else if key.eq_ignore_ascii_case("currency") {
            anyhow::bail!(
                "Coinbase {ENV_FX_FEED_URL} query keys must use canonical lowercase `currency`"
            );
        } else {
            anyhow::bail!(
                "Coinbase {ENV_FX_FEED_URL} accepts only `currency=BTC` \
                 (required on api.coinbase.com; optional on custom hosts that default to BTC)"
            );
        }
    }
    if is_official_coinbase_host(url) && !saw_currency {
        anyhow::bail!(
            "Coinbase {ENV_FX_FEED_URL} on api.coinbase.com requires `?currency=BTC` \
             (Coinbase defaults an empty query to USD)"
        );
    }
    Ok(())
}

fn validate_kraken_trades_path(url: &Url) -> anyhow::Result<()> {
    let segments: Vec<&str> = url
        .path_segments()
        .map(|path| path.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default();
    let canonical = segments == ["0", "public", "Trades"];
    let loopback_trades = is_loopback_host(url)
        && segments
            .last()
            .is_some_and(|segment| segment.eq_ignore_ascii_case("Trades"));
    if segments
        .last()
        .is_some_and(|segment| segment.eq_ignore_ascii_case("ticker"))
    {
        anyhow::bail!(
            "Kraken {ENV_FX_FEED_URL} must use the price-timestamped /0/public/Trades \
             endpoint rather than /0/public/Ticker"
        );
    }
    if !canonical && !loopback_trades {
        anyhow::bail!("Kraken {ENV_FX_FEED_URL} must use the /0/public/Trades endpoint");
    }
    Ok(())
}

fn validate_coinbase_exchange_rates_path(url: &Url) -> anyhow::Result<()> {
    let segments: Vec<&str> = url
        .path_segments()
        .map(|path| path.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default();
    let ok = if is_loopback_host(url) {
        segments == ["exchange-rates"] || segments == ["v2", "exchange-rates"]
    } else {
        segments == ["v2", "exchange-rates"]
    };
    if !ok {
        anyhow::bail!("Coinbase {ENV_FX_FEED_URL} must use the /v2/exchange-rates endpoint");
    }
    Ok(())
}

/// Extracts a registry fiat currency from a validated Bitstamp single-pair URL.
fn bitstamp_ticker_pair_currency(parsed: &Url) -> Option<Currency> {
    match classify_bitstamp_ticker_url(parsed).ok()? {
        BitstampTickerEndpoint::SinglePair(currency) => Some(currency),
        BitstampTickerEndpoint::AllPairs => None,
    }
}

/// Names an unsupported fiat on a Bitstamp single-pair ticker URL (USD, EUR, and
/// GBP are the only supported Bitstamp fiat markets).
fn bitstamp_unsupported_single_pair_currency(parsed: &Url) -> Option<Currency> {
    if parsed.query().is_some_and(|query| !query.is_empty()) {
        return None;
    }
    let segments: Vec<&str> = parsed
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .collect();
    let ["api", "v2", "ticker", pair] = segments.as_slice() else {
        return None;
    };
    let pair = pair.to_ascii_lowercase();
    let quote = pair.strip_prefix("btc")?;
    let currency = Currency::parse(&quote.to_ascii_uppercase()).filter(|c| c.is_fiat())?;
    if BITSTAMP_SUPPORTED_CURRENCIES.contains(&currency) {
        None
    } else {
        Some(currency)
    }
}

/// Derives the fiat currency from a Bitstamp single-pair URL path string.
/// Returns `None` for the all-pairs array endpoint (`/api/v2/ticker/`) or any
/// unrecognized path.
fn bitstamp_single_pair_currency(feed_url: &str) -> Option<Currency> {
    let parsed = Url::parse(feed_url).ok()?;
    bitstamp_ticker_pair_currency(&parsed)
}

fn parse_bitstamp_object(
    object: &HashMap<String, Box<RawValue>>,
    observed_at: DateTime<Utc>,
) -> Result<RateSample, FxError> {
    let timestamp = object
        .get("timestamp")
        .and_then(|raw| parse_raw_unix_seconds(raw.get()))
        .ok_or(FxError::Malformed)?;
    let updated_at = DateTime::from_timestamp(timestamp, 0).ok_or(FxError::Malformed)?;
    check_freshness(updated_at, observed_at)?;
    let rate = object
        .get("last")
        .and_then(|raw| parse_raw_decimal(raw.get()))
        .ok_or(FxError::Malformed)?;
    if !rate.is_valid_rate() {
        return Err(FxError::OutOfBounds);
    }
    Ok(RateSample {
        rate,
        fetched_at: updated_at,
    })
}

fn parse_bitstamp_snapshot_with_hint(
    body: &[u8],
    observed_at: DateTime<Utc>,
    single_pair_hint: Option<Currency>,
    supported_fiat: &'static [Currency],
) -> Result<FxSnapshot, FxError> {
    let raw_payload: Box<RawValue> =
        serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    match raw_payload.get().trim_start().as_bytes().first() {
        Some(b'[') => {
            let items: Vec<Box<RawValue>> =
                serde_json::from_str(raw_payload.get()).map_err(|_| FxError::Malformed)?;
            let parsed_objects: Vec<BitstampArrayObject> = items
                .iter()
                .filter_map(|item| serde_json::from_str(item.get()).ok())
                .collect();
            let samples = supported_fiat
                .iter()
                .copied()
                .map(|currency| {
                    let expected_pair = format!("BTC/{}", currency.code());
                    let matched: Vec<&BitstampArrayObject> = parsed_objects
                        .iter()
                        .filter(|obj| obj.pairs.iter().any(|pair| pair == &expected_pair))
                        .collect();
                    let sample = if matched.len() != 1 || matched[0].has_duplicate_key {
                        Err(FxError::Malformed)
                    } else {
                        parse_bitstamp_object(&matched[0].fields, observed_at)
                    };
                    (currency, sample)
                })
                .collect();
            Ok(FxSnapshot {
                supported_fiat,
                samples,
            })
        }
        Some(b'{') => {
            let UniqueRawObject(object) =
                serde_json::from_str(raw_payload.get()).map_err(|_| FxError::Malformed)?;
            let pair_currency = match object.get("pair") {
                Some(value) => {
                    let pair: String =
                        serde_json::from_str(value.get()).map_err(|_| FxError::Malformed)?;
                    let quote = pair.strip_prefix("BTC/").ok_or(FxError::Malformed)?;
                    let currency = Currency::parse(quote)
                        .filter(|c| c.is_fiat())
                        .ok_or(FxError::Malformed)?;
                    if let Some(hint) = single_pair_hint {
                        if hint != currency {
                            return Err(FxError::Malformed);
                        }
                    }
                    currency
                }
                None => single_pair_hint.ok_or(FxError::Malformed)?,
            };
            if !supported_fiat.contains(&pair_currency) {
                return Err(FxError::UnsupportedCurrency);
            }
            let parsed_single = parse_bitstamp_object(&object, observed_at);
            let samples = supported_fiat
                .iter()
                .copied()
                .map(|currency| {
                    if currency == pair_currency {
                        (currency, parsed_single.clone())
                    } else {
                        (currency, Err(FxError::Malformed))
                    }
                })
                .collect();
            Ok(FxSnapshot {
                supported_fiat,
                samples,
            })
        }
        _ => Err(FxError::Malformed),
    }
}

/// Parses one feed response for `provider` and `feed_url` into the
/// per-currency snapshot. `header_date` is required by `Coinbase`, whose JSON
/// exchange-rates payload omits a timestamp; Blocktank, Kraken Trades,
/// Mempool, and Bitstamp validate per-payload or per-trade timestamps
/// directly. `feed_url` supplies the currency hint for Bitstamp single-pair
/// endpoints (`/api/v2/ticker/btc<ccy>/`) whose JSON object omits `"pair"`.
pub fn parse_snapshot_for_url(
    provider: FxFeedProvider,
    feed_url: &str,
    body: &[u8],
    observed_at: DateTime<Utc>,
    header_date: Option<DateTime<Utc>>,
) -> Result<FxSnapshot, FxError> {
    let parsed = Url::parse(feed_url).map_err(|_| FxError::Malformed)?;
    if provider == FxFeedProvider::Bitstamp
        && bitstamp_unsupported_single_pair_currency(&parsed).is_some()
    {
        return Err(FxError::UnsupportedCurrency);
    }
    let supported_fiat = supported_fiat_for(provider, &parsed).map_err(|_| FxError::Malformed)?;
    match provider {
        FxFeedProvider::Blocktank => parse_snapshot(body, observed_at, supported_fiat),
        FxFeedProvider::Kraken => parse_kraken_snapshot(body, observed_at, supported_fiat),
        FxFeedProvider::Coinbase => {
            parse_coinbase_snapshot(body, observed_at, header_date, supported_fiat)
        }
        FxFeedProvider::Mempool => parse_mempool_snapshot(body, observed_at, supported_fiat),
        FxFeedProvider::Bitstamp => parse_bitstamp_snapshot_with_hint(
            body,
            observed_at,
            bitstamp_single_pair_currency(feed_url),
            supported_fiat,
        ),
    }
}

/// Parses one feed response for `provider` into the per-currency snapshot.
/// `header_date` is required by `Coinbase`, whose JSON payload omits a
/// timestamp.
pub fn parse_snapshot_for(
    provider: FxFeedProvider,
    body: &[u8],
    observed_at: DateTime<Utc>,
    header_date: Option<DateTime<Utc>>,
) -> Result<FxSnapshot, FxError> {
    parse_snapshot_for_url(
        provider,
        provider.default_url(),
        body,
        observed_at,
        header_date,
    )
}

/// Parses one Blocktank-format feed response into the per-currency snapshot.
/// Only a body that is not the expected JSON shape fails as a whole.
pub fn parse_snapshot(
    body: &[u8],
    observed_at: DateTime<Utc>,
    supported_fiat: &'static [Currency],
) -> Result<FxSnapshot, FxError> {
    let feed: Feed = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    Ok(FxSnapshot {
        supported_fiat,
        samples: supported_fiat
            .iter()
            .copied()
            .map(|currency| {
                (
                    currency,
                    parse_blocktank_ticker(&feed, currency, observed_at),
                )
            })
            .collect(),
    })
}

/// The BTCUSD sample of a Blocktank feed body (the USD-only contract: the
/// other tickers are not consulted).
pub fn parse_feed(body: &[u8], observed_at: DateTime<Utc>) -> Result<RateSample, FxError> {
    parse_feed_for(body, observed_at, Currency::Usd)
}

/// The `BTC<CCY>` sample of a Blocktank feed body for one fiat currency.
pub fn parse_feed_for(
    body: &[u8],
    observed_at: DateTime<Utc>,
    currency: Currency,
) -> Result<RateSample, FxError> {
    let feed: Feed = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    parse_blocktank_ticker(&feed, currency, observed_at)
}

pub fn median(mut rates: Vec<Decimal>) -> Option<Decimal> {
    if rates.len() < REFERENCE_MIN_SAMPLES {
        return None;
    }
    // Fail closed: every value must widen to the common scale without
    // overflow, or there is no trustworthy reference to sort.
    let scale = rates.iter().map(|rate| rate.scale).max()?;
    for rate in &rates {
        rate.widened(scale)?;
    }
    rates.sort_by(|left, right| left.cmp(right).unwrap_or(Ordering::Equal));
    Some(rates[rates.len() / 2].clone())
}

pub fn within_deviation(rate: &Decimal, reference: &Decimal) -> bool {
    let scale = rate.scale.max(reference.scale);
    // Fail closed: an unwidenable value is treated as out of bounds.
    let (Some(rate_value), Some(reference_value)) = (rate.widened(scale), reference.widened(scale))
    else {
        return false;
    };
    let difference = rate_value.abs_diff(reference_value);
    let (Some(lhs), Some(rhs)) = (
        U320::from_u128(difference).checked_mul(U320::from_u64(10_000)),
        U320::from_u128(reference_value).checked_mul(U320::from_u128(MAX_DEVIATION_BPS)),
    ) else {
        return false;
    };
    lhs <= rhs
}

/// Converts fiat minor units into satoshis with exact integer arithmetic:
/// `ceil(total_minor * 100_000_000 / (10^exponent * BTC<CCY>))`.
/// Any positive fiat amount yields a positive satoshi total that fits `i64`;
/// zero totals are refused and overflow fails closed.
pub fn quote_sats(total_minor: i64, exponent: i32, rate: &Decimal) -> Result<u64, FxError> {
    let total = u128::try_from(total_minor).map_err(|_| FxError::Overflow)?;
    if total == 0 {
        return Err(FxError::BelowMinimum);
    }
    let exponent = u32::try_from(exponent).map_err(|_| FxError::Overflow)?;
    if exponent > MAX_CURRENCY_EXPONENT || !rate.is_valid_rate() {
        return Err(FxError::Overflow);
    }
    let rate_scale = U320::pow10(rate.scale).ok_or(FxError::Overflow)?;
    let fiat_scale = U320::pow10(exponent).ok_or(FxError::Overflow)?;
    let numerator = U320::from_u128(total)
        .checked_mul(U320::from_u64(100_000_000))
        .and_then(|v| v.checked_mul(rate_scale))
        .ok_or(FxError::Overflow)?;
    let denominator = fiat_scale
        .checked_mul(U320::from_u128(rate.mantissa))
        .ok_or(FxError::Overflow)?;
    let sats = numerator.div_ceil(denominator).ok_or(FxError::Overflow)?;
    if sats.is_zero() {
        return Err(FxError::BelowMinimum);
    }
    let sats = sats.to_u64().ok_or(FxError::Overflow)?;
    if i64::try_from(sats).is_err() {
        return Err(FxError::Overflow);
    }
    Ok(sats)
}

/// Guards the Bitcoin amount paykit-server fixed for a fiat price against
/// the service's own quote: strictly positive, representable as `i64`, and
/// within [`MAX_DEVIATION_BPS`] of the service's quote.
pub fn guard_fixed_sats(fixed_sats: u64, quoted_sats: u64) -> Result<(), FxError> {
    if fixed_sats == 0 {
        return Err(FxError::BelowMinimum);
    }
    if i64::try_from(fixed_sats).is_err() {
        return Err(FxError::Overflow);
    }
    let fixed = u128::from(fixed_sats);
    let quoted = u128::from(quoted_sats);
    let gap = fixed.abs_diff(quoted);
    let within = gap
        .checked_mul(10_000)
        .zip(quoted.checked_mul(MAX_DEVIATION_BPS))
        .is_some_and(|(gap, allowed)| gap <= allowed);
    if within {
        Ok(())
    } else {
        Err(FxError::DeviationExceeded)
    }
}

/// Guards the USDT amount paykit-server fixed for a satoshi price against the
/// service's own rate: strictly positive, representable as `i64`, and within
/// [`MAX_DEVIATION_BPS`] of the USDT0 millionths the price is worth at `rate`
/// (US dollars per bitcoin).
pub fn guard_fixed_usdt_units(fixed_units: u64, sats: u64, rate: &Decimal) -> Result<(), FxError> {
    if fixed_units == 0 {
        return Err(FxError::BelowMinimum);
    }
    if i64::try_from(fixed_units).is_err() {
        return Err(FxError::Overflow);
    }
    if !rate.is_valid_rate() {
        return Err(FxError::Overflow);
    }
    // One satoshi is rate / 10^8 US dollars, which is rate / 100 USDT0
    // millionths.
    let scale = U320::pow10(rate.scale)
        .and_then(|v| v.checked_mul(U320::from_u64(100)))
        .ok_or(FxError::Overflow)?;
    let numerator = U320::from_u64(sats)
        .checked_mul(U320::from_u128(rate.mantissa))
        .ok_or(FxError::Overflow)?;
    let expected = numerator.div_ceil(scale).ok_or(FxError::Overflow)?;
    if expected.is_zero() {
        return Err(FxError::BelowMinimum);
    }
    let fixed = U320::from_u64(fixed_units);
    let gap = if fixed >= expected {
        fixed.checked_sub(expected).ok_or(FxError::Overflow)?
    } else {
        expected.checked_sub(fixed).ok_or(FxError::Overflow)?
    };
    let lhs = gap
        .checked_mul(U320::from_u64(10_000))
        .ok_or(FxError::Overflow)?;
    let rhs = expected
        .checked_mul(U320::from_u128(MAX_DEVIATION_BPS))
        .ok_or(FxError::Overflow)?;
    if lhs <= rhs {
        Ok(())
    } else {
        Err(FxError::DeviationExceeded)
    }
}

pub fn reference_cutoff(now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::seconds(REFERENCE_WINDOW_SECS)
}

/// Converts fiat minor units into USD cents with exact integer arithmetic,
/// rounding up:
/// `ceil(total_minor * 100 * BTCUSD / (10^exponent * BTC<CCY>))`.
///
/// This is the multi-asset Paykit denomination of a non-USD fiat price: the
/// two rates must come from the same snapshot (the cross rate of a currency
/// is `BTCUSD / BTC<CCY>`). A positive total always yields at least one
/// cent.
pub fn usd_cents_for(
    total_minor: i64,
    exponent: i32,
    usd_rate: &Decimal,
    currency_rate: &Decimal,
) -> Result<u64, FxError> {
    let total = u128::try_from(total_minor).map_err(|_| FxError::Overflow)?;
    if total == 0 {
        return Err(FxError::BelowMinimum);
    }
    let exponent = u32::try_from(exponent).map_err(|_| FxError::Overflow)?;
    if exponent > MAX_CURRENCY_EXPONENT
        || !usd_rate.is_valid_rate()
        || !currency_rate.is_valid_rate()
    {
        return Err(FxError::Overflow);
    }
    let currency_scale = U320::pow10(currency_rate.scale).ok_or(FxError::Overflow)?;
    let usd_scale = U320::pow10(usd_rate.scale).ok_or(FxError::Overflow)?;
    let fiat_scale = U320::pow10(exponent).ok_or(FxError::Overflow)?;
    let numerator = U320::from_u128(total)
        .checked_mul(U320::from_u64(100))
        .and_then(|v| v.checked_mul(U320::from_u128(usd_rate.mantissa)))
        .and_then(|v| v.checked_mul(currency_scale))
        .ok_or(FxError::Overflow)?;
    let denominator = fiat_scale
        .checked_mul(U320::from_u128(currency_rate.mantissa))
        .and_then(|v| v.checked_mul(usd_scale))
        .ok_or(FxError::Overflow)?;
    let cents = numerator.div_ceil(denominator).ok_or(FxError::Overflow)?;
    let cents = cents.to_u64().ok_or(FxError::Overflow)?;
    if i64::try_from(cents).is_err() {
        return Err(FxError::Overflow);
    }
    Ok(cents)
}

/// Euro cents of an order total, from the persisted reference medians alone
/// (no feed fetch), rounding up: `ceil(total_minor * 100 * BTCEUR /
/// (10^exponent * BTC<CCY>))`, the arithmetic of [`usd_cents_for`] with the
/// euro ticker on top. A euro total is already in euro cents; a Bitcoin
/// total is converted with the euro ticker alone, one bitcoin being 1 and one
/// satoshi 10^-8 of it. Used by the EU annual-limit guard, which compares
/// an amount against a threshold and has no payment to quote.
pub async fn eur_cents_reference(
    conn: &mut sqlx::PgConnection,
    currency: Currency,
    exponent: i32,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<u64, FxError> {
    let total = u64::try_from(total_minor).map_err(|_| FxError::Overflow)?;
    if total == 0 {
        return Ok(0);
    }
    if currency == Currency::Eur {
        return Ok(total);
    }
    let eur_rate = reference_median(&mut *conn, Currency::Eur, now).await?;
    let currency_rate = match currency {
        Currency::Btc => Decimal {
            mantissa: 1,
            scale: 0,
        },
        Currency::Sat => Decimal {
            mantissa: 100_000_000,
            scale: 0,
        },
        fiat => reference_median(&mut *conn, fiat, now).await?,
    };
    usd_cents_for(total_minor, exponent, &eur_rate, &currency_rate)
}

/// The persisted reference of one currency: the median of the samples the
/// sampler accepted inside [`REFERENCE_WINDOW_SECS`], or `MissingReference`
/// below [`REFERENCE_MIN_SAMPLES`].
pub async fn reference_median<'e, E>(
    executor: E,
    currency: Currency,
    now: DateTime<Utc>,
) -> Result<Decimal, FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rates: Vec<(String,)> = sqlx::query_as(
        "SELECT rate::text FROM fx_rate_samples \
         WHERE currency = $1 AND accepted_at >= $2 ORDER BY accepted_at",
    )
    .bind(currency.code())
    .bind(reference_cutoff(now))
    .fetch_all(executor)
    .await
    .map_err(|_| FxError::MissingReference)?;
    let parsed: Vec<Decimal> = rates
        .into_iter()
        .map(|(rate,)| Decimal::parse(&rate).ok_or(FxError::MissingReference))
        .collect::<Result<Vec<_>, _>>()?;
    median(parsed).ok_or(FxError::MissingReference)
}

/// The reference BTCUSD rate from `provider`: the current feed rate, accepted
/// only while it sits within [`MAX_DEVIATION_BPS`] of the median of the
/// persisted samples.
pub async fn reference_rate_for<'e, E>(
    executor: E,
    provider: FxFeedProvider,
    feed_url: &str,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let feed = FxFeedConfig::resolve(provider, feed_url).map_err(|_| FxError::Malformed)?;
    if !feed.supports_fiat(Currency::Usd) {
        return Err(FxError::UnsupportedCurrency);
    }
    let median_rate = reference_median(executor, Currency::Usd, now).await?;
    let current = fetch_current_for(provider, feed_url, now).await?;
    if !within_deviation(&current.rate, &median_rate) {
        return Err(FxError::DeviationExceeded);
    }
    Ok((current.rate.clone(), current))
}

/// [`reference_rate_for`] against the Blocktank wire format.
pub async fn reference_rate<'e, E>(
    executor: E,
    feed_url: &str,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    reference_rate_for(executor, FxFeedProvider::Blocktank, feed_url, now).await
}

/// Quotes a fiat price in satoshis from `provider` with the currency's own
/// `BTC<CCY>` ticker: the reference first (a currency with fewer than
/// [`REFERENCE_MIN_SAMPLES`] samples is refused without a feed fetch), then
/// one snapshot fetch, the deviation guard and [`quote_sats`].
pub async fn quote_fiat_for<'e, E>(
    executor: E,
    provider: FxFeedProvider,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample, u64), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    quote_denominated(
        executor,
        provider,
        feed_url,
        currency,
        total_minor,
        currency.exponent(),
        now,
    )
    .await
}

/// [`quote_fiat_for`] against the Blocktank wire format.
pub async fn quote_fiat<'e, E>(
    executor: E,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample, u64), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    quote_fiat_for(
        executor,
        FxFeedProvider::Blocktank,
        feed_url,
        currency,
        total_minor,
        now,
    )
    .await
}

/// [`quote_fiat`] for USD with the caller's exponent.
pub async fn quote_usd<'e, E>(
    executor: E,
    feed_url: &str,
    total_minor: i64,
    exponent: i32,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample, u64), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    quote_denominated(
        executor,
        FxFeedProvider::Blocktank,
        feed_url,
        Currency::Usd,
        total_minor,
        exponent,
        now,
    )
    .await
}

async fn quote_denominated<'e, E>(
    executor: E,
    provider: FxFeedProvider,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    exponent: i32,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample, u64), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if !currency.is_fiat() {
        return Err(FxError::Malformed);
    }
    let feed = FxFeedConfig::resolve(provider, feed_url).map_err(|_| FxError::Malformed)?;
    if !feed.supports_fiat(currency) {
        return Err(FxError::UnsupportedCurrency);
    }
    let median_rate = reference_median(executor, currency, now).await?;
    let current = fetch_snapshot_currencies_for(
        feed.provider,
        feed_url,
        feed.supported_currencies(),
        &[currency],
        now,
    )
    .await?
    .sample(currency)?;
    if !within_deviation(&current.rate, &median_rate) {
        return Err(FxError::DeviationExceeded);
    }
    let sats = quote_sats(total_minor, exponent, &current.rate)?;
    Ok((current.rate.clone(), current, sats))
}

/// The cross rate behind a non-USD price converted to USD: both tickers of
/// one snapshot, each guarded against its own reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossRate {
    /// `EURUSD`: USD per one unit of the listing currency.
    pub pair: String,
    /// `BTCUSD` of the snapshot.
    pub usd_rate: Decimal,
    /// `BTC<CCY>` of the snapshot.
    pub currency_rate: Decimal,
    /// The older of the two ticker timestamps.
    pub fetched_at: DateTime<Utc>,
}

impl CrossRate {
    /// USD per one major unit of the currency (`BTCUSD / BTC<CCY>`),
    /// rounded half up to 12 decimals. Informational: the amount charged is
    /// computed from the two exact rates by [`usd_cents_for`].
    pub fn rate_string(&self) -> Option<String> {
        const PLACES: u32 = 12;
        if !self.usd_rate.is_valid_rate() || !self.currency_rate.is_valid_rate() {
            return None;
        }
        let numerator = U320::from_u128(self.usd_rate.mantissa)
            .checked_mul(U320::pow10(PLACES.checked_add(self.currency_rate.scale)?)?)?;
        let denominator = U320::from_u128(self.currency_rate.mantissa)
            .checked_mul(U320::pow10(self.usd_rate.scale)?)?;
        let (scaled, _) = numerator
            .checked_add(denominator.shr1())?
            .div_rem(denominator)?;
        let digits = scaled.to_decimal_string();
        let scale = PLACES as usize;
        if digits.len() <= scale {
            Some(format!("0.{:0>width$}", digits, width = scale))
        } else {
            let split = digits.len() - scale;
            Some(format!("{}.{}", &digits[..split], &digits[split..]))
        }
    }
}

/// An order price in USD cents: the amount a multi-asset Paykit request is
/// denominated in. `cross` is `None` when the price already is USD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsdSettlement {
    pub cents: u64,
    pub cross: Option<CrossRate>,
}

/// Everything one snapshot says about one order price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderQuote {
    pub currency: Currency,
    /// The guarded `BTC<CCY>` sample (the rate of the direct sats quote).
    pub direct: RateSample,
    /// The order total in satoshis at `direct`: the guard on the Bitcoin
    /// amount Paykit fixes.
    pub sats: Result<u64, FxError>,
    pub usd: UsdSettlement,
}

/// Quotes one fiat order total from a single snapshot of `provider` for the
/// multi-asset contract: the guarded direct `BTC<CCY>` quote and the
/// USD-cents settlement. A non-USD currency also needs a guarded `BTCUSD`
/// from the same snapshot; each leg is checked against its own persisted
/// reference before either amount is computed.
pub async fn quote_order_for(
    conn: &mut sqlx::PgConnection,
    provider: FxFeedProvider,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<OrderQuote, FxError> {
    if !currency.is_fiat() {
        return Err(FxError::Malformed);
    }
    let feed = FxFeedConfig::resolve(provider, feed_url).map_err(|_| FxError::Malformed)?;
    if !feed.supports_fiat(currency) {
        return Err(FxError::UnsupportedCurrency);
    }
    if currency != Currency::Usd && !feed.supports_fiat(Currency::Usd) {
        return Err(FxError::UnsupportedCurrency);
    }
    let direct_reference = reference_median(&mut *conn, currency, now).await?;
    let usd_reference = if currency == Currency::Usd {
        None
    } else {
        Some(reference_median(&mut *conn, Currency::Usd, now).await?)
    };
    let currencies: &[Currency] = if currency == Currency::Usd {
        &[Currency::Usd]
    } else {
        &[currency, Currency::Usd]
    };
    let snapshot = fetch_snapshot_currencies_for(
        feed.provider,
        feed_url,
        feed.supported_currencies(),
        currencies,
        now,
    )
    .await?;
    let direct = snapshot.sample(currency)?;
    if !within_deviation(&direct.rate, &direct_reference) {
        return Err(FxError::DeviationExceeded);
    }
    let sats = quote_sats(total_minor, currency.exponent(), &direct.rate);
    let usd = match usd_reference {
        None => UsdSettlement {
            cents: u64::try_from(total_minor).map_err(|_| FxError::Overflow)?,
            cross: None,
        },
        Some(usd_reference) => {
            let usd_sample = snapshot.sample(Currency::Usd)?;
            if !within_deviation(&usd_sample.rate, &usd_reference) {
                return Err(FxError::DeviationExceeded);
            }
            let cents = usd_cents_for(
                total_minor,
                currency.exponent(),
                &usd_sample.rate,
                &direct.rate,
            )?;
            UsdSettlement {
                cents,
                cross: Some(CrossRate {
                    pair: format!("{}USD", currency.code()),
                    usd_rate: usd_sample.rate,
                    currency_rate: direct.rate.clone(),
                    fetched_at: usd_sample.fetched_at.min(direct.fetched_at),
                }),
            }
        }
    };
    Ok(OrderQuote {
        currency,
        direct,
        sats,
        usd,
    })
}

/// [`quote_order_for`] against the Blocktank wire format.
pub async fn quote_order(
    conn: &mut sqlx::PgConnection,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<OrderQuote, FxError> {
    quote_order_for(
        conn,
        FxFeedProvider::Blocktank,
        feed_url,
        currency,
        total_minor,
        now,
    )
    .await
}

struct FetchedFeed {
    body: Vec<u8>,
    header_date: Option<DateTime<Utc>>,
}

fn parse_http_date(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(raw.trim())
        .ok()
        .map(|date| date.with_timezone(&Utc))
}

fn extract_single_http_date(headers: &reqwest::header::HeaderMap) -> Option<DateTime<Utc>> {
    let mut dates = headers.get_all(reqwest::header::DATE).iter();
    let first = dates.next()?;
    if dates.next().is_some() {
        return None;
    }
    first.to_str().ok().and_then(parse_http_date)
}

fn validate_http_age(headers: &reqwest::header::HeaderMap) -> Result<(), FxError> {
    let mut ages = headers.get_all(reqwest::header::AGE).iter();
    let Some(first) = ages.next() else {
        return Ok(());
    };
    if ages.next().is_some() {
        return Err(FxError::Malformed);
    }
    let raw = first.to_str().map_err(|_| FxError::Malformed)?.trim();
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(FxError::Malformed);
    }
    let age_secs = raw.parse::<i64>().map_err(|_| FxError::Malformed)?;
    if age_secs > MAX_RATE_AGE_SECS {
        return Err(FxError::Stale);
    }
    Ok(())
}

async fn build_fx_http_client(
    feed_url: &str,
    deadline: tokio::time::Instant,
) -> Result<(Url, reqwest::Client), FxError> {
    let parsed = validate_fx_feed_url(feed_url).map_err(|_| FxError::Malformed)?;
    let loopback = is_loopback_host(&parsed);
    let port = parsed.port_or_known_default().ok_or(FxError::Malformed)?;

    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .https_only(!loopback)
        .user_agent("pubky-marketplace-service");

    match parsed.host() {
        Some(Host::Domain(domain)) => {
            let addresses: Vec<SocketAddr> =
                tokio::time::timeout_at(deadline, tokio::net::lookup_host((domain, port)))
                    .await
                    .map_err(|_| FxError::Malformed)?
                    .map_err(|_| FxError::Malformed)?
                    .collect();
            if addresses.is_empty()
                || !addresses
                    .iter()
                    .all(|address| is_allowed_fx_ip(address.ip(), loopback))
            {
                return Err(FxError::Malformed);
            }
            builder = builder.resolve_to_addrs(domain, &addresses);
        }
        Some(Host::Ipv4(ip)) => {
            if !is_allowed_fx_ip(IpAddr::V4(ip), loopback) {
                return Err(FxError::Malformed);
            }
        }
        Some(Host::Ipv6(ip)) => {
            if !is_allowed_fx_ip(IpAddr::V6(ip), loopback) {
                return Err(FxError::Malformed);
            }
        }
        None => return Err(FxError::Malformed),
    }

    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(FxError::Malformed);
    }

    let client = builder
        .connect_timeout(FX_CONNECT_TIMEOUT.min(remaining))
        .timeout(remaining)
        .build()
        .map_err(|_| FxError::Malformed)?;
    Ok((parsed, client))
}

async fn fetch_body_with_client(
    client: &reqwest::Client,
    url: Url,
    deadline: tokio::time::Instant,
) -> Result<FetchedFeed, FxError> {
    if deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .is_zero()
    {
        return Err(FxError::Malformed);
    }
    let mut response = tokio::time::timeout_at(
        deadline,
        client
            .get(url)
            .header(reqwest::header::CACHE_CONTROL, "no-cache")
            .send(),
    )
    .await
    .map_err(|_| FxError::Malformed)?
    .map_err(|_| FxError::Malformed)?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(FxError::Malformed);
    }
    validate_http_age(response.headers())?;
    let header_date = extract_single_http_date(response.headers());
    if response
        .content_length()
        .is_some_and(|len| len > MAX_FX_RESPONSE_BYTES as u64)
    {
        return Err(FxError::Malformed);
    }
    let body = tokio::time::timeout_at(deadline, async {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| FxError::Malformed)? {
            if body
                .len()
                .checked_add(chunk.len())
                .is_none_or(|total| total > MAX_FX_RESPONSE_BYTES)
            {
                return Err(FxError::Malformed);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    })
    .await
    .map_err(|_| FxError::Malformed)??;
    Ok(FetchedFeed { body, header_date })
}

async fn fetch_body(feed_url: &str) -> Result<FetchedFeed, FxError> {
    fetch_body_with_timeout(feed_url, FX_REQUEST_TIMEOUT).await
}

async fn fetch_body_with_timeout(
    feed_url: &str,
    budget: StdDuration,
) -> Result<FetchedFeed, FxError> {
    if budget.is_zero() {
        return Err(FxError::Malformed);
    }
    let deadline = tokio::time::Instant::now() + budget;
    let (parsed, client) = build_fx_http_client(feed_url, deadline).await?;
    fetch_body_with_client(&client, parsed, deadline).await
}

async fn fetch_kraken_snapshot_currencies(
    feed_url: &str,
    supported_fiat: &'static [Currency],
    currencies: &[Currency],
    observed_at: DateTime<Utc>,
    budget: StdDuration,
) -> Result<FxSnapshot, FxError> {
    if budget.is_zero() {
        return Err(FxError::Malformed);
    }
    let deadline = tokio::time::Instant::now() + budget;
    let (parsed, client) = build_fx_http_client(feed_url, deadline).await?;

    // If the configured URL already specifies a `pair` query parameter,
    // canonicalize it (`pair=<pair>&count=1`, stripping `since` and any
    // non-canonical `pair`/`count` casing) and fetch that exact pair once.
    let explicit_pairs: Vec<String> = parsed
        .query_pairs()
        .filter(|(key, _)| key.eq_ignore_ascii_case("pair"))
        .map(|(_, value)| value.into_owned())
        .collect();
    if !explicit_pairs.is_empty() {
        if explicit_pairs.len() != 1
            || explicit_pairs[0].trim().is_empty()
            || explicit_pairs[0].contains(',')
        {
            return Err(FxError::Malformed);
        }
        let canonical = kraken_canonical_pair_url(&parsed, explicit_pairs[0].trim());
        let fetched = fetch_body_with_client(&client, canonical, deadline).await?;
        return parse_kraken_snapshot(&fetched.body, observed_at, supported_fiat);
    }

    let mut target_currencies: Vec<Currency> = Vec::with_capacity(currencies.len());
    for &currency in currencies {
        if supported_fiat.contains(&currency) && !target_currencies.contains(&currency) {
            target_currencies.push(currency);
        }
    }
    if target_currencies.is_empty() {
        return Err(FxError::Malformed);
    }

    let mut join_set = tokio::task::JoinSet::new();
    for currency in target_currencies.iter().copied() {
        let pair_url = kraken_pair_url(&parsed, currency)?;
        let client = client.clone();
        join_set.spawn(async move {
            let outcome = match fetch_body_with_client(&client, pair_url, deadline).await {
                Ok(fetched) => match parse_kraken_envelope(&fetched.body) {
                    Ok(result) => Ok(parse_kraken_pair_trades(&result, currency, observed_at)),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            (currency, outcome)
        });
    }

    let mut per_currency: HashMap<Currency, Result<RateSample, FxError>> =
        HashMap::with_capacity(target_currencies.len());
    let mut any_valid_envelope = false;
    let mut first_envelope_error: Option<FxError> = None;

    while let Some(joined) = join_set.join_next().await {
        let (currency, outcome) = joined.map_err(|_| FxError::Malformed)?;
        match outcome {
            Ok(sample) => {
                any_valid_envelope = true;
                per_currency.insert(currency, sample);
            }
            Err(error) => {
                if first_envelope_error.is_none() {
                    first_envelope_error = Some(error);
                }
                per_currency.insert(currency, Err(error));
            }
        }
    }

    if !any_valid_envelope {
        return Err(first_envelope_error.unwrap_or(FxError::Malformed));
    }

    let samples = target_currencies
        .into_iter()
        .map(|currency| {
            let sample = per_currency
                .remove(&currency)
                .unwrap_or(Err(FxError::Malformed));
            (currency, sample)
        })
        .collect();
    Ok(FxSnapshot {
        supported_fiat,
        samples,
    })
}

async fn fetch_snapshot_currencies_for(
    provider: FxFeedProvider,
    feed_url: &str,
    supported_fiat: &'static [Currency],
    currencies: &[Currency],
    now: DateTime<Utc>,
) -> Result<FxSnapshot, FxError> {
    if provider == FxFeedProvider::Kraken {
        return fetch_kraken_snapshot_currencies(
            feed_url,
            supported_fiat,
            currencies,
            now,
            FX_REQUEST_TIMEOUT,
        )
        .await;
    }
    let fetched = fetch_body(feed_url).await?;
    parse_snapshot_for_url(provider, feed_url, &fetched.body, now, fetched.header_date)
}

/// One fetch of `provider` at `feed_url`, every URL-supported fiat ticker
/// validated.
pub async fn fetch_snapshot_for(
    provider: FxFeedProvider,
    feed_url: &str,
    now: DateTime<Utc>,
) -> Result<FxSnapshot, FxError> {
    let feed = FxFeedConfig::resolve(provider, feed_url).map_err(|_| FxError::Malformed)?;
    fetch_snapshot_currencies_for(
        feed.provider,
        feed_url,
        feed.supported_currencies(),
        feed.supported_currencies(),
        now,
    )
    .await
}

/// One fetch of the Blocktank feed, every fiat ticker validated.
pub async fn fetch_snapshot(feed_url: &str, now: DateTime<Utc>) -> Result<FxSnapshot, FxError> {
    fetch_snapshot_for(FxFeedProvider::Blocktank, feed_url, now).await
}

/// One fetch of `provider` at `feed_url`, the USD ticker only.
pub async fn fetch_current_for(
    provider: FxFeedProvider,
    feed_url: &str,
    now: DateTime<Utc>,
) -> Result<RateSample, FxError> {
    let feed = FxFeedConfig::resolve(provider, feed_url).map_err(|_| FxError::Malformed)?;
    fetch_snapshot_currencies_for(
        feed.provider,
        feed_url,
        feed.supported_currencies(),
        &[Currency::Usd],
        now,
    )
    .await?
    .sample(Currency::Usd)
}

/// One fetch of the Blocktank feed, the USD ticker only.
pub async fn fetch_current(feed_url: &str, now: DateTime<Utc>) -> Result<RateSample, FxError> {
    let fetched = fetch_body(feed_url).await?;
    parse_feed(&fetched.body, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_seller::StoreMode;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn spawn_raw_http_server(
        raw_response: Vec<u8>,
    ) -> (String, Arc<AtomicUsize>, Arc<std::sync::Mutex<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("raw http binds");
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let last_request = Arc::new(std::sync::Mutex::new(String::new()));
        let hits_bg = hits.clone();
        let req_bg = last_request.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                hits_bg.fetch_add(1, AtomicOrdering::SeqCst);
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                *req_bg.lock().unwrap() = String::from_utf8_lossy(&buf[..n]).into_owned();
                let _ = socket.write_all(&raw_response).await;
                let _ = socket.shutdown().await;
            }
        });
        (format!("http://{addr}"), hits, last_request)
    }

    #[test]
    fn live_fixture_selects_only_btcusd() {
        let now = "2026-09-13T16:21:00Z".parse().unwrap();
        let sample = parse_feed(
            include_bytes!("../tests/fixtures/blocktank_fx_rates_btc.json"),
            now,
        )
        .expect("captured production response parses");
        assert_eq!(sample.rate.to_string_value(), "77197");
    }

    #[test]
    fn parse_caps_the_fraction_at_eighteen_digits() {
        assert_eq!(
            Decimal::parse("77197.12345678")
                .expect("eight fraction digits parse")
                .to_string_value(),
            "77197.12345678"
        );
        assert_eq!(
            Decimal::parse("77197.123456789")
                .expect("nine fraction digits parse losslessly")
                .to_string_value(),
            "77197.123456789"
        );
        assert_eq!(
            Decimal::parse("77197.123456789012345678")
                .expect("eighteen fraction digits parse losslessly")
                .to_string_value(),
            "77197.123456789012345678"
        );
        // Nineteen digits exceeds MAX_FRACTION_SCALE; a 40-digit fraction is
        // rejected outright so checked widening and BigUint powers stay bounded.
        assert_eq!(Decimal::parse("77197.1234567890123456789"), None);
        let forty_digit_fraction = format!("77197.{}", "1".repeat(40));
        assert_eq!(Decimal::parse(&forty_digit_fraction), None);
    }

    #[test]
    fn comparison_paths_fail_closed_on_unwidenable_values() {
        // A mantissa near u128::MAX cannot be widened to a fractional
        // scale: comparisons fail closed instead of wrapping.
        let huge = Decimal {
            mantissa: u128::MAX,
            scale: 0,
        };
        assert!(
            !huge.is_valid_rate(),
            "unwidenable values fail rate validation"
        );
        let fractional = Decimal::parse("77197.5").unwrap();
        assert!(
            !within_deviation(&huge, &fractional),
            "an unwidenable value is treated as a deviation breach"
        );
        assert_eq!(
            median(vec![
                huge,
                Decimal::parse("77197").unwrap(),
                Decimal::parse("77200.5").unwrap(),
            ]),
            None,
            "an unwidenable sample voids the reference"
        );
    }

    #[test]
    fn quote_uses_checked_integer_ceiling_without_artificial_floor_or_cap() {
        let rate = Decimal::parse("10000").unwrap();
        assert_eq!(quote_sats(1_000_000, 2, &rate), Ok(100_000_000));
        assert_eq!(quote_sats(1, 2, &rate), Ok(100));
        assert_eq!(quote_sats(10_000_000_000, 2, &rate), Ok(1_000_000_000_000));
    }

    #[test]
    fn quote_usd_two_decimal_matrix_uses_exact_rational_ceiling() {
        let rate = Decimal::parse("100000").unwrap();
        assert_eq!(quote_sats(1, 2, &rate), Ok(10));
        assert_eq!(quote_sats(99, 2, &rate), Ok(990));
        assert_eq!(quote_sats(1_000, 2, &rate), Ok(10_000));
        assert_eq!(quote_sats(1_001, 2, &rate), Ok(10_010));
        assert_eq!(quote_sats(12_500, 2, &rate), Ok(125_000));
        assert_eq!(quote_sats(12_501, 2, &rate), Ok(125_010));
        assert_eq!(quote_sats(25_000_000, 2, &rate), Ok(250_000_000));
    }

    #[test]
    fn quote_rejects_zero_negative_and_overflow_inputs() {
        let rate = Decimal::parse("77197").unwrap();
        assert_eq!(quote_sats(0, 2, &rate), Err(FxError::BelowMinimum));
        assert_eq!(quote_sats(-1, 2, &rate), Err(FxError::Overflow));
        assert_eq!(
            quote_sats(i64::MAX, i32::MAX, &rate),
            Err(FxError::Overflow)
        );
        // Result exceeding i64::MAX fails closed with Overflow instead of
        // panicking when stored in BIGINT columns.
        let tiny_rate = Decimal::parse("0.00000001").unwrap();
        assert_eq!(quote_sats(i64::MAX, 0, &tiny_rate), Err(FxError::Overflow));
    }

    #[test]
    fn quote_supports_zero_and_three_decimal_currency_exponents_with_ceiling() {
        let rate = Decimal::parse("77197").unwrap();
        assert_eq!(quote_sats(1_000, 0, &rate), Ok(1_295_388));
        assert_eq!(quote_sats(100, 3, &rate), Ok(130));
    }

    #[test]
    fn a_fixed_bitcoin_amount_is_guarded_by_deviation_and_positive_i64_safety() {
        assert_eq!(guard_fixed_sats(100_000, 100_000), Ok(()));
        assert_eq!(guard_fixed_sats(105_000, 100_000), Ok(()));
        assert_eq!(guard_fixed_sats(95_000, 100_000), Ok(()));
        assert_eq!(
            guard_fixed_sats(105_001, 100_000),
            Err(FxError::DeviationExceeded)
        );
        assert_eq!(
            guard_fixed_sats(94_999, 100_000),
            Err(FxError::DeviationExceeded)
        );
        assert_eq!(guard_fixed_sats(0, 1_000), Err(FxError::BelowMinimum));
        assert_eq!(guard_fixed_sats(1, 1), Ok(()));
        assert_eq!(guard_fixed_sats(500, 500), Ok(()));
        assert_eq!(guard_fixed_sats(999, 1_000), Ok(()));
        assert_eq!(guard_fixed_sats(250_000_000, 250_000_000), Ok(()));
        assert_eq!(guard_fixed_sats(5_000, 0), Err(FxError::DeviationExceeded));
        assert_eq!(guard_fixed_sats(u64::MAX, u64::MAX), Err(FxError::Overflow));
    }

    #[test]
    fn a_fixed_usdt_amount_is_guarded_against_the_service_rate_and_i64_bound() {
        // 50,000 sats at 81,000 USD per BTC is 40.5 USDT0.
        let rate = Decimal::parse("81000").unwrap();
        assert_eq!(guard_fixed_usdt_units(40_500_000, 50_000, &rate), Ok(()));
        assert_eq!(guard_fixed_usdt_units(42_525_000, 50_000, &rate), Ok(()));
        assert_eq!(guard_fixed_usdt_units(38_475_000, 50_000, &rate), Ok(()));
        for fixed in [42_525_001, 38_474_999, 1, 81_000_000] {
            assert_eq!(
                guard_fixed_usdt_units(fixed, 50_000, &rate),
                Err(FxError::DeviationExceeded),
                "{fixed}"
            );
        }
        assert_eq!(
            guard_fixed_usdt_units(0, 50_000, &rate),
            Err(FxError::BelowMinimum)
        );
        let fractional = Decimal::parse("77197.5").unwrap();
        assert_eq!(
            guard_fixed_usdt_units(38_599_000, 50_000, &fractional),
            Ok(())
        );
        let huge = Decimal {
            mantissa: u128::MAX,
            scale: 0,
        };
        assert_eq!(
            guard_fixed_usdt_units(1, u64::MAX, &huge),
            Err(FxError::Overflow)
        );
        // Any fixed USDT amount above i64::MAX fails closed with Overflow.
        let above_i64 = i64::MAX as u64 + 1;
        let matching_rate = Decimal::parse("200").unwrap();
        assert_eq!(
            guard_fixed_usdt_units(above_i64, i64::MAX as u64 / 2, &matching_rate),
            Err(FxError::Overflow)
        );
    }

    #[test]
    fn deviation_boundaries_are_inclusive() {
        let median = Decimal::parse("100000").unwrap();
        assert!(within_deviation(
            &Decimal::parse("105000").unwrap(),
            &median
        ));
        assert!(within_deviation(&Decimal::parse("95000").unwrap(), &median));
        assert!(!within_deviation(
            &Decimal::parse("105001").unwrap(),
            &median
        ));
        assert!(!within_deviation(
            &Decimal::parse("94999").unwrap(),
            &median
        ));
    }

    #[test]
    fn malformed_or_duplicate_btcusd_is_rejected() {
        let now = "2026-09-13T16:21:00Z".parse().unwrap();
        let body = br#"{"tickers":[
            {"symbol":"BTCUSD","lastPrice":"77178","base":"BTC","quote":"USD","lastUpdatedAt":1789316268233},
            {"symbol":"BTCUSD","lastPrice":"77179","base":"BTC","quote":"USD","lastUpdatedAt":1789316268233}
        ]}"#;
        assert_eq!(parse_feed(body, now), Err(FxError::Malformed));
    }

    #[test]
    fn feed_rejects_missing_stale_future_and_zero_while_allowing_any_positive_rate() {
        let now: DateTime<Utc> = "2026-09-13T16:21:00Z".parse().unwrap();
        let body = |price: &str, timestamp: i64| {
            format!(
                r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"{price}","base":"BTC","quote":"USD","lastUpdatedAt":{timestamp}}}]}}"#
            )
        };
        let current = now.timestamp_millis();
        assert_eq!(
            parse_feed(br#"{"tickers":[]}"#, now),
            Err(FxError::Malformed)
        );
        assert_eq!(
            parse_feed(body("77197", current - 301_000).as_bytes(), now),
            Err(FxError::Stale)
        );
        assert_eq!(
            parse_feed(body("77197", current + 61_000).as_bytes(), now),
            Err(FxError::Future)
        );
        assert_eq!(
            parse_feed(body("0", current).as_bytes(), now),
            Err(FxError::OutOfBounds)
        );
        assert_eq!(
            parse_feed(body("0.00000000", current).as_bytes(), now),
            Err(FxError::OutOfBounds)
        );
        assert_eq!(
            parse_feed(body("-5", current).as_bytes(), now),
            Err(FxError::Malformed)
        );
        // Market prices outside the removed static $10k..$1M band parse cleanly.
        assert_eq!(
            parse_feed(body("5000", current).as_bytes(), now)
                .expect("below former floor")
                .rate
                .to_string_value(),
            "5000"
        );
        assert_eq!(
            parse_feed(body("2500000", current).as_bytes(), now)
                .expect("above former ceiling")
                .rate
                .to_string_value(),
            "2500000"
        );
    }

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/blocktank_fx_rates_btc.json");
    const FIXTURE_NOW: &str = "2026-09-13T16:21:00Z";

    fn rate(value: &str) -> Decimal {
        Decimal::parse(value).expect("decimal")
    }

    #[test]
    fn live_fixture_carries_all_seven_pinned_tickers() {
        let now = FIXTURE_NOW.parse().unwrap();
        let snapshot =
            parse_snapshot(FIXTURE, now, &Currency::FIAT).expect("captured response parses");
        let expected = [
            (Currency::Usd, "77197"),
            (Currency::Eur, "66481"),
            (Currency::Chf, "62992"),
            (Currency::Gbp, "57037"),
            (Currency::Jpy, "11745955"),
            (Currency::Cad, "106992"),
            (Currency::Aud, "107607"),
        ];
        for (currency, price) in expected {
            let sample = snapshot.sample(currency).unwrap_or_else(|error| {
                panic!("{currency}: {error:?}");
            });
            assert_eq!(sample.rate.to_string_value(), price, "{currency}");
            assert_eq!(
                parse_feed_for(FIXTURE, now, currency).map(|sample| sample.rate),
                Ok(rate(price)),
                "{currency}"
            );
        }
        assert_eq!(snapshot.samples().count(), 7);
    }

    #[test]
    fn a_bad_ticker_refuses_only_its_own_currency() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp_millis();
        let body = format!(
            r#"{{"tickers":[
                {{"symbol":"BTCUSD","lastPrice":"77197","base":"BTC","quote":"USD","lastUpdatedAt":{current}}},
                {{"symbol":"BTCEUR","lastPrice":"66481","base":"BTC","quote":"EUR","lastUpdatedAt":{stale}}},
                {{"symbol":"BTCJPY","lastPrice":"11745955","base":"BTC","quote":"JPY","lastUpdatedAt":{future}}},
                {{"symbol":"BTCGBP","lastPrice":"0","base":"BTC","quote":"GBP","lastUpdatedAt":{current}}},
                {{"symbol":"BTCCHF","lastPrice":"62992","base":"BTC","quote":"CHF","lastUpdatedAt":{current}}},
                {{"symbol":"BTCCHF","lastPrice":"62993","base":"BTC","quote":"CHF","lastUpdatedAt":{current}}}
            ]}}"#,
            stale = current - 301_000,
            future = current + 61_000,
        );
        let snapshot =
            parse_snapshot(body.as_bytes(), now, &Currency::FIAT).expect("shape is valid");
        assert_eq!(
            snapshot.sample(Currency::Usd).map(|sample| sample.rate),
            Ok(rate("77197"))
        );
        assert_eq!(snapshot.sample(Currency::Eur), Err(FxError::Stale));
        assert_eq!(snapshot.sample(Currency::Jpy), Err(FxError::Future));
        assert_eq!(snapshot.sample(Currency::Gbp), Err(FxError::OutOfBounds));
        assert_eq!(snapshot.sample(Currency::Chf), Err(FxError::Malformed));
        assert_eq!(snapshot.sample(Currency::Cad), Err(FxError::Malformed));
        assert_eq!(snapshot.sample(Currency::Aud), Err(FxError::Malformed));
        assert_eq!(
            parse_snapshot(b"not json", now, &Currency::FIAT),
            Err(FxError::Malformed),
            "only an unreadable body fails the whole snapshot"
        );
    }

    #[test]
    fn every_currency_enforces_positive_finite_rates_and_staleness_and_skew_guards() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp_millis();
        let body = |currency: Currency, price: &str, updated: i64| {
            format!(
                r#"{{"tickers":[{{"symbol":"{}","lastPrice":"{price}","base":"BTC","quote":"{}","lastUpdatedAt":{updated}}}]}}"#,
                currency.feed_symbol().unwrap(),
                currency.code()
            )
        };
        for currency in Currency::FIAT {
            let parse = |price: &str, updated: i64| {
                parse_feed_for(body(currency, price, updated).as_bytes(), now, currency)
                    .map(|sample| sample.rate.to_string_value())
            };
            assert_eq!(parse("1", current), Ok("1".to_string()), "{currency}");
            assert_eq!(
                parse("500000000", current),
                Ok("500000000".to_string()),
                "{currency}"
            );
            assert_eq!(
                parse("77197", current - 300_000),
                Ok("77197".to_string()),
                "{currency}: exactly the maximum age is fresh"
            );
            assert_eq!(
                parse("77197", current - 300_001),
                Err(FxError::Stale),
                "{currency}"
            );
            assert_eq!(
                parse("77197", current + 60_000),
                Ok("77197".to_string()),
                "{currency}: exactly the maximum skew is accepted"
            );
            assert_eq!(
                parse("77197", current + 60_001),
                Err(FxError::Future),
                "{currency}"
            );
            assert_eq!(parse("0", current), Err(FxError::OutOfBounds), "{currency}");
            assert_eq!(parse("-5", current), Err(FxError::Malformed), "{currency}");
        }
    }

    #[test]
    fn a_ticker_must_carry_its_own_symbol_base_and_quote() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp_millis();
        let mismatched = [
            ("BTCEUR", "BTC", "USD"),
            ("BTCUSD", "BTC", "EUR"),
            ("BTCEUR", "ETH", "EUR"),
            ("EURBTC", "EUR", "BTC"),
        ];
        for (symbol, base, quote) in mismatched {
            let body = format!(
                r#"{{"tickers":[{{"symbol":"{symbol}","lastPrice":"66481","base":"{base}","quote":"{quote}","lastUpdatedAt":{current}}}]}}"#
            );
            assert_eq!(
                parse_feed_for(body.as_bytes(), now, Currency::Eur),
                Err(FxError::Malformed),
                "{symbol}/{base}/{quote}"
            );
        }
    }

    #[test]
    fn sats_quote_rounds_up_for_each_currency_at_the_fixture_rates() {
        // ceil(total * 1e8 / (10^exponent * BTC<CCY>)), recomputed
        // independently with exact fractions.
        let cases = [
            (Currency::Usd, "77197", 13_700, 177_469),
            (Currency::Eur, "66481", 13_700, 206_074),
            (Currency::Chf, "62992", 13_700, 217_488),
            (Currency::Gbp, "57037", 13_700, 240_195),
            (Currency::Jpy, "11745955", 13_700, 116_636),
            (Currency::Cad, "106992", 13_700, 128_047),
            (Currency::Aud, "107607", 13_700, 127_316),
        ];
        for (currency, price, total, sats) in cases {
            assert_eq!(
                quote_sats(total, currency.exponent(), &rate(price)),
                Ok(sats),
                "{currency}"
            );
        }
        assert_eq!(
            quote_sats(1_000, 0, &rate("11745955")),
            Ok(8_514),
            "1,000 JPY"
        );
        assert_eq!(
            quote_sats(100, 0, &rate("11745955")),
            Ok(852),
            "100 JPY quotes cleanly without an artificial 1,000-sat floor"
        );
        assert_eq!(quote_sats(1, 0, &rate("11745955")), Ok(9));
    }

    #[test]
    fn sats_quote_is_exact_on_division_and_one_sat_up_past_it() {
        // 1,000.00 EUR at 100,000 EUR per BTC is exactly 1,000,000 sats;
        // one more cent costs 10 sats more; a JPY price of 1,000 at
        // 10,000,000 is exactly 10,000 and 1,001 is 10,010.
        assert_eq!(quote_sats(100_000, 2, &rate("100000")), Ok(1_000_000));
        assert_eq!(quote_sats(100_001, 2, &rate("100000")), Ok(1_000_010));
        assert_eq!(quote_sats(1_000, 0, &rate("10000000")), Ok(10_000));
        assert_eq!(quote_sats(1_001, 0, &rate("10000000")), Ok(10_010));
        // A non-divisible rate rounds up, never to nearest: 10.00 EUR at
        // 66,481 is 15,041.89 sats and bills 15,042.
        assert_eq!(quote_sats(1_000, 2, &rate("66481")), Ok(15_042));
        // A fractional rate scales the denominator, not the amount.
        assert_eq!(quote_sats(10_000, 2, &rate("66481.5")), Ok(150_418));
    }

    #[test]
    fn sats_quote_allows_orders_above_one_bitcoin_for_every_currency() {
        assert_eq!(
            quote_sats(100_000_000_000, 0, &rate("11745955")),
            Ok(851_356_913_934),
            "100 billion JPY"
        );
        assert_eq!(quote_sats(10_000_000, 2, &rate("100000")), Ok(100_000_000));
        assert_eq!(quote_sats(10_000_001, 2, &rate("100000")), Ok(100_000_010));
        assert_eq!(
            quote_sats(50_000_000, 2, &rate("100000")),
            Ok(500_000_000),
            "5 BTC fiat-priced order"
        );
    }

    #[test]
    fn usd_cents_use_the_cross_rate_and_round_up() {
        // ceil(total * 100 * BTCUSD / (10^exponent * BTC<CCY>)), exact
        // fractions, fixture rates.
        let usd = rate("77197");
        let cases = [
            (Currency::Eur, "66481", 13_700, 15_909),
            (Currency::Chf, "62992", 13_700, 16_790),
            (Currency::Gbp, "57037", 13_700, 18_543),
            (Currency::Jpy, "11745955", 13_700, 9_004),
            (Currency::Cad, "106992", 13_700, 9_885),
            (Currency::Aud, "107607", 13_700, 9_829),
        ];
        for (currency, price, total, cents) in cases {
            assert_eq!(
                usd_cents_for(total, currency.exponent(), &usd, &rate(price)),
                Ok(cents),
                "{currency}"
            );
        }
        // JPY has no minor unit: 1 yen is 0.657 cents and bills 1 cent,
        // 100 yen is 65.7 and bills 66, 1,000 yen is 657.2 and bills 658.
        let jpy = rate("11745955");
        assert_eq!(usd_cents_for(1, 0, &usd, &jpy), Ok(1));
        assert_eq!(usd_cents_for(100, 0, &usd, &jpy), Ok(66));
        assert_eq!(usd_cents_for(1_000, 0, &usd, &jpy), Ok(658));
    }

    #[test]
    fn usd_cents_are_exact_when_the_cross_rate_divides_and_one_cent_up_past_it() {
        // BTCUSD 100,000 over BTCEUR 50,000 is exactly 2 USD per EUR.
        let usd = rate("100000");
        let eur = rate("50000");
        assert_eq!(usd_cents_for(1_000, 2, &usd, &eur), Ok(2_000));
        assert_eq!(usd_cents_for(1_001, 2, &usd, &eur), Ok(2_002));
        // A cross rate of exactly one is the identity at every amount.
        let same = rate("90000");
        for total in [1, 99, 100, 12_345, 9_007_199_254_740_991] {
            assert_eq!(usd_cents_for(total, 2, &same, &same), Ok(total as u64));
        }
        // A rate that does not divide rounds up, never to nearest: 1.01
        // EUR at 1/3 is 0.3367 USD (33.67 cents).
        assert_eq!(
            usd_cents_for(101, 2, &rate("30000"), &rate("90000")),
            Ok(34)
        );
        // Fractional rates scale both sides.
        assert_eq!(
            usd_cents_for(10_000, 2, &rate("100000.5"), &rate("50000.25")),
            Ok(20_000)
        );
    }

    #[test]
    fn high_precision_exchange_rates_preserve_full_precision_in_quote_sats_and_usd_cents() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();

        // Exact rational reproduction for denominator precision:
        // total_minor = 9_000_000_000_000 (USD, exponent 2) and rate 77197.123456789.
        // Exact ceiling is 116_584_654_932_613 sats (< 1 sat excess); flooring at
        // 8 decimal places would inflate this to 116_584_654_932_626 (+13 sats).
        let exact_rate = Decimal::parse("77197.123456789").expect("9-decimal rate parses");
        assert_eq!(
            quote_sats(9_000_000_000_000, 2, &exact_rate),
            Ok(116_584_654_932_613)
        );
        // 18-decimal rate whose intermediate u128 product would overflow still
        // yields the exact rational ceiling via BigUint arithmetic.
        let eighteen_dec =
            Decimal::parse("77197.123456789012345678").expect("18-decimal rate parses");
        assert_eq!(
            quote_sats(9_000_000_000_000, 2, &eighteen_dec),
            Ok(116_584_654_932_613)
        );

        // High-precision Coinbase rates are preserved losslessly for both
        // numerator and denominator in usd_cents_for.
        let body = br#"{
            "data": {
                "currency": "BTC",
                "rates": {
                    "USD": "100000.000000001",
                    "EUR": "50000.0000000005",
                    "GBP": "50000.00000000000000000000",
                    "CHF": "77197.123456789"
                }
            }
        }"#;
        let snapshot = parse_snapshot_for(FxFeedProvider::Coinbase, body, now, Some(now))
            .expect("coinbase parses");
        let usd = snapshot.sample(Currency::Usd).unwrap();
        let eur = snapshot.sample(Currency::Eur).unwrap();
        let gbp = snapshot.sample(Currency::Gbp).unwrap();
        let chf = snapshot.sample(Currency::Chf).unwrap();
        assert_eq!(usd.rate.to_string_value(), "100000.000000001");
        assert_eq!(eur.rate.to_string_value(), "50000.0000000005");
        assert_eq!(gbp.rate.to_string_value(), "50000.000000000000000000");
        assert_eq!(chf.rate.to_string_value(), "77197.123456789");

        // Exact ratio 100000.000000001 / 50000.0000000005 == 2 exactly.
        assert_eq!(usd_cents_for(1_000, 2, &usd.rate, &eur.rate), Ok(2_000));
        // Exact ratio 100000.000000001 / 50000 * 1000 == 2000.00000000002 (ceil 2001).
        assert_eq!(usd_cents_for(1_000, 2, &usd.rate, &gbp.rate), Ok(2_001));
        assert_eq!(
            quote_sats(9_000_000_000_000, 2, &chf.rate),
            Ok(116_584_654_932_613)
        );
        // 18-decimal cross rate at large order totals stays exact (< 1 cent excess).
        assert_eq!(
            usd_cents_for(
                9_000_000_000_000,
                2,
                &rate("100000.000000000000000001"),
                &rate("50000.000000000000000000")
            ),
            Ok(18_000_000_000_001)
        );

        // Sub-$1 high-precision rates up to 18 decimals parse exactly; non-zero
        // digits beyond 18 decimals fail closed instead of flooring.
        let extreme = br#"{
            "data": {
                "currency": "BTC",
                "rates": {
                    "USD": "0.000000019",
                    "EUR": "77197.1234567890123456789",
                    "GBP": "0.0000000000000000009",
                    "CAD": "0.0000000000000000010000"
                }
            }
        }"#;
        let extreme_snap = parse_snapshot_for(FxFeedProvider::Coinbase, extreme, now, Some(now))
            .expect("coinbase object parses");
        assert_eq!(
            extreme_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "0.000000019"
        );
        assert_eq!(extreme_snap.sample(Currency::Eur), Err(FxError::Malformed));
        assert_eq!(extreme_snap.sample(Currency::Gbp), Err(FxError::Malformed));
        assert_eq!(
            extreme_snap
                .sample(Currency::Cad)
                .unwrap()
                .rate
                .to_string_value(),
            "0.000000000000000001"
        );
    }

    #[test]
    fn usd_cents_refuse_zero_negative_and_overflowing_inputs() {
        let usd = rate("77197");
        let eur = rate("66481");
        assert_eq!(usd_cents_for(0, 2, &usd, &eur), Err(FxError::BelowMinimum));
        assert_eq!(usd_cents_for(-1, 2, &usd, &eur), Err(FxError::Overflow));
        assert_eq!(usd_cents_for(100, -1, &usd, &eur), Err(FxError::Overflow));
        assert_eq!(
            usd_cents_for(100, i32::MAX, &usd, &eur),
            Err(FxError::Overflow)
        );
        let huge_usd = Decimal {
            mantissa: u128::MAX,
            scale: 0,
        };
        assert_eq!(
            usd_cents_for(1, 2, &huge_usd, &eur),
            Err(FxError::Overflow),
            "an unrepresentable numerator fails closed"
        );
        let zero = Decimal {
            mantissa: 0,
            scale: 0,
        };
        assert_eq!(usd_cents_for(1, 2, &usd, &zero), Err(FxError::Overflow));
    }

    #[test]
    fn cross_rate_string_is_the_rounded_ratio_of_the_two_tickers() {
        let cross = CrossRate {
            pair: "EURUSD".to_string(),
            usd_rate: rate("77197"),
            currency_rate: rate("66481"),
            fetched_at: DateTime::from_timestamp(1_789_316_448, 0).unwrap(),
        };
        // 77197 / 66481 = 1.16118891111746..., rounded half up at 12 places.
        assert_eq!(cross.rate_string().as_deref(), Some("1.161188911117"));
        let exact = CrossRate {
            usd_rate: rate("100000"),
            currency_rate: rate("50000"),
            ..cross
        };
        assert_eq!(exact.rate_string().as_deref(), Some("2.000000000000"));
    }

    #[test]
    fn kraken_adapter_parses_all_seven_currencies_and_enforces_trade_timestamp_guards() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();
        let body = format!(
            r#"{{
                "error": [],
                "result": {{
                    "XXBTZUSD": [["77197.50000", "0.01", {current}.1234567, "b", "l", "", 1]],
                    "XXBTZEUR": [["66481.25000", "0.01", {current}, "s", "m", "", 2]],
                    "XBTCHF":   [["62992.00000", "0.01", {current}, "b", "l", "", 3]],
                    "XXBTZGBP": [["57037.00000", "0.01", {current}, "b", "l", "", 4]],
                    "XXBTZJPY": [["11745955", "0.01", {current}, "b", "l", "", 5]],
                    "XXBTZCAD": [["106992.00000", "0.01", {current}, "b", "l", "", 6]],
                    "XBTAUD":   [["107607.00000", "0.01", {current}, "b", "l", "", 7]],
                    "last": "{current}123456700"
                }}
            }}"#
        );
        // Kraken Trades uses per-trade execution timestamps, so HTTP Date is
        // not required and cannot mask stale trades.
        let snapshot = parse_snapshot_for(FxFeedProvider::Kraken, body.as_bytes(), now, None)
            .expect("kraken trades parse");
        let usd = snapshot.sample(Currency::Usd).unwrap();
        assert_eq!(usd.rate.to_string_value(), "77197.50000");
        assert_eq!(
            usd.fetched_at,
            DateTime::from_timestamp(current, 123_456_700).unwrap()
        );
        assert_eq!(
            snapshot
                .sample(Currency::Eur)
                .unwrap()
                .rate
                .to_string_value(),
            "66481.25000"
        );
        assert_eq!(
            snapshot
                .sample(Currency::Chf)
                .unwrap()
                .rate
                .to_string_value(),
            "62992.00000"
        );
        assert_eq!(
            snapshot
                .sample(Currency::Jpy)
                .unwrap()
                .rate
                .to_string_value(),
            "11745955"
        );
        assert_eq!(
            snapshot
                .sample(Currency::Aud)
                .unwrap()
                .rate
                .to_string_value(),
            "107607.00000"
        );

        // Non-empty error list fails closed.
        let errored = br#"{"error":["EQuery:Unknown asset pair"],"result":{}}"#;
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Kraken, errored, now, Some(now)),
            Err(FxError::Malformed)
        );
        // Fresh HTTP Date header never masks an inactive pair's stale trade
        // (> 300s old), empty trade list, or future-skewed trade (> 60s ahead).
        let guarded = format!(
            r#"{{
                "error": [],
                "result": {{
                    "XXBTZUSD": [["77197.00", "0.01", {}, "b", "l", "", 1]],
                    "XXBTZEUR": [["66481.00", "0.01", {}, "b", "l", "", 2]],
                    "XBTCHF": [],
                    "XXBTZGBP": [["57037.00", "0.01", {current}, "b", "l", "", 3], ["57038.00", "0.01", {}, "b", "l", "", 4]],
                    "XXBTZJPY": [["0", "0.01", {current}, "b", "l", "", 5]]
                }}
            }}"#,
            current - 301,
            current + 61,
            current - 1,
        );
        let guarded_snap =
            parse_snapshot_for(FxFeedProvider::Kraken, guarded.as_bytes(), now, Some(now))
                .expect("envelope parses");
        assert_eq!(guarded_snap.sample(Currency::Usd), Err(FxError::Stale));
        assert_eq!(guarded_snap.sample(Currency::Eur), Err(FxError::Future));
        assert_eq!(guarded_snap.sample(Currency::Chf), Err(FxError::Malformed));
        assert_eq!(
            guarded_snap.sample(Currency::Gbp),
            Err(FxError::Malformed),
            "out-of-order trade timestamps fail closed"
        );
        assert_eq!(
            guarded_snap.sample(Currency::Jpy),
            Err(FxError::OutOfBounds)
        );

        // Old /0/public/Ticker object schema (no trade timestamp) fails closed.
        let ticker_shape = br#"{"error":[],"result":{"XXBTZUSD":{"c":["77197","0.01"]}}}"#;
        let ticker_snap = parse_snapshot_for(FxFeedProvider::Kraken, ticker_shape, now, Some(now))
            .expect("envelope parses");
        assert_eq!(ticker_snap.sample(Currency::Usd), Err(FxError::Malformed));

        // Duplicate aliases for one currency fail closed.
        let duplicate = format!(
            r#"{{"error":[],"result":{{"XXBTZUSD":[["77197","0.01",{current}]],"XBTUSD":[["77198","0.01",{current}]]}}}}"#
        );
        let dup_snap = parse_snapshot_for(FxFeedProvider::Kraken, duplicate.as_bytes(), now, None)
            .expect("snapshot parses");
        assert_eq!(dup_snap.sample(Currency::Usd), Err(FxError::Malformed));
    }

    #[test]
    fn coinbase_adapter_parses_exchange_rates_and_spot_and_enforces_guards() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let body = br#"{
            "data": {
                "currency": "BTC",
                "rates": {
                    "USD": "77197.50",
                    "EUR": "66481.123456789012",
                    "CHF": "62992",
                    "GBP": "57037",
                    "JPY": "11745955",
                    "CAD": "106992",
                    "AUD": "107607"
                }
            }
        }"#;
        let snapshot = parse_snapshot_for(FxFeedProvider::Coinbase, body, now, Some(now))
            .expect("coinbase exchange-rates parses");
        assert_eq!(
            snapshot
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197.50"
        );
        let eur = snapshot.sample(Currency::Eur).unwrap();
        assert_eq!(eur.rate.to_string_value(), "66481.123456789012");
        assert_eq!(
            snapshot
                .sample(Currency::Jpy)
                .unwrap()
                .rate
                .to_string_value(),
            "11745955"
        );

        // Missing HTTP Date header fails closed.
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Coinbase, body, now, None),
            Err(FxError::Malformed)
        );

        // Single-pair spot response works for its currency and fails others.
        let spot = br#"{"data":{"base":"BTC","currency":"USD","amount":"77197.25"}}"#;
        let spot_snap = parse_snapshot_for(FxFeedProvider::Coinbase, spot, now, Some(now))
            .expect("coinbase spot parses");
        assert_eq!(
            spot_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197.25"
        );
        assert_eq!(spot_snap.sample(Currency::Eur), Err(FxError::Malformed));

        // Wrong base currency fails closed.
        let eth = br#"{"data":{"currency":"ETH","rates":{"USD":"3000"}}}"#;
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Coinbase, eth, now, Some(now)),
            Err(FxError::Malformed)
        );
        // Stale and future HTTP Date headers are rejected.
        assert_eq!(
            parse_snapshot_for(
                FxFeedProvider::Coinbase,
                body,
                now,
                Some(now - Duration::seconds(301))
            ),
            Err(FxError::Stale)
        );
        assert_eq!(
            parse_snapshot_for(
                FxFeedProvider::Coinbase,
                body,
                now,
                Some(now + Duration::seconds(61))
            ),
            Err(FxError::Future)
        );
    }

    #[test]
    fn mempool_adapter_parses_all_seven_currencies_and_enforces_timestamp_and_bounds() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();
        let body = format!(
            r#"{{"time":{current},"USD":77197,"EUR":66481,"GBP":57037,"CAD":106992,"CHF":62992,"AUD":107607,"JPY":11745955}}"#
        );
        let snapshot = parse_snapshot_for(FxFeedProvider::Mempool, body.as_bytes(), now, None)
            .expect("mempool parses");
        assert_eq!(
            snapshot
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197"
        );
        assert_eq!(
            snapshot
                .sample(Currency::Jpy)
                .unwrap()
                .rate
                .to_string_value(),
            "11745955"
        );
        assert_eq!(
            snapshot.sample(Currency::Usd).unwrap().fetched_at,
            DateTime::from_timestamp(current, 0).unwrap()
        );

        let stale = format!(r#"{{"time":{},"USD":77197}}"#, current - 301);
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Mempool, stale.as_bytes(), now, None),
            Err(FxError::Stale)
        );
        let future = format!(r#"{{"time":{},"USD":77197}}"#, current + 61);
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Mempool, future.as_bytes(), now, None),
            Err(FxError::Future)
        );
        let zero = format!(r#"{{"time":{current},"USD":0,"EUR":-10}}"#);
        let zero_snap = parse_snapshot_for(FxFeedProvider::Mempool, zero.as_bytes(), now, None)
            .expect("object parses");
        assert_eq!(zero_snap.sample(Currency::Usd), Err(FxError::OutOfBounds));
        assert_eq!(zero_snap.sample(Currency::Eur), Err(FxError::Malformed));
    }

    #[test]
    fn bitstamp_adapter_supports_usd_eur_gbp_isolates_altcoins_and_reports_unsupported_fiat() {
        assert_eq!(
            FxFeedProvider::Bitstamp.supported_currencies(),
            &[Currency::Usd, Currency::Eur, Currency::Gbp]
        );
        for supported in [
            Currency::Usd,
            Currency::Eur,
            Currency::Gbp,
            Currency::Btc,
            Currency::Sat,
        ] {
            assert!(FxFeedProvider::Bitstamp.supports_currency(supported));
        }
        for unsupported in [Currency::Chf, Currency::Jpy, Currency::Cad, Currency::Aud] {
            assert!(!FxFeedProvider::Bitstamp.supports_fiat(unsupported));
            assert!(!FxFeedProvider::Bitstamp.supports_currency(unsupported));
        }
        for full_provider in [
            FxFeedProvider::Blocktank,
            FxFeedProvider::Kraken,
            FxFeedProvider::Coinbase,
            FxFeedProvider::Mempool,
        ] {
            assert_eq!(full_provider.supported_currencies(), &Currency::FIAT);
            for currency in Currency::FIAT {
                assert!(full_provider.supports_fiat(currency));
                assert!(full_provider.supports_currency(currency));
            }
            assert!(full_provider.supports_currency(Currency::Btc));
            assert!(full_provider.supports_currency(Currency::Sat));
        }

        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();
        // Array response containing malformed unrelated altcoin objects alongside
        // valid BTC/USD and BTC/EUR and stale BTC/GBP. CHF/JPY/CAD/AUD are
        // outside Bitstamp's market inventory and report UnsupportedCurrency.
        let body = format!(
            r#"[
                {{"pair":"ETH/USD","last":null}},
                {{"broken_entry":true}},
                12345,
                {{"pair":"BTC/USD","last":"77197","timestamp":"{current}"}},
                {{"pair":"BTC/EUR","last":"66481.5","timestamp":"{current}"}},
                {{"pair":"BTC/GBP","last":"57037","timestamp":"{}"}}
            ]"#,
            current - 301
        );
        let snapshot = parse_snapshot_for(FxFeedProvider::Bitstamp, body.as_bytes(), now, None)
            .expect("bitstamp array isolates unrelated malformed entries");
        assert_eq!(snapshot.samples().count(), 3);
        assert_eq!(
            snapshot
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197"
        );
        assert_eq!(
            snapshot
                .sample(Currency::Eur)
                .unwrap()
                .rate
                .to_string_value(),
            "66481.5"
        );
        assert_eq!(snapshot.sample(Currency::Gbp), Err(FxError::Stale));
        for unsupported in [Currency::Chf, Currency::Jpy, Currency::Cad, Currency::Aud] {
            assert_eq!(
                snapshot.sample(unsupported),
                Err(FxError::UnsupportedCurrency),
                "{unsupported}"
            );
        }

        // Real Bitstamp single-pair response omits "pair"; currency comes from
        // the validated /api/v2/ticker/btcusd/ path.
        let real_single = format!(
            r#"{{"timestamp":"{current}","open":"77000","high":"78000","low":"76500","last":"77197","volume":"12.3","vwap":"77100","bid":"77196","ask":"77198","side":"0","open_24":"77000","percent_change_24":"0.26"}}"#
        );
        let single_snap = parse_snapshot_for_url(
            FxFeedProvider::Bitstamp,
            "https://www.bitstamp.net/api/v2/ticker/btcusd/",
            real_single.as_bytes(),
            now,
            None,
        )
        .expect("real single-pair bitstamp response parses via URL currency hint");
        assert_eq!(
            single_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197"
        );
        assert_eq!(
            single_snap.sample(Currency::Eur),
            Err(FxError::UnsupportedCurrency)
        );
        assert_eq!(
            single_snap.sample(Currency::Chf),
            Err(FxError::UnsupportedCurrency)
        );

        // Unsupported Bitstamp single-pair URL or object fails closed with
        // UnsupportedCurrency.
        assert_eq!(
            parse_snapshot_for_url(
                FxFeedProvider::Bitstamp,
                "https://www.bitstamp.net/api/v2/ticker/btcchf/",
                real_single.as_bytes(),
                now,
                None,
            ),
            Err(FxError::UnsupportedCurrency)
        );

        // Single-pair object without "pair" on the all-pairs URL fails closed.
        assert_eq!(
            parse_snapshot_for_url(
                FxFeedProvider::Bitstamp,
                BITSTAMP_FX_URL,
                real_single.as_bytes(),
                now,
                None,
            ),
            Err(FxError::Malformed)
        );
    }

    #[test]
    fn http_date_parser_handles_imf_fixdate_gmt() {
        let parsed = parse_http_date("Fri, 27 Nov 2026 18:45:00 GMT").expect("valid GMT date");
        assert_eq!(
            parsed,
            "2026-11-27T18:45:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(parse_http_date("not-a-date"), None);
    }

    #[test]
    fn fx_feed_lookup_pins_marketplace_and_opens_single_seller_mode() {
        let seller = marketplace_domain::pubky::encode_pubky(&[9; 32]);
        let marketplace = StoreMode::Marketplace;
        let single = StoreMode::SingleSeller {
            pinned: Some(seller),
        };
        let claim_mode = StoreMode::SingleSeller { pinned: None };
        let lookup = |pairs: &[(&str, &str)]| {
            let owned: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            move |name: &str| {
                owned
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
            }
        };

        // Marketplace defaults to pinned Blocktank and accepts explicit pinned values.
        assert_eq!(
            fx_feed_from_lookup(&lookup(&[]), &marketplace).unwrap(),
            FxFeedConfig::pinned_blocktank()
        );
        assert_eq!(
            fx_feed_from_lookup(
                &lookup(&[
                    (ENV_FX_FEED_PROVIDER, "blocktank"),
                    (ENV_FX_FEED_URL, FX_URL)
                ]),
                &marketplace
            )
            .unwrap(),
            FxFeedConfig::pinned_blocktank()
        );
        // Marketplace refuses custom URLs and non-Blocktank providers.
        let url_err = fx_feed_from_lookup(
            &lookup(&[(ENV_FX_FEED_URL, "https://attacker.example/fx")]),
            &marketplace,
        )
        .expect_err("marketplace refuses custom URL");
        assert!(
            url_err.to_string().contains("STORE_MODE=single_seller")
                && url_err.to_string().contains("STORE_SELLER_PUBKY"),
            "{url_err}"
        );
        let provider_err =
            fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_PROVIDER, "kraken")]), &marketplace)
                .expect_err("marketplace refuses non-blocktank provider");
        assert!(
            provider_err
                .to_string()
                .contains("STORE_MODE=single_seller")
                && provider_err.to_string().contains("STORE_SELLER_PUBKY"),
            "{provider_err}"
        );

        // Single-seller claim mode (`STORE_MODE=single_seller`, no pin) also
        // honors custom URLs and built-in providers identically to pinned mode.
        let claim_default = fx_feed_from_lookup(&lookup(&[]), &claim_mode).unwrap();
        assert!(claim_default.is_pinned());
        let claim_kraken = fx_feed_from_lookup(
            &lookup(&[(
                ENV_FX_FEED_URL,
                "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&count=1",
            )]),
            &claim_mode,
        )
        .unwrap();
        assert_eq!(claim_kraken.provider, FxFeedProvider::Kraken);
        assert_eq!(claim_kraken.supported_currencies(), &[Currency::Usd]);
        assert!(!claim_kraken.is_pinned());
        let claim_custom = fx_feed_from_lookup(
            &lookup(&[(ENV_FX_FEED_URL, "https://fx.claim.example/rates")]),
            &claim_mode,
        )
        .unwrap();
        assert_eq!(claim_custom.provider, FxFeedProvider::Blocktank);
        assert_eq!(claim_custom.url, "https://fx.claim.example/rates");
        assert!(!claim_custom.is_pinned());

        // Single-seller defaults to pinned Blocktank when unset.
        let default_single = fx_feed_from_lookup(&lookup(&[]), &single).unwrap();
        assert!(default_single.is_pinned());

        // Single-seller honors a custom Blocktank-format https URL and loopback http URLs.
        let custom = fx_feed_from_lookup(
            &lookup(&[(ENV_FX_FEED_URL, "https://fx.myshop.example/rates")]),
            &single,
        )
        .unwrap();
        assert_eq!(custom.provider, FxFeedProvider::Blocktank);
        assert_eq!(custom.url, "https://fx.myshop.example/rates");
        assert!(!custom.is_pinned());

        for loopback_url in [
            "http://127.0.0.1:8999/api/v1/prices",
            "http://localhost:8999/api/v1/prices",
            "http://[::1]:8999/api/v1/prices",
        ] {
            let resolved = fx_feed_from_lookup(
                &lookup(&[
                    (ENV_FX_FEED_PROVIDER, "mempool"),
                    (ENV_FX_FEED_URL, loopback_url),
                ]),
                &single,
            )
            .unwrap_or_else(|e| panic!("{loopback_url} must be accepted: {e}"));
            assert_eq!(resolved.provider, FxFeedProvider::Mempool);
            assert_eq!(resolved.url, loopback_url);
        }

        // Single-seller honors built-in provider names without needing a URL.
        for provider in FxFeedProvider::ALL {
            let resolved = fx_feed_from_lookup(
                &lookup(&[(ENV_FX_FEED_PROVIDER, provider.as_str())]),
                &single,
            )
            .unwrap();
            assert_eq!(resolved.provider, provider);
            assert_eq!(resolved.url, provider.default_url());
            assert_eq!(resolved.is_pinned(), provider == FxFeedProvider::Blocktank);
        }

        // Single-seller infers provider from well-known exchange URLs.
        let inferred_mempool =
            fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, MEMPOOL_FX_URL)]), &single).unwrap();
        assert_eq!(inferred_mempool.provider, FxFeedProvider::Mempool);
        let inferred_coinbase =
            fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, COINBASE_FX_URL)]), &single).unwrap();
        assert_eq!(inferred_coinbase.provider, FxFeedProvider::Coinbase);
        let inferred_kraken =
            fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, KRAKEN_FX_URL)]), &single).unwrap();
        assert_eq!(inferred_kraken.provider, FxFeedProvider::Kraken);
        let kraken_single_pair = fx_feed_from_lookup(
            &lookup(&[(
                ENV_FX_FEED_URL,
                "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&count=1",
            )]),
            &single,
        )
        .unwrap();
        assert_eq!(kraken_single_pair.provider, FxFeedProvider::Kraken);
        assert_eq!(
            kraken_single_pair.supported_currencies(),
            &[Currency::Usd],
            "one Kraken pair quotes one fiat only"
        );

        // Supported Bitstamp single-pair URLs are accepted; unsupported
        // Bitstamp markets and Kraken /Ticker or multi-pair Trades URLs are
        // refused at boot.
        for good_bitstamp in [
            "https://www.bitstamp.net/api/v2/ticker/btcusd/",
            "https://www.bitstamp.net/api/v2/ticker/btceur/",
            "https://www.bitstamp.net/api/v2/ticker/btcgbp/",
        ] {
            let resolved =
                fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, good_bitstamp)]), &single)
                    .unwrap_or_else(|e| panic!("{good_bitstamp} must be accepted: {e}"));
            assert_eq!(resolved.provider, FxFeedProvider::Bitstamp);
            let expected_fiat: &[Currency] = match good_bitstamp {
                u if u.contains("btcusd") => &[Currency::Usd],
                u if u.contains("btceur") => &[Currency::Eur],
                u if u.contains("btcgbp") => &[Currency::Gbp],
                _ => unreachable!("unexpected Bitstamp fixture URL"),
            };
            assert_eq!(resolved.supported_currencies(), expected_fiat);
        }
        for bad_provider_url in [
            "https://api1.blocktank.to/api/fx/rates/btc?cache=1",
            "https://api1.blocktank.to/api/fx/rates/not-btc",
            "https://api1.blocktank.to/not-an-rates-endpoint",
            "https://mempool.space/api/v1/prices?foo=bar",
            "https://mempool.space/api/v1/not-prices",
            "https://mempool.space/not-the-prices-endpoint",
            "https://api.coinbase.com/v2/exchange-rates",
            "https://api.coinbase.com/v2/exchange-rates/",
            "https://api.coinbase.com/v2/exchange-rates?",
            "https://API.COINBASE.COM/v2/exchange-rates",
            "https://api.coinbase.com./v2/exchange-rates",
            "https://api.coinbase.com/v2/exchange-rates?currency=btc",
            "https://api.coinbase.com/v2/exchange-rates?currency=USD",
            "https://api.coinbase.com/v2/exchange-rates?currency=BTC&currency=BTC",
            "https://api.coinbase.com/v2/exchange-rates/extra",
            "https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD",
            "https://api.kraken.com/0/public/OHLC?pair=XXBTZUSD",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD,XXBTZEUR",
            "https://api.kraken.com/0/public/Trades?since=0",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&since=0",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&SINCE=1700000000000000000",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&pair=XXBTZEUR",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&foo=bar",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&count=500",
            "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD&count=1&count=1",
            "https://api.kraken.com/0/public/Trades?PAIR=XXBTZUSD&count=1",
            "https://api.kraken.com/0/public/Trades?pair=",
            "https://www.bitstamp.net/api/v2/ticker/btcchf/",
            "https://www.bitstamp.net/api/v2/ticker/btcjpy/",
            "https://www.bitstamp.net/api/v2/ticker/btccad/",
            "https://www.bitstamp.net/api/v2/ticker/btcaud/",
            "https://www.bitstamp.net/api/v2/ticker/btcsek/",
            "https://www.bitstamp.net/api/v2/ticker/ethusd/",
            "https://www.bitstamp.net/api/v2/ohlc/btcusd/",
        ] {
            assert!(
                fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, bad_provider_url)]), &single)
                    .is_err(),
                "{bad_provider_url:?} must be refused"
            );
        }

        // kraken_pair_url strips `since`, `pair`, and `count` before setting
        // the canonical single-pair count=1 query.
        let dirty =
            Url::parse("https://api.kraken.com/0/public/Trades?since=0&foo=bar&count=500").unwrap();
        let cleaned = kraken_pair_url(&dirty, Currency::Usd).unwrap();
        let pairs: Vec<(String, String)> = cleaned
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("pair".to_string(), "XXBTZUSD".to_string()),
                ("count".to_string(), "1".to_string()),
            ]
        );

        // Non-loopback plaintext HTTP and metadata/private/link-local literals fail closed.
        for bad_url in [
            " ",
            "ftp://example.com/fx",
            "http://fx.myshop.example/rates",
            "http://api.kraken.com/0/public/Trades",
            "http://169.254.169.254/latest/meta-data/",
            "https://169.254.169.254/latest/meta-data/",
            "https://[::ffff:169.254.169.254]/latest/meta-data/",
            "https://[::169.254.169.254]/latest/meta-data/",
            "https://[64:ff9b::169.254.169.254]/latest/meta-data/",
            "https://[fe80::1]/fx",
            "https://[fd00:ec2::254]/latest/meta-data/",
            "https://0.0.0.0/fx",
            "https://[::]/fx",
            "https://224.0.0.1/fx",
            "https://[ff02::1]/fx",
            "https://10.0.0.1/fx",
            "https://192.168.1.1/fx",
            "https://user:pass@example.com/fx",
            "https://example.com/fx#frag",
            "not-a-url",
        ] {
            assert!(
                fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_URL, bad_url)]), &single).is_err(),
                "{bad_url:?} must be refused"
            );
        }
        for bad_provider in [" ", "unknown", "Kraken"] {
            assert!(
                fx_feed_from_lookup(&lookup(&[(ENV_FX_FEED_PROVIDER, bad_provider)]), &single)
                    .is_err(),
                "{bad_provider:?}"
            );
        }
    }

    #[tokio::test]
    async fn fetch_body_rejects_redirects_without_following_target() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let target_body = format!(r#"{{"time":{},"USD":77197}}"#, now.timestamp());
        let target_raw = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{target_body}",
            target_body.len()
        );
        let (target_url, target_hits, _) = spawn_raw_http_server(target_raw.into_bytes()).await;

        let redirect_raw = format!(
            "HTTP/1.1 302 Found\r\nLocation: {target_url}/api/v1/prices\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let (redirect_url, redirect_hits, _) =
            spawn_raw_http_server(redirect_raw.into_bytes()).await;

        let result = fetch_snapshot_for(
            FxFeedProvider::Mempool,
            &format!("{redirect_url}/api/v1/prices"),
            now,
        )
        .await;
        assert_eq!(result, Err(FxError::Malformed));
        assert_eq!(redirect_hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            target_hits.load(AtomicOrdering::SeqCst),
            0,
            "redirect target must never be contacted"
        );
    }

    #[tokio::test]
    async fn fetch_body_enforces_one_mib_cap_with_and_without_content_length() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let oversized = vec![b'x'; MAX_FX_RESPONSE_BYTES + 1];

        // Case 1: explicit Content-Length above 1 MiB.
        let mut with_cl = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            oversized.len()
        )
        .into_bytes();
        with_cl.extend_from_slice(&oversized);
        let (url_cl, _, _) = spawn_raw_http_server(with_cl).await;
        assert_eq!(
            fetch_snapshot_for(
                FxFeedProvider::Mempool,
                &format!("{url_cl}/api/v1/prices"),
                now
            )
            .await,
            Err(FxError::Malformed)
        );

        // Case 2: chunked Transfer-Encoding (no Content-Length) exceeding 1 MiB.
        let chunk_size = 65_536;
        let mut chunked =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        for piece in oversized.chunks(chunk_size) {
            chunked.extend_from_slice(format!("{:x}\r\n", piece.len()).as_bytes());
            chunked.extend_from_slice(piece);
            chunked.extend_from_slice(b"\r\n");
        }
        chunked.extend_from_slice(b"0\r\n\r\n");
        let (url_chunked, _, _) = spawn_raw_http_server(chunked).await;
        assert_eq!(
            fetch_snapshot_for(
                FxFeedProvider::Mempool,
                &format!("{url_chunked}/api/v1/prices"),
                now,
            )
            .await,
            Err(FxError::Malformed)
        );
    }

    #[tokio::test]
    async fn fetch_body_fails_closed_on_missing_duplicate_malformed_stale_date_and_age_headers() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let fresh_date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let stale_date = (now - Duration::seconds(301))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let future_date = (now + Duration::seconds(61))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let coinbase_body = r#"{"data":{"currency":"BTC","rates":{"USD":"77197"}}}"#;

        let cases: Vec<(&str, String, FxError)> = vec![
            (
                "missing Date",
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Malformed,
            ),
            (
                "duplicate Date",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nDate: {fresh_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Malformed,
            ),
            (
                "malformed Date",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: not-a-date\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Malformed,
            ),
            (
                "stale Date",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: {stale_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Stale,
            ),
            (
                "future Date",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: {future_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Future,
            ),
            (
                "stale Age header",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nAge: 301\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Stale,
            ),
            (
                "malformed Age header",
                format!(
                    "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nAge: bad\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
                    coinbase_body.len()
                ),
                FxError::Malformed,
            ),
        ];

        for (label, raw, expected) in cases {
            let (url, _, _) = spawn_raw_http_server(raw.into_bytes()).await;
            let got = fetch_snapshot_for(
                FxFeedProvider::Coinbase,
                &format!("{url}/v2/exchange-rates"),
                now,
            )
            .await;
            assert_eq!(got, Err(expected), "{label}");
        }

        // Valid single Date + valid Age succeeds and sends Cache-Control: no-cache.
        let ok_raw = format!(
            "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nAge: 10\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{coinbase_body}",
            coinbase_body.len()
        );
        let (ok_url, _, last_req) = spawn_raw_http_server(ok_raw.into_bytes()).await;
        let snap = fetch_snapshot_for(
            FxFeedProvider::Coinbase,
            &format!("{ok_url}/v2/exchange-rates"),
            now,
        )
        .await
        .expect("single fresh Date and Age <= 300 succeeds");
        assert_eq!(
            snap.sample(Currency::Usd).unwrap().rate.to_string_value(),
            "77197"
        );
        assert!(
            last_req
                .lock()
                .unwrap()
                .to_ascii_lowercase()
                .contains("cache-control: no-cache"),
            "outbound FX request must send Cache-Control: no-cache"
        );
    }

    #[tokio::test]
    async fn fetch_kraken_trades_queries_per_pair_and_rejects_inactive_pair_despite_fresh_http_date(
    ) {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();
        let fresh_date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("kraken trades server binds");
        let addr = listener.local_addr().unwrap();
        let seen_paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen_bg = seen_paths.clone();

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let target = req
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                seen_bg.lock().unwrap().push(target.clone());

                let body = if target.contains("pair=XXBTZUSD") {
                    format!(
                        r#"{{"error":[],"result":{{"XXBTZUSD":[["77197.123456789","0.01",{}.25,"b","l","",1]],"last":"1"}}}}"#,
                        current - 5
                    )
                } else if target.contains("pair=XXBTZEUR") {
                    format!(
                        r#"{{"error":[],"result":{{"XXBTZEUR":[["66481.5","0.01",{},"b","l","",2]],"last":"2"}}}}"#,
                        current - 2
                    )
                } else if target.contains("pair=XBTCHF") {
                    r#"{"error":[],"result":{"XBTCHF":[],"last":"0"}}"#.to_string()
                } else if target.contains("pair=XXBTZGBP") {
                    format!(
                        r#"{{"error":[],"result":{{"XXBTZGBP":[["57037","0.01",{},"b","l","",3]],"last":"3"}}}}"#,
                        current + 61
                    )
                } else if target.contains("pair=XXBTZJPY") {
                    // Inactive pair: last trade executed 1 hour ago, even though
                    // HTTP Date is fresh.
                    format!(
                        r#"{{"error":[],"result":{{"XXBTZJPY":[["11745955","0.01",{},"b","l","",4]],"last":"4"}}}}"#,
                        current - 3600
                    )
                } else if target.contains("pair=XXBTZCAD") {
                    format!(
                        r#"{{"error":[],"result":{{"XXBTZCAD":[["106992","0.01",{current},"b","l","",5]],"last":"5"}}}}"#
                    )
                } else if target.contains("pair=XBTAUD") {
                    format!(
                        r#"{{"error":[],"result":{{"XBTAUD":[["107607","0.01",{current},"b","l","",6]],"last":"6"}}}}"#
                    )
                } else {
                    r#"{"error":["EQuery:Unknown asset pair"],"result":{}}"#.to_string()
                };

                let raw = format!(
                    "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(raw.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let base_url = format!("http://{addr}/0/public/Trades");
        let snap = fetch_snapshot_for(FxFeedProvider::Kraken, &base_url, now)
            .await
            .expect("kraken multi-pair trades snapshot succeeds");

        let usd = snap.sample(Currency::Usd).expect("fresh USD trade");
        assert_eq!(usd.rate.to_string_value(), "77197.123456789");
        assert_eq!(
            usd.fetched_at,
            DateTime::from_timestamp(current - 5, 250_000_000).unwrap(),
            "fetched_at must come from the trade timestamp, never HTTP Date"
        );
        assert_eq!(
            snap.sample(Currency::Eur).unwrap().rate.to_string_value(),
            "66481.5"
        );
        assert_eq!(snap.sample(Currency::Chf), Err(FxError::Malformed));
        assert_eq!(snap.sample(Currency::Gbp), Err(FxError::Future));
        assert_eq!(
            snap.sample(Currency::Jpy),
            Err(FxError::Stale),
            "inactive pair stale trade must fail closed despite fresh HTTP Date"
        );
        assert_eq!(
            snap.sample(Currency::Cad).unwrap().rate.to_string_value(),
            "106992"
        );
        assert_eq!(
            snap.sample(Currency::Aud).unwrap().rate.to_string_value(),
            "107607"
        );

        let recorded = seen_paths.lock().unwrap().clone();
        assert_eq!(recorded.len(), 7);
        for path in &recorded {
            assert!(
                path.contains("count=1"),
                "every Kraken Trades query must request count=1: {path}"
            );
        }

        // Single-pair custom URL with explicit `?pair=` fetches once only.
        seen_paths.lock().unwrap().clear();
        let single_url = format!("{base_url}?pair=XXBTZUSD&count=1");
        let single = fetch_snapshot_for(FxFeedProvider::Kraken, &single_url, now)
            .await
            .expect("explicit single-pair Kraken Trades URL parses");
        assert_eq!(
            single.sample(Currency::Usd).unwrap().rate.to_string_value(),
            "77197.123456789"
        );
        assert_eq!(
            single.sample(Currency::Eur),
            Err(FxError::UnsupportedCurrency)
        );
        assert_eq!(seen_paths.lock().unwrap().len(), 1);
    }

    #[test]
    fn json_numeric_rates_and_timestamps_parse_lexically_without_f64_rounding() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();

        // Mempool raw JSON numbers: 18 fractional digits parse losslessly
        // (whereas f64 would round after ~15 significant digits); 19 non-zero
        // fractional digits fail closed; trailing zeros past 18 strip cleanly.
        let mempool_body = format!(
            r#"{{
                "time": {current},
                "USD": 77197.123456789012345678,
                "EUR": 77197.1234567890123456789,
                "GBP": 50000.00000000000000000000,
                "CHF": 77197.123456789
            }}"#
        );
        let mempool_snap =
            parse_snapshot_for(FxFeedProvider::Mempool, mempool_body.as_bytes(), now, None)
                .expect("mempool object parses");
        assert_eq!(
            mempool_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197.123456789012345678"
        );
        assert_eq!(
            mempool_snap.sample(Currency::Eur),
            Err(FxError::Malformed),
            "19 non-zero fractional digits in a JSON number must fail closed"
        );
        assert_eq!(
            mempool_snap
                .sample(Currency::Gbp)
                .unwrap()
                .rate
                .to_string_value(),
            "50000.000000000000000000"
        );
        assert_eq!(
            quote_sats(
                9_000_000_000_000,
                2,
                &mempool_snap.sample(Currency::Chf).unwrap().rate
            ),
            Ok(116_584_654_932_613)
        );

        // Coinbase raw JSON numbers preserve 18 fractional digits and reject 19.
        let coinbase_body = br#"{
            "data": {
                "currency": "BTC",
                "rates": {
                    "USD": 100000.000000000000000001,
                    "EUR": 50000.000000000000000000,
                    "GBP": 77197.1234567890123456789
                }
            }
        }"#;
        let coinbase_snap =
            parse_snapshot_for(FxFeedProvider::Coinbase, coinbase_body, now, Some(now))
                .expect("coinbase numeric payload parses");
        assert_eq!(
            coinbase_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "100000.000000000000000001"
        );
        assert_eq!(
            usd_cents_for(
                9_000_000_000_000,
                2,
                &coinbase_snap.sample(Currency::Usd).unwrap().rate,
                &coinbase_snap.sample(Currency::Eur).unwrap().rate,
            ),
            Ok(18_000_000_000_001)
        );
        assert_eq!(coinbase_snap.sample(Currency::Gbp), Err(FxError::Malformed));

        // Bitstamp raw JSON numbers preserve 18 fractional digits and reject 19.
        let bitstamp_body = format!(
            r#"[
                {{"pair":"BTC/USD","last":77197.123456789012345678,"timestamp":{current}}},
                {{"pair":"BTC/EUR","last":77197.1234567890123456789,"timestamp":{current}}}
            ]"#
        );
        let bitstamp_snap = parse_snapshot_for(
            FxFeedProvider::Bitstamp,
            bitstamp_body.as_bytes(),
            now,
            None,
        )
        .expect("bitstamp array parses");
        assert_eq!(
            bitstamp_snap
                .sample(Currency::Usd)
                .unwrap()
                .rate
                .to_string_value(),
            "77197.123456789012345678"
        );
        assert_eq!(bitstamp_snap.sample(Currency::Eur), Err(FxError::Malformed));

        // Kraken numeric trade timestamp with 19 total digits (9 fractional
        // nanosecond digits) preserves exact nanoseconds without f64 rounding.
        let kraken_body = format!(
            r#"{{
                "error": [],
                "result": {{
                    "XXBTZUSD": [[77197.123456789012345678, "0.01", {current}.123456789, "b", "l", "", 1]],
                    "XXBTZEUR": [[77197.1234567890123456789, "0.01", {current}.123456789, "b", "l", "", 2]]
                }}
            }}"#
        );
        let kraken_snap =
            parse_snapshot_for(FxFeedProvider::Kraken, kraken_body.as_bytes(), now, None)
                .expect("kraken envelope parses");
        let usd = kraken_snap.sample(Currency::Usd).unwrap();
        assert_eq!(usd.rate.to_string_value(), "77197.123456789012345678");
        assert_eq!(
            usd.fetched_at,
            DateTime::from_timestamp(current, 123_456_789).unwrap(),
            "numeric trade timestamp must preserve exact nanoseconds without f64 rounding"
        );
        assert_eq!(kraken_snap.sample(Currency::Eur), Err(FxError::Malformed));
    }

    #[test]
    fn u320_boundary_arithmetic_handles_carry_borrow_overflow_and_division() {
        let max = U320([u64::MAX; 5]);
        let one = U320::from_u64(1);
        let two = U320::from_u64(2);

        // Multi-limb carry propagation and top-limb overflow.
        let carry_in = U320([u64::MAX, u64::MAX, 0, 0, 0]);
        assert_eq!(carry_in.checked_add(one), Some(U320([0, 0, 1, 0, 0])));
        assert_eq!(max.checked_add(one), None);

        // Multi-limb borrow propagation and underflow.
        assert_eq!(U320([0, 0, 1, 0, 0]).checked_sub(one), Some(carry_in));
        assert_eq!(U320::ZERO.checked_sub(one), None);
        assert_eq!(max.checked_sub(max), Some(U320::ZERO));

        // High-limb multiply overflow (product beyond limb 4 and carry out of limb 4).
        assert_eq!(
            U320([0, 0, 0, 1, 0]).checked_mul(U320([0, 0, 1, 0, 0])),
            None
        );
        assert_eq!(U320([0, 0, 0, 0, 1 << 63]).checked_mul(two), None);
        assert_eq!(
            U320([0, 0, 0, 0, (1 << 63) - 1]).checked_mul(two),
            Some(U320([0, 0, 0, 0, u64::MAX - 1]))
        );

        // Division by zero fails closed.
        assert_eq!(one.div_rem(U320::ZERO), None);
        assert_eq!(one.div_ceil(U320::ZERO), None);

        // Top-bit divisors and full 320-bit dividend invariants: q * d + r == n, r < d.
        for divisor in [
            U320([0, 0, 0, 0, 1 << 63]),
            U320([1, 0, 0, 0, 1 << 63]),
            U320([5, 0, 0, 0, 1 << 63]),
            U320([u64::MAX, u64::MAX, u64::MAX, u64::MAX, (1 << 63) | 7]),
            max,
        ] {
            let (q, r) = max.div_rem(divisor).expect("non-zero divisor");
            assert!(r < divisor);
            assert_eq!(
                q.checked_mul(divisor).and_then(|p| p.checked_add(r)),
                Some(max)
            );
            let ceil = max.div_ceil(divisor).expect("div_ceil succeeds");
            if r.is_zero() {
                assert_eq!(ceil, q);
            } else {
                assert_eq!(ceil, q.checked_add(one).unwrap());
            }
        }
        // div_ceil by 1 at max does not overflow when remainder is zero.
        assert_eq!(max.div_ceil(one), Some(max));
        // div_rem when dividend < divisor yields quotient 0 and full remainder.
        let below = U320([u64::MAX, u64::MAX, u64::MAX, u64::MAX, (1 << 63) - 1]);
        let high = U320([1, 0, 0, 0, 1 << 63]);
        assert_eq!(below.div_rem(high), Some((U320::ZERO, below)));
        assert_eq!(below.div_ceil(high), Some(one));
    }

    #[tokio::test]
    async fn fetch_snapshot_supports_real_bitstamp_single_pair_url_over_loopback() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let body = format!(
            r#"{{"timestamp":"{}","last":"77197","bid":"77196","ask":"77198"}}"#,
            now.timestamp()
        );
        let raw = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (url, _, _) = spawn_raw_http_server(raw.into_bytes()).await;
        let snap = fetch_snapshot_for(
            FxFeedProvider::Bitstamp,
            &format!("{url}/api/v2/ticker/btcusd/"),
            now,
        )
        .await
        .expect("single-pair bitstamp endpoint parses cleanly");
        assert_eq!(
            snap.sample(Currency::Usd).unwrap().rate.to_string_value(),
            "77197"
        );
        assert_eq!(
            snap.sample(Currency::Eur),
            Err(FxError::UnsupportedCurrency)
        );
    }

    #[test]
    fn ip_allowlist_rejects_non_public_ipv6_transition_and_site_local_ranges() {
        let seller = marketplace_domain::pubky::encode_pubky(&[9; 32]);
        let single = StoreMode::SingleSeller {
            pinned: Some(seller),
        };
        let lookup = |url: &str| {
            let owned = url.to_string();
            move |name: &str| (name == ENV_FX_FEED_URL).then(|| owned.clone())
        };

        for non_public in [
            "2002:a9fe:a9fe::",
            "2002:7f00:1::",
            "fec0::1",
            "2001::1",
            "::ffff:0:7f00:1",
            "3fff::1",
        ] {
            let ip: IpAddr = non_public.parse().expect("valid IPv6 literal");
            assert!(
                !is_allowed_fx_ip(ip, false),
                "non-public IPv6 {non_public} must be refused for remote feeds"
            );
            assert!(
                !is_allowed_fx_ip(ip, true),
                "non-public IPv6 {non_public} must not pass as loopback"
            );
            let url = format!("https://[{non_public}]/fx");
            assert!(
                fx_feed_from_lookup(&lookup(&url), &single).is_err(),
                "{url} must be rejected at boot"
            );
        }

        for public in [
            "1.1.1.1",
            "93.184.216.34",
            "2606:4700:4700::1111",
            "2606:2800:220:1:248:1893:25c8:1946",
        ] {
            let ip: IpAddr = public.parse().expect("valid public IP literal");
            assert!(
                is_allowed_fx_ip(ip, false),
                "public address {public} must remain allowed"
            );
            let url = match ip {
                IpAddr::V4(_) => format!("https://{public}/fx"),
                IpAddr::V6(_) => format!("https://[{public}]/fx"),
            };
            assert!(
                fx_feed_from_lookup(&lookup(&url), &single).is_ok(),
                "{url} must be accepted in single-seller mode"
            );
        }
    }

    #[tokio::test]
    async fn fetch_body_enforces_overall_deadline_on_stalled_headers_and_body() {
        // Server 1: accepts TCP and stalls forever before sending headers.
        let stalled_headers = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stalled headers server binds");
        let headers_addr = stalled_headers.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = stalled_headers.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                tokio::time::sleep(StdDuration::from_secs(30)).await;
            }
        });

        let budget = StdDuration::from_millis(200);
        let started = tokio::time::Instant::now();
        let res = fetch_body_with_timeout(&format!("http://{headers_addr}/fx"), budget).await;
        assert!(matches!(res, Err(FxError::Malformed)));
        assert!(
            started.elapsed() < StdDuration::from_secs(1),
            "stalled headers must time out within budget, took {:?}",
            started.elapsed()
        );

        // Server 2: sends HTTP 200 headers and partial body, then stalls forever.
        let stalled_body = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stalled body server binds");
        let body_addr = stalled_body.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = stalled_body.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"tim")
                    .await;
                tokio::time::sleep(StdDuration::from_secs(30)).await;
            }
        });

        let started = tokio::time::Instant::now();
        let res = fetch_body_with_timeout(&format!("http://{body_addr}/fx"), budget).await;
        assert!(matches!(res, Err(FxError::Malformed)));
        assert!(
            started.elapsed() < StdDuration::from_secs(1),
            "stalled streaming body must time out within budget, took {:?}",
            started.elapsed()
        );

        // Zero budget fails closed immediately.
        let res = fetch_body_with_timeout("http://127.0.0.1:1/fx", StdDuration::ZERO).await;
        assert!(matches!(res, Err(FxError::Malformed)));
    }

    #[test]
    fn exchange_adapters_reject_duplicate_json_keys_fail_closed() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();

        // Mempool: duplicate currency key hiding an invalid 19-decimal rate
        // must fail closed rather than collapsing to the last value.
        let mempool_dup_rate = format!(
            r#"{{"time":{current},"USD":77197.1234567890123456789,"USD":77197,"EUR":66481}}"#
        );
        assert_eq!(
            parse_snapshot_for(
                FxFeedProvider::Mempool,
                mempool_dup_rate.as_bytes(),
                now,
                None
            ),
            Err(FxError::Malformed)
        );
        let mempool_dup_time = format!(
            r#"{{"time":{},"time":{current},"USD":77197}}"#,
            current - 600
        );
        assert_eq!(
            parse_snapshot_for(
                FxFeedProvider::Mempool,
                mempool_dup_time.as_bytes(),
                now,
                None
            ),
            Err(FxError::Malformed)
        );

        // Kraken `result`: duplicate pair key fails closed.
        let kraken_dup = format!(
            r#"{{"error":[],"result":{{"XXBTZUSD":[["77197.1234567890123456789","0.01",{current}]],"XXBTZUSD":[["77197","0.01",{current}]]}}}}"#
        );
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Kraken, kraken_dup.as_bytes(), now, None),
            Err(FxError::Malformed)
        );

        // Coinbase `rates`: duplicate currency key fails closed.
        let coinbase_dup = br#"{"data":{"currency":"BTC","rates":{"USD":"77197.1234567890123456789","USD":"77197"}}}"#;
        assert_eq!(
            parse_snapshot_for(FxFeedProvider::Coinbase, coinbase_dup, now, Some(now)),
            Err(FxError::Malformed)
        );

        // Bitstamp single-pair object: duplicate `last` or `timestamp` fails closed.
        let bitstamp_single_dup = format!(
            r#"{{"timestamp":"{current}","last":"77197.1234567890123456789","last":"77197"}}"#
        );
        assert_eq!(
            parse_snapshot_for_url(
                FxFeedProvider::Bitstamp,
                "https://www.bitstamp.net/api/v2/ticker/btcusd/",
                bitstamp_single_dup.as_bytes(),
                now,
                None,
            ),
            Err(FxError::Malformed)
        );

        // Bitstamp array: duplicate-key unrelated altcoin stays isolated while
        // any matched BTC/<CCY> entry with a duplicate key fails closed (even
        // if another valid BTC/<CCY> entry is present or `pair` is repeated).
        let bitstamp_arr_dup = format!(
            r#"[
                {{"pair":"ETH/USD","last":"3000","last":"3001","timestamp":"{current}"}},
                {{"pair":"BTC/USD","last":"77197.1234567890123456789","last":"77197","timestamp":"{current}"}},
                {{"pair":"BTC/EUR","last":"66481","timestamp":"{current}"}},
                {{"pair":"BTC/GBP","pair":"ETH/GBP","last":"57037","timestamp":"{current}"}},
                {{"pair":"BTC/GBP","last":"57037","timestamp":"{current}"}}
            ]"#
        );
        let snap = parse_snapshot_for(
            FxFeedProvider::Bitstamp,
            bitstamp_arr_dup.as_bytes(),
            now,
            None,
        )
        .expect("array envelope parses with altcoin isolation");
        assert_eq!(
            snap.sample(Currency::Usd),
            Err(FxError::Malformed),
            "duplicate-key BTC/USD entry must fail closed"
        );
        assert_eq!(
            snap.sample(Currency::Eur)
                .expect("unrelated duplicate-key altcoin must not block BTC/EUR")
                .rate
                .to_string_value(),
            "66481"
        );
        assert_eq!(
            snap.sample(Currency::Gbp),
            Err(FxError::Malformed),
            "ambiguous duplicate-pair BTC/GBP entry must fail closed even alongside a clean entry"
        );

        // Duplicate-key BTC/USD alongside a clean BTC/USD also fails closed.
        let bitstamp_arr_two = format!(
            r#"[
                {{"pair":"BTC/USD","last":"1","last":"2","timestamp":"{current}"}},
                {{"pair":"BTC/USD","last":"77197","timestamp":"{current}"}}
            ]"#
        );
        let snap_two = parse_snapshot_for(
            FxFeedProvider::Bitstamp,
            bitstamp_arr_two.as_bytes(),
            now,
            None,
        )
        .expect("array parses");
        assert_eq!(snap_two.sample(Currency::Usd), Err(FxError::Malformed));
    }

    #[tokio::test]
    async fn fetch_kraken_single_pair_requires_canonical_boot_url() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let current = now.timestamp();
        let body = format!(
            r#"{{"error":[],"result":{{"XXBTZUSD":[["77197.5","0.01",{current},"b","l","",1]],"last":"1"}}}}"#
        );
        let raw = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (url, hits, last_request) = spawn_raw_http_server(raw.into_bytes()).await;

        for bad_query in [
            "?pair=XXBTZUSD&foo=bar",
            "?pair=XXBTZUSD&count=500",
            "?pair=XXBTZUSD&count=1&count=1",
        ] {
            assert!(
                FxFeedConfig::resolve(
                    FxFeedProvider::Kraken,
                    &format!("{url}/0/public/Trades{bad_query}"),
                )
                .is_err(),
                "{bad_query} must be refused at boot"
            );
        }

        let feed = FxFeedConfig::resolve(
            FxFeedProvider::Kraken,
            &format!("{url}/0/public/Trades?pair=XXBTZUSD&count=1"),
        )
        .expect("canonical Kraken query");
        assert_eq!(feed.supported_currencies(), &[Currency::Usd]);

        let snap = fetch_snapshot_for(
            FxFeedProvider::Kraken,
            &format!("{url}/0/public/Trades?pair=XXBTZUSD&count=1"),
            now,
        )
        .await
        .expect("canonical single-pair Kraken URL fetches");
        assert_eq!(
            snap.sample(Currency::Usd).unwrap().rate.to_string_value(),
            "77197.5"
        );
        assert_eq!(
            snap.sample(Currency::Eur),
            Err(FxError::UnsupportedCurrency)
        );
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);

        let request_line = last_request
            .lock()
            .unwrap()
            .lines()
            .next()
            .unwrap_or("")
            .to_string();
        assert_eq!(
            request_line,
            "GET /0/public/Trades?pair=XXBTZUSD&count=1 HTTP/1.1"
        );
    }

    #[test]
    fn region_policy_fx_admission_requires_eur_only_when_eu_c4_limits_can_run() {
        use crate::region_policy::{RegionModes, RegionPolicy};

        let usd_only = FxFeedAdmission {
            provider: FxFeedProvider::Kraken,
            supported_fiat: &[Currency::Usd],
        };
        let eur_only = FxFeedAdmission {
            provider: FxFeedProvider::Bitstamp,
            supported_fiat: &[Currency::Eur],
        };
        let bitstamp_all = FxFeedAdmission {
            provider: FxFeedProvider::Bitstamp,
            supported_fiat: &BITSTAMP_SUPPORTED_CURRENCIES,
        };

        // Legacy flags: EU_FENCE_ENABLED=true requires EUR; digital-only does not.
        assert!(
            ensure_region_policy_fx_admission(RegionPolicy::legacy(false, false), usd_only).is_ok()
        );
        assert!(
            ensure_region_policy_fx_admission(RegionPolicy::legacy(false, true), usd_only).is_ok()
        );
        let err = ensure_region_policy_fx_admission(RegionPolicy::legacy(true, false), usd_only)
            .expect_err("EU_FENCE_ENABLED=true must reject USD-only feed");
        assert!(err.to_string().contains(EU_LIMITS_REQUIRE_EUR_FX), "{err}");
        assert!(
            ensure_region_policy_fx_admission(RegionPolicy::legacy(true, false), eur_only).is_ok()
        );
        assert!(
            ensure_region_policy_fx_admission(RegionPolicy::legacy(true, true), bitstamp_all)
                .is_ok()
        );

        // Explicit REGION_MODES: open/blocked/non-EU-nuanced allow USD-only; any nuanced EU-27 requires EUR.
        for allowed in [
            "none",
            "EU=blocked,GB=blocked,US=blocked",
            "EU=blocked,GB=nuanced,US=blocked",
            "GB=nuanced,CH=nuanced,UNKNOWN=nuanced",
            "US=blocked",
            "EU=nuanced,EU=blocked",
            "DE=nuanced,DE=open",
        ] {
            let modes = RegionModes::parse(allowed).expect("valid modes");
            assert!(
                ensure_region_policy_fx_admission(RegionPolicy::explicit(&modes), usd_only).is_ok(),
                "{allowed} must admit USD-only feed"
            );
        }
        for requires_eur in [
            "EU=nuanced",
            "EU=nuanced,GB=nuanced",
            "EU=nuanced,GB=nuanced,US=blocked",
            "EU=blocked,DE=nuanced",
            "FR=nuanced",
            "EU=nuanced,DE=open",
        ] {
            let modes = RegionModes::parse(requires_eur).expect("valid modes");
            let err = ensure_region_policy_fx_admission(RegionPolicy::explicit(&modes), usd_only)
                .expect_err("nuanced EU-27 must reject USD-only feed");
            assert!(
                err.to_string().contains(EU_LIMITS_REQUIRE_EUR_FX),
                "{requires_eur}: {err}"
            );
            assert!(
                ensure_region_policy_fx_admission(RegionPolicy::explicit(&modes), eur_only).is_ok(),
                "{requires_eur} must admit EUR-only feed"
            );
            assert!(
                ensure_region_policy_fx_admission(RegionPolicy::explicit(&modes), bitstamp_all)
                    .is_ok(),
                "{requires_eur} must admit Bitstamp all-pairs feed"
            );
        }
    }

    #[tokio::test]
    async fn coinbase_official_queryless_url_fails_closed_while_custom_btc_host_works() {
        let now: DateTime<Utc> = FIXTURE_NOW.parse().unwrap();
        let fresh_date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        // Official Coinbase queryless URLs fail closed before any network fetch.
        for bad_url in [
            "https://api.coinbase.com/v2/exchange-rates",
            "https://api.coinbase.com/v2/exchange-rates/",
            "https://api.coinbase.com/v2/exchange-rates?",
            "https://API.COINBASE.COM/v2/exchange-rates",
            "https://api.coinbase.com./v2/exchange-rates",
            "https://api.coinbase.com/v2/exchange-rates?currency=btc",
        ] {
            let err = FxFeedConfig::resolve(FxFeedProvider::Coinbase, bad_url)
                .expect_err("official Coinbase URL without ?currency=BTC must fail closed");
            assert!(err.to_string().contains("currency=BTC"), "{bad_url}: {err}");
            assert_eq!(
                fetch_snapshot_for(FxFeedProvider::Coinbase, bad_url, now).await,
                Err(FxError::Malformed),
                "{bad_url} fetch must fail closed before network IO"
            );
        }

        // Canonical official Coinbase URL resolves as all seven fiat currencies.
        let official = FxFeedConfig::resolve(FxFeedProvider::Coinbase, COINBASE_FX_URL)
            .expect("canonical Coinbase URL resolves");
        assert_eq!(official.supported_currencies(), &Currency::FIAT);

        // A queryless custom/loopback Coinbase-format endpoint returning BTC
        // succeeds, while one returning Coinbase's USD default fails closed.
        let btc_body = r#"{"data":{"currency":"BTC","rates":{"USD":"77197","EUR":"66481","CHF":"62000","GBP":"58000","JPY":"11500000","CAD":"105000","AUD":"118000"}}}"#;
        let btc_raw = format!(
            "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{btc_body}",
            btc_body.len()
        );
        let (btc_base, btc_hits, _) = spawn_raw_http_server(btc_raw.into_bytes()).await;
        let btc_url = format!("{btc_base}/v2/exchange-rates");
        let snap = fetch_snapshot_for(FxFeedProvider::Coinbase, &btc_url, now)
            .await
            .expect("queryless custom/loopback BTC Coinbase endpoint fetches");
        assert_eq!(btc_hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            snap.sample(Currency::Usd).unwrap().rate.to_string_value(),
            "77197"
        );
        assert_eq!(
            snap.sample(Currency::Eur).unwrap().rate.to_string_value(),
            "66481"
        );

        let usd_body = r#"{"data":{"currency":"USD","rates":{"BTC":"0.00001295"}}}"#;
        let usd_raw = format!(
            "HTTP/1.1 200 OK\r\nDate: {fresh_date}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{usd_body}",
            usd_body.len()
        );
        let (usd_base, usd_hits, _) = spawn_raw_http_server(usd_raw.into_bytes()).await;
        let usd_url = format!("{usd_base}/v2/exchange-rates");
        assert_eq!(
            fetch_snapshot_for(FxFeedProvider::Coinbase, &usd_url, now).await,
            Err(FxError::Malformed),
            "USD-base Coinbase response must fail closed"
        );
        assert_eq!(usd_hits.load(AtomicOrdering::SeqCst), 1);
    }
}
