//! Per-region commerce modes: the one place that says what each country may
//! do on Pubky Market.
//!
//! `REGION_MODES` is a comma-separated list of `SELECTOR=MODE`. A selector is
//! an ISO 3166-1 alpha-2 code, `EU` (the 27 member states) or `UNKNOWN` (a
//! request whose IP country cannot be determined). Later entries override
//! earlier ones, so `EU=nuanced,DE=blocked` makes Germany the only blocked
//! member state. A mode is:
//!
//! - `open`: no commerce restriction;
//! - `nuanced`: the design the service shipped first. C3 (a buyer here buys
//!   only from a seller who declared private), C4 (the DAC7 cap for sellers
//!   resident in the EU-27) and C3b (digital items held back);
//! - `blocked`: no buying, no selling and no paid unlocks for people in the
//!   region.
//!
//! A country no entry names is `open`. `none` means every country is `open`.
//! `UNKNOWN` defaults to the strictest mode any country has, so with one
//! region blocked a request of unknown origin is blocked for commerce, and
//! with only nuanced regions it is held for digital items only.
//!
//! This module is pure: no I/O, no clock. [`crate::commerce_region`] reads
//! the facts and enforces; the fence and the digital hold ask this module
//! what a country's mode is.
//!
//! With `REGION_MODES` unset the policy is derived from the two older flags,
//! exactly as before: `EU_FENCE_ENABLED` turns on C3 and C4 and
//! `DIGITAL_REGION_BLOCK_ENABLED` turns on C3b, each for the EU-27 and GB as
//! nuanced regions, and nothing is ever blocked.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;

use marketplace_domain::region::{is_eu_country, EU_27, GB};

/// What a region may do. Ordered from the loosest to the strictest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Mode {
    Open,
    Nuanced,
    Blocked,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Open => "open",
            Mode::Nuanced => "nuanced",
            Mode::Blocked => "blocked",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "open" => Some(Mode::Open),
            "nuanced" => Some(Mode::Nuanced),
            "blocked" => Some(Mode::Blocked),
            _ => None,
        }
    }
}

/// Why a request's IP blocks commerce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpBlock {
    /// The country of the request's IP, from the trusted edge, is blocked.
    Country,
    /// The edge gave no usable IP country and unknown origins are blocked.
    Unknown,
}

/// Why a buyer's command is refused for its region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Block {
    /// The country the buyer declared (`buyer_country`) is blocked.
    Declared,
    /// The request's IP blocks it.
    Ip(IpBlock),
}

/// A `REGION_MODES` value that cannot be read. The text names the entry so
/// the operator can fix it; the service does not start with one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionModesError(String);

impl fmt::Display for RegionModesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegionModesError {}

fn invalid(message: impl Into<String>) -> RegionModesError {
    RegionModesError(message.into())
}

/// The mode of every country and of an unknown origin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionModes {
    /// Countries a selector named; every other country is `open`.
    countries: BTreeMap<String, Mode>,
    /// The `UNKNOWN` entry, when one was given.
    unknown: Option<Mode>,
}

impl RegionModes {
    /// The EU-27 and GB as nuanced regions: what `EU_FENCE_ENABLED` and
    /// `DIGITAL_REGION_BLOCK_ENABLED` always meant.
    pub fn legacy_nuanced() -> Self {
        let countries = EU_27
            .iter()
            .copied()
            .chain([GB])
            .map(|code| (code.to_string(), Mode::Nuanced))
            .collect();
        Self {
            countries,
            unknown: None,
        }
    }

    /// Reads a `REGION_MODES` value. Input is case-insensitive and
    /// whitespace around entries is ignored; codes are stored uppercase.
    pub fn parse(raw: &str) -> Result<Self, RegionModesError> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(invalid(
                "REGION_MODES is empty; write SELECTOR=MODE entries, or `none` to open every region",
            ));
        }
        if raw.eq_ignore_ascii_case("none") {
            return Ok(Self::default());
        }
        let mut modes = Self::default();
        for entry in raw.split(',') {
            let entry = entry.trim();
            let Some((selector, mode)) = entry.split_once('=') else {
                return Err(invalid(if entry.is_empty() {
                    "REGION_MODES has an empty entry".to_string()
                } else if entry.eq_ignore_ascii_case("none") {
                    "REGION_MODES `none` must be the whole value".to_string()
                } else {
                    format!("REGION_MODES entry `{entry}` is not SELECTOR=MODE")
                }));
            };
            let (selector, mode) = (selector.trim(), mode.trim());
            let Some(mode) = Mode::parse(mode) else {
                return Err(invalid(format!(
                    "REGION_MODES entry `{entry}` has the mode `{mode}`; use open, nuanced or blocked"
                )));
            };
            modes.apply(selector, mode, entry)?;
        }
        Ok(modes)
    }

    fn apply(&mut self, selector: &str, mode: Mode, entry: &str) -> Result<(), RegionModesError> {
        let selector = selector.to_ascii_uppercase();
        match selector.as_str() {
            "UNKNOWN" => self.unknown = Some(mode),
            "EU" => {
                for code in EU_27 {
                    self.countries.insert(code.to_string(), mode);
                }
            }
            "UK" => {
                return Err(invalid(format!(
                    "REGION_MODES entry `{entry}`: the United Kingdom is GB"
                )))
            }
            "XX" => {
                return Err(invalid(format!(
                    "REGION_MODES entry `{entry}`: XX is the edge's unknown marker; use UNKNOWN"
                )))
            }
            code if code.len() == 2 && code.bytes().all(|byte| byte.is_ascii_uppercase()) => {
                self.countries.insert(selector, mode);
            }
            _ => {
                return Err(invalid(format!(
                    "REGION_MODES entry `{entry}` has the selector `{selector}`; use an ISO \
                     3166-1 alpha-2 code, EU or UNKNOWN"
                )))
            }
        }
        Ok(())
    }

    /// The mode of `country` (uppercase ISO 3166-1 alpha-2).
    pub fn mode_of(&self, country: &str) -> Mode {
        self.countries.get(country).copied().unwrap_or(Mode::Open)
    }

    /// The mode of a request whose IP country is unknown: the `UNKNOWN`
    /// entry, else the strictest mode any country has.
    pub fn unknown_mode(&self) -> Mode {
        self.unknown
            .unwrap_or_else(|| self.countries.values().copied().max().unwrap_or(Mode::Open))
    }

    /// Whether any request can be refused for commerce: a region or the
    /// unknown origin is blocked.
    pub fn has_blocked(&self) -> bool {
        self.unknown_mode() == Mode::Blocked
            || self.countries.values().any(|mode| *mode == Mode::Blocked)
    }

    /// Whether any named country is nuanced: only then do C3 and C4 need the
    /// buyer's declared country.
    pub fn has_nuanced_country(&self) -> bool {
        self.countries.values().any(|mode| *mode == Mode::Nuanced)
    }

    /// Whether digital items are held back for any region.
    pub fn holds_digital(&self) -> bool {
        self.unknown_mode() == Mode::Nuanced
            || self.countries.values().any(|mode| *mode == Mode::Nuanced)
    }

    /// Whether any region is restricted at all.
    pub fn is_active(&self) -> bool {
        self.has_blocked() || self.holds_digital()
    }

    /// C3: a buyer in `country` buys only from a declared private seller.
    pub fn private_seller_only(&self, country: &str) -> bool {
        self.mode_of(country) == Mode::Nuanced
    }

    /// C4's annual limit: the seller is resident in the EU-27 and that
    /// country is nuanced. A shop record with no usable country has no
    /// residence and no cap.
    pub fn annual_limit_applies(&self, seller_country: Option<&str>) -> bool {
        seller_country
            .is_some_and(|country| is_eu_country(country) && self.mode_of(country) == Mode::Nuanced)
    }

    /// C4's re-confirmation point: the buyer is in the EU-27 and that
    /// country is nuanced.
    pub fn reconfirmation_applies(&self, buyer_country: &str) -> bool {
        is_eu_country(buyer_country) && self.mode_of(buyer_country) == Mode::Nuanced
    }

    /// C3b: digital items are held back for a buyer whose country (declared
    /// or from the IP) is nuanced or blocked.
    pub fn digital_held(&self, country: &str) -> bool {
        self.mode_of(country) != Mode::Open
    }

    /// Whether a seller whose shop record says `country` may not sell.
    pub fn shop_country_blocked(&self, country: Option<&str>) -> bool {
        country.is_some_and(|country| self.mode_of(country) == Mode::Blocked)
    }

    /// The IP half of the block rule: the request's IP country is blocked,
    /// or unknown while unknown origins are blocked.
    pub fn ip_block(&self, ip_country: Option<&str>) -> Option<IpBlock> {
        match ip_country {
            Some(country) if self.mode_of(country) == Mode::Blocked => Some(IpBlock::Country),
            Some(_) => None,
            None if self.unknown_mode() == Mode::Blocked => Some(IpBlock::Unknown),
            None => None,
        }
    }

    /// Whether a buyer is blocked: the declared country is blocked, else the
    /// IP country is blocked, or unknown while unknown origins are blocked.
    /// A buyer cannot dodge a block by declaring another country or by
    /// sending no country at all.
    pub fn buyer_block(&self, declared: Option<&str>, ip_country: Option<&str>) -> Option<Block> {
        if declared.is_some_and(|country| self.mode_of(country) == Mode::Blocked) {
            return Some(Block::Declared);
        }
        self.ip_block(ip_country).map(Block::Ip)
    }
}

static LEGACY_MODES: LazyLock<RegionModes> = LazyLock::new(RegionModes::legacy_nuanced);

/// The modes in force and which of the three code paths are on, read from
/// the configuration ([`crate::config::Config::region_policy`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionPolicy<'a> {
    modes: &'a RegionModes,
    fence: bool,
    digital: bool,
}

impl<'a> RegionPolicy<'a> {
    /// `REGION_MODES` unset: the two legacy flags, each with its own toggle.
    pub fn legacy(eu_fence_enabled: bool, digital_region_block_enabled: bool) -> Self {
        Self {
            modes: &LEGACY_MODES,
            fence: eu_fence_enabled,
            digital: digital_region_block_enabled,
        }
    }

    /// `REGION_MODES` set: it wins. Every nuanced region runs C3, C4 and
    /// C3b whatever the legacy flags say, and the fence reads the sellers'
    /// shop records whenever any region is restricted.
    pub fn explicit(modes: &'a RegionModes) -> Self {
        let fence = modes.is_active();
        let digital = modes.holds_digital();
        Self {
            modes,
            fence,
            digital,
        }
    }

    pub fn modes(&self) -> &'a RegionModes {
        self.modes
    }

    /// The modes C3 and C4 read, when the fence is on.
    pub fn fence_modes(&self) -> Option<&'a RegionModes> {
        self.fence.then_some(self.modes)
    }

    pub fn fence_active(&self) -> bool {
        self.fence
    }

    pub fn digital_active(&self) -> bool {
        self.digital
    }

    /// Whether commerce commands are checked against blocked regions.
    pub fn commerce_active(&self) -> bool {
        self.modes.has_blocked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modes(raw: &str) -> RegionModes {
        RegionModes::parse(raw).unwrap_or_else(|error| panic!("{raw}: {error}"))
    }

    const LAUNCH: &str = "EU=blocked,GB=blocked,US=blocked";

    #[test]
    fn the_launch_value_blocks_the_eu_27_gb_and_the_us() {
        let launch = modes(LAUNCH);
        for code in EU_27.iter().copied().chain(["GB", "US"]) {
            assert_eq!(launch.mode_of(code), Mode::Blocked, "{code}");
        }
        for code in ["CH", "NO", "IS", "LI", "CA", "JP", "BR", "AU"] {
            assert_eq!(launch.mode_of(code), Mode::Open, "{code}");
        }
        assert_eq!(launch.unknown_mode(), Mode::Blocked);
        assert!(launch.has_blocked());
        assert!(!launch.holds_digital());
    }

    #[test]
    fn every_later_outcome_is_a_value() {
        let nuanced_eu_and_gb = modes("EU=nuanced,GB=nuanced,US=blocked");
        assert_eq!(nuanced_eu_and_gb.mode_of("DE"), Mode::Nuanced);
        assert_eq!(nuanced_eu_and_gb.mode_of("GB"), Mode::Nuanced);
        assert_eq!(nuanced_eu_and_gb.mode_of("US"), Mode::Blocked);
        assert_eq!(nuanced_eu_and_gb.unknown_mode(), Mode::Blocked);

        let us_unlocked = modes("EU=blocked,GB=blocked");
        assert_eq!(us_unlocked.mode_of("US"), Mode::Open);
        assert_eq!(us_unlocked.mode_of("FR"), Mode::Blocked);

        let both = modes("EU=nuanced,GB=nuanced");
        assert_eq!(both, RegionModes::legacy_nuanced());
        assert_eq!(both.unknown_mode(), Mode::Nuanced);
        assert!(!both.has_blocked());
        assert!(both.holds_digital());
    }

    #[test]
    fn none_opens_every_region() {
        for raw in ["none", "NONE", " None "] {
            let open = modes(raw);
            assert_eq!(open, RegionModes::default(), "{raw}");
            for code in ["DE", "GB", "US", "CH"] {
                assert_eq!(open.mode_of(code), Mode::Open, "{raw} {code}");
            }
            assert_eq!(open.unknown_mode(), Mode::Open);
            assert!(!open.is_active());
        }
    }

    #[test]
    fn a_later_entry_overrides_an_earlier_one() {
        let germany = modes("EU=nuanced,DE=blocked");
        assert_eq!(germany.mode_of("DE"), Mode::Blocked);
        assert_eq!(germany.mode_of("FR"), Mode::Nuanced);

        let eu_last = modes("DE=blocked,EU=nuanced");
        assert_eq!(eu_last.mode_of("DE"), Mode::Nuanced);

        let repeated = modes("US=blocked,US=open");
        assert_eq!(repeated.mode_of("US"), Mode::Open);
        assert!(!repeated.has_blocked());
    }

    #[test]
    fn input_is_case_insensitive_and_codes_are_stored_uppercase() {
        let mixed = modes(" eu = Blocked , gb=NUANCED,us=open, Unknown = open ");
        assert_eq!(mixed.mode_of("DE"), Mode::Blocked);
        assert_eq!(mixed.mode_of("GB"), Mode::Nuanced);
        assert_eq!(mixed.mode_of("US"), Mode::Open);
        assert_eq!(mixed.unknown_mode(), Mode::Open);
        assert_eq!(
            mixed.mode_of("gb"),
            Mode::Open,
            "lookups take canonical codes"
        );
        assert!(mixed
            .countries
            .keys()
            .all(|code| code == &code.to_ascii_uppercase()));
    }

    #[test]
    fn unknown_defaults_to_the_strictest_mode_in_force() {
        assert_eq!(modes("none").unknown_mode(), Mode::Open);
        assert_eq!(modes("US=open").unknown_mode(), Mode::Open);
        assert_eq!(modes("EU=nuanced").unknown_mode(), Mode::Nuanced);
        assert_eq!(modes("EU=nuanced,US=blocked").unknown_mode(), Mode::Blocked);
        assert_eq!(modes("DE=blocked,DE=open").unknown_mode(), Mode::Open);
    }

    #[test]
    fn an_explicit_unknown_entry_wins_over_the_default() {
        let open = modes("EU=blocked,UNKNOWN=open");
        assert_eq!(open.unknown_mode(), Mode::Open);
        assert!(open.has_blocked());
        assert_eq!(open.ip_block(None), None);

        let nuanced = modes("US=blocked,UNKNOWN=nuanced");
        assert_eq!(nuanced.unknown_mode(), Mode::Nuanced);
        assert_eq!(nuanced.ip_block(None), None);
        assert!(nuanced.holds_digital());

        let blocked_alone = modes("UNKNOWN=blocked");
        assert!(blocked_alone.has_blocked());
        assert_eq!(blocked_alone.ip_block(None), Some(IpBlock::Unknown));
        assert_eq!(blocked_alone.ip_block(Some("US")), None);
    }

    #[test]
    fn malformed_values_are_refused_with_a_clear_error() {
        for (raw, needle) in [
            ("", "empty"),
            ("   ", "empty"),
            ("EU", "not SELECTOR=MODE"),
            ("EU=", "mode ``"),
            ("EU=closed", "mode `closed`"),
            ("=blocked", "selector ``"),
            ("EU=blocked,", "empty entry"),
            (",EU=blocked", "empty entry"),
            ("EU=blocked,,US=blocked", "empty entry"),
            ("EUR=blocked", "selector `EUR`"),
            ("D=blocked", "selector `D`"),
            ("1A=blocked", "selector `1A`"),
            ("D-=blocked", "selector `D-`"),
            ("UK=blocked", "the United Kingdom is GB"),
            ("XX=blocked", "use UNKNOWN"),
            ("none,EU=blocked", "must be the whole value"),
            ("EU=blocked,none", "must be the whole value"),
            ("EU=blocked=open", "mode `blocked=open`"),
            ("EU:blocked", "not SELECTOR=MODE"),
        ] {
            let error = RegionModes::parse(raw).expect_err(raw).to_string();
            assert!(error.contains(needle), "{raw:?} gave {error:?}");
            assert!(error.contains("REGION_MODES"), "{raw:?} gave {error:?}");
        }
    }

    #[test]
    fn a_blocked_declared_country_blocks_whatever_the_ip_says() {
        let launch = modes(LAUNCH);
        for ip in [Some("CH"), Some("DE"), None] {
            assert_eq!(
                launch.buyer_block(Some("US"), ip),
                Some(Block::Declared),
                "{ip:?}"
            );
        }
    }

    #[test]
    fn a_blocked_ip_blocks_whatever_the_buyer_declares() {
        let launch = modes(LAUNCH);
        assert_eq!(
            launch.buyer_block(Some("CH"), Some("US")),
            Some(Block::Ip(IpBlock::Country))
        );
        assert_eq!(
            launch.buyer_block(Some("CH"), Some("FR")),
            Some(Block::Ip(IpBlock::Country))
        );
        assert_eq!(
            launch.buyer_block(None, Some("GB")),
            Some(Block::Ip(IpBlock::Country))
        );
    }

    #[test]
    fn an_unknown_ip_blocks_only_while_unknown_resolves_to_blocked() {
        let launch = modes(LAUNCH);
        assert_eq!(
            launch.buyer_block(Some("CH"), None),
            Some(Block::Ip(IpBlock::Unknown))
        );
        let unknown_open = modes("EU=blocked,GB=blocked,US=blocked,UNKNOWN=open");
        assert_eq!(unknown_open.buyer_block(Some("CH"), None), None);
        assert_eq!(
            unknown_open.buyer_block(Some("CH"), Some("US")),
            Some(Block::Ip(IpBlock::Country))
        );
        let nuanced_only = modes("EU=nuanced,GB=nuanced");
        assert_eq!(nuanced_only.buyer_block(Some("CH"), None), None);
    }

    #[test]
    fn an_open_declared_country_and_ip_pass() {
        let launch = modes(LAUNCH);
        assert_eq!(launch.buyer_block(Some("CH"), Some("CH")), None);
        assert_eq!(launch.buyer_block(Some("NO"), Some("CA")), None);
        assert_eq!(modes("none").buyer_block(Some("US"), Some("DE")), None);
        assert_eq!(modes("none").buyer_block(Some("US"), None), None);
    }

    #[test]
    fn a_nuanced_region_is_never_a_commerce_block() {
        let nuanced = modes("EU=nuanced,GB=nuanced");
        assert_eq!(nuanced.buyer_block(Some("DE"), Some("GB")), None);
        assert_eq!(nuanced.ip_block(Some("FR")), None);
        assert!(!nuanced.shop_country_blocked(Some("DE")));
    }

    #[test]
    fn a_seller_is_blocked_by_the_shop_country_only_when_it_is_blocked() {
        let launch = modes(LAUNCH);
        assert!(launch.shop_country_blocked(Some("US")));
        assert!(launch.shop_country_blocked(Some("DE")));
        assert!(!launch.shop_country_blocked(Some("CH")));
        assert!(
            !launch.shop_country_blocked(None),
            "no country, no residence"
        );
    }

    #[test]
    fn c3_applies_to_nuanced_buyers_only() {
        let mixed = modes("EU=nuanced,GB=blocked,US=open");
        assert!(mixed.private_seller_only("DE"));
        assert!(!mixed.private_seller_only("GB"));
        assert!(!mixed.private_seller_only("US"));
        assert!(!mixed.private_seller_only("CH"));
    }

    #[test]
    fn c4_stays_eu_27_and_applies_only_where_the_country_is_nuanced() {
        let eu_and_ch = modes("EU=nuanced,GB=nuanced,CH=nuanced");
        assert!(eu_and_ch.annual_limit_applies(Some("DE")));
        assert!(
            !eu_and_ch.annual_limit_applies(Some("GB")),
            "DAC7 is EU only"
        );
        assert!(
            !eu_and_ch.annual_limit_applies(Some("CH")),
            "DAC7 is EU only"
        );
        assert!(!eu_and_ch.annual_limit_applies(None));
        assert!(eu_and_ch.reconfirmation_applies("FR"));
        assert!(!eu_and_ch.reconfirmation_applies("GB"));
        assert!(!eu_and_ch.reconfirmation_applies("CH"));

        let germany_open = modes("EU=nuanced,DE=open");
        assert!(!germany_open.annual_limit_applies(Some("DE")));
        assert!(germany_open.annual_limit_applies(Some("FR")));
        assert!(!germany_open.reconfirmation_applies("DE"));

        let blocked = modes("EU=blocked");
        assert!(!blocked.annual_limit_applies(Some("DE")));
        assert!(!blocked.reconfirmation_applies("DE"));
    }

    #[test]
    fn the_digital_hold_covers_nuanced_and_blocked_countries() {
        let mixed = modes("EU=nuanced,US=blocked");
        assert!(mixed.digital_held("DE"));
        assert!(mixed.digital_held("US"));
        assert!(!mixed.digital_held("CH"));
        assert!(!mixed.digital_held("GB"));
    }

    #[test]
    fn the_legacy_policy_keeps_the_two_flags_separate() {
        for (fence, digital) in [(false, false), (true, false), (false, true), (true, true)] {
            let policy = RegionPolicy::legacy(fence, digital);
            assert_eq!(policy.fence_active(), fence);
            assert_eq!(policy.digital_active(), digital);
            assert_eq!(policy.fence_modes().is_some(), fence);
            assert!(!policy.commerce_active(), "the legacy flags never block");
            for code in EU_27.iter().copied().chain(["GB"]) {
                assert_eq!(policy.modes().mode_of(code), Mode::Nuanced, "{code}");
            }
            assert_eq!(policy.modes().mode_of("US"), Mode::Open);
            assert_eq!(policy.modes().unknown_mode(), Mode::Nuanced);
        }
    }

    #[test]
    fn explicit_modes_run_every_path_they_need() {
        let launch_modes = modes(LAUNCH);
        let launch = RegionPolicy::explicit(&launch_modes);
        assert!(launch.commerce_active());
        assert!(launch.fence_active(), "the seller's shop country is read");
        assert!(!launch.digital_active(), "nothing is nuanced");

        let nuanced_modes = modes("EU=nuanced,GB=nuanced");
        let nuanced = RegionPolicy::explicit(&nuanced_modes);
        assert!(!nuanced.commerce_active());
        assert!(nuanced.fence_active());
        assert!(nuanced.digital_active());

        let mixed_modes = modes("EU=nuanced,GB=nuanced,US=blocked");
        let mixed = RegionPolicy::explicit(&mixed_modes);
        assert!(mixed.commerce_active());
        assert!(mixed.fence_active());
        assert!(mixed.digital_active());

        let none_modes = modes("none");
        let none = RegionPolicy::explicit(&none_modes);
        assert!(!none.commerce_active());
        assert!(!none.fence_active());
        assert!(!none.digital_active());
        assert!(none.fence_modes().is_none());
    }
}
