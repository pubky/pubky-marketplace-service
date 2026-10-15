//! The payment asset model: what a Paykit request is denominated in, what a
//! buyer sends, and where it settles.
//!
//! - [`PaymentAsset`] is the *denomination* of an upstream Paykit request,
//!   stored in `orders.paykit_asset` (open text, migration 0052): Bitcoin for
//!   the legacy `amount_sats` request, and Bitcoin or USD for the
//!   multi-asset request. A later one is a new variant plus a new stored
//!   value, not a migration, and [`PaymentAsset::from_column`] refuses a
//!   stored value this build does not know instead of reading it as Bitcoin.
//! - [`AcceptedAsset`] is a payment asset a Paykit request can offer (what
//!   the buyer may send), and [`OfferedAssets`] is what paykit-server
//!   reported each offered asset costs.
//! - [`PaymentOption`] is what the buyer picks (`{method}.{asset}.{network}`),
//!   and [`PaymentTerms`] is what that choice costs, recorded on the order
//!   (migration 0053).
//! - [`MarketplaceAssets`] is which denominations the deployed paykit-server
//!   accepts.

use sqlx::PgConnection;
use uuid::Uuid;

use crate::payments::PaykitApi;

/// What an upstream Paykit request is denominated in: satoshis, or US cents
/// when paykit-server fixes the Bitcoin rate itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentAsset {
    Btc,
    Usd,
}

impl PaymentAsset {
    /// The value stored in `orders.paykit_asset`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Usd => "USD",
        }
    }

    /// Reads the stored value; an asset this build does not know is `None`.
    pub fn from_column(value: &str) -> Option<Self> {
        match value {
            "BTC" => Some(Self::Btc),
            "USD" => Some(Self::Usd),
            _ => None,
        }
    }
}

/// An asset a Paykit request offers the buyer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AcceptedAsset {
    Btc,
    Usdt,
}

impl AcceptedAsset {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Usdt => "USDT",
        }
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "BTC" => Some(Self::Btc),
            "USDT" => Some(Self::Usdt),
            _ => None,
        }
    }

    /// The decimals of the asset's base unit: satoshis, USDT0 millionths.
    pub const fn exponent(self) -> i16 {
        match self {
            Self::Btc => 8,
            Self::Usdt => 6,
        }
    }
}

/// What paykit-server reported each offered asset costs, in the asset's base
/// units. An asset that is not offered is `None`; at least one is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OfferedAssets {
    pub btc_sats: Option<u64>,
    pub usdt_units: Option<u64>,
}

impl OfferedAssets {
    pub fn amount(self, asset: AcceptedAsset) -> Option<u64> {
        match asset {
            AcceptedAsset::Btc => self.btc_sats,
            AcceptedAsset::Usdt => self.usdt_units,
        }
    }

    /// The offered assets in wire order (`BTC` before `USDT`).
    pub fn assets(self) -> Vec<AcceptedAsset> {
        [AcceptedAsset::Btc, AcceptedAsset::Usdt]
            .into_iter()
            .filter(|asset| self.amount(*asset).is_some())
            .collect()
    }

    pub fn is_empty(self) -> bool {
        self.btc_sats.is_none() && self.usdt_units.is_none()
    }

    /// The `orders.paykit_payment_amounts` document.
    pub fn to_json(self) -> serde_json::Value {
        let mut amounts = serde_json::Map::new();
        for asset in self.assets() {
            amounts.insert(
                asset.as_str().to_string(),
                serde_json::json!(self.amount(asset)),
            );
        }
        serde_json::Value::Object(amounts)
    }

    /// Reads the stored document; anything that is not an object of known
    /// assets with whole positive amounts is `None`.
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let object = value.as_object()?;
        let mut offered = Self::default();
        for (key, amount) in object {
            let amount = amount.as_u64().filter(|amount| *amount > 0)?;
            match AcceptedAsset::from_wire(key)? {
                AcceptedAsset::Btc => offered.btc_sats = Some(amount),
                AcceptedAsset::Usdt => offered.usdt_units = Some(amount),
            }
        }
        (!offered.is_empty()).then_some(offered)
    }
}

/// What the buyer picks at checkout: one `{method}.{asset}.{network}` option
/// (`fiat` stands for "the order's own currency"). It is distinct from
/// [`PaymentAsset`], which is what a Paykit request is *denominated* in: the
/// lock or request asset is the price denomination, not necessarily what the
/// buyer sends, so a USD-denominated request may be paid in USDT.
///
/// `payment_method` stays the buyer-facing label ([`Self::method`]); every
/// existing wire value is unchanged and `usdt` is the only new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentOption {
    PaykitBtcBitcoin,
    PaykitUsdtArbitrumOne,
    PaypalFiat,
}

impl PaymentOption {
    pub const ALL: [Self; 3] = [
        Self::PaykitBtcBitcoin,
        Self::PaykitUsdtArbitrumOne,
        Self::PaypalFiat,
    ];

    /// The canonical option id, as stored in
    /// `seller_accepted_payment_options.option_id`.
    pub const fn id(self) -> &'static str {
        match self {
            Self::PaykitBtcBitcoin => "paykit.btc.bitcoin",
            Self::PaykitUsdtArbitrumOne => "paykit.usdt.arbitrum-one",
            Self::PaypalFiat => "paypal.fiat",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|option| option.id() == id)
    }

    /// The buyer-facing label carried by `orders.payment_method`.
    pub const fn method(self) -> &'static str {
        match self {
            Self::PaykitBtcBitcoin => "bitcoin",
            Self::PaykitUsdtArbitrumOne => "usdt",
            Self::PaypalFiat => "paypal",
        }
    }

    pub fn from_method(method: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|option| option.method() == method)
    }

    /// Where the option settles; fiat methods settle off-chain.
    pub const fn network(self) -> Option<PaymentNetwork> {
        match self {
            Self::PaykitBtcBitcoin => Some(PaymentNetwork::Bitcoin),
            Self::PaykitUsdtArbitrumOne => Some(PaymentNetwork::ArbitrumOne),
            Self::PaypalFiat => None,
        }
    }

    /// Whether the option exists only while `USDT_PAYMENTS_ENABLED` is on.
    pub const fn requires_usdt_flag(self) -> bool {
        matches!(self, Self::PaykitUsdtArbitrumOne)
    }
}

/// Where a payment asset settles (`orders.payment_network`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentNetwork {
    Bitcoin,
    /// USDT0 on Arbitrum One, chain 42161.
    ArbitrumOne,
}

impl PaymentNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bitcoin => "bitcoin",
            Self::ArbitrumOne => "arbitrum-one",
        }
    }
}

/// What the buyer sent (`orders.payment_asset`). Legacy Bitcoin and PayPal
/// orders (and historical Stripe orders) leave these columns NULL; an order
/// bound through the `paykit` method records the rail that actually paid it
/// when the payment settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementAsset {
    Btc,
    Usdt,
}

impl SettlementAsset {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Usdt => "USDT",
        }
    }

    /// The decimals of the amount in `orders.payment_amount_minor`: USDT0
    /// counts millionths.
    pub const fn exponent(self) -> i16 {
        match self {
            Self::Btc => 8,
            Self::Usdt => 6,
        }
    }

    pub const fn network(self) -> PaymentNetwork {
        match self {
            Self::Btc => PaymentNetwork::Bitcoin,
            Self::Usdt => PaymentNetwork::ArbitrumOne,
        }
    }
}

/// How the amount sent was derived from the order's price of record
/// (`orders.payment_quote_basis`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteBasis {
    /// Exact 1:1 USD to USDT, with no rate source and no rounding.
    Parity,
    /// The price was already in the asset sent (a Bitcoin price paid in
    /// Bitcoin): no conversion.
    Native,
    /// One Bitcoin rate fixed by paykit-server when the request was
    /// prepared, never refetched.
    FixedRate,
}

impl QuoteBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parity => "parity",
            Self::Native => "native",
            Self::FixedRate => "fixed_rate",
        }
    }
}

/// Why an order's price cannot be quoted in USDT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParityQuoteError {
    /// Only `USD/2` prices convert at parity.
    UnsupportedPrice,
    /// The total is not a positive amount.
    NotPositive,
    /// The amount in millionths does not fit the column.
    Overflow,
}

/// The amount a buyer sends in `asset`, recorded on the order when the
/// payment option is bound. The order's `currency` and `total_minor` stay the
/// price of record; this is what the buyer pays (`$25.00` as `25.000000`
/// USDT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentTerms {
    pub asset: SettlementAsset,
    pub network: PaymentNetwork,
    pub amount_minor: i64,
    pub quote_basis: QuoteBasis,
}

/// USDT millionths per USD cent at parity.
const USDT_MILLIONTHS_PER_CENT: i64 = 10_000;

impl PaymentTerms {
    /// USDT0 on Arbitrum One at exact parity with a `USD/2` price:
    /// `usdt_millionths = total_cents x 10_000`, the rule paykit-server
    /// applies to USD to USDT. Any other price is refused: listings are never
    /// priced in USDT, and a rate-based quote needs Paykit to own it.
    pub fn usdt_at_parity(
        currency: &str,
        exponent: i32,
        total_minor: i64,
    ) -> Result<Self, ParityQuoteError> {
        if currency != "USD" || exponent != 2 {
            return Err(ParityQuoteError::UnsupportedPrice);
        }
        if total_minor <= 0 {
            return Err(ParityQuoteError::NotPositive);
        }
        let amount_minor = total_minor
            .checked_mul(USDT_MILLIONTHS_PER_CENT)
            .ok_or(ParityQuoteError::Overflow)?;
        Ok(Self {
            asset: SettlementAsset::Usdt,
            network: PaymentNetwork::ArbitrumOne,
            amount_minor,
            quote_basis: QuoteBasis::Parity,
        })
    }

    /// The terms of an order that settled on `asset` for `amount_minor` base
    /// units, as paykit-server reported them. A USD price paid in USDT is
    /// parity; a Bitcoin price paid in Bitcoin is native; every other pair
    /// crossed Bitcoin at the rate paykit-server fixed.
    pub fn settled(
        denomination: PaymentAsset,
        asset: AcceptedAsset,
        amount_minor: i64,
    ) -> Option<Self> {
        if amount_minor <= 0 {
            return None;
        }
        let (asset, quote_basis) = match (denomination, asset) {
            (PaymentAsset::Usd, AcceptedAsset::Usdt) => (SettlementAsset::Usdt, QuoteBasis::Parity),
            (PaymentAsset::Btc, AcceptedAsset::Btc) => (SettlementAsset::Btc, QuoteBasis::Native),
            (_, AcceptedAsset::Btc) => (SettlementAsset::Btc, QuoteBasis::FixedRate),
            (_, AcceptedAsset::Usdt) => (SettlementAsset::Usdt, QuoteBasis::FixedRate),
        };
        Some(Self {
            asset,
            network: asset.network(),
            amount_minor,
            quote_basis,
        })
    }

    /// Records the terms on the order in one statement, so the five columns
    /// are written together or not at all. Returns whether the order took
    /// them: an order that already carries terms keeps them.
    pub async fn store(
        &self,
        conn: &mut PgConnection,
        order_id: Uuid,
    ) -> Result<bool, sqlx::Error> {
        let updated = sqlx::query(
            "UPDATE orders SET payment_asset = $2, payment_network = $3, \
             payment_amount_minor = $4, payment_exponent = $5, payment_quote_basis = $6 \
             WHERE id = $1 AND payment_asset IS NULL",
        )
        .bind(order_id)
        .bind(self.asset.as_str())
        .bind(self.network.as_str())
        .bind(self.amount_minor)
        .bind(self.asset.exponent())
        .bind(self.quote_basis.as_str())
        .execute(&mut *conn)
        .await?;
        Ok(updated.rows_affected() == 1)
    }
}

/// The assets the deployed paykit-server's Marketplace prepare accepts
/// (`PAYKIT_MARKETPLACE_ASSETS`). It is configuration, not a probe: an upstream
/// that cannot carry USDT must never be sent a body it would reject, so USDT
/// is listed only once the deployed contract has it. Bitcoin is always
/// present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketplaceAssets {
    usdt: bool,
}

impl MarketplaceAssets {
    pub const BTC_ONLY: Self = Self { usdt: false };

    pub const fn includes_usdt(self) -> bool {
        self.usdt
    }

    /// Parses the comma-separated variable: unset or blank is Bitcoin only;
    /// otherwise every entry is `BTC` or `USDT`, `BTC` is required, nothing
    /// repeats, and `USDT` needs the upstream API (the fork has no USDT).
    pub fn parse(value: Option<&str>, api: PaykitApi) -> anyhow::Result<Self> {
        const NAME: &str = "PAYKIT_MARKETPLACE_ASSETS";
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::BTC_ONLY);
        };
        let (mut btc, mut usdt) = (false, false);
        for entry in value.split(',').map(str::trim) {
            let seen = match entry {
                "BTC" => &mut btc,
                "USDT" => &mut usdt,
                "" => anyhow::bail!("{NAME} must not contain an empty entry"),
                other => anyhow::bail!("{NAME} entry {other:?} must be BTC or USDT"),
            };
            if std::mem::replace(seen, true) {
                anyhow::bail!("{NAME} must not repeat {entry}");
            }
        }
        if !btc {
            anyhow::bail!("{NAME} must include BTC");
        }
        if usdt && api != PaykitApi::Upstream {
            anyhow::bail!("{NAME} may include USDT only with PAYKIT_SERVER_API=upstream");
        }
        Ok(Self { usdt })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bitcoin_and_usd_are_known_denominations() {
        assert_eq!(PaymentAsset::from_column("BTC"), Some(PaymentAsset::Btc));
        assert_eq!(PaymentAsset::from_column("USD"), Some(PaymentAsset::Usd));
        assert_eq!(PaymentAsset::Btc.as_str(), "BTC");
        assert_eq!(PaymentAsset::Usd.as_str(), "USD");
        for other in ["", "btc", "usd", "USDT", "EUR"] {
            assert_eq!(PaymentAsset::from_column(other), None, "{other:?}");
        }
    }

    #[test]
    fn every_option_has_one_id_and_one_method_and_both_round_trip() {
        let ids: Vec<&str> = PaymentOption::ALL
            .into_iter()
            .map(PaymentOption::id)
            .collect();
        assert_eq!(
            ids,
            [
                "paykit.btc.bitcoin",
                "paykit.usdt.arbitrum-one",
                "paypal.fiat"
            ]
        );
        let methods: Vec<&str> = PaymentOption::ALL
            .into_iter()
            .map(PaymentOption::method)
            .collect();
        assert_eq!(methods, ["bitcoin", "usdt", "paypal"]);
        for option in PaymentOption::ALL {
            assert_eq!(PaymentOption::from_id(option.id()), Some(option));
            assert_eq!(PaymentOption::from_method(option.method()), Some(option));
        }
        for unknown in ["", "BITCOIN", "USDT", "paykit.usdt", "paypal", "lightning"] {
            assert_eq!(PaymentOption::from_id(unknown), None, "{unknown:?}");
        }
        for unknown in [
            "",
            "BITCOIN",
            "USDT",
            "paykit.usdt.arbitrum-one",
            "lightning",
        ] {
            assert_eq!(PaymentOption::from_method(unknown), None, "{unknown:?}");
        }
    }

    #[test]
    fn only_usdt_needs_the_flag_and_only_onchain_options_have_a_network() {
        let flagged: Vec<PaymentOption> = PaymentOption::ALL
            .into_iter()
            .filter(|option| option.requires_usdt_flag())
            .collect();
        assert_eq!(flagged, [PaymentOption::PaykitUsdtArbitrumOne]);
        assert_eq!(
            PaymentOption::PaykitBtcBitcoin.network(),
            Some(PaymentNetwork::Bitcoin)
        );
        assert_eq!(
            PaymentOption::PaykitUsdtArbitrumOne.network(),
            Some(PaymentNetwork::ArbitrumOne)
        );
        assert_eq!(PaymentOption::PaypalFiat.network(), None);
        assert_eq!(PaymentNetwork::Bitcoin.as_str(), "bitcoin");
        assert_eq!(PaymentNetwork::ArbitrumOne.as_str(), "arbitrum-one");
    }

    #[test]
    fn a_usd_total_quotes_exact_parity_in_millionths() {
        for (cents, millionths) in [
            (1, 10_000),
            (100, 1_000_000),
            (2_500, 25_000_000),
            (12_345, 123_450_000),
            (i64::MAX / 10_000, (i64::MAX / 10_000) * 10_000),
        ] {
            let terms = PaymentTerms::usdt_at_parity("USD", 2, cents).expect("parity quote");
            assert_eq!(terms.amount_minor, millionths, "{cents} cents");
            assert_eq!(terms.asset, SettlementAsset::Usdt);
            assert_eq!(terms.asset.as_str(), "USDT");
            assert_eq!(terms.asset.exponent(), 6);
            assert_eq!(terms.network, PaymentNetwork::ArbitrumOne);
            assert_eq!(terms.quote_basis, QuoteBasis::Parity);
            assert_eq!(terms.quote_basis.as_str(), "parity");
        }
    }

    #[test]
    fn a_price_that_is_not_usd_cents_has_no_parity_quote() {
        for (currency, exponent) in [
            ("USD", 0),
            ("USD", 3),
            ("EUR", 2),
            ("SAT", 0),
            ("BTC", 8),
            ("USDT", 6),
            ("usd", 2),
        ] {
            assert_eq!(
                PaymentTerms::usdt_at_parity(currency, exponent, 2_500),
                Err(ParityQuoteError::UnsupportedPrice),
                "{currency}/{exponent}"
            );
        }
    }

    #[test]
    fn a_total_that_is_not_positive_or_overflows_has_no_parity_quote() {
        for total in [0, -1, i64::MIN] {
            assert_eq!(
                PaymentTerms::usdt_at_parity("USD", 2, total),
                Err(ParityQuoteError::NotPositive),
                "{total}"
            );
        }
        for total in [i64::MAX / 10_000 + 1, i64::MAX] {
            assert_eq!(
                PaymentTerms::usdt_at_parity("USD", 2, total),
                Err(ParityQuoteError::Overflow),
                "{total}"
            );
        }
    }

    #[test]
    fn an_offer_reads_back_what_it_wrote_and_refuses_anything_else() {
        let offered = OfferedAssets {
            btc_sats: Some(51_637),
            usdt_units: Some(137_000_000),
        };
        assert_eq!(
            offered.to_json(),
            serde_json::json!({ "BTC": 51_637, "USDT": 137_000_000 })
        );
        assert_eq!(OfferedAssets::from_json(&offered.to_json()), Some(offered));
        assert_eq!(offered.assets(), [AcceptedAsset::Btc, AcceptedAsset::Usdt]);
        let usdt_only = OfferedAssets {
            btc_sats: None,
            usdt_units: Some(5),
        };
        assert_eq!(usdt_only.assets(), [AcceptedAsset::Usdt]);
        assert_eq!(usdt_only.to_json(), serde_json::json!({ "USDT": 5 }));
        assert!(OfferedAssets::default().is_empty());
        for bad in [
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!({ "BTC": 0 }),
            serde_json::json!({ "BTC": -1 }),
            serde_json::json!({ "BTC": "5" }),
            serde_json::json!({ "DAI": 5 }),
            serde_json::json!({ "btc": 5 }),
            serde_json::Value::Null,
        ] {
            assert_eq!(OfferedAssets::from_json(&bad), None, "{bad}");
        }
    }

    #[test]
    fn a_settled_rail_records_its_asset_and_how_the_price_was_derived() {
        use AcceptedAsset::{Btc, Usdt};
        use PaymentAsset::{Btc as PriceBtc, Usd};
        let cases = [
            (
                Usd,
                Usdt,
                SettlementAsset::Usdt,
                "usdt",
                "parity",
                "arbitrum-one",
                6,
            ),
            (
                Usd,
                Btc,
                SettlementAsset::Btc,
                "btc",
                "fixed_rate",
                "bitcoin",
                8,
            ),
            (
                PriceBtc,
                Btc,
                SettlementAsset::Btc,
                "btc",
                "native",
                "bitcoin",
                8,
            ),
            (
                PriceBtc,
                Usdt,
                SettlementAsset::Usdt,
                "usdt",
                "fixed_rate",
                "arbitrum-one",
                6,
            ),
        ];
        for (price, paid, asset, _, basis, network, exponent) in cases {
            let terms = PaymentTerms::settled(price, paid, 42).expect("terms");
            assert_eq!(terms.asset, asset);
            assert_eq!(terms.quote_basis.as_str(), basis);
            assert_eq!(terms.network.as_str(), network);
            assert_eq!(terms.asset.exponent(), exponent);
            assert_eq!(terms.amount_minor, 42);
        }
        for amount in [0, -1] {
            assert_eq!(PaymentTerms::settled(Usd, Usdt, amount), None);
        }
        assert_eq!(AcceptedAsset::from_wire("BTC"), Some(Btc));
        assert_eq!(AcceptedAsset::from_wire("btc"), None);
        assert_eq!(
            serde_json::to_value(Usdt).expect("serializes"),
            serde_json::json!("USDT")
        );
    }

    #[test]
    fn marketplace_assets_default_to_bitcoin_only() {
        for unset in [None, Some(""), Some("   ")] {
            for api in [PaykitApi::Fork, PaykitApi::Upstream] {
                let assets = MarketplaceAssets::parse(unset, api).expect("default");
                assert_eq!(assets, MarketplaceAssets::BTC_ONLY, "{unset:?}");
                assert!(!assets.includes_usdt());
            }
        }
    }

    #[test]
    fn marketplace_assets_list_usdt_only_on_the_upstream_api() {
        for value in ["BTC,USDT", " BTC , USDT ", "USDT,BTC"] {
            let assets = MarketplaceAssets::parse(Some(value), PaykitApi::Upstream).expect(value);
            assert!(assets.includes_usdt(), "{value:?}");
            let refused = MarketplaceAssets::parse(Some(value), PaykitApi::Fork)
                .expect_err("the fork has no USDT");
            assert!(refused.to_string().contains("upstream"), "{refused}");
        }
        for api in [PaykitApi::Fork, PaykitApi::Upstream] {
            let assets = MarketplaceAssets::parse(Some("BTC"), api).expect("BTC");
            assert!(!assets.includes_usdt());
        }
    }

    #[test]
    fn marketplace_assets_refuse_a_malformed_list() {
        for (value, reason) in [
            ("USDT", "must include BTC"),
            ("BTC,", "empty entry"),
            (",BTC", "empty entry"),
            ("BTC,,USDT", "empty entry"),
            ("BTC,BTC", "repeat BTC"),
            ("BTC,USDT,USDT", "repeat USDT"),
            ("BTC,DAI", "must be BTC or USDT"),
            ("btc", "must be BTC or USDT"),
            ("BTC USDT", "must be BTC or USDT"),
        ] {
            let refused =
                MarketplaceAssets::parse(Some(value), PaykitApi::Upstream).expect_err(value);
            assert!(refused.to_string().contains(reason), "{value:?}: {refused}");
        }
    }
}
