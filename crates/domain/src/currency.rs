//! The currencies the marketplace prices listings and orders in.
//!
//! Seven fiat currencies (ISO 4217 minor-unit exponents) plus Bitcoin in two
//! spellings. Every listing, offer, bid and order amount must be one of
//! these with the exponent this registry assigns; anything else is refused
//! with [`REASON_CURRENCY_UNSUPPORTED`] or [`REASON_CURRENCY_EXPONENT`].
//!
//! The Blocktank rate feed (`https://api1.blocktank.to/api/fx/rates/btc`)
//! carries one `BTC<CCY>` ticker per fiat currency; [`Currency::feed_symbol`]
//! names it.

use std::fmt;

/// The refusal reason for a currency outside the registry.
pub const REASON_CURRENCY_UNSUPPORTED: &str = "currency_unsupported";
/// The refusal reason for a registry currency with the wrong exponent.
pub const REASON_CURRENCY_EXPONENT: &str = "currency_exponent_mismatch";
/// The refusal reason for a Locks-gated listing priced in a currency the
/// Lock Server cannot count (anything but USD, BTC and SAT).
pub const REASON_LOCKS_CURRENCY_UNSUPPORTED: &str = "locks_currency_unsupported";

/// The supported codes, in the order clients should list them.
pub const SUPPORTED_CODES: &str = "USD, EUR, CHF, GBP, JPY, CAD, AUD, BTC, SAT";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Currency {
    Usd,
    Eur,
    Chf,
    Gbp,
    Jpy,
    Cad,
    Aud,
    /// Bitcoin priced in satoshis at exponent 8 (one minor unit is a sat).
    Btc,
    /// Satoshis at exponent 0.
    Sat,
}

impl Currency {
    /// The fiat currencies, each with a `BTC<CCY>` ticker on the feed.
    pub const FIAT: [Currency; 7] = [
        Currency::Usd,
        Currency::Eur,
        Currency::Chf,
        Currency::Gbp,
        Currency::Jpy,
        Currency::Cad,
        Currency::Aud,
    ];

    pub fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "USD" => Self::Usd,
            "EUR" => Self::Eur,
            "CHF" => Self::Chf,
            "GBP" => Self::Gbp,
            "JPY" => Self::Jpy,
            "CAD" => Self::Cad,
            "AUD" => Self::Aud,
            "BTC" => Self::Btc,
            "SAT" => Self::Sat,
            _ => return None,
        })
    }

    /// The currency for a `(code, exponent)` pair, only when the exponent is
    /// the one the registry assigns.
    pub fn for_money(code: &str, exponent: i32) -> Option<Self> {
        Self::parse(code).filter(|currency| currency.exponent() == exponent)
    }

    pub fn code(self) -> &'static str {
        match self {
            Self::Usd => "USD",
            Self::Eur => "EUR",
            Self::Chf => "CHF",
            Self::Gbp => "GBP",
            Self::Jpy => "JPY",
            Self::Cad => "CAD",
            Self::Aud => "AUD",
            Self::Btc => "BTC",
            Self::Sat => "SAT",
        }
    }

    /// ISO 4217 minor-unit exponent for fiat (JPY 0, the others 2); 8 for
    /// BTC and 0 for SAT.
    pub fn exponent(self) -> i32 {
        match self {
            Self::Jpy | Self::Sat => 0,
            Self::Btc => 8,
            Self::Usd | Self::Eur | Self::Chf | Self::Gbp | Self::Cad | Self::Aud => 2,
        }
    }

    pub fn is_fiat(self) -> bool {
        !self.is_bitcoin()
    }

    /// `BTC` and `SAT` totals are both already satoshi minor units.
    pub fn is_bitcoin(self) -> bool {
        matches!(self, Self::Btc | Self::Sat)
    }

    /// The Blocktank ticker symbol for a fiat currency (`BTCEUR`), `None`
    /// for the Bitcoin spellings.
    pub fn feed_symbol(self) -> Option<&'static str> {
        Some(match self {
            Self::Usd => "BTCUSD",
            Self::Eur => "BTCEUR",
            Self::Chf => "BTCCHF",
            Self::Gbp => "BTCGBP",
            Self::Jpy => "BTCJPY",
            Self::Cad => "BTCCAD",
            Self::Aud => "BTCAUD",
            Self::Btc | Self::Sat => return None,
        })
    }

    /// Whether the Lock Server can count a price in this denomination:
    /// USD (cents), BTC and SAT (sats).
    pub fn locks_supported(self) -> bool {
        matches!(self, Self::Usd | Self::Btc | Self::Sat)
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// Why a `(code, exponent)` pair is not a registry money denomination.
pub fn refusal_reason(code: &str, exponent: i32) -> Option<&'static str> {
    match Currency::parse(code) {
        None => Some(REASON_CURRENCY_UNSUPPORTED),
        Some(currency) if currency.exponent() != exponent => Some(REASON_CURRENCY_EXPONENT),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_exponents_follow_iso_4217_and_the_bitcoin_units() {
        let expected = [
            ("USD", 2),
            ("EUR", 2),
            ("CHF", 2),
            ("GBP", 2),
            ("JPY", 0),
            ("CAD", 2),
            ("AUD", 2),
            ("BTC", 8),
            ("SAT", 0),
        ];
        for (code, exponent) in expected {
            let currency = Currency::parse(code).expect(code);
            assert_eq!(currency.code(), code);
            assert_eq!(currency.exponent(), exponent, "{code}");
            assert_eq!(Currency::for_money(code, exponent), Some(currency));
            assert_eq!(Currency::for_money(code, exponent + 1), None, "{code}");
        }
    }

    #[test]
    fn only_the_registry_codes_parse() {
        for code in [
            "", "usd", "Usd", "USDT", "XBT", "SEK", "XAU", "BTC ", "EURO",
        ] {
            assert_eq!(Currency::parse(code), None, "{code:?}");
        }
        assert_eq!(Currency::FIAT.len(), 7);
    }

    #[test]
    fn fiat_has_a_feed_symbol_and_bitcoin_does_not() {
        for currency in Currency::FIAT {
            assert!(currency.is_fiat());
            assert_eq!(
                currency.feed_symbol().map(str::to_string),
                Some(format!("BTC{}", currency.code()))
            );
        }
        for currency in [Currency::Btc, Currency::Sat] {
            assert!(currency.is_bitcoin());
            assert!(!currency.is_fiat());
            assert_eq!(currency.feed_symbol(), None);
        }
    }

    #[test]
    fn locks_counts_only_usd_and_bitcoin() {
        let supported: Vec<_> = [
            "USD", "EUR", "CHF", "GBP", "JPY", "CAD", "AUD", "BTC", "SAT",
        ]
        .into_iter()
        .filter(|code| Currency::parse(code).unwrap().locks_supported())
        .collect();
        assert_eq!(supported, ["USD", "BTC", "SAT"]);
    }

    #[test]
    fn refusal_reasons_name_the_unsupported_and_the_mis_exponented() {
        assert_eq!(refusal_reason("USD", 2), None);
        assert_eq!(refusal_reason("JPY", 0), None);
        assert_eq!(refusal_reason("JPY", 2), Some(REASON_CURRENCY_EXPONENT));
        assert_eq!(refusal_reason("USD", 0), Some(REASON_CURRENCY_EXPONENT));
        assert_eq!(refusal_reason("SEK", 2), Some(REASON_CURRENCY_UNSUPPORTED));
        assert_eq!(refusal_reason("", 2), Some(REASON_CURRENCY_UNSUPPORTED));
    }
}
