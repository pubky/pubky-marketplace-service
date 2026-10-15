//! A partial refund Pubky made in sats or USDT, as an amount in the order's
//! currency, from the payment basis the order itself recorded.
//!
//! The framework hands the basis over as a [`RefundQuote`] read from the order
//! when the refund was queued (the Bitcoin quote pinned at bind, or the USDT
//! payment terms), so the conversion uses the rate the buyer actually paid at
//! and never a fresh one. All arithmetic is checked integer arithmetic and
//! rounds half up to the currency's minor unit. The basis is also returned as
//! one line of text for the store's refund reason, so the merchant can see how
//! the amount was derived.
//!
//! * Bitcoin: `minor = round(sats * 10^exponent * rate / 10^8)` with `rate` the
//!   price of one BTC in the order currency (`BTC<CCY>`), exactly as the
//!   service priced the order. A quote in another currency than the order's
//!   (a non-USD order priced through USD) is refused rather than guessed.
//! * USDT: `minor = round(amount * order_total / paid)`, the order's own ratio
//!   of what was paid in USDT to what the order totals.

use chrono::SecondsFormat;

use crate::connectors::provider::{ProviderError, RefundQuote, RefundRecord};
use crate::fx::Decimal;

use super::money;

#[derive(Debug, PartialEq, Eq)]
pub struct Converted {
    /// In minor units of the order currency.
    pub minor: i64,
    /// How it was derived, for the store's refund reason.
    pub basis: String,
}

fn unavailable(message: &str) -> ProviderError {
    ProviderError::Rejected {
        code: "refund_basis_unavailable".into(),
        message: message.to_string(),
    }
}

/// `numerator / denominator` rounded half up, `None` on overflow or a zero
/// denominator.
fn round_div(numerator: u128, denominator: u128) -> Option<u128> {
    if denominator == 0 {
        return None;
    }
    numerator
        .checked_add(denominator / 2)
        .map(|rounded| rounded / denominator)
}

pub fn convert(refund: &RefundRecord, amount: i64) -> Result<Converted, ProviderError> {
    let Some(quote) = &refund.quote else {
        return Err(ProviderError::Rejected {
            code: "refund_not_in_store_currency".into(),
            message: "A partial refund paid in another unit cannot be valued in the store's \
                      currency without the order's payment quote; record it in the store by hand."
                .into(),
        });
    };
    let amount =
        u128::try_from(amount).map_err(|_| unavailable("The refund amount is negative."))?;
    let currency = refund.order_currency.to_ascii_uppercase();
    let exponent = refund.order_exponent;
    let overflow = || unavailable("The refund amount is out of range.");
    let (minor, basis) = match (quote, refund.amount_unit.as_str()) {
        (
            RefundQuote::Bitcoin {
                currency: quoted_currency,
                exponent: quoted_exponent,
                rate,
                source,
                fetched_at,
                ..
            },
            "sat",
        ) => {
            if !quoted_currency.eq_ignore_ascii_case(&currency) || *quoted_exponent != exponent {
                return Err(unavailable(
                    "The order's Bitcoin quote is in another currency than the order.",
                ));
            }
            let decimal = Decimal::parse(rate)
                .ok_or_else(|| unavailable("The order's rate is unreadable."))?;
            let exponent_scale = 10u128
                .checked_pow(u32::try_from(exponent).map_err(|_| overflow())?)
                .ok_or_else(overflow)?;
            let rate_scale = 10u128.checked_pow(decimal.scale).ok_or_else(overflow)?;
            let numerator = amount
                .checked_mul(exponent_scale)
                .and_then(|value| value.checked_mul(decimal.mantissa))
                .ok_or_else(overflow)?;
            let denominator = 100_000_000u128
                .checked_mul(rate_scale)
                .ok_or_else(overflow)?;
            let minor = round_div(numerator, denominator).ok_or_else(overflow)?;
            let when = fetched_at
                .map(|at| at.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_else(|| "time unrecorded".into());
            (
                minor,
                format!(
                    "{amount} sat = {} {currency} at BTC{currency} {rate} ({}, {when})",
                    money::format_minor(i64::try_from(minor).map_err(|_| overflow())?, exponent),
                    source.as_deref().unwrap_or("source unrecorded"),
                ),
            )
        }
        (
            RefundQuote::Asset {
                asset,
                paid_minor,
                paid_exponent,
                basis,
                ..
            },
            "usdt",
        ) => {
            let paid = u128::try_from(*paid_minor)
                .ok()
                .filter(|paid| *paid > 0)
                .ok_or_else(|| unavailable("The order recorded no USDT amount."))?;
            let total = u128::try_from(refund.order_total_minor)
                .ok()
                .filter(|total| *total > 0)
                .ok_or_else(|| unavailable("The order has no total."))?;
            let minor = amount
                .checked_mul(total)
                .and_then(|numerator| round_div(numerator, paid))
                .ok_or_else(overflow)?;
            let display = |value: u128, exponent: i32| -> Result<String, ProviderError> {
                Ok(money::format_minor(
                    i64::try_from(value).map_err(|_| overflow())?,
                    exponent,
                ))
            };
            (
                minor,
                format!(
                    "{} {asset} = {} {currency} at the order's payment of {} {asset} for {} {currency}{}",
                    display(amount, *paid_exponent)?,
                    display(minor, exponent)?,
                    display(paid, *paid_exponent)?,
                    display(total, exponent)?,
                    basis.as_deref().map(|basis| format!(" ({basis})")).unwrap_or_default(),
                ),
            )
        }
        _ => {
            return Err(unavailable(
                "The order's payment basis does not match the unit of the refund.",
            ))
        }
    };
    Ok(Converted {
        minor: i64::try_from(minor).map_err(|_| overflow())?,
        basis,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    fn record(unit: &str, quote: Option<RefundQuote>) -> RefundRecord {
        RefundRecord {
            refund_id: "r".into(),
            pubky_order_id: Uuid::nil(),
            amount_minor: None,
            amount_unit: unit.into(),
            transaction_id: None,
            order_total_minor: 5090,
            order_currency: "CHF".into(),
            order_exponent: 2,
            full: false,
            quote,
        }
    }

    fn bitcoin(rate: &str, currency: &str) -> RefundQuote {
        RefundQuote::Bitcoin {
            currency: currency.into(),
            exponent: 2,
            rate: rate.into(),
            quoted_sats: Some(87_759),
            source: Some("blocktank".into()),
            fetched_at: Some("2026-10-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap()),
        }
    }

    fn usdt() -> RefundQuote {
        RefundQuote::Asset {
            asset: "USDT".into(),
            network: Some("ethereum".into()),
            paid_minor: 63_625_000,
            paid_exponent: 6,
            basis: Some("usd parity".into()),
        }
    }

    #[test]
    fn sats_convert_at_the_orders_own_rate_and_say_how() {
        let refund = record("sat", Some(bitcoin("58000.00000000", "CHF")));
        let converted = convert(&refund, 20_000).unwrap();
        assert_eq!(converted.minor, 1160);
        assert_eq!(
            converted.basis,
            "20000 sat = 11.60 CHF at BTCCHF 58000.00000000 (blocktank, 2026-10-15T12:00:00Z)"
        );
    }

    #[test]
    fn sats_round_half_up_and_a_full_quote_returns_the_total() {
        let refund = record("sat", Some(bitcoin("58000", "CHF")));
        assert_eq!(
            convert(&refund, 87_759).unwrap().minor,
            5090,
            "the quoted sats are the order total"
        );
        assert_eq!(
            convert(&refund, 1).unwrap().minor,
            0,
            "0.058 cents rounds down"
        );
        assert_eq!(
            convert(&refund, 863).unwrap().minor,
            50,
            "50.05 cents rounds down"
        );
        let half = record("sat", Some(bitcoin("50000", "CHF")));
        assert_eq!(convert(&half, 1000).unwrap().minor, 50, "exactly 50.0");
        assert_eq!(convert(&half, 1001).unwrap().minor, 50);
        let tie = record("sat", Some(bitcoin("100000.5", "CHF")));
        assert_eq!(convert(&tie, 500).unwrap().minor, 50, "50.00025 cents");
        let rounds_up = record("sat", Some(bitcoin("100000", "CHF")));
        assert_eq!(convert(&rounds_up, 1_500).unwrap().minor, 150);
        assert_eq!(
            convert(&record("sat", Some(bitcoin("65000", "CHF"))), 77)
                .unwrap()
                .minor,
            5,
            "5.005 rounds to 5"
        );
        assert_eq!(
            convert(&record("sat", Some(bitcoin("65000", "CHF"))), 78)
                .unwrap()
                .minor,
            5,
            "5.07 rounds to 5"
        );
        assert_eq!(
            convert(&record("sat", Some(bitcoin("100000", "CHF"))), 5)
                .unwrap()
                .minor,
            1,
            "0.5 rounds up"
        );
    }

    #[test]
    fn usdt_converts_by_the_orders_own_ratio_and_says_how() {
        let refund = record("usdt", Some(usdt()));
        let converted = convert(&refund, 10_000_000).unwrap();
        assert_eq!(converted.minor, 800);
        assert_eq!(
            converted.basis,
            "10.000000 USDT = 8.00 CHF at the order's payment of 63.625000 USDT for 50.90 CHF (usd parity)"
        );
        assert_eq!(
            convert(&refund, 3_333_333).unwrap().minor,
            267,
            "266.66 rounds up"
        );
        assert_eq!(convert(&refund, 63_625_000).unwrap().minor, 5090);
    }

    #[test]
    fn without_a_matching_basis_nothing_is_guessed() {
        let none = record("sat", None);
        assert!(
            matches!(convert(&none, 1000), Err(ProviderError::Rejected { code, .. }) if code == "refund_not_in_store_currency")
        );
        let usd_quote = record("sat", Some(bitcoin("65000", "USD")));
        assert!(
            matches!(convert(&usd_quote, 1000), Err(ProviderError::Rejected { code, .. }) if code == "refund_basis_unavailable")
        );
        let wrong_unit = record("usdt", Some(bitcoin("65000", "CHF")));
        assert!(
            matches!(convert(&wrong_unit, 1000), Err(ProviderError::Rejected { code, .. }) if code == "refund_basis_unavailable")
        );
        let bad_rate = record("sat", Some(bitcoin("not-a-rate", "CHF")));
        assert!(convert(&bad_rate, 1000).is_err());
        assert!(convert(&record("sat", Some(bitcoin("58000", "CHF"))), -1).is_err());
        let mut unpaid = usdt();
        if let RefundQuote::Asset { paid_minor, .. } = &mut unpaid {
            *paid_minor = 0;
        }
        assert!(convert(&record("usdt", Some(unpaid)), 1000).is_err());
    }

    #[test]
    fn huge_amounts_are_computed_exactly_or_refused_never_wrapped() {
        let usdt_refund = record("usdt", Some(usdt()));
        let exact = (u128::from(i64::MAX.unsigned_abs()) * 5090 + 63_625_000 / 2) / 63_625_000;
        assert_eq!(
            convert(&usdt_refund, i64::MAX).unwrap().minor as u128,
            exact
        );
        let absurd = record(
            "sat",
            Some(bitcoin("99999999999999999999999999999.99999999", "CHF")),
        );
        assert!(convert(&absurd, i64::MAX).is_err());
    }
}
