//! One refund model for every provider.
//!
//! A refund Pubky records (`refund.record_external` or a refunded review
//! resolution) carries an amount in the unit `refund_destination` records
//! that order's refunds in. The framework turns it into what a provider is
//! asked to do, in two steps no provider repeats:
//!
//! 1. **Unit.** The queued mirror carries the recorded total in the order
//!    currency. The record names its unit (`external_refund.amount_unit`,
//!    [`RefundUnit`]); a record written before refunds named one is read in
//!    the unit its writer used ([`RefundUnit::unnamed`], the rule migration
//!    0093 backfilled). [`OrderBasis::recorded_unit`] maps it:
//!    - `sat`: an order paid on the Bitcoin rail records its refund in the
//!      sats it received, payment code included, whatever its price currency
//!      ([`BitcoinPayment::received_sats`]). [`order_currency_amount`] takes
//!      a sats refund on a sats-priced order as it is, clamped to the order
//!      total when it includes the payment code or an overpayment, and
//!      converts one on a fiat-priced order in proportion to the sats
//!      received, rounded half up to the minor unit and never above the
//!      total, with the basis for the store refund's note. A fiat-priced
//!      order with no single Bitcoin payment to convert against is blocked.
//!    - `usdt0`: a USDT-paid order (`payment_method = 'usdt'` or
//!      `payment_asset = 'USDT'`) priced in anything but USD records its
//!      refund in USDT0 units, the paid asset's own
//!      (`refund_destination::refund_terms` and
//!      `received_refund_amount_minor`, from the multi-currency change, #133).
//!    - `order`: any other refund is in the order currency. A USD order paid
//!      in USDT is capped at the USDT it received instead of its total, so an
//!      overpaid one can record more than its total; the store, which was
//!      paid the total, is mirrored the total.
//!
//!    [`order_currency_amount`] converts a USDT0 refund to the order currency
//!    with the order's own recorded conversion (`orders.usd_conversion_pair`
//!    and `usd_conversion_cents`, migration 0061: what the order's total
//!    converted to when its USDT request was bound), in exact integer
//!    arithmetic, rounded half up to the minor unit, and describes the basis
//!    for the store refund's note. An order with no recorded conversion, or
//!    with no USDT terms to read the unit from, cannot be converted: the
//!    mirror is queued as blocked and surfaces as a quarantined action for the
//!    seller to record by hand.
//! 2. **Difference.** When the action runs, [`amount_to_mirror`] compares the
//!    recorded total with what the store already shows refunded
//!    ([`StoreRefundTotals`](super::provider::StoreRefundTotals), read from
//!    the provider fresh) and yields only the amount still to mirror, never
//!    taking the store's refunds above its payment.
//!
//! Every connector table is created by migrations 0062 and later of the
//! connector PRs and `CONNECTORS_ENABLED` defaults off, so no mirror queued
//! under an older mirror payload shape (an amount in sats or USDT with an
//! `amount_unit` label) can exist on any database: the payload written here,
//! always in the order currency, is the only one the worker has ever read.

use super::provider::{ProviderError, StoreRefundTotals};
#[cfg(doc)]
use crate::refund_destination::BitcoinPayment;
use crate::refund_destination::{priced_in_sats, RefundUnit};

/// Blocked-mirror reason: the recorded unit cannot be turned into the order
/// currency.
pub const REFUND_AMOUNT_UNCONVERTIBLE: &str = "refund_amount_unconvertible";
pub const REFUND_AMOUNT_MISSING: &str = "refund_amount_missing";
pub const CURRENCY_MISMATCH: &str = "currency_mismatch";
pub const REFUND_EXCEEDS_PAYMENT: &str = "refund_exceeds_payment";

/// The unit an order records its refunds in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedUnit {
    /// The order currency's minor units.
    OrderCurrency,
    /// USDT0 base units, `10^-exponent` of a USDT each. The exponent is the
    /// order's `payment_exponent`; it is `None` when the order carries no
    /// USDT terms to read it from.
    Usdt0 { exponent: Option<i32> },
    /// Sats, of the `received` sats the order was paid on the Bitcoin rail;
    /// `None` when no single Bitcoin payment is on record.
    Sats { received: Option<i64> },
}

/// The order facts a refund is recorded and converted against, as the
/// `orders` columns hold them, and the unit the refund record is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBasis {
    pub currency: String,
    pub exponent: i32,
    pub total_minor: i64,
    /// The unit of the recorded refund's `amount_minor`.
    pub unit: RefundUnit,
    pub payment_exponent: Option<i32>,
    /// The order's recorded conversion to USD cents (`orders.usd_conversion_*`,
    /// migration 0061); `None` for every order bound without one.
    pub conversion: Option<UsdConversion>,
    /// The sats the order received on the Bitcoin rail
    /// ([`BitcoinPayment::received_sats`]); `None` off that rail.
    pub received_sats: Option<i64>,
    /// The order was paid in USDT (`refund_destination::is_usdt_payment`).
    pub usdt_paid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsdConversion {
    /// `EURUSD`: USD per one unit of the order currency.
    pub pair: String,
    /// The USD amount the multi-asset request was denominated in, in cents:
    /// what the order's total converted to.
    pub cents: i64,
}

impl OrderBasis {
    /// The recorded refund's unit, with what converting it needs.
    pub fn recorded_unit(&self) -> RecordedUnit {
        match self.unit {
            RefundUnit::Order => RecordedUnit::OrderCurrency,
            RefundUnit::Usdt0 => RecordedUnit::Usdt0 {
                exponent: self.payment_exponent,
            },
            RefundUnit::Sat => RecordedUnit::Sats {
                received: self.received_sats,
            },
        }
    }
}

/// A recorded refund total in the order currency, and how it got there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCurrencyAmount {
    pub amount_minor: i64,
    /// Set when the recorded amount was converted: for the store refund's
    /// note.
    pub basis: Option<String>,
}

/// A refund that cannot be mirrored as recorded: the seller must record it in
/// the store by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocked {
    pub code: &'static str,
    pub detail: String,
}

/// The recorded refund total (`recorded_minor`, in the order's recorded unit)
/// in the order currency's minor units.
pub fn order_currency_amount(
    order: &OrderBasis,
    recorded_minor: i64,
) -> Result<OrderCurrencyAmount, Blocked> {
    match order.recorded_unit() {
        RecordedUnit::OrderCurrency if order.usdt_paid && recorded_minor > order.total_minor => {
            Ok(OrderCurrencyAmount {
                amount_minor: order.total_minor,
                basis: Some(format!(
                    "Refunded {} {} in USDT, more than the order total because the buyer \
                     overpaid; the store records the order total.",
                    decimal(i128::from(recorded_minor), order.exponent),
                    order.currency,
                )),
            })
        }
        RecordedUnit::OrderCurrency => Ok(OrderCurrencyAmount {
            amount_minor: recorded_minor,
            basis: None,
        }),
        RecordedUnit::Usdt0 { exponent } => {
            let (Some(conversion), Some(exponent)) = (&order.conversion, exponent) else {
                return Err(Blocked {
                    code: REFUND_AMOUNT_UNCONVERTIBLE,
                    detail: format!(
                        "The refund is recorded in USDT0 units and the order, priced in {}, has \
                         no recorded conversion or USDT terms to convert it with; record it in the \
                         store by hand.",
                        order.currency
                    ),
                });
            };
            let Some(converted) = convert_usdt0(order, conversion, exponent, recorded_minor) else {
                return Err(Blocked {
                    code: REFUND_AMOUNT_UNCONVERTIBLE,
                    detail: format!(
                        "The USDT0 refund cannot be converted to {} exactly; record it in the \
                         store by hand.",
                        order.currency
                    ),
                });
            };
            Ok(OrderCurrencyAmount {
                amount_minor: converted,
                basis: Some(format!(
                    "Refunded {} USDT0, converted to {} at the order's own conversion ({} {} = \
                     {} USD, {}), rounded half up to the minor unit.",
                    decimal(i128::from(recorded_minor), exponent),
                    order.currency,
                    decimal(i128::from(order.total_minor), order.exponent),
                    order.currency,
                    decimal(i128::from(conversion.cents), 2),
                    conversion.pair,
                )),
            })
        }
        RecordedUnit::Sats { received } => sats_in_order_currency(order, received, recorded_minor),
    }
}

/// A sats refund in the order currency. A sats-priced order's minor unit is
/// the sat, so the refund stands as recorded up to the total: what it
/// records above, the payment code or an overpayment, has no place on the
/// store order. A fiat-priced order's refund is the same share of its total
/// as the refund is of the sats received.
fn sats_in_order_currency(
    order: &OrderBasis,
    received: Option<i64>,
    recorded_minor: i64,
) -> Result<OrderCurrencyAmount, Blocked> {
    if priced_in_sats(&order.currency, order.exponent) {
        let amount_minor = recorded_minor.min(order.total_minor);
        let basis = (recorded_minor > order.total_minor).then(|| {
            format!(
                "Refunded {recorded_minor} sats, more than the order total of {} sats: the \
                 payment code or an overpayment, which the store order does not carry. Mirrored \
                 as the order total.",
                order.total_minor
            )
        });
        return Ok(OrderCurrencyAmount {
            amount_minor,
            basis,
        });
    }
    let Some(received) = received else {
        return Err(Blocked {
            code: REFUND_AMOUNT_UNCONVERTIBLE,
            detail: format!(
                "The refund is recorded in sats and the order, priced in {}, has no single \
                 Bitcoin payment to convert it against; record it in the store by hand.",
                order.currency
            ),
        });
    };
    let Some(converted) = proportional(recorded_minor, order.total_minor, received) else {
        return Err(Blocked {
            code: REFUND_AMOUNT_UNCONVERTIBLE,
            detail: format!(
                "The refund of {recorded_minor} sats cannot be converted to {}; record it in the \
                 store by hand.",
                order.currency
            ),
        });
    };
    Ok(OrderCurrencyAmount {
        amount_minor: converted,
        basis: Some(format!(
            "Refunded {recorded_minor} of the {received} sats received for {} {}, converted in \
             that proportion and rounded half up to the minor unit.",
            decimal(i128::from(order.total_minor), order.exponent),
            order.currency,
        )),
    })
}

/// `recorded x total / received`, rounded half up and never above the total.
fn proportional(recorded_minor: i64, total_minor: i64, received: i64) -> Option<i64> {
    if recorded_minor < 0 || received <= 0 {
        return None;
    }
    let numerator = i128::from(recorded_minor).checked_mul(i128::from(total_minor))?;
    let denominator = i128::from(received);
    let rounded = numerator.checked_add(denominator / 2)? / denominator;
    i64::try_from(rounded.min(i128::from(total_minor))).ok()
}

/// `refund x 10^-exponent` USDT0 is `refund x 100 / 10^exponent` USD cents,
/// and the order's conversion makes `cents` USD cents the order's
/// `total_minor`, so the refund is `x * total * 100 / (cents * 10^exponent)`
/// order units, rounded half up and never above the order total.
fn convert_usdt0(
    order: &OrderBasis,
    conversion: &UsdConversion,
    exponent: i32,
    recorded_minor: i64,
) -> Option<i64> {
    if recorded_minor < 0 || exponent < 0 {
        return None;
    }
    let numerator = i128::from(recorded_minor)
        .checked_mul(i128::from(order.total_minor))?
        .checked_mul(100)?;
    let denominator = i128::from(conversion.cents)
        .checked_mul(10_i128.checked_pow(u32::try_from(exponent).ok()?)?)?;
    if denominator <= 0 {
        return None;
    }
    let rounded = numerator.checked_add(denominator / 2)? / denominator;
    i64::try_from(rounded.min(i128::from(order.total_minor))).ok()
}

/// `minor x 10^-exponent` as a decimal string.
fn decimal(minor: i128, exponent: i32) -> String {
    let Ok(exponent) = usize::try_from(exponent) else {
        return minor.to_string();
    };
    if exponent == 0 {
        return minor.to_string();
    }
    let sign = if minor < 0 { "-" } else { "" };
    let digits = minor.unsigned_abs().to_string();
    let padded = format!("{digits:0>width$}", width = exponent + 1);
    let (whole, fraction) = padded.split_at(padded.len() - exponent);
    format!("{sign}{whole}.{fraction}")
}

/// What is still to mirror to a store, given the recorded refund total and
/// what the store already shows: `None` when the store already shows at least
/// the recorded total. The store's refunds never go above its payment: a
/// recorded total larger than the payment is refused.
pub fn amount_to_mirror(
    recorded_total_minor: i64,
    order_currency: &str,
    store: &StoreRefundTotals,
) -> Result<Option<i64>, ProviderError> {
    if recorded_total_minor <= 0 {
        return Err(rejected(
            REFUND_AMOUNT_MISSING,
            "The recorded refund has no amount to mirror.",
        ));
    }
    if !store.currency.eq_ignore_ascii_case(order_currency) {
        return Err(rejected(
            CURRENCY_MISMATCH,
            format!(
                "The store order is in {} but the Pubky refund is in {order_currency}; record it \
                 in the store by hand.",
                store.currency
            ),
        ));
    }
    if recorded_total_minor > store.paid_minor {
        return Err(rejected(
            REFUND_EXCEEDS_PAYMENT,
            "The refund is larger than the payment recorded on the store order.",
        ));
    }
    let delta = recorded_total_minor - store.refunded_minor;
    Ok((delta > 0).then_some(delta))
}

fn rejected(code: &str, message: impl Into<String>) -> ProviderError {
    ProviderError::Rejected {
        code: code.to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(
        currency: &str,
        total_minor: i64,
        unit: RefundUnit,
        exponent: Option<i32>,
        conversion: Option<(&str, i64)>,
    ) -> OrderBasis {
        OrderBasis {
            currency: currency.into(),
            exponent: 2,
            total_minor,
            unit,
            payment_exponent: exponent,
            conversion: conversion.map(|(pair, cents)| UsdConversion {
                pair: pair.into(),
                cents,
            }),
            received_sats: None,
            usdt_paid: false,
        }
    }

    fn bitcoin_order(currency: &str, exponent: i32, total_minor: i64, received: i64) -> OrderBasis {
        OrderBasis {
            exponent,
            received_sats: Some(received),
            ..order(currency, total_minor, RefundUnit::Sat, None, None)
        }
    }

    fn eur_usdt_order() -> OrderBasis {
        order(
            "EUR",
            13_700,
            RefundUnit::Usdt0,
            Some(6),
            Some(("EURUSD", 14_854)),
        )
    }

    fn store(paid: i64, refunded: i64) -> StoreRefundTotals {
        StoreRefundTotals {
            currency: "EUR".into(),
            paid_minor: paid,
            refunded_minor: refunded,
        }
    }

    #[test]
    fn an_order_in_its_own_currency_records_in_it() {
        let usd = order("USD", 5_000, RefundUnit::Order, Some(6), None);
        assert_eq!(usd.recorded_unit(), RecordedUnit::OrderCurrency);
        assert_eq!(
            order_currency_amount(&usd, 1_234).expect("amount"),
            OrderCurrencyAmount {
                amount_minor: 1_234,
                basis: None
            }
        );
    }

    #[test]
    fn an_overpaid_usd_usdt_refund_mirrors_the_order_total() {
        // 50.00 USD overpaid by 1.00 USDT and refunded as received: the store
        // was paid 50.00, so it is mirrored 50.00 and says why.
        let usd = OrderBasis {
            usdt_paid: true,
            ..order("USD", 5_000, RefundUnit::Order, Some(6), None)
        };
        let mirrored = order_currency_amount(&usd, 5_100).expect("amount");
        assert_eq!(mirrored.amount_minor, 5_000);
        let basis = mirrored.basis.expect("a basis");
        assert!(basis.contains("51.00 USD in USDT"), "{basis}");
        assert!(basis.contains("the buyer overpaid"), "{basis}");
        assert_eq!(
            amount_to_mirror(
                mirrored.amount_minor,
                "USD",
                &StoreRefundTotals {
                    currency: "USD".into(),
                    paid_minor: 5_000,
                    refunded_minor: 0,
                },
            )
            .expect("mirrors"),
            Some(5_000)
        );
        // Any other rail keeps what was recorded: its record is capped at the
        // total already.
        let paypal = order("USD", 5_000, RefundUnit::Order, None, None);
        assert_eq!(
            order_currency_amount(&paypal, 5_100)
                .expect("amount")
                .amount_minor,
            5_100
        );
    }

    #[test]
    fn the_recorded_unit_is_the_one_the_record_names() {
        assert_eq!(
            order("EUR", 100, RefundUnit::Usdt0, None, None).recorded_unit(),
            RecordedUnit::Usdt0 { exponent: None }
        );
        assert_eq!(
            order("EUR", 100, RefundUnit::Usdt0, Some(6), None).recorded_unit(),
            RecordedUnit::Usdt0 { exponent: Some(6) }
        );
        for currency in ["USD", "SAT"] {
            assert_eq!(
                bitcoin_order(currency, 2, 100, 51_637).recorded_unit(),
                RecordedUnit::Sats {
                    received: Some(51_637)
                }
            );
        }
        // An order-unit record stays in the order currency even when the
        // order now reads as paid on the Bitcoin rail: a refund recorded
        // before refunds were in sats.
        let legacy = OrderBasis {
            unit: RefundUnit::Order,
            ..bitcoin_order("USD", 2, 13_700, 51_637)
        };
        assert_eq!(legacy.recorded_unit(), RecordedUnit::OrderCurrency);
        assert_eq!(
            order_currency_amount(&legacy, 13_700).expect("as recorded"),
            OrderCurrencyAmount {
                amount_minor: 13_700,
                basis: None
            }
        );
    }

    #[test]
    fn a_sats_refund_on_a_fiat_price_with_no_single_bitcoin_payment_is_blocked() {
        // A refunded review recorded the observed sats of an order paid on
        // both rails: no received amount to convert it against.
        let both_rails = order("USD", 13_700, RefundUnit::Sat, None, None);
        let blocked = order_currency_amount(&both_rails, 51_637).expect_err("blocked");
        assert_eq!(blocked.code, REFUND_AMOUNT_UNCONVERTIBLE);
        assert!(blocked.detail.contains("by hand"), "{}", blocked.detail);
        // A sats price needs no conversion.
        let sats_price = OrderBasis {
            exponent: 0,
            ..order("SAT", 50_000, RefundUnit::Sat, None, None)
        };
        assert_eq!(
            order_currency_amount(&sats_price, 20_000)
                .expect("as recorded")
                .amount_minor,
            20_000
        );
    }

    #[test]
    fn the_whole_payment_converts_to_exactly_the_order_total() {
        // 148.54 USD paid as 148.540000 USDT0 for 137.00 EUR.
        let converted = order_currency_amount(&eur_usdt_order(), 148_540_000).expect("amount");
        assert_eq!(converted.amount_minor, 13_700);
        let basis = converted.basis.expect("a basis");
        assert!(basis.contains("148.540000 USDT0"), "{basis}");
        assert!(basis.contains("137.00 EUR = 148.54 USD, EURUSD"), "{basis}");
        assert!(basis.contains("rounded half up"), "{basis}");
    }

    #[test]
    fn a_partial_usdt0_refund_rounds_half_up_to_the_minor_unit() {
        let eur = eur_usdt_order();
        // 74.270000 USDT0 is exactly half: 68.50 EUR.
        assert_eq!(
            order_currency_amount(&eur, 74_270_000)
                .expect("half")
                .amount_minor,
            6_850
        );
        // 1.000000 USDT0 is 92.23 order units: 92.
        let one = order_currency_amount(&eur, 1_000_000).expect("one");
        assert_eq!(one.amount_minor, 92);
        // 1.00 EUR = 2.00 USD, so one order unit is 0.020000 USDT0: half of
        // it, 0.010000 USDT0, is exactly 0.5 units and rounds up to 1, and a
        // millionth less rounds down to 0.
        let tie = order(
            "EUR",
            100,
            RefundUnit::Usdt0,
            Some(6),
            Some(("EURUSD", 200)),
        );
        assert_eq!(
            order_currency_amount(&tie, 10_000)
                .expect("tie")
                .amount_minor,
            1
        );
        assert_eq!(
            order_currency_amount(&tie, 9_999)
                .expect("below")
                .amount_minor,
            0
        );
    }

    #[test]
    fn a_conversion_never_exceeds_the_order_total() {
        let order = eur_usdt_order();
        assert_eq!(
            order_currency_amount(&order, 999_999_999)
                .expect("over")
                .amount_minor,
            13_700
        );
    }

    #[test]
    fn an_order_with_no_recorded_conversion_or_no_usdt_terms_is_blocked() {
        let no_conversion = order("EUR", 13_700, RefundUnit::Usdt0, Some(6), None);
        let no_terms = order(
            "EUR",
            13_700,
            RefundUnit::Usdt0,
            None,
            Some(("EURUSD", 14_854)),
        );
        for unconvertible in [no_conversion, no_terms] {
            let blocked = order_currency_amount(&unconvertible, 1_000_000).expect_err("blocked");
            assert_eq!(blocked.code, REFUND_AMOUNT_UNCONVERTIBLE);
            assert!(blocked.detail.contains("by hand"));
        }
    }

    #[test]
    fn a_sats_refund_on_a_fiat_price_converts_in_proportion_to_the_sats_received() {
        // 137.00 USD paid as 51,637 sats.
        let usd = bitcoin_order("USD", 2, 13_700, 51_637);
        let whole = order_currency_amount(&usd, 51_637).expect("whole");
        assert_eq!(whole.amount_minor, 13_700);
        let basis = whole.basis.expect("a basis");
        assert!(
            basis.contains("51637 of the 51637 sats received for 137.00 USD"),
            "{basis}"
        );
        // 25,818.5 sats is half: 25,819 sats rounds to 68.50 USD.
        assert_eq!(
            order_currency_amount(&usd, 25_819)
                .expect("half")
                .amount_minor,
            6_850
        );
        assert_eq!(
            order_currency_amount(&usd, 1)
                .expect("one sat")
                .amount_minor,
            0
        );
        // Never above the total.
        assert_eq!(
            order_currency_amount(&usd, 60_000)
                .expect("over")
                .amount_minor,
            13_700
        );
    }

    #[test]
    fn a_sats_refund_on_a_sats_price_stands_up_to_the_total() {
        // 50,000 sats with a 1,637-sat payment code.
        for (currency, exponent) in [("SAT", 0), ("BTC", 8)] {
            let sats = bitcoin_order(currency, exponent, 50_000, 51_637);
            assert_eq!(
                order_currency_amount(&sats, 25_000).expect("part"),
                OrderCurrencyAmount {
                    amount_minor: 25_000,
                    basis: None
                }
            );
            let with_code = order_currency_amount(&sats, 51_637).expect("with code");
            assert_eq!(with_code.amount_minor, 50_000);
            let basis = with_code.basis.expect("a basis");
            assert!(basis.contains("payment code"), "{basis}");
        }
    }

    #[test]
    fn the_amount_to_mirror_is_the_difference_to_what_the_store_shows() {
        assert_eq!(
            amount_to_mirror(5_000, "EUR", &store(13_700, 0)).expect("delta"),
            Some(5_000)
        );
        assert_eq!(
            amount_to_mirror(5_000, "EUR", &store(13_700, 2_000)).expect("delta"),
            Some(3_000)
        );
        assert_eq!(
            amount_to_mirror(5_000, "eur", &store(13_700, 5_000)).expect("done"),
            None
        );
        assert_eq!(
            amount_to_mirror(5_000, "EUR", &store(13_700, 9_000)).expect("more already"),
            None
        );
    }

    #[test]
    fn the_amount_to_mirror_never_goes_above_the_payment() {
        let error = amount_to_mirror(13_701, "EUR", &store(13_700, 0)).expect_err("above");
        assert!(matches!(
            error,
            ProviderError::Rejected { ref code, .. } if code == REFUND_EXCEEDS_PAYMENT
        ));
        assert_eq!(
            amount_to_mirror(13_700, "EUR", &store(13_700, 0)).expect("whole"),
            Some(13_700)
        );
    }

    #[test]
    fn a_store_in_another_currency_or_a_missing_amount_is_refused() {
        let other = StoreRefundTotals {
            currency: "CHF".into(),
            paid_minor: 13_700,
            refunded_minor: 0,
        };
        assert!(matches!(
            amount_to_mirror(1_000, "EUR", &other),
            Err(ProviderError::Rejected { ref code, .. }) if code == CURRENCY_MISMATCH
        ));
        assert!(matches!(
            amount_to_mirror(0, "EUR", &store(13_700, 0)),
            Err(ProviderError::Rejected { ref code, .. }) if code == REFUND_AMOUNT_MISSING
        ));
    }

    #[test]
    fn decimals_print_exactly() {
        assert_eq!(decimal(13_700, 2), "137.00");
        assert_eq!(decimal(5, 2), "0.05");
        assert_eq!(decimal(148_540_000, 6), "148.540000");
        assert_eq!(decimal(7, 0), "7");
    }
}
