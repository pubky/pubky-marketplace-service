//! One refund model for every provider.
//!
//! A refund Pubky records (`refund.record_external`) carries an amount in the
//! order currency's minor units, capped at the order total, on every rail:
//! Bitcoin and USDT included. The framework turns it into what a provider is
//! asked to do, in two steps no provider repeats:
//!
//! 1. **Unit.** The queued mirror carries the recorded total in the order
//!    currency. The one exception is a USDT-paid order priced in anything but
//!    USD, which records the refund in USDT0 units (the paid asset's own, see
//!    `refund_destination::refund_cap_minor`). [`order_currency_amount`]
//!    converts that to the order currency with the order's own recorded
//!    conversion, in exact integer arithmetic, rounded half up to the minor
//!    unit, and describes the basis for the store refund's note. An order
//!    with no recorded conversion cannot be converted: the mirror is queued
//!    as blocked and surfaces as a quarantined action for the seller to
//!    record by hand, as a provider would have refused it.
//! 2. **Difference.** When the action runs, [`amount_to_mirror`] compares the
//!    recorded total with what the store already shows refunded
//!    ([`StoreRefundTotals`](super::provider::StoreRefundTotals), read from
//!    the provider fresh) and yields only the amount still to mirror, never
//!    taking the store's refunds above its payment.
//!
//! The conversion reads the order through `to_jsonb(orders)` rather than a
//! column list, so a database that does not have the conversion columns yet
//! (they arrive with migration 0061) has no conversion to read: the same code
//! compiles and runs, and an order with a recorded unit it cannot convert is
//! blocked as above. Nothing here names a column that only 0061 creates other
//! than as an optional JSON key.

use serde_json::Value;

use super::provider::{ProviderError, StoreRefundTotals};

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
    /// USDT0 base units, `10^-exponent` of a USDT each.
    Usdt0 { exponent: i32 },
}

/// The order facts a refund is recorded and converted against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBasis {
    pub currency: String,
    pub exponent: i32,
    pub total_minor: i64,
    pub payment_method: Option<String>,
    pub payment_asset: Option<String>,
    pub payment_exponent: Option<i32>,
    /// The order's recorded conversion to USD cents (migration 0061), when
    /// the column set exists and the order carries one.
    pub conversion: Option<UsdConversion>,
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
    /// Reads the order from its `to_jsonb(orders)` form. `None` when the row
    /// lacks a field every order has.
    pub fn from_row(row: &Value) -> Option<Self> {
        let text = |key: &str| row.get(key).and_then(Value::as_str).map(str::to_string);
        let number = |key: &str| row.get(key).and_then(Value::as_i64);
        let conversion = match (text("usd_conversion_pair"), number("usd_conversion_cents")) {
            (Some(pair), Some(cents)) if cents > 0 => Some(UsdConversion { pair, cents }),
            _ => None,
        };
        Some(Self {
            currency: text("currency")?,
            exponent: i32::try_from(number("exponent")?).ok()?,
            total_minor: number("total_minor")?,
            payment_method: text("payment_method"),
            payment_asset: text("payment_asset"),
            payment_exponent: number("payment_exponent")
                .and_then(|value| i32::try_from(value).ok()),
            conversion,
        })
    }

    /// The unit `refund.record_external` records this order's refunds in:
    /// USDT0 units for a USDT-paid order priced in anything but USD, the
    /// order currency otherwise (the rule `refund_cap_minor` applies).
    pub fn recorded_unit(&self) -> RecordedUnit {
        let usdt_paid = self.payment_method.as_deref() == Some("usdt")
            || self.payment_asset.as_deref() == Some("USDT");
        match self.payment_exponent {
            Some(exponent) if usdt_paid && self.currency != "USD" => {
                RecordedUnit::Usdt0 { exponent }
            }
            _ => RecordedUnit::OrderCurrency,
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
        RecordedUnit::OrderCurrency => Ok(OrderCurrencyAmount {
            amount_minor: recorded_minor,
            basis: None,
        }),
        RecordedUnit::Usdt0 { exponent } => {
            let Some(conversion) = &order.conversion else {
                return Err(Blocked {
                    code: REFUND_AMOUNT_UNCONVERTIBLE,
                    detail: format!(
                        "The refund is recorded in USDT0 units and the order, priced in {}, has \
                         no recorded conversion to convert it with; record it in the store by hand.",
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
    }
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
    use serde_json::json;

    fn eur_usdt_order() -> OrderBasis {
        OrderBasis::from_row(&json!({
            "currency": "EUR", "exponent": 2, "total_minor": 13_700,
            "payment_method": "paykit", "payment_asset": "USDT", "payment_exponent": 6,
            "usd_conversion_pair": "EURUSD", "usd_conversion_cents": 14_854,
        }))
        .expect("order")
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
        let order = OrderBasis::from_row(&json!({
            "currency": "USD", "exponent": 2, "total_minor": 5_000,
            "payment_method": "usdt", "payment_asset": "USDT", "payment_exponent": 6,
        }))
        .expect("order");
        assert_eq!(order.recorded_unit(), RecordedUnit::OrderCurrency);
        assert_eq!(
            order_currency_amount(&order, 1_234).expect("amount"),
            OrderCurrencyAmount {
                amount_minor: 1_234,
                basis: None
            }
        );
    }

    #[test]
    fn a_usdt_paid_non_usd_order_records_in_usdt0_units() {
        assert_eq!(
            eur_usdt_order().recorded_unit(),
            RecordedUnit::Usdt0 { exponent: 6 }
        );
        let bitcoin = OrderBasis::from_row(&json!({
            "currency": "EUR", "exponent": 2, "total_minor": 13_700,
            "payment_method": "bitcoin",
        }))
        .expect("order");
        assert_eq!(bitcoin.recorded_unit(), RecordedUnit::OrderCurrency);
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
        let order = eur_usdt_order();
        // 74.270000 USDT0 is exactly half: 68.50 EUR.
        assert_eq!(
            order_currency_amount(&order, 74_270_000)
                .expect("half")
                .amount_minor,
            6_850
        );
        // 1.000000 USDT0 is 92.23 order units: 92.
        let one = order_currency_amount(&order, 1_000_000).expect("one");
        assert_eq!(one.amount_minor, 92);
        // 1.00 EUR = 2.00 USD, so one order unit is 0.020000 USDT0: half of
        // it, 0.010000 USDT0, is exactly 0.5 units and rounds up to 1, and a
        // millionth less rounds down to 0.
        let tie = OrderBasis::from_row(&json!({
            "currency": "EUR", "exponent": 2, "total_minor": 100,
            "payment_method": "usdt", "payment_exponent": 6,
            "usd_conversion_pair": "EURUSD", "usd_conversion_cents": 200,
        }))
        .expect("order");
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
    fn an_order_with_no_recorded_conversion_is_blocked() {
        let order = OrderBasis::from_row(&json!({
            "currency": "EUR", "exponent": 2, "total_minor": 13_700,
            "payment_method": "usdt", "payment_exponent": 6,
        }))
        .expect("order");
        let blocked = order_currency_amount(&order, 1_000_000).expect_err("blocked");
        assert_eq!(blocked.code, REFUND_AMOUNT_UNCONVERTIBLE);
        assert!(blocked.detail.contains("by hand"));
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
