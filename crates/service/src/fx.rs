//! Production FX reference handling for fiat-at-bind quotes.
//!
//! One Blocktank snapshot (`FX_URL`) carries a `BTC<CCY>` ticker for each of
//! the seven fiat currencies the marketplace prices in (USD, EUR, CHF, GBP,
//! JPY, CAD, AUD). Every ticker passes the same guards (exactly one ticker
//! per symbol, staleness, future skew, per-currency rate bounds) before a
//! rate is used, and a quote additionally passes the reference-deviation
//! guard against that currency's own persisted median. The source contract
//! is the live response captured in
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
//!   ([`quote_sats`]), refused below [`MIN_QUOTE_SATS`] and above
//!   [`MAX_QUOTE_SATS`];
//! - fiat to USD cents (the multi-asset Paykit denomination):
//!   `ceil(total_minor * 100 * BTCUSD / (10^exponent * BTC<CCY>))`
//!   ([`usd_cents_for`]); the cross rate is the ratio of two tickers of the
//!   same snapshot.

use std::cmp::Ordering;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use marketplace_domain::currency::Currency;
use serde::Deserialize;

pub const FX_SOURCE: &str = "blocktank";
pub const FX_URL: &str = "https://api1.blocktank.to/api/fx/rates/btc";
pub const MAX_DEVIATION_BPS: u128 = 500;
pub const MAX_QUOTE_SATS: u128 = 100_000_000;
pub const MIN_QUOTE_SATS: u128 = 1_000;
pub const MAX_RATE_AGE_SECS: i64 = 300;
pub const MAX_FUTURE_SKEW_SECS: i64 = 60;
pub const REFERENCE_WINDOW_SECS: i64 = 3_600;
pub const REFERENCE_MIN_SAMPLES: usize = 3;

/// The inclusive sanity bounds of one `BTC<CCY>` rate (units of the
/// currency per one bitcoin), `None` for the Bitcoin spellings. They are
/// wide enough to survive any plausible market and tight enough to reject a
/// unit mix-up (a cents or yen feed error). `0061_multi_currency_fx.sql`
/// repeats them as `fx_rate_samples_rate_check`.
pub const fn rate_bounds(currency: Currency) -> Option<(u128, u128)> {
    Some(match currency {
        Currency::Usd => (10_000, 1_000_000),
        Currency::Eur | Currency::Chf => (7_000, 1_000_000),
        Currency::Gbp => (6_000, 900_000),
        Currency::Jpy => (1_000_000, 200_000_000),
        Currency::Cad => (10_000, 1_500_000),
        Currency::Aud => (10_000, 1_700_000),
        Currency::Btc | Currency::Sat => return None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    pub mantissa: u128,
    pub scale: u32,
}

impl Decimal {
    pub fn parse(value: &str) -> Option<Self> {
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        // The fraction is capped at 8 digits (one satoshi of BTC precision)
        // so the checked scaling in the comparison paths can never be asked
        // to widen a mantissa by more than 10^8.
        if fraction.len() > 8 || whole.len() > 38 {
            return None;
        }
        if whole.is_empty()
            || !whole.chars().all(|c| c.is_ascii_digit())
            || !fraction.chars().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let mantissa = digits.parse().ok()?;
        Some(Self {
            mantissa,
            scale: fraction.len().try_into().ok()?,
        })
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

    fn in_bounds(&self, currency: Currency) -> bool {
        let Some((low, high)) = rate_bounds(currency) else {
            return false;
        };
        let min = Self {
            mantissa: low,
            scale: 0,
        };
        let max = Self {
            mantissa: high,
            scale: 0,
        };
        // Fail closed: an unwidenable value is treated as out of bounds.
        match (self.cmp(&min), self.cmp(&max)) {
            (Some(low), Some(high)) => low != Ordering::Less && high != Ordering::Greater,
            _ => false,
        }
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
    CapExceeded,
}

/// Every ticker of one feed response, each fiat currency validated on its
/// own: a stale or missing JPY ticker never blocks a USD quote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FxSnapshot {
    samples: Vec<(Currency, Result<RateSample, FxError>)>,
}

impl FxSnapshot {
    /// The guarded rate sample of `currency`: `Malformed` when the feed
    /// carried no (or more than one) ticker for it, the ticker's own
    /// refusal when it failed a guard.
    pub fn sample(&self, currency: Currency) -> Result<RateSample, FxError> {
        self.samples
            .iter()
            .find(|(candidate, _)| *candidate == currency)
            .map_or(Err(FxError::Malformed), |(_, sample)| sample.clone())
    }

    /// Every fiat currency with its validation outcome.
    pub fn samples(&self) -> impl Iterator<Item = (Currency, &Result<RateSample, FxError>)> {
        self.samples
            .iter()
            .map(|(currency, sample)| (*currency, sample))
    }
}

fn parse_ticker(
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
    let age = observed_at.signed_duration_since(updated_at);
    if age > Duration::seconds(MAX_RATE_AGE_SECS) {
        return Err(FxError::Stale);
    }
    if age < -Duration::seconds(MAX_FUTURE_SKEW_SECS) {
        return Err(FxError::Future);
    }
    let rate = Decimal::parse(&ticker.last_price).ok_or(FxError::Malformed)?;
    if !rate.in_bounds(currency) {
        return Err(FxError::OutOfBounds);
    }
    Ok(RateSample {
        rate,
        fetched_at: updated_at,
    })
}

/// Parses one feed response into the per-currency snapshot. Only a body
/// that is not the pinned JSON shape fails as a whole.
pub fn parse_snapshot(body: &[u8], observed_at: DateTime<Utc>) -> Result<FxSnapshot, FxError> {
    let feed: Feed = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    Ok(FxSnapshot {
        samples: Currency::FIAT
            .into_iter()
            .map(|currency| (currency, parse_ticker(&feed, currency, observed_at)))
            .collect(),
    })
}

/// The BTCUSD sample of a feed body (the USD-only contract: the other
/// tickers are not consulted).
pub fn parse_feed(body: &[u8], observed_at: DateTime<Utc>) -> Result<RateSample, FxError> {
    parse_feed_for(body, observed_at, Currency::Usd)
}

/// The `BTC<CCY>` sample of a feed body for one fiat currency.
pub fn parse_feed_for(
    body: &[u8],
    observed_at: DateTime<Utc>,
    currency: Currency,
) -> Result<RateSample, FxError> {
    let feed: Feed = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    parse_ticker(&feed, currency, observed_at)
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
    let (Some(scaled_difference), Some(scaled_bound)) = (
        difference.checked_mul(10_000),
        reference_value.checked_mul(MAX_DEVIATION_BPS),
    ) else {
        return false;
    };
    scaled_difference <= scaled_bound
}

/// Converts fiat minor units into satoshis with checked integer arithmetic:
/// ceil(total_minor * 100_000_000 / (10^exponent * BTCUSD)).
pub fn quote_sats(total_minor: i64, exponent: i32, rate: &Decimal) -> Result<u64, FxError> {
    let total = u128::try_from(total_minor).map_err(|_| FxError::Overflow)?;
    let exponent = u32::try_from(exponent).map_err(|_| FxError::Overflow)?;
    let fiat_scale = 10u128.checked_pow(exponent).ok_or(FxError::Overflow)?;
    let rate_scale = 10u128.checked_pow(rate.scale).ok_or(FxError::Overflow)?;
    let numerator = total
        .checked_mul(100_000_000)
        .and_then(|value| value.checked_mul(rate_scale))
        .ok_or(FxError::Overflow)?;
    let denominator = fiat_scale
        .checked_mul(rate.mantissa)
        .ok_or(FxError::Overflow)?;
    if denominator == 0 {
        return Err(FxError::Overflow);
    }
    let sats = numerator
        .checked_add(denominator - 1)
        .ok_or(FxError::Overflow)?
        / denominator;
    if sats == 0 {
        return Err(FxError::BelowMinimum);
    }
    if sats < MIN_QUOTE_SATS {
        return Err(FxError::BelowMinimum);
    }
    if sats > MAX_QUOTE_SATS {
        return Err(FxError::CapExceeded);
    }
    u64::try_from(sats).map_err(|_| FxError::Overflow)
}

/// Guards the Bitcoin amount paykit-server fixed for a USD price against the
/// service's own quote: within [`MAX_DEVIATION_BPS`] of it, and inside the
/// same minimum and maximum a service quote must respect.
pub fn guard_fixed_sats(fixed_sats: u64, quoted_sats: u64) -> Result<(), FxError> {
    let fixed = u128::from(fixed_sats);
    if fixed < MIN_QUOTE_SATS {
        return Err(FxError::BelowMinimum);
    }
    if fixed > MAX_QUOTE_SATS {
        return Err(FxError::CapExceeded);
    }
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
/// service's own rate: the USDT0 millionths the price is worth at `rate` (US
/// dollars per bitcoin) must sit within [`MAX_DEVIATION_BPS`] of what
/// paykit-server offered.
pub fn guard_fixed_usdt_units(fixed_units: u64, sats: u64, rate: &Decimal) -> Result<(), FxError> {
    // One satoshi is rate / 10^8 US dollars, which is rate / 100 USDT0
    // millionths.
    let scale = 10_u128
        .checked_pow(rate.scale)
        .and_then(|scale| scale.checked_mul(100))
        .ok_or(FxError::Overflow)?;
    let expected = u128::from(sats)
        .checked_mul(rate.mantissa)
        .ok_or(FxError::Overflow)?
        .div_ceil(scale);
    let fixed = u128::from(fixed_units);
    if fixed == 0 || expected == 0 {
        return Err(FxError::BelowMinimum);
    }
    let within = fixed
        .abs_diff(expected)
        .checked_mul(10_000)
        .zip(expected.checked_mul(MAX_DEVIATION_BPS))
        .is_some_and(|(gap, allowed)| gap <= allowed);
    if within {
        Ok(())
    } else {
        Err(FxError::DeviationExceeded)
    }
}

pub fn reference_cutoff(now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::seconds(REFERENCE_WINDOW_SECS)
}

/// Converts fiat minor units into USD cents with checked integer
/// arithmetic, rounding up:
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
    let currency_scale = 10u128
        .checked_pow(currency_rate.scale)
        .ok_or(FxError::Overflow)?;
    let usd_scale = 10u128
        .checked_pow(usd_rate.scale)
        .ok_or(FxError::Overflow)?;
    let fiat_scale = 10u128.checked_pow(exponent).ok_or(FxError::Overflow)?;
    let numerator = total
        .checked_mul(100)
        .and_then(|value| value.checked_mul(usd_rate.mantissa))
        .and_then(|value| value.checked_mul(currency_scale))
        .ok_or(FxError::Overflow)?;
    let denominator = fiat_scale
        .checked_mul(currency_rate.mantissa)
        .and_then(|value| value.checked_mul(usd_scale))
        .ok_or(FxError::Overflow)?;
    if denominator == 0 {
        return Err(FxError::Overflow);
    }
    let cents = numerator
        .checked_add(denominator - 1)
        .ok_or(FxError::Overflow)?
        / denominator;
    u64::try_from(cents).map_err(|_| FxError::Overflow)
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
    median(
        rates
            .into_iter()
            .filter_map(|(rate,)| Decimal::parse(&rate))
            .collect(),
    )
    .ok_or(FxError::MissingReference)
}

/// The reference BTCUSD rate: the current feed rate, accepted only while it
/// sits within [`MAX_DEVIATION_BPS`] of the median of the persisted samples.
pub async fn reference_rate<'e, E>(
    executor: E,
    feed_url: &str,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample), FxError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let median_rate = reference_median(executor, Currency::Usd, now).await?;
    let current = fetch_current(feed_url, now).await?;
    if !within_deviation(&current.rate, &median_rate) {
        return Err(FxError::DeviationExceeded);
    }
    Ok((current.rate.clone(), current))
}

/// Quotes a fiat price in satoshis with the currency's own `BTC<CCY>`
/// ticker: the reference first (a currency with fewer than
/// [`REFERENCE_MIN_SAMPLES`] samples is refused without a feed fetch), then
/// one snapshot fetch, the deviation guard and [`quote_sats`].
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
    quote_denominated(
        executor,
        feed_url,
        currency,
        total_minor,
        currency.exponent(),
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
    let median_rate = reference_median(executor, currency, now).await?;
    let current = fetch_snapshot(feed_url, now).await?.sample(currency)?;
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
        let numerator = self
            .usd_rate
            .mantissa
            .checked_mul(10u128.checked_pow(PLACES.checked_add(self.currency_rate.scale)?)?)?;
        let denominator = self
            .currency_rate
            .mantissa
            .checked_mul(10u128.checked_pow(self.usd_rate.scale)?)?;
        if denominator == 0 {
            return None;
        }
        let scaled = numerator.checked_add(denominator / 2)? / denominator;
        Some(
            Decimal {
                mantissa: scaled,
                scale: PLACES,
            }
            .to_string_value(),
        )
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
    /// amount Paykit fixes. `BelowMinimum`/`CapExceeded` do not stop the
    /// USD settlement (a USDT-only request needs no Bitcoin amount).
    pub sats: Result<u64, FxError>,
    pub usd: UsdSettlement,
}

/// Quotes one fiat order total from a single snapshot for the multi-asset
/// contract: the guarded direct `BTC<CCY>` quote and the USD-cents
/// settlement. A non-USD currency also needs a guarded `BTCUSD` from the
/// same snapshot; each leg is checked against its own persisted reference
/// before either amount is computed.
pub async fn quote_order(
    conn: &mut sqlx::PgConnection,
    feed_url: &str,
    currency: Currency,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<OrderQuote, FxError> {
    if !currency.is_fiat() {
        return Err(FxError::Malformed);
    }
    let direct_reference = reference_median(&mut *conn, currency, now).await?;
    let usd_reference = if currency == Currency::Usd {
        None
    } else {
        Some(reference_median(&mut *conn, Currency::Usd, now).await?)
    };
    let snapshot = fetch_snapshot(feed_url, now).await?;
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

async fn fetch_body(feed_url: &str) -> Result<Vec<u8>, FxError> {
    let response = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(3))
        .build()
        .map_err(|_| FxError::Malformed)?
        .get(feed_url)
        .send()
        .await
        .map_err(|_| FxError::Malformed)?
        .error_for_status()
        .map_err(|_| FxError::Malformed)?;
    Ok(response
        .bytes()
        .await
        .map_err(|_| FxError::Malformed)?
        .to_vec())
}

/// One fetch of the feed, every fiat ticker validated.
pub async fn fetch_snapshot(feed_url: &str, now: DateTime<Utc>) -> Result<FxSnapshot, FxError> {
    parse_snapshot(&fetch_body(feed_url).await?, now)
}

/// One fetch of the feed, the USD ticker only.
pub async fn fetch_current(feed_url: &str, now: DateTime<Utc>) -> Result<RateSample, FxError> {
    parse_feed(&fetch_body(feed_url).await?, now)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn parse_caps_the_fraction_at_eight_digits() {
        assert_eq!(
            Decimal::parse("77197.12345678")
                .expect("eight fraction digits parse")
                .to_string_value(),
            "77197.12345678"
        );
        // Nine digits is already too wide; a 40-digit fraction is rejected
        // outright so the checked widening paths can never be stretched.
        assert_eq!(Decimal::parse("77197.123456789"), None);
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
            !huge.in_bounds(Currency::Usd),
            "far outside the permitted range"
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
    fn quote_uses_checked_integer_ceiling_and_minimum() {
        let rate = Decimal::parse("10000").unwrap();
        assert_eq!(quote_sats(1_000_000, 2, &rate), Ok(100_000_000));
        assert_eq!(
            quote_sats(1, 2, &rate),
            Err(FxError::BelowMinimum),
            "small fiat principals are rejected without uplift"
        );
        assert_eq!(
            quote_sats(10_000_000_000, 2, &rate),
            Err(FxError::CapExceeded)
        );
    }

    #[test]
    fn quote_usd_two_decimal_matrix_uses_exact_rational_ceiling() {
        let rate = Decimal::parse("100000").unwrap();
        assert_eq!(quote_sats(99, 2, &rate), Err(FxError::BelowMinimum));
        assert_eq!(quote_sats(1_000, 2, &rate), Ok(10_000));
        assert_eq!(quote_sats(1_001, 2, &rate), Ok(10_010));
        assert_eq!(quote_sats(12_500, 2, &rate), Ok(125_000));
        assert_eq!(quote_sats(12_501, 2, &rate), Ok(125_010));
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
    }

    #[test]
    fn quote_rejects_zero_and_three_decimal_currency_exponents_at_bind_boundary() {
        let rate = Decimal::parse("77197").unwrap();
        assert_eq!(quote_sats(1_000, 0, &rate), Ok(1_295_388));
        assert_eq!(quote_sats(100, 3, &rate), Err(FxError::BelowMinimum));
    }

    #[test]
    fn a_fixed_bitcoin_amount_is_guarded_by_the_quote_deviation_and_bounds() {
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
        assert_eq!(guard_fixed_sats(999, 1_000), Err(FxError::BelowMinimum));
        assert_eq!(guard_fixed_sats(1_000, 1_000), Ok(()));
        assert_eq!(
            guard_fixed_sats(MAX_QUOTE_SATS as u64 + 1, MAX_QUOTE_SATS as u64),
            Err(FxError::CapExceeded)
        );
        assert_eq!(
            guard_fixed_sats(MAX_QUOTE_SATS as u64, MAX_QUOTE_SATS as u64),
            Ok(())
        );
        assert_eq!(guard_fixed_sats(5_000, 0), Err(FxError::DeviationExceeded));
        assert_eq!(
            guard_fixed_sats(u64::MAX, u64::MAX),
            Err(FxError::CapExceeded)
        );
    }

    #[test]
    fn a_fixed_usdt_amount_is_guarded_against_the_service_rate() {
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
    fn feed_rejects_missing_stale_future_zero_and_out_of_band_samples() {
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
            parse_feed(body("1000001", current).as_bytes(), now),
            Err(FxError::OutOfBounds)
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
        let snapshot = parse_snapshot(FIXTURE, now).expect("captured response parses");
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
                {{"symbol":"BTCGBP","lastPrice":"1","base":"BTC","quote":"GBP","lastUpdatedAt":{current}}},
                {{"symbol":"BTCCHF","lastPrice":"62992","base":"BTC","quote":"CHF","lastUpdatedAt":{current}}},
                {{"symbol":"BTCCHF","lastPrice":"62993","base":"BTC","quote":"CHF","lastUpdatedAt":{current}}}
            ]}}"#,
            stale = current - 301_000,
            future = current + 61_000,
        );
        let snapshot = parse_snapshot(body.as_bytes(), now).expect("shape is valid");
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
            parse_snapshot(b"not json", now),
            Err(FxError::Malformed),
            "only an unreadable body fails the whole snapshot"
        );
    }

    #[test]
    fn every_currency_has_inclusive_bounds_and_its_own_staleness_and_skew_guards() {
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
            let (low, high) = rate_bounds(currency).expect("fiat has bounds");
            let parse = |price: &str, updated: i64| {
                parse_feed_for(body(currency, price, updated).as_bytes(), now, currency)
                    .map(|sample| sample.rate.to_string_value())
            };
            assert_eq!(parse(&low.to_string(), current), Ok(low.to_string()));
            assert_eq!(parse(&high.to_string(), current), Ok(high.to_string()));
            assert_eq!(
                parse(&(low - 1).to_string(), current),
                Err(FxError::OutOfBounds),
                "{currency} below the band"
            );
            assert_eq!(
                parse(&(high + 1).to_string(), current),
                Err(FxError::OutOfBounds),
                "{currency} above the band"
            );
            assert_eq!(
                parse(&low.to_string(), current - 300_000),
                Ok(low.to_string()),
                "{currency}: exactly the maximum age is fresh"
            );
            assert_eq!(
                parse(&low.to_string(), current - 300_001),
                Err(FxError::Stale),
                "{currency}"
            );
            assert_eq!(
                parse(&low.to_string(), current + 60_000),
                Ok(low.to_string()),
                "{currency}: exactly the maximum skew is accepted"
            );
            assert_eq!(
                parse(&low.to_string(), current + 60_001),
                Err(FxError::Future),
                "{currency}"
            );
            assert_eq!(parse("0", current), Err(FxError::OutOfBounds), "{currency}");
            assert_eq!(parse("-5", current), Err(FxError::Malformed), "{currency}");
        }
        assert_eq!(rate_bounds(Currency::Btc), None);
        assert_eq!(rate_bounds(Currency::Sat), None);
        assert!(!rate("77197").in_bounds(Currency::Btc));
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
            Err(FxError::BelowMinimum),
            "100 JPY is 852 sats, under the minimum"
        );
        assert_eq!(
            quote_sats(1, 0, &rate("11745955")),
            Err(FxError::BelowMinimum)
        );
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
    fn sats_quote_caps_at_one_bitcoin_for_every_currency() {
        assert_eq!(
            quote_sats(100_000_000_000, 0, &rate("11745955")),
            Err(FxError::CapExceeded),
            "100 billion JPY"
        );
        assert_eq!(quote_sats(10_000_000, 2, &rate("100000")), Ok(100_000_000));
        assert_eq!(
            quote_sats(10_000_001, 2, &rate("100000")),
            Err(FxError::CapExceeded)
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
}
