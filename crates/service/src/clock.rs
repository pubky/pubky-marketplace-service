use std::sync::Mutex;

use chrono::{DateTime, SecondsFormat, Utc};

/// Server time authority. `issued_at` on commands is diagnostic only; every
/// deadline (reservation expiry, auth token window, session TTL) uses this
/// clock.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// How far ahead of this instance's clock a stored rate-bucket `updated_at`
/// may be before it is treated as implausible.
///
/// Within this window a later stored timestamp is kept as is: the request
/// earns no refill until its clock passes it, and the timestamp never moves
/// back, so requests that reach a bucket out of clock order (or from
/// instances whose clocks differ by up to this much) credit no refill
/// interval twice. A stored timestamp further ahead (a broken clock or a
/// hand-edited row) is reset to `now` by the next request, admitted or
/// refused, with no refill credited, so such a bucket recovers within one
/// refill interval.
///
/// Instances must keep their clocks synchronized well inside this window. A
/// larger difference between instances, or a clock sample that waits longer
/// than this for a pool connection or the bucket lock, lets one request reset
/// a genuine later timestamp, and the next request then credits that
/// difference as refill (up to the bucket capacity).
pub const MAX_BUCKET_TIMESTAMP_AHEAD: chrono::TimeDelta = chrono::TimeDelta::minutes(5);

/// Deterministic clock for tests, mirroring the injectable `now` of the
/// TypeScript prototype suite.
pub struct AdjustableClock {
    now: Mutex<DateTime<Utc>>,
}

impl AdjustableClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().expect("clock lock poisoned") = now;
    }

    pub fn advance_seconds(&self, seconds: i64) {
        let mut guard = self.now.lock().expect("clock lock poisoned");
        *guard += chrono::Duration::seconds(seconds);
    }
}

impl Clock for AdjustableClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().expect("clock lock poisoned")
    }
}

/// Canonical wire timestamp format: RFC 3339 with milliseconds and `Z`.
pub fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Tokens a bucket holds after `elapsed_ms` whole milliseconds of refill at
/// `per_minute` tokens a minute, capped at `capacity`.
///
/// Admission measures elapsed time in whole milliseconds, so every bucket
/// admission and every `Retry-After` derived from it goes through this one
/// expression.
pub(crate) fn refilled_tokens(stored: f64, elapsed_ms: i64, per_minute: f64, capacity: f64) -> f64 {
    let elapsed = elapsed_ms.max(0) as f64 / 1_000.0;
    (stored + elapsed * per_minute / 60.0).min(capacity)
}

/// The longest refill wait the helpers report, in milliseconds (one year),
/// for a bucket that cannot refill to one token.
const MAX_REFILL_THRESHOLD_MS: i64 = 365 * 24 * 60 * 60 * 1_000;

/// How many steps the exact search may move from its first estimate. The
/// estimate is within one step of exact: it is wrong only by floating point
/// error, far below a millisecond.
const REFILL_SEARCH_STEPS: u32 = 8;

/// The fewest whole milliseconds of refill after which
/// [`refilled_tokens`] reaches one token, found under the same floating
/// point arithmetic as admission.
fn refill_threshold_ms(stored: f64, per_minute: f64, capacity: f64) -> i64 {
    if !stored.is_finite() || !per_minute.is_finite() || per_minute <= 0.0 || capacity < 1.0 {
        return MAX_REFILL_THRESHOLD_MS;
    }
    let admits = |ms: i64| refilled_tokens(stored, ms, per_minute, capacity) >= 1.0;
    let estimate = ((1.0 - stored) * 60_000.0 / per_minute).ceil();
    let mut ms = if estimate > 0.0 {
        (estimate.min(MAX_REFILL_THRESHOLD_MS as f64)) as i64
    } else {
        0
    };
    for _ in 0..REFILL_SEARCH_STEPS {
        if admits(ms) || ms >= MAX_REFILL_THRESHOLD_MS {
            break;
        }
        ms += 1;
    }
    for _ in 0..REFILL_SEARCH_STEPS {
        if ms == 0 || !admits(ms - 1) {
            break;
        }
        ms -= 1;
    }
    ms
}

/// Whole seconds a client must wait before a bucket admits it again: the
/// smallest `Retry-After` (at least 1) for which a request arriving that
/// long after `now` is admitted.
///
/// `stored` is the bucket's token count at `effective`, the instant its
/// refill is measured from (`effective` can be after `now`: refill starts
/// once the clock reaches it). Admission counts refill in whole
/// milliseconds, so the wait runs until `effective` plus the whole
/// milliseconds [`refill_threshold_ms`] needs, not until a continuous refill
/// reaches one token. The serving clock keeps its nanoseconds, so the sum is
/// kept in exact integer nanoseconds and only the final rounding goes up to a
/// whole second.
pub(crate) fn retry_after_seconds(
    stored: f64,
    per_minute: f64,
    capacity: f64,
    effective: DateTime<Utc>,
    now: DateTime<Utc>,
) -> i64 {
    let until_effective_ns = match (effective - now).num_nanoseconds() {
        Some(nanoseconds) => i128::from(nanoseconds),
        None if effective > now => i128::from(i64::MAX),
        None => i128::from(i64::MIN),
    };
    let refill_ns = i128::from(refill_threshold_ms(stored, per_minute, capacity)) * 1_000_000;
    let wait_ns = until_effective_ns + refill_ns;
    let seconds = if wait_ns <= 0 {
        1
    } else {
        (wait_ns + 999_999_999) / 1_000_000_000
    };
    i64::try_from(seconds).unwrap_or(i64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 24, 12, 0, 0).unwrap()
    }

    /// Admission for a request `after` the sampled `now`, as the buckets
    /// measure it: whole milliseconds since `effective`.
    fn admits_after(
        stored: f64,
        per_minute: f64,
        capacity: f64,
        effective: DateTime<Utc>,
        now: DateTime<Utc>,
        after_seconds: i64,
    ) -> bool {
        let at = now + Duration::seconds(after_seconds);
        let elapsed_ms = (at - effective).num_milliseconds();
        refilled_tokens(stored, elapsed_ms, per_minute, capacity) >= 1.0
    }

    #[test]
    fn seven_per_minute_with_a_half_millisecond_pending_wait_waits_ten_seconds() {
        let now = base();
        let effective = now + Duration::microseconds(428_500);
        // One token needs 8,572 whole milliseconds at 7 a minute. Nine
        // seconds leaves 8,571 after the 428.5 ms pending wait.
        assert_eq!(refill_threshold_ms(0.0, 7.0, 14.0), 8_572);
        assert_eq!(retry_after_seconds(0.0, 7.0, 14.0, effective, now), 10);
        assert!(!admits_after(0.0, 7.0, 14.0, effective, now, 9));
        assert!(admits_after(0.0, 7.0, 14.0, effective, now, 10));
    }

    #[test]
    fn refill_threshold_is_the_exact_admission_millisecond() {
        for per_minute in [1.0, 3.0, 7.0, 11.0, 60.0, 100.0, 120.0, 1_000.0] {
            for stored in [0.0, 0.25, 0.5, 0.999_999, 1e-9, 0.333_333_333_333_333_3] {
                let capacity = per_minute * 2.0;
                let threshold = refill_threshold_ms(stored, per_minute, capacity);
                assert!(
                    refilled_tokens(stored, threshold, per_minute, capacity) >= 1.0,
                    "{per_minute}/min from {stored}: {threshold} ms must admit"
                );
                assert!(
                    threshold == 0
                        || refilled_tokens(stored, threshold - 1, per_minute, capacity) < 1.0,
                    "{per_minute}/min from {stored}: {} ms must not admit",
                    threshold - 1
                );
            }
        }
    }

    #[test]
    fn retry_after_is_the_smallest_whole_second_that_admits() {
        let now = base();
        let offsets_ns: [i64; 12] = [
            -90_000_000_000,
            -1_500_000_000,
            -1,
            0,
            1,
            999_999,
            1_000_000,
            428_500_000,
            999_999_999,
            59_999_999_999,
            60_000_000_001,
            299_999_999_999,
        ];
        for per_minute in [1.0, 3.0, 7.0, 11.0, 60.0, 100.0, 1_000.0] {
            for stored in [0.0, 0.25, 0.5, 0.999_999, 1e-9] {
                for offset in offsets_ns {
                    let capacity = per_minute * 2.0;
                    let effective = now + Duration::nanoseconds(offset);
                    // The bucket is refused only while it holds less than one
                    // token after the whole milliseconds already elapsed.
                    let held = refilled_tokens(
                        stored,
                        (now - effective).num_milliseconds(),
                        per_minute,
                        capacity,
                    );
                    if held >= 1.0 {
                        continue;
                    }
                    let seconds = retry_after_seconds(stored, per_minute, capacity, effective, now);
                    assert!(seconds >= 1);
                    assert!(
                        admits_after(stored, per_minute, capacity, effective, now, seconds),
                        "{per_minute}/min from {stored}, effective {offset} ns: {seconds} s must admit"
                    );
                    assert!(
                        seconds == 1
                            || !admits_after(
                                stored,
                                per_minute,
                                capacity,
                                effective,
                                now,
                                seconds - 1
                            ),
                        "{per_minute}/min from {stored}, effective {offset} ns: {} s must not admit",
                        seconds - 1
                    );
                }
            }
        }
    }

    #[test]
    fn nanosecond_remainders_round_the_wait_up() {
        let now = base();
        // 60 per minute: one token a second.
        let at = |nanoseconds: i64| now + Duration::nanoseconds(nanoseconds);
        assert_eq!(
            retry_after_seconds(0.0, 60.0, 120.0, at(59_999_999_999), now),
            61
        );
        assert_eq!(
            retry_after_seconds(0.0, 60.0, 120.0, at(60_000_000_000), now),
            61
        );
        assert_eq!(
            retry_after_seconds(0.0, 60.0, 120.0, at(60_000_000_001), now),
            62
        );
    }

    #[test]
    fn a_bucket_that_cannot_refill_reports_a_long_but_finite_wait() {
        let now = base();
        let year = 365 * 24 * 60 * 60;
        assert_eq!(retry_after_seconds(0.0, 60.0, 0.5, now, now), year);
        assert_eq!(retry_after_seconds(0.0, 0.0, 10.0, now, now), year);
        assert_eq!(retry_after_seconds(f64::NAN, 60.0, 10.0, now, now), year);
        let far = retry_after_seconds(0.0, 60.0, 10.0, DateTime::<Utc>::MAX_UTC, now);
        assert!(
            far > 9_000_000_000,
            "an overflowing offset saturates: {far}"
        );
    }
}
