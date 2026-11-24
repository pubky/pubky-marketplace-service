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
