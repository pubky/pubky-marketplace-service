//! The listing's return policy, frozen on the order.
//!
//! A seller publishes `returnPolicy` on each listing record
//! (`acceptsReturns`, `returnWindowDays`). The service reads it from the
//! seller-owned listing record on the homeserver when the order's payment
//! starts, together with the seller's delivery terms
//! ([`crate::seller_terms::capture_for_order`]), and freezes it on the order,
//! so a later edit of the listing never changes what the buyer bought under.
//! `return.request` then refuses a return on a final-sale order and after the
//! return window.
//!
//! An order whose lines carry different policies keeps the most generous one:
//! it accepts returns when any line does, within the longest window. When any
//! line's record cannot be read or carries no usable policy, nothing is frozen;
//! an order with nothing frozen (that, or one paid before 0081) accepts a
//! return request as before.

use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};

/// Longest return window the Shop lets a seller set, in days.
pub const MAX_RETURN_WINDOW_DAYS: i64 = 365;

/// One listing's return terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReturnPolicy {
    pub accepts_returns: bool,
    /// Days after delivery a return may be requested; `None` when returns
    /// are not accepted.
    pub return_window_days: Option<i32>,
}

impl ReturnPolicy {
    /// Reads `returnPolicy` from a listing record. A policy that accepts
    /// returns without a window in 1 to 365 days, or that is missing or
    /// malformed, reads as unknown.
    pub fn from_listing_record(record: &Value) -> Option<Self> {
        let policy = record.get("returnPolicy")?;
        match policy.get("acceptsReturns")?.as_bool()? {
            false => Some(Self {
                accepts_returns: false,
                return_window_days: None,
            }),
            true => {
                let days = policy.get("returnWindowDays")?.as_i64()?;
                if !(1..=MAX_RETURN_WINDOW_DAYS).contains(&days) {
                    return None;
                }
                Some(Self {
                    accepts_returns: true,
                    return_window_days: i32::try_from(days).ok(),
                })
            }
        }
    }

    /// The most generous of two lines' policies.
    pub fn merge(self, other: Self) -> Self {
        match (self.accepts_returns, other.accepts_returns) {
            (false, false) => self,
            (true, false) => self,
            (false, true) => other,
            (true, true) => Self {
                accepts_returns: true,
                return_window_days: self.return_window_days.max(other.return_window_days),
            },
        }
    }

    pub fn to_json(self) -> Value {
        json!({
            "accepts_returns": self.accepts_returns,
            "return_window_days": self.return_window_days,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let accepts_returns = value.get("accepts_returns")?.as_bool()?;
        let return_window_days = value
            .get("return_window_days")
            .and_then(Value::as_i64)
            .and_then(|days| i32::try_from(days).ok());
        Some(Self {
            accepts_returns,
            return_window_days,
        })
    }
}

/// Reads every line's listing record and freezes the order's policy in
/// `orders.return_policy`. Called from the payment-start capture, inside the
/// transaction that holds the order row, and only once per order.
pub async fn capture_for_order(
    tx: &mut Transaction<'_, Postgres>,
    homeserver: &dyn HomeserverListingClient,
    order_id: Uuid,
    seller_pubky: &str,
) -> Result<(), sqlx::Error> {
    let listing_ids: Vec<(String,)> = sqlx::query_as(
        "SELECT l.listing_id FROM orders o \
         CROSS JOIN LATERAL jsonb_array_elements(o.lines) AS line \
         JOIN listings l ON l.aggregate_id = line->>'listing_aggregate_id' \
         WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut frozen: Option<ReturnPolicy> = None;
    for (listing_id,) in &listing_ids {
        let policy = match homeserver.fetch_listing(seller_pubky, listing_id).await {
            HomeserverFetchOutcome::Found(record) => ReturnPolicy::from_listing_record(&record),
            _ => None,
        };
        let Some(policy) = policy else {
            tracing::warn!(
                order_id = %order_id,
                listing_id = %listing_id,
                "listing return policy unreadable at payment start; order keeps no return policy"
            );
            return Ok(());
        };
        frozen = Some(frozen.map_or(policy, |current| current.merge(policy)));
    }
    let Some(policy) = frozen else {
        return Ok(());
    };
    sqlx::query("UPDATE orders SET return_policy = $2 WHERE id = $1")
        .bind(order_id)
        .bind(policy.to_json())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Why a frozen policy refuses a return request now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnRefusal {
    FinalSale,
    WindowClosed,
}

/// Checks a return request against the order's frozen policy. `delivered_at`
/// is when the order reached the buyer; when it is unknown the window is not
/// enforced.
pub fn check_request(
    policy: Option<ReturnPolicy>,
    delivered_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<(), ReturnRefusal> {
    let Some(policy) = policy else {
        return Ok(());
    };
    if !policy.accepts_returns {
        return Err(ReturnRefusal::FinalSale);
    }
    match (policy.return_window_days, delivered_at) {
        (Some(days), Some(delivered_at))
            if now > delivered_at + Duration::days(i64::from(days)) =>
        {
            Err(ReturnRefusal::WindowClosed)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(day: u32) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&format!("2026-10-{day:02}T12:00:00Z"))
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn reads_the_listing_policy() {
        let final_sale = json!({ "returnPolicy": { "acceptsReturns": false, "buyerPaysReturnShipping": false } });
        assert_eq!(
            ReturnPolicy::from_listing_record(&final_sale),
            Some(ReturnPolicy {
                accepts_returns: false,
                return_window_days: None
            })
        );
        let thirty = json!({ "returnPolicy": { "acceptsReturns": true, "returnWindowDays": 30 } });
        assert_eq!(
            ReturnPolicy::from_listing_record(&thirty),
            Some(ReturnPolicy {
                accepts_returns: true,
                return_window_days: Some(30)
            })
        );
        for unusable in [
            json!({}),
            json!({ "returnPolicy": { "acceptsReturns": true } }),
            json!({ "returnPolicy": { "acceptsReturns": true, "returnWindowDays": 0 } }),
            json!({ "returnPolicy": { "acceptsReturns": "no" } }),
        ] {
            assert_eq!(
                ReturnPolicy::from_listing_record(&unusable),
                None,
                "{unusable}"
            );
        }
    }

    #[test]
    fn merge_keeps_the_most_generous_policy() {
        let final_sale = ReturnPolicy {
            accepts_returns: false,
            return_window_days: None,
        };
        let fourteen = ReturnPolicy {
            accepts_returns: true,
            return_window_days: Some(14),
        };
        let thirty = ReturnPolicy {
            accepts_returns: true,
            return_window_days: Some(30),
        };
        assert_eq!(final_sale.merge(fourteen), fourteen);
        assert_eq!(fourteen.merge(final_sale), fourteen);
        assert_eq!(fourteen.merge(thirty), thirty);
        assert_eq!(final_sale.merge(final_sale), final_sale);
    }

    #[test]
    fn check_refuses_final_sale_and_a_closed_window() {
        let final_sale = ReturnPolicy {
            accepts_returns: false,
            return_window_days: None,
        };
        let fourteen = ReturnPolicy {
            accepts_returns: true,
            return_window_days: Some(14),
        };
        assert_eq!(check_request(None, Some(at(1)), at(30)), Ok(()));
        assert_eq!(
            check_request(Some(final_sale), Some(at(1)), at(2)),
            Err(ReturnRefusal::FinalSale)
        );
        assert_eq!(check_request(Some(fourteen), Some(at(1)), at(15)), Ok(()));
        assert_eq!(
            check_request(Some(fourteen), Some(at(1)), at(16)),
            Err(ReturnRefusal::WindowClosed)
        );
        assert_eq!(check_request(Some(fourteen), None, at(30)), Ok(()));
    }
}
