//! Seller-set post-purchase periods.
//!
//! A seller publishes two optional integer-day fields on their shop record
//! (`deliveryAssumeDays`, `autoCompleteDays`, 1 to 60 days): how long after
//! shipment the service assumes delivery, and how long after delivery the
//! order completes unless a return is opened. The service reads them from
//! the seller-owned shop record on the homeserver (only the seller can write
//! that path) and freezes them on the order when its payment starts, so a
//! later change to the record never moves an open order's clock. A period
//! the seller left unset stays NULL on the order and the sweeps apply the
//! configured `DELIVERY_ASSUME_DAYS` / `AUTO_COMPLETE_DAYS` default.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};

/// Shortest period a seller may set, in days.
pub const MIN_TERM_DAYS: i64 = 1;
/// Longest period a seller may set, in days.
pub const MAX_TERM_DAYS: i64 = 60;

/// Shop-record field naming the days after shipment when delivery is assumed.
const DELIVERY_ASSUME_FIELD: &str = "deliveryAssumeDays";
/// Shop-record field naming the days after delivery when the order completes.
const AUTO_COMPLETE_FIELD: &str = "autoCompleteDays";

/// The periods a seller set. `None` means unset: the configured default
/// applies when the sweeps run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SellerDeliveryTerms {
    pub delivery_assume_days: Option<i32>,
    pub auto_complete_days: Option<i32>,
}

impl SellerDeliveryTerms {
    /// Reads the two fields from a shop record. A field that is absent, not
    /// an integer, or outside 1 to 60 reads as unset: the record is the
    /// seller's, but the service only honors periods the Shop itself would
    /// have accepted.
    pub fn from_shop_record(record: &Value) -> Self {
        Self {
            delivery_assume_days: bounded_days(record, DELIVERY_ASSUME_FIELD),
            auto_complete_days: bounded_days(record, AUTO_COMPLETE_FIELD),
        }
    }
}

fn bounded_days(record: &Value, field: &str) -> Option<i32> {
    let days = record.get(field)?.as_i64()?;
    if !(MIN_TERM_DAYS..=MAX_TERM_DAYS).contains(&days) {
        return None;
    }
    i32::try_from(days).ok()
}

/// Freezes the seller's delivery periods on the order, once. Call it at a
/// payment lock point, inside the transaction that holds the order row.
///
/// - An order whose terms were already captured is left untouched, without
///   reading the homeserver again.
/// - Without a configured homeserver, or when the seller has no shop record
///   (`NotFound`), the order captures no periods and the defaults apply.
/// - When the shop record cannot be read (`Unavailable`), the payment still
///   starts: the order freezes the configured defaults and an
///   `ALERT seller_terms_fallback_unreadable_shop_record` is logged with the
///   seller pubky and order id, so payments are not coupled to homeserver
///   availability.
pub async fn capture_for_order(
    tx: &mut Transaction<'_, Postgres>,
    homeserver: Option<&dyn HomeserverListingClient>,
    order_id: Uuid,
    seller_pubky: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let already_captured: bool = sqlx::query_scalar(
        "SELECT delivery_terms_captured_at IS NOT NULL FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(&mut **tx)
    .await?;
    if already_captured {
        return Ok(());
    }
    let terms = match homeserver {
        None => SellerDeliveryTerms::default(),
        Some(homeserver) => match homeserver.fetch_shop(seller_pubky).await {
            HomeserverFetchOutcome::Found(record) => SellerDeliveryTerms::from_shop_record(&record),
            HomeserverFetchOutcome::NotFound => SellerDeliveryTerms::default(),
            HomeserverFetchOutcome::Unavailable => {
                tracing::error!(
                    seller_pubky = %seller_pubky,
                    order_id = %order_id,
                    "ALERT seller_terms_fallback_unreadable_shop_record: seller shop record \
                     unreadable at payment start; order frozen with the configured default \
                     delivery periods (seller_pubky={seller_pubky} order_id={order_id})"
                );
                SellerDeliveryTerms::default()
            }
        },
    };
    sqlx::query(
        "UPDATE orders SET delivery_assume_days = $2, auto_complete_days = $3, \
         delivery_terms_captured_at = $4 \
         WHERE id = $1 AND delivery_terms_captured_at IS NULL",
    )
    .bind(order_id)
    .bind(terms.delivery_assume_days)
    .bind(terms.auto_complete_days)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_both_periods_from_a_shop_record() {
        let terms = SellerDeliveryTerms::from_shop_record(&json!({
            "name": "Shop",
            "deliveryAssumeDays": 7,
            "autoCompleteDays": 30,
        }));
        assert_eq!(
            terms,
            SellerDeliveryTerms {
                delivery_assume_days: Some(7),
                auto_complete_days: Some(30),
            }
        );
    }

    #[test]
    fn absent_fields_read_as_unset() {
        assert_eq!(
            SellerDeliveryTerms::from_shop_record(&json!({ "name": "Shop" })),
            SellerDeliveryTerms::default()
        );
        assert_eq!(
            SellerDeliveryTerms::from_shop_record(&json!({
                "deliveryAssumeDays": 9,
            })),
            SellerDeliveryTerms {
                delivery_assume_days: Some(9),
                auto_complete_days: None,
            }
        );
    }

    #[test]
    fn the_bounds_are_inclusive_and_anything_else_reads_as_unset() {
        for (value, expected) in [
            (json!(1), Some(1)),
            (json!(60), Some(60)),
            (json!(0), None),
            (json!(61), None),
            (json!(-3), None),
            (json!(7.5), None),
            (json!("14"), None),
            (json!(null), None),
            (json!(true), None),
        ] {
            let terms = SellerDeliveryTerms::from_shop_record(&json!({
                "deliveryAssumeDays": value,
                "autoCompleteDays": value,
            }));
            assert_eq!(terms.delivery_assume_days, expected, "{value}");
            assert_eq!(terms.auto_complete_days, expected, "{value}");
        }
    }

    #[test]
    fn a_record_that_is_not_an_object_reads_as_unset() {
        assert_eq!(
            SellerDeliveryTerms::from_shop_record(&json!([1, 2])),
            SellerDeliveryTerms::default()
        );
    }
}
