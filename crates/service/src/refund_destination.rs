//! The refund destination of a USDT order: the Arbitrum One address the buyer
//! confirmed for the seller's refund, and the rules a recorded USDT refund
//! follows.
//!
//! The marketplace never holds funds and does not assume the address a USDT
//! payment came from can take a refund (the buyer may have paid from an
//! exchange). The buyer confirms an address on the order
//! (`refund.confirm_destination`), the seller sends the refund from their own
//! wallet, and records the Arbitrum transaction hash. Nothing here verifies
//! the refund on Arbitrum: it is a record of what the seller said they did.
//!
//! The destination applies to orders paid in USDT only: the legacy `usdt`
//! method, and an order bound through `paykit` whose payment settled on the
//! USDT rail (`payment_asset`). An order paid any other way never reads or
//! writes it, so a refund always follows the rail that was paid. The unit and
//! cap of a recorded refund ([`refund_terms`]) cover every rail: what was
//! actually paid, in sats for Bitcoin. Every refund record names its unit
//! ([`RefundUnit`]).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::model::OrderRow;
use crate::payment_attempt::OfferedAssets;

/// The `orders.payment_method` of an order paid in USDT.
pub const USDT_PAYMENT_METHOD: &str = "usdt";
/// What a USDT refund is sent in.
pub const REFUND_ASSET: &str = "USDT";
/// Where a USDT refund settles.
pub const REFUND_NETWORK: &str = "arbitrum-one";
/// The only address source today: the buyer typed it.
pub const SOURCE_BUYER_ENTERED: &str = "buyer_entered";

/// Whether the order was paid with USDT: bound to the legacy `usdt` method,
/// or settled on the USDT rail of a Paykit request.
pub fn is_usdt_order(order: &OrderRow) -> bool {
    is_usdt_payment(
        order.payment_method.as_deref(),
        order.payment_asset.as_deref(),
    )
}

/// [`is_usdt_order`] on the two `orders` columns it reads.
pub fn is_usdt_payment(payment_method: Option<&str>, payment_asset: Option<&str>) -> bool {
    payment_method == Some(USDT_PAYMENT_METHOD) || payment_asset == Some(REFUND_ASSET)
}

fn hex_body(address: &str) -> Option<&str> {
    let body = address.strip_prefix("0x")?;
    (body.len() == 40 && body.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(body)
}

/// The EIP-55 mixed-case form of 40 hex digits: a letter is upper-case when
/// the matching nibble of the Keccak-256 of the lower-case digits is 8 or
/// more.
fn eip55_checksum(body: &str) -> String {
    let lower = body.to_ascii_lowercase();
    let digest = Keccak256::digest(lower.as_bytes());
    lower
        .char_indices()
        .map(|(index, digit)| {
            let nibble = if index % 2 == 0 {
                digest[index / 2] >> 4
            } else {
                digest[index / 2] & 0x0f
            };
            if digit.is_ascii_alphabetic() && nibble >= 8 {
                digit.to_ascii_uppercase()
            } else {
                digit
            }
        })
        .collect()
}

/// Whether `input` is an Arbitrum One address: `0x` and 40 hex digits. An
/// all-lower-case or all-upper-case address carries no checksum and is
/// accepted; a mixed-case one must match its EIP-55 checksum, because a
/// wrong-case digit is how a typo is caught.
pub fn is_valid_address(input: &str) -> bool {
    let Some(body) = hex_body(input) else {
        return false;
    };
    let has_lower = body.bytes().any(|byte| byte.is_ascii_lowercase());
    let has_upper = body.bytes().any(|byte| byte.is_ascii_uppercase());
    !(has_lower && has_upper) || body == eip55_checksum(body)
}

/// Whether `input` is an Arbitrum transaction hash: `0x` and 64 lower-case
/// hex digits.
pub fn is_transaction_hash(input: &str) -> bool {
    input.strip_prefix("0x").is_some_and(|body| {
        body.len() == 64
            && body
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// The buyer-confirmed destination of one order.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RefundDestinationRow {
    pub order_id: Uuid,
    pub buyer_pubky: String,
    pub asset: String,
    pub network: String,
    pub address: String,
    pub address_source: String,
    pub confirmed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const DESTINATION_COLUMNS: &str =
    "order_id, buyer_pubky, asset, network, address, address_source, confirmed_at, updated_at";

impl RefundDestinationRow {
    /// The participant projection: the address, where it settles, who chose
    /// it, and when the buyer last confirmed it. Never in a public
    /// projection and never logged.
    pub fn view(&self) -> Value {
        json!({
            "address": self.address,
            "network": self.network,
            "asset": self.asset,
            "source": self.address_source,
            "confirmed_at": format_timestamp(self.confirmed_at),
        })
    }
}

/// The order's destination, if the buyer confirmed one.
pub async fn fetch(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<RefundDestinationRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {DESTINATION_COLUMNS} FROM order_refund_destinations WHERE order_id = $1"
    ))
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Stores the buyer's address, replacing an earlier one. The caller holds
/// the order row lock.
pub async fn upsert(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    buyer_pubky: &str,
    address: &str,
    now: DateTime<Utc>,
) -> Result<RefundDestinationRow, sqlx::Error> {
    sqlx::query_as(&format!(
        "INSERT INTO order_refund_destinations \
         (order_id, buyer_pubky, asset, network, address, address_source, confirmed_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7) \
         ON CONFLICT (order_id) DO UPDATE SET address = EXCLUDED.address, \
         address_source = EXCLUDED.address_source, confirmed_at = EXCLUDED.confirmed_at, \
         updated_at = EXCLUDED.updated_at \
         RETURNING {DESTINATION_COLUMNS}"
    ))
    .bind(order_id)
    .bind(buyer_pubky)
    .bind(REFUND_ASSET)
    .bind(REFUND_NETWORK)
    .bind(address)
    .bind(SOURCE_BUYER_ENTERED)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
}

/// Stamps `refund_destination` onto the participant views of USDT orders:
/// the destination, or `null` until the buyer confirms one. Orders of every
/// other method get no key, so their projections are unchanged. The views
/// are matched to orders by `id`.
pub async fn attach_to_views(
    pool: &PgPool,
    orders: &[OrderRow],
    views: &mut [Value],
) -> Result<(), sqlx::Error> {
    let usdt_ids: Vec<Uuid> = orders
        .iter()
        .filter(|order| is_usdt_order(order))
        .map(|order| order.id)
        .collect();
    if usdt_ids.is_empty() {
        return Ok(());
    }
    let rows: Vec<RefundDestinationRow> = sqlx::query_as(&format!(
        "SELECT {DESTINATION_COLUMNS} FROM order_refund_destinations WHERE order_id = ANY($1)"
    ))
    .bind(&usdt_ids)
    .fetch_all(pool)
    .await?;
    let destinations: HashMap<Uuid, Value> =
        rows.iter().map(|row| (row.order_id, row.view())).collect();
    for view in views {
        let Some(id) = view["id"].as_str().and_then(|id| Uuid::parse_str(id).ok()) else {
            continue;
        };
        if usdt_ids.contains(&id) {
            view["refund_destination"] = destinations.get(&id).cloned().unwrap_or(Value::Null);
        }
    }
    Ok(())
}

/// The USDT amount the order received, as a refund of the whole payment
/// records it. `payment_amount_minor` is what the observed transfer carried
/// (a transfer for another amount than quoted included), so a mismatched
/// payment is refunded as it arrived. A USD order records it in order units
/// (cents), and an amount between two cents as the lower one; an order priced
/// in anything else records the paid asset's own units (USDT0 millionths),
/// because its price is not in a unit a USDT refund can be expressed in.
/// `None` when the order carries no USDT terms, or a USD order's paid exponent
/// does not convert to cents.
pub fn received_refund_amount_minor(order: &OrderRow) -> Option<i64> {
    let received = order.payment_amount_minor?;
    if order.currency != "USD" {
        return Some(received);
    }
    usdt_units_per_order_unit(order).map(|divisor| received / divisor)
}

/// The USDT amount a refund of the whole payment records, when it records
/// anything: `None` for no USDT terms, an unconvertible exponent, and a USD
/// order paid less than one cent, which floors to 0 and fails the refund
/// constraint. The cap stays [`received_refund_amount_minor`]'s 0, so a
/// seller's record of such an order is refused too.
pub fn recordable_refund_amount_minor(order: &OrderRow) -> Option<i64> {
    received_refund_amount_minor(order).filter(|amount| *amount > 0)
}

/// The unit a recorded refund's `amount_minor` is in. Every refund written
/// since migration 0093 names it as `external_refund.amount_unit`, and 0093
/// named it on every refund written before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefundUnit {
    /// The order currency's minor units.
    Order,
    /// Sats: the order was paid on the Bitcoin rail.
    Sat,
    /// USDT0 base units: a USDT-paid order priced in anything but USD.
    Usdt0,
}

impl RefundUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Order => "order",
            Self::Sat => "sat",
            Self::Usdt0 => "usdt0",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "order" => Some(Self::Order),
            "sat" => Some(Self::Sat),
            "usdt0" => Some(Self::Usdt0),
            _ => None,
        }
    }

    /// The unit a refund record names, if it names one.
    pub fn of_record(record: &Value) -> Option<Self> {
        record
            .get("amount_unit")
            .and_then(Value::as_str)
            .and_then(Self::parse)
    }

    /// The unit of a refund recorded before refunds named one, as the code
    /// that wrote it recorded it: the refunded manual-review resolution
    /// (`by_review`) wrote a Bitcoin order's frozen observed sats, and every
    /// other writer the order total's units, except USDT0 for a USDT-paid
    /// order priced in anything but USD. Migration 0093 applies the same rule.
    pub fn unnamed(usdt_paid: bool, currency: &str, by_review: bool) -> Self {
        match (usdt_paid, by_review) {
            (true, _) if currency != "USD" => Self::Usdt0,
            (true, _) => Self::Order,
            (false, true) => Self::Sat,
            (false, false) => Self::Order,
        }
    }
}

/// What a refund of this order is recorded in and capped at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefundTerms {
    pub unit: RefundUnit,
    pub cap_minor: i64,
    /// The decimal places of `unit`: 0 for sats, the paid asset's for
    /// USDT0, the order's own otherwise.
    pub exponent: i32,
}

impl RefundTerms {
    /// The seller's projection: what a refund is entered in and up to.
    pub fn view(&self) -> Value {
        json!({
            "amount_minor": self.cap_minor,
            "unit": self.unit.as_str(),
            "exponent": self.exponent,
        })
    }

    /// Whether a refund record covers everything a refund may be: `None`
    /// when it names no unit, or another unit than these terms, so its
    /// amount cannot be read against the cap.
    pub fn covered_by(&self, record: &Value) -> Option<bool> {
        if RefundUnit::of_record(record)? != self.unit {
            return None;
        }
        let amount = record.get("amount_minor")?.as_i64()?;
        Some(amount >= self.cap_minor)
    }
}

/// What was actually paid, in the unit a refund of it is recorded in: the
/// sats an order paid on the Bitcoin rail received, whatever its price
/// currency; for a USDT-paid order the USDT it received
/// ([`received_refund_amount_minor`]), in USDT0 units when it is priced in
/// anything but USD, so an underpaid USDT order is capped below its total and
/// an overpaid one may record its excess; the order total otherwise.
pub fn refund_terms(order: &OrderRow) -> RefundTerms {
    if let Some(sats) = BitcoinPayment::of(order).received_sats() {
        return RefundTerms {
            unit: RefundUnit::Sat,
            cap_minor: sats,
            exponent: 0,
        };
    }
    if !is_usdt_order(order) {
        return RefundTerms {
            unit: RefundUnit::Order,
            cap_minor: order.total_minor,
            exponent: order.exponent,
        };
    }
    let cap_minor = received_refund_amount_minor(order).unwrap_or(order.total_minor);
    if order.currency != "USD" {
        return RefundTerms {
            unit: RefundUnit::Usdt0,
            cap_minor,
            exponent: order.payment_exponent.map_or(USDT0_EXPONENT, i32::from),
        };
    }
    RefundTerms {
        unit: RefundUnit::Order,
        cap_minor,
        exponent: order.exponent,
    }
}

/// How many of the paid asset's base units make one order unit of a USD
/// order: `10^(payment_exponent - exponent)`.
fn usdt_units_per_order_unit(order: &OrderRow) -> Option<i64> {
    let paid_exponent = i32::from(order.payment_exponent?);
    let steps = u32::try_from(paid_exponent.checked_sub(order.exponent)?).ok()?;
    10_i64.checked_pow(steps).filter(|divisor| *divisor > 0)
}

/// Whether the order's own minor unit is the sat.
pub fn priced_in_sats(currency: &str, exponent: i32) -> bool {
    matches!((currency, exponent), ("SAT", 0) | ("BTC", 8))
}

/// The `orders` columns a Bitcoin payment's sats are read from.
#[derive(Debug, Clone, Copy)]
pub struct BitcoinPayment<'a> {
    pub payment_method: Option<&'a str>,
    pub payment_asset: Option<&'a str>,
    pub payment_amount_minor: Option<i64>,
    pub paykit_observed_sats: Option<i64>,
    pub paykit_total_sats: Option<i64>,
    /// Both rails were paid (`paykit_second_rail_at`).
    pub second_rail_paid: bool,
    /// The request offered USDT as well as Bitcoin.
    pub usdt_offered: bool,
}

impl<'a> BitcoinPayment<'a> {
    pub fn of(order: &'a OrderRow) -> Self {
        Self {
            payment_method: order.payment_method.as_deref(),
            payment_asset: order.payment_asset.as_deref(),
            payment_amount_minor: order.payment_amount_minor,
            paykit_observed_sats: order.paykit_observed_sats,
            paykit_total_sats: order.paykit_total_sats,
            second_rail_paid: order.paykit_second_rail_at.is_some(),
            usdt_offered: usdt_offered(
                order.paykit_accepted_assets.as_deref(),
                order.paykit_payment_amounts.as_ref(),
            ),
        }
    }

    /// The sats an order paid on the Bitcoin rail received, which its
    /// refunds are recorded in: the observed amount frozen at settlement,
    /// else the settled Bitcoin terms, else the payable its request asked
    /// for, payment code included. `None` for any other order: USDT, PayPal,
    /// an order with no sats on record, one paid on both rails with no first
    /// rail named, whose refund follows no single rail, and one that records
    /// no rail while its request offered USDT and no Bitcoin was observed
    /// (settlement leaves the asset unrecorded for an unknown denomination or
    /// a payment that names no amount, so the money may have been USDT).
    pub fn received_sats(&self) -> Option<i64> {
        if !matches!(
            self.payment_method,
            Some(PAYKIT_PAYMENT_METHOD | BITCOIN_PAYMENT_METHOD)
        ) {
            return None;
        }
        let only_bitcoin_possible = self.paykit_observed_sats.is_some() || !self.usdt_offered;
        let settled_in_bitcoin = match self.payment_asset {
            Some(BITCOIN_ASSET) => true,
            None if !self.second_rail_paid && only_bitcoin_possible => false,
            _ => return None,
        };
        let settled = self.payment_amount_minor.filter(|_| settled_in_bitcoin);
        self.paykit_observed_sats
            .or(settled)
            .or(self.paykit_total_sats)
            .filter(|sats| *sats > 0)
    }
}

/// Whether a Paykit request offered USDT: by the assets paykit-server derived
/// for it, or by its offered amounts. A request with neither is the legacy
/// Bitcoin-only `amount_sats` request.
pub fn usdt_offered(accepted_assets: Option<&[String]>, payment_amounts: Option<&Value>) -> bool {
    accepted_assets.is_some_and(|assets| assets.iter().any(|asset| asset == REFUND_ASSET))
        || payment_amounts
            .and_then(OfferedAssets::from_json)
            .is_some_and(|offered| offered.usdt_units.is_some())
}

const PAYKIT_PAYMENT_METHOD: &str = "paykit";
const BITCOIN_PAYMENT_METHOD: &str = "bitcoin";
/// The `orders.payment_asset` of a Paykit payment settled on the Bitcoin rail.
const BITCOIN_ASSET: &str = "BTC";
/// USDT0's decimal places, for an order whose USDT terms name none.
const USDT0_EXPONENT: i32 = 6;

#[cfg(test)]
mod tests {
    use super::*;

    /// A 137.00 order paid on the USDT rail with `received` USDT0 millionths.
    fn usdt_order(currency: &str, received: Option<i64>) -> OrderRow {
        let mut row = OrderRow::new_for_insert(
            Uuid::nil(),
            None,
            None,
            "buyer".to_string(),
            "seller".to_string(),
            true,
            json!([]),
            None,
            13_700,
            0,
            13_700,
            currency.to_string(),
            2,
            Uuid::nil(),
            false,
            None,
            "shipping".to_string(),
            Utc::now(),
            "listing".to_string(),
        );
        row.payment_method = Some("paykit".to_string());
        row.payment_asset = Some(REFUND_ASSET.to_string());
        row.payment_amount_minor = received;
        row.payment_exponent = Some(6);
        row
    }

    #[test]
    fn a_usd_usdt_refund_is_capped_at_the_usdt_received_not_the_total() {
        // Exactly the quote: the cap is the total, as before, in cents.
        assert_eq!(
            refund_terms(&usdt_order("USD", Some(137_000_000))),
            RefundTerms {
                unit: RefundUnit::Order,
                cap_minor: 13_700,
                exponent: 2,
            }
        );
        // One USDT short: no more than arrived.
        assert_eq!(
            refund_terms(&usdt_order("USD", Some(136_000_000))).cap_minor,
            13_600
        );
        // One USDT over: the excess is refundable.
        assert_eq!(
            refund_terms(&usdt_order("USD", Some(138_000_000))).cap_minor,
            13_800
        );
        // Between two cents: the lower one, never more than arrived.
        assert_eq!(
            refund_terms(&usdt_order("USD", Some(137_009_999))).cap_minor,
            13_700
        );
        // No USDT terms recorded: the total.
        assert_eq!(refund_terms(&usdt_order("USD", None)).cap_minor, 13_700);
    }

    #[test]
    fn a_non_usd_usdt_refund_is_capped_at_the_usdt0_received() {
        assert_eq!(
            refund_terms(&usdt_order("EUR", Some(148_539_999))),
            RefundTerms {
                unit: RefundUnit::Usdt0,
                cap_minor: 148_539_999,
                exponent: 6,
            }
        );
        assert_eq!(refund_terms(&usdt_order("EUR", None)).cap_minor, 13_700);
    }

    #[test]
    fn an_order_paid_another_way_is_capped_at_its_total() {
        let mut paypal = usdt_order("USD", Some(138_000_000));
        paypal.payment_method = Some("paypal".to_string());
        paypal.payment_asset = None;
        assert_eq!(refund_terms(&paypal).cap_minor, 13_700);
    }

    #[test]
    fn the_whole_refund_is_the_usdt_received_in_recorded_units() {
        assert_eq!(
            received_refund_amount_minor(&usdt_order("USD", Some(136_000_000))),
            Some(13_600)
        );
        assert_eq!(
            received_refund_amount_minor(&usdt_order("USD", Some(138_000_000))),
            Some(13_800)
        );
        // Between two cents: the lower one, like the cap.
        assert_eq!(
            received_refund_amount_minor(&usdt_order("USD", Some(137_009_999))),
            Some(13_700)
        );
        let mut no_paid_exponent = usdt_order("USD", Some(137_000_000));
        no_paid_exponent.payment_exponent = None;
        assert_eq!(received_refund_amount_minor(&no_paid_exponent), None);
        // Less than a cent floors to 0: capped at 0, never recordable.
        for dust in [1, 9_999] {
            let order = usdt_order("USD", Some(dust));
            assert_eq!(received_refund_amount_minor(&order), Some(0));
            assert_eq!(refund_terms(&order).cap_minor, 0);
            assert_eq!(recordable_refund_amount_minor(&order), None);
        }
        assert_eq!(
            recordable_refund_amount_minor(&usdt_order("USD", Some(10_000))),
            Some(1)
        );
        assert_eq!(refund_terms(&no_paid_exponent).cap_minor, 13_700);
        assert_eq!(
            received_refund_amount_minor(&usdt_order("EUR", Some(148_539_999))),
            Some(148_539_999)
        );
    }

    // The EIP-55 reference vectors (EIP-55, "Test Cases").
    const CHECKSUMMED: [&str; 8] = [
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
        "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
        "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
        "0x52908400098527886E0F7030069857D2E4169EE7",
        "0x8617E340B3D01FA5F11F306F4090FD50E238070D",
        "0xde709f2102306220921060314715629080e2fb77",
        "0x27b1fdb04752bbc536007a920d24acb045561c26",
    ];

    #[test]
    fn eip55_reference_vectors_validate() {
        for address in CHECKSUMMED {
            assert!(is_valid_address(address), "{address}");
        }
    }

    #[test]
    fn a_wrong_case_digit_in_a_mixed_case_address_is_refused() {
        for address in &CHECKSUMMED[..6] {
            for (index, character) in address.char_indices().skip(2) {
                if !character.is_ascii_alphabetic() {
                    continue;
                }
                let flipped = if character.is_ascii_uppercase() {
                    character.to_ascii_lowercase()
                } else {
                    character.to_ascii_uppercase()
                };
                let mut typo = address.to_string();
                typo.replace_range(index..=index, &flipped.to_string());
                assert!(!is_valid_address(&typo), "{typo}");
            }
        }
    }

    #[test]
    fn single_case_addresses_carry_no_checksum() {
        assert!(is_valid_address(
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
        ));
        assert!(is_valid_address(
            "0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED"
        ));
        assert!(is_valid_address(&format!("0x{}", "1".repeat(40))));
    }

    #[test]
    fn malformed_addresses_are_refused() {
        for address in [
            "",
            "0x",
            "5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0X5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beae",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaedd",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaeg",
            " 0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed\n",
            "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
        ] {
            assert!(!is_valid_address(address), "{address:?}");
        }
    }

    #[test]
    fn transaction_hashes_are_66_lower_case_hex() {
        assert!(is_transaction_hash(&format!("0x{}", "ab12".repeat(16))));
        for hash in [
            String::new(),
            "0x".to_string(),
            "ab12".repeat(16),
            format!("0x{}", "ab12".repeat(15)),
            format!("0x{}", "ab12".repeat(17)),
            format!("0x{}", "AB12".repeat(16)),
            format!("0x{}g", "ab12".repeat(15) + "ab1"),
            format!(" 0x{}", "ab12".repeat(16)),
        ] {
            assert!(!is_transaction_hash(&hash), "{hash:?}");
        }
    }

    fn bitcoin(asset: Option<&'static str>) -> BitcoinPayment<'static> {
        BitcoinPayment {
            payment_method: Some(PAYKIT_PAYMENT_METHOD),
            payment_asset: asset,
            payment_amount_minor: None,
            paykit_observed_sats: None,
            paykit_total_sats: Some(51_637),
            second_rail_paid: false,
            usdt_offered: false,
        }
    }

    #[test]
    fn a_bitcoin_refund_is_capped_at_the_sats_received() {
        // A fork order records no asset: the payable, payment code included.
        assert_eq!(bitcoin(None).received_sats(), Some(51_637));
        // The observed amount wins over the payable, short or over.
        for observed in [50_000, 60_000] {
            let paid = BitcoinPayment {
                paykit_observed_sats: Some(observed),
                ..bitcoin(None)
            };
            assert_eq!(paid.received_sats(), Some(observed));
        }
        // Settled Bitcoin terms are sats (exponent 8).
        let settled = BitcoinPayment {
            payment_amount_minor: Some(52_000),
            ..bitcoin(Some(BITCOIN_ASSET))
        };
        assert_eq!(settled.received_sats(), Some(52_000));
        // The Bitcoin-only bind keeps the legacy method name.
        let legacy = BitcoinPayment {
            payment_method: Some(BITCOIN_PAYMENT_METHOD),
            ..bitcoin(None)
        };
        assert_eq!(legacy.received_sats(), Some(51_637));
    }

    #[test]
    fn no_sats_cap_off_the_bitcoin_rail() {
        assert_eq!(bitcoin(Some(REFUND_ASSET)).received_sats(), None);
        let both_rails = BitcoinPayment {
            second_rail_paid: true,
            ..bitcoin(None)
        };
        assert_eq!(both_rails.received_sats(), None);
        let paypal = BitcoinPayment {
            payment_method: Some("paypal"),
            ..bitcoin(None)
        };
        assert_eq!(paypal.received_sats(), None);
        let unbound = BitcoinPayment {
            paykit_total_sats: None,
            ..bitcoin(None)
        };
        assert_eq!(unbound.received_sats(), None);
    }

    #[test]
    fn an_unrecorded_rail_is_bitcoin_only_when_usdt_was_not_possible() {
        // The request offered USDT and settlement recorded no rail: the money
        // may have been USDT, so no sats cap applies.
        let offered_usdt = BitcoinPayment {
            usdt_offered: true,
            ..bitcoin(None)
        };
        assert_eq!(offered_usdt.received_sats(), None);
        // Bitcoin observed on such a request is the rail that was paid.
        let observed = BitcoinPayment {
            paykit_observed_sats: Some(51_000),
            ..offered_usdt
        };
        assert_eq!(observed.received_sats(), Some(51_000));
        // A recorded Bitcoin rail needs no observation.
        let recorded = BitcoinPayment {
            payment_asset: Some(BITCOIN_ASSET),
            ..offered_usdt
        };
        assert_eq!(recorded.received_sats(), Some(51_637));
    }

    #[test]
    fn usdt_is_offered_by_accepted_assets_or_offered_amounts() {
        assert!(!usdt_offered(None, None));
        assert!(!usdt_offered(Some(&["BTC".to_string()]), None));
        assert!(usdt_offered(
            Some(&["BTC".to_string(), "USDT".to_string()]),
            None
        ));
        assert!(usdt_offered(
            None,
            Some(&json!({"BTC": 51_637, "USDT": 40_000_000}))
        ));
        assert!(!usdt_offered(None, Some(&json!({"BTC": 51_637}))));
    }

    #[test]
    fn refund_units_round_trip_and_unknown_ones_are_not_named() {
        for unit in [RefundUnit::Order, RefundUnit::Sat, RefundUnit::Usdt0] {
            assert_eq!(RefundUnit::parse(unit.as_str()), Some(unit));
            assert_eq!(
                RefundUnit::of_record(&json!({"amount_unit": unit.as_str()})),
                Some(unit)
            );
        }
        assert_eq!(RefundUnit::parse("cents"), None);
        assert_eq!(RefundUnit::of_record(&json!({"amount_minor": 5})), None);
        assert_eq!(RefundUnit::of_record(&json!({"amount_unit": 5})), None);
    }

    #[test]
    fn an_unnamed_refund_is_read_in_the_unit_its_writer_used() {
        // A refunded review recorded a Bitcoin order's frozen observed sats.
        assert_eq!(RefundUnit::unnamed(false, "USD", true), RefundUnit::Sat);
        assert_eq!(RefundUnit::unnamed(false, "EUR", true), RefundUnit::Sat);
        // Every other writer recorded the order total's units.
        assert_eq!(RefundUnit::unnamed(false, "USD", false), RefundUnit::Order);
        // A USDT order is in USDT0 unless priced in USD, by either writer.
        for by_review in [false, true] {
            assert_eq!(
                RefundUnit::unnamed(true, "EUR", by_review),
                RefundUnit::Usdt0
            );
            assert_eq!(
                RefundUnit::unnamed(true, "USD", by_review),
                RefundUnit::Order
            );
        }
    }

    #[test]
    fn a_refund_is_full_only_when_it_reaches_the_cap_in_the_same_unit() {
        let terms = RefundTerms {
            unit: RefundUnit::Sat,
            cap_minor: 51_637,
            exponent: 0,
        };
        let sats = |amount: i64| json!({"amount_minor": amount, "amount_unit": "sat"});
        assert_eq!(terms.covered_by(&sats(51_637)), Some(true));
        assert_eq!(terms.covered_by(&sats(25_819)), Some(false));
        // Cents, or a record that names no unit, are not read against sats.
        assert_eq!(
            terms.covered_by(&json!({"amount_minor": 13_700, "amount_unit": "order"})),
            None
        );
        assert_eq!(terms.covered_by(&json!({"amount_minor": 51_637})), None);
        assert_eq!(terms.covered_by(&json!({"amount_unit": "sat"})), None);
    }

    #[test]
    fn sats_are_the_order_unit_of_sat_and_btc_prices_only() {
        assert!(priced_in_sats("SAT", 0));
        assert!(priced_in_sats("BTC", 8));
        assert!(!priced_in_sats("BTC", 2));
        assert!(!priced_in_sats("USD", 2));
        assert!(!priced_in_sats("SAT", 2));
    }
}
