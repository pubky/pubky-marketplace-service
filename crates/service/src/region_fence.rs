//! The EU scope fence (legal launch plan C2-C4), behind `EU_FENCE_ENABLED`
//! or, with `REGION_MODES` set, for every nuanced region
//! ([`crate::region_policy`]).
//!
//! The launch design keeps business-seller duties out of EU sales by
//! limiting what the service accepts, from facts the sellers and buyers
//! publish themselves:
//!
//! - **C3, regional gate.** A buyer in a nuanced region (the EU-27 and GB
//!   unless `REGION_MODES` says otherwise) can buy only from a seller who
//!   declared `sellerStatus: "private"` in their own shop record.
//!   A seller who declared `"trader"`, or declared nothing, is unavailable to
//!   these buyers: `SELLER_UNAVAILABLE_IN_REGION` with reason
//!   `business_seller` or `undeclared_seller`. GB is in C3 because UK
//!   consumers buying from a business have a 14-day withdrawal right under
//!   the Consumer Contracts Regulations 2013; C4 below stays EU-only (DAC7).
//! - **C4, annual limit.** A seller whose shop country is in the EU-27 and is
//!   a nuanced region stops
//!   at 29 paid sales and EUR 2,000 of paid orders per calendar year (UTC),
//!   whichever comes first: `SELLER_ANNUAL_LIMIT_REACHED`. Only paid orders
//!   count (a confirmed payment, whatever became of the order afterwards, so
//!   a refunded sale still counts); an unpaid checkout never uses a slot.
//!   Because nothing reserves a slot before payment, the limit is checked
//!   again where money is taken: see [`recheck_at_payment`]. A seller whose
//!   shop record carries no usable country has no residence, so no cap
//!   applies to them (LG16: the fence never reads a missing country as EU).
//!   A private seller of any country who reaches the same numbers in sales to
//!   EU buyers must re-confirm their status (`sellerStatusConfirmedAt` in the
//!   shop record) before the next such sale: `SELLER_UNAVAILABLE_IN_REGION`
//!   with reason `private_seller_reconfirmation_required`. The count restarts
//!   at the confirmation. The re-confirmation point is checked when the order
//!   is created, on paid sales only.
//!
//! The buyer's country is the one the buyer's client sends in clear text
//! (`buyer_country`); the sealed address is never opened. It is a
//! declaration, like the seller's: the fence is a design limit on what the
//! service accepts, not identity verification.
//!
//! Evaluation has two halves. [`gate`] runs before the command's transaction
//! with the moderation and lifecycle gates: it requires `buyer_country`,
//! reads each seller's shop record from their homeserver (fail closed, like
//! the lifecycle gate), applies C3, and refuses an offer or bid on a seller
//! already at a limit. The orders themselves are counted inside the order's
//! transaction by [`lock_sellers`] and [`check_new_order`], under a
//! per-seller advisory lock taken before any listing lock. An auction's
//! winning order is created by the close and is not refused there, since the
//! bidder already won; the limit is enforced at the bid and again at payment.
//!
//! A seller whose shop country is a blocked region cannot sell at all:
//! [`gate`] refuses every buyer command on their listings with
//! `SELLER_UNAVAILABLE_IN_REGION`, reason `seller_region_blocked`, from the
//! same shop record read.
//!
//! With the fence off nothing here refuses and no shop record is read.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, TimeZone, Utc};
use marketplace_domain::commands::{Command, CommandPayload};
use marketplace_domain::region::{shop_country_from_record, SellerStatus, EU_27};
use marketplace_domain::{Currency, ErrorCode, ValidationIssue};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, Transaction};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use crate::listing_moderation::SoldListing;
use crate::refusal_audit::RefusalKind;
use crate::region_policy::RegionModes;
use crate::result::CommandFailure;
use crate::AppState;

/// Paid sales a seller resident in the EU may make per calendar year.
pub const ANNUAL_SALES_LIMIT: i64 = 29;
/// Paid orders a seller resident in the EU may take per calendar year, in
/// euro cents.
pub const ANNUAL_EUR_LIMIT_CENTS: u64 = 200_000;

pub const REGION_MESSAGE: &str = "This seller doesn't sell to buyers in the EU or UK.";
/// A buyer command on a listing whose seller's shop country is blocked.
pub const SELLER_BLOCKED_MESSAGE: &str = "This seller can't sell on Pubky Market right now.";
pub const RECONFIRM_MESSAGE: &str =
    "This seller must confirm they still sell as a private individual before selling again.";
pub const LIMIT_MESSAGE: &str = "This seller isn't taking new orders right now.";
pub const REASON_BUSINESS: &str = "business_seller";
pub const REASON_UNDECLARED: &str = "undeclared_seller";
/// The seller's shop country is a blocked region.
pub const REASON_SELLER_REGION_BLOCKED: &str = "seller_region_blocked";
pub const REASON_RECONFIRM: &str = "private_seller_reconfirmation_required";
pub const REASON_SALES_LIMIT: &str = "annual_sales_limit";
pub const REASON_AMOUNT_LIMIT: &str = "annual_amount_limit";

/// The longest one command waits for its seller shop reads.
pub const READ_TIMEOUT: Duration = Duration::from_secs(12);
const MAX_CONCURRENT_READS: usize = 8;

/// Advisory-lock seed for one seller's annual counters. Not one of the seeds
/// listed in `test_migrator.rs`.
const SELLER_LOCK_SEED: i64 = 6381;

/// What the seller's shop record says, read for one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SellerFacts {
    pub status: SellerStatus,
    /// `location.countryCode` of the shop record, when valid.
    pub country: Option<String>,
    /// `sellerStatusConfirmedAt`: when the seller last re-confirmed that
    /// they sell as a private individual.
    pub status_confirmed_at: Option<DateTime<Utc>>,
}

/// The shop record's `sellerStatusConfirmedAt`: a valid RFC 3339 timestamp
/// that is not in the future, else `None` (a future or malformed value counts
/// as no confirmation).
pub fn status_confirmed_at_from_record(
    record: &Value,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    record
        .get("sellerStatusConfirmedAt")
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|moment| moment.with_timezone(&Utc))
        .filter(|moment| *moment <= now)
}

impl SellerFacts {
    pub fn from_shop_record(record: &Value, now: DateTime<Utc>) -> Self {
        Self {
            status: SellerStatus::from_shop_record(record),
            country: shop_country_from_record(record),
            status_confirmed_at: status_confirmed_at_from_record(record, now),
        }
    }

    /// A seller without a shop record declared nothing.
    pub fn undeclared() -> Self {
        Self {
            status: SellerStatus::Undeclared,
            country: None,
            status_confirmed_at: None,
        }
    }
}

/// What [`gate`] learned for one command, handed to the handler that
/// creates the order. Empty (`enabled == false`) when the fence is off or
/// the command sells nothing.
#[derive(Debug, Clone, Default)]
pub struct FencePlan {
    pub enabled: bool,
    modes: RegionModes,
    buyer_country: Option<String>,
    sellers: BTreeMap<String, SellerFacts>,
}

impl FencePlan {
    pub fn off() -> Self {
        Self::default()
    }

    pub fn facts(&self, seller_pubky: &str) -> Option<&SellerFacts> {
        self.sellers.get(seller_pubky)
    }

    pub fn buyer_country(&self) -> Option<&str> {
        self.buyer_country.as_deref()
    }

    /// The sellers whose counters the order's transaction must serialize.
    pub fn sellers(&self) -> impl Iterator<Item = &str> {
        self.sellers.keys().map(String::as_str)
    }
}

/// The buyer country a command carries, for the four commands that sell:
/// `Some(None)` when the command sells but sent none, `None` for every other
/// command.
pub fn buyer_country_of(payload: &CommandPayload) -> Option<Option<&str>> {
    match payload {
        CommandPayload::CreateCheckout(p) => Some(p.buyer_country.as_deref()),
        CommandPayload::CreateOffer(p) => Some(p.buyer_country.as_deref()),
        CommandPayload::OfferCheckout(p) => Some(p.buyer_country.as_deref()),
        CommandPayload::PlaceBid(p) => Some(p.buyer_country.as_deref()),
        _ => None,
    }
}

pub(crate) fn region_refusal(reason: &'static str, message: &str) -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::SellerUnavailableInRegion,
        message,
        reason,
    )
}

fn limit_refusal(reason: &'static str) -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::SellerAnnualLimitReached,
        LIMIT_MESSAGE,
        reason,
    )
}

pub(crate) fn unavailable() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::UpstreamUnavailable,
        ErrorCode::UpstreamUnavailable,
        crate::listing_lifecycle::UNAVAILABLE_MESSAGE,
    )
}

pub(crate) fn missing_country() -> CommandFailure {
    CommandFailure::refused_with_issues(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "The marketplace command is invalid.",
        vec![ValidationIssue {
            path: "payload.buyer_country".to_string(),
            message: "Expected the buyer's ISO 3166-1 alpha-2 country code".to_string(),
            reason: None,
        }],
    )
}

/// The refusal C3 gives a buyer in a nuanced region for a seller who is not a
/// declared private seller; `None` when the sale is open.
pub fn region_verdict(
    modes: &RegionModes,
    facts: &SellerFacts,
    buyer_country: &str,
) -> Option<CommandFailure> {
    if !modes.private_seller_only(buyer_country) {
        return None;
    }
    match facts.status {
        SellerStatus::Private => None,
        SellerStatus::Trader => Some(region_refusal(REASON_BUSINESS, REGION_MESSAGE)),
        SellerStatus::Undeclared => Some(region_refusal(REASON_UNDECLARED, REGION_MESSAGE)),
    }
}

/// Runs before the command's transaction. With the fence off, or for a
/// command that sells nothing, returns [`FencePlan::off`]. Otherwise the
/// buyer's country is required, every live seller's shop record is read
/// (fail closed), a seller whose shop country is blocked is refused, C3 is
/// applied, and an offer or bid on a seller already at an annual limit is
/// refused.
pub async fn gate(
    state: &AppState,
    command: &Command,
    listings: &[SoldListing],
) -> Result<FencePlan, CommandFailure> {
    let Some(modes) = state.config.region_policy().fence_modes() else {
        return Ok(FencePlan::off());
    };
    let Some(country) = buyer_country_of(&command.payload) else {
        return Ok(FencePlan::off());
    };
    if country.is_none() && modes.has_nuanced_country() {
        return Err(missing_country());
    }
    let mut sellers: Vec<String> = listings
        .iter()
        .filter(|listing| listing.live)
        .map(|listing| listing.seller_pubky.clone())
        .collect();
    sellers.sort();
    sellers.dedup();
    let now = state.clock.now();
    let mut plan = FencePlan {
        enabled: true,
        modes: modes.clone(),
        buyer_country: country.map(str::to_string),
        sellers: BTreeMap::new(),
    };
    if sellers.is_empty() {
        return Ok(plan);
    }
    let Some(homeserver) = state.homeserver.clone() else {
        return Err(unavailable());
    };
    plan.sellers = read_sellers(homeserver, &sellers, now).await?;
    for seller in &sellers {
        let facts = &plan.sellers[seller];
        if modes.shop_country_blocked(facts.country.as_deref()) {
            return Err(region_refusal(
                REASON_SELLER_REGION_BLOCKED,
                SELLER_BLOCKED_MESSAGE,
            ));
        }
        if let Some(failure) = country.and_then(|country| region_verdict(modes, facts, country)) {
            return Err(failure);
        }
    }
    if matches!(
        command.payload,
        CommandPayload::CreateOffer(_) | CommandPayload::PlaceBid(_)
    ) {
        let mut conn = state.pool.acquire().await.map_err(|error| {
            tracing::error!(%error, "eu fence could not read seller counters");
            unavailable()
        })?;
        for seller in &sellers {
            let facts = &plan.sellers[seller];
            check_limits(&mut conn, modes, seller, facts, country, None, now).await?;
        }
    }
    Ok(plan)
}

pub(crate) async fn read_sellers(
    homeserver: Arc<dyn HomeserverListingClient>,
    sellers: &[String],
    now: DateTime<Utc>,
) -> Result<BTreeMap<String, SellerFacts>, CommandFailure> {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READS));
    let read = async {
        let mut reads = JoinSet::new();
        for seller in sellers {
            let homeserver = homeserver.clone();
            let permits = permits.clone();
            let seller = seller.clone();
            reads.spawn(async move {
                let _permit = permits.acquire_owned().await.ok();
                let outcome = homeserver.fetch_shop(&seller).await;
                (seller, outcome)
            });
        }
        let mut facts = BTreeMap::new();
        while let Some(joined) = reads.join_next().await {
            let Ok((seller, outcome)) = joined else {
                return None;
            };
            let seller_facts = match outcome {
                HomeserverFetchOutcome::Found(record) => {
                    SellerFacts::from_shop_record(&record, now)
                }
                HomeserverFetchOutcome::NotFound => SellerFacts::undeclared(),
                HomeserverFetchOutcome::Unavailable => return None,
            };
            facts.insert(seller, seller_facts);
        }
        Some(facts)
    };
    match tokio::time::timeout(READ_TIMEOUT, read).await {
        Ok(Some(facts)) => Ok(facts),
        Ok(None) | Err(_) => {
            tracing::warn!(
                sellers = sellers.len(),
                "eu fence seller shop record reads failed or timed out"
            );
            Err(unavailable())
        }
    }
}

/// Takes the per-seller counter locks, in seller order, as the first locks
/// of an order transaction: before any drop, listing or variant row, so a
/// concurrent checkout of the same seller waits here and never holds a
/// listing the other needs (`docs/lock-order.md`).
pub async fn lock_sellers(
    tx: &mut Transaction<'_, Postgres>,
    plan: &FencePlan,
) -> Result<(), sqlx::Error> {
    if !plan.enabled {
        return Ok(());
    }
    for seller in plan.sellers() {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(seller)
            .bind(SELLER_LOCK_SEED)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// The order about to be created, for the amount test.
pub struct NewOrder<'a> {
    pub total_minor: i64,
    pub currency: &'a str,
    pub exponent: i32,
}

/// Inside the order transaction, after [`lock_sellers`], before the order
/// row is inserted: refuses the order when C4 would be exceeded by it,
/// counting paid orders only. The refusal is returned for the handler to
/// answer; the transaction rolls back. Payment is checked again by
/// [`recheck_at_payment`].
pub async fn check_new_order(
    tx: &mut Transaction<'_, Postgres>,
    plan: &FencePlan,
    seller_pubky: &str,
    order: NewOrder<'_>,
    now: DateTime<Utc>,
) -> Result<(), CommandFailure> {
    if !plan.enabled {
        return Ok(());
    }
    let Some(facts) = plan.facts(seller_pubky) else {
        // The gate read every live seller of the command; an order for a
        // seller it did not read is not one the fence can vouch for.
        return Err(unavailable());
    };
    check_limits(
        tx,
        &plan.modes,
        seller_pubky,
        facts,
        plan.buyer_country(),
        Some(order),
        now,
    )
    .await
}

/// Counted usage in a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub sales: i64,
    pub eur_cents: u64,
}

/// Which orders a window counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Every buyer: the EU-resident seller's annual limit.
    AllBuyers,
    /// Only buyers in the EU-27: a private seller's re-confirmation point.
    EuBuyers,
}

pub fn year_start(now: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(now.year(), 1, 1, 0, 0, 0)
        .single()
        .expect("January 1st exists")
}

/// The seller's counted orders since `from`: paid orders only, meaning a
/// confirmed payment since `from`, whatever became of the order afterwards
/// (a refunded or cancelled sale still counts). Open orders do not count.
/// `exclude_order` leaves one order out, so an order being paid is not
/// counted against itself. Totals are converted to euro cents with the
/// persisted reference rates, per currency, rounding up.
pub async fn usage(
    conn: &mut PgConnection,
    seller_pubky: &str,
    from: DateTime<Utc>,
    scope: Scope,
    exclude_order: Option<Uuid>,
    now: DateTime<Utc>,
) -> Result<Usage, CommandFailure> {
    let rows: Vec<(String, i32, i64, i64)> = sqlx::query_as(
        "SELECT o.currency, o.exponent, COUNT(*)::bigint, COALESCE(SUM(o.total_minor), 0)::bigint \
         FROM orders o \
         WHERE o.seller_pubky = $1 \
           AND ($3 = false OR o.buyer_country = ANY($4)) \
           AND ($5::uuid IS NULL OR o.id <> $5) \
           AND EXISTS ( \
                SELECT 1 FROM payments p \
                JOIN events e ON e.aggregate_id = 'payment:' || p.id::text \
                 AND e.kind = 'payment.confirmed' \
                WHERE p.order_id = o.id AND e.occurred_at >= $2) \
         GROUP BY o.currency, o.exponent",
    )
    .bind(seller_pubky)
    .bind(from)
    .bind(scope == Scope::EuBuyers)
    .bind(
        EU_27
            .iter()
            .map(|code| code.to_string())
            .collect::<Vec<_>>(),
    )
    .bind(exclude_order)
    .fetch_all(&mut *conn)
    .await
    .map_err(|error| {
        tracing::error!(%error, "eu fence could not count the seller's orders");
        unavailable()
    })?;
    let mut total = Usage::default();
    for (code, exponent, sales, minor) in rows {
        total.sales += sales;
        total.eur_cents = total
            .eur_cents
            .saturating_add(eur_cents(&mut *conn, &code, exponent, minor, now).await?);
    }
    Ok(total)
}

async fn eur_cents(
    conn: &mut PgConnection,
    code: &str,
    exponent: i32,
    total_minor: i64,
    now: DateTime<Utc>,
) -> Result<u64, CommandFailure> {
    let Some(currency) = Currency::for_money(code, exponent) else {
        tracing::error!(
            currency = code,
            exponent,
            "eu fence met an unknown order currency"
        );
        return Err(unavailable());
    };
    crate::fx::eur_cents_reference(conn, currency, exponent, total_minor, now)
        .await
        .map_err(|error| {
            tracing::error!(
                ?error,
                currency = code,
                "eu fence could not convert to euro"
            );
            unavailable()
        })
}

/// Applies C4 for one seller against `buyer_country`. `order` is the order
/// being created; `None` asks whether the seller can take a sale at all (an
/// offer or a bid). `Err` carries the refusal the caller returns as is.
pub async fn check_limits(
    conn: &mut PgConnection,
    modes: &RegionModes,
    seller_pubky: &str,
    facts: &SellerFacts,
    buyer_country: Option<&str>,
    order: Option<NewOrder<'_>>,
    now: DateTime<Utc>,
) -> Result<(), CommandFailure> {
    let annual_limit = modes.annual_limit_applies(facts.country.as_deref());
    let reconfirmation = facts.status == SellerStatus::Private
        && buyer_country.is_some_and(|country| modes.reconfirmation_applies(country));
    if !annual_limit && !reconfirmation {
        return Ok(());
    }
    let new_cents = match &order {
        Some(order) => {
            eur_cents(
                &mut *conn,
                order.currency,
                order.exponent,
                order.total_minor,
                now,
            )
            .await?
        }
        None => 1,
    };
    if annual_limit {
        let annual = usage(
            conn,
            seller_pubky,
            year_start(now),
            Scope::AllBuyers,
            None,
            now,
        )
        .await?;
        if let Some(reason) = exceeded_by(&annual, new_cents) {
            return Err(limit_refusal(reason));
        }
    }
    if reconfirmation {
        let from = facts
            .status_confirmed_at
            .map_or(year_start(now), |confirmed| confirmed.max(year_start(now)));
        let eu_sales = usage(conn, seller_pubky, from, Scope::EuBuyers, None, now).await?;
        if exceeded_by(&eu_sales, new_cents).is_some() {
            return Err(region_refusal(REASON_RECONFIRM, RECONFIRM_MESSAGE));
        }
    }
    Ok(())
}

/// The seller limits where money is taken (legal launch plan C4, LG15 and
/// LG18): at payment start, so a buyer is not asked to pay a seller who cannot
/// take the sale, and at payment confirmation, so a payment that would cross
/// a limit is never marked paid. Two rules are checked:
///
/// - **The annual limit** of an EU-resident seller (29 sales or EUR 2,000):
///   `SELLER_ANNUAL_LIMIT_REACHED`.
/// - **The re-confirmation point** of a private seller selling to a buyer in
///   the EU-27 (29 sales or EUR 2,000 to EU buyers since their last
///   re-confirmation): `SELLER_UNAVAILABLE_IN_REGION` with reason
///   `private_seller_reconfirmation_required`.
///
/// Nothing reserves a slot before payment, so several unpaid orders and
/// several payments in flight can each pass the order-time check; this is the
/// check that decides. It counts the seller's paid orders other than
/// `order_id`, under the same per-seller advisory lock the order-time check
/// takes, so two payments confirming at once cannot both take the last slot.
///
/// Every fact comes from the order, frozen at its payment start with the
/// seller's declaration, not from a fresh shop read: a payment is never
/// coupled to the homeserver being reachable. `orders.seller_country` is the
/// residence (none means no residence and no cap, as at order time);
/// `orders.seller_status` and `orders.buyer_country` decide whether the
/// re-confirmation point applies; `orders.seller_status_confirmed_at` is where
/// its count starts (the start of the year when none). A seller who
/// re-confirms after an order's payment started therefore does not release
/// that payment, but the next order's payment starts from the new date.
///
/// `Err` is the refusal for the rule that failed. At payment start the caller
/// returns it and the buyer never pays. At confirmation the money is already
/// on its way (the marketplace never holds funds), so every confirming path
/// treats it like any other order that can no longer be confirmed: the payment
/// goes to `manual_review`, nothing is marked paid, no receipt is issued and
/// the seller and ops decide the refund. [`is_limit_at_payment`] names these
/// refusals for those paths.
pub async fn recheck_at_payment(
    tx: &mut Transaction<'_, Postgres>,
    modes: &RegionModes,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), CommandFailure> {
    let row: Option<OrderToPay> = sqlx::query_as(
        "SELECT seller_pubky, seller_country, seller_status, buyer_country, \
                seller_status_confirmed_at, total_minor, currency, exponent \
         FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        tracing::error!(%error, "eu fence could not read the order to pay");
        unavailable()
    })?;
    let Some(order) = row else {
        return Err(unavailable());
    };
    let annual_limit = modes.annual_limit_applies(order.seller_country.as_deref());
    let reconfirmation = order.seller_status.as_deref() == Some(SellerStatus::Private.as_str())
        && order
            .buyer_country
            .as_deref()
            .is_some_and(|country| modes.reconfirmation_applies(country));
    if !annual_limit && !reconfirmation {
        return Ok(());
    }
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(&order.seller_pubky)
        .bind(SELLER_LOCK_SEED)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            tracing::error!(%error, "eu fence could not lock the seller's counters");
            unavailable()
        })?;
    let new_cents = eur_cents(tx, &order.currency, order.exponent, order.total_minor, now).await?;
    let refusal = if annual_limit {
        let annual = usage(
            tx,
            &order.seller_pubky,
            year_start(now),
            Scope::AllBuyers,
            Some(order_id),
            now,
        )
        .await?;
        exceeded_by(&annual, new_cents).map(|reason| (limit_refusal(reason), reason, annual))
    } else {
        None
    };
    let refusal = match refusal {
        Some(refusal) => Some(refusal),
        None if reconfirmation => {
            let from = order
                .seller_status_confirmed_at
                .map_or(year_start(now), |confirmed| confirmed.max(year_start(now)));
            let eu_sales = usage(
                tx,
                &order.seller_pubky,
                from,
                Scope::EuBuyers,
                Some(order_id),
                now,
            )
            .await?;
            exceeded_by(&eu_sales, new_cents).map(|_| {
                (
                    region_refusal(REASON_RECONFIRM, RECONFIRM_MESSAGE),
                    REASON_RECONFIRM,
                    eu_sales,
                )
            })
        }
        None => None,
    };
    match refusal {
        None => Ok(()),
        Some((failure, reason, counted)) => {
            tracing::error!(
                order_id = %order_id,
                seller_pubky = %order.seller_pubky,
                reason,
                paid_sales = counted.sales,
                paid_eur_cents = counted.eur_cents,
                "ALERT eu_seller_limit_at_payment: a payment would cross an EU seller limit; it \
                 is refused at payment start or sent to manual review at confirmation \
                 (order_id={order_id} seller_pubky={})",
                order.seller_pubky
            );
            Err(failure)
        }
    }
}

/// The facts of an order about to be paid that the seller limits turn on.
#[derive(sqlx::FromRow)]
struct OrderToPay {
    seller_pubky: String,
    seller_country: Option<String>,
    seller_status: Option<String>,
    buyer_country: Option<String>,
    seller_status_confirmed_at: Option<DateTime<Utc>>,
    total_minor: i64,
    currency: String,
    exponent: i32,
}

/// Whether `failure` is a refusal [`recheck_at_payment`] gives: the annual
/// limit or the private seller's re-confirmation point. The paths that
/// confirm a payment route such a refusal to manual review (or refund
/// required) instead of failing the money they already hold.
pub fn is_limit_at_payment(failure: &CommandFailure) -> bool {
    matches!(
        failure.code(),
        ErrorCode::SellerAnnualLimitReached | ErrorCode::SellerUnavailableInRegion
    )
}

/// Which limit one more sale of `new_cents` would cross, if either.
fn exceeded_by(usage: &Usage, new_cents: u64) -> Option<&'static str> {
    if usage.sales + 1 > ANNUAL_SALES_LIMIT {
        Some(REASON_SALES_LIMIT)
    } else if usage.eur_cents.saturating_add(new_cents) > ANNUAL_EUR_LIMIT_CENTS {
        Some(REASON_AMOUNT_LIMIT)
    } else {
        None
    }
}

/// `GET /v1/sellers/{seller_pubky}/annual-limit`: what the annual limit
/// (C4) says about one seller, for the Shop's seller settings and listing
/// pages. Any signed-in actor may ask and learns `limit_reached` (the seller
/// cannot take another sale from any buyer). The counters, the residence and
/// `reconfirmation_required` are the seller's own and shown to nobody else.
/// With the fence off the answer is `{"enabled": false}` and no shop record
/// is read.
pub async fn get_annual_limit(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Extension(actor): axum::Extension<crate::auth::Actor>,
    axum::extract::Path(seller_pubky): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use serde_json::json;

    let reply = |status: StatusCode, body: Value| (status, axum::Json(body)).into_response();
    let failure = |failure: CommandFailure| {
        reply(
            StatusCode::from_u16(failure.http_status()).expect("error codes map to statuses"),
            failure.body(),
        )
    };
    if marketplace_domain::commands::validate_actor(&seller_pubky).is_err() {
        return failure(CommandFailure::refused(
            RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The seller was not found.",
        ));
    }
    let Some(modes) = state.config.region_policy().fence_modes() else {
        return reply(StatusCode::OK, json!({ "enabled": false }));
    };
    let now = state.clock.now();
    let Some(homeserver) = state.homeserver.clone() else {
        return failure(unavailable());
    };
    let facts = match read_sellers(homeserver, std::slice::from_ref(&seller_pubky), now).await {
        Ok(mut facts) => facts
            .remove(&seller_pubky)
            .unwrap_or_else(SellerFacts::undeclared),
        Err(refusal) => return failure(refusal),
    };
    let own = actor.0 == seller_pubky;
    let mut conn = match state.pool.acquire().await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "eu fence could not read seller counters");
            return failure(unavailable());
        }
    };
    let year = year_start(now);
    let resident_in_eu = modes.annual_limit_applies(facts.country.as_deref());
    let annual = if resident_in_eu {
        match usage(&mut conn, &seller_pubky, year, Scope::AllBuyers, None, now).await {
            Ok(annual) => Some(annual),
            Err(refusal) => return failure(refusal),
        }
    } else {
        None
    };
    let limit_reached = annual.is_some_and(|annual| {
        annual.sales >= ANNUAL_SALES_LIMIT || annual.eur_cents >= ANNUAL_EUR_LIMIT_CENTS
    });
    let mut body = json!({
        "enabled": true,
        "year": now.year(),
        "limit_reached": limit_reached,
    });
    if own {
        body["resident_in_eu"] = json!(resident_in_eu);
        body["sales_limit"] = json!(ANNUAL_SALES_LIMIT);
        body["eur_limit_cents"] = json!(ANNUAL_EUR_LIMIT_CENTS);
        if let Some(annual) = annual {
            body["sales"] = json!(annual.sales);
            body["eur_cents"] = json!(annual.eur_cents);
        }
        if facts.status == SellerStatus::Private {
            let from = facts
                .status_confirmed_at
                .map_or(year, |confirmed| confirmed.max(year));
            match usage(&mut conn, &seller_pubky, from, Scope::EuBuyers, None, now).await {
                Ok(eu) => {
                    body["eu_buyer_sales"] = json!(eu.sales);
                    body["eu_buyer_eur_cents"] = json!(eu.eur_cents);
                    body["reconfirmation_required"] = json!(
                        eu.sales >= ANNUAL_SALES_LIMIT || eu.eur_cents >= ANNUAL_EUR_LIMIT_CENTS
                    );
                }
                Err(refusal) => return failure(refusal),
            }
        }
    }
    reply(StatusCode::OK, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn year_start_is_january_first_utc() {
        assert_eq!(
            year_start(at("2026-10-23T13:45:00Z")),
            at("2026-01-01T00:00:00Z")
        );
        assert_eq!(
            year_start(at("2026-12-31T23:59:59Z")),
            at("2026-01-01T00:00:00Z")
        );
        assert_eq!(
            year_start(at("2027-01-01T00:00:00Z")),
            at("2027-01-01T00:00:00Z")
        );
    }

    #[test]
    fn facts_read_status_country_and_a_past_confirmation() {
        let now = at("2026-10-23T12:00:00Z");
        let facts = SellerFacts::from_shop_record(
            &json!({
                "sellerStatus": "private",
                "location": { "countryCode": "DE" },
                "sellerStatusConfirmedAt": "2026-10-01T08:00:00.000Z",
            }),
            now,
        );
        assert_eq!(facts.status, SellerStatus::Private);
        assert_eq!(facts.country.as_deref(), Some("DE"));
        assert_eq!(facts.status_confirmed_at, Some(at("2026-10-01T08:00:00Z")));
        assert!(RegionModes::legacy_nuanced().annual_limit_applies(facts.country.as_deref()));
    }

    #[test]
    fn a_confirmation_from_the_future_or_a_bad_timestamp_reads_as_absent() {
        let now = at("2026-10-23T12:00:00Z");
        for value in [json!("2026-10-24T00:00:00Z"), json!("yesterday"), json!(5)] {
            let facts = SellerFacts::from_shop_record(
                &json!({ "sellerStatus": "private", "sellerStatusConfirmedAt": value }),
                now,
            );
            assert_eq!(facts.status_confirmed_at, None, "{value}");
        }
    }

    #[test]
    fn a_record_with_nothing_is_undeclared_and_not_resident() {
        let facts = SellerFacts::from_shop_record(&json!({}), at("2026-10-23T12:00:00Z"));
        assert_eq!(facts, SellerFacts::undeclared());
        assert!(!RegionModes::legacy_nuanced().annual_limit_applies(facts.country.as_deref()));
    }

    #[test]
    fn only_a_private_seller_is_open_to_the_eu_and_gb() {
        let private = SellerFacts {
            status: SellerStatus::Private,
            ..SellerFacts::undeclared()
        };
        let trader = SellerFacts {
            status: SellerStatus::Trader,
            ..SellerFacts::undeclared()
        };
        let undeclared = SellerFacts::undeclared();
        let modes = RegionModes::legacy_nuanced();
        assert!(region_verdict(&modes, &private, "DE").is_none());
        assert_eq!(
            region_verdict(&modes, &trader, "DE").and_then(|failure| failure.reason()),
            Some(REASON_BUSINESS)
        );
        assert_eq!(
            region_verdict(&modes, &undeclared, "FR").and_then(|failure| failure.reason()),
            Some(REASON_UNDECLARED)
        );
        for country in ["GB", "IE"] {
            assert!(
                region_verdict(&modes, &private, country).is_none(),
                "{country}"
            );
            assert_eq!(
                region_verdict(&modes, &trader, country).and_then(|failure| failure.reason()),
                Some(REASON_BUSINESS),
                "{country}"
            );
            assert_eq!(
                region_verdict(&modes, &undeclared, country).and_then(|failure| failure.reason()),
                Some(REASON_UNDECLARED),
                "{country}"
            );
        }
        for outside in ["CH", "US", "NO", "BR", "UK"] {
            assert!(
                region_verdict(&modes, &trader, outside).is_none(),
                "{outside}"
            );
            assert!(
                region_verdict(&modes, &undeclared, outside).is_none(),
                "{outside}"
            );
        }
    }

    #[test]
    fn c3_follows_the_modes_not_a_fixed_country_list() {
        let trader = SellerFacts {
            status: SellerStatus::Trader,
            ..SellerFacts::undeclared()
        };
        let modes = RegionModes::parse("EU=nuanced,DE=open,US=nuanced,GB=blocked").unwrap();
        assert!(region_verdict(&modes, &trader, "FR").is_some());
        assert!(region_verdict(&modes, &trader, "US").is_some());
        assert!(
            region_verdict(&modes, &trader, "DE").is_none(),
            "an open member state buys from anyone"
        );
        assert!(
            region_verdict(&modes, &trader, "GB").is_none(),
            "a blocked country is refused earlier, by the commerce gate"
        );
        assert!(region_verdict(&RegionModes::default(), &trader, "FR").is_none());
    }

    #[test]
    fn a_command_carries_a_buyer_country_only_when_it_sells() {
        let raw = json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000000500",
            "aggregate_id": format!("listing:{}_boots_01", "y".repeat(52)),
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "auction.place_bid",
            "payload": {
                "maximum_amount": { "amount_minor": 10_000, "currency": "USD", "exponent": 2 },
                "buyer_country": "DE",
            },
        });
        let bid = marketplace_domain::commands::parse_command(&raw).expect("bid");
        assert_eq!(buyer_country_of(&bid.payload), Some(Some("DE")));
        let mut without = raw.clone();
        without["payload"]
            .as_object_mut()
            .expect("payload")
            .remove("buyer_country");
        let bid = marketplace_domain::commands::parse_command(&without).expect("bid");
        assert_eq!(buyer_country_of(&bid.payload), Some(None));
        let mut close = raw;
        close["kind"] = json!("auction.close");
        close["payload"] = json!({});
        let close = marketplace_domain::commands::parse_command(&close).expect("close");
        assert_eq!(buyer_country_of(&close.payload), None);
    }
}
