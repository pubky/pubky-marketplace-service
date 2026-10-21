//! Review commands. `review.create` is ported from the TypeScript prototype
//! engine and rates each listing of an order: a buyer's review is one row
//! and one purchase attestation per reviewed listing, all sharing the
//! order's `order_ref` (v1 terms rate the first line); a seller's review of
//! the buyer is one row per order, keyed and attested to the first line.
//! Uniqueness is enforced by the `reviews_one_per_order_role_listing`
//! database constraint rather than application logic: the rows are inserted
//! before the order's revision compare-and-swap, so two submissions racing
//! on the same item are decided by the constraint, and two racing on
//! different items by the compare-and-swap. `review.update` is this service
//! only. A buyer may add items, and either participant may revise, within a
//! bounded window of their first review of the order.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{ReviewTerms, ReviewTermsPayload, SellerRatingTerms};
use marketplace_domain::state_machines::{can_transition, order_machine};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::attestor::{amount_band, claim_role, Attestor};
use crate::executor::insert_event;
use crate::handlers::{
    fetch_order, fetch_order_for_update, fetch_order_reviews, guard_order_action,
    insert_notification_intent, order_json_with_reviews, REVIEW_COLUMNS,
};
use crate::model::{OrderRow, ReviewRow};
use crate::queries::ORDER_COLUMNS;
use crate::refusal_audit::RefusalKind;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// How long a reviewer may add items to and edit their review of an order,
/// counted from their first review of it. This service only — the
/// prototype had no review editing, so the bound is a deliberate policy
/// choice documented in the README.
pub const REVIEW_EDIT_WINDOW_SECONDS: i64 = 24 * 60 * 60;

/// Order states eligible for review (prototype engine table).
const REVIEWABLE_STATES: [&str; 3] = ["delivered", "completed", "closed"];

/// One distinct listing of an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderListing {
    /// `listing:{seller}_{listing_id}`, as on the order line.
    pub aggregate_id: String,
    /// The seller's record id, as review items name it.
    pub listing_id: String,
    /// The canonical public listing URI the purchase attestation binds.
    pub uri: String,
}

/// The order's distinct listings in line order. Every order line carries
/// `listing_aggregate_id` in the `listing:{seller}_{listing_id}` format,
/// and the seller is the order's seller by construction (checkout groups
/// lines per seller).
pub fn order_listings(order: &OrderRow) -> Vec<OrderListing> {
    let prefix = format!("listing:{}_", order.seller_pubky);
    let mut listings: Vec<OrderListing> = Vec::new();
    for line in order.lines.as_array().expect("order lines are an array") {
        let aggregate_id = line["listing_aggregate_id"]
            .as_str()
            .expect("order lines always carry a listing aggregate id");
        if listings
            .iter()
            .any(|listing| listing.aggregate_id == aggregate_id)
        {
            continue;
        }
        let listing_id = aggregate_id
            .strip_prefix(&prefix)
            .expect("listing aggregate ids follow the listing:{seller}_{id} format");
        listings.push(OrderListing {
            aggregate_id: aggregate_id.to_string(),
            listing_id: listing_id.to_string(),
            uri: format!(
                "pubky://{}/pub/pubky.app/marketplace/v1/listings/{}",
                order.seller_pubky, listing_id
            ),
        });
    }
    listings
}

fn line_position(listings: &[OrderListing], aggregate_id: &str) -> usize {
    listings
        .iter()
        .position(|listing| listing.aggregate_id == aggregate_id)
        .unwrap_or(listings.len())
}

/// One listing a command rates.
struct ItemTerms<'a> {
    listing: &'a OrderListing,
    rating: i32,
    text: &'a str,
}

/// The command's terms resolved to the order's listings, in line order, and
/// the buyer's seller ratings. v1 terms rate the first line.
fn resolve_terms<'a>(
    listings: &'a [OrderListing],
    role: &str,
    terms: ReviewTerms<'a>,
) -> Result<(Vec<ItemTerms<'a>>, Option<SellerRatingTerms>), CommandFailure> {
    let (items, seller) = match terms {
        ReviewTerms::Order { rating, text } => {
            let first = listings.first().expect("every order has a line");
            return Ok((
                vec![ItemTerms {
                    listing: first,
                    rating: i32::try_from(rating).expect("rating validated to 1..=5"),
                    text,
                }],
                None,
            ));
        }
        ReviewTerms::Items { items, seller } => (items, seller),
    };
    if role == "seller" {
        return Err(CommandFailure::refused(
            RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "A seller reviews the buyer once per order, with a rating and text.",
        ));
    }
    let mut resolved = Vec::with_capacity(items.len());
    for item in items {
        let Some(listing) = listings
            .iter()
            .find(|listing| listing.listing_id == item.listing_id)
        else {
            return Err(CommandFailure::refused(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                &format!("Listing {} is not part of this order.", item.listing_id),
            ));
        };
        resolved.push(ItemTerms {
            listing,
            rating: i32::try_from(item.rating).expect("rating validated to 1..=5"),
            text: &item.text,
        });
    }
    resolved.sort_by_key(|item| line_position(listings, &item.listing.aggregate_id));
    Ok((resolved, seller))
}

fn seller_ratings(seller: SellerRatingTerms) -> (i32, i32) {
    (
        i32::try_from(seller.shipping).expect("rating validated to 1..=5"),
        i32::try_from(seller.communication).expect("rating validated to 1..=5"),
    )
}

/// The reviewer's rows for the order, in line order.
async fn reviewer_reviews(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    reviewer_pubky: &str,
    listings: &[OrderListing],
    for_update: bool,
) -> Result<Vec<ReviewRow>, sqlx::Error> {
    let lock = if for_update { " FOR UPDATE" } else { "" };
    let mut rows: Vec<ReviewRow> = sqlx::query_as(&format!(
        "SELECT {REVIEW_COLUMNS} FROM reviews \
         WHERE order_id = $1 AND reviewer_pubky = $2 ORDER BY created_at, id{lock}"
    ))
    .bind(order_id)
    .bind(reviewer_pubky)
    .fetch_all(&mut **tx)
    .await?;
    rows.sort_by_key(|row| line_position(listings, &row.listing_aggregate_id));
    Ok(rows)
}

/// Refuses once the reviewer's window, opened by their first review of the
/// order, has closed.
fn window_closed(existing: &[ReviewRow], now: DateTime<Utc>) -> Option<CommandFailure> {
    let opened = existing.iter().map(|row| row.created_at).min()?;
    (now > opened + chrono::Duration::seconds(REVIEW_EDIT_WINDOW_SECONDS)).then(|| {
        CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The review edit window has closed.",
        )
    })
}

/// Writes the seller ratings onto every one of the reviewer's rows of the
/// order that differs, returning the ids it changed.
async fn write_seller_ratings(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    reviewer_pubky: &str,
    (shipping, communication): (i32, i32),
    now: DateTime<Utc>,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "UPDATE reviews SET shipping = $3, communication = $4, updated_at = $5 \
         WHERE order_id = $1 AND reviewer_pubky = $2 \
           AND (shipping IS DISTINCT FROM $3 OR communication IS DISTINCT FROM $4) \
         RETURNING id",
    )
    .bind(order_id)
    .bind(reviewer_pubky)
    .bind(shipping)
    .bind(communication)
    .bind(now)
    .fetch_all(&mut **tx)
    .await
}

pub async fn create(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ReviewTermsPayload,
    attestor: Option<&Attestor>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Deliberately no FOR UPDATE: the review inserts happen before the
    // order's compare-and-swap, so two same-reviewer commands racing past
    // this read are decided by the unique constraint (same item) or the
    // compare-and-swap (different items).
    let Some(order) = fetch_order(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if !REVIEWABLE_STATES.contains(&order.state.as_str()) {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The order is not eligible for review.",
        )));
    }
    let (role, subject) = if actor == order.buyer_pubky {
        ("buyer", order.seller_pubky.clone())
    } else {
        ("seller", order.buyer_pubky.clone())
    };
    let listings = order_listings(&order);
    let (items, seller) = match resolve_terms(&listings, role, payload.terms()) {
        Ok(resolved) => resolved,
        Err(failure) => return Ok(Err(failure)),
    };

    let existing = reviewer_reviews(tx, order.id, actor, &listings, false).await?;
    if let Some(item) = items.iter().find(|item| {
        existing
            .iter()
            .any(|row| row.listing_aggregate_id == item.listing.aggregate_id)
    }) {
        let message = match payload.terms() {
            ReviewTerms::Order { .. } => "This participant already reviewed the order.".to_string(),
            ReviewTerms::Items { .. } => format!(
                "This participant already reviewed listing {}.",
                item.listing.listing_id
            ),
        };
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            &message,
        )));
    }
    if let Some(failure) = window_closed(&existing, now) {
        return Ok(Err(failure));
    }

    // Every item review of the order carries the same seller ratings: new
    // ones from this command, otherwise the ones already on the order.
    let ratings = seller.map(seller_ratings).or_else(|| {
        existing
            .first()
            .and_then(|row| row.shipping.zip(row.communication))
    });
    let mut written: Vec<Uuid> = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let id = if index == 0 {
            command.command_id
        } else {
            Uuid::new_v4()
        };
        sqlx::query(
            "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
             listing_aggregate_id, rating, text, shipping, communication, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)",
        )
        .bind(id)
        .bind(order.id)
        .bind(actor)
        .bind(role)
        .bind(&subject)
        .bind(&item.listing.aggregate_id)
        .bind(item.rating)
        .bind(item.text)
        .bind(ratings.map(|(shipping, _)| shipping))
        .bind(ratings.map(|(_, communication)| communication))
        .bind(now)
        .execute(&mut **tx)
        .await?;
        written.push(id);
    }
    if let Some(ratings) = seller.map(seller_ratings) {
        written.extend(write_seller_ratings(tx, order.id, actor, ratings, now).await?);
    }

    // Purchase attestations (ADR 0024): one per reviewed listing, issued
    // inside this same transaction, stored append-only for idempotent
    // re-fetch, and returned in the command result for the client to embed
    // in each published record.
    if let Some(attestor) = attestor {
        let context =
            AttestationContext::for_order(tx, &order, payload.allow_amount_band, now).await?;
        for item in &items {
            issue_attestation(
                tx,
                attestor,
                &context,
                &order,
                actor,
                &subject,
                role,
                item.listing,
                now,
            )
            .await?;
        }
    }

    #[cfg(feature = "test-faults")]
    test_faults::after_review_insert(order.id).await;

    let new_state = if order.state == "delivered" {
        "completed"
    } else {
        order.state.as_str()
    };
    debug_assert!(can_transition(&order_machine(), &order.state, new_state));
    let updated: Option<OrderRow> = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, updated_at = $4 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(new_state)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(updated) = updated else {
        let current: (i64,) = sqlx::query_as("SELECT revision FROM orders WHERE id = $1")
            .bind(order.id)
            .fetch_one(&mut **tx)
            .await?;
        return Ok(Err(CommandFailure::refused_with_revision(
            RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The order revision is stale.",
            current.0,
        )));
    };

    let order_aggregate_id = ids::order_aggregate_id(updated.id);
    let event_id = insert_event(
        tx,
        command.command_id,
        &order_aggregate_id,
        updated.revision,
        actor,
        "review.created",
        now,
    )
    .await?;
    // One notification per review action, however many items it rates.
    insert_notification_intent(
        tx,
        event_id,
        "review_received",
        &subject,
        actor,
        &order_aggregate_id,
        None,
        now,
    )
    .await?;
    let result = review_result(tx, &updated, actor, &listings, &written).await?;
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result,
    }))
}

/// The command result of a review action: the order with every review, the
/// rows this command wrote (`review` is the first of them in line order),
/// their stored attestations, and the reviewer's first-line attestation as
/// `attestation`, the field single-listing clients read. Both attestation
/// fields are absent when nothing was attested (no attestor configured).
async fn review_result(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    actor: &str,
    listings: &[OrderListing],
    written: &[Uuid],
) -> Result<Value, sqlx::Error> {
    let reviews = fetch_order_reviews(tx, order.id).await?;
    let mut own: Vec<&ReviewRow> = reviews
        .iter()
        .filter(|row| row.reviewer_pubky == actor && written.contains(&row.id))
        .collect();
    own.sort_by_key(|row| line_position(listings, &row.listing_aggregate_id));
    let mut result = json!({
        "kind": "review",
        "order": order_json_with_reviews(order, &reviews),
        "review": own.first().map(|row| row.view()),
        "reviews": own.iter().map(|row| row.view()).collect::<Vec<_>>(),
    });
    let stored = fetch_attestations(&mut **tx, order, actor).await?;
    let attestations: Vec<Value> = own
        .iter()
        .filter_map(|row| {
            stored
                .iter()
                .find(|(listing, _)| listing.aggregate_id == row.listing_aggregate_id)
                .map(|(_, attestation)| attestation.clone())
        })
        .collect();
    if !attestations.is_empty() {
        result["attestations"] = Value::Array(attestations);
    }
    if let Some((_, first_line)) = stored
        .iter()
        .find(|(listing, _)| Some(listing) == listings.first())
    {
        result["attestation"] = json!({
            "jws": first_line["jws"],
            "claims": first_line["claims"],
        });
    }
    Ok(result)
}

/// Day-granularity completion date and the D2 amount band, shared by every
/// attestation one command issues.
struct AttestationContext {
    completed_on: String,
    band: Option<String>,
}

impl AttestationContext {
    async fn for_order(
        tx: &mut Transaction<'_, Postgres>,
        order: &OrderRow,
        reviewer_allows_band: bool,
        now: DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        // The delivery confirmation when the order was delivered, otherwise
        // the issuance day.
        let delivered: Option<(DateTime<Utc>,)> = sqlx::query_as(
            "SELECT occurred_at FROM events WHERE aggregate_id = $1 AND kind = 'fulfillment.delivered' \
             ORDER BY occurred_at DESC LIMIT 1",
        )
        .bind(ids::order_aggregate_id(order.id))
        .fetch_optional(&mut **tx)
        .await?;
        let completed_on = delivered
            .map(|(occurred_at,)| occurred_at)
            .unwrap_or(now)
            .format("%Y-%m-%d")
            .to_string();

        // D2 both-sides consent: the band is included only when the seller's
        // standing preference allows it AND the reviewer opted in.
        let seller_allows: Option<(bool,)> = sqlx::query_as(
            "SELECT allows_amount_band FROM attestation_band_consents WHERE seller_pubky = $1",
        )
        .bind(&order.seller_pubky)
        .fetch_optional(&mut **tx)
        .await?;
        let band = if reviewer_allows_band && seller_allows.map(|(allows,)| allows).unwrap_or(false)
        {
            amount_band(&order.currency, order.total_minor)
        } else {
            None
        };
        Ok(Self { completed_on, band })
    }
}

/// Issues and stores the purchase attestation for one (order, reviewer,
/// listing).
#[allow(clippy::too_many_arguments)]
async fn issue_attestation(
    tx: &mut Transaction<'_, Postgres>,
    attestor: &Attestor,
    context: &AttestationContext,
    order: &OrderRow,
    reviewer: &str,
    subject: &str,
    role: &str,
    listing: &OrderListing,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let issued = attestor.issue_purchase_attestation(
        order.id,
        reviewer,
        subject,
        claim_role(role),
        &listing.uri,
        &context.completed_on,
        context.band.clone(),
        now,
    );
    sqlx::query(
        "INSERT INTO review_attestations \
         (order_id, reviewer_pubky, listing_uri, order_ref, jws, claims, issued_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(order.id)
    .bind(reviewer)
    .bind(&listing.uri)
    .bind(&issued.order_ref)
    .bind(&issued.jws)
    .bind(&issued.claims)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The reviewer's stored attestations for the order in line order, each as
/// the `{ listing_id, jws, claims }` object results and reads return.
pub async fn fetch_attestations<'e, E>(
    executor: E,
    order: &OrderRow,
    reviewer_pubky: &str,
) -> Result<Vec<(OrderListing, Value)>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    let rows: Vec<(String, String, Value)> = sqlx::query_as(
        "SELECT listing_uri, jws, claims FROM review_attestations \
         WHERE order_id = $1 AND reviewer_pubky = $2",
    )
    .bind(order.id)
    .bind(reviewer_pubky)
    .fetch_all(executor)
    .await?;
    let listings = order_listings(order);
    let mut attestations: Vec<(OrderListing, Value)> = rows
        .into_iter()
        .filter_map(|(uri, jws, claims)| {
            let listing = listings.iter().find(|listing| listing.uri == uri)?;
            Some((
                listing.clone(),
                json!({ "listing_id": listing.listing_id, "jws": jws, "claims": claims }),
            ))
        })
        .collect();
    attestations.sort_by_key(|(listing, _)| line_position(&listings, &listing.aggregate_id));
    Ok(attestations)
}

pub async fn update(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ReviewTermsPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    let role = if actor == order.buyer_pubky {
        "buyer"
    } else {
        "seller"
    };
    let listings = order_listings(&order);
    let (items, seller) = match resolve_terms(&listings, role, payload.terms()) {
        Ok(resolved) => resolved,
        Err(failure) => return Ok(Err(failure)),
    };
    let existing = reviewer_reviews(tx, order.id, actor, &listings, true).await?;
    let mut targets: Vec<(&ReviewRow, &ItemTerms<'_>)> = Vec::with_capacity(items.len());
    for item in &items {
        let Some(row) = existing
            .iter()
            .find(|row| row.listing_aggregate_id == item.listing.aggregate_id)
        else {
            let message = if matches!(payload.terms(), ReviewTerms::Order { .. }) {
                "This participant has not reviewed the order.".to_string()
            } else {
                format!(
                    "This participant has not reviewed listing {}.",
                    item.listing.listing_id
                )
            };
            return Ok(Err(CommandFailure::refused(
                RefusalKind::NotFound,
                ErrorCode::NotFound,
                &message,
            )));
        };
        targets.push((row, item));
    }
    if let Some(failure) = window_closed(&existing, now) {
        return Ok(Err(failure));
    }

    let mut written: Vec<Uuid> = Vec::with_capacity(targets.len());
    for (row, item) in &targets {
        sqlx::query("UPDATE reviews SET rating = $2, text = $3, updated_at = $4 WHERE id = $1")
            .bind(row.id)
            .bind(item.rating)
            .bind(item.text)
            .bind(now)
            .execute(&mut **tx)
            .await?;
        written.push(row.id);
    }
    if let Some(ratings) = seller.map(seller_ratings) {
        written.extend(write_seller_ratings(tx, order.id, actor, ratings, now).await?);
    }
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &ids::order_aggregate_id(updated.id),
        updated.revision,
        actor,
        "review.updated",
        now,
    )
    .await?;
    // The attestations are unchanged by edits (they attest the purchase,
    // not the text); they are echoed back so the client can republish the
    // revised records without a second read.
    let result = review_result(tx, &updated, actor, &listings, &written).await?;
    // No notification: the prototype emitted none (it had no review edits).
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result,
    }))
}

/// Test-only fault hooks for `review.create`. Compiled out in release
/// builds: the module exists only under the `test-faults` cargo feature,
/// which is enabled solely for integration tests (the crate's
/// dev-dependency on itself).
#[cfg(feature = "test-faults")]
pub mod test_faults {
    use std::sync::{Arc, Mutex};
    use tokio::sync::Barrier;

    /// A pause between a create's inserts and its compare-and-swap: the
    /// command waits at `arrived`, then at `release`.
    #[derive(Clone)]
    pub struct InsertGate {
        pub arrived: Arc<Barrier>,
        pub release: Arc<Barrier>,
    }

    static GATES: Mutex<Vec<(uuid::Uuid, InsertGate)>> = Mutex::new(Vec::new());

    /// Arms one pause for the next `review.create` on `order_id` that gets
    /// past its inserts. Arm several times to pause several commands.
    pub fn arm_after_insert(order_id: uuid::Uuid, gate: InsertGate) {
        GATES
            .lock()
            .expect("fault hook mutex")
            .push((order_id, gate));
    }

    pub(super) async fn after_review_insert(order_id: uuid::Uuid) {
        let gate = {
            let mut gates = GATES.lock().expect("fault hook mutex");
            gates
                .iter()
                .position(|(armed, _)| *armed == order_id)
                .map(|index| gates.remove(index).1)
        };
        if let Some(gate) = gate {
            gate.arrived.wait().await;
            gate.release.wait().await;
        }
    }
}
