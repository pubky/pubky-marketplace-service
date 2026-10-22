//! Bitcoin money that arrives after the payment window with an amount that
//! does not match the invoice. On-time money with the wrong amount waits in
//! `manual_review` (`review_reason = amount_mismatch`) and never pays; late
//! money must do the same before the late-money fork re-takes the unit, and
//! only an exact late payment may auto-complete a still-free order.

mod common;

use chrono::{DateTime, Utc};
use common::paykit_review::{
    bound_order, poll_now, resolve_call, status_confirmed, OBSERVED_TXID, TOTAL_SATS,
};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::workers::expire_due_payment_windows;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const LATE_AFTER_SECONDS: i64 = 7300;

/// A confirmed, producer-flagged late settlement of `observed_sats`.
fn late_status(amount_matched: bool, observed_sats: i64) -> Value {
    let mut status = bitcoin_status_v2(
        "confirmed",
        amount_matched,
        "exclusive",
        Some(OBSERVED_TXID),
        u64::try_from(observed_sats).ok(),
        Some(2),
    );
    status["late_settlement"] = json!(true);
    status
}

struct Facts {
    order_state: String,
    payment_state: String,
    review_reason: Option<String>,
    stock_held: bool,
    observed_sats: Option<i64>,
}

async fn facts(pool: &PgPool, order_id: &str) -> Facts {
    let (order_state, payment_state, review_reason, stock_held, observed_sats): (
        String,
        String,
        Option<String>,
        bool,
        Option<i64>,
    ) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason, o.stock_held, o.paykit_observed_sats \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order and payment");
    Facts {
        order_state,
        payment_state,
        review_reason,
        stock_held,
        observed_sats,
    }
}

async fn receipts(pool: &PgPool, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM receipts WHERE order_id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(pool)
        .await
        .expect("receipt count")
}

async fn sold_quantity(pool: &PgPool, seller: &TestActor) -> i64 {
    sqlx::query_scalar("SELECT sold_quantity FROM listings WHERE aggregate_id = $1")
        .bind(format!("listing:{}_boots_01", seller.pubky))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

async fn available_quantity(pool: &PgPool, seller: &TestActor) -> i64 {
    sqlx::query_scalar("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
        .bind(format!("listing:{}_boots_01", seller.pubky))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

async fn payment_manual_review_events(pool: &PgPool, order_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p \
         ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = 'payment.manual_review'",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("manual review events")
}

/// Seller notices queued for `order_id` with `review_reason`.
async fn review_notices(pool: &PgPool, order_id: &str, review_reason: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_manual_review' \
         AND payload->>'aggregate_id' = $1 AND payload->>'review_reason' = $2",
    )
    .bind(format!("order:{order_id}"))
    .bind(review_reason)
    .fetch_one(pool)
    .await
    .expect("review notices")
}

/// An exclusive bitcoin order whose payment window has lapsed.
struct Lapsed {
    app: TestApp,
    paykit: FakePaykit,
    seller: TestActor,
    order_id: String,
    reference: String,
    after: DateTime<Utc>,
}

async fn lapsed_order(pool: PgPool) -> Lapsed {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(LATE_AFTER_SECONDS);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let lapsed = facts(&app.pool, &order_id).await;
    assert_eq!(lapsed.order_state, "cancelled");
    assert_eq!(lapsed.payment_state, "expired");
    assert_eq!(available_quantity(&app.pool, &seller).await, 16);
    Lapsed {
        app,
        paykit,
        seller,
        order_id,
        reference,
        after,
    }
}

async fn assert_amount_mismatch_review(lapsed: &Lapsed, observed_sats: i64) {
    let pool = &lapsed.app.pool;
    let facts = facts(pool, &lapsed.order_id).await;
    assert_eq!(facts.payment_state, "manual_review", "payment waits");
    assert_eq!(
        facts.review_reason.as_deref(),
        Some("amount_mismatch"),
        "the review names the amount"
    );
    assert_ne!(facts.order_state, "paid", "a mismatch never completes");
    assert!(!facts.stock_held, "the unit is not re-taken");
    assert_eq!(
        facts.observed_sats,
        Some(observed_sats),
        "the observation is frozen"
    );
    assert_eq!(receipts(pool, &lapsed.order_id).await, 0, "no receipt");
    assert_eq!(sold_quantity(pool, &lapsed.seller).await, 0);
    assert_eq!(
        available_quantity(pool, &lapsed.seller).await,
        16,
        "the unit stays on the listing"
    );
    assert_eq!(
        payment_manual_review_events(pool, &lapsed.order_id).await,
        1
    );
    assert_eq!(
        review_notices(pool, &lapsed.order_id, "amount_mismatch").await,
        1,
        "the seller is told why"
    );
    assert_eq!(
        review_notices(pool, &lapsed.order_id, "late_settlement").await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_underpayment_waits_for_the_seller_instead_of_completing(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    let observed = TOTAL_SATS - 1_000;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(false, observed));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    assert_amount_mismatch_review(&lapsed, observed).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_overpayment_waits_for_the_seller_instead_of_completing(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    let observed = TOTAL_SATS + 1_000;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(false, observed));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    assert_amount_mismatch_review(&lapsed, observed).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_exact_payment_still_completes_the_free_order(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(true, TOTAL_SATS));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    let pool = &lapsed.app.pool;
    let facts = facts(pool, &lapsed.order_id).await;
    assert_eq!(facts.order_state, "paid");
    assert_eq!(facts.payment_state, "confirmed");
    assert!(facts.review_reason.is_none());
    assert_eq!(receipts(pool, &lapsed.order_id).await, 1);
    assert_eq!(sold_quantity(pool, &lapsed.seller).await, 1);
    assert_eq!(available_quantity(pool, &lapsed.seller).await, 15);
    assert_eq!(
        payment_manual_review_events(pool, &lapsed.order_id).await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_mismatch_with_the_unit_gone_keeps_the_amount_review(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{}_boots_01", lapsed.seller.pubky))
    .execute(&lapsed.app.pool)
    .await
    .expect("sell the listing out");
    let observed = TOTAL_SATS - 1_000;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(false, observed));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    let facts = facts(&lapsed.app.pool, &lapsed.order_id).await;
    assert_eq!(facts.payment_state, "manual_review");
    assert_eq!(
        facts.review_reason.as_deref(),
        Some("amount_mismatch"),
        "the amount decides before stock does, as for on-time money"
    );
    assert_eq!(receipts(&lapsed.app.pool, &lapsed.order_id).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_mismatch_first_seen_before_the_expiry_sweep_names_the_amount(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(LATE_AFTER_SECONDS);
    let observed = TOTAL_SATS - 1_000;

    // The producer flags the settlement late before the marketplace window
    // sweep has run: the payment is still awaiting its entitlement.
    paykit.set_status(&reference, late_status(false, observed));
    assert!(poll_now(&app, after).await >= 1);
    let first = facts(&app.pool, &order_id).await;
    assert_eq!(first.payment_state, "manual_review");
    assert_eq!(first.review_reason.as_deref(), Some("amount_mismatch"));

    // The sweep that follows leaves the review alone.
    expire_due_payment_windows(&app.state, after + chrono::Duration::seconds(60))
        .await
        .expect("window sweep runs");
    let swept = facts(&app.pool, &order_id).await;
    assert_eq!(swept.payment_state, "manual_review");
    assert_eq!(swept.review_reason.as_deref(), Some("amount_mismatch"));
    assert_eq!(receipts(&app.pool, &order_id).await, 0);
    assert_eq!(sold_quantity(&app.pool, &seller).await, 0);
    assert_eq!(payment_manual_review_events(&app.pool, &order_id).await, 1);
    assert_eq!(
        review_notices(&app.pool, &order_id, "amount_mismatch").await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_mismatch_racing_the_expiry_sweep_never_pays_or_double_reviews(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(LATE_AFTER_SECONDS);
    let observed = TOTAL_SATS + 1_000;
    paykit.set_status(&reference, late_status(false, observed));

    let (_swept, polled) = tokio::join!(
        expire_due_payment_windows(&app.state, after),
        poll_now(&app, after),
    );
    let mut polled = polled;
    // Whichever side lost the race gets another turn.
    for step in 1..=3 {
        if polled > 0 && facts(&app.pool, &order_id).await.payment_state == "manual_review" {
            break;
        }
        polled = poll_now(&app, after + chrono::Duration::seconds(60 * step)).await;
    }

    let settled = facts(&app.pool, &order_id).await;
    assert_eq!(settled.payment_state, "manual_review");
    assert_eq!(settled.review_reason.as_deref(), Some("amount_mismatch"));
    assert_ne!(settled.order_state, "paid");
    assert_eq!(receipts(&app.pool, &order_id).await, 0);
    assert_eq!(sold_quantity(&app.pool, &seller).await, 0);
    assert_eq!(payment_manual_review_events(&app.pool, &order_id).await, 1);
    assert_eq!(
        review_notices(&app.pool, &order_id, "amount_mismatch").await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_mismatch_leaves_the_unit_for_another_buyer(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    let observed = TOTAL_SATS - 1_000;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(false, observed));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    assert_amount_mismatch_review(&lapsed, observed).await;

    // Another buyer can still check out the unit the late money did not take.
    let other = new_actor(&lapsed.app).await;
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", lapsed.seller.pubky))
            .fetch_one(&lapsed.app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&lapsed.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(&lapsed.app, &other.token, &checkout).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_can_still_resolve_a_late_mismatch_as_paid(pool: PgPool) {
    let lapsed = lapsed_order(pool).await;
    let observed = TOTAL_SATS + 1_000;
    lapsed
        .paykit
        .set_status(&lapsed.reference, late_status(false, observed));
    assert!(poll_now(&lapsed.app, lapsed.after + chrono::Duration::seconds(60)).await >= 1);
    assert_amount_mismatch_review(&lapsed, observed).await;

    let (status, body) = resolve_call(
        &lapsed.app,
        &lapsed.seller.token,
        &lapsed.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "resolution: {body}");
    let pool = &lapsed.app.pool;
    let facts = facts(pool, &lapsed.order_id).await;
    assert_eq!(facts.order_state, "paid");
    assert_eq!(facts.payment_state, "confirmed");
    assert_eq!(receipts(pool, &lapsed.order_id).await, 1);
    assert_eq!(sold_quantity(pool, &lapsed.seller).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_on_time_mismatch_is_unchanged(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    paykit.set_status(&reference, status_confirmed("exclusive", false, 2));
    assert_eq!(poll_now(&app, app.clock.now()).await, 1);
    let facts = facts(&app.pool, &order_id).await;
    assert_eq!(facts.payment_state, "manual_review");
    assert_eq!(facts.review_reason.as_deref(), Some("amount_mismatch"));
    assert_eq!(facts.order_state, "pending_payment");
    assert!(facts.stock_held);
    assert_eq!(receipts(&app.pool, &order_id).await, 0);
}
