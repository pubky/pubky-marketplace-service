//! The fork-to-upstream Paykit cutover (README, "Cutover runbook"): the ALERT
//! for a fork-issued request polled under the upstream configuration, and the
//! two SQL checks the runbook tells an operator to run, executed here against
//! the real schema so the runbook cannot drift from it.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::paykit_review::{
    bound_order, create_sat_order, enable_bitcoin, into_manual_review_held, poll_now, resolve_call,
    status_confirmed,
};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payments::{PaykitApi, PaykitClient};
use marketplace_service::workers::{
    drain_outbox, expire_due_payment_windows, verify_due_paykit_payments,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

const README: &str = include_str!("../../../README.md");
const ALERT_CODE: &str = "paykit_fork_row_polled_under_upstream";

/// The SQL fence that follows `marker` in the README.
fn runbook_query(marker: &str) -> String {
    let after = README
        .split(marker)
        .nth(1)
        .unwrap_or_else(|| panic!("README carries {marker}"));
    let start = after
        .find("```sql\n")
        .expect("a sql fence follows the marker")
        + "```sql\n".len();
    let length = after[start..].find("```").expect("the sql fence closes");
    after[start..start + length].to_string()
}

/// Runs a runbook query as if `now()` were `at`: the test clock is not the
/// database clock, so the one `now()` in each query is pinned.
async fn run_at(pool: &PgPool, marker: &str, at: DateTime<Utc>) -> (i64, i64) {
    let sql =
        runbook_query(marker).replace("now()", &format!("'{}'::timestamptz", at.to_rfc3339()));
    sqlx::query_as(&sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("{marker} query failed: {error}\n{sql}"))
}

const GATE: &str = "<!-- paykit-cutover-gate-query -->";
const DECOMMISSION: &str = "<!-- paykit-cutover-decommission-query -->";

fn alerts_for(reference: &str) -> usize {
    captured_logs()
        .lines()
        .filter(|line| line.contains(ALERT_CODE) && line.contains(reference))
        .count()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_fork_request_polled_under_the_upstream_configuration_raises_an_alert(pool: PgPool) {
    install_log_capture();
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (_order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;

    // The fork configuration polls the request without an alert.
    let mut later = app.clock.now() + Duration::seconds(60);
    poll_now(&app, later).await;
    assert_eq!(alerts_for(&reference), 0, "fork polling is silent");

    // The same request polled by a client configured for upstream.
    let upstream = PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("upstream client builds")
        .with_api(PaykitApi::Upstream);
    later += Duration::seconds(60);
    verify_due_paykit_payments(&app.state, &upstream, later)
        .await
        .expect("poll runs");
    assert_eq!(
        alerts_for(&reference),
        1,
        "one alert for the polled request"
    );

    // Every poll of the request alerts again; nothing else does.
    later += Duration::seconds(60);
    verify_due_paykit_payments(&app.state, &upstream, later)
        .await
        .expect("poll runs");
    assert_eq!(alerts_for(&reference), 2);
    let line = captured_logs()
        .lines()
        .find(|line| line.contains(ALERT_CODE) && line.contains(&reference))
        .expect("the alert line")
        .to_string();
    assert!(line.contains("ALERT"), "{line}");
    assert!(line.contains(&seller.pubky), "{line}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_gate_query_counts_every_fork_request_still_in_flight(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let start = app.clock.now();
    assert_eq!(
        run_at(&pool, GATE, start).await,
        (0, 0),
        "an empty database is clear"
    );

    // An active invoice blocks the flip; a confirmed payment does not.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (_order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    assert_eq!(run_at(&pool, GATE, start).await, (1, 0), "active invoice");
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, start + Duration::seconds(60)).await >= 1);
    assert_eq!(
        run_at(&pool, GATE, start + Duration::seconds(60)).await,
        (0, 0),
        "a confirmed payment no longer needs the fork"
    );

    // An expired, undetected invoice blocks the flip until the 24 h late
    // window after its expiry has passed.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, _reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let expired_at = start + Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&app.state, expired_at)
            .await
            .expect("expiry sweep")
            >= 1
    );
    let expires_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT paykit_expires_at FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).expect("order uuid"))
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert_eq!(
        run_at(&pool, GATE, expires_at + Duration::hours(23)).await,
        (1, 0),
        "inside the late window"
    );
    assert_eq!(
        run_at(&pool, GATE, expires_at + Duration::hours(25)).await,
        (0, 0),
        "late window closed"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_gate_query_counts_a_released_attempt_until_it_closes(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "bind failed: {body}");
    let invoice: Uuid = sqlx::query_scalar("SELECT paykit_invoice_id FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("order row");
    paykit.set_invoice_state(invoice, "expired_tail");
    paykit.script_activate(
        invoice,
        vec![FakePaykitReply::Error(409, "invoice_finalized".to_string())],
    );
    drain_outbox(
        &app.pool,
        app.state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref()),
        app.clock.now(),
        30,
    )
    .await
    .expect("activation delivers");
    assert_eq!(
        run_at(&pool, GATE, app.clock.now()).await,
        (0, 1),
        "the released attempt is still watched"
    );

    // Past 24 h after its expiry the poller closes it unpaid.
    let expires_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT expires_at FROM paykit_superseded_attempts WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("released attempt row");
    let closed_at = expires_at + Duration::hours(25);
    marketplace_service::paykit_attempts::verify_due_released_attempts(
        &app.state,
        app.state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref())
            .expect("paykit client"),
        closed_at,
    )
    .await
    .expect("released attempts poll");
    assert_eq!(run_at(&pool, GATE, closed_at).await, (0, 0));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_decommission_query_counts_manual_review_and_unfinished_resolutions(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let now = app.clock.now();
    assert_eq!(run_at(&pool, DECOMMISSION, now).await, (0, 0));

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    assert_eq!(
        run_at(&pool, DECOMMISSION, now).await,
        (1, 0),
        "a payment in manual review still needs the issuing stack"
    );

    // The seller's resolution queues a delivery to the issuing stack.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(
        run_at(&pool, DECOMMISSION, now).await,
        (0, 1),
        "the resolution is queued until the stack acknowledges it"
    );
}
