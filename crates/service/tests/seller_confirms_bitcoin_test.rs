//! The seller's Bitcoin confirmation option (legal launch plan B4): a seller
//! whose shop record says `bitcoinConfirmation: "seller"` confirms their own
//! Bitcoin payments. Upstream Paykit reports every order as `exclusive`, so
//! for an opted-in seller the service reads the report as `shared_manual`:
//! on-time money enters the seller-confirmation window and only the seller's
//! attestation pays the order. Default off; nothing changes for sellers who
//! did not opt in.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{bound_order, confirm_call, resolve_call};
use common::{
    exclusive_facts, new_actor, spawn_fake_homeserver, test_app_with_payments,
    with_shop_homeserver, FakeHomeserver, FakePaykitStatus, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::{PaykitObservation, PaykitStatusFacts, PaykitStatusOutcome};
use marketplace_service::workers::{expire_due_payment_windows, verify_due_paykit_payments};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// The status double the poll reads. The real upstream status route is a
/// different endpoint this harness does not serve, so the outcome Paykit would
/// report (upstream always reports `exclusive`) is set directly by reference.
fn confirmed(late: bool) -> PaykitStatusOutcome {
    PaykitStatusOutcome::Confirmed {
        amount_matched: true,
        facts: PaykitStatusFacts {
            late_settlement: late,
            observation: PaykitObservation {
                txid: Some("9f2c".repeat(16)),
                observed_sats: Some(51_637),
                confirmations: Some(2),
                ..PaykitObservation::default()
            },
            ..exclusive_facts()
        },
    }
}

/// Money that does not match the order's amount, as a confirmed report.
fn confirmed_short() -> PaykitStatusOutcome {
    let PaykitStatusOutcome::Confirmed { facts, .. } = confirmed(false) else {
        unreachable!("confirmed() builds a confirmed outcome");
    };
    PaykitStatusOutcome::Confirmed {
        amount_matched: false,
        facts: PaykitStatusFacts {
            amount_matched: Some(false),
            ..facts
        },
    }
}

/// An underpaid sighting: upstream reports it as `detected`.
fn detected_short() -> PaykitStatusOutcome {
    let PaykitStatusOutcome::Detected { facts } = detected() else {
        unreachable!("detected() builds a detected outcome");
    };
    PaykitStatusOutcome::Detected {
        facts: PaykitStatusFacts {
            amount_matched: Some(false),
            ..facts
        },
    }
}

fn detected() -> PaykitStatusOutcome {
    PaykitStatusOutcome::Detected {
        facts: PaykitStatusFacts {
            observation: PaykitObservation {
                txid: Some("9f2c".repeat(16)),
                observed_sats: Some(51_637),
                confirmations: Some(0),
                ..PaykitObservation::default()
            },
            ..exclusive_facts()
        },
    }
}

async fn poll_now(
    app: &TestApp,
    status: &FakePaykitStatus,
    now: chrono::DateTime<chrono::Utc>,
) -> u64 {
    verify_due_paykit_payments(&app.state, status, now)
        .await
        .expect("poll runs")
}

fn shop_record(seller: &TestActor, extra: Value) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "shop",
        "ownerPubky": seller.pubky,
        "name": "Confirm Shop",
    });
    for (key, value) in extra.as_object().expect("extra object") {
        record[key] = value.clone();
    }
    record
}

struct Fixture {
    app: TestApp,
    paykit: common::FakePaykit,
    status: FakePaykitStatus,
    homeserver: FakeHomeserver,
    seller: TestActor,
    buyer: TestActor,
}

async fn fixture(pool: PgPool) -> Fixture {
    let (app, paykit) = test_app_with_payments(pool).await;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    Fixture {
        app,
        paykit,
        status: FakePaykitStatus::default(),
        homeserver,
        seller,
        buyer,
    }
}

/// Binds a Bitcoin order, then presents it as an upstream Paykit order: the
/// fork fixture is the only bind harness, and an upstream order differs by
/// its API and by carrying no stack pins.
async fn upstream_bound(fx: &Fixture) -> (String, String, String) {
    let (order_id, payment_id, reference) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    sqlx::query(
        "UPDATE orders SET paykit_api = 'upstream', paykit_stack_id = NULL, \
         paykit_stack_endpoint = NULL, paykit_allocation_mode = NULL, \
         paykit_payment_reference = gen_random_uuid(), paykit_operation_id = 'upstream-op', \
         paykit_payment_window_seconds = 1800, paykit_asset = 'BTC' WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .execute(&fx.app.pool)
    .await
    .expect("present the order as upstream");
    (order_id, payment_id, reference)
}

async fn confirms_bitcoin(pool: &PgPool, order_id: &str) -> Option<bool> {
    sqlx::query_scalar("SELECT seller_confirms_bitcoin FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn states(pool: &PgPool, order_id: &str) -> (String, String, String) {
    sqlx::query_as(
        "SELECT o.state, o.paykit_request_state, p.state FROM orders o \
         JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order and payment")
}

async fn confirmed_events(pool: &PgPool, payment_id: &str) -> i64 {
    common::count(
        pool,
        &format!(
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed' \
             AND aggregate_id = 'payment:{payment_id}'"
        ),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_option_freezes_on_the_order_at_the_bind(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, _, _) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    assert_eq!(confirms_bitcoin(&fx.app.pool, &order_id).await, Some(true));

    // A later change to the record never moves an open order.
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    assert_eq!(confirms_bitcoin(&fx.app.pool, &order_id).await, Some(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_the_option_the_order_freezes_false_and_an_unreadable_record_freezes_null(
    pool: PgPool,
) {
    let fx = fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, _, _) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    assert_eq!(confirms_bitcoin(&fx.app.pool, &order_id).await, Some(false));

    let other = new_actor(&fx.app).await;
    fx.homeserver.fail_shop_record(&other.pubky);
    let (order_id, _, _) = bound_order(&fx.app, &fx.paykit, &other, &fx.buyer, "exclusive").await;
    assert_eq!(confirms_bitcoin(&fx.app.pool, &order_id).await, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_opted_in_upstream_payment_waits_for_the_seller_who_then_pays_the_order(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;

    // Paykit reports the money as exclusive and confirmed: the service does not pay.
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(
        states(&fx.app.pool, &order_id).await,
        (
            "pending_payment".to_string(),
            "awaiting_seller_confirmation".to_string(),
            "awaiting_entitlement".to_string()
        )
    );
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);

    // A later report changes nothing about who pays.
    fx.app.clock.advance_seconds(60);
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "pending_payment");

    // The seller's confirmation pays it, once.
    let (code, body) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_detected_payment_enters_the_same_window(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, detected());
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    let (_, request_state, payment_state) = states(&fx.app.pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_the_option_the_same_report_pays_the_order_by_itself(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_whose_option_was_never_read_is_not_held(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.fail_shop_record(&fx.seller.pubky);
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    assert_eq!(confirms_bitcoin(&fx.app.pool, &order_id).await, None);
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_option_does_not_touch_a_fork_order(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, _, reference) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_settlement_still_takes_the_late_path_not_the_window(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, _, reference) = upstream_bound(&fx).await;
    fx.app.clock.advance_seconds(3 * 3_600);
    fx.status.set_outcome(&reference, confirmed(true));
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    let (_, request_state, _) = states(&fx.app.pool, &order_id).await;
    assert_ne!(request_state, "awaiting_seller_confirmation");
}

// --- interleavings (money path) ----------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_seller_confirmations_and_polls_pay_the_order_exactly_once(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, confirmed(false));
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(
        states(&fx.app.pool, &order_id).await.1,
        "awaiting_seller_confirmation"
    );

    let body = json!({});
    let (confirm_a, confirm_b, _polled) = tokio::join!(
        confirm_call(&fx.app, &fx.seller.token, &order_id, &body),
        confirm_call(&fx.app, &fx.seller.token, &order_id, &body),
        poll_now(&fx.app, &fx.status, fx.app.clock.now()),
    );
    let answers = [confirm_a, confirm_b];
    assert!(
        answers.iter().any(|(code, _)| *code == StatusCode::OK),
        "one of the confirmations pays the order: {answers:?}"
    );
    for (code, response) in &answers {
        if *code != StatusCode::OK {
            assert_eq!(*code, StatusCode::CONFLICT, "{response}");
            assert_eq!(
                response["error"]["reason"],
                json!("order_not_awaiting_confirmation")
            );
        }
    }
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);

    // Later polls and confirmations never pay it again.
    fx.app.clock.advance_seconds(60);
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    let (code, _) = confirm_call(&fx.app, &fx.seller.token, &order_id, &body).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CONFLICT,
        "{code}"
    );
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

// --- underpaid money (ovitrif, 19 Oct) ---------------------------------------------

async fn payment_review_reason(pool: &PgPool, payment_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT review_reason FROM payments WHERE id = $1")
        .bind(Uuid::parse_str(payment_id).expect("payment uuid"))
        .fetch_one(pool)
        .await
        .expect("payment row")
}

async fn observed_amount_matched(pool: &PgPool, order_id: &str) -> Option<bool> {
    sqlx::query_scalar(
        "SELECT (paykit_observation->>'amount_matched')::boolean FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_underpaid_confirmed_payment_goes_to_manual_review_not_to_the_seller(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, confirmed_short());
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);

    let (order_state, request_state, payment_state) = states(&fx.app.pool, &order_id).await;
    assert_eq!(payment_state, "manual_review");
    assert_ne!(request_state, "awaiting_seller_confirmation");
    assert_eq!(order_state, "pending_payment");
    assert_eq!(
        payment_review_reason(&fx.app.pool, &payment_id)
            .await
            .as_deref(),
        Some("amount_mismatch")
    );
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
    // The seller has nothing to confirm.
    let (code, _) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
    assert_ne!(code, StatusCode::OK);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_underpaid_sighting_never_enters_the_window(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, detected_short());
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    let (_, request_state, payment_state) = states(&fx.app.pool, &order_id).await;
    assert_ne!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);

    // The same payment topped up to the full amount then waits for the seller.
    fx.app.clock.advance_seconds(60);
    fx.status.set_outcome(&reference, confirmed(false));
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(
        states(&fx.app.pool, &order_id).await.1,
        "awaiting_seller_confirmation"
    );
    assert_eq!(
        observed_amount_matched(&fx.app.pool, &order_id).await,
        Some(true)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_matched_sighting_is_stored_as_matched_and_a_later_short_report_cannot_be_confirmed(
    pool: PgPool,
) {
    let fx = fixture(pool).await;
    fx.homeserver.put_shop_record(
        &fx.seller.pubky,
        shop_record(&fx.seller, json!({ "bitcoinConfirmation": "seller" })),
    );
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, detected());
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(
        states(&fx.app.pool, &order_id).await.1,
        "awaiting_seller_confirmation"
    );
    assert_eq!(
        observed_amount_matched(&fx.app.pool, &order_id).await,
        Some(true)
    );

    // Inside the window a later report only refreshes the facts, so the
    // stored observation now says the amount does not match ...
    fx.app.clock.advance_seconds(60);
    fx.status.set_outcome(&reference, confirmed_short());
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(
        observed_amount_matched(&fx.app.pool, &order_id).await,
        Some(false)
    );

    // ... and the seller cannot attest it as paid.
    let (code, body) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("confirmation_observation_mismatch")
    );
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "pending_payment");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
}

// --- seller confirmation as the deployment default (PAYMENT_CONFIRMATION) ----

async fn seller_mode_fixture(pool: PgPool) -> Fixture {
    let mut config = marketplace_service::config::Config::for_tests();
    config.payment_confirmation = marketplace_service::config::PaymentConfirmation::Seller;
    let (app, paykit, _ipn) = common::test_app_with_payments_config(pool, config).await;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    Fixture {
        app,
        paykit,
        status: FakePaykitStatus::default(),
        homeserver,
        seller,
        buyer,
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_seller_mode_every_upstream_payment_waits_without_any_opt_in(pool: PgPool) {
    let fx = seller_mode_fixture(pool).await;
    // No shop record option, and an unreadable record: the deployment decides.
    for record in [Some(shop_record(&fx.seller, json!({}))), None] {
        match record {
            Some(record) => fx.homeserver.put_shop_record(&fx.seller.pubky, record),
            None => fx.homeserver.fail_shop_record(&fx.seller.pubky),
        }
        let (order_id, payment_id, reference) = upstream_bound(&fx).await;
        fx.status.set_outcome(&reference, confirmed(false));
        assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
        assert_eq!(
            states(&fx.app.pool, &order_id).await,
            (
                "pending_payment".to_string(),
                "awaiting_seller_confirmation".to_string(),
                "awaiting_entitlement".to_string()
            )
        );
        assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
        let (code, body) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_seller_mode_underpaid_and_late_money_keep_their_routes(pool: PgPool) {
    let fx = seller_mode_fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    fx.status.set_outcome(&reference, confirmed_short());
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_eq!(
        payment_review_reason(&fx.app.pool, &payment_id)
            .await
            .as_deref(),
        Some("amount_mismatch")
    );
    assert_ne!(
        states(&fx.app.pool, &order_id).await.1,
        "awaiting_seller_confirmation"
    );

    let (order_id, _, reference) = upstream_bound(&fx).await;
    fx.app.clock.advance_seconds(3 * 3_600);
    fx.status.set_outcome(&reference, confirmed(true));
    poll_now(&fx.app, &fx.status, fx.app.clock.now()).await;
    assert_ne!(
        states(&fx.app.pool, &order_id).await.1,
        "awaiting_seller_confirmation"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_seller_mode_late_money_for_a_free_unit_waits_for_the_seller(pool: PgPool) {
    let fx = seller_mode_fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    let after_window = fx.app.clock.now() + chrono::Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&fx.app.state, after_window)
            .await
            .expect("payment-window reaper runs")
            >= 1
    );
    assert_eq!(
        states(&fx.app.pool, &order_id).await,
        (
            "cancelled".to_string(),
            "pending".to_string(),
            "expired".to_string()
        )
    );

    // The money arrives after the window while the unit is still free: it
    // is not paid by the observation, the seller is asked to resolve it.
    fx.status.set_outcome(&reference, confirmed(true));
    assert!(
        poll_now(
            &fx.app,
            &fx.status,
            after_window + chrono::Duration::seconds(60)
        )
        .await
            >= 1
    );
    let (order_state, _, payment_state) = states(&fx.app.pool, &order_id).await;
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(
        payment_review_reason(&fx.app.pool, &payment_id)
            .await
            .as_deref(),
        Some("late_settlement")
    );
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
    let receipts = common::count(
        &fx.app.pool,
        &format!("SELECT COUNT(*) FROM receipts WHERE order_id = '{order_id}'"),
    )
    .await;
    assert_eq!(receipts, 0);
    let stock_held: bool = sqlx::query_scalar("SELECT stock_held FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).expect("order uuid"))
        .fetch_one(&fx.app.pool)
        .await
        .expect("order row");
    assert!(
        !stock_held,
        "the free unit is not taken until the seller decides"
    );
    let notices = common::count(
        &fx.app.pool,
        &format!(
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_manual_review' \
             AND payload->>'review_reason' = 'late_settlement' \
             AND payload->>'recipient_pubky' = '{}' \
             AND payload->>'aggregate_id' LIKE '%{order_id}'",
            fx.seller.pubky
        ),
    )
    .await;
    assert_eq!(notices, 1, "the seller is told to resolve it");

    // The seller's own `paid` resolution re-takes the unit and pays the order.
    let (code, body) = resolve_call(
        &fx.app,
        &fx.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_seller_mode_concurrent_polls_of_late_money_enter_one_review_and_pay_nothing(
    pool: PgPool,
) {
    let fx = seller_mode_fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    let after_window = fx.app.clock.now() + chrono::Duration::seconds(7_300);
    expire_due_payment_windows(&fx.app.state, after_window)
        .await
        .expect("payment-window reaper runs");
    fx.status.set_outcome(&reference, confirmed(true));
    let at = after_window + chrono::Duration::seconds(60);
    let (_, _, _) = tokio::join!(
        verify_due_paykit_payments(&fx.app.state, &fx.status, at),
        verify_due_paykit_payments(&fx.app.state, &fx.status, at),
        verify_due_paykit_payments(&fx.app.state, &fx.status, at),
    );
    let (order_state, _, payment_state) = states(&fx.app.pool, &order_id).await;
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 0);
    let reviews = common::count(
        &fx.app.pool,
        &format!(
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.manual_review' \
             AND aggregate_id = 'payment:{payment_id}'"
        ),
    )
    .await;
    assert_eq!(reviews, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_automatic_mode_late_money_for_a_free_unit_still_completes_the_order(pool: PgPool) {
    let fx = fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    let after_window = fx.app.clock.now() + chrono::Duration::seconds(7_300);
    expire_due_payment_windows(&fx.app.state, after_window)
        .await
        .expect("payment-window reaper runs");
    fx.status.set_outcome(&reference, confirmed(true));
    poll_now(
        &fx.app,
        &fx.status,
        after_window + chrono::Duration::seconds(60),
    )
    .await;
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn in_seller_mode_a_retired_fork_order_keeps_its_creators_mode(pool: PgPool) {
    let fx = seller_mode_fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, _, reference) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_frozen_before_the_switch_existed_reads_as_automatic(pool: PgPool) {
    let fx = seller_mode_fixture(pool).await;
    fx.homeserver
        .put_shop_record(&fx.seller.pubky, shop_record(&fx.seller, json!({})));
    let (order_id, payment_id, reference) = upstream_bound(&fx).await;
    sqlx::query("UPDATE orders SET payment_confirmation = NULL WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).expect("order uuid"))
        .execute(&fx.app.pool)
        .await
        .expect("present the order as pre-0091");
    fx.status.set_outcome(&reference, confirmed(false));
    assert!(poll_now(&fx.app, &fx.status, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx.app.pool, &order_id).await.0, "paid");
    assert_eq!(confirmed_events(&fx.app.pool, &payment_id).await, 1);
}
