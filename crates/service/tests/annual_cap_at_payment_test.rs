//! The EU seller's annual limit where money is taken (legal launch plan C4,
//! LG15). Only paid orders count, so nothing reserves a slot before payment;
//! a Bitcoin payment that would cross 29 sales is refused at payment start,
//! and one already in flight when the last slot goes is sent to manual review
//! at confirmation instead of being marked paid.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use common::paykit_review::{enable_bitcoin, poll_now, resolve_call, status_confirmed};
use common::{
    checkout_command_with_id, execute, indexed_command_id, new_actor, payment_command,
    register_sat_command, send, spawn_fake_homeserver, test_app_with_payments_config,
    with_shop_homeserver, FakeHomeserver, FakePaykit, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

struct Placed {
    buyer: TestActor,
    order_id: String,
    payment_id: String,
}

struct Fixture {
    app: TestApp,
    paykit: FakePaykit,
    seller: TestActor,
    buyer_country: &'static str,
    homeserver: FakeHomeserver,
    shop_country: &'static str,
}

async fn seed_eur(pool: &PgPool, now: DateTime<Utc>) {
    for (currency, rate) in [("EUR", "90000"), ("USD", "100000")] {
        for back in 0..3 {
            let accepted = now - Duration::seconds(60 * back);
            let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
            let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
            sqlx::query(
                "INSERT INTO fx_rate_samples \
                 (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
                 VALUES ($1, $2::numeric, $3, $3, $4, 'blocktank')",
            )
            .bind(currency)
            .bind(rate)
            .bind(accepted)
            .bind(bucket)
            .execute(pool)
            .await
            .expect("seed fx sample");
        }
    }
}

async fn fixture(pool: PgPool, country: &'static str, buyer_country: &'static str) -> Fixture {
    let mut config = Config::for_tests();
    config.eu_fence_enabled = true;
    let (app, paykit, _ipn) = test_app_with_payments_config(pool, config).await;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    seed_eur(&app.pool, app.clock.now()).await;
    let seller = new_actor(&app).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, country, None));
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sat_command(&seller.pubky, 100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    enable_bitcoin(&app, &paykit, &seller).await;
    paykit.set_allocation_mode("exclusive");
    Fixture {
        app,
        paykit,
        seller,
        buyer_country,
        homeserver,
        shop_country: country,
    }
}

fn shop_record(seller: &TestActor, country: &str, confirmed_at: Option<String>) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "shop",
        "ownerPubky": seller.pubky,
        "name": "Cap Shop",
        "sellerStatus": "private",
        "location": { "countryCode": country },
    });
    if let Some(confirmed_at) = confirmed_at {
        record["sellerStatusConfirmedAt"] = json!(confirmed_at);
    }
    record
}

impl Fixture {
    /// The seller's shop record, with `sellerStatusConfirmedAt` when given.
    fn put_record(&self, confirmed_at: Option<String>) {
        self.homeserver.put_shop_record(
            &self.seller.pubky,
            shop_record(&self.seller, self.shop_country, confirmed_at),
        );
    }

    /// The seller re-confirms now: later than every sale so far, and not in the future.
    fn reconfirm(&self) {
        self.app.clock.advance_seconds(60);
        self.put_record(Some(self.app.clock.now().to_rfc3339()));
        self.app.clock.advance_seconds(60);
    }
}

async fn place(fx: &Fixture, number: u64) -> (StatusCode, Value, Option<Placed>) {
    let buyer = new_actor(&fx.app).await;
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE seller_pubky = $1")
            .bind(&fx.seller.pubky)
            .fetch_one(&fx.app.pool)
            .await
            .expect("listing revision");
    let mut command =
        checkout_command_with_id(&fx.seller.pubky, &indexed_command_id(0x9100, number));
    command["payload"]["lines"][0]["expected_revision"] = json!(revision);
    command["payload"]["buyer_country"] = json!(fx.buyer_country);
    let (status, body) = execute(&fx.app, &buyer.token, &command).await;
    let placed = (status == StatusCode::OK).then(|| Placed {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
        buyer,
    });
    (status, body, placed)
}

async fn placed(fx: &Fixture, number: u64) -> Placed {
    let (status, body, placed) = place(fx, number).await;
    assert_eq!(status, StatusCode::OK, "checkout {number} failed: {body}");
    placed.expect("order placed")
}

/// Brings the seller to `n` paid sales on the sandbox.
async fn paid_sales(fx: &Fixture, n: u64) {
    for number in 1..=n {
        let order = placed(fx, number).await;
        let (code, body) = execute(
            &fx.app,
            &order.buyer.token,
            &payment_command(&order.payment_id, 1, "confirmed", 1, 70_000 + number),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "paying {number}: {body}");
    }
}

async fn bind(fx: &Fixture, order: &Placed) -> (StatusCode, Value) {
    send(
        fx.app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&order.buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await
}

async fn bind_ok(fx: &Fixture, order: &Placed) -> String {
    let (status, body) = bind(fx, order).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let client = fx
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&fx.app.pool, client, fx.app.clock.now(), 30)
        .await
        .expect("activation delivers");
    // A refused bind spends its attempt number, so the reference is the latest.
    let order_uuid = Uuid::parse_str(&order.order_id).expect("order uuid");
    let attempt: i32 = sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1")
        .bind(order_uuid)
        .fetch_one(&fx.app.pool)
        .await
        .expect("bind attempt");
    attempt_reference(order_uuid, attempt)
}

async fn paid_events(fx: &Fixture) -> i64 {
    common::count(
        &fx.app.pool,
        "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'",
    )
    .await
}

async fn states(fx: &Fixture, order: &Placed) -> (String, String) {
    sqlx::query_as(
        "SELECT o.state, p.state FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).expect("order uuid"))
    .fetch_one(&fx.app.pool)
    .await
    .expect("order and payment")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_payment_that_would_cross_the_limit_goes_to_manual_review_not_to_paid(
    pool: PgPool,
) {
    let fx = fixture(pool, "DE", "DE").await;
    paid_sales(&fx, 27).await;
    // Three orders are open when the last slots go: each passed the order-time
    // check and each can start paying, because only paid sales count.
    let first = placed(&fx, 101).await;
    let second = placed(&fx, 102).await;
    let third = placed(&fx, 103).await;
    let first_reference = bind_ok(&fx, &first).await;
    let second_reference = bind_ok(&fx, &second).await;
    let third_reference = bind_ok(&fx, &third).await;

    // The 28th and 29th payments fit.
    for reference in [&first_reference, &second_reference] {
        fx.paykit
            .set_status(reference, status_confirmed("exclusive", true, 2));
    }
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 2);
    assert_eq!(states(&fx, &first).await.0, "paid");
    assert_eq!(states(&fx, &second).await.0, "paid");
    assert_eq!(paid_events(&fx).await, 29);

    // The third settles on chain after the last slot is gone: the money is
    // real, so the payment is held for review and nothing is marked paid.
    fx.app.clock.advance_seconds(60);
    fx.paykit
        .set_status(&third_reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    let (order_state, payment_state) = states(&fx, &third).await;
    assert_eq!(order_state, "pending_payment");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(paid_events(&fx).await, 29, "no payment.confirmed for it");
    assert_eq!(
        common::count(&fx.app.pool, "SELECT COUNT(*) FROM receipts").await,
        29,
        "no receipt for it"
    );

    // The seller cannot resolve it into a sale: the funds go back.
    let (code, body) = resolve_call(
        &fx.app,
        &fx.seller.token,
        &third.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("refund_required"), "{body}");
    assert_eq!(states(&fx, &third).await.1, "manual_review");
    assert_eq!(paid_events(&fx).await, 29);

    // Refunding it is open, and it takes no slot.
    let (code, body) = resolve_call(
        &fx.app,
        &fx.seller.token,
        &third.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "refund-0001" }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(paid_events(&fx).await, 29);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_buyer_is_not_asked_to_pay_a_seller_at_the_limit(pool: PgPool) {
    let fx = fixture(pool, "DE", "DE").await;
    paid_sales(&fx, 28).await;
    let early = placed(&fx, 101).await;
    let late = placed(&fx, 102).await;

    // One more sale reaches the limit; the other order, placed earlier,
    // cannot start a Bitcoin payment any more.
    let reference = bind_ok(&fx, &early).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(paid_events(&fx).await, 29);

    let (status, body) = bind(&fx, &late).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("SELLER_ANNUAL_LIMIT_REACHED"),
        "{body}"
    );
    assert_eq!(
        body["error"]["reason"],
        json!("annual_sales_limit"),
        "{body}"
    );
    // Nothing was taken or held: the order is still unpaid and unbound.
    let (order_state, payment_state) = states(&fx, &late).await;
    assert_eq!(
        (order_state.as_str(), payment_state.as_str()),
        ("pending_payment", "awaiting_entitlement")
    );
    let method: Option<String> =
        sqlx::query_scalar("SELECT payment_method FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&late.order_id).expect("order uuid"))
            .fetch_one(&fx.app.pool)
            .await
            .expect("payment method");
    assert_eq!(method, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_outside_the_eu_has_no_cap_at_payment(pool: PgPool) {
    let fx = fixture(pool, "CH", "US").await;
    paid_sales(&fx, 29).await;
    let thirtieth = placed(&fx, 101).await;
    let reference = bind_ok(&fx, &thirtieth).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &thirtieth).await.0, "paid");
    assert_eq!(paid_events(&fx).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_polls_of_two_settled_payments_take_one_last_slot(pool: PgPool) {
    let fx = fixture(pool, "DE", "DE").await;
    paid_sales(&fx, 28).await;
    let first = placed(&fx, 101).await;
    let second = placed(&fx, 102).await;
    let first_reference = bind_ok(&fx, &first).await;
    let second_reference = bind_ok(&fx, &second).await;
    for reference in [&first_reference, &second_reference] {
        fx.paykit
            .set_status(reference, status_confirmed("exclusive", true, 2));
    }

    // Two polls run at the same moment over the same two settled payments.
    let now = fx.app.clock.now();
    let (a, b) = tokio::join!(poll_now(&fx.app, now), poll_now(&fx.app, now));
    assert!(a + b >= 2, "both payments were looked at ({a}, {b})");

    assert_eq!(
        paid_events(&fx).await,
        29,
        "exactly one takes the 29th slot"
    );
    let mut outcomes = vec![states(&fx, &first).await, states(&fx, &second).await];
    outcomes.sort();
    assert_eq!(
        outcomes,
        vec![
            ("paid".to_string(), "confirmed".to_string()),
            ("pending_payment".to_string(), "manual_review".to_string()),
        ]
    );
}

// --- LG18: the private seller's re-confirmation point at payment ------------

async fn confirmed_at_of(fx: &Fixture, order: &Placed) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT seller_status_confirmed_at FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order.order_id).expect("order uuid"))
        .fetch_one(&fx.app.pool)
        .await
        .expect("order row")
}

fn is_reconfirmation_refusal(body: &Value) -> bool {
    body["error"]["code"] == json!("SELLER_UNAVAILABLE_IN_REGION")
        && body["error"]["reason"] == json!("private_seller_reconfirmation_required")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_confirmation_date_freezes_on_the_order_at_its_payment_start(pool: PgPool) {
    let fx = fixture(pool, "CH", "DE").await;
    let confirmed = fx.app.clock.now() - Duration::days(3);
    fx.put_record(Some(confirmed.to_rfc3339()));
    let first = placed(&fx, 1).await;
    // Not frozen before the payment starts.
    assert_eq!(confirmed_at_of(&fx, &first).await, None);
    bind_ok(&fx, &first).await;
    assert_eq!(confirmed_at_of(&fx, &first).await, Some(confirmed));

    // A later change to the record never moves an order whose payment started.
    fx.put_record(Some((fx.app.clock.now() - Duration::days(1)).to_rfc3339()));
    assert_eq!(confirmed_at_of(&fx, &first).await, Some(confirmed));

    // A date in the future, or none, freezes as none.
    fx.put_record(Some((fx.app.clock.now() + Duration::days(1)).to_rfc3339()));
    let future = placed(&fx, 2).await;
    bind_ok(&fx, &future).await;
    assert_eq!(confirmed_at_of(&fx, &future).await, None);
    fx.put_record(None);
    let none = placed(&fx, 3).await;
    bind_ok(&fx, &none).await;
    assert_eq!(confirmed_at_of(&fx, &none).await, None);

    // An unreadable record freezes none, and the payment still starts.
    let unreadable = placed(&fx, 4).await;
    fx.homeserver.fail_shop_record(&fx.seller.pubky);
    let (status, body) = bind(&fx, &unreadable).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(confirmed_at_of(&fx, &unreadable).await, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_buyer_is_not_asked_to_pay_a_private_seller_past_the_reconfirmation_point(pool: PgPool) {
    let fx = fixture(pool, "CH", "DE").await;
    paid_sales(&fx, 28).await;
    let early = placed(&fx, 101).await;
    let late = placed(&fx, 102).await;

    // One more EU sale reaches the point; the order placed earlier cannot
    // start a Bitcoin payment any more.
    let reference = bind_ok(&fx, &early).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(paid_events(&fx).await, 29);

    let (status, body) = bind(&fx, &late).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(is_reconfirmation_refusal(&body), "{body}");
    let (order_state, payment_state) = states(&fx, &late).await;
    assert_eq!(
        (order_state.as_str(), payment_state.as_str()),
        ("pending_payment", "awaiting_entitlement")
    );

    // The seller re-confirms: the same order can now start paying, from the
    // new date.
    fx.reconfirm();
    let reference = bind_ok(&fx, &late).await;
    assert!(confirmed_at_of(&fx, &late).await.is_some());
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &late).await.0, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_already_on_chain_past_the_reconfirmation_point_goes_to_manual_review(pool: PgPool) {
    let fx = fixture(pool, "CH", "DE").await;
    paid_sales(&fx, 27).await;
    let orders = [
        placed(&fx, 101).await,
        placed(&fx, 102).await,
        placed(&fx, 103).await,
    ];
    let mut references = Vec::new();
    for order in &orders {
        references.push(bind_ok(&fx, order).await);
    }
    for reference in &references[..2] {
        fx.paykit
            .set_status(reference, status_confirmed("exclusive", true, 2));
    }
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 2);
    assert_eq!(paid_events(&fx).await, 29);

    // The third settles after the point: real money, so review, not paid.
    fx.app.clock.advance_seconds(60);
    fx.paykit
        .set_status(&references[2], status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(
        states(&fx, &orders[2]).await,
        ("pending_payment".to_string(), "manual_review".to_string())
    );
    assert_eq!(paid_events(&fx).await, 29);
    assert_eq!(
        common::count(&fx.app.pool, "SELECT COUNT(*) FROM receipts").await,
        29
    );

    // The seller cannot resolve it into a sale, even after re-confirming:
    // the date frozen on the order is the one the payment started with.
    fx.reconfirm();
    let (code, body) = resolve_call(
        &fx.app,
        &fx.seller.token,
        &orders[2].order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("refund_required"), "{body}");
    assert_eq!(paid_events(&fx).await, 29);
    let (code, body) = resolve_call(
        &fx.app,
        &fx.seller.token,
        &orders[2].order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "refund-0002" }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_reconfirmation_after_the_payment_started_does_not_release_it(pool: PgPool) {
    let fx = fixture(pool, "CH", "DE").await;
    paid_sales(&fx, 28).await;
    let first = placed(&fx, 101).await;
    let second = placed(&fx, 102).await;
    let first_reference = bind_ok(&fx, &first).await;
    let second_reference = bind_ok(&fx, &second).await;
    fx.paykit
        .set_status(&first_reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(paid_events(&fx).await, 29);

    // The seller re-confirms while the second payment is in flight.
    fx.reconfirm();
    fx.paykit
        .set_status(&second_reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(
        states(&fx, &second).await,
        ("pending_payment".to_string(), "manual_review".to_string())
    );

    // A new order's payment starts from the new date and is taken.
    let fresh = placed(&fx, 103).await;
    let reference = bind_ok(&fx, &fresh).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &fresh).await.0, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_reconfirmation_point_only_applies_to_sales_to_eu_buyers(pool: PgPool) {
    let fx = fixture(pool, "CH", "US").await;
    paid_sales(&fx, 29).await;
    let thirtieth = placed(&fx, 101).await;
    let reference = bind_ok(&fx, &thirtieth).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &thirtieth).await.0, "paid");
    assert_eq!(paid_events(&fx).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_reconfirmation_point_leaves_a_gb_buyers_payment_alone(pool: PgPool) {
    // C3 now covers GB, but the re-confirmation point is part of the DAC7
    // guard (EU-only): the seller's 29 paid sales to EU buyers do not stop a
    // GB buyer from starting or completing a payment.
    let mut fx = fixture(pool, "CH", "DE").await;
    paid_sales(&fx, 28).await;
    let early = placed(&fx, 100).await;
    let late = placed(&fx, 101).await;
    fx.buyer_country = "GB";
    let gb_buyer = placed(&fx, 102).await;

    // One more EU sale reaches the point.
    let reference = bind_ok(&fx, &early).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(paid_events(&fx).await, 29);

    let (status, body) = bind(&fx, &late).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(is_reconfirmation_refusal(&body), "{body}");

    let reference = bind_ok(&fx, &gb_buyer).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &gb_buyer).await.0, "paid");
    assert_eq!(paid_events(&fx).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sales_to_gb_buyers_do_not_bring_a_private_seller_to_the_reconfirmation_point(
    pool: PgPool,
) {
    let fx = fixture(pool, "CH", "GB").await;
    paid_sales(&fx, 29).await;
    let thirtieth = placed(&fx, 101).await;
    let reference = bind_ok(&fx, &thirtieth).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(states(&fx, &thirtieth).await.0, "paid");
    assert_eq!(paid_events(&fx).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_resident_sellers_annual_limit_at_payment_still_covers_a_gb_buyer(pool: PgPool) {
    let fx = fixture(pool, "DE", "GB").await;
    paid_sales(&fx, 28).await;
    let early = placed(&fx, 101).await;
    let late = placed(&fx, 102).await;
    let reference = bind_ok(&fx, &early).await;
    fx.paykit
        .set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&fx.app, fx.app.clock.now()).await >= 1);
    assert_eq!(paid_events(&fx).await, 29);

    let (status, body) = bind(&fx, &late).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("SELLER_ANNUAL_LIMIT_REACHED"),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_polls_of_two_settled_payments_take_one_reconfirmation_slot(pool: PgPool) {
    let fx = fixture(pool, "CH", "DE").await;
    paid_sales(&fx, 28).await;
    let first = placed(&fx, 101).await;
    let second = placed(&fx, 102).await;
    let first_reference = bind_ok(&fx, &first).await;
    let second_reference = bind_ok(&fx, &second).await;
    for reference in [&first_reference, &second_reference] {
        fx.paykit
            .set_status(reference, status_confirmed("exclusive", true, 2));
    }
    let now = fx.app.clock.now();
    let (a, b) = tokio::join!(poll_now(&fx.app, now), poll_now(&fx.app, now));
    assert!(a + b >= 2, "both payments were looked at ({a}, {b})");

    assert_eq!(
        paid_events(&fx).await,
        29,
        "exactly one takes the last slot"
    );
    let mut outcomes = vec![states(&fx, &first).await, states(&fx, &second).await];
    outcomes.sort();
    assert_eq!(
        outcomes,
        vec![
            ("paid".to_string(), "confirmed".to_string()),
            ("pending_payment".to_string(), "manual_review".to_string()),
        ]
    );
}
