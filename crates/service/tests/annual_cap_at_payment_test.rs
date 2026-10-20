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
    with_shop_homeserver, FakePaykit, TestActor, TestApp,
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

async fn fixture(pool: PgPool, country: &str, buyer_country: &'static str) -> Fixture {
    let mut config = Config::for_tests();
    config.eu_fence_enabled = true;
    let (app, paykit, _ipn) = test_app_with_payments_config(pool, config).await;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    seed_eur(&app.pool, app.clock.now()).await;
    let seller = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Cap Shop",
            "sellerStatus": "private",
            "location": { "countryCode": country },
        }),
    );
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
    attempt_reference(Uuid::parse_str(&order.order_id).expect("order uuid"), 1)
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
