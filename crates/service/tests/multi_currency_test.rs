//! Multi-currency prices and fiat-at-bind quotes on the legacy `amount_sats`
//! contract, against real Postgres with the real FX wrapper over the
//! scripted Blocktank double: every supported currency binds with its own
//! `BTC<CCY>` ticker and rounds up, each guard (staleness, future skew,
//! bounds, deviation, missing ticker, missing reference) refuses only its
//! own currency, the sampler keeps one reference per currency, PayPal
//! charges in the listing currency (EUR and JPY), listing validation gives
//! typed refusals, and the quote holds under interleaved and concurrent
//! binds. The multi-asset (USD cents) contract is in
//! `multi_currency_paykit_test.rs`.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use common::fx_feed::{fx_body, fx_body_multi, FakeFxFeed};
use common::paykit_review::enable_bitcoin;
use common::*;
use marketplace_domain::currency::Currency;
use marketplace_service::clock::Clock;
use marketplace_service::fx::{self, FxError};
use marketplace_service::workers::sample_fx_rate;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// The pinned fixture rates (`tests/fixtures/blocktank_fx_rates_btc.json`).
const PRICES: [(&str, &str); 7] = [
    ("USD", "77197"),
    ("EUR", "66481"),
    ("CHF", "62992"),
    ("GBP", "57037"),
    ("JPY", "11745955"),
    ("CAD", "106992"),
    ("AUD", "107607"),
];

/// Every fixture order totals 13,700 minor units of its currency (12,500 +
/// 1,200 shipping): 137.00 in two-decimal currencies, 13,700 JPY.
const TOTAL_MINOR: i64 = 13_700;

/// `ceil(13_700 * 10^8 / (10^exponent * BTC<CCY>))`, recomputed outside the
/// service with exact fractions.
const QUOTED_SATS: [(&str, u64); 7] = [
    ("USD", 177_469),
    ("EUR", 206_074),
    ("CHF", 217_488),
    ("GBP", 240_195),
    ("JPY", 116_636),
    ("CAD", 128_047),
    ("AUD", 127_316),
];

fn price_of(code: &str) -> &'static str {
    PRICES
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, price)| *price)
        .expect("fixture price")
}

fn sats_of(code: &str) -> u64 {
    QUOTED_SATS
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, sats)| *sats)
        .expect("quoted sats")
}

fn exponent_of(code: &str) -> i32 {
    Currency::parse(code).expect("registry currency").exponent()
}

/// The ticker timestamp the double serves for `now` (millisecond wire
/// precision).
fn ticker_time(now: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(now.timestamp_millis()).expect("timestamp")
}

fn body_at(now: DateTime<Utc>) -> String {
    fx_body_multi(&PRICES, now.timestamp_millis())
}

/// Seeds `count` accepted reference samples of one currency, one-minute
/// buckets ending at `now`, exactly what the sampler would have persisted.
async fn seed_samples(pool: &PgPool, currency: &str, rate: &str, now: DateTime<Utc>, count: i64) {
    for back in 0..count {
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

async fn seed_all_references(pool: &PgPool, now: DateTime<Utc>) {
    for (currency, price) in PRICES {
        seed_samples(pool, currency, price, now, 3).await;
    }
}

struct Fixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
}

async fn fixture(pool: PgPool) -> Fixture {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool).await;
    fx.set_body(body_at(app.clock.now()));
    Fixture { app, paykit, fx }
}

/// A fixed-price listing of 12,500 + 1,200 shipping minor units in
/// `currency`, registered by a new Bitcoin-enabled seller.
async fn seller_in(fixture: &Fixture, currency: &str) -> TestActor {
    let seller = new_actor(&fixture.app).await;
    enable_bitcoin(&fixture.app, &fixture.paykit, &seller).await;
    let mut command = register_command(&seller.pubky, 20);
    command["command_id"] = json!(Uuid::new_v4().to_string());
    command["payload"]["unit_price"] = json!({
        "amount_minor": 12_500,
        "currency": currency,
        "exponent": exponent_of(currency),
    });
    let (status, body) = execute(&fixture.app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "register {currency}: {body}");
    seller
}

async fn checkout(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    let mut command = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    command["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string()
}

async fn bind(app: &TestApp, token: &str, order_id: &str, method: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": method }),
    )
    .await
}

/// Starts a bitcoin bind on its own task so a test can interleave it.
fn spawn_bind(
    app: &TestApp,
    token: &str,
    order_id: &str,
) -> tokio::task::JoinHandle<(StatusCode, Value)> {
    let router = app.router.clone();
    let token = token.to_string();
    let order_id = order_id.to_string();
    tokio::spawn(async move {
        send(
            router,
            "POST",
            &format!("/v0/orders/{order_id}/payment-method"),
            Some(&token),
            &json!({ "method": "bitcoin" }),
        )
        .await
    })
}

#[derive(Debug, sqlx::FromRow)]
struct Facts {
    state: String,
    currency: String,
    exponent: i32,
    total_minor: i64,
    payment_method: Option<String>,
    stock_held: bool,
    bitcoin_quote_rate: Option<String>,
    bitcoin_quote_source: Option<String>,
    bitcoin_quote_fetched_at: Option<DateTime<Utc>>,
    bitcoin_quote_currency: Option<String>,
    bitcoin_quote_exponent: Option<i16>,
    bitcoin_quoted_sats: Option<i64>,
    paykit_total_sats: Option<i64>,
}

async fn facts(pool: &PgPool, order_id: &str) -> Facts {
    sqlx::query_as(
        "SELECT state, currency, exponent, total_minor, payment_method, stock_held, \
         bitcoin_quote_rate::text, bitcoin_quote_source, bitcoin_quote_fetched_at, \
         bitcoin_quote_currency::text, bitcoin_quote_exponent, bitcoin_quoted_sats, \
         paykit_total_sats FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order row")
}

fn assert_unbound_without_side_effects(facts: &Facts, paykit: &FakePaykit, context: &str) {
    assert_eq!(facts.state, "pending_payment", "{context}");
    assert_eq!(facts.payment_method, None, "{context}");
    assert!(!facts.stock_held, "{context}: no hold");
    assert_eq!(facts.bitcoin_quote_rate, None, "{context}: no quote");
    assert!(paykit.requests().is_empty(), "{context}: nothing prepared");
}

// ---------------------------------------------------------------------------
// Listing and order validation
// ---------------------------------------------------------------------------

fn listing_in(seller: &str, currency: &str, exponent: i32) -> Value {
    let mut command = register_command(seller, 5);
    command["command_id"] = json!(Uuid::new_v4().to_string());
    command["payload"]["unit_price"] =
        json!({ "amount_minor": 12_500, "currency": currency, "exponent": exponent });
    command
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_registry_currency_registers_and_the_rest_are_refused_with_a_typed_reason(
    pool: PgPool,
) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    for (code, exponent) in [
        ("USD", 2),
        ("EUR", 2),
        ("CHF", 2),
        ("GBP", 2),
        ("JPY", 0),
        ("CAD", 2),
        ("AUD", 2),
        ("BTC", 8),
        ("SAT", 0),
    ] {
        let seller = new_actor(&app).await;
        let (status, body) = execute(
            &app,
            &seller.token,
            &listing_in(&seller.pubky, code, exponent),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{code}/{exponent}: {body}");
    }

    let unsupported = "currency_unsupported";
    let mismatch = "currency_exponent_mismatch";
    let refused = [
        ("SEK", 2, unsupported, "payload.unit_price.currency"),
        ("USDT", 6, unsupported, "payload.unit_price.currency"),
        ("usd", 2, unsupported, "payload.unit_price.currency"),
        ("XBT", 8, unsupported, "payload.unit_price.currency"),
        ("USD", 3, mismatch, "payload.unit_price.exponent"),
        ("USD", 0, mismatch, "payload.unit_price.exponent"),
        ("JPY", 2, mismatch, "payload.unit_price.exponent"),
        ("EUR", 0, mismatch, "payload.unit_price.exponent"),
        ("SAT", 2, mismatch, "payload.unit_price.exponent"),
        ("BTC", 0, mismatch, "payload.unit_price.exponent"),
    ];
    for (code, exponent, reason, path) in refused {
        let seller = new_actor(&app).await;
        let (status, body) = execute(
            &app,
            &seller.token,
            &listing_in(&seller.pubky, code, exponent),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{code}/{exponent}: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        assert_eq!(body["error"]["reason"], json!(reason), "{code}/{exponent}");
        let issues = body["error"]["issues"].as_array().expect("issues");
        assert!(
            issues
                .iter()
                .any(|issue| issue["path"] == json!(path) && issue["reason"] == json!(reason)),
            "{code}/{exponent}: {issues:?}"
        );
        if reason == unsupported {
            assert!(
                body.to_string()
                    .contains("USD, EUR, CHF, GBP, JPY, CAD, AUD, BTC, SAT"),
                "the refusal names the supported currencies: {body}"
            );
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_and_bids_outside_the_registry_are_refused_before_any_state_changes(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut bid = place_bid_command(&seller.pubky, 90, 10_000, 1);
    bid["payload"]["maximum_amount"]["currency"] = json!("SEK");
    let (status, body) = execute(&app, &buyer.token, &bid).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    let bids: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bids")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(bids, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_locks_listing_priced_in_a_currency_the_lock_server_cannot_count_is_refused(
    pool: PgPool,
) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    let locked = |seller: &TestActor, code: &str, exponent: i32| {
        let mut listing = listing_in(&seller.pubky, code, exponent);
        listing["payload"]["digital_lock"] = json!({
            "policyUri": lock_resource_for(&seller.pubky),
            "criterionId": "paykit",
        });
        as_digital_locks_listing(&mut listing);
        listing
    };
    // USD, BTC and SAT are the Lock Server's units.
    for (code, exponent) in [("USD", 2), ("BTC", 8), ("SAT", 0)] {
        let seller = new_actor(&app).await;
        let (status, body) = execute(&app, &seller.token, &locked(&seller, code, exponent)).await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
    }
    for (code, exponent) in [
        ("EUR", 2),
        ("CHF", 2),
        ("GBP", 2),
        ("JPY", 0),
        ("CAD", 2),
        ("AUD", 2),
    ] {
        let seller = new_actor(&app).await;
        let (status, body) = execute(&app, &seller.token, &locked(&seller, code, exponent)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{code}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!("locks_currency_unsupported"),
            "{code}"
        );
        // The same currency without a lock is an ordinary listing.
        let seller = new_actor(&app).await;
        let (status, body) = execute(
            &app,
            &seller.token,
            &listing_in(&seller.pubky, code, exponent),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{code} without a lock: {body}");
    }
}

// ---------------------------------------------------------------------------
// The legacy contract: every currency quotes sats with its own ticker
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_fiat_currency_binds_with_its_own_ticker_and_rounds_up(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;

    for (index, (code, price)) in PRICES.into_iter().enumerate() {
        let seller = seller_in(&fixture, code).await;
        let order_id = checkout(&fixture.app, &seller, &buyer).await;
        let before = facts(&pool, &order_id).await;
        assert_eq!(
            (before.currency.as_str(), before.exponent),
            (code, exponent_of(code))
        );
        assert_eq!(before.total_minor, TOTAL_MINOR);

        let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let expected = sats_of(code);
        let quote = &body["order"]["bitcoin_quote"];
        assert_eq!(quote["rate"], json!(price), "{code}");
        assert_eq!(quote["pair"], json!(format!("BTC{code}")), "{code}");
        assert_eq!(quote["source"], json!("blocktank"));
        assert_eq!(quote["currency"], json!(code));
        assert_eq!(quote["exponent"], json!(exponent_of(code)));
        assert_eq!(quote["quoted_sats"], json!(expected), "{code}");
        assert_eq!(quote["spread_bps"], json!(0));
        assert_eq!(
            quote["fetched_at"],
            json!(marketplace_service::clock::format_timestamp(ticker_time(
                now
            ))),
            "the quote carries the ticker's own time"
        );
        // paykit-server adds its nonce to the amount it was asked for.
        assert!(
            body["order"]["paykit_total_sats"].as_u64().expect("total") >= expected,
            "{code}"
        );
        // The listing-currency total stays the order's price of record.
        assert_eq!(body["order"]["total"]["currency"], json!(code));
        assert_eq!(body["order"]["total"]["amount_minor"], json!(TOTAL_MINOR));

        let stored = facts(&pool, &order_id).await;
        assert_eq!(stored.payment_method.as_deref(), Some("bitcoin"));
        assert_eq!(stored.bitcoin_quote_rate.as_deref(), Some(price));
        assert_eq!(stored.bitcoin_quote_source.as_deref(), Some("blocktank"));
        assert_eq!(stored.bitcoin_quote_fetched_at, Some(ticker_time(now)));
        assert_eq!(stored.bitcoin_quote_currency.as_deref(), Some(code));
        assert_eq!(
            stored.bitcoin_quote_exponent,
            Some(exponent_of(code) as i16)
        );
        assert_eq!(stored.bitcoin_quoted_sats, Some(expected as i64));
        assert!(stored.paykit_total_sats.expect("paykit total") >= expected as i64);
        assert!(stored.stock_held);
        let requests = fixture.paykit.requests();
        assert_eq!(requests.len(), index + 1);
        assert_eq!(requests[index].amount_sats, expected, "{code}");
    }
    assert_eq!(fixture.fx.fetch_count(), 7, "one snapshot fetch per bind");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_priced_listing_binds_without_a_feed_and_keeps_its_amount(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let buyer = new_actor(&fixture.app).await;
    for (code, exponent) in [("SAT", 0), ("BTC", 8)] {
        let seller = new_actor(&fixture.app).await;
        enable_bitcoin(&fixture.app, &fixture.paykit, &seller).await;
        let mut command = register_command(&seller.pubky, 5);
        command["command_id"] = json!(Uuid::new_v4().to_string());
        command["payload"]["unit_price"] =
            json!({ "amount_minor": 50_000, "currency": code, "exponent": exponent });
        command["payload"]["shipping_minor"] = json!(0);
        let (status, body) = execute(&fixture.app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let order_id = checkout(&fixture.app, &seller, &buyer).await;
        let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        assert_eq!(
            fixture
                .paykit
                .requests()
                .last()
                .expect("prepared")
                .amount_sats,
            50_000,
            "the price itself is the amount asked of paykit"
        );
        assert_eq!(
            body["order"]["bitcoin_quote"]["rate"],
            json!(null),
            "a satoshi price has no FX quote"
        );
        assert!(body["order"]["bitcoin_quote"].get("pair").is_none());
    }
    assert_eq!(
        fixture.fx.fetch_count(),
        0,
        "a sats price never reads the feed"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_in_a_currency_outside_the_registry_is_refused_before_any_work(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    seed_all_references(&pool, fixture.app.clock.now()).await;
    let buyer = new_actor(&fixture.app).await;
    let seller = seller_in(&fixture, "EUR").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    sqlx::query("UPDATE orders SET currency = 'SEK' WHERE id = $1::uuid")
        .bind(&order_id)
        .execute(&pool)
        .await
        .unwrap();
    let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    assert_eq!(fixture.fx.fetch_count(), 0);
    assert!(fixture.paykit.requests().is_empty());
}

// ---------------------------------------------------------------------------
// Guards are per currency
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn each_guard_refuses_only_the_currency_it_failed_for(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let current = now.timestamp_millis();
    let ticker = |currency: &str, price: &str, updated: i64| {
        format!(
            r#"{{"symbol":"BTC{currency}","lastPrice":"{price}","base":"BTC","quote":"{currency}","lastUpdatedAt":{updated}}}"#
        )
    };
    // USD stays healthy. EUR is stale, JPY from the future, GBP out of its
    // band, CHF carries no ticker, CAD is zero, AUD deviates 10% from the
    // persisted reference.
    let feed = format!(
        r#"{{"tickers":[{},{},{},{},{},{}]}}"#,
        ticker("USD", price_of("USD"), current),
        ticker("EUR", price_of("EUR"), current - 301_000),
        ticker("JPY", price_of("JPY"), current + 61_000),
        ticker("GBP", "5999", current),
        ticker("CAD", "0", current),
        ticker("AUD", "118368", current),
    );
    fixture.fx.set_body(feed);
    let buyer = new_actor(&fixture.app).await;
    let expected = [
        ("EUR", "fx_rate_stale"),
        ("JPY", "fx_rate_stale"),
        ("GBP", "fx_rate_out_of_bounds"),
        ("CHF", "fx_reference_unavailable"),
        ("CAD", "fx_rate_out_of_bounds"),
        ("AUD", "fx_deviation_exceeded"),
    ];
    for (code, reason) in expected {
        let seller = seller_in(&fixture, code).await;
        let order_id = checkout(&fixture.app, &seller, &buyer).await;
        let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
        assert_eq!(status, StatusCode::CONFLICT, "{code}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason), "{code}: {body}");
        assert_unbound_without_side_effects(&facts(&pool, &order_id).await, &fixture.paykit, code);
    }
    let seller = seller_in(&fixture, "USD").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "USD is unaffected: {body}");
    assert_eq!(fixture.paykit.requests().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_deviation_bound_is_five_percent_of_each_currencys_own_reference(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;
    for (code, reference) in [("EUR", 66_481u64), ("JPY", 11_745_955u64)] {
        let inside = reference * 105 / 100;
        let outside = inside + 1;
        // The bound is inclusive: exactly 105% of the reference binds, one
        // unit more is refused.
        for (rate, accepted) in [(inside, true), (outside, false)] {
            fixture.fx.set_body(fx_body_multi(
                &[("USD", price_of("USD")), (code, &rate.to_string())],
                now.timestamp_millis(),
            ));
            let seller = seller_in(&fixture, code).await;
            let order_id = checkout(&fixture.app, &seller, &buyer).await;
            let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
            if accepted {
                assert_eq!(status, StatusCode::OK, "{code} at {rate}: {body}");
                assert_eq!(
                    body["order"]["bitcoin_quote"]["rate"],
                    json!(rate.to_string())
                );
            } else {
                assert_eq!(status, StatusCode::CONFLICT, "{code} at {rate}: {body}");
                assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));
            }
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_currency_with_fewer_than_three_samples_is_refused_without_a_fetch(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_samples(&pool, "USD", price_of("USD"), now, 3).await;
    seed_samples(&pool, "EUR", price_of("EUR"), now, 2).await;
    let buyer = new_actor(&fixture.app).await;

    let seller = seller_in(&fixture, "EUR").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(fixture.fx.fetch_count(), 0, "no reference, no fetch");
    assert_unbound_without_side_effects(&facts(&pool, &order_id).await, &fixture.paykit, "EUR");

    // Another currency's samples never stand in for it: JPY has none.
    let seller = seller_in(&fixture, "JPY").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(fixture.fx.fetch_count(), 0);

    // A third EUR sample completes the reference.
    seed_samples(
        &pool,
        "EUR",
        price_of("EUR"),
        now - Duration::seconds(180),
        1,
    )
    .await;
    let seller = seller_in(&fixture, "EUR").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fixture.fx.fetch_count(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_feed_leaves_every_currency_unbound_with_no_side_effects(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    seed_all_references(&pool, fixture.app.clock.now()).await;
    let buyer = new_actor(&fixture.app).await;
    fixture.fx.set_status(500);
    for (code, _) in PRICES {
        let seller = seller_in(&fixture, code).await;
        let order_id = checkout(&fixture.app, &seller, &buyer).await;
        let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
        assert_eq!(status, StatusCode::CONFLICT, "{code}: {body}");
        assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
        assert_unbound_without_side_effects(&facts(&pool, &order_id).await, &fixture.paykit, code);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_quote_below_the_minimum_or_above_the_cap_is_refused_for_every_currency(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    seed_all_references(&pool, fixture.app.clock.now()).await;
    let buyer = new_actor(&fixture.app).await;
    // 100 JPY is 852 sats (under 1,000): refused. 100,000,000 JPY is 851
    // million sats (over 1 BTC): refused. 1,000 JPY (8,514 sats) binds.
    for (total, reason) in [
        (100, Some("below_minimum_sats")),
        (100_000_000, Some("fx_rate_out_of_bounds")),
        (1_000, None),
    ] {
        let seller = new_actor(&fixture.app).await;
        enable_bitcoin(&fixture.app, &fixture.paykit, &seller).await;
        let mut command = register_command(&seller.pubky, 5);
        command["command_id"] = json!(Uuid::new_v4().to_string());
        command["payload"]["unit_price"] =
            json!({ "amount_minor": total, "currency": "JPY", "exponent": 0 });
        command["payload"]["shipping_minor"] = json!(0);
        let (status, body) = execute(&fixture.app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let order_id = checkout(&fixture.app, &seller, &buyer).await;
        let (status, body) = bind(&fixture.app, &buyer.token, &order_id, "bitcoin").await;
        match reason {
            Some(reason) => {
                assert_eq!(status, StatusCode::CONFLICT, "{total} JPY: {body}");
                assert_eq!(body["error"]["reason"], json!(reason), "{total} JPY");
            }
            None => {
                assert_eq!(status, StatusCode::OK, "{total} JPY: {body}");
                assert_eq!(
                    fixture
                        .paykit
                        .requests()
                        .last()
                        .expect("prepared")
                        .amount_sats,
                    8_514
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The sampler keeps one reference per currency
// ---------------------------------------------------------------------------

async fn sample_rows(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT currency::text, rate::text FROM fx_rate_samples ORDER BY currency, sample_bucket",
    )
    .fetch_all(pool)
    .await
    .expect("sample rows")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_sampler_stores_one_sample_per_currency_from_one_fetch(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    let url = &fixture.app.state.config.fx_feed_url;

    assert_eq!(sample_fx_rate(&pool, url, now).await.unwrap(), 7);
    assert_eq!(
        fixture.fx.fetch_count(),
        1,
        "one fetch feeds every currency"
    );
    let mut expected: Vec<(String, String)> = PRICES
        .iter()
        .map(|(currency, price)| (currency.to_string(), price.to_string()))
        .collect();
    expected.sort();
    assert_eq!(sample_rows(&pool).await, expected);

    assert_eq!(
        sample_fx_rate(&pool, url, now).await.unwrap(),
        0,
        "the bucket constraint caps each currency at one sample"
    );
    assert_eq!(sample_rows(&pool).await.len(), 7);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_sampler_rejects_an_outlier_for_its_own_currency_only(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now - Duration::seconds(60)).await;
    let url = &fixture.app.state.config.fx_feed_url;
    // EUR jumps 10%; JPY carries no ticker; GBP is stale; the rest hold.
    let current = now.timestamp_millis();
    let tickers: Vec<String> = PRICES
        .iter()
        .filter(|(currency, _)| *currency != "JPY")
        .map(|(currency, price)| {
            let (price, updated) = match *currency {
                "EUR" => ("73129".to_string(), current),
                "GBP" => (price.to_string(), current - 400_000),
                _ => (price.to_string(), current),
            };
            format!(
                r#"{{"symbol":"BTC{currency}","lastPrice":"{price}","base":"BTC","quote":"{currency}","lastUpdatedAt":{updated}}}"#
            )
        })
        .collect();
    fixture
        .fx
        .set_body(format!(r#"{{"tickers":[{}]}}"#, tickers.join(",")));

    let accepted = sample_fx_rate(&pool, url, now).await.unwrap();
    assert_eq!(accepted, 4, "USD, CHF, CAD and AUD");
    let stored: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT currency::text FROM fx_rate_samples \
         WHERE accepted_at = $1 ORDER BY 1",
    )
    .bind(now)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, ["AUD", "CAD", "CHF", "USD"]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usd_only_feed_still_samples_usd(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    fixture
        .fx
        .set_body(fx_body(price_of("USD"), now.timestamp_millis()));
    let url = &fixture.app.state.config.fx_feed_url;
    assert_eq!(sample_fx_rate(&pool, url, now).await.unwrap(), 1);
    assert_eq!(
        sample_rows(&pool).await,
        [("USD".to_string(), "77197".to_string())]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn rate_bounds_match_the_table_check(pool: PgPool) {
    for (index, currency) in Currency::FIAT.into_iter().enumerate() {
        let (low, high) = fx::rate_bounds(currency).expect("fiat bounds");
        let insert = |rate: u128, bucket: i64| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "INSERT INTO fx_rate_samples \
                     (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
                     VALUES ($1, $2::numeric, now(), now(), to_timestamp($3), 'blocktank')",
                )
                .bind(currency.code())
                .bind(rate.to_string())
                .bind(1_800_000_000 + index as i64 * 100 + bucket)
                .execute(&pool)
                .await
            }
        };
        insert(low, 0)
            .await
            .unwrap_or_else(|e| panic!("{currency} low: {e}"));
        insert(high, 1)
            .await
            .unwrap_or_else(|e| panic!("{currency} high: {e}"));
        assert!(insert(low - 1, 2).await.is_err(), "{currency} below");
        assert!(insert(high + 1, 3).await.is_err(), "{currency} above");
    }
    let outside = sqlx::query(
        "INSERT INTO fx_rate_samples \
         (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
         VALUES ('SEK', 100000, now(), now(), now(), 'blocktank')",
    )
    .execute(&pool)
    .await;
    assert!(
        outside.is_err(),
        "only the seven fiat currencies are sampled"
    );
}

// ---------------------------------------------------------------------------
// The USD conversion of a non-USD price (multi-asset Paykit denomination)
// ---------------------------------------------------------------------------

async fn quote_order(
    fixture: &Fixture,
    currency: Currency,
    total_minor: i64,
) -> Result<fx::OrderQuote, FxError> {
    let mut conn = fixture.app.pool.acquire().await.expect("connection");
    fx::quote_order(
        &mut conn,
        &fixture.app.state.config.fx_feed_url,
        currency,
        total_minor,
        fixture.app.clock.now(),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_non_usd_price_converts_to_usd_cents_at_the_cross_rate_of_one_snapshot(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let cases = [
        (Currency::Eur, 15_909u64),
        (Currency::Chf, 16_790),
        (Currency::Gbp, 18_543),
        (Currency::Jpy, 9_004),
        (Currency::Cad, 9_885),
        (Currency::Aud, 9_829),
    ];
    for (index, (currency, cents)) in cases.into_iter().enumerate() {
        let quote = quote_order(&fixture, currency, TOTAL_MINOR)
            .await
            .expect("quote");
        assert_eq!(quote.usd.cents, cents, "{currency}");
        assert_eq!(quote.sats, Ok(sats_of(currency.code())), "{currency}");
        let cross = quote.usd.cross.expect("a non-USD price has a cross rate");
        assert_eq!(cross.pair, format!("{}USD", currency.code()));
        assert_eq!(cross.usd_rate.to_string_value(), price_of("USD"));
        assert_eq!(
            cross.currency_rate.to_string_value(),
            price_of(currency.code())
        );
        assert_eq!(cross.fetched_at, ticker_time(now));
        assert_eq!(
            quote.direct.rate.to_string_value(),
            price_of(currency.code())
        );
        assert_eq!(
            fixture.fx.fetch_count(),
            index + 1,
            "both legs and the direct quote come from one fetch"
        );
    }

    let usd = quote_order(&fixture, Currency::Usd, TOTAL_MINOR)
        .await
        .expect("usd");
    assert_eq!(usd.usd.cents, TOTAL_MINOR as u64, "USD is the identity");
    assert_eq!(usd.usd.cross, None);
    assert_eq!(usd.sats, Ok(177_469));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_cross_rate_guards_both_legs_against_their_own_references(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    let current = now.timestamp_millis();
    seed_all_references(&pool, now).await;

    // USD leg 10% off its reference while EUR holds: refused, though the
    // direct EUR rate alone is fine.
    fixture.fx.set_body(fx_body_multi(
        &[("USD", "84917"), ("EUR", price_of("EUR"))],
        current,
    ));
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::DeviationExceeded)
    );
    // EUR leg 10% off while USD holds.
    fixture.fx.set_body(fx_body_multi(
        &[("USD", price_of("USD")), ("EUR", "73129")],
        current,
    ));
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::DeviationExceeded)
    );
    // A stale USD ticker refuses a EUR conversion.
    let stale = format!(
        r#"{{"tickers":[
            {{"symbol":"BTCUSD","lastPrice":"77197","base":"BTC","quote":"USD","lastUpdatedAt":{}}},
            {{"symbol":"BTCEUR","lastPrice":"66481","base":"BTC","quote":"EUR","lastUpdatedAt":{current}}}
        ]}}"#,
        current - 301_000
    );
    fixture.fx.set_body(stale);
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::Stale)
    );
    // No USD ticker at all.
    fixture
        .fx
        .set_body(fx_body_multi(&[("EUR", price_of("EUR"))], current));
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::Malformed)
    );
    // A reference below three samples for either leg is a missing reference
    // and costs no fetch.
    fixture.fx.set_body(body_at(now));
    sqlx::query("DELETE FROM fx_rate_samples WHERE currency = 'USD'")
        .execute(&pool)
        .await
        .unwrap();
    let fetches = fixture.fx.fetch_count();
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::MissingReference)
    );
    assert_eq!(fixture.fx.fetch_count(), fetches);
    // A restored USD reference but no EUR reference.
    seed_samples(&pool, "USD", price_of("USD"), now, 3).await;
    sqlx::query("DELETE FROM fx_rate_samples WHERE currency = 'EUR'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        quote_order(&fixture, Currency::Eur, TOTAL_MINOR).await,
        Err(FxError::MissingReference)
    );
    assert_eq!(fixture.fx.fetch_count(), fetches);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_only_request_needs_no_bitcoin_amount_so_a_tiny_price_still_converts(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    seed_all_references(&pool, fixture.app.clock.now()).await;
    // 1 JPY is 9 sats (under the 1,000-sat minimum) but one USD cent.
    let quote = quote_order(&fixture, Currency::Jpy, 1)
        .await
        .expect("quote");
    assert_eq!(quote.sats, Err(FxError::BelowMinimum));
    assert_eq!(quote.usd.cents, 1);
    // 100 billion JPY exceeds the one-bitcoin cap for the Bitcoin leg only.
    let quote = quote_order(&fixture, Currency::Jpy, 100_000_000_000)
        .await
        .expect("quote");
    assert_eq!(quote.sats, Err(FxError::CapExceeded));
    assert!(quote.usd.cents > 0);
    // Bitcoin spellings are not fiat and have no ticker.
    assert_eq!(
        quote_order(&fixture, Currency::Sat, 1_000).await,
        Err(FxError::Malformed)
    );
}

// ---------------------------------------------------------------------------
// PayPal charges in the listing currency
// ---------------------------------------------------------------------------

async fn paypal_order(
    app: &TestApp,
    currency: &str,
    unit_minor: i64,
    shipping_minor: i64,
) -> (TestActor, TestActor, String) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut command = register_command(&seller.pubky, 5);
    command["command_id"] = json!(Uuid::new_v4().to_string());
    command["payload"]["unit_price"] = json!({
        "amount_minor": unit_minor,
        "currency": currency,
        "exponent": exponent_of(currency),
    });
    command["payload"]["shipping_minor"] = json!(shipping_minor);
    let (status, body) = execute(app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{currency}: {body}");
    let order_id = checkout(app, &seller, &buyer).await;
    (seller, buyer, order_id)
}

fn ipn(
    order_id: &str,
    status: &str,
    currency: &str,
    gross: &str,
    txn: &str,
) -> Vec<(String, String)> {
    [
        ("payment_status", status),
        ("receiver_email", "merchant@example.com"),
        ("business", "merchant@example.com"),
        ("mc_gross", gross),
        ("mc_currency", currency),
        ("custom", order_id),
        ("txn_id", txn),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .collect()
}

fn encode(fields: &[(String, String)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in fields {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

async fn post_ipn(app: &TestApp, fields: Vec<(String, String)>) -> StatusCode {
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        encode(&fields).into_bytes(),
    )
    .await;
    status
}

async fn order_view(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paypal_charges_every_supported_currency_natively(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    // 12,500 + 1,200 minor units: 137.00 in two-decimal currencies and
    // 13,700 (no decimal point) in JPY.
    for (code, amount) in [
        ("USD", "137.00"),
        ("EUR", "137.00"),
        ("CHF", "137.00"),
        ("GBP", "137.00"),
        ("JPY", "13700"),
        ("CAD", "137.00"),
        ("AUD", "137.00"),
    ] {
        let (_seller, buyer, order_id) = paypal_order(&app, code, 12_500, 1_200).await;
        let (status, body) = bind(&app, &buyer.token, &order_id, "paypal").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let url = url::Url::parse(
            body["order"]["fiat_checkout_url"]
                .as_str()
                .expect("checkout url"),
        )
        .expect("url");
        let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs["amount"], amount, "{code}");
        assert_eq!(pairs["currency_code"], code);
        assert_eq!(body["order"]["total"]["currency"], json!(code));
        assert_eq!(body["order"]["payment_method"], json!("paypal"));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_verified_ipn_in_the_listing_currency_pays_eur_and_jpy_orders(pool: PgPool) {
    let (app, _paykit, ipn_double) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, (code, exponent, gross, wrong_gross)) in
        [("EUR", 2, "137.00", "13700"), ("JPY", 0, "13700", "137.00")]
            .into_iter()
            .enumerate()
    {
        let (_seller, buyer, order_id) = paypal_order(&app, code, 12_500, 1_200).await;
        let (status, body) = bind(&app, &buyer.token, &order_id, "paypal").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let txn = format!("7XP31449AB12345{index}C");

        // Wrong currency (the USD fixture currency), wrong amount for the
        // currency's exponent, and an underpayment are dropped.
        for (currency, amount) in [("USD", gross), (code, wrong_gross), (code, "1")] {
            assert_eq!(
                post_ipn(&app, ipn(&order_id, "Completed", currency, amount, &txn)).await,
                StatusCode::OK
            );
            let view = order_view(&app, &buyer.token, &order_id).await;
            assert_eq!(
                view["state"],
                json!("pending_payment"),
                "{code} {currency} {amount}"
            );
        }

        assert_eq!(
            post_ipn(&app, ipn(&order_id, "Completed", code, gross, &txn)).await,
            StatusCode::OK
        );
        let view = order_view(&app, &buyer.token, &order_id).await;
        assert_eq!(view["state"], json!("paid"), "{code}: {view}");
        assert_eq!(view["fiat_verification"], json!("gateway-notified"));
        assert_eq!(view["total"]["currency"], json!(code));
        assert_eq!(view["total"]["exponent"], json!(exponent));
        assert_eq!(view["total"]["amount_minor"], json!(TOTAL_MINOR));
        let receipt: (String, i32, i64) = sqlx::query_as(
            "SELECT currency, exponent, amount_minor FROM payments WHERE order_id = $1::uuid",
        )
        .bind(&order_id)
        .fetch_one(&pool)
        .await
        .expect("payment row");
        assert_eq!(receipt, (code.to_string(), exponent, TOTAL_MINOR));
    }
    assert!(!ipn_double.postbacks().is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paypal_refund_is_recorded_in_the_listing_currency(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, (code, gross, refund_gross)) in
        [("EUR", "137.00", "-137.00"), ("JPY", "13700", "-13700")]
            .into_iter()
            .enumerate()
    {
        let (_seller, buyer, order_id) = paypal_order(&app, code, 12_500, 1_200).await;
        let (status, body) = bind(&app, &buyer.token, &order_id, "paypal").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let payment_txn = format!("8YQ42550BC23456{index}D");
        assert_eq!(
            post_ipn(&app, ipn(&order_id, "Completed", code, gross, &payment_txn)).await,
            StatusCode::OK
        );

        // A refund in another currency is held, never applied.
        let mut wrong = ipn(
            &order_id,
            "Refunded",
            "USD",
            refund_gross,
            &format!("9ZR5366{index}CD34567E"),
        );
        wrong.push(("parent_txn_id".to_string(), payment_txn.clone()));
        assert_eq!(post_ipn(&app, wrong).await, StatusCode::OK);
        let view = order_view(&app, &buyer.token, &order_id).await;
        assert_eq!(
            view["state"],
            json!("paid"),
            "{code}: wrong-currency refund held"
        );

        let mut refund = ipn(
            &order_id,
            "Refunded",
            code,
            refund_gross,
            &format!("2WF5816{index}VJ0384519"),
        );
        refund.push(("parent_txn_id".to_string(), payment_txn.clone()));
        refund.push(("receiver_id".to_string(), "S8XGHLYDW9T3S".to_string()));
        assert_eq!(post_ipn(&app, refund).await, StatusCode::OK);
        let view = order_view(&app, &buyer.token, &order_id).await;
        assert_eq!(view["state"], json!("refunded_external"), "{code}: {view}");
        assert_eq!(
            view["external_refund"]["amount_minor"],
            json!(TOTAL_MINOR),
            "{code}"
        );
        assert_eq!(
            view["total"]["currency"],
            json!(code),
            "the refund is in the order's own currency"
        );
    }
}

// ---------------------------------------------------------------------------
// Auctions are currency-generic
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_eur_auction_winner_binds_with_a_eur_quote(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let seller = new_actor(&fixture.app).await;
    let winner = new_actor(&fixture.app).await;
    let (status, body) = send(
        fixture.app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fixture.paykit.set_claimed(&seller.pubky);

    let mut auction = register_auction_command(&seller.pubky);
    auction["payload"]["unit_price"]["currency"] = json!("EUR");
    auction["payload"]["auction_terms"]["minimum_increment"]["currency"] = json!("EUR");
    auction["payload"]["auction_reserve"]["reserve_price"]["currency"] = json!("EUR");
    let (status, body) = execute(&fixture.app, &seller.token, &auction).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut bid = place_bid_command(&seller.pubky, 20, 10_000, 1);
    bid["payload"]["maximum_amount"]["currency"] = json!("EUR");
    let (status, body) = execute(&fixture.app, &winner.token, &bid).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The same bid in USD on a EUR auction is not comparable.
    let mut usd_bid = place_bid_command(&seller.pubky, 21, 12_000, 2);
    usd_bid["payload"]["maximum_amount"]["currency"] = json!("USD");
    let (status, _) = execute(&fixture.app, &winner.token, &usd_bid).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let runner_up = new_actor(&fixture.app).await;
    let mut lower = place_bid_command(&seller.pubky, 22, 8_000, 2);
    lower["payload"]["maximum_amount"]["currency"] = json!("EUR");
    let (status, body) = execute(&fixture.app, &runner_up.token, &lower).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fixture.app.clock.advance_seconds(11 * 60);
    let (status, body) = execute(
        &fixture.app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 950),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_id, currency, total): (String, String, i64) = sqlx::query_as(
        "SELECT id::text, currency, total_minor FROM orders \
         WHERE buyer_pubky = $1 AND auction_aggregate_id IS NOT NULL",
    )
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("winning order");
    assert_eq!(currency, "EUR");

    fixture.fx.set_body(body_at(fixture.app.clock.now()));
    let address = json!({
        "name": "Ada Lovelace", "line1": "12 Analytical St", "line2": "",
        "city": "London", "region": "", "postal_code": "N1 9GU", "country_code": "GB",
    });
    let (status, body) = send(
        fixture.app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &json!({ "method": "bitcoin", "delivery_address": address }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let expected = ((total as u128) * 100_000_000).div_ceil(100 * 66_481) as u64;
    assert_eq!(body["order"]["bitcoin_quote"]["pair"], json!("BTCEUR"));
    assert_eq!(body["order"]["bitcoin_quote"]["rate"], json!("66481"));
    assert_eq!(
        body["order"]["bitcoin_quote"]["quoted_sats"],
        json!(expected)
    );
    assert_eq!(body["order"]["total"]["currency"], json!("EUR"));
}

// ---------------------------------------------------------------------------
// The quote under interleaved and concurrent binds
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_that_overlaps_the_first_bind_returns_its_quote_and_a_second_order_quotes_fresh(
    pool: PgPool,
) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;
    let seller = seller_in(&fixture, "EUR").await;
    let first = checkout(&fixture.app, &seller, &buyer).await;
    let second = checkout(&fixture.app, &seller, &buyer).await;
    let current = now.timestamp_millis();

    // Bind the first order and hold its feed request after the feed has
    // answered (the body is taken at rate 66,481 on arrival).
    let held = fixture.fx.hold_next_fetch();
    let first_bind = spawn_bind(&fixture.app, &buyer.token, &first);
    held.request_arrived().await;

    // The market moves 2% while the first bind is in flight.
    fixture.fx.set_body(fx_body_multi(
        &[("USD", "77197"), ("EUR", "67811")],
        current,
    ));

    // A replay of the same bind queues behind the first (the order row is
    // locked), and a bind of the other order quotes at the new rate at once.
    let replay = spawn_bind(&fixture.app, &buyer.token, &first);
    let (status, other) = bind(&fixture.app, &buyer.token, &second, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{other}");
    assert_eq!(other["order"]["bitcoin_quote"]["rate"], json!("67811"));

    held.release();
    let (status, first_body) = first_bind.await.expect("first bind task");
    assert_eq!(status, StatusCode::OK, "{first_body}");
    let (status, replay_body) = replay.await.expect("replay task");
    assert_eq!(status, StatusCode::OK, "{replay_body}");

    let first_quote = &first_body["order"]["bitcoin_quote"];
    assert_eq!(
        first_quote["rate"],
        json!("66481"),
        "fixed when it was fetched"
    );
    assert_eq!(first_quote["quoted_sats"], json!(sats_of("EUR")));
    assert_eq!(
        &replay_body["order"]["bitcoin_quote"], first_quote,
        "the overlapping replay returns the stored quote, never a requote"
    );
    let stored_first = facts(&pool, &first).await;
    assert_eq!(stored_first.bitcoin_quote_rate.as_deref(), Some("66481"));
    assert_eq!(
        stored_first.bitcoin_quoted_sats,
        Some(sats_of("EUR") as i64)
    );
    let stored_second = facts(&pool, &second).await;
    assert_eq!(stored_second.bitcoin_quote_rate.as_deref(), Some("67811"));
    let second_sats = (13_700u128 * 100_000_000).div_ceil(100 * 67_811) as i64;
    assert_eq!(stored_second.bitcoin_quoted_sats, Some(second_sats));

    let requests = fixture.paykit.requests();
    assert_eq!(
        requests.len(),
        2,
        "one prepare per order, none for the replay"
    );
    let mut amounts: Vec<u64> = requests.iter().map(|request| request.amount_sats).collect();
    amounts.sort_unstable();
    let mut expected = vec![sats_of("EUR"), second_sats as u64];
    expected.sort_unstable();
    assert_eq!(
        amounts, expected,
        "each prepare carries its own order's quote"
    );
    assert_eq!(fixture.fx.fetch_count(), 2, "the replay fetched nothing");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bind_refused_on_a_stale_ticker_leaves_the_queued_bind_to_quote_fresh(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;
    let seller = seller_in(&fixture, "JPY").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;
    let current = now.timestamp_millis();

    // The held fetch answers with a stale JPY ticker.
    fixture.fx.set_body(fx_body_multi(
        &[("USD", "77197"), ("JPY", price_of("JPY"))],
        current - 301_000,
    ));
    let held = fixture.fx.hold_next_fetch();
    let stale_bind = spawn_bind(&fixture.app, &buyer.token, &order_id);
    held.request_arrived().await;
    fixture.fx.set_body(body_at(now));
    let queued = spawn_bind(&fixture.app, &buyer.token, &order_id);
    held.release();

    let (status, body) = stale_bind.await.expect("stale bind task");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_rate_stale"));
    let (status, body) = queued.await.expect("queued bind task");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["order"]["bitcoin_quote"]["rate"],
        json!(price_of("JPY"))
    );
    assert_eq!(
        body["order"]["bitcoin_quote"]["quoted_sats"],
        json!(sats_of("JPY"))
    );
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.bitcoin_quoted_sats, Some(sats_of("JPY") as i64));
    assert_eq!(fixture.paykit.requests().len(), 1, "exactly one prepare");
    assert_eq!(fixture.fx.fetch_count(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn many_concurrent_binds_of_one_eur_order_store_one_quote_and_prepare_once(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;
    let seller = seller_in(&fixture, "EUR").await;
    let order_id = checkout(&fixture.app, &seller, &buyer).await;

    let tasks: Vec<_> = (0..8)
        .map(|_| spawn_bind(&fixture.app, &buyer.token, &order_id))
        .collect();
    let mut quotes = Vec::new();
    for task in tasks {
        let (status, body) = task.await.expect("bind task");
        assert_eq!(status, StatusCode::OK, "{body}");
        quotes.push(body["order"]["bitcoin_quote"].clone());
    }
    assert!(
        quotes.iter().all(|quote| quote == &quotes[0]),
        "every racer observes the stored quote: {quotes:?}"
    );
    assert_eq!(quotes[0]["quoted_sats"], json!(sats_of("EUR")));
    assert_eq!(fixture.fx.fetch_count(), 1, "exactly one racer fetched");
    assert_eq!(fixture.paykit.requests().len(), 1, "exactly one prepare");
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.bitcoin_quoted_sats, Some(sats_of("EUR") as i64));
    assert!(stored.stock_held);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_binds_of_different_currencies_each_quote_their_own_ticker(pool: PgPool) {
    let fixture = fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_all_references(&pool, now).await;
    let buyer = new_actor(&fixture.app).await;
    let mut orders = Vec::new();
    for (code, _) in PRICES {
        let seller = seller_in(&fixture, code).await;
        orders.push((code, checkout(&fixture.app, &seller, &buyer).await));
    }
    let tasks: Vec<_> = orders
        .iter()
        .map(|(code, order_id)| (*code, spawn_bind(&fixture.app, &buyer.token, order_id)))
        .collect();
    for (code, task) in tasks {
        let (status, body) = task.await.expect("bind task");
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
        let quote = &body["order"]["bitcoin_quote"];
        assert_eq!(quote["pair"], json!(format!("BTC{code}")), "{code}");
        assert_eq!(quote["rate"], json!(price_of(code)), "{code}");
        assert_eq!(quote["quoted_sats"], json!(sats_of(code)), "{code}");
    }
    assert_eq!(fixture.paykit.requests().len(), 7);
}
