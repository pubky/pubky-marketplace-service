//! FX-at-bind integration tests (USD/2 convert-at-bind): feed-failure
//! fail-closed behavior with zero side effects and no rate/feed leakage in
//! tracing, the 3-sample bootstrap, the persisted median across a process
//! restart with outlier rejection at insert, bind idempotency against the
//! feed fetch counter, the prepare/activate/void lifecycle carrying the
//! quoted sats, exact/under/over settlement, refunds derived from the
//! frozen observation, and the USD end-to-end projection — all against real
//! Postgres with the real marketplace FX wrapper over a scripted local
//! Blocktank double.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use common::fx_feed::{fx_body, FakeFxFeed};
use common::paykit_review::{enable_bitcoin, poll_now, resolve_call};
use common::*;
use marketplace_service::bitcoin_review::{
    route_due_seller_confirmation_windows, SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{
    drain_outbox, expire_due_payment_windows, sample_fx_rate, sample_fx_rate_for,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

/// The fixture order total: 12_500 + 1_200 shipping at USD/2.
const ORDER_TOTAL_MINOR: u128 = 13_700;
const RATE: &str = "77197";

fn expected_quoted_sats(total_minor: u128, rate: u128) -> u128 {
    let numerator = total_minor * 100_000_000;
    let denominator = 100 * rate;
    numerator.div_ceil(denominator)
}

#[derive(Debug, sqlx::FromRow)]
struct FxOrderFacts {
    state: String,
    payment_method: Option<String>,
    stock_held: bool,
    paykit_request_state: Option<String>,
    bitcoin_quoted_sats: Option<i64>,
    bitcoin_quote_source: Option<String>,
    bitcoin_quote_expires_at: Option<DateTime<Utc>>,
    paykit_total_sats: Option<i64>,
    paykit_expires_at: Option<DateTime<Utc>>,
    paykit_observed_sats: Option<i64>,
    paykit_observation: Option<Value>,
    hold_expires_at: Option<DateTime<Utc>>,
    hold_source: Option<String>,
}

async fn order_facts(pool: &PgPool, order_id: &str) -> FxOrderFacts {
    sqlx::query_as(
        "SELECT state, payment_method, stock_held, paykit_request_state, bitcoin_quoted_sats, \
         bitcoin_quote_source, bitcoin_quote_expires_at, paykit_total_sats, paykit_expires_at, \
         paykit_observed_sats, paykit_observation, hold_expires_at, hold_source FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn payment_state(pool: &PgPool, order_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(pool)
        .await
        .expect("payment row")
}

/// Seeds `count` accepted reference samples at one-minute buckets ending at
/// `now`, exactly what the sampler would have persisted.
async fn seed_fx_samples(pool: &PgPool, rate: &str, now: DateTime<Utc>, count: i64) {
    for back in 0..count {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank')",
        )
        .bind(rate)
        .bind(accepted)
        .bind(bucket)
        .execute(pool)
        .await
        .expect("seed fx sample");
    }
}

async fn sample_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM fx_rate_samples")
        .fetch_one(pool)
        .await
        .expect("sample count")
}

struct FxFixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
    seller: TestActor,
    buyer: TestActor,
}

async fn fx_fixture(pool: PgPool) -> FxFixture {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 20)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    fx.set_body(fx_body(RATE, now.timestamp_millis()));
    FxFixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn upstream_fx_fixture(pool: PgPool) -> FxFixture {
    let (app, paykit, fx) = test_app_with_upstream_paykit_and_fx(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 20)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    fx.set_body(fx_body(RATE, now.timestamp_millis()));
    FxFixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn checkout_usd(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    }
}

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await
}

/// Delivers the pending `paykit.activate` outbox row so the order's invoice
/// is observing (and therefore claimable by the status poller).
async fn activate_bound(fixture: &FxFixture) {
    let client = fixture
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&fixture.app.pool, Some(client), fixture.app.clock.now(), 30)
        .await
        .expect("activation delivers");
}

fn quoted_sats() -> u128 {
    expected_quoted_sats(ORDER_TOTAL_MINOR, u128::from_str(RATE).unwrap())
}

fn assert_no_side_effects(facts: &FxOrderFacts, paykit: &FakePaykit, context: &str) {
    assert_eq!(facts.payment_method, None, "{context}: no method bound");
    assert!(
        !facts.stock_held,
        "{context}: bind refusal must not acquire a hold"
    );
    assert_eq!(
        facts.hold_source.as_deref(),
        None,
        "{context}: bind refusal must not write hold_source"
    );
    assert_eq!(facts.bitcoin_quoted_sats, None, "{context}: no quote row");
    assert!(
        paykit.requests().is_empty(),
        "{context}: no Paykit request left the process"
    );
}

async fn restock_listing(pool: &PgPool, seller_pubky: &str) {
    sqlx::query(
        "UPDATE listings SET available_quantity = total_quantity, reserved_quantity = 0, \
         sold_quantity = 0, state = 'available', server_revision = server_revision + 1 \
         WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller_pubky))
    .execute(pool)
    .await
    .expect("restock listing");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn feed_failures_are_typed_409_with_no_side_effects_and_clean_tracing(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    #[derive(Clone)]
    struct Buffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Buffer;
        fn make_writer(&self) -> Buffer {
            self.clone()
        }
    }
    let subscriber = tracing_subscriber::fmt()
        .with_writer(Buffer(buffer.clone()))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let cases: Vec<(String, &str)> = vec![
        (r#"{"tickers":[]}"#.to_string(), "fx_reference_unavailable"),
        (
            format!(
                r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"77197","base":"BTC","quote":"USD","lastUpdatedAt":{now_ms}}},{{"symbol":"BTCUSD","lastPrice":"77198","base":"BTC","quote":"USD","lastUpdatedAt":{now_ms}}}]}}"#,
                now_ms = now.timestamp_millis()
            ),
            "fx_reference_unavailable",
        ),
        (
            fx_body(RATE, (now - Duration::seconds(301)).timestamp_millis()),
            "fx_rate_stale",
        ),
        (
            fx_body(RATE, (now + Duration::seconds(61)).timestamp_millis()),
            "fx_rate_stale",
        ),
        (
            fx_body("83000", now.timestamp_millis()),
            "fx_deviation_exceeded",
        ),
    ];
    for (body, reason) in cases {
        fixture.fx.set_body(body);
        let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
        let (status, body) =
            bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{reason}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason), "{body}");
        let facts = order_facts(&pool, &order.order_id).await;
        assert_no_side_effects(&facts, &fixture.paykit, reason);
    }
    // Transport failure: the feed is down entirely.
    fixture.fx.set_status(500);
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "transport failure");

    let captured = String::from_utf8(buffer.lock().expect("buffer lock").clone()).unwrap();
    for line in captured.lines() {
        let lower = line.to_lowercase();
        assert!(
            !lower.contains("lastprice") && !lower.contains("tickers") && !lower.contains("btcusd"),
            "tracing must never carry feed body excerpts:\n{line}"
        );
        // A numeric rate may only ever surface inside an FX/quote log line;
        // none may (bare digit runs in ids/pubkeys are not rate leaks).
        if lower.contains("fx") || lower.contains("rate") || lower.contains("quote") {
            assert!(
                !line.contains(RATE) && !line.contains("83000"),
                "tracing must never carry numeric FX rates:\n{line}"
            );
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bootstrap_below_three_samples_is_refused_without_a_fetch(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();

    // Zero samples: the reference is unavailable and the feed is never dialed.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(
        fixture.fx.fetch_count(),
        0,
        "no feed fetch without a median"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "zero samples");

    // Two samples: still below the bootstrap floor.
    seed_fx_samples(&pool, RATE, now, 2).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(fixture.fx.fetch_count(), 0, "no feed fetch below 3 samples");
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "two samples");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sampler_persists_samples_rejects_outliers_and_quotes_across_restart(pool: PgPool) {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool.clone()).await;
    let now = app.clock.now();
    // Three accepted samples straight through the production sampler.
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        fx.set_body(fx_body(RATE, t.timestamp_millis()));
        let accepted = sample_fx_rate(&pool, &fx.base_url, t)
            .await
            .expect("sampler runs");
        assert_eq!(accepted, 1, "sample at -{back}s accepted");
    }
    assert_eq!(sample_count(&pool).await, 3);

    // An outlier (> 500 bps off the persisted median) never reaches the table.
    let t = now + Duration::seconds(60);
    fx.set_body(fx_body("83000", t.timestamp_millis()));
    let accepted = sample_fx_rate(&pool, &fx.base_url, t)
        .await
        .expect("sampler runs");
    assert_eq!(accepted, 0, "outlier rejected at insert");
    assert_eq!(sample_count(&pool).await, 3, "outlier was not persisted");

    // A fresh process (new AppState, new feed double) on the SAME database
    // quotes from the persisted median: nothing is cached in memory.
    let (restarted, paykit2, fx2) = test_app_with_payments_and_fx(pool.clone()).await;
    let seller = new_actor(&restarted).await;
    let buyer = new_actor(&restarted).await;
    enable_bitcoin(&restarted, &paykit2, &seller).await;
    let (status, body) = execute(
        &restarted,
        &seller.token,
        &register_command(&seller.pubky, 5),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let now2 = restarted.clock.now();
    fx2.set_body(fx_body(RATE, now2.timestamp_millis()));
    let order = checkout_usd(&restarted, &seller, &buyer).await;
    let (status, body) = bind_bitcoin(&restarted, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind after restart failed: {body}");
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(i64::try_from(expected_quoted_sats(ORDER_TOTAL_MINOR, 77197)).unwrap()),
        "the restarted process quotes from the persisted reference"
    );
    assert_eq!(paykit.requests().len(), 0, "the first process is untouched");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bind_replay_conflict_and_race_yield_exactly_one_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats();

    // First bind: exactly one feed fetch, one Paykit prepare.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let quote = body["order"]["bitcoin_quote"].clone();
    assert_eq!(quote["quoted_sats"], json!(expected as i64));
    assert_eq!(quote["rate"], json!(RATE));
    assert_eq!(quote["source"], json!("blocktank"));
    assert_eq!(fixture.fx.fetch_count(), 1);
    assert_eq!(fixture.paykit.requests().len(), 1);

    // Same order + same method: the stored quote, byte-identical, zero fetch.
    let (status, replay) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "replay failed: {replay}");
    assert_eq!(
        replay["order"]["bitcoin_quote"], quote,
        "stored quote replayed"
    );
    assert_eq!(
        fixture.fx.fetch_count(),
        1,
        "replay must not refetch the feed"
    );
    assert_eq!(
        fixture.paykit.requests().len(),
        1,
        "replay must not re-prepare"
    );

    // A different method on the bound order is a conflict.
    let (status, body) = send(
        fixture.app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&fixture.buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("payment_method_already_bound")
    );

    // Concurrent bind race: exactly one quote, one hold, one fetch, one prepare.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (first, second) = tokio::join!(
        bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id),
        bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id),
    );
    assert_eq!(first.0, StatusCode::OK, "first: {}", first.1);
    assert_eq!(second.0, StatusCode::OK, "second: {}", second.1);
    assert_eq!(
        first.1["order"]["bitcoin_quote"], second.1["order"]["bitcoin_quote"],
        "both racers observe the same stored quote"
    );
    assert_eq!(
        fixture.fx.fetch_count(),
        2,
        "exactly one race winner fetched"
    );
    assert_eq!(
        fixture.paykit.requests().len(),
        2,
        "exactly one prepare won"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.bitcoin_quoted_sats, Some(expected as i64));
    assert!(facts.stock_held, "the winner holds the stock");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn lifecycle_prepare_activate_void_and_expiry_keep_the_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    // Prepare carries the quoted sats; quote_expires_at IS the Paykit expiry.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(
        fixture.paykit.requests()[0].amount_sats,
        expected,
        "the Paykit prepare body charges exactly the quoted sats"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.bitcoin_quoted_sats, Some(expected as i64));
    assert_eq!(
        facts.bitcoin_quote_expires_at, facts.paykit_expires_at,
        "quote_expires_at == the Paykit invoice expires_at"
    );
    assert_eq!(facts.paykit_expires_at, facts.hold_expires_at);
    let quote_expires_at = facts.bitcoin_quote_expires_at;

    // Activate carries the same prepared invoice through the outbox.
    let client = fixture
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&pool, Some(client), now, 30)
        .await
        .expect("activation delivers");
    let calls = fixture.paykit.calls();
    assert!(
        calls.iter().any(|call| call.path.ends_with("/activate")),
        "activation reached the Paykit seam: {calls:?}"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.bitcoin_quote_expires_at, quote_expires_at,
        "the quote expiry is immutable through activation"
    );

    // The payment window lapses on the activated order: the order cancels
    // and the quote evidence is retained.
    let later = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, later)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "cancelled");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "expiry keeps the quote"
    );
    assert_eq!(
        facts.bitcoin_quote_expires_at, quote_expires_at,
        "expiry keeps the quote expiry"
    );
    assert_eq!(facts.bitcoin_quote_source.as_deref(), Some("blocktank"));

    // A still-preparing bind is voided at the Paykit seam on expiry, and its
    // quote is retained identically.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    fixture.app.clock.set(now);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "second bind failed: {body}");
    assert!(
        expire_due_payment_windows(&fixture.app.state, later)
            .await
            .expect("window sweep runs")
            >= 1
    );
    drain_outbox(&pool, Some(client), later, 30)
        .await
        .expect("void delivers");
    let calls = fixture.paykit.calls();
    assert!(
        calls.iter().any(|call| call.path.ends_with("/void")),
        "the lapsed preparing bind is voided at the Paykit seam: {calls:?}"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "cancelled");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "void keeps the quote"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn upstream_usd_activation_updates_quote_deadline_and_keeps_outbox_moving(pool: PgPool) {
    let fixture = upstream_fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    let first = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &first.order_id).await;
    assert_eq!(status, StatusCode::OK, "first bind failed: {body}");
    let second = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &second.order_id).await;
    assert_eq!(status, StatusCode::OK, "second bind failed: {body}");

    let client = fixture
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&pool, Some(client), now, 30)
        .await
        .expect("both activation rows drain");
    for order in [&first, &second] {
        let activation_delivered_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT delivered_at FROM outbox WHERE kind = 'paykit.activate' \
             AND payload->>'order_id' = $1",
        )
        .bind(&order.order_id)
        .fetch_one(&pool)
        .await
        .expect("activation row");
        assert!(
            activation_delivered_at.is_some(),
            "the activation row for {} must drain",
            order.order_id
        );
        let facts = order_facts(&pool, &order.order_id).await;
        assert_eq!(facts.paykit_request_state.as_deref(), Some("pending"));
        assert!(facts.paykit_expires_at.is_some());
        assert_eq!(
            facts.bitcoin_quote_expires_at, facts.paykit_expires_at,
            "the FX constraint follows the producer payment deadline"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn upstream_usd_status_recovery_updates_quote_deadline_with_paykit_deadline(pool: PgPool) {
    let fixture = upstream_fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let order_id = Uuid::parse_str(&order.order_id).expect("order id");
    let invoice_id: Uuid = sqlx::query_scalar("SELECT paykit_invoice_id FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("upstream invoice id");

    // Model activation committed upstream while Marketplace lost its response.
    // Tracking resumes without a known producer deadline; status must recover it.
    fixture.paykit.set_invoice_state(invoice_id, "active");
    sqlx::query(
        "UPDATE orders SET paykit_activation_state = 'active', paykit_request_state = 'pending' \
         WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("model lost activation response");
    let deadline =
        DateTime::from_timestamp_micros((now + Duration::minutes(30)).timestamp_micros())
            .expect("microsecond payment deadline");
    fixture.paykit.set_marketplace_status(
        invoice_id,
        json!({
            "invoice_id": invoice_id,
            "state": "active",
            "activated_at": now,
            "payment_deadline": deadline,
            "proposal_delivery_state": "delivered",
            "request_state": "accepted",
            "payment_state": "undetected",
            "bitcoin": null,
            "outcome": null,
            "resolved_at": null
        }),
    );

    assert_eq!(poll_now(&fixture.app, now).await, 0);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.paykit_expires_at, Some(deadline));
    assert_eq!(facts.bitcoin_quote_expires_at, Some(deadline));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shared_manual_extension_and_late_settlement_retain_the_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    // The 24-hour shared-manual hold extension retains the quote.
    fixture.paykit.set_allocation_mode("shared_manual");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    let deadline = now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS);
    assert_eq!(
        facts.hold_expires_at,
        Some(deadline),
        "the hold extends across the 24h seller-confirmation window"
    );
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "extension keeps the quote"
    );
    assert!(
        facts.bitcoin_quote_expires_at.is_some(),
        "extension keeps the quote expiry"
    );

    // Late settlement on an exclusive order whose window lapsed: cancelled,
    // manual review, every quote field retained as evidence.
    fixture.paykit.set_allocation_mode("exclusive");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2(
        "confirmed",
        true,
        "exclusive",
        Some("fx-late-txid"),
        Some(total_sats as u64),
        Some(2),
    );
    late["late_settlement"] = json!(true);
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "paid");
    assert_eq!(payment_state(&pool, &order.order_id).await, "confirmed");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "late settlement keeps the quote"
    );
    assert!(
        facts.bitcoin_quote_expires_at.is_some(),
        "late settlement keeps the quote expiry"
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "the late observation is frozen as evidence"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn quote_divergent_observation_keeps_an_active_seller_window(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    fixture.paykit.set_allocation_mode("shared_manual");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-window-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert_eq!(poll_now(&fixture.app, now).await, 1);
    fixture.paykit.set_allocation_mode("exclusive");
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "exclusive",
            Some("fx-window-txid"),
            Some(total_sats as u64 + 1),
            Some(3),
        ),
    );
    assert_eq!(poll_now(&fixture.app, now + Duration::seconds(60)).await, 0);

    // A later exclusive report off the bind quote never moves a waiting
    // order: the seller sees the new amount and decides.
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        payment_state(&pool, &order.order_id).await,
        "awaiting_entitlement"
    );
    let (entered, deadline, observation): (
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT paykit_seller_confirmation_entered_at, \
         paykit_seller_confirmation_deadline, paykit_observation FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window columns");
    assert_eq!(entered, Some(now));
    assert!(deadline.is_some());
    assert_eq!(
        observation["observed_sats"],
        serde_json::json!(total_sats + 1)
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "the entry observation stays frozen"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn settlement_only_exact_non_late_exclusive_auto_pays(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    let settle = |observed_delta: i64, amount_matched: bool| {
        let fixture = &fixture;
        let pool = &pool;
        async move {
            let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
            let (status, body) =
                bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
            assert_eq!(status, StatusCode::OK, "bind failed: {body}");
            activate_bound(fixture).await;
            let total_sats = order_facts(pool, &order.order_id)
                .await
                .paykit_total_sats
                .expect("invoice total");
            let observed = total_sats + observed_delta;
            let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
            fixture.paykit.set_status(
                &reference,
                bitcoin_status_v2(
                    "confirmed",
                    amount_matched,
                    "exclusive",
                    Some("fx-settle-txid"),
                    u64::try_from(observed).ok(),
                    Some(2),
                ),
            );
            assert_eq!(poll_now(&fixture.app, now).await, 1);
            order.order_id
        }
    };

    // Exact: the invoice total lands on-chain; the order auto-pays.
    let order_id = settle(0, true).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "paid", "exact non-late exclusive auto-pays");
    assert_eq!(
        facts.paykit_observed_sats, facts.paykit_total_sats,
        "the exact observation is frozen"
    );

    // Over: paykit-server itself calls the amount matched, but the observed
    // sats exceed the frozen quote — a human must review.
    let order_id = settle(1_000, true).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(
        facts.state, "pending_payment",
        "over-payment never auto-pays"
    );
    assert_eq!(payment_state(&pool, &order_id).await, "manual_review");
    assert!(
        facts.bitcoin_quoted_sats.is_some(),
        "review keeps the quote"
    );

    // Under: paykit-server reports the shortfall; the same review class.
    let order_id = settle(-1_000, false).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(
        facts.state, "pending_payment",
        "under-payment never auto-pays"
    );
    assert_eq!(payment_state(&pool, &order_id).await, "manual_review");
    assert!(
        facts.bitcoin_quoted_sats.is_some(),
        "review keeps the quote"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn late_exclusive_settlement_pays_only_the_exact_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let after_window = now + Duration::seconds(7300);

    let settle_late = |observed_delta: i64| {
        let fixture = &fixture;
        let pool = &pool;
        async move {
            let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
            let (status, body) =
                bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
            assert_eq!(status, StatusCode::OK, "bind failed: {body}");
            activate_bound(fixture).await;
            let total_sats = order_facts(pool, &order.order_id)
                .await
                .paykit_total_sats
                .expect("invoice total");
            assert!(
                expire_due_payment_windows(&fixture.app.state, after_window)
                    .await
                    .expect("window sweep runs")
                    >= 1
            );
            let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
            // paykit-server calls the amount matched; only the frozen quote
            // can tell the marketplace the figure is wrong.
            let mut late = bitcoin_status_v2(
                "confirmed",
                true,
                "exclusive",
                Some("fx-late-delta-txid"),
                u64::try_from(total_sats + observed_delta).ok(),
                Some(2),
            );
            late["late_settlement"] = json!(true);
            fixture.paykit.set_status(&reference, late);
            assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
            order.order_id
        }
    };

    let order_id = settle_late(0).await;
    assert_eq!(
        order_facts(&pool, &order_id).await.state,
        "paid",
        "an exact late payment completes the free order"
    );
    assert_eq!(payment_state(&pool, &order_id).await, "confirmed");

    for delta in [1_000, -1_000] {
        let order_id = settle_late(delta).await;
        let facts = order_facts(&pool, &order_id).await;
        assert_ne!(
            facts.state, "paid",
            "a late mismatch (delta {delta}) never pays"
        );
        assert!(!facts.stock_held, "the unit is not re-taken");
        assert_eq!(payment_state(&pool, &order_id).await, "manual_review");
        let reason: Option<String> =
            sqlx::query_scalar("SELECT review_reason FROM payments WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason.as_deref(), Some("amount_mismatch"));
        assert!(
            facts.bitcoin_quoted_sats.is_some(),
            "review keeps the quote"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refunds_derive_from_the_frozen_observation_only(pool: PgPool) {
    install_log_capture();
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    // A late exclusive settlement observed exactly at the invoice total
    // lands in manual review; the refund derives from the FROZEN observed
    // sats (quote + nonce), never the fiat total and never the bare quote.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let bound = order_facts(&pool, &order.order_id).await;
    let total_sats = bound.paykit_total_sats.expect("invoice total");
    let quoted_sats = bound.bitcoin_quoted_sats.expect("frozen quote");
    assert_ne!(
        total_sats, ORDER_TOTAL_MINOR as i64,
        "sats are not fiat minor"
    );
    assert_ne!(
        total_sats, quoted_sats,
        "the invoice total carries the nonce"
    );
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2(
        "confirmed",
        true,
        "exclusive",
        Some("fx-refund-txid"),
        Some(total_sats as u64),
        Some(2),
    );
    late["late_settlement"] = json!(true);
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&pool)
    .await
    .expect("sold-out so late money cannot complete");
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "frozen at first sight"
    );
    let (status, body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-1" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "refund resolve failed: {body}\n{}",
        captured_logs()
    );
    let external_refund: Value =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("external refund row");
    assert_eq!(
        external_refund["amount_minor"],
        json!(total_sats),
        "the refund derives from the frozen observation (quote + nonce sats)"
    );

    // A NULL frozen observation blocks the automatic refund entirely.
    restock_listing(&pool, &fixture.seller.pubky).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2("confirmed", true, "exclusive", None, None, None);
    late["late_settlement"] = json!(true);
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&pool)
    .await
    .expect("sold-out so late money cannot complete");
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        facts.paykit_observed_sats, None,
        "nothing observed, nothing frozen"
    );
    let (status, _body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-2" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a NULL frozen observation blocks the automatic refund"
    );
    assert_eq!(
        payment_state(&pool, &order.order_id).await,
        "manual_review",
        "the blocked refund leaves the review open"
    );

    // A later observation refreshes the JSON facts only; the frozen column
    // is assigned at ENTRY (A1) and is single-assignment (the shared-manual
    // status-only refresh path).
    fixture.paykit.set_allocation_mode("shared_manual");
    restock_listing(&pool, &fixture.seller.pubky).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    fixture.app.clock.set(now);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-refresh-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "A1: the first authority-establishing observation is frozen at entry"
    );
    // The second observation carries different facts (more confirmations).
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-refresh-txid"),
            Some(total_sats as u64),
            Some(9),
        ),
    );
    let _ = poll_now(&fixture.app, now + Duration::seconds(30)).await;
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "the refresh touches the JSON only, never the frozen column"
    );
    let observation = facts.paykit_observation.expect("observation facts");
    assert_eq!(
        observation["confirmations"],
        json!(9),
        "the later observation refreshed the JSON facts"
    );
    // Route the 24h window: the reaper's COALESCE freeze is a fallback for
    // pre-existing rows and must not win against the entry freeze.
    let routed = route_due_seller_confirmation_windows(
        &fixture.app.state,
        now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("seller-window reaper runs");
    assert!(routed >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "frozen at entry (A1); the reaper's fallback freeze never wins"
    );
    let observation_after = facts
        .paykit_observation
        .expect("observation facts retained");
    assert_eq!(observation_after["confirmations"], json!(9));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shared_manual_freeze_pins_the_first_observation_for_refunds(pool: PgPool) {
    install_log_capture();
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    fixture.paykit.set_allocation_mode("shared_manual");

    // N sats: the FIRST matching shared-manual observation establishes
    // authority, enters awaiting_seller_confirmation, and is frozen onto
    // the order in the same transaction (A1).
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let n_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-freeze-txid"),
            Some(n_sats as u64),
            Some(1),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(n_sats),
        "A1: frozen at entry, in the same transaction as the state change"
    );
    assert_eq!(
        facts
            .paykit_observation
            .as_ref()
            .expect("observation facts")["observed_sats"],
        json!(n_sats)
    );

    // A later status refresh reports M != N sats: the JSON observation
    // moves to M, the frozen column stays N.
    let m_sats = n_sats + 1_000;
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            false,
            "shared_manual",
            Some("fx-freeze-txid"),
            Some(m_sats as u64),
            Some(3),
        ),
    );
    let _ = poll_now(&fixture.app, now + Duration::seconds(30)).await;
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(n_sats),
        "the freeze pins the first observation"
    );
    assert_eq!(
        facts
            .paykit_observation
            .as_ref()
            .expect("observation facts")["observed_sats"],
        json!(m_sats),
        "the JSON carries the refreshed facts"
    );

    // Route the 24h window: the reaper's fallback freeze never wins.
    let routed = route_due_seller_confirmation_windows(
        &fixture.app.state,
        now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("seller-window reaper runs");
    assert!(routed >= 1);
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        order_facts(&pool, &order.order_id)
            .await
            .paykit_observed_sats,
        Some(n_sats),
        "still the first observation after the reaper"
    );

    // The refund derives from the frozen first observation N, never M.
    let (status, body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-freeze" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "refund resolve failed: {body}\n{}",
        captured_logs()
    );
    let external_refund: Value =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("external refund row");
    assert_eq!(
        external_refund["amount_minor"],
        json!(n_sats),
        "the refund derives from the frozen first observation"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usd_bind_end_to_end_amount_and_role_scoped_projection(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    // The Paykit prepare body charges exactly the quoted sats.
    assert_eq!(fixture.paykit.requests().len(), 1);
    assert_eq!(fixture.paykit.requests()[0].amount_sats, expected);

    // Buyer and seller projections expose the quote, scoped like every
    // other order field; a stranger gets the same 404 as for any order.
    for (token, role) in [
        (&fixture.buyer.token, "buyer"),
        (&fixture.seller.token, "seller"),
    ] {
        let (status, body) = send(
            fixture.app.router.clone(),
            "GET",
            &format!("/v1/orders/{}", order.order_id),
            Some(token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{role}: {body}");
        let quote = &body["bitcoin_quote"];
        assert_eq!(quote["quoted_sats"], json!(expected as i64), "{role}");
        assert_eq!(quote["rate"], json!(RATE), "{role}");
        assert_eq!(quote["source"], json!("blocktank"), "{role}");
        assert!(
            quote["fetched_at"].is_string(),
            "{role}: fx_rate_fetched_at"
        );
        assert!(quote["expires_at"].is_string(), "{role}: quote_expires_at");
        // Fiat monetary fields are untouched by the conversion.
        assert_eq!(
            body["total"]["amount_minor"],
            json!(ORDER_TOTAL_MINOR as i64),
            "{role}"
        );
        assert_eq!(body["total"]["currency"], json!("USD"), "{role}");
    }
    let stranger = new_actor(&fixture.app).await;
    let (status, _) = send(
        fixture.app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order.order_id),
        Some(&stranger.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "strangers see no quote");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usd_offer_checkout_bitcoin_bind_keeps_settlement_and_merchandise_typed(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    seed_fx_samples(&pool, RATE, fixture.app.clock.now(), 3).await;
    let (status, body) = execute(
        &fixture.app,
        &fixture.buyer.token,
        &create_offer_command(&fixture.seller.pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        &fixture.app,
        &fixture.seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000f101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    sqlx::query(
        "UPDATE offers SET accepted_listing_record_sha256 = $2, accepted_variant_id = $3 \
         WHERE id = $1",
    )
    .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer uuid"))
    .bind("a".repeat(64))
    .bind("boots_01")
    .execute(&fixture.app.pool)
    .await
    .expect("accepted snapshot normalized");
    let (award_id, listing_revision): (Uuid, i64) =
        sqlx::query_as("SELECT award_id, accepted_listing_revision FROM offers WHERE id = $1")
            .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer uuid"))
            .fetch_one(&fixture.app.pool)
            .await
            .expect("accepted award");
    let checkout = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-00000000f102",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": award_id,
            "listing_aggregate_id": listing_aggregate(&fixture.seller.pubky),
            "listing_revision": listing_revision,
            "listing_record_sha256": "a".repeat(64),
            "variant_id": "boots_01",
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = execute(&fixture.app, &fixture.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let order_id = body["result"]["order"]["id"].as_str().expect("order id");
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    let (status, projection) = send(
        fixture.app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&fixture.buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "projection failed: {projection}");
    assert_eq!(
        projection["payment"]["amount"],
        json!({
            "amount_minor": projection["bitcoin_payable"]["amount_minor"],
            "currency": "SAT",
            "exponent": 0
        })
    );
    assert_eq!(
        projection["payment"]["merchandise_amount"],
        json!({"amount_minor": 10_000, "currency": "USD", "exponent": 2})
    );
    assert_eq!(
        projection["merchandise_total"],
        json!({"amount_minor": 10_000, "currency": "USD", "exponent": 2})
    );
    assert_eq!(projection["priced_from"], json!("offer"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn single_seller_public_ticker_adapters_sample_guard_and_bind_end_to_end(pool: PgPool) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    for provider in [
        FxFeedProvider::Mempool,
        FxFeedProvider::Kraken,
        FxFeedProvider::Coinbase,
        FxFeedProvider::Bitstamp,
    ] {
        sqlx::query("DELETE FROM fx_rate_samples")
            .execute(&pool)
            .await
            .expect("clear samples");
        let mut config = Config::for_tests();
        config.fx_feed_provider = provider;
        let (mut app, paykit, fx) =
            test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let feed_url = fx.url_for(provider);
        app = common::reconfigured(&app, |cfg| {
            cfg.store_mode = StoreMode::SingleSeller {
                pinned: Some(seller.pubky.clone()),
            };
            cfg.fx_feed_provider = provider;
            cfg.fx_feed_url = feed_url.clone();
        });

        let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{health}");
        let expected_currencies = match provider {
            FxFeedProvider::Bitstamp => json!(["USD", "EUR", "GBP"]),
            _ => json!(["USD", "EUR", "CHF", "GBP", "JPY", "CAD", "AUD"]),
        };
        assert_eq!(
            health["fx_source"],
            json!({
                "provider": provider.as_str(),
                "pinned": false,
                "currencies": expected_currencies
            })
        );

        enable_bitcoin(&app, &paykit, &seller).await;
        let (status, body) =
            execute(&app, &seller.token, &register_command(&seller.pubky, 10)).await;
        assert_eq!(status, StatusCode::OK, "register failed: {body}");

        let now = app.clock.now();
        let set_feed = |usd_price: &str, ts: DateTime<Utc>, header_ts: DateTime<Utc>| {
            fx.set_date_header(Some(header_ts));
            match provider {
                FxFeedProvider::Mempool => fx.set_body(format!(
                    r#"{{"time":{},"USD":{usd_price},"EUR":66481,"GBP":57037,"CAD":106992,"CHF":62992,"AUD":107607,"JPY":11745955}}"#,
                    ts.timestamp()
                )),
                FxFeedProvider::Kraken => {
                    let t = ts.timestamp();
                    let pairs = std::collections::HashMap::from([
                        (
                            "XXBTZUSD".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XXBTZUSD":[["{usd_price}","0.01",{t}.5,"b","l","",1]],"last":"1"}}}}"#
                            ),
                        ),
                        (
                            "XXBTZEUR".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XXBTZEUR":[["66481","0.01",{t},"b","l","",2]],"last":"2"}}}}"#
                            ),
                        ),
                        (
                            "XBTCHF".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XBTCHF":[["62992","0.01",{t},"b","l","",3]],"last":"3"}}}}"#
                            ),
                        ),
                        (
                            "XXBTZGBP".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XXBTZGBP":[["57037","0.01",{t},"b","l","",4]],"last":"4"}}}}"#
                            ),
                        ),
                        (
                            "XXBTZJPY".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XXBTZJPY":[["11745955","0.01",{t},"b","l","",5]],"last":"5"}}}}"#
                            ),
                        ),
                        (
                            "XXBTZCAD".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XXBTZCAD":[["106992","0.01",{t},"b","l","",6]],"last":"6"}}}}"#
                            ),
                        ),
                        (
                            "XBTAUD".to_string(),
                            format!(
                                r#"{{"error":[],"result":{{"XBTAUD":[["107607","0.01",{t},"b","l","",7]],"last":"7"}}}}"#
                            ),
                        ),
                    ]);
                    fx.set_pair_bodies(pairs);
                }
                FxFeedProvider::Coinbase => fx.set_body(format!(
                    r#"{{"data":{{"currency":"BTC","rates":{{"USD":"{usd_price}","EUR":"66481"}}}}}}"#
                )),
                FxFeedProvider::Bitstamp => fx.set_body(format!(
                    r#"[{{"pair":"BTC/USD","last":"{usd_price}","timestamp":"{}"}},{{"pair":"BTC/EUR","last":"66481","timestamp":"{}"}},{{"pair":"BTC/GBP","last":"57037","timestamp":"{}"}}]"#,
                    ts.timestamp(),
                    ts.timestamp(),
                    ts.timestamp()
                )),
                FxFeedProvider::Blocktank => unreachable!(),
            }
        };

        // Sample 3 buckets through the real sampler; verify source column.
        for back in [120_i64, 60, 0] {
            let t = now - Duration::seconds(back);
            set_feed(RATE, t, t);
            let accepted = sample_fx_rate_for(&pool, provider, &feed_url, t)
                .await
                .expect("sampler runs");
            if provider == FxFeedProvider::Bitstamp {
                assert_eq!(accepted, 3, "bitstamp samples USD/EUR/GBP cleanly");
            } else {
                assert!(accepted >= 1, "{} accepted at -{back}s", provider.as_str());
            }
        }
        let stored_source: String =
            sqlx::query_scalar("SELECT source FROM fx_rate_samples WHERE currency = 'USD' LIMIT 1")
                .fetch_one(&pool)
                .await
                .expect("sample source");
        assert_eq!(stored_source, provider.as_str());

        // Stale response is refused cleanly. On Kraken, keep HTTP Date fresh
        // while the trade timestamp is stale to prove price-tied freshness.
        let stale_ts = now - Duration::seconds(301);
        let stale_header = if provider == FxFeedProvider::Kraken {
            now
        } else {
            stale_ts
        };
        set_feed(RATE, stale_ts, stale_header);
        let stale_order = checkout_usd(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &stale_order.order_id).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "{}: {body}",
            provider.as_str()
        );
        assert_eq!(body["error"]["reason"], json!("fx_rate_stale"));

        // Outlier (> 5% off median) is refused cleanly.
        set_feed("83000", now, now);
        let outlier_order = checkout_usd(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &outlier_order.order_id).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "{}: {body}",
            provider.as_str()
        );
        assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));

        // Fresh in-band quote binds and records the provider name as quote source.
        set_feed(RATE, now, now);
        let ok_order = checkout_usd(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &ok_order.order_id).await;
        assert_eq!(status, StatusCode::OK, "{}: {body}", provider.as_str());
        assert_eq!(
            body["order"]["bitcoin_quote"]["source"],
            json!(provider.as_str())
        );
        let facts = order_facts(&pool, &ok_order.order_id).await;
        assert_eq!(
            facts.bitcoin_quote_source.as_deref(),
            Some(provider.as_str())
        );
        assert_eq!(facts.bitcoin_quoted_sats, Some(quoted_sats() as i64));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn market_rates_above_one_million_usd_and_quotes_above_one_btc_bind_cleanly(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    // Rate above the removed $1,000,000 static ceiling ($2,500,000/BTC).
    let high_rate = "2500000";
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        fixture
            .fx
            .set_body(fx_body(high_rate, t.timestamp_millis()));
        let accepted = sample_fx_rate(&pool, &fixture.fx.base_url, t)
            .await
            .expect("sampler accepts $2.5M/BTC");
        assert_eq!(accepted, 1);
    }
    // $137.00 at $2,500,000/BTC is 5,480 sats; a $5.00 order is 200 sats (under 1,000).
    let mut small = register_command(&fixture.seller.pubky, 5);
    small["command_id"] = json!(Uuid::new_v4().to_string());
    small["aggregate_id"] = json!(format!("listing:{}_pin_01", fixture.seller.pubky));
    small["payload"]["listing_id"] = json!("pin_01");
    small["payload"]["unit_price"] =
        json!({ "amount_minor": 500, "currency": "USD", "exponent": 2 });
    small["payload"]["shipping_minor"] = json!(0);
    let (status, body) = execute(&fixture.app, &fixture.seller.token, &small).await;
    assert_eq!(status, StatusCode::OK, "register small: {body}");
    let mut checkout = checkout_command_with_id(&fixture.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["listing_aggregate_id"] =
        json!(format!("listing:{}_pin_01", fixture.seller.pubky));
    checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(&fixture.app, &fixture.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout small: {body}");
    let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, order_id).await;
    assert_eq!(status, StatusCode::OK, "bind small at $2.5M/BTC: {body}");
    assert_eq!(
        order_facts(&pool, order_id).await.bitcoin_quoted_sats,
        Some(200)
    );

    // A $5,000,000.00 order at $2,500,000/BTC is 200,000,000 sats (2.0 BTC).
    let mut large = register_command(&fixture.seller.pubky, 5);
    large["command_id"] = json!(Uuid::new_v4().to_string());
    large["aggregate_id"] = json!(format!("listing:{}_house_01", fixture.seller.pubky));
    large["payload"]["listing_id"] = json!("house_01");
    large["payload"]["unit_price"] =
        json!({ "amount_minor": 500_000_000, "currency": "USD", "exponent": 2 });
    large["payload"]["shipping_minor"] = json!(0);
    let (status, body) = execute(&fixture.app, &fixture.seller.token, &large).await;
    assert_eq!(status, StatusCode::OK, "register large: {body}");
    let mut checkout = checkout_command_with_id(&fixture.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["listing_aggregate_id"] =
        json!(format!("listing:{}_house_01", fixture.seller.pubky));
    checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(&fixture.app, &fixture.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout large: {body}");
    let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, order_id).await;
    assert_eq!(status, StatusCode::OK, "bind 2 BTC order: {body}");
    assert_eq!(
        order_facts(&pool, order_id).await.bitcoin_quoted_sats,
        Some(200_000_000)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn fork_nonce_overflow_above_i64_or_u64_rolls_back_and_courtesy_voids(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    // Rate 1,000,000 USD/BTC: total_minor = i64::MAX USD cents quotes to exact i64::MAX sats.
    let rate = "1000000";
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        fixture.fx.set_body(fx_body(rate, t.timestamp_millis()));
        assert_eq!(
            sample_fx_rate(&pool, &fixture.fx.base_url, t)
                .await
                .unwrap(),
            1
        );
    }

    // Case 1: legacy_amount_sats = i64::MAX with nonce_sats = 437 yields i64::MAX + 437.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    sqlx::query(
        "UPDATE orders SET subtotal_minor = $2, shipping_minor = 0, total_minor = $2 WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).unwrap())
    .bind(i64::MAX)
    .execute(&pool)
    .await
    .expect("set i64::MAX order total");
    fixture.paykit.set_nonce_sats(437);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "i64::MAX + 437: {body}");
    assert_eq!(body["error"]["reason"], json!("paykit_total_inconsistent"));
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.payment_method, None);
    assert!(!facts.stock_held);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        fixture
            .paykit
            .calls()
            .iter()
            .any(|call| call.path.ends_with("/void")),
        "prepared fork invoice exceeding i64::MAX must be courtesy-voided"
    );

    // Case 2: u64-overflowing nonce_sats wraps in the mock server; checked_add rejects it.
    let order2 = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    fixture.paykit.set_nonce_sats(u64::MAX);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order2.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "u64 overflow nonce: {body}");
    assert_eq!(body["error"]["reason"], json!("paykit_total_inconsistent"));
    assert_eq!(
        order_facts(&pool, &order2.order_id).await.payment_method,
        None
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn high_precision_denominator_and_cross_rates_preserve_exact_ceiling_end_to_end(
    pool: PgPool,
) {
    use marketplace_domain::currency::Currency;
    use marketplace_service::config::Config;
    use marketplace_service::fx::{quote_order_for, FxFeedProvider};
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    let mut config = Config::for_tests();
    config.fx_feed_provider = FxFeedProvider::Coinbase;
    let (mut app, paykit, fx) =
        test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let coinbase_url = fx.url_for(FxFeedProvider::Coinbase);
    app = common::reconfigured(&app, |cfg| {
        cfg.store_mode = StoreMode::SingleSeller {
            pinned: Some(seller.pubky.clone()),
        };
        cfg.fx_feed_provider = FxFeedProvider::Coinbase;
        cfg.fx_feed_url = coinbase_url.clone();
    });
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 10)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");

    let now = app.clock.now();
    // Exact reproduction vector: total_minor = 9_000_000_000_000 (USD, exp 2)
    // and rate 77197.123456789. Exact rational ceiling is 116_584_654_932_613
    // sats (< 1 sat excess); 8-decimal flooring would inflate it to
    // 116_584_654_932_626 (+13 sats).
    let precise_body = r#"{
        "data": {
            "currency": "BTC",
            "rates": {
                "USD": "77197.123456789",
                "EUR": "66481.123456789012"
            }
        }
    }"#;
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        fx.set_date_header(Some(t));
        fx.set_body(precise_body.to_string());
        let accepted = sample_fx_rate_for(&pool, FxFeedProvider::Coinbase, &coinbase_url, t)
            .await
            .expect("sampler persists full decimal precision");
        assert_eq!(accepted, 2);
    }

    // PostgreSQL NUMERIC stores and reads back all fractional digits intact.
    let stored_usd: String =
        sqlx::query_scalar("SELECT rate::text FROM fx_rate_samples WHERE currency = 'USD' LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("stored USD sample");
    assert_eq!(stored_usd, "77197.123456789");
    let stored_eur: String =
        sqlx::query_scalar("SELECT rate::text FROM fx_rate_samples WHERE currency = 'EUR' LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("stored EUR sample");
    assert_eq!(stored_eur, "66481.123456789012");

    // DB-backed quote_order_for reproduces exact 116_584_654_932_613 sats.
    fx.set_date_header(Some(now));
    fx.set_body(precise_body.to_string());
    let mut conn = pool.acquire().await.expect("pg conn");
    let quote = quote_order_for(
        &mut conn,
        FxFeedProvider::Coinbase,
        &coinbase_url,
        Currency::Usd,
        9_000_000_000_000,
        now,
    )
    .await
    .expect("quote_order_for succeeds");
    assert_eq!(quote.direct.rate.to_string_value(), "77197.123456789");
    assert_eq!(quote.sats, Ok(116_584_654_932_613));

    // Full HTTP bind on a 9_000_000_000_000-cent USD order records exact sats
    // and full-precision rate in PostgreSQL.
    let order = checkout_usd(&app, &seller, &buyer).await;
    sqlx::query(
        "UPDATE orders SET subtotal_minor = $2, shipping_minor = 0, total_minor = $2 WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).unwrap())
    .bind(9_000_000_000_000_i64)
    .execute(&pool)
    .await
    .expect("set 9e12 order total");
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "precise bind failed: {body}");
    assert_eq!(
        body["order"]["bitcoin_quote"]["quoted_sats"],
        json!(116_584_654_932_613_i64)
    );
    assert_eq!(
        body["order"]["bitcoin_quote"]["rate"],
        json!("77197.123456789")
    );
    let stored_order_rate: String =
        sqlx::query_scalar("SELECT bitcoin_quote_rate::text FROM orders WHERE id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&pool)
            .await
            .expect("stored order quote rate");
    assert_eq!(stored_order_rate, "77197.123456789");
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.bitcoin_quoted_sats, Some(116_584_654_932_613));

    // High-precision cross rate in quote_order_for yields exact < 1 cent ceiling.
    sqlx::query("DELETE FROM fx_rate_samples")
        .execute(&pool)
        .await
        .unwrap();
    let cross_now = now + Duration::seconds(180);
    let cross_exact_body = r#"{
        "data": {
            "currency": "BTC",
            "rates": {
                "USD": "100000.000000001",
                "EUR": "50000.0000000005"
            }
        }
    }"#;
    for back in [120_i64, 60, 0] {
        let t = cross_now - Duration::seconds(back);
        fx.set_date_header(Some(t));
        fx.set_body(cross_exact_body.to_string());
        assert_eq!(
            sample_fx_rate_for(&pool, FxFeedProvider::Coinbase, &coinbase_url, t)
                .await
                .unwrap(),
            2
        );
    }
    fx.set_date_header(Some(cross_now));
    fx.set_body(cross_exact_body.to_string());
    let eur_exact = quote_order_for(
        &mut conn,
        FxFeedProvider::Coinbase,
        &coinbase_url,
        Currency::Eur,
        1_000,
        cross_now,
    )
    .await
    .expect("exact EUR cross quote");
    assert_eq!(eur_exact.usd.cents, 2_000);
    let cross_ceil_body = r#"{
        "data": {
            "currency": "BTC",
            "rates": {
                "USD": "100000.000000001",
                "EUR": "50000.000000000000"
            }
        }
    }"#;
    fx.set_body(cross_ceil_body.to_string());
    let eur_ceil = quote_order_for(
        &mut conn,
        FxFeedProvider::Coinbase,
        &coinbase_url,
        Currency::Eur,
        1_000,
        cross_now,
    )
    .await
    .expect("ceiling EUR cross quote");
    assert_eq!(eur_ceil.usd.cents, 2_001);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitstamp_provider_rejects_unsupported_currencies_with_422_and_samples_usd_eur_gbp(
    pool: PgPool,
) {
    use marketplace_domain::currency::Currency;
    use marketplace_service::config::Config;
    use marketplace_service::fx::{quote_fiat_for, quote_order_for, FxError, FxFeedProvider};
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    let mut config = Config::for_tests();
    config.fx_feed_provider = FxFeedProvider::Bitstamp;
    let (mut app, paykit, fx) =
        test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let bitstamp_url = fx.url_for(FxFeedProvider::Bitstamp);
    app = common::reconfigured(&app, |cfg| {
        cfg.store_mode = StoreMode::SingleSeller {
            pinned: Some(seller.pubky.clone()),
        };
        cfg.fx_feed_provider = FxFeedProvider::Bitstamp;
        cfg.fx_feed_url = bitstamp_url.clone();
    });
    enable_bitcoin(&app, &paykit, &seller).await;

    let now = app.clock.now();
    // Bitstamp array publishes BTC/USD, BTC/EUR, and BTC/GBP only; sampler
    // cleanly persists all 3 with zero failures on CHF/JPY/CAD/AUD.
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        let ts = t.timestamp();
        fx.set_body(format!(
            r#"[
                {{"pair":"BTC/USD","last":"77197","timestamp":"{ts}"}},
                {{"pair":"BTC/EUR","last":"66481","timestamp":"{ts}"}},
                {{"pair":"BTC/GBP","last":"57037","timestamp":"{ts}"}}
            ]"#
        ));
        let accepted = sample_fx_rate_for(&pool, FxFeedProvider::Bitstamp, &bitstamp_url, t)
            .await
            .expect("bitstamp sampler succeeds");
        assert_eq!(accepted, 3);
    }

    let make_listing = |id: &str, code: &str, exponent: i32| {
        let mut cmd = register_command(&seller.pubky, 5);
        cmd["command_id"] = json!(Uuid::new_v4().to_string());
        cmd["aggregate_id"] = json!(format!("listing:{}_{id}", seller.pubky));
        cmd["payload"]["listing_id"] = json!(id);
        cmd["payload"]["unit_price"] = json!({
            "amount_minor": 12_500,
            "currency": code,
            "exponent": exponent,
        });
        cmd
    };

    // USD, EUR, GBP, BTC, and SAT listings are accepted.
    for (id, code, exp) in [
        ("ok_usd", "USD", 2),
        ("ok_eur", "EUR", 2),
        ("ok_gbp", "GBP", 2),
        ("ok_btc", "BTC", 8),
        ("ok_sat", "SAT", 0),
    ] {
        let (status, body) = execute(&app, &seller.token, &make_listing(id, code, exp)).await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");
    }

    // CHF, JPY, CAD, and AUD listings are refused upfront with 422.
    for (id, code, exp) in [
        ("bad_chf", "CHF", 2),
        ("bad_jpy", "JPY", 0),
        ("bad_cad", "CAD", 2),
        ("bad_aud", "AUD", 2),
    ] {
        let (status, body) = execute(&app, &seller.token, &make_listing(id, code, exp)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{code}: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("USD, EUR, GBP, BTC, SAT"),
            "{body}"
        );
    }

    // Updating an existing USD listing to an unsupported currency is refused
    // with 422.
    let mut revise = make_listing("ok_usd", "CHF", 2);
    revise["expected_revision"] = json!(1);
    revise["payload"]["listing_revision"] = json!(2);
    let (status, body) = execute(&app, &seller.token, &revise).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "revise: {body}");
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // Offers and auction bids in CHF are refused upfront with 422.
    let mut bid = place_bid_command(&seller.pubky, 90, 10_000, 1);
    bid["payload"]["maximum_amount"]["currency"] = json!("CHF");
    let (status, body) = execute(&app, &buyer.token, &bid).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "bid: {body}");
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["command_id"] = json!(Uuid::new_v4().to_string());
    offer["aggregate_id"] = json!(format!("listing:{}_ok_usd", seller.pubky));
    offer["payload"]["amount"]["currency"] = json!("CHF");
    let (status, body) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "offer: {body}");
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // Simulate a pre-existing CHF listing and CHF order (created before
    // switching FX_FEED_PROVIDER=bitstamp): checkout and Paykit bind refuse
    // upfront with 422 and zero FX fetches or Paykit requests.
    let chf_agg = format!("listing:{}_ok_gbp", seller.pubky);
    sqlx::query("UPDATE listings SET unit_price_currency = 'CHF' WHERE aggregate_id = $1")
        .bind(&chf_agg)
        .execute(&pool)
        .await
        .expect("mark listing CHF");
    let mut chf_checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    chf_checkout["payload"]["lines"][0]["listing_aggregate_id"] = json!(&chf_agg);
    chf_checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &chf_checkout).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "checkout on existing CHF listing: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // Updating the pre-existing CHF listing to USD is allowed.
    let mut migrate = make_listing("ok_gbp", "USD", 2);
    migrate["expected_revision"] = json!(1);
    migrate["payload"]["listing_revision"] = json!(2);
    let (status, body) = execute(&app, &seller.token, &migrate).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "migrating CHF listing to USD: {body}"
    );

    // Pre-existing CHF order refuses both `bitcoin` and `paykit` binds with
    // 422 before spending paykit_bind_attempt, fetching FX, or calling Paykit.
    let mut usd_checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    usd_checkout["payload"]["lines"][0]["listing_aggregate_id"] =
        json!(format!("listing:{}_ok_usd", seller.pubky));
    usd_checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &usd_checkout).await;
    assert_eq!(status, StatusCode::OK, "usd checkout: {body}");
    let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();
    sqlx::query("UPDATE orders SET currency = 'CHF' WHERE id = $1::uuid")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("mark order CHF");
    let fetches_before = fx.fetch_count();
    let paykit_before = paykit.requests().len();
    let attempts_before: i32 =
        sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("bind attempt count");
    assert_eq!(attempts_before, 0);

    for method in ["bitcoin", "paykit"] {
        let (status, body) = send(
            app.router.clone(),
            "POST",
            &format!("/v0/orders/{order_id}/payment-method"),
            Some(&buyer.token),
            &json!({ "method": method }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "bind CHF order via {method} on Bitstamp: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    }
    let attempts_after: i32 =
        sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("bind attempt count after refusal");
    assert_eq!(
        attempts_after, 0,
        "422 currency_unsupported refusal must not increment paykit_bind_attempt"
    );
    assert_eq!(
        fx.fetch_count(),
        fetches_before,
        "no FX fetch on unsupported currency"
    );
    assert_eq!(
        paykit.requests().len(),
        paykit_before,
        "no Paykit call on unsupported currency"
    );

    // Non-buyer receives 403 not_buyer (never 422 currency probe).
    let bystander = new_actor(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&bystander.token),
        &json!({ "method": "paykit" }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "non-buyer bind: {body}");
    assert_eq!(body["error"]["reason"], json!("not_buyer"));

    // Once the CHF order's payment is no longer awaiting_entitlement (expired),
    // the upfront currency pre-check yields to 409 payment_not_awaiting.
    sqlx::query("UPDATE payments SET state = 'expired' WHERE order_id = $1::uuid")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("expire CHF payment");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paykit" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "expired CHF order payment must return 409 payment_not_awaiting: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("payment_not_awaiting"));

    // Pre-existing CHF offer refuses offer.counter, offer.accept, and
    // offer.checkout with 422 currency_unsupported.
    let offer_id = Uuid::new_v4();
    let mut create_usd_offer = create_offer_command(&seller.pubky, 1);
    create_usd_offer["command_id"] = json!(offer_id.to_string());
    create_usd_offer["aggregate_id"] = json!(format!("listing:{}_ok_usd", seller.pubky));
    let (status, body) = execute(&app, &buyer.token, &create_usd_offer).await;
    assert_eq!(status, StatusCode::OK, "create USD offer: {body}");
    let offer_agg = format!("offer:{offer_id}");
    sqlx::query("UPDATE offers SET currency = 'CHF' WHERE id = $1")
        .bind(offer_id)
        .execute(&pool)
        .await
        .expect("mark offer CHF");

    let mut counter_cmd = counter_offer_command(1);
    counter_cmd["command_id"] = json!(Uuid::new_v4().to_string());
    counter_cmd["aggregate_id"] = json!(&offer_agg);
    counter_cmd["payload"]["offer_id"] = json!(offer_id.to_string());
    let (status, body) = execute(&app, &seller.token, &counter_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "offer.counter on CHF offer: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    let mut accept_cmd = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    accept_cmd["aggregate_id"] = json!(&offer_agg);
    accept_cmd["payload"]["offer_id"] = json!(offer_id.to_string());
    let (status, body) = execute(&app, &seller.token, &accept_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "offer.accept on CHF offer: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // Accept a second USD offer on the mirrored `boots_01` listing through the
    // real command, then switch its persisted currency to CHF to simulate a
    // pre-existing accepted CHF award before calling offer.checkout.
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    assert_eq!(status, StatusCode::OK, "register boots_01: {body}");
    let offer2_id = Uuid::new_v4();
    let mut create_usd_offer2 = create_offer_command(&seller.pubky, 1);
    create_usd_offer2["command_id"] = json!(offer2_id.to_string());
    let (status, body) = execute(&app, &buyer.token, &create_usd_offer2).await;
    assert_eq!(status, StatusCode::OK, "create second USD offer: {body}");
    let offer2_agg = format!("offer:{offer2_id}");
    let mut accept_usd = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    accept_usd["aggregate_id"] = json!(&offer2_agg);
    accept_usd["payload"]["offer_id"] = json!(offer2_id.to_string());
    let (status, accepted_body) = execute(&app, &seller.token, &accept_usd).await;
    assert_eq!(status, StatusCode::OK, "accept USD offer: {accepted_body}");
    let (award_id, accepted_rev, accepted_listing_rev, accepted_sha, accepted_variant): (
        Uuid,
        i64,
        i64,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT award_id, revision, accepted_listing_revision, \
                accepted_listing_record_sha256, accepted_variant_id \
         FROM offers WHERE id = $1",
    )
    .bind(offer2_id)
    .fetch_one(&pool)
    .await
    .expect("accepted award row");
    sqlx::query("UPDATE offers SET currency = 'CHF', accepted_currency = 'CHF' WHERE id = $1")
        .bind(offer2_id)
        .execute(&pool)
        .await
        .expect("mark accepted offer CHF");
    let offer_checkout_cmd = json!({
        "version": 1,
        "command_id": Uuid::new_v4().to_string(),
        "aggregate_id": offer2_agg,
        "expected_revision": accepted_rev,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": offer2_id.to_string(),
            "award_id": award_id.to_string(),
            "listing_aggregate_id": listing_aggregate(&seller.pubky),
            "listing_revision": accepted_listing_rev,
            "listing_record_sha256": accepted_sha,
            "variant_id": accepted_variant,
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = execute(&app, &buyer.token, &offer_checkout_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "offer.checkout on CHF offer: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // listing.sync for a homeserver record priced in CHF refuses with 422.
    let mut sync_cfg = Config::for_tests();
    sync_cfg.store_mode = StoreMode::SingleSeller {
        pinned: Some(seller.pubky.clone()),
    };
    sync_cfg.fx_feed_provider = FxFeedProvider::Bitstamp;
    sync_cfg.fx_feed_url = bitstamp_url.clone();
    let (sync_app, homeserver) = test_app_with_homeserver_config(pool.clone(), sync_cfg).await;
    let sync_seller = new_actor(&sync_app).await;
    let sync_app = common::reconfigured(&sync_app, |cfg| {
        cfg.store_mode = StoreMode::SingleSeller {
            pinned: Some(sync_seller.pubky.clone()),
        };
    });
    homeserver.put_record(
        &sync_seller.pubky,
        "chf_sync_01",
        json!({
            "title": "Swiss watch",
            "revision": 1,
            "location": { "countryCode": "CH", "region": null },
            "media": [{
                "id": "media_01",
                "type": "image",
                "mimeType": "image/jpeg",
                "contentHash": "a".repeat(64),
                "byteSize": 999_533,
            }],
            "variants": [{
                "id": "v1",
                "enabled": true,
                "quantity": 3,
                "sku": null,
                "priceOverride": null,
            }],
            "shippingOptions": [{
                "id": "ship_flat",
                "pricing": "flat",
                "label": "Shipping",
                "price": { "amountMinor": 500, "currency": "CHF", "exponent": 2 },
                "estimatedMinDays": 2,
                "estimatedMaxDays": 5,
            }],
            "sale": {
                "acceptsOffers": true,
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 12_500, "currency": "CHF", "exponent": 2 },
            },
        }),
    );
    let mut sync_cmd = sync_command(&sync_seller.pubky, "chf_sync_01", 1);
    sync_cmd["command_id"] = json!(Uuid::new_v4().to_string());
    let (status, body) = execute(&sync_app, &sync_seller.token, &sync_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "listing.sync CHF on Bitstamp: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

    // Direct fx::quote_order_for and fx::quote_fiat_for also fail closed with
    // UnsupportedCurrency without fetching the feed.
    let mut conn = pool.acquire().await.expect("pg conn");
    for unsupported in [Currency::Chf, Currency::Jpy, Currency::Cad, Currency::Aud] {
        assert_eq!(
            quote_order_for(
                &mut conn,
                FxFeedProvider::Bitstamp,
                &bitstamp_url,
                unsupported,
                10_000,
                now
            )
            .await,
            Err(FxError::UnsupportedCurrency)
        );
        assert_eq!(
            quote_fiat_for(
                &mut *conn,
                FxFeedProvider::Bitstamp,
                &bitstamp_url,
                unsupported,
                10_000,
                now
            )
            .await,
            Err(FxError::UnsupportedCurrency)
        );
    }
    assert_eq!(fx.fetch_count(), fetches_before);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn fx_sampler_fetches_at_most_once_per_60s_bucket_across_success_and_failure(pool: PgPool) {
    use marketplace_service::fx::FxFeedProvider;

    let fx = common::fx_feed::spawn_fake_fx().await;
    let kraken_url = fx.url_for(FxFeedProvider::Kraken);
    let bucket0 = "2026-09-13T16:20:00Z".parse::<DateTime<Utc>>().unwrap();

    let set_blocktank_body = |ts: DateTime<Utc>| {
        fx.set_body(common::fx_feed::fx_body_multi(
            &[
                ("USD", "77197"),
                ("EUR", "66481"),
                ("CHF", "62992"),
                ("GBP", "57037"),
                ("JPY", "11745955"),
                ("CAD", "106992"),
                ("AUD", "107607"),
            ],
            ts.timestamp_millis(),
        ));
    };

    let set_kraken_pairs = |ts: DateTime<Utc>| {
        let t = ts.timestamp();
        let pairs = std::collections::HashMap::from([
            (
                "XXBTZUSD".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XXBTZUSD":[["77197","0.01",{t},"b","l","",1]],"last":"1"}}}}"#
                ),
            ),
            (
                "XXBTZEUR".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XXBTZEUR":[["66481","0.01",{t},"b","l","",2]],"last":"2"}}}}"#
                ),
            ),
            (
                "XBTCHF".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XBTCHF":[["62992","0.01",{t},"b","l","",3]],"last":"3"}}}}"#
                ),
            ),
            (
                "XXBTZGBP".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XXBTZGBP":[["57037","0.01",{t},"b","l","",4]],"last":"4"}}}}"#
                ),
            ),
            (
                "XXBTZJPY".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XXBTZJPY":[["11745955","0.01",{t},"b","l","",5]],"last":"5"}}}}"#
                ),
            ),
            (
                "XXBTZCAD".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XXBTZCAD":[["106992","0.01",{t},"b","l","",6]],"last":"6"}}}}"#
                ),
            ),
            (
                "XBTAUD".to_string(),
                format!(
                    r#"{{"error":[],"result":{{"XBTAUD":[["107607","0.01",{t},"b","l","",7]],"last":"7"}}}}"#
                ),
            ),
        ]);
        fx.set_pair_bodies(pairs);
    };

    // 1. Blocktank: 1 fetch on call 1, 0 fetches on call 2 in the same 60s
    // bucket, then resumes on the next 60s bucket.
    let t0 = bucket0 + Duration::seconds(5);
    set_blocktank_body(t0);
    assert_eq!(sample_fx_rate(&pool, &fx.base_url, t0).await.unwrap(), 7);
    assert_eq!(fx.fetch_count(), 1);

    let t0_retry = bucket0 + Duration::seconds(25);
    set_blocktank_body(t0_retry);
    assert_eq!(
        sample_fx_rate(&pool, &fx.base_url, t0_retry).await.unwrap(),
        0
    );
    assert_eq!(
        fx.fetch_count(),
        1,
        "second Blocktank tick in same 60s bucket must not hit feed"
    );

    let t1 = bucket0 + Duration::seconds(60);
    set_blocktank_body(t1);
    assert_eq!(sample_fx_rate(&pool, &fx.base_url, t1).await.unwrap(), 7);
    assert_eq!(fx.fetch_count(), 2);

    // 2. Blocktank all-stale bucket: 1 attempt on tick 1, 0 attempts on tick 2
    // in the same bucket, resumes cleanly on the next bucket.
    let t2 = bucket0 + Duration::seconds(125);
    set_blocktank_body(t2 - Duration::seconds(400));
    assert_eq!(sample_fx_rate(&pool, &fx.base_url, t2).await.unwrap(), 0);
    assert_eq!(fx.fetch_count(), 3);

    let t2_retry = bucket0 + Duration::seconds(145);
    set_blocktank_body(t2_retry);
    assert_eq!(
        sample_fx_rate(&pool, &fx.base_url, t2_retry).await.unwrap(),
        0
    );
    assert_eq!(
        fx.fetch_count(),
        3,
        "stale failure in same 60s bucket must not retry until next bucket"
    );

    let t3 = bucket0 + Duration::seconds(180);
    set_blocktank_body(t3);
    assert_eq!(sample_fx_rate(&pool, &fx.base_url, t3).await.unwrap(), 7);
    assert_eq!(fx.fetch_count(), 4);

    // 3. Kraken: 7 per-pair requests on call 1, 0 requests on call 2 in the
    // same 60s bucket, resumes on next bucket; HTTP 429 throttled bucket fires
    // only 1 snapshot attempt (7 requests) and 0 on same-bucket retries.
    sqlx::query("DELETE FROM fx_rate_samples")
        .execute(&pool)
        .await
        .unwrap();
    let before_kraken = fx.fetch_count();
    set_kraken_pairs(t0);
    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t0)
            .await
            .unwrap(),
        7
    );
    assert_eq!(fx.fetch_count() - before_kraken, 7);

    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t0_retry)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        fx.fetch_count() - before_kraken,
        7,
        "second Kraken tick in same 60s bucket must send 0 requests"
    );

    set_kraken_pairs(t1);
    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t1)
            .await
            .unwrap(),
        7
    );
    assert_eq!(fx.fetch_count() - before_kraken, 14);

    // Upstream HTTP 429 in bucket t2: at most one 7-request attempt in that
    // 60s bucket, zero retries at t2 + 20s, resumes at t3.
    fx.set_status(429);
    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(fx.fetch_count() - before_kraken, 21);

    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t2_retry)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        fx.fetch_count() - before_kraken,
        21,
        "HTTP 429 failure in same 60s bucket must not burst again"
    );

    set_kraken_pairs(t3);
    assert_eq!(
        sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_url, t3)
            .await
            .unwrap(),
        7
    );
    assert_eq!(fx.fetch_count() - before_kraken, 28);
}

async fn seed_fx_samples_for(
    pool: &PgPool,
    currency: &str,
    rate: &str,
    source: &str,
    now: DateTime<Utc>,
    count: i64,
) {
    for back in 0..count {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ($1, $2::numeric, $3, $3, $4, $5)",
        )
        .bind(currency)
        .bind(rate)
        .bind(accepted)
        .bind(bucket)
        .bind(source)
        .execute(pool)
        .await
        .expect("seed fx sample");
    }
}

/// Single-pair Kraken and Bitstamp URLs quote one fiat; admission, sampling,
/// `/health`, and bind-time quoting stay consistent even when fresh EUR samples
/// already exist in PostgreSQL.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn single_pair_kraken_and_bitstamp_urls_restrict_fiat_admission_sampling_and_bind(
    pool: PgPool,
) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    type SinglePairCase = (
        &'static str,
        FxFeedProvider,
        fn(&common::fx_feed::FakeFxFeed) -> String,
    );

    let cases: [SinglePairCase; 2] = [
        ("kraken_usd_pair", FxFeedProvider::Kraken, |fx| {
            format!(
                "{}?pair=XXBTZUSD&count=1",
                fx.url_for(FxFeedProvider::Kraken)
            )
        }),
        ("bitstamp_usd_pair", FxFeedProvider::Bitstamp, |fx| {
            format!("{}btcusd/", fx.url_for(FxFeedProvider::Bitstamp))
        }),
    ];

    for (_label, provider, url_for) in cases {
        sqlx::query("DELETE FROM fx_rate_samples")
            .execute(&pool)
            .await
            .expect("clear samples");

        let mut config = Config::for_tests();
        config.fx_feed_provider = provider;
        let (mut app, paykit, fx) =
            test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
        let feed_url = url_for(&fx);
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        app = common::reconfigured(&app, |cfg| {
            cfg.store_mode = StoreMode::SingleSeller {
                pinned: Some(seller.pubky.clone()),
            };
            cfg.fx_feed_provider = provider;
            cfg.fx_feed_url = feed_url.clone();
        });

        let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{health}");
        assert_eq!(
            health["fx_source"],
            json!({
                "provider": provider.as_str(),
                "pinned": false,
                "currencies": ["USD"]
            })
        );

        enable_bitcoin(&app, &paykit, &seller).await;

        let make_listing = |id: &str, code: &str| {
            let mut cmd = register_command(&seller.pubky, 5);
            cmd["command_id"] = json!(Uuid::new_v4().to_string());
            cmd["aggregate_id"] = json!(format!("listing:{}_{id}", seller.pubky));
            cmd["payload"]["listing_id"] = json!(id);
            cmd["payload"]["unit_price"] = json!({
                "amount_minor": 12_500,
                "currency": code,
                "exponent": if code == "JPY" { 0 } else { 2 },
            });
            cmd
        };

        let (status, body) = execute(&app, &seller.token, &make_listing("ok_usd", "USD")).await;
        assert_eq!(status, StatusCode::OK, "USD listing: {body}");

        let (status, body) = execute(&app, &seller.token, &make_listing("bad_eur", "EUR")).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "EUR listing: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("USD, BTC, SAT"),
            "refusal must name URL-supported currencies: {body}"
        );

        let now = app.clock.now();

        let set_usd_feed = |ts: DateTime<Utc>| {
            fx.set_date_header(Some(ts));
            match provider {
                FxFeedProvider::Kraken => {
                    let t = ts.timestamp();
                    fx.set_pair_bodies(std::collections::HashMap::from([(
                        "XXBTZUSD".to_string(),
                        format!(
                            r#"{{"error":[],"result":{{"XXBTZUSD":[["{RATE}","0.01",{t}.5,"b","l","",1]],"last":"1"}}}}"#
                        ),
                    )]));
                }
                FxFeedProvider::Bitstamp => {
                    let ts = ts.timestamp();
                    fx.set_body(format!(
                        r#"{{"pair":"BTC/USD","last":"{RATE}","timestamp":"{ts}"}}"#
                    ));
                }
                _ => unreachable!(),
            }
        };

        for back in [120_i64, 60, 0] {
            let t = now - Duration::seconds(back);
            set_usd_feed(t);
            let accepted = sample_fx_rate_for(&pool, provider, &feed_url, t)
                .await
                .expect("sampler runs");
            assert_eq!(
                accepted,
                1,
                "{} single-pair feed samples USD only",
                provider.as_str()
            );
        }

        seed_fx_samples_for(&pool, "EUR", "66481", provider.as_str(), now, 3).await;

        let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
        checkout["payload"]["lines"][0]["listing_aggregate_id"] =
            json!(format!("listing:{}_ok_usd", seller.pubky));
        checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
        let (status, body) = execute(&app, &buyer.token, &checkout).await;
        assert_eq!(status, StatusCode::OK, "USD checkout: {body}");
        let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();

        sqlx::query("UPDATE orders SET currency = 'EUR' WHERE id = $1::uuid")
            .bind(order_id)
            .execute(&pool)
            .await
            .expect("mark order EUR");

        let fetches_before = fx.fetch_count();
        let paykit_before = paykit.requests().len();
        let attempts_before: i32 =
            sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
                .bind(order_id)
                .fetch_one(&pool)
                .await
                .expect("bind attempt count");
        assert_eq!(attempts_before, 0);

        for method in ["bitcoin", "paykit"] {
            let (status, body) = send(
                app.router.clone(),
                "POST",
                &format!("/v0/orders/{order_id}/payment-method"),
                Some(&buyer.token),
                &json!({ "method": method }),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "EUR bind on {} single-pair {}: {body}",
                provider.as_str(),
                method
            );
            assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
        }
        let attempts_after: i32 =
            sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
                .bind(order_id)
                .fetch_one(&pool)
                .await
                .expect("bind attempt count after refusal");
        assert_eq!(
            attempts_after, 0,
            "EUR bind must not increment paykit_bind_attempt"
        );
        assert_eq!(fx.fetch_count(), fetches_before, "no FX fetch for EUR bind");
        assert_eq!(
            paykit.requests().len(),
            paykit_before,
            "no Paykit call for EUR bind"
        );

        set_usd_feed(now);
        let (status, body) = bind_bitcoin(&app, &buyer.token, order_id).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "EUR order must stay refused after USD feed is fresh: {body}"
        );
        assert_eq!(body["error"]["reason"], json!("currency_unsupported"));

        sqlx::query("UPDATE orders SET currency = 'USD' WHERE id = $1::uuid")
            .bind(order_id)
            .execute(&pool)
            .await
            .expect("revert order to USD");
        set_usd_feed(now);
        let (status, body) = bind_bitcoin(&app, &buyer.token, order_id).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} USD bind on single-pair URL: {body}",
            provider.as_str()
        );
        assert_eq!(
            body["order"]["bitcoin_quote"]["source"],
            json!(provider.as_str())
        );
    }
}

/// Legacy `Fork` + EUR-only Kraken/Bitstamp URLs: sampler, bind, and quotes stay on EUR.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn fork_eur_only_single_pair_kraken_and_bitstamp_bind_end_to_end(pool: PgPool) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    const EUR_RATE: &str = "66481";
    const ORDER_TOTAL_MINOR: u128 = 13_700;

    type EurPairCase = (&'static str, FxFeedProvider, fn(&FakeFxFeed) -> String);

    let cases: [EurPairCase; 2] = [
        ("kraken_eur_pair", FxFeedProvider::Kraken, |fx| {
            format!(
                "{}?pair=XXBTZEUR&count=1",
                fx.url_for(FxFeedProvider::Kraken)
            )
        }),
        ("bitstamp_eur_pair", FxFeedProvider::Bitstamp, |fx| {
            format!("{}btceur/", fx.url_for(FxFeedProvider::Bitstamp))
        }),
    ];

    for (label, provider, url_for) in cases {
        sqlx::query("DELETE FROM fx_rate_samples")
            .execute(&pool)
            .await
            .expect("clear samples");

        let mut config = Config::for_tests();
        config.fx_feed_provider = provider;
        let (mut app, paykit, fx) =
            test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
        let feed_url = url_for(&fx);
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        app = common::reconfigured(&app, |cfg| {
            cfg.store_mode = StoreMode::SingleSeller {
                pinned: Some(seller.pubky.clone()),
            };
            cfg.fx_feed_provider = provider;
            cfg.fx_feed_url = feed_url.clone();
        });

        let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{label} health: {health}");
        assert_eq!(
            health["fx_source"]["currencies"],
            json!(["EUR"]),
            "{label} health currencies"
        );

        enable_bitcoin(&app, &paykit, &seller).await;
        let mut cmd = register_command(&seller.pubky, 5);
        cmd["payload"]["unit_price"]["currency"] = json!("EUR");
        let (status, body) = execute(&app, &seller.token, &cmd).await;
        assert_eq!(status, StatusCode::OK, "{label} EUR listing: {body}");

        let now = app.clock.now();
        let set_eur_feed = |ts: DateTime<Utc>| {
            fx.set_date_header(Some(ts));
            match provider {
                FxFeedProvider::Kraken => {
                    let t = ts.timestamp();
                    fx.set_pair_bodies(std::collections::HashMap::from([(
                        "XXBTZEUR".to_string(),
                        format!(
                            r#"{{"error":[],"result":{{"XXBTZEUR":[["{EUR_RATE}","0.01",{t},"b","l","",1]],"last":"1"}}}}"#
                        ),
                    )]));
                }
                FxFeedProvider::Bitstamp => {
                    let ts = ts.timestamp();
                    fx.set_body(format!(
                        r#"{{"pair":"BTC/EUR","last":"{EUR_RATE}","timestamp":"{ts}"}}"#
                    ));
                }
                _ => unreachable!(),
            }
        };

        for back in [120_i64, 60, 0] {
            let t = now - Duration::seconds(back);
            set_eur_feed(t);
            let accepted = sample_fx_rate_for(&pool, provider, &feed_url, t)
                .await
                .expect("sampler runs");
            assert_eq!(
                accepted, 1,
                "{label} EUR-only feed samples one currency at -{back}s"
            );
        }
        let stored_source: String =
            sqlx::query_scalar("SELECT source FROM fx_rate_samples WHERE currency = 'EUR' LIMIT 1")
                .fetch_one(&pool)
                .await
                .expect("EUR sample source");
        assert_eq!(stored_source, provider.as_str());

        let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
        checkout["payload"]["lines"][0]["listing_aggregate_id"] =
            json!(listing_aggregate(&seller.pubky));
        checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
        let (status, body) = execute(&app, &buyer.token, &checkout).await;
        assert_eq!(status, StatusCode::OK, "{label} EUR checkout: {body}");
        let order_id = body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id");

        set_eur_feed(now);
        let fetches_before = fx.fetch_count();
        let (status, body) = bind_bitcoin(&app, &buyer.token, order_id).await;
        assert_eq!(status, StatusCode::OK, "{label} EUR bind: {body}");
        let expected_sats =
            expected_quoted_sats(ORDER_TOTAL_MINOR, EUR_RATE.parse().expect("eur rate"));
        assert_eq!(
            body["order"]["bitcoin_quote"]["source"],
            json!(provider.as_str())
        );
        assert_eq!(body["order"]["bitcoin_quote"]["pair"], json!("BTCEUR"));
        assert_eq!(body["order"]["bitcoin_quote"]["rate"], json!(EUR_RATE));
        assert_eq!(
            body["order"]["bitcoin_quote"]["quoted_sats"],
            json!(expected_sats as i64)
        );
        let facts = order_facts(&pool, order_id).await;
        assert_eq!(facts.bitcoin_quoted_sats, Some(expected_sats as i64));
        assert_eq!(
            facts.bitcoin_quote_source.as_deref(),
            Some(provider.as_str())
        );
        assert_eq!(
            fx.fetch_count(),
            fetches_before + 1,
            "{label} bind performs one live EUR fetch"
        );
    }
}

/// `multi_asset` needs USD in the feed; the fork still honors EUR-only single-pair URLs.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn multi_asset_and_fork_single_pair_feeds_differ_on_usd_admission(pool: PgPool) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::payment_attempt::MarketplaceContract;
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::store_seller::StoreMode;

    type ForkCase<'a> = (&'a str, FxFeedProvider, fn(&FakeFxFeed) -> String);
    let fork_cases: [ForkCase<'_>; 2] = [
        ("kraken_eur_pair", FxFeedProvider::Kraken, |fx| {
            format!(
                "{}?pair=XXBTZEUR&count=1",
                fx.url_for(FxFeedProvider::Kraken)
            )
        }),
        ("bitstamp_eur_pair", FxFeedProvider::Bitstamp, |fx| {
            format!("{}btceur/", fx.url_for(FxFeedProvider::Bitstamp))
        }),
    ];

    for (label, provider, url_for) in fork_cases {
        let mut config = Config::for_tests();
        config.fx_feed_provider = provider;
        let (mut app, paykit, fx) =
            test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Fork).await;
        let feed_url = url_for(&fx);
        let seller = new_actor(&app).await;
        app = common::reconfigured(&app, |cfg| {
            cfg.store_mode = StoreMode::SingleSeller {
                pinned: Some(seller.pubky.clone()),
            };
            cfg.fx_feed_provider = provider;
            cfg.fx_feed_url = feed_url.clone();
        });

        let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{label} health: {health}");
        assert_eq!(
            health["fx_source"]["currencies"],
            json!(["EUR"]),
            "{label} health currencies"
        );

        enable_bitcoin(&app, &paykit, &seller).await;
        let mut cmd = register_command(&seller.pubky, 5);
        cmd["payload"]["unit_price"]["currency"] = json!("EUR");
        let (status, body) = execute(&app, &seller.token, &cmd).await;
        assert_eq!(status, StatusCode::OK, "{label} EUR listing: {body}");
    }

    let mut config = Config::for_tests();
    config.paykit_marketplace_contract = MarketplaceContract::MultiAsset;
    let (mut app, paykit, fx) =
        test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Upstream).await;
    paykit.use_multi_asset_contract();
    let seller = new_actor(&app).await;
    let feed_url = format!(
        "{}?pair=XXBTZUSD&count=1",
        fx.url_for(FxFeedProvider::Kraken)
    );
    app = common::reconfigured(&app, |cfg| {
        cfg.store_mode = StoreMode::SingleSeller {
            pinned: Some(seller.pubky.clone()),
        };
        cfg.fx_feed_provider = FxFeedProvider::Kraken;
        cfg.fx_feed_url = feed_url.clone();
        cfg.paykit_marketplace_contract = MarketplaceContract::MultiAsset;
    });

    enable_bitcoin(&app, &paykit, &seller).await;
    let buyer = new_actor(&app).await;
    let cmd = register_command(&seller.pubky, 5);
    let (status, body) = execute(&app, &seller.token, &cmd).await;
    assert_eq!(status, StatusCode::OK, "USD listing: {body}");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["listing_aggregate_id"] =
        json!(listing_aggregate(&seller.pubky));
    checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();

    let now = app.clock.now();
    seed_fx_samples_for(&pool, "EUR", "66481", "kraken", now, 3).await;
    sqlx::query("UPDATE orders SET currency = 'EUR', exponent = 2 WHERE id = $1::uuid")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("EUR order");

    let attempts_before: i32 =
        sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("attempts");
    assert_eq!(attempts_before, 0);

    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paykit" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "multi_asset EUR on USD-only feed: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    let attempts_after: i32 =
        sqlx::query_scalar("SELECT paykit_bind_attempt FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("attempts after");
    assert_eq!(attempts_after, 0, "must not spend paykit_bind_attempt");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_accept_offer_checkout_and_checkout_create_refuse_unsupported_currency_with_zero_upstream_io(
    pool: PgPool,
) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::homeserver::{
        DeliverableFetchOutcome, HomeserverEventsOutcome, HomeserverFetchOutcome,
        HomeserverListingClient, HomeserverRawFetchOutcome,
    };
    use marketplace_service::listing_moderation::{
        ListingModerationClient, ListingModerationGate, ModerationLookup,
    };
    use marketplace_service::region_policy::RegionModes;
    use marketplace_service::store_seller::StoreMode;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    struct CountingHomeserver {
        inner: Arc<dyn HomeserverListingClient>,
        listing_fetches: AtomicUsize,
        raw_listing_fetches: AtomicUsize,
        shop_fetches: AtomicUsize,
        profile_fetches: AtomicUsize,
    }

    impl CountingHomeserver {
        fn new(inner: Arc<dyn HomeserverListingClient>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                listing_fetches: AtomicUsize::new(0),
                raw_listing_fetches: AtomicUsize::new(0),
                shop_fetches: AtomicUsize::new(0),
                profile_fetches: AtomicUsize::new(0),
            })
        }

        fn reset(&self) {
            self.listing_fetches.store(0, AtomicOrdering::SeqCst);
            self.raw_listing_fetches.store(0, AtomicOrdering::SeqCst);
            self.shop_fetches.store(0, AtomicOrdering::SeqCst);
            self.profile_fetches.store(0, AtomicOrdering::SeqCst);
        }

        fn total_fetches(&self) -> usize {
            self.listing_fetches.load(AtomicOrdering::SeqCst)
                + self.raw_listing_fetches.load(AtomicOrdering::SeqCst)
                + self.shop_fetches.load(AtomicOrdering::SeqCst)
                + self.profile_fetches.load(AtomicOrdering::SeqCst)
        }
    }

    impl HomeserverListingClient for CountingHomeserver {
        fn fetch_listing<'a>(
            &'a self,
            seller_pubky: &'a str,
            listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            self.listing_fetches.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.fetch_listing(seller_pubky, listing_id)
        }

        fn fetch_listing_raw<'a>(
            &'a self,
            seller_pubky: &'a str,
            listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverRawFetchOutcome> + Send + 'a>> {
            self.raw_listing_fetches
                .fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.fetch_listing_raw(seller_pubky, listing_id)
        }

        fn fetch_shipping_profile_raw<'a>(
            &'a self,
            seller_pubky: &'a str,
            profile_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverRawFetchOutcome> + Send + 'a>> {
            self.profile_fetches.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner
                .fetch_shipping_profile_raw(seller_pubky, profile_id)
        }

        fn fetch_drop<'a>(
            &'a self,
            seller_pubky: &'a str,
            drop_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            self.inner.fetch_drop(seller_pubky, drop_id)
        }

        fn fetch_shop<'a>(
            &'a self,
            seller_pubky: &'a str,
        ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
            self.shop_fetches.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.fetch_shop(seller_pubky)
        }

        fn fetch_deliverable_digest<'a>(
            &'a self,
            seller_pubky: &'a str,
            deliverable_id: &'a str,
            version: i64,
            max_len: u64,
        ) -> Pin<Box<dyn Future<Output = DeliverableFetchOutcome> + Send + 'a>> {
            self.inner
                .fetch_deliverable_digest(seller_pubky, deliverable_id, version, max_len)
        }

        fn listing_events<'a>(
            &'a self,
            seller_pubky: &'a str,
            path: &'a str,
            after_cursor: Option<&'a str>,
            reverse: bool,
            limit: u16,
        ) -> Pin<Box<dyn Future<Output = HomeserverEventsOutcome> + Send + 'a>> {
            self.inner
                .listing_events(seller_pubky, path, after_cursor, reverse, limit)
        }
    }

    struct CountingModeration {
        lookups: AtomicUsize,
    }

    impl CountingModeration {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                lookups: AtomicUsize::new(0),
            })
        }

        fn reset(&self) {
            self.lookups.store(0, AtomicOrdering::SeqCst);
        }

        fn count(&self) -> usize {
            self.lookups.load(AtomicOrdering::SeqCst)
        }
    }

    impl ListingModerationClient for CountingModeration {
        fn lookup<'a>(
            &'a self,
            _seller_pubky: &'a str,
            _listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
            self.lookups.fetch_add(1, AtomicOrdering::SeqCst);
            Box::pin(async { ModerationLookup::Clear })
        }
    }

    let homeserver = spawn_fake_homeserver().await;
    let counting_hs = CountingHomeserver::new(homeserver.client());
    let counting_mod = CountingModeration::new();
    let moderation_gate = Arc::new(ListingModerationGate::new(counting_mod.clone()));

    let base_app = test_app_with_homeserver_client(pool.clone(), counting_hs.clone()).await;
    let seller = new_actor(&base_app).await;
    let buyer = new_actor(&base_app).await;
    let stranger = new_actor(&base_app).await;

    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Solo Shop",
            "sellerStatus": "private",
            "location": { "countryCode": "CH" },
        }),
    );
    let profile_sha = homeserver.put_shipping_profile(
        &seller.pubky,
        "prof_chf",
        &json!({
            "schemaVersion": 1,
            "recordType": "shipping_profile",
            "ownerPubky": seller.pubky,
            "revision": 1,
            "createdAt": "2026-08-19T21:00:00.000Z",
            "updatedAt": "2026-08-19T21:00:00.000Z",
            "profileId": "prof_chf",
            "name": "Standard",
            "currency": "CHF",
            "exponent": 2,
            "origins": ["CH"],
            "excludedCountries": [],
            "zones": [{
                "zoneId": "z_all",
                "name": "World",
                "countries": [],
                "restOfWorld": true,
                "rates": [{
                    "pricing": "flat",
                    "rateId": "r_flat",
                    "label": "Flat",
                    "amountMinor": 500,
                    "estimatedMinDays": 2,
                    "estimatedMaxDays": 5,
                }],
            }],
        }),
    );

    // Register a CHF listing with a shipping profile before narrowing the feed
    // to Bitstamp (USD/EUR/GBP only).
    let mut reg = register_command(&seller.pubky, 10);
    reg["command_id"] = json!(Uuid::new_v4().to_string());
    reg["aggregate_id"] = json!(format!("listing:{}_chf_item", seller.pubky));
    reg["payload"]["listing_id"] = json!("chf_item");
    reg["payload"]["unit_price"] = json!({
        "amount_minor": 12_500,
        "currency": "CHF",
        "exponent": 2,
    });
    reg["payload"]["shipping_profile_id"] = json!("prof_chf");
    homeserver.put_record(
        &seller.pubky,
        "chf_item",
        json!({
            "title": "Swiss item",
            "revision": 1,
            "state": "active",
            "location": { "countryCode": "CH", "region": null },
            "media": [{
                "id": "m1",
                "type": "image",
                "mimeType": "image/jpeg",
                "contentHash": "a".repeat(64),
                "byteSize": 1000,
            }],
            "variants": [{
                "id": "v1",
                "enabled": true,
                "quantity": 10,
                "sku": null,
                "priceOverride": null,
            }],
            "shippingProfileId": "prof_chf",
            "sale": {
                "acceptsOffers": true,
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 12_500, "currency": "CHF", "exponent": 2 },
            },
        }),
    );
    let (status, body) = execute(&base_app, &seller.token, &reg).await;
    assert_eq!(status, StatusCode::OK, "register CHF listing: {body}");

    let chf_agg = format!("listing:{}_chf_item", seller.pubky);
    let shipping_choice = json!({
        "seller_pubky": seller.pubky,
        "profile_id": "prof_chf",
        "rate_id": "r_flat",
        "profile_sha256": profile_sha,
    });

    // Create two actionable CHF offers before feed narrowing: offer 1 remains
    // pending; offer 2 is accepted before narrowing to test both replay and
    // offer.checkout after narrowing.
    let offer1_id = Uuid::new_v4();
    let mut create_offer1 = create_offer_command(&seller.pubky, 1);
    create_offer1["command_id"] = json!(offer1_id.to_string());
    create_offer1["aggregate_id"] = json!(&chf_agg);
    create_offer1["payload"]["amount"] =
        json!({ "amount_minor": 10_000, "currency": "CHF", "exponent": 2 });
    create_offer1["payload"]["destination_country"] = json!("CH");
    create_offer1["payload"]["shipping_choice"] = shipping_choice.clone();
    let (status, body) = execute(&base_app, &buyer.token, &create_offer1).await;
    assert_eq!(status, StatusCode::OK, "create CHF offer 1: {body}");

    let offer2_id = Uuid::new_v4();
    let mut create_offer2 = create_offer_command(&seller.pubky, 1);
    create_offer2["command_id"] = json!(offer2_id.to_string());
    create_offer2["aggregate_id"] = json!(&chf_agg);
    create_offer2["payload"]["amount"] =
        json!({ "amount_minor": 11_000, "currency": "CHF", "exponent": 2 });
    create_offer2["payload"]["destination_country"] = json!("CH");
    create_offer2["payload"]["shipping_choice"] = shipping_choice.clone();
    let (status, body) = execute(&base_app, &buyer.token, &create_offer2).await;
    assert_eq!(status, StatusCode::OK, "create CHF offer 2: {body}");

    let accept2_cmd_id = Uuid::new_v4();
    let mut accept2_cmd = offer_action("offer.accept", 1, &accept2_cmd_id.to_string());
    accept2_cmd["aggregate_id"] = json!(format!("offer:{offer2_id}"));
    accept2_cmd["payload"]["offer_id"] = json!(offer2_id.to_string());
    let (status, body) = execute(&base_app, &seller.token, &accept2_cmd).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "accept CHF offer 2 before narrowing: {body}"
    );
    let (award_id, accepted_rev, accepted_listing_rev, accepted_sha, accepted_variant): (
        Uuid,
        i64,
        i64,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT award_id, revision, accepted_listing_revision, \
                accepted_listing_record_sha256, accepted_variant_id \
         FROM offers WHERE id = $1",
    )
    .bind(offer2_id)
    .fetch_one(&pool)
    .await
    .expect("accepted CHF offer 2 row");

    // Narrow the deployment to Bitstamp (USD, EUR, GBP only) with moderation,
    // homeserver, shipping profiles, and nuanced EU region modes all active.
    let mut narrowed_config = Config::for_tests();
    narrowed_config.store_mode = StoreMode::SingleSeller {
        pinned: Some(seller.pubky.clone()),
    };
    narrowed_config.fx_feed_provider = FxFeedProvider::Bitstamp;
    narrowed_config.fx_feed_url = marketplace_service::fx::BITSTAMP_FX_URL.to_string();
    narrowed_config.region_modes = Some(RegionModes::parse("EU=nuanced,GB=nuanced").unwrap());
    marketplace_service::fx::sync_config_fx_admission(&mut narrowed_config)
        .expect("bitstamp config sync");
    let narrowed_state =
        marketplace_service::AppState::new(pool.clone(), base_app.clock.clone(), narrowed_config)
            .with_homeserver(Some(counting_hs.clone()))
            .with_listing_moderation(Some(moderation_gate));
    let narrowed_app = TestApp {
        router: marketplace_service::http::build_router(narrowed_state.clone()),
        pool: pool.clone(),
        clock: base_app.clock.clone(),
        state: narrowed_state,
    };

    // 1. Actionable CHF offer.accept by rightful acceptor (seller) fails with
    //    422 currency_unsupported with ZERO upstream homeserver or moderation IO.
    counting_hs.reset();
    counting_mod.reset();
    let mut accept1_cmd = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    accept1_cmd["aggregate_id"] = json!(format!("offer:{offer1_id}"));
    accept1_cmd["payload"]["offer_id"] = json!(offer1_id.to_string());
    let (status, body) = execute(&narrowed_app, &seller.token, &accept1_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "offer.accept on narrowed CHF offer: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    assert_eq!(
        counting_hs.total_fetches(),
        0,
        "offer.accept currency refusal must run before all homeserver reads (listing={}, raw={}, shop={}, profile={})",
        counting_hs.listing_fetches.load(AtomicOrdering::SeqCst),
        counting_hs.raw_listing_fetches.load(AtomicOrdering::SeqCst),
        counting_hs.shop_fetches.load(AtomicOrdering::SeqCst),
        counting_hs.profile_fetches.load(AtomicOrdering::SeqCst),
    );
    assert_eq!(
        counting_mod.count(),
        0,
        "offer.accept currency refusal must run before moderation lookups"
    );

    // 2. Auth and revision precedence are preserved on the same CHF offer.
    let mut stranger_accept = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    stranger_accept["aggregate_id"] = json!(format!("offer:{offer1_id}"));
    stranger_accept["payload"]["offer_id"] = json!(offer1_id.to_string());
    let (status, body) = execute(&narrowed_app, &stranger.token, &stranger_accept).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "stranger offer.accept must stay 403 UNAUTHORIZED: {body}"
    );
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));

    let mut self_accept = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    self_accept["aggregate_id"] = json!(format!("offer:{offer1_id}"));
    self_accept["payload"]["offer_id"] = json!(offer1_id.to_string());
    let (status, body) = execute(&narrowed_app, &buyer.token, &self_accept).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "author self-accept must stay 403 UNAUTHORIZED: {body}"
    );
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));

    let mut stale_accept = offer_action("offer.accept", 99, &Uuid::new_v4().to_string());
    stale_accept["aggregate_id"] = json!(format!("offer:{offer1_id}"));
    stale_accept["payload"]["offer_id"] = json!(offer1_id.to_string());
    let (status, body) = execute(&narrowed_app, &seller.token, &stale_accept).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "stale revision must stay 409 REVISION_CONFLICT: {body}"
    );
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));

    // 3. Exact replay of the prior accepted CHF offer.accept still returns
    //    200 OK with zero upstream fetches.
    counting_hs.reset();
    counting_mod.reset();
    let (status, body) = execute(&narrowed_app, &seller.token, &accept2_cmd).await;
    assert_eq!(status, StatusCode::OK, "offer.accept replay: {body}");
    assert_eq!(
        counting_hs.total_fetches(),
        0,
        "offer.accept replay must perform zero homeserver reads"
    );
    assert_eq!(
        counting_mod.count(),
        0,
        "offer.accept replay must perform zero moderation lookups"
    );

    // 4. offer.checkout on the pre-existing accepted CHF offer refuses with
    //    422 currency_unsupported and zero upstream IO, while preserving
    //    buyer-only authorization for strangers.
    counting_hs.reset();
    counting_mod.reset();
    let offer_checkout_cmd = json!({
        "version": 1,
        "command_id": Uuid::new_v4().to_string(),
        "aggregate_id": format!("offer:{offer2_id}"),
        "expected_revision": accepted_rev,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": offer2_id.to_string(),
            "award_id": award_id.to_string(),
            "listing_aggregate_id": chf_agg,
            "listing_revision": accepted_listing_rev,
            "listing_record_sha256": accepted_sha,
            "variant_id": accepted_variant,
            "quantity": 1,
            "buyer_country": "CH",
            "destination_country": "CH",
            "delivery_address": {
                "name": "Alice Example",
                "line1": "1 Bahnhofstrasse",
                "line2": "",
                "city": "Zurich",
                "region": "ZH",
                "postal_code": "8001",
                "country_code": "CH"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = execute(&narrowed_app, &buyer.token, &offer_checkout_cmd).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "offer.checkout on narrowed CHF offer: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    assert_eq!(
        counting_hs.total_fetches(),
        0,
        "offer.checkout currency refusal must perform zero homeserver reads"
    );
    assert_eq!(
        counting_mod.count(),
        0,
        "offer.checkout currency refusal must perform zero moderation lookups"
    );

    let mut stranger_offer_checkout = offer_checkout_cmd.clone();
    stranger_offer_checkout["command_id"] = json!(Uuid::new_v4().to_string());
    let (status, body) = execute(&narrowed_app, &stranger.token, &stranger_offer_checkout).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "stranger offer.checkout must stay 403 UNAUTHORIZED: {body}"
    );

    // 5. checkout.create on the pre-existing CHF listing refuses with
    //    422 currency_unsupported and zero upstream IO, while preserving
    //    seller self-purchase authorization.
    let current_rev: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(&chf_agg)
            .fetch_one(&pool)
            .await
            .expect("chf listing revision");
    counting_hs.reset();
    counting_mod.reset();
    let mut chf_checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    chf_checkout["payload"]["lines"][0]["listing_aggregate_id"] = json!(&chf_agg);
    chf_checkout["payload"]["lines"][0]["expected_revision"] = json!(current_rev);
    chf_checkout["payload"]["buyer_country"] = json!("CH");
    chf_checkout["payload"]["destination_country"] = json!("CH");
    chf_checkout["payload"]["shipping_choices"] = json!([shipping_choice]);
    let (status, body) = execute(&narrowed_app, &buyer.token, &chf_checkout).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "checkout.create on narrowed CHF listing: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("currency_unsupported"));
    assert_eq!(
        counting_hs.total_fetches(),
        0,
        "checkout.create currency refusal must perform zero homeserver reads"
    );
    assert_eq!(
        counting_mod.count(),
        0,
        "checkout.create currency refusal must perform zero moderation lookups"
    );

    let mut self_checkout = chf_checkout.clone();
    let self_cmd_id = Uuid::new_v4();
    self_checkout["command_id"] = json!(self_cmd_id.to_string());
    self_checkout["aggregate_id"] = json!(format!("checkout:{self_cmd_id}"));
    let (status, body) = execute(&narrowed_app, &seller.token, &self_checkout).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "seller self-checkout must stay 403 UNAUTHORIZED: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usd_only_blocked_or_gb_nuanced_runs_without_eur_and_nuanced_eu_uses_eur_reference(
    pool: PgPool,
) {
    use marketplace_service::config::Config;
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::payments::PaykitApi;
    use marketplace_service::region_policy::RegionModes;
    use marketplace_service::store_seller::StoreMode;

    // Part A: STORE_MODE=single_seller (claim mode, unclaimed then claimed)
    // with USD-only Kraken URL under `EU=blocked,GB=nuanced,US=blocked`.
    // No EU-27 country is nuanced, so zero EUR rows are ever needed for
    // checkout, payment bind, or GET /v1/sellers/{seller}/annual-limit.
    let mut cfg = Config::for_tests();
    cfg.store_mode = StoreMode::SingleSeller { pinned: None };
    cfg.fx_feed_provider = FxFeedProvider::Kraken;
    cfg.region_modes = Some(RegionModes::parse("EU=blocked,GB=nuanced,US=blocked").unwrap());
    let (mut app, paykit, fx) =
        test_app_with_paykit_fx_config(pool.clone(), cfg, PaykitApi::Fork).await;
    let homeserver = spawn_fake_homeserver().await;
    app = with_shop_homeserver(app, &homeserver);
    let kraken_usd_url = format!(
        "{}?pair=XXBTZUSD&count=1",
        fx.url_for(FxFeedProvider::Kraken)
    );
    app = common::reconfigured(&app, |c| {
        c.store_mode = StoreMode::SingleSeller { pinned: None };
        c.fx_feed_provider = FxFeedProvider::Kraken;
        c.fx_feed_url = kraken_usd_url.clone();
        c.region_modes = Some(RegionModes::parse("EU=blocked,GB=nuanced,US=blocked").unwrap());
    });

    let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "unclaimed health: {health}");
    assert_eq!(
        health["store"],
        json!({ "mode": "single_seller", "state": "unclaimed", "seller": null })
    );
    assert_eq!(
        health["fx_source"],
        json!({ "provider": "kraken", "pinned": false, "currencies": ["USD"] })
    );

    let now = app.clock.now();
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        let ts = t.timestamp();
        fx.set_pair_bodies(std::collections::HashMap::from([(
            "XXBTZUSD".to_string(),
            format!(
                r#"{{"error":[],"result":{{"XXBTZUSD":[["100000","0.01",{ts},"b","l","",1]],"last":"1"}}}}"#
            ),
        )]));
        let accepted = sample_fx_rate_for(&pool, FxFeedProvider::Kraken, &kraken_usd_url, t)
            .await
            .expect("USD-only Kraken sample");
        assert_eq!(accepted, 1);
    }
    let eur_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM fx_rate_samples WHERE currency = 'EUR'")
            .fetch_one(&pool)
            .await
            .expect("eur sample count");
    assert_eq!(eur_rows, 0, "USD-only feed must sample zero EUR rows");

    // Claim the single-seller store for a CH private seller.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    sqlx::query(
        "INSERT INTO store_seller_binding (singleton, seller_pubky, bound_by) \
         VALUES (TRUE, $1, 'claim')",
    )
    .bind(&seller.pubky)
    .execute(&pool)
    .await
    .expect("bind claimed seller");
    app.state.store.mark_claimed(seller.pubky.clone());
    let (status, health) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "claimed health: {health}");
    assert_eq!(
        health["store"],
        json!({ "mode": "single_seller", "state": "claimed", "seller": seller.pubky })
    );

    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Swiss Shop",
            "sellerStatus": "private",
            "location": { "countryCode": "CH" },
        }),
    );
    enable_bitcoin(&app, &paykit, &seller).await;
    let mut reg = register_command(&seller.pubky, 5);
    reg["command_id"] = json!(Uuid::new_v4().to_string());
    homeserver.publish_registration(&reg);
    let (status, body) = execute(&app, &seller.token, &reg).await;
    assert_eq!(status, StatusCode::OK, "USD listing: {body}");

    // Both an open buyer (CH) and a nuanced non-EU buyer (GB) succeed with zero EUR samples.
    for (idx, country) in [(1_u64, "CH"), (2_u64, "GB")] {
        let rev: i64 =
            sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(listing_aggregate(&seller.pubky))
                .fetch_one(&pool)
                .await
                .expect("listing revision");
        let mut checkout =
            checkout_command_with_id(&seller.pubky, &indexed_command_id(0x9800, idx));
        checkout["payload"]["lines"][0]["expected_revision"] = json!(rev);
        checkout["payload"]["buyer_country"] = json!(country);
        checkout["payload"]["destination_country"] = json!(country);
        let (status, body) = execute(&app, &buyer.token, &checkout).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "USD checkout for {country} buyer with zero EUR samples: {body}"
        );
        let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();
        let (status, body) = bind_bitcoin(&app, &buyer.token, order_id).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "USD Bitcoin bind for {country} buyer with zero EUR samples: {body}"
        );
    }

    // Annual-limit endpoint reports disabled (never fails 503 on missing EUR).
    let (status, limit_body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/sellers/{}/annual-limit", seller.pubky),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "annual-limit: {limit_body}");
    assert_eq!(limit_body, json!({ "enabled": false }));

    // Part B: Nuanced EU policy (`EU=nuanced,GB=nuanced,US=blocked`) with a
    // multi-currency Bitstamp feed (USD, EUR, GBP) samples EUR alongside USD,
    // allowing an EU-resident private seller (DE) to pass C4 conversion on USD
    // checkouts, payment bind, and /annual-limit.
    sqlx::query("DELETE FROM fx_rate_samples")
        .execute(&pool)
        .await
        .expect("clear samples");
    let bitstamp_all_url = fx.url_for(FxFeedProvider::Bitstamp);
    let app_eu = common::reconfigured(&app, |c| {
        c.store_mode = StoreMode::SingleSeller {
            pinned: Some(seller.pubky.clone()),
        };
        c.fx_feed_provider = FxFeedProvider::Bitstamp;
        c.fx_feed_url = bitstamp_all_url.clone();
        c.region_modes = Some(RegionModes::parse("EU=nuanced,GB=nuanced,US=blocked").unwrap());
    });
    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Berlin Shop",
            "sellerStatus": "private",
            "location": { "countryCode": "DE" },
        }),
    );
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        let ts = t.timestamp();
        fx.set_body(format!(
            r#"[
                {{"pair":"BTC/USD","last":"100000","timestamp":"{ts}"}},
                {{"pair":"BTC/EUR","last":"90000","timestamp":"{ts}"}},
                {{"pair":"BTC/GBP","last":"80000","timestamp":"{ts}"}}
            ]"#
        ));
        let accepted = sample_fx_rate_for(&pool, FxFeedProvider::Bitstamp, &bitstamp_all_url, t)
            .await
            .expect("Bitstamp all-pairs sample");
        assert_eq!(accepted, 3);
    }
    let rev: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing revision before EU checkout");
    let mut eu_checkout = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x9800, 3));
    eu_checkout["payload"]["lines"][0]["expected_revision"] = json!(rev);
    eu_checkout["payload"]["buyer_country"] = json!("CH");
    eu_checkout["payload"]["destination_country"] = json!("CH");
    let (status, body) = execute(&app_eu, &buyer.token, &eu_checkout).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "nuanced-EU DE seller USD checkout with EUR coverage: {body}"
    );
    let order_id = body["result"]["orders"][0]["id"].as_str().unwrap();
    let (status, body) = bind_bitcoin(&app_eu, &buyer.token, order_id).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "nuanced-EU DE seller USD bind with EUR coverage: {body}"
    );
    let (status, limit_body) = send(
        app_eu.router.clone(),
        "GET",
        &format!("/v1/sellers/{}/annual-limit", seller.pubky),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "nuanced-EU annual-limit: {limit_body}"
    );
    assert_eq!(limit_body["enabled"], json!(true));
    assert_eq!(limit_body["resident_in_eu"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_accept_and_offer_checkout_preflight_sample_clock_after_db_wait_for_expiry(
    pool: PgPool,
) {
    use marketplace_service::fx::FxFeedProvider;
    use marketplace_service::store_seller::StoreMode;

    // Create two CHF offers before narrowing to Bitstamp (USD, EUR, GBP only):
    // offer 1 stays pending (for offer.accept OFFER_EXPIRED); offer 2 is
    // accepted (for offer.checkout AWARD_EXPIRED).
    let base_app = test_app(pool.clone()).await;
    let seller = new_actor(&base_app).await;
    let buyer = new_actor(&base_app).await;

    let mut reg = register_command(&seller.pubky, 10);
    reg["command_id"] = json!(Uuid::new_v4().to_string());
    reg["aggregate_id"] = json!(format!("listing:{}_chf_exp", seller.pubky));
    reg["payload"]["listing_id"] = json!("chf_exp");
    reg["payload"]["unit_price"] = json!({
        "amount_minor": 10_000,
        "currency": "CHF",
        "exponent": 2,
    });
    let (status, body) = execute(&base_app, &seller.token, &reg).await;
    assert_eq!(status, StatusCode::OK, "register CHF listing: {body}");
    let chf_agg = format!("listing:{}_chf_exp", seller.pubky);

    let offer1_id = Uuid::new_v4();
    let mut create1 = create_offer_command(&seller.pubky, 1);
    create1["command_id"] = json!(offer1_id.to_string());
    create1["aggregate_id"] = json!(&chf_agg);
    create1["payload"]["amount"] =
        json!({ "amount_minor": 9_000, "currency": "CHF", "exponent": 2 });
    create1["payload"]["expires_in_seconds"] = json!(300);
    let (status, body) = execute(&base_app, &buyer.token, &create1).await;
    assert_eq!(status, StatusCode::OK, "create CHF offer 1: {body}");

    let offer2_id = Uuid::new_v4();
    let mut create2 = create_offer_command(&seller.pubky, 1);
    create2["command_id"] = json!(offer2_id.to_string());
    create2["aggregate_id"] = json!(&chf_agg);
    create2["payload"]["amount"] =
        json!({ "amount_minor": 9_500, "currency": "CHF", "exponent": 2 });
    create2["payload"]["expires_in_seconds"] = json!(300);
    let (status, body) = execute(&base_app, &buyer.token, &create2).await;
    assert_eq!(status, StatusCode::OK, "create CHF offer 2: {body}");

    let mut accept2 = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    accept2["aggregate_id"] = json!(format!("offer:{offer2_id}"));
    accept2["payload"]["offer_id"] = json!(offer2_id.to_string());
    let (status, body) = execute(&base_app, &seller.token, &accept2).await;
    assert_eq!(status, StatusCode::OK, "accept CHF offer 2: {body}");
    let (award_id, accepted_rev, accepted_listing_rev, accepted_sha, accepted_variant): (
        Uuid,
        i64,
        i64,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT award_id, revision, accepted_listing_revision, \
                accepted_listing_record_sha256, accepted_variant_id \
         FROM offers WHERE id = $1",
    )
    .bind(offer2_id)
    .fetch_one(&pool)
    .await
    .expect("accepted CHF offer 2 row");

    // Configure Bitstamp (does not quote CHF). Keeping Marketplace store mode
    // ensures `store_seller::command_refusal` performs zero DB queries so the
    // very first query blocking on `offers` is guaranteed to be the FX
    // preflight row lookup inside `accept_currency_refusal` /
    // `checkout_currency_refusal`.
    let narrowed = common::reconfigured(&base_app, |cfg| {
        cfg.store_mode = StoreMode::Marketplace;
        cfg.fx_feed_provider = FxFeedProvider::Bitstamp;
        cfg.fx_feed_url = marketplace_service::fx::BITSTAMP_FX_URL.to_string();
    });

    // Helper: waits until the spawned preflight SELECT on `offers` is blocked
    // on our ACCESS EXCLUSIVE table lock.
    async fn wait_for_offers_preflight_lock(pool: &PgPool) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::bigint FROM pg_stat_activity \
                 WHERE datname = current_database() \
                   AND wait_event_type = 'Lock' \
                   AND strpos(query, 'FROM offers WHERE id =') > 0 \
                   AND strpos(query, 'listing_aggregate_id') > 0",
            )
            .fetch_one(pool)
            .await
            .expect("poll pg_stat_activity");
            if waiting >= 1 {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "preflight offers SELECT never blocked on table lock"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Case 1: offer.accept enters preflight BEFORE offer 1 expires, blocks on
    // the offers DB lookup while the clock crosses offer.expires_at (+301s),
    // and must return 409 OFFER_EXPIRED rather than 422 currency_unsupported.
    let mut lock_tx = pool.begin().await.expect("begin lock tx 1");
    sqlx::query("LOCK TABLE offers IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_tx)
        .await
        .expect("lock offers table 1");

    let mut accept1 = offer_action("offer.accept", 1, &Uuid::new_v4().to_string());
    accept1["aggregate_id"] = json!(format!("offer:{offer1_id}"));
    accept1["payload"]["offer_id"] = json!(offer1_id.to_string());
    let router1 = narrowed.router.clone();
    let token1 = seller.token.clone();
    let accept_task = tokio::spawn(async move {
        send(router1, "POST", "/v1/commands", Some(&token1), &accept1).await
    });

    wait_for_offers_preflight_lock(&pool).await;
    // Advance clock past offer 1's 300s expiry (while still before offer 2's 1800s award expiry).
    narrowed.clock.advance_seconds(301);
    lock_tx.rollback().await.expect("release lock tx 1");

    let (status, body) = accept_task.await.expect("accept task joins");
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "offer.accept crossing expiry during DB wait must return 409 OFFER_EXPIRED: {body}"
    );
    assert_eq!(body["error"]["code"], json!("OFFER_EXPIRED"));

    // Case 2: offer.checkout enters preflight BEFORE offer 2's award deadline
    // (1800s from creation; 301s elapsed so 1499s remain), blocks on the
    // offers DB lookup while the clock crosses award_expires_at (+1500s), and
    // must return 409 AWARD_EXPIRED rather than 422 currency_unsupported.
    let mut lock_tx2 = pool.begin().await.expect("begin lock tx 2");
    sqlx::query("LOCK TABLE offers IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_tx2)
        .await
        .expect("lock offers table 2");

    let offer_checkout_cmd = json!({
        "version": 1,
        "command_id": Uuid::new_v4().to_string(),
        "aggregate_id": format!("offer:{offer2_id}"),
        "expected_revision": accepted_rev,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": offer2_id.to_string(),
            "award_id": award_id.to_string(),
            "listing_aggregate_id": chf_agg,
            "listing_revision": accepted_listing_rev,
            "listing_record_sha256": accepted_sha,
            "variant_id": accepted_variant,
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Example",
                "line1": "1 Bahnhofstrasse",
                "line2": "",
                "city": "Zurich",
                "region": "ZH",
                "postal_code": "8001",
                "country_code": "CH"
            },
            "guarantee_policy_version": 1
        }
    });
    let router2 = narrowed.router.clone();
    let token2 = buyer.token.clone();
    let checkout_task = tokio::spawn(async move {
        send(
            router2,
            "POST",
            "/v1/commands",
            Some(&token2),
            &offer_checkout_cmd,
        )
        .await
    });

    wait_for_offers_preflight_lock(&pool).await;
    narrowed.clock.advance_seconds(1_500);
    lock_tx2.rollback().await.expect("release lock tx 2");

    let (status, body) = checkout_task.await.expect("checkout task joins");
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "offer.checkout crossing award deadline during DB wait must return 409 AWARD_EXPIRED: {body}"
    );
    assert_eq!(body["error"]["code"], json!("AWARD_EXPIRED"));
}
