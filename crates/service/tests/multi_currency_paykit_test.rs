//! Multi-currency prices on the multi-asset Paykit contract: a non-USD fiat
//! price becomes USD cents (rounded up, one Blocktank snapshot) and is
//! requested with `asset: USD`, so USDT stays at parity with those cents and
//! paykit-server fixes the Bitcoin rate itself. The service still guards
//! paykit-server's fixed Bitcoin amount against its own `BTC<CCY>` quote.
//! The conversion is recorded on the order, pinned with a released attempt
//! and projected as `fx_conversion`. The legacy contract keeps converting
//! fiat to sats with the direct ticker.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use common::fx_feed::{fx_body_multi, FakeFxFeed};
use common::paykit_review::resolve_call;
use common::*;
use marketplace_domain::currency::Currency;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::{MarketplaceAssets, MarketplaceContract};
use marketplace_service::payments::PaykitApi;
use marketplace_service::workers::drain_outbox;
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

/// 13,700 minor units of each currency in USD cents at the cross rate,
/// rounded up: `ceil(13_700 * 100 * 77197 / (10^exponent * BTC<CCY>))`,
/// recomputed outside the service with exact fractions.
const CENTS: [(&str, u64); 6] = [
    ("EUR", 15_909),
    ("CHF", 16_790),
    ("GBP", 18_543),
    ("JPY", 9_004),
    ("CAD", 9_885),
    ("AUD", 9_829),
];

/// The direct `BTC<CCY>` quote of 13,700 minor units, in sats.
const DIRECT_SATS: [(&str, u64); 6] = [
    ("EUR", 206_074),
    ("CHF", 217_488),
    ("GBP", 240_195),
    ("JPY", 116_636),
    ("CAD", 128_047),
    ("AUD", 127_316),
];

const TX_HASH: &str = "0x9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";

fn lookup<T: Copy>(table: &[(&str, T)], code: &str) -> T {
    table
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, value)| *value)
        .expect("table entry")
}

fn price_of(code: &str) -> &'static str {
    PRICES
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, price)| *price)
        .expect("fixture price")
}

/// What the double prices `cents` at in sats (its BTCUSD is 77,197).
fn paykit_sats(cents: u64) -> u64 {
    (u128::from(cents) * 100_000_000).div_ceil(100 * 77_197) as u64
}

fn ticker_time(now: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(now.timestamp_millis()).expect("timestamp")
}

struct Setup {
    currency: &'static str,
    multi_asset: bool,
    bitcoin: bool,
    usdt: bool,
}

impl Setup {
    fn both(currency: &'static str) -> Self {
        Self {
            currency,
            multi_asset: true,
            bitcoin: true,
            usdt: true,
        }
    }
}

struct Fixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
    seller: TestActor,
    buyer: TestActor,
}

async fn seed_samples(pool: &PgPool, now: DateTime<Utc>) {
    for (currency, price) in PRICES {
        for back in 0..3_i64 {
            let accepted = now - Duration::seconds(60 * back);
            let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
            let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
            sqlx::query(
                "INSERT INTO fx_rate_samples \
                 (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
                 VALUES ($1, $2::numeric, $3, $3, $4, 'blocktank') ON CONFLICT DO NOTHING",
            )
            .bind(currency)
            .bind(price)
            .bind(accepted)
            .bind(bucket)
            .execute(pool)
            .await
            .expect("seed fx sample");
        }
    }
}

async fn put_config(app: &TestApp, token: &str, body: Value) {
    let (status, response) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {response}");
}

async fn fixture(pool: PgPool, setup: Setup) -> Fixture {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    if setup.multi_asset {
        config.paykit_marketplace_contract = MarketplaceContract::MultiAsset;
    }
    config.paykit_marketplace_assets =
        MarketplaceAssets::parse(Some("BTC,USDT"), PaykitApi::Upstream).expect("assets parse");
    let (app, paykit, fx) =
        test_app_with_paykit_fx_config(pool.clone(), config, PaykitApi::Upstream).await;
    if setup.multi_asset {
        paykit.use_multi_asset_contract();
    }
    paykit.set_usd_per_btc(77_197);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(
        &app,
        &seller.token,
        json!({ "bitcoin_enabled": setup.bitcoin, "usdt_enabled": setup.usdt }),
    )
    .await;
    paykit.set_claimed(&seller.pubky);
    paykit.set_receivable(&seller.pubky, &["BTC", "USDT"]);
    let mut listing = register_command(&seller.pubky, 40);
    listing["payload"]["unit_price"] = json!({
        "amount_minor": 12_500,
        "currency": setup.currency,
        "exponent": Currency::parse(setup.currency).expect("registry").exponent(),
    });
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    fx.set_body(fx_body_multi(&PRICES, now.timestamp_millis()));
    seed_samples(&pool, now).await;
    Fixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn checkout(f: &Fixture) -> PendingOrder {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", f.seller.pubky))
            .fetch_one(&f.app.pool)
            .await
            .expect("listing row");
    let mut command = checkout_command_with_id(&f.seller.pubky, &Uuid::new_v4().to_string());
    command["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(&f.app, &f.buyer.token, &command).await;
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

async fn bind(f: &Fixture, order_id: &str, method: &str) -> (StatusCode, Value) {
    send(
        f.app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&f.buyer.token),
        &json!({ "method": method }),
    )
    .await
}

async fn drain(f: &Fixture) {
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&f.app.pool, client, f.app.clock.now(), 30)
        .await
        .expect("outbox drains");
}

async fn read_order(f: &Fixture, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

fn uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).expect("uuid")
}

fn marketplace_call(paykit: &FakePaykit, path: &str) -> Option<Value> {
    paykit
        .calls()
        .into_iter()
        .rev()
        .find(|call| call.path == path)
        .map(|call| call.body)
}

fn prepare_body(paykit: &FakePaykit) -> Value {
    marketplace_call(paykit, "/marketplace/payment-requests/prepare").expect("a prepare call")
}

#[derive(Debug, sqlx::FromRow)]
struct Facts {
    payment_method: Option<String>,
    paykit_invoice_id: Option<Uuid>,
    paykit_asset: Option<String>,
    paykit_amount_minor: Option<i64>,
    bitcoin_quote_rate: Option<String>,
    bitcoin_quoted_sats: Option<i64>,
    usd_conversion_pair: Option<String>,
    usd_conversion_source: Option<String>,
    usd_conversion_usd_rate: Option<String>,
    usd_conversion_currency_rate: Option<String>,
    usd_conversion_fetched_at: Option<DateTime<Utc>>,
    usd_conversion_cents: Option<i64>,
}

async fn facts(pool: &PgPool, order_id: &str) -> Facts {
    sqlx::query_as(
        "SELECT payment_method, paykit_invoice_id, paykit_asset, paykit_amount_minor, \
         bitcoin_quote_rate::text, bitcoin_quoted_sats, usd_conversion_pair, \
         usd_conversion_source, usd_conversion_usd_rate::text, \
         usd_conversion_currency_rate::text, usd_conversion_fetched_at, usd_conversion_cents \
         FROM orders WHERE id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row")
}

// ---------------------------------------------------------------------------
// The request the service sends
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_non_usd_price_is_requested_as_usd_cents_and_usdt_stays_at_parity(pool: PgPool) {
    for (code, _) in CENTS {
        let f = fixture(pool.clone(), Setup::both(code)).await;
        let order = checkout(&f).await;
        let cents = lookup(&CENTS, code);
        let (status, body) = bind(&f, &order.order_id, "paykit").await;
        assert_eq!(status, StatusCode::OK, "{code}: {body}");

        let prepare = prepare_body(&f.paykit);
        assert_eq!(prepare["amount"], json!(cents.to_string()), "{code}");
        assert_eq!(prepare["asset"], json!("USD"), "{code}");
        assert!(prepare.get("amount_sats").is_none(), "{code}: {prepare}");
        assert!(
            prepare.get("accepted_assets").is_none(),
            "{code}: a seller taking both is never narrowed"
        );

        let view = &body["order"];
        assert_eq!(view["accepted_assets"], json!(["BTC", "USDT"]));
        assert_eq!(
            view["payment_amounts"],
            json!({ "BTC": paykit_sats(cents), "USDT": cents * 10_000 }),
            "{code}: USDT is exactly the converted cents at parity"
        );
        // The listing-currency total stays the price of record.
        assert_eq!(view["total"]["currency"], json!(code));
        assert_eq!(view["total"]["amount_minor"], json!(13_700));

        // The direct BTC<CCY> quote is the guard and is recorded as such.
        let quote = &view["bitcoin_quote"];
        assert_eq!(quote["pair"], json!(format!("BTC{code}")), "{code}");
        assert_eq!(quote["rate"], json!(price_of(code)));
        assert_eq!(quote["quoted_sats"], json!(lookup(&DIRECT_SATS, code)));
        assert_eq!(quote["currency"], json!(code));

        // The conversion is on the order and in its projection.
        let conversion = &view["fx_conversion"];
        assert_eq!(conversion["pair"], json!(format!("{code}USD")), "{code}");
        assert_eq!(conversion["source"], json!("blocktank"));
        assert_eq!(conversion["usd_rate"], json!("77197"));
        assert_eq!(conversion["currency_rate"], json!(price_of(code)));
        assert_eq!(
            conversion["from"],
            json!({ "amount_minor": 13_700, "currency": code, "exponent":
                Currency::parse(code).expect("registry").exponent() })
        );
        assert_eq!(
            conversion["to"],
            json!({ "amount_minor": cents, "currency": "USD", "exponent": 2 })
        );
        assert_eq!(
            conversion["fetched_at"],
            json!(marketplace_service::clock::format_timestamp(ticker_time(
                f.app.clock.now()
            )))
        );
        let rate = conversion["rate"].as_str().expect("cross rate");
        assert_eq!(
            rate.split_once('.').expect("decimal").1.len(),
            12,
            "{code}: {rate}"
        );

        let stored = facts(&pool, &order.order_id).await;
        assert_eq!(stored.paykit_asset.as_deref(), Some("USD"));
        assert_eq!(stored.paykit_amount_minor, Some(cents as i64));
        assert_eq!(
            stored.usd_conversion_pair.as_deref(),
            Some(format!("{code}USD").as_str())
        );
        assert_eq!(stored.usd_conversion_source.as_deref(), Some("blocktank"));
        assert_eq!(stored.usd_conversion_usd_rate.as_deref(), Some("77197"));
        assert_eq!(
            stored.usd_conversion_currency_rate.as_deref(),
            Some(price_of(code))
        );
        assert_eq!(
            stored.usd_conversion_fetched_at,
            Some(ticker_time(f.app.clock.now()))
        );
        assert_eq!(stored.usd_conversion_cents, Some(cents as i64));
        assert_eq!(stored.bitcoin_quote_rate.as_deref(), Some(price_of(code)));
        assert_eq!(
            stored.bitcoin_quoted_sats,
            Some(lookup(&DIRECT_SATS, code) as i64)
        );
        assert_eq!(f.fx.fetch_count(), 1, "{code}: one snapshot for the bind");

        // Activation repeats the same denomination.
        drain(&f).await;
        let activate = marketplace_call(&f.paykit, "/marketplace/payment-requests/activate")
            .expect("activation sent");
        assert_eq!(activate["amount"], json!(cents.to_string()), "{code}");
        assert_eq!(activate["asset"], json!("USD"), "{code}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usd_price_needs_no_conversion_and_its_projection_is_unchanged(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both("USD")).await;
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(prepare_body(&f.paykit)["amount"], json!("13700"));
    assert_eq!(prepare_body(&f.paykit)["asset"], json!("USD"));
    assert!(
        body["order"].get("fx_conversion").is_none(),
        "{}",
        body["order"]
    );
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.usd_conversion_pair, None);
    assert_eq!(stored.usd_conversion_cents, None);
    assert_eq!(body["order"]["bitcoin_quote"]["pair"], json!("BTCUSD"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_only_seller_pays_the_converted_cents_at_parity_without_a_bitcoin_quote(
    pool: PgPool,
) {
    let f = fixture(
        pool.clone(),
        Setup {
            bitcoin: false,
            ..Setup::both("EUR")
        },
    )
    .await;
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount"], json!("15909"));
    assert_eq!(prepare["asset"], json!("USD"));
    assert_eq!(prepare["accepted_assets"], json!(["USDT"]));
    let view = &body["order"];
    assert_eq!(view["accepted_assets"], json!(["USDT"]));
    assert_eq!(view["payment_amounts"], json!({ "USDT": 159_090_000 }));
    assert_eq!(view["bitcoin_quote"]["rate"], json!(null), "no Bitcoin leg");
    assert_eq!(view["fx_conversion"]["to"]["amount_minor"], json!(15_909));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.bitcoin_quote_rate, None);
    assert_eq!(stored.usd_conversion_cents, Some(15_909));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_contract_converts_fiat_to_sats_with_the_direct_ticker(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            multi_asset: false,
            usdt: false,
            ..Setup::both("EUR")
        },
    )
    .await;
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount_sats"], json!(206_074), "{prepare}");
    assert!(prepare.get("asset").is_none(), "{prepare}");
    assert!(body["order"].get("fx_conversion").is_none());
    assert_eq!(body["order"]["bitcoin_quote"]["pair"], json!("BTCEUR"));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.usd_conversion_pair, None);
    assert_eq!(stored.paykit_amount_minor, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_bitcoin_bind_of_a_non_usd_price_is_unchanged_on_the_new_contract(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both("JPY")).await;
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["asset"], json!("BTC"));
    assert_eq!(prepare["amount"], json!("116636"));
    assert_eq!(prepare["accepted_assets"], json!(["BTC"]));
    assert!(body["order"].get("fx_conversion").is_none());
    assert_eq!(body["order"]["bitcoin_quote"]["pair"], json!("BTCJPY"));
}

// ---------------------------------------------------------------------------
// Guards
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_amount_far_from_the_direct_quote_is_refused_voided_and_leaves_no_record(
    pool: PgPool,
) {
    for code in ["EUR", "JPY"] {
        let f = fixture(pool.clone(), Setup::both(code)).await;
        f.paykit.set_usd_per_btc(40_000);
        let order = checkout(&f).await;
        let (status, body) = bind(&f, &order.order_id, "paykit").await;
        assert_eq!(status, StatusCode::CONFLICT, "{code}: {body}");
        assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));
        let stored = facts(&pool, &order.order_id).await;
        assert_eq!(stored.payment_method, None);
        assert_eq!(stored.paykit_invoice_id, None);
        assert_eq!(stored.usd_conversion_pair, None, "{code}: nothing recorded");
        assert_eq!(stored.bitcoin_quote_rate, None);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            f.paykit
                .calls()
                .iter()
                .any(|call| call.path == "/marketplace/payment-requests/void"),
            "{code}: the prepared invoice is voided"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_conversion_with_an_unusable_leg_is_refused_before_paykit_is_called(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both("EUR")).await;
    let now = f.app.clock.now();
    let current = now.timestamp_millis();
    let cases = [
        // The USD leg is stale while EUR is fresh.
        (
            format!(
                r#"{{"tickers":[
                    {{"symbol":"BTCUSD","lastPrice":"77197","base":"BTC","quote":"USD","lastUpdatedAt":{}}},
                    {{"symbol":"BTCEUR","lastPrice":"66481","base":"BTC","quote":"EUR","lastUpdatedAt":{current}}}
                ]}}"#,
                current - 301_000
            ),
            "fx_rate_stale",
        ),
        // The EUR leg is 10% from its reference.
        (
            fx_body_multi(&[("USD", "77197"), ("EUR", "73129")], current),
            "fx_deviation_exceeded",
        ),
        // The USD ticker is missing.
        (
            fx_body_multi(&[("EUR", "66481")], current),
            "fx_reference_unavailable",
        ),
    ];
    for (feed, reason) in cases {
        f.fx.set_body(feed);
        let order = checkout(&f).await;
        let (status, body) = bind(&f, &order.order_id, "paykit").await;
        assert_eq!(status, StatusCode::CONFLICT, "{reason}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason));
        assert!(
            marketplace_call(&f.paykit, "/marketplace/payment-requests/prepare").is_none(),
            "{reason}: paykit was never asked"
        );
        assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);
    }
    // A USDT-only seller has no Bitcoin leg to fall back on: the feed is
    // needed for the USD amount, so an unreadable feed refuses the bind.
    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    f.fx.set_status(500);
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usd_priced_usdt_only_seller_still_binds_with_the_feed_down(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            bitcoin: false,
            ..Setup::both("USD")
        },
    )
    .await;
    f.fx.set_status(500);
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(prepare_body(&f.paykit)["amount"], json!("13700"));
    assert_eq!(f.fx.fetch_count(), 0, "USDT at parity needs no feed");
}

// ---------------------------------------------------------------------------
// Re-binds, released attempts, refunds
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_released_attempt_keeps_its_conversion_and_a_rebind_leaves_none_on_the_order(
    pool: PgPool,
) {
    let f = fixture(pool.clone(), Setup::both("EUR")).await;
    let order = checkout(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let invoice = facts(&pool, &order.order_id)
        .await
        .paykit_invoice_id
        .expect("invoice");
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);

    bind_paypal_method(&f.app, &f.seller, &f.buyer, &order.order_id).await;
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["payment_method"], json!("paypal"));
    assert!(view.get("fx_conversion").is_none(), "{view}");
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.usd_conversion_pair, None);
    assert_eq!(stored.usd_conversion_cents, None);

    let pinned: (Option<String>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT usd_conversion_pair, usd_conversion_cents, usd_conversion_usd_rate::text \
         FROM paykit_superseded_attempts WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(uuid(&order.order_id))
    .bind(invoice)
    .fetch_one(&pool)
    .await
    .expect("the released attempt is recorded");
    assert_eq!(
        pinned,
        (
            Some("EURUSD".to_string()),
            Some(15_909),
            Some("77197".to_string())
        ),
        "the released attempt keeps its conversion"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_of_a_non_usd_order_is_recorded_in_usdt_units_and_capped_at_what_was_paid(
    pool: PgPool,
) {
    let f = fixture(pool.clone(), Setup::both("EUR")).await;
    let order = checkout(&f).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    drain(&f).await;
    let stored: (Uuid, DateTime<Utc>, i64) = sqlx::query_as(
        "SELECT paykit_invoice_id, paykit_expires_at, revision FROM orders WHERE id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("order");
    let (invoice, deadline, revision) = stored;
    let offered_units = 15_909u64 * 10_000;
    let paid_units = offered_units - 1;
    // The transfer is included one unit short: review, never paid.
    let status_body = json!({
        "invoice_id": invoice,
        "state": "active",
        "proposal_delivery_state": "delivered",
        "request_state": "accepted",
        "payment_state": "confirmed",
        "activated_at": (deadline - Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Micros, true),
        "payment_deadline": deadline.to_rfc3339_opts(SecondsFormat::Micros, true),
        "accepted_assets": ["BTC", "USDT"],
        "payment_amounts": {
            "BTC": paykit_sats(15_909).to_string(),
            "USDT": offered_units.to_string(),
        },
        "bitcoin": null,
        "usdt_arbitrum": {
            "tx_hash": TX_HASH,
            "observed_amount": paid_units.to_string(),
            "confirmations": 2,
            "amount_matched": false,
            "paid_on_time": false,
            "finalized": false,
        },
        "paid_asset": null,
        "outcome": null,
        "resolved_at": null,
    });
    f.paykit.set_marketplace_status(invoice, status_body);
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    marketplace_service::workers::verify_due_paykit_payments(
        &f.app.state,
        client,
        Utc::now() + Duration::seconds(300),
    )
    .await
    .expect("poll runs");
    let revision_now: i64 = sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1")
        .bind(uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("revision");
    assert!(revision_now >= revision);
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["payment_asset"], json!("USDT"));
    assert_ne!(view["state"], json!("paid"));
    assert_eq!(view["payment_amount_minor"], json!(paid_units));

    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision_now,
            json!({ "address": address }),
            41,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "address confirmed: {body}");
    let reference = format!("0x{}", "ef56".repeat(16));
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund recorded: {body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(paid_units),
        "a EUR-priced order refunds in USDT0 millionths, the amount paid"
    );
    assert_eq!(view["total"]["currency"], json!("EUR"));
}
