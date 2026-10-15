//! One Paykit bind (`paykit`) for every asset a seller accepts: the request
//! the service sends, the offer it records, settlement on the rail the buyer
//! actually paid, USDT finality gating physical fulfilment, both rails paid,
//! and the compatibility rules (rc16 upstream, fork, legacy `bitcoin` bind).
//!
//! The Marketplace status bodies built here are spec-derived from the PK1
//! contract (`usdt_arbitrum`, `accepted_assets`, `payment_amounts`,
//! `paid_asset`); `single_paykit_captured_test.rs` replays the bodies
//! captured from paykit-server.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use common::fx_feed::{fx_body, FakeFxFeed};
use common::paykit_review::{
    create_sat_order, delivered_notifications, enable_bitcoin, poll_now, resolve_call,
};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::MarketplaceAssets;
use marketplace_service::payments::PaykitApi;
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const RATE: &str = "77197";
const TOTAL_CENTS: u64 = 13_700;
const TOTAL_USDT_UNITS: u64 = 137_000_000;
const TX_HASH: &str = "0x9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";
const BTC_TXID: &str = "00000000000000000000000000000000000000000000000000000000000000a1";

fn quoted_sats() -> u64 {
    (u128::from(TOTAL_CENTS) * 100_000_000).div_ceil(100 * 77_197) as u64
}

fn config(usdt_flag: bool, multi_asset: bool) -> Config {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = usdt_flag;
    if multi_asset {
        config.paykit_marketplace_assets =
            MarketplaceAssets::parse(Some("BTC,USDT"), PaykitApi::Upstream).expect("assets parse");
    }
    config
}

struct Fixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
    seller: TestActor,
    buyer: TestActor,
}

struct Setup {
    api: PaykitApi,
    usdt_flag: bool,
    multi_asset: bool,
    consent: bool,
    receivable: &'static [&'static str],
    usd_listing: bool,
}

impl Setup {
    /// The deployment this change targets: upstream rc17, USDT on, a seller
    /// who accepts both assets.
    fn both() -> Self {
        Self {
            api: PaykitApi::Upstream,
            usdt_flag: true,
            multi_asset: true,
            consent: true,
            receivable: &["BTC", "USDT"],
            usd_listing: true,
        }
    }
}

async fn seed_fx_samples(pool: &PgPool, now: DateTime<Utc>) {
    for back in 0..3_i64 {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank') \
             ON CONFLICT DO NOTHING",
        )
        .bind(RATE)
        .bind(accepted)
        .bind(bucket)
        .execute(pool)
        .await
        .expect("seed fx sample");
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
    let (app, paykit, fx) = test_app_with_paykit_fx_config(
        pool.clone(),
        config(setup.usdt_flag, setup.multi_asset),
        setup.api,
    )
    .await;
    if setup.multi_asset {
        paykit.use_multi_asset_contract();
    }
    paykit.set_usd_per_btc(77_197);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut rails = json!({ "bitcoin_enabled": true });
    if setup.usdt_flag {
        rails["usdt_enabled"] = json!(setup.consent);
    }
    put_config(&app, &seller.token, rails).await;
    paykit.set_claimed(&seller.pubky);
    paykit.set_receivable(&seller.pubky, setup.receivable);
    if setup.usd_listing {
        let (status, body) =
            execute(&app, &seller.token, &register_command(&seller.pubky, 40)).await;
        assert_eq!(status, StatusCode::OK, "register failed: {body}");
    }
    if setup.api == PaykitApi::Upstream {
        let now = app.clock.now();
        fx.set_body(fx_body(RATE, now.timestamp_millis()));
        seed_fx_samples(&pool, now).await;
    }
    Fixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn checkout_usd(f: &Fixture) -> PendingOrder {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", f.seller.pubky))
            .fetch_one(&f.app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&f.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(&f.app, &f.buyer.token, &checkout).await;
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

fn prepare_body(paykit: &FakePaykit) -> Value {
    paykit
        .calls()
        .into_iter()
        .rev()
        .find(|call| call.path == "/marketplace/payment-requests/prepare")
        .expect("a prepare call")
        .body
}

#[derive(Debug, sqlx::FromRow)]
struct OrderFacts {
    state: String,
    payment_method: Option<String>,
    paykit_invoice_id: Option<Uuid>,
    paykit_asset: Option<String>,
    paykit_amount_minor: Option<i64>,
    paykit_total_sats: Option<i64>,
    paykit_request_state: Option<String>,
    paykit_expires_at: Option<DateTime<Utc>>,
    bitcoin_quoted_sats: Option<i64>,
    payment_asset: Option<String>,
    payment_finality: Option<String>,
    paykit_second_rail_at: Option<DateTime<Utc>>,
    revision: i64,
}

async fn facts(pool: &PgPool, order_id: &str) -> OrderFacts {
    sqlx::query_as(
        "SELECT state, payment_method, paykit_invoice_id, paykit_asset, paykit_amount_minor, \
         paykit_total_sats, paykit_request_state, paykit_expires_at, bitcoin_quoted_sats, \
         payment_asset, payment_finality, paykit_second_rail_at, revision \
         FROM orders WHERE id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn payment_row(pool: &PgPool, order_id: &str) -> (String, i64, String, i32, Option<String>) {
    sqlx::query_as(
        "SELECT state, amount_minor, currency, exponent, review_reason FROM payments \
         WHERE order_id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("payment row")
}

async fn count(pool: &PgPool, sql: &str, order_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(uuid(order_id))
        .fetch_one(pool)
        .await
        .expect("count")
}

fn usdt_leg(confirmations: u32, matched: bool, on_time: bool, finalized: bool) -> Value {
    json!({
        "tx_hash": TX_HASH,
        "observed_units": TOTAL_USDT_UNITS,
        "confirmations": confirmations,
        "amount_matched": matched,
        "paid_on_time": on_time,
        "finalized": finalized,
    })
}

fn bitcoin_leg(confirmations: u32, matched: bool) -> Value {
    json!({
        "txid": BTC_TXID,
        "observed_sats": quoted_sats(),
        "confirmations": confirmations,
        "amount_matched": matched,
        "paid_on_time": true,
    })
}

struct StatusSpec {
    payment_state: &'static str,
    bitcoin: Option<Value>,
    usdt: Option<Value>,
    paid_asset: Option<&'static str>,
}

/// A Marketplace status body in the multi-asset contract's shape, for an
/// active invoice offering both assets.
fn status_body(invoice: Uuid, deadline: DateTime<Utc>, spec: StatusSpec) -> Value {
    json!({
        "invoice_id": invoice,
        "state": "active",
        "proposal_delivery_state": "delivered",
        "request_state": "accepted",
        "payment_state": spec.payment_state,
        "activated_at": (deadline - Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Micros, true),
        "payment_deadline": deadline.to_rfc3339_opts(SecondsFormat::Micros, true),
        "bitcoin": spec.bitcoin,
        "usdt_arbitrum": spec.usdt,
        "accepted_assets": ["BTC", "USDT"],
        "payment_amounts": { "BTC": quoted_sats(), "USDT": TOTAL_USDT_UNITS },
        "paid_asset": spec.paid_asset,
        "outcome": null,
        "resolved_at": null,
    })
}

fn usdt_status(invoice: Uuid, deadline: DateTime<Utc>, usdt: Value) -> Value {
    status_body(
        invoice,
        deadline,
        StatusSpec {
            payment_state: "confirmed",
            bitcoin: None,
            usdt: Some(usdt),
            paid_asset: Some("USDT"),
        },
    )
}

fn nothing_observed(invoice: Uuid, deadline: DateTime<Utc>) -> Value {
    status_body(
        invoice,
        deadline,
        StatusSpec {
            payment_state: "undetected",
            bitcoin: None,
            usdt: None,
            paid_asset: None,
        },
    )
}

struct Active {
    order: PendingOrder,
    invoice: Uuid,
    deadline: DateTime<Utc>,
}

/// A `paykit` order bound and activated at paykit-server, ready to poll.
async fn active_order(f: &Fixture) -> Active {
    let order = checkout_usd(f).await;
    let (status, body) = bind(f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "paykit bind failed: {body}");
    drain(f).await;
    let facts = facts(&f.app.pool, &order.order_id).await;
    assert_eq!(facts.paykit_request_state.as_deref(), Some("pending"));
    Active {
        invoice: facts.paykit_invoice_id.expect("invoice pinned"),
        deadline: facts.paykit_expires_at.expect("deadline pinned"),
        order,
    }
}

/// The poll instant for the n-th poll of a test: every call is past the
/// previous call's poll interval.
fn at(n: i64) -> DateTime<Utc> {
    Utc::now() + Duration::seconds(300 * n)
}

fn ship(order_id: &str, revision: i64, number: u64) -> Value {
    order_command(
        "fulfillment.ship",
        order_id,
        revision,
        json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
        number,
    )
}

// ---------------------------------------------------------------------------
// The request the service sends and the offer it records
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_paykit_bind_sends_the_denomination_and_records_every_offered_asset(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount"], json!(TOTAL_CENTS.to_string()));
    assert_eq!(prepare["asset"], json!("USD"));
    assert!(
        prepare.get("accepted_assets").is_none(),
        "a seller that takes both assets is never narrowed: {prepare}"
    );
    assert!(prepare.get("amount_sats").is_none(), "{prepare}");

    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC", "USDT"]));
    assert_eq!(
        view["payment_amounts"],
        json!({ "BTC": quoted_sats(), "USDT": TOTAL_USDT_UNITS })
    );
    assert!(
        view.get("payment_asset").is_none() && view.get("payment_finality").is_none(),
        "nothing is paid yet: {view}"
    );
    assert_eq!(view["paykit_total_sats"], json!(quoted_sats()));

    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_asset.as_deref(), Some("USD"));
    assert_eq!(stored.paykit_amount_minor, Some(TOTAL_CENTS as i64));
    assert_eq!(stored.bitcoin_quoted_sats, Some(quoted_sats() as i64));
    let payment = payment_row(&pool, &order.order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (quoted_sats() as i64, "SAT", 0)
    );

    drain(&f).await;
    let activate = f
        .paykit
        .calls()
        .into_iter()
        .find(|call| call.path == "/marketplace/payment-requests/activate")
        .expect("activation sent");
    assert_eq!(activate.body["amount"], json!(TOTAL_CENTS.to_string()));
    assert_eq!(activate.body["asset"], json!("USD"));
    assert!(
        activate.body.get("total_sats").is_none(),
        "{}",
        activate.body
    );
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_request_state.as_deref(), Some("pending"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_consent_and_the_flag_narrow_the_request_to_bitcoin(pool: PgPool) {
    for (usdt_flag, consent) in [(true, false), (false, false)] {
        let f = fixture(
            pool.clone(),
            Setup {
                usdt_flag,
                consent,
                ..Setup::both()
            },
        )
        .await;
        let order = checkout_usd(&f).await;
        let (status, body) = bind(&f, &order.order_id, "paykit").await;
        assert_eq!(status, StatusCode::OK, "bind failed: {body}");
        assert_eq!(
            prepare_body(&f.paykit)["accepted_assets"],
            json!(["BTC"]),
            "flag {usdt_flag}, consent {consent}"
        );
        assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
        assert_eq!(
            body["order"]["payment_amounts"],
            json!({ "BTC": quoted_sats() })
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_without_a_usdt_address_offers_bitcoin_only(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            receivable: &["BTC"],
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert!(prepare_body(&f.paykit).get("accepted_assets").is_none());
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_fx_feed_narrows_to_usdt_and_fails_a_bitcoin_only_bind(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    f.fx.set_status(500);
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(prepare_body(&f.paykit)["accepted_assets"], json!(["USDT"]));
    let view = &body["order"];
    assert_eq!(view["accepted_assets"], json!(["USDT"]));
    assert_eq!(view["payment_amounts"], json!({ "USDT": TOTAL_USDT_UNITS }));
    assert!(view["paykit_total_sats"].is_null(), "{view}");
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(
        stored.bitcoin_quoted_sats, None,
        "no Bitcoin quote is pinned"
    );
    let payment = payment_row(&pool, &order.order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (TOTAL_USDT_UNITS as i64, "USDT", 6),
        "a USDT-only request types its payment in USDT0"
    );
    drain(&f).await;
    assert_eq!(
        facts(&pool, &order.order_id)
            .await
            .paykit_request_state
            .as_deref(),
        Some("pending"),
        "a USDT-only request activates by its denomination"
    );

    let g = fixture(
        pool.clone(),
        Setup {
            consent: false,
            ..Setup::both()
        },
    )
    .await;
    g.fx.set_status(500);
    let order = checkout_usd(&g).await;
    let (status, body) = bind(&g, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert!(
        g.paykit
            .calls()
            .iter()
            .all(|call| !call.path.starts_with("/marketplace/payment-requests")),
        "no request left the process"
    );
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_amount_far_from_the_service_quote_is_refused_and_voided(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    f.paykit.set_usd_per_btc(40_000);
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.payment_method, None);
    assert_eq!(stored.paykit_invoice_id, None);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        f.paykit
            .calls()
            .iter()
            .any(|call| call.path == "/marketplace/payment-requests/void"),
        "the prepared invoice is voided"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_priced_order_is_requested_in_sats_and_never_offers_usdt(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            usd_listing: false,
            ..Setup::both()
        },
    )
    .await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["asset"], json!("BTC"));
    assert_eq!(prepare["accepted_assets"], json!(["BTC"]));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_asset.as_deref(), Some("BTC"));
    assert_eq!(
        stored.paykit_amount_minor, stored.paykit_total_sats,
        "no conversion: the request amount is the Bitcoin amount"
    );
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_bitcoin_bind_keeps_its_service_quote_on_the_new_contract(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["asset"], json!("BTC"));
    assert_eq!(prepare["amount"], json!(quoted_sats().to_string()));
    assert_eq!(prepare["accepted_assets"], json!(["BTC"]));
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("bitcoin"));
    assert!(
        view.get("accepted_assets").is_none() && view.get("payment_amounts").is_none(),
        "a legacy order keeps its projection: {view}"
    );
    assert_eq!(view["paykit_total_sats"], json!(quoted_sats()));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_an_rc16_upstream_the_paykit_bind_is_the_legacy_sats_request(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            multi_asset: false,
            usdt_flag: false,
            consent: false,
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount_sats"], json!(quoted_sats()));
    for key in ["amount", "asset", "accepted_assets"] {
        assert!(prepare.get(key).is_none(), "{key} in {prepare}");
    }
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC"]));
    assert_eq!(view["payment_amounts"], json!({ "BTC": quoted_sats() }));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_amount_minor, None, "legacy activation");
    drain(&f).await;
    let activate = f
        .paykit
        .calls()
        .into_iter()
        .find(|call| call.path == "/marketplace/payment-requests/activate")
        .expect("activation sent");
    assert_eq!(activate.body["total_sats"], json!(quoted_sats()));
    assert!(activate.body.get("amount").is_none(), "{}", activate.body);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_the_fork_the_paykit_bind_is_bitcoin_through_the_fork(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            api: PaykitApi::Fork,
            multi_asset: false,
            usdt_flag: false,
            consent: false,
            usd_listing: false,
            ..Setup::both()
        },
    )
    .await;
    enable_bitcoin(&f.app, &f.paykit, &f.seller).await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let requests = f.paykit.requests();
    assert_eq!(requests.len(), 1, "one fork prepare");
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC"]));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_amount_minor, None);
    assert!(
        f.paykit
            .calls()
            .iter()
            .all(|call| !call.path.starts_with("/marketplace/")),
        "the fork has no Marketplace routes"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_usdt_bind_stays_refused_and_rebinding_another_method_is_refused(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "usdt").await;
    assert_ne!(status, StatusCode::OK, "{body}");
    assert_eq!(body["error"]["reason"], json!("usdt_unavailable"), "{body}");
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);

    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "same method is idempotent: {body}");
    let (status, body) = bind(&f, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("payment_method_already_bound")
    );
    assert_eq!(
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path == "/marketplace/payment-requests/prepare")
            .count(),
        1,
        "the idempotent rebind prepared nothing"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_public_config_names_the_one_paykit_method_and_its_assets(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let (status, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["paykit_available"], json!(true), "{body}");
    assert_eq!(body["accepted_assets"], json!(["BTC", "USDT"]), "{body}");
    assert_eq!(body["bitcoin_available"], json!(true));
    assert_eq!(body["usdt_available"], json!(true));

    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": true, "usdt_enabled": false }),
    )
    .await;
    let (_, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(body["paykit_available"], json!(true), "{body}");
    assert_eq!(body["accepted_assets"], json!(["BTC"]), "{body}");
    assert_eq!(body["usdt_available"], json!(false));

    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    let (_, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(
        body["paykit_available"],
        json!(false),
        "Paykit needs the seller's Bitcoin rail switch: {body}"
    );
    assert_eq!(body["accepted_assets"], json!([]));
}

// ---------------------------------------------------------------------------
// Settlement by the rail that paid
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_pays_at_inclusion_and_fulfilment_waits_for_finality(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);

    let (payment_state, amount, currency, exponent, _) = payment_row(&pool, &order_id).await;
    assert_eq!(payment_state, "confirmed");
    assert_eq!(
        (amount, currency.as_str(), exponent),
        (TOTAL_USDT_UNITS as i64, "USDT", 6),
        "the payment is retyped to the rail that paid"
    );
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["payment_asset"], json!("USDT"));
    assert_eq!(view["payment_network"], json!("arbitrum-one"));
    assert_eq!(view["payment_amount_minor"], json!(TOTAL_USDT_UNITS));
    assert_eq!(view["payment_exponent"], json!(6));
    assert_eq!(view["payment_quote_basis"], json!("parity"));
    assert_eq!(view["payment_finality"], json!("pending"));
    assert_eq!(view["accepted_assets"], json!(["BTC", "USDT"]));
    let receipt: (String, i64, i32) =
        sqlx::query_as("SELECT currency, total_minor, exponent FROM receipts WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("receipt");
    assert_eq!(
        (receipt.0.as_str(), receipt.1, receipt.2),
        ("USDT", TOTAL_USDT_UNITS as i64, 6)
    );

    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(&f.app, &f.seller.token, &ship(&order_id, revision, 1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["reason"],
        json!("payment_not_final"),
        "{body}"
    );
    assert_eq!(facts(&pool, &order_id).await.state, "paid");

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(220, true, true, true),
        ),
    );
    assert_eq!(poll_now(&f.app, at(2)).await, 1, "finality moves the order");
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("final"));
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship after finality: {body}");
    assert_eq!(body["result"]["order"]["payment_finality"], json!("final"));
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:' || $1::text \
             AND kind = 'payment.finality_changed'",
            &order_id
        )
        .await,
        2,
        "pending, then final"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_reorged_usdt_transfer_reverts_until_it_returns_and_final_is_terminal(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(2, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );

    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("reverted"));
    assert_eq!(stored.state, "paid", "finality never un-pays the order");
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("payment_not_final"));

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending"),
        "a transfer that reappears is pending again"
    );

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(300, true, true, true),
        ),
    );
    poll_now(&f.app, at(4)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("final")
    );
    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(5)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("final"),
        "a finalized transfer is never reverted by a later poll"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unavailable_status_never_reverts_a_usdt_payment(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    f.paykit.fail_marketplace_status_with(503, "unavailable");
    poll_now(&f.app, at(2)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );
    f.paykit.clear_marketplace_status_failure();
    // `paid_asset` names Bitcoin, which has no observation: the echo
    // disagrees with itself and the whole status fails closed.
    let mut malformed = usdt_status(
        active.invoice,
        active.deadline,
        usdt_leg(1, true, true, false),
    );
    malformed["paid_asset"] = json!("BTC");
    f.paykit.set_marketplace_status(active.invoice, malformed);
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending"),
        "a status that fails closed is never read as a reorg"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_through_the_paykit_method_settles_as_before_and_records_its_rail(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_asset"], json!("BTC"));
    assert_eq!(view["payment_network"], json!("bitcoin"));
    assert_eq!(view["payment_amount_minor"], json!(quoted_sats()));
    assert_eq!(view["payment_exponent"], json!(8));
    assert_eq!(view["payment_quote_basis"], json!("fixed_rate"));
    assert!(view.get("payment_finality").is_none(), "{view}");
    let payment = payment_row(&pool, &order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (quoted_sats() as i64, "SAT", 0)
    );
    let stored = facts(&pool, &order_id).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Bitcoin ships at once: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_transfer_for_the_wrong_amount_or_too_late_goes_to_review_never_paid(pool: PgPool) {
    for (matched, on_time) in [(false, true), (true, false)] {
        let f = fixture(pool.clone(), Setup::both()).await;
        let active = active_order(&f).await;
        let order_id = active.order.order_id.clone();
        f.paykit.set_marketplace_status(
            active.invoice,
            status_body(
                active.invoice,
                active.deadline,
                StatusSpec {
                    payment_state: "confirmed",
                    bitcoin: None,
                    usdt: Some(usdt_leg(1, matched, on_time, false)),
                    paid_asset: None,
                },
            ),
        );
        poll_now(&f.app, at(1)).await;
        let stored = facts(&pool, &order_id).await;
        assert_ne!(stored.state, "paid", "matched {matched}, on time {on_time}");
        assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
        let payment = payment_row(&pool, &order_id).await;
        assert_ne!(
            payment.0, "confirmed",
            "matched {matched}, on time {on_time}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_both_rails_goes_to_manual_review_and_is_never_refunded(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("BTC"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let payment = payment_row(&pool, &order_id).await;
    assert_eq!(payment.0, "manual_review");
    assert_eq!(payment.4.as_deref(), Some("amount_mismatch"));
    let stored = facts(&pool, &order_id).await;
    assert_eq!(
        stored.state, "pending_payment",
        "nothing settles on its own"
    );
    assert_eq!(
        stored.payment_asset.as_deref(),
        Some("BTC"),
        "the rail Paykit names as paid first is the order's paid rail"
    );
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM receipts WHERE order_id = $1",
            &order_id
        )
        .await,
        0
    );
    let (external, observation): (Option<Value>, Option<Value>) =
        sqlx::query_as("SELECT external_refund, paykit_observation FROM orders WHERE id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert!(external.is_none(), "never an automatic refund");
    let observation = observation.expect("both rails frozen");
    assert_eq!(observation["other_rail_asset"], json!("USDT"));
    assert_eq!(observation["other_rail_reference"], json!(TX_HASH));
    let seller_view = {
        let (status, body) = send(
            f.app.router.clone(),
            "GET",
            &format!("/v1/orders/{order_id}"),
            Some(&f.seller.token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    };
    assert_eq!(
        seller_view["paykit_observation"]["other_rail_asset"],
        json!("USDT"),
        "the seller sees both rails"
    );
    let notices = delivered_notifications(&f.app, &f.seller.token, "bitcoin_manual_review").await;
    assert_eq!(notices.len(), 1, "the seller is told once: {notices:?}");
    assert_eq!(notices[0]["review_reason"], json!("amount_mismatch"));

    // A later poll leaves the review alone.
    assert_eq!(poll_now(&f.app, at(2)).await, 0);
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");

    // The seller resolves it through the existing path.
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "reason": "returned the extra payment from my wallet" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolve failed: {body}");
    assert_eq!(facts(&pool, &order_id).await.state, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_the_other_rail_after_settlement_is_held_for_an_operator_once(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(facts(&pool, &order_id).await.state, "paid");

    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(2, true)),
                usdt: Some(usdt_leg(5, true, true, false)),
                paid_asset: Some("USDT"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(2)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid", "the order is never un-paid");
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(payment_row(&pool, &order_id).await.0, "confirmed");
    let held: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_all(&pool)
    .await
    .expect("held attempts");
    assert_eq!(
        held,
        vec![(
            "needs_review".to_string(),
            Some("payment_settled".to_string())
        )]
    );
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM paykit_superseded_attempts WHERE order_id = $1",
            &order_id
        )
        .await,
        1,
        "flagged once"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_after_a_bitcoin_settlement_is_held_for_an_operator(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_asset.as_deref(),
        Some("BTC")
    );
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(4, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.payment_asset.as_deref(), Some("BTC"));
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM paykit_superseded_attempts \
             WHERE order_id = $1 AND state = 'needs_review'",
            &order_id
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_follows_the_paid_rail_and_a_bitcoin_refund_does_not(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": address }),
            5,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "USDT order takes an address: {body}"
    );
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["refund_destination"]["address"], json!(address));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_order_has_no_refund_address_to_confirm(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            6,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_on_a_released_attempt_is_held_for_review_not_routed(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first = facts(&pool, &order_id).await;
    let invoice = first.paykit_invoice_id.expect("invoice");
    // Paykit finalized the invoice before the activation arrived: the
    // marketplace releases the attempt.
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order_id).await.payment_method, None);
    let deadline = Utc::now() + Duration::minutes(30);
    f.paykit.set_marketplace_status(
        invoice,
        usdt_status(invoice, deadline, usdt_leg(1, true, true, false)),
    );
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("client");
    marketplace_service::paykit_attempts::verify_due_released_attempts(&f.app.state, client, at(1))
        .await
        .expect("released attempts poll");
    let held: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_all(&pool)
    .await
    .expect("held attempts");
    assert_eq!(
        held,
        vec![("needs_review".to_string(), Some("other_rail".to_string()))]
    );
    assert_eq!(
        payment_row(&pool, &order_id).await.0,
        "awaiting_entitlement"
    );
}

// ---------------------------------------------------------------------------
// Money path: concurrent polls of one order
// ---------------------------------------------------------------------------

/// Two polls of one order overlap (the first is held at the status call
/// while the second claims, which the poll interval allows once the clock
/// moved on) and apply different observations. Whatever the interleaving,
/// the order settles exactly once or goes to review exactly once, never
/// both, never twice.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_polls_settle_or_review_an_order_exactly_once(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    f.paykit.delay_marketplace_status(400);
    for round in 0..4_i64 {
        let active = active_order(&f).await;
        let order_id = active.order.order_id.clone();
        let usdt_only = usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        );
        let both = status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("USDT"),
            },
        );
        let (first, second) = if round % 2 == 0 {
            (usdt_only.clone(), both.clone())
        } else {
            (both.clone(), usdt_only.clone())
        };
        f.paykit.set_marketplace_status(active.invoice, first);
        let state = f.app.state.clone();
        let started = tokio::spawn(async move {
            let client = state
                .payments
                .as_ref()
                .and_then(|payments| payments.paykit.as_ref())
                .expect("client");
            marketplace_service::workers::verify_due_paykit_payments(
                &state,
                client,
                at(10 * (round + 1)),
            )
            .await
            .expect("first poll")
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        f.paykit.set_marketplace_status(active.invoice, second);
        let second_poll = poll_now(&f.app, at(10 * (round + 1) + 5)).await;
        let first_poll = started.await.expect("first poll joins");
        // Orders settled in earlier rounds are polled too (finality and
        // second-rail watch), so the count only proves something moved.
        assert!(
            first_poll + second_poll >= 1,
            "round {round}: the order moved ({first_poll}, {second_poll})"
        );
        let status_reads = f
            .paykit
            .calls()
            .iter()
            .filter(|call| {
                call.path == "/marketplace/payment-requests/status"
                    && call.body["invoice_id"] == json!(active.invoice)
            })
            .count();
        assert_eq!(status_reads, 2, "round {round}: both polls read the status");

        let stored = facts(&pool, &order_id).await;
        let payment = payment_row(&pool, &order_id).await;
        let receipts = count(
            &pool,
            "SELECT COUNT(*) FROM receipts WHERE order_id = $1",
            &order_id,
        )
        .await;
        let reviews = count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE aggregate_id LIKE 'payment:%' \
             AND command_id = $1 AND kind = 'payment.manual_review'",
            &order_id,
        )
        .await;
        let confirmed = count(
            &pool,
            "SELECT COUNT(*) FROM events e JOIN payments p \
             ON e.aggregate_id = 'payment:' || p.id::text \
             WHERE p.order_id = $1 AND e.kind = 'payment.confirmed'",
            &order_id,
        )
        .await;
        match payment.0.as_str() {
            "confirmed" => {
                assert_eq!(stored.state, "paid", "round {round}");
                assert_eq!(receipts, 1, "round {round}: one receipt");
                assert_eq!((confirmed, reviews), (1, 0), "round {round}");
                assert_eq!(
                    stored.payment_asset.as_deref(),
                    Some("USDT"),
                    "round {round}"
                );
                assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
            }
            "manual_review" => {
                assert_eq!(stored.state, "pending_payment", "round {round}");
                assert_eq!(receipts, 0, "round {round}: nothing settled");
                assert_eq!((confirmed, reviews), (0, 1), "round {round}: one review");
            }
            other => panic!("round {round}: payment ended {other}"),
        }
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:' || $1::text \
                 AND kind = 'payment.finality_changed'",
                &order_id
            )
            .await,
            i64::from(stored.payment_asset.as_deref() == Some("USDT")),
            "round {round}: finality recorded once, only for a USDT rail"
        );
    }
}

// ---------------------------------------------------------------------------
// PayPal is untouched
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paypal_checkout_and_ipn_settlement_are_untouched_on_the_multi_asset_deployment(
    pool: PgPool,
) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let total = bind_paypal_method(&f.app, &f.seller, &f.buyer, &order.order_id).await;
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["payment_method"], json!("paypal"));
    assert!(view["fiat_checkout_url"].as_str().is_some(), "{view}");
    for key in [
        "accepted_assets",
        "payment_amounts",
        "payment_asset",
        "payment_finality",
    ] {
        assert!(view.get(key).is_none(), "{key} on a PayPal order: {view}");
    }
    assert_eq!(
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path.starts_with("/marketplace/payment-requests"))
            .count(),
        0,
        "PayPal never reaches Paykit"
    );

    assert_eq!(
        post_completed_ipn(&f.app, &order.order_id, "TXN-PK1-1", total).await,
        StatusCode::OK
    );
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["fiat_verification"], json!("gateway-notified"));
    for key in ["accepted_assets", "payment_asset", "payment_finality"] {
        assert!(
            view.get(key).is_none(),
            "{key} on a paid PayPal order: {view}"
        );
    }
    // The worker never polls a PayPal order, and a PayPal order ships at once.
    assert_eq!(poll_now(&f.app, at(1)).await, 0);
    let stored = facts(&pool, &order.order_id).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order.order_id, stored.revision, 7),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PayPal ships without finality: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_voided_paykit_bind_leaves_nothing_on_a_later_paypal_bind(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);

    bind_paypal_method(&f.app, &f.seller, &f.buyer, &order.order_id).await;
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["payment_method"], json!("paypal"));
    for key in ["accepted_assets", "payment_amounts", "payment_finality"] {
        assert!(view.get(key).is_none(), "{key} survived the rebind: {view}");
    }
    let leftovers: (
        Option<i64>,
        Option<Vec<String>>,
        Option<Value>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT paykit_amount_minor, paykit_accepted_assets, paykit_payment_amounts, \
             paykit_asset FROM orders WHERE id = $1",
    )
    .bind(uuid(&order.order_id))
    .fetch_one(&pool)
    .await
    .expect("order row");
    assert_eq!(leftovers, (None, None, None, None));
}
