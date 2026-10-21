//! The EU scope fence (legal launch plan C2-C4) behind `EU_FENCE_ENABLED`.
//!
//! Off, the service accepts and stores the buyer's country and refuses
//! nothing on these grounds. On, a buyer in the EU-27 or GB buys only from a
//! declared private seller (C3), a seller resident in the EU stops at 29 paid sales
//! or EUR 2,000 of paid orders a calendar year (unpaid orders never count, and
//! the limit is checked again where the payment is taken), and a private
//! seller selling to EU buyers must re-confirm after the same numbers.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::region_fence::{self, Scope, Usage};
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command_with_id, close_auction_command, count, create_offer_command, execute,
    indexed_command_id, new_actor, offer_action, payment_command, place_bid_command,
    register_auction_command, register_command, send, spawn_fake_homeserver, test_app_with_config,
    with_shop_homeserver, FakeHomeserver, TestActor, TestApp, OFFER_COMMAND_ID,
};

fn shop_record(seller: &TestActor, status: Option<&str>, country: &str) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "shop",
        "ownerPubky": seller.pubky,
        "name": "Fence Shop",
        "location": { "countryCode": country },
    });
    if let Some(status) = status {
        record["sellerStatus"] = json!(status);
    }
    record
}

async fn app_with_fence(pool: PgPool, enabled: bool) -> (TestApp, FakeHomeserver) {
    let mut config = Config::for_tests();
    config.eu_fence_enabled = enabled;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(test_app_with_config(pool, config).await, &homeserver);
    (app, homeserver)
}

struct Seller {
    actor: TestActor,
}

impl Seller {
    async fn new(
        app: &TestApp,
        homeserver: &FakeHomeserver,
        status: Option<&str>,
        country: &str,
        quantity: i64,
    ) -> Self {
        let actor = new_actor(app).await;
        homeserver.put_shop_record(&actor.pubky, shop_record(&actor, status, country));
        let mut listing = register_command(&actor.pubky, quantity);
        listing["payload"]["unit_price"]["amount_minor"] = json!(1_000);
        let (code, body) = execute(app, &actor.token, &listing).await;
        assert_eq!(code, StatusCode::OK, "register failed: {body}");
        Self { actor }
    }

    async fn priced(
        app: &TestApp,
        homeserver: &FakeHomeserver,
        status: Option<&str>,
        country: &str,
        unit_price_minor: i64,
    ) -> Self {
        let actor = new_actor(app).await;
        homeserver.put_shop_record(&actor.pubky, shop_record(&actor, status, country));
        let mut listing = register_command(&actor.pubky, 100);
        listing["payload"]["unit_price"]["amount_minor"] = json!(unit_price_minor);
        let (code, body) = execute(app, &actor.token, &listing).await;
        assert_eq!(code, StatusCode::OK, "register failed: {body}");
        Self { actor }
    }
}

fn checkout(seller: &TestActor, number: u64, country: Option<&str>) -> Value {
    let mut command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7000, number));
    if let Some(country) = country {
        command["payload"]["buyer_country"] = json!(country);
    }
    command
}

async fn listing_revision(app: &TestApp, seller: &TestActor) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE seller_pubky = $1")
        .bind(&seller.pubky)
        .fetch_one(&app.pool)
        .await
        .expect("listing revision")
}

async fn buy(
    app: &TestApp,
    buyer: &TestActor,
    seller: &TestActor,
    number: u64,
    country: Option<&str>,
) -> (StatusCode, Value) {
    let mut command = checkout(seller, number, country);
    command["payload"]["lines"][0]["expected_revision"] =
        json!(listing_revision(app, seller).await);
    execute(app, &buyer.token, &command).await
}

#[track_caller]
fn assert_refused(
    result: &(StatusCode, Value),
    status: StatusCode,
    code: &str,
    reason: Option<&str>,
) {
    let (actual, body) = result;
    assert_eq!(*actual, status, "{body}");
    assert_eq!(body["ok"], json!(false), "{body}");
    assert_eq!(body["error"]["code"], json!(code), "{body}");
    match reason {
        Some(reason) => assert_eq!(body["error"]["reason"], json!(reason), "{body}"),
        None => assert!(body["error"].get("reason").is_none(), "{body}"),
    }
}

#[track_caller]
fn assert_accepted(result: &(StatusCode, Value)) {
    assert_eq!(result.0, StatusCode::OK, "{}", result.1);
    assert_eq!(result.1["ok"], json!(true), "{}", result.1);
}

async fn seed_fx(pool: &PgPool, currency: &str, rate: &str, now: DateTime<Utc>) {
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

async fn seed_usd_and_eur(app: &TestApp) {
    let now = app.clock.now();
    seed_fx(&app.pool, "USD", "100000", now).await;
    seed_fx(&app.pool, "EUR", "90000", now).await;
}

async fn orders_of(app: &TestApp, seller: &TestActor) -> i64 {
    let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders WHERE seller_pubky = $1")
        .bind(&seller.pubky)
        .fetch_one(&app.pool)
        .await
        .expect("order count");
    total
}

async fn buyer_country_of_orders(app: &TestApp, seller: &TestActor) -> Vec<Option<String>> {
    sqlx::query_scalar(
        "SELECT buyer_country FROM orders WHERE seller_pubky = $1 ORDER BY created_at",
    )
    .bind(&seller.pubky)
    .fetch_all(&app.pool)
    .await
    .expect("order countries")
}

/// Gives a seller `n` open (pending) orders from one buyer. Open orders never
/// count toward the annual limit.
async fn open_orders(app: &TestApp, seller: &TestActor, from: u64, n: u64, country: &str) {
    let buyer = new_actor(app).await;
    for number in from..from + n {
        let result = buy(app, &buyer, seller, number, Some(country)).await;
        assert_accepted(&result);
    }
}

fn payment_id_of(result: &(StatusCode, Value)) -> String {
    result.1["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string()
}

/// Confirms one order's payment on the sandbox, as its buyer.
async fn pay(
    app: &TestApp,
    buyer: &TestActor,
    payment_id: &str,
    number: u64,
) -> (StatusCode, Value) {
    execute(
        app,
        &buyer.token,
        &payment_command(payment_id, 1, "confirmed", 1, 10_000 + number),
    )
    .await
}

/// Gives a seller `n` paid orders from one buyer: the only orders that count.
async fn fill(app: &TestApp, seller: &TestActor, from: u64, n: u64, country: &str) {
    let buyer = new_actor(app).await;
    for number in from..from + n {
        let result = buy(app, &buyer, seller, number, Some(country)).await;
        assert_accepted(&result);
        let paid = pay(app, &buyer, &payment_id_of(&result), number).await;
        assert_eq!(paid.0, StatusCode::OK, "paying order {number}: {}", paid.1);
    }
}

async fn paid_events(app: &TestApp, seller: &TestActor) -> i64 {
    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events e JOIN payments p ON e.aggregate_id = 'payment:' || p.id::text \
         JOIN orders o ON o.id = p.order_id \
         WHERE e.kind = 'payment.confirmed' AND o.seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("paid events");
    total
}

// --- flag off ---------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_fence_off_nothing_is_refused_and_the_country_is_stored(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, false).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "DE", 50).await;
    let buyer = new_actor(&app).await;

    // No country, an EU buyer and a business seller: all accepted.
    assert_accepted(&buy(&app, &buyer, &trader.actor, 1, None).await);
    assert_accepted(&buy(&app, &buyer, &trader.actor, 2, Some("FR")).await);
    assert_eq!(
        buyer_country_of_orders(&app, &trader.actor).await,
        vec![None, Some("FR".to_string())]
    );

    // A malformed country is still a validation error.
    let bad = buy(&app, &buyer, &trader.actor, 3, Some("fr")).await;
    assert_eq!(bad.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", bad.1);

    // Past 29 paid sales for an EU-resident seller: still nothing is refused,
    // and no payment is stopped at confirmation.
    fill(&app, &trader.actor, 100, 30, "DE").await;
    assert_eq!(orders_of(&app, &trader.actor).await, 32);
    assert_eq!(paid_events(&app, &trader.actor).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_fence_off_no_shop_record_is_read_for_the_command(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, false).await;
    let seller = Seller::new(&app, &homeserver, None, "DE", 5).await;
    let buyer = new_actor(&app).await;
    homeserver.fail_shop_record(&seller.actor.pubky);
    assert_accepted(&buy(&app, &buyer, &seller.actor, 1, Some("DE")).await);
}

// --- C2: the buyer's country is required ------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_fence_on_every_selling_command_needs_the_buyer_country(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 5).await;
    let buyer = new_actor(&app).await;

    let missing = buy(&app, &buyer, &seller.actor, 1, None).await;
    assert_eq!(missing.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", missing.1);
    assert_eq!(missing.1["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        missing.1["error"]["issues"][0]["path"],
        json!("payload.buyer_country")
    );
    assert_eq!(orders_of(&app, &seller.actor).await, 0);

    let offer = create_offer_command(&seller.actor.pubky, 1);
    let missing = execute(&app, &buyer.token, &offer).await;
    assert_eq!(missing.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", missing.1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    let auction = new_actor(&app).await;
    homeserver.put_shop_record(&auction.pubky, shop_record(&auction, Some("private"), "CH"));
    let (code, body) = execute(
        &app,
        &auction.token,
        &register_auction_command(&auction.pubky),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let missing = execute(
        &app,
        &buyer.token,
        &place_bid_command(&auction.pubky, 1, 7_000, 1),
    )
    .await;
    assert_eq!(missing.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", missing.1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 0);

    assert_accepted(&buy(&app, &buyer, &seller.actor, 2, Some("CH")).await);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_is_answered_from_its_stored_result_without_the_gate(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 5).await;
    let buyer = new_actor(&app).await;
    let command = checkout(&seller.actor, 1, Some("DE"));
    assert_accepted(&execute(&app, &buyer.token, &command).await);

    homeserver.put_shop_record(
        &seller.actor.pubky,
        shop_record(&seller.actor, Some("trader"), "CH"),
    );
    let replay = execute(&app, &buyer.token, &command).await;
    assert_accepted(&replay);
    assert_eq!(orders_of(&app, &seller.actor).await, 1);

    let fresh = buy(&app, &buyer, &seller.actor, 2, Some("DE")).await;
    assert_refused(
        &fresh,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
}

// --- C3: the regional gate --------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_buyer_is_refused_by_business_and_undeclared_sellers(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 5).await;
    let undeclared = Seller::new(&app, &homeserver, None, "US", 5).await;
    let nonsense = Seller::new(&app, &homeserver, Some("Private"), "US", 5).await;
    let no_shop = new_actor(&app).await;
    let (code, body) = execute(&app, &no_shop.token, &register_command(&no_shop.pubky, 5)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let buyer = new_actor(&app).await;

    for country in ["DE", "FR", "GR", "IE", "MT"] {
        assert_refused(
            &buy(&app, &buyer, &trader.actor, 1, Some(country)).await,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            Some("business_seller"),
        );
    }
    for seller in [&undeclared.actor, &nonsense.actor, &no_shop] {
        assert_refused(
            &buy(&app, &buyer, seller, 2, Some("NL")).await,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            Some("undeclared_seller"),
        );
    }
    for seller in [&trader.actor, &undeclared.actor, &nonsense.actor, &no_shop] {
        assert_eq!(orders_of(&app, seller).await, 0);
    }
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.created'"
        )
        .await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyers_outside_the_eu_and_uk_buy_from_any_seller(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 10).await;
    let undeclared = Seller::new(&app, &homeserver, None, "US", 10).await;
    let buyer = new_actor(&app).await;
    for (number, country) in ["US", "CH", "NO", "BR", "JP", "AU"].into_iter().enumerate() {
        let number = number as u64 + 1;
        assert_accepted(&buy(&app, &buyer, &trader.actor, number, Some(country)).await);
        assert_accepted(&buy(&app, &buyer, &undeclared.actor, number + 100, Some(country)).await);
    }
    assert_eq!(buyer_country_of_orders(&app, &trader.actor).await.len(), 6);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_buyer_buys_from_a_private_seller_and_the_country_is_stored(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 5).await;
    let buyer = new_actor(&app).await;
    assert_accepted(&buy(&app, &buyer, &seller.actor, 1, Some("DE")).await);
    assert_eq!(
        buyer_country_of_orders(&app, &seller.actor).await,
        vec![Some("DE".to_string())]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_shop_record_fails_closed_for_every_buyer(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 5).await;
    let buyer = new_actor(&app).await;
    homeserver.fail_shop_record(&seller.actor.pubky);
    for (number, country) in [(1, "DE"), (2, "GB"), (4, "US")] {
        assert_refused(
            &buy(&app, &buyer, &seller.actor, number, Some(country)).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "UPSTREAM_UNAVAILABLE",
            None,
        );
    }
    assert_eq!(orders_of(&app, &seller.actor).await, 0);
    homeserver.heal_shop_record(&seller.actor.pubky);
    assert_accepted(&buy(&app, &buyer, &seller.actor, 3, Some("DE")).await);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_and_bids_from_the_eu_follow_the_same_gate(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 5).await;
    let buyer = new_actor(&app).await;

    let mut offer = create_offer_command(&trader.actor.pubky, 1);
    offer["payload"]["buyer_country"] = json!("DE");
    assert_refused(
        &execute(&app, &buyer.token, &offer).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    offer["payload"]["buyer_country"] = json!("US");
    assert_accepted(&execute(&app, &buyer.token, &offer).await);
    let stored: Option<String> = sqlx::query_scalar("SELECT buyer_country FROM offers")
        .fetch_one(&app.pool)
        .await
        .expect("offer country");
    assert_eq!(stored.as_deref(), Some("US"));

    let auction_seller = new_actor(&app).await;
    homeserver.put_shop_record(
        &auction_seller.pubky,
        shop_record(&auction_seller, Some("trader"), "US"),
    );
    let (code, body) = execute(
        &app,
        &auction_seller.token,
        &register_auction_command(&auction_seller.pubky),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let mut bid = place_bid_command(&auction_seller.pubky, 1, 7_000, 1);
    bid["payload"]["buyer_country"] = json!("FR");
    assert_refused(
        &execute(&app, &buyer.token, &bid).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 0);
    bid["payload"]["buyer_country"] = json!("BR");
    assert_accepted(&execute(&app, &buyer.token, &bid).await);
    let stored: Option<String> = sqlx::query_scalar("SELECT buyer_country FROM bids")
        .fetch_one(&app.pool)
        .await
        .expect("bid country");
    assert_eq!(stored.as_deref(), Some("BR"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_auction_order_inherits_the_winning_bids_country(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let seller = new_actor(&app).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "CH"));
    let (code, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let buyer = new_actor(&app).await;
    let runner_up = new_actor(&app).await;
    let mut bid = place_bid_command(&seller.pubky, 1, 7_000, 1);
    bid["payload"]["buyer_country"] = json!("DE");
    assert_accepted(&execute(&app, &buyer.token, &bid).await);
    let mut second = place_bid_command(&seller.pubky, 2, 6_500, 2);
    second["payload"]["buyer_country"] = json!("US");
    assert_accepted(&execute(&app, &runner_up.token, &second).await);

    app.clock.set(
        "2026-08-19T22:11:00Z"
            .parse()
            .expect("an instant after the auction ends"),
    );
    let close = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 3),
    )
    .await;
    assert_accepted(&close);
    assert_eq!(
        buyer_country_of_orders(&app, &seller).await,
        vec![Some("DE".to_string())]
    );
}

// --- C4: the annual limit ---------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_resident_seller_stops_at_29_sales_and_the_30th_is_refused(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 29, "DE").await;
    assert_eq!(orders_of(&app, &seller.actor).await, 29);

    let buyer = new_actor(&app).await;
    for country in ["DE", "US"] {
        assert_refused(
            &buy(&app, &buyer, &seller.actor, 500, Some(country)).await,
            StatusCode::CONFLICT,
            "SELLER_ANNUAL_LIMIT_REACHED",
            Some("annual_sales_limit"),
        );
    }
    assert_eq!(orders_of(&app, &seller.actor).await, 29);

    // Offers and bids on a seller at the limit are refused before they start.
    let mut offer = create_offer_command(&seller.actor.pubky, 1);
    offer["payload"]["buyer_country"] = json!("US");
    assert_refused(
        &execute(&app, &buyer.token, &offer).await,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_sales_limit"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    // A seller outside the EU is not limited.
    let swiss = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &swiss.actor, 1_000, 31, "US").await;
    assert_eq!(orders_of(&app, &swiss.actor).await, 31);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_lowercase_or_padded_shop_country_is_still_an_eu_residence(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let buyer = new_actor(&app).await;
    for (country, base) in [("de", 0), (" FR ", 1_000)] {
        let seller = Seller::new(&app, &homeserver, Some("private"), country, 100).await;
        fill(&app, &seller.actor, base + 1, 29, "US").await;
        assert_refused(
            &buy(&app, &buyer, &seller.actor, base + 500, Some("US")).await,
            StatusCode::CONFLICT,
            "SELLER_ANNUAL_LIMIT_REACHED",
            Some("annual_sales_limit"),
        );
        assert_eq!(orders_of(&app, &seller.actor).await, 29, "{country:?}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_shop_country_that_is_not_a_code_is_not_an_eu_residence(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "GERMANY", 100).await;
    fill(&app, &seller.actor, 1, 31, "US").await;
    assert_eq!(orders_of(&app, &seller.actor).await, 31);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_paid_orders_count_and_a_paid_order_that_is_later_cancelled_still_does(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "FR", 100).await;
    let buyer = new_actor(&app).await;
    let mut payments = Vec::new();
    for number in 1..=29 {
        let result = buy(&app, &buyer, &seller.actor, number, Some("FR")).await;
        assert_accepted(&result);
        payments.push(payment_id_of(&result));
    }
    // 29 open orders use no slot: a 30th and a 31st checkout are accepted.
    assert_accepted(&buy(&app, &buyer, &seller.actor, 100, Some("FR")).await);
    assert_accepted(&buy(&app, &buyer, &seller.actor, 101, Some("US")).await);

    // Ten are paid: nine slots left, so checkouts still go through.
    for (index, payment_id) in payments.iter().take(10).enumerate() {
        let paid = pay(&app, &buyer, payment_id, index as u64 + 1).await;
        assert_eq!(paid.0, StatusCode::OK, "payment failed: {}", paid.1);
    }
    assert_accepted(&buy(&app, &buyer, &seller.actor, 102, Some("FR")).await);

    // The 29th payment takes the last slot, and then no new order is taken.
    for (index, payment_id) in payments.iter().enumerate().skip(10) {
        let paid = pay(&app, &buyer, payment_id, index as u64 + 1).await;
        assert_eq!(paid.0, StatusCode::OK, "payment failed: {}", paid.1);
    }
    assert_eq!(paid_events(&app, &seller.actor).await, 29);
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 200, Some("FR")).await,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_sales_limit"),
    );

    // A sale that is cancelled or refunded afterwards keeps its slot: the
    // payment was taken.
    sqlx::query(
        "UPDATE orders SET state = 'cancelled' WHERE seller_pubky = $1 AND id IN \
         (SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
          WHERE o.seller_pubky = $1 AND p.state = 'confirmed' LIMIT 5)",
    )
    .bind(&seller.actor.pubky)
    .execute(&app.pool)
    .await
    .expect("cancel some paid orders");
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 201, Some("FR")).await,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_sales_limit"),
    );

    // The window is the calendar year: the same orders are all inside this
    // year and none of them is inside the next one.
    let now = app.clock.now();
    let mut conn = app.pool.acquire().await.expect("connection");
    let this_year = region_fence::usage(
        &mut conn,
        &seller.actor.pubky,
        region_fence::year_start(now),
        Scope::AllBuyers,
        None,
        now,
    )
    .await
    .expect("usage this year");
    assert_eq!(this_year.sales, 29);
    let next_year = now + Duration::days(200);
    let next = region_fence::usage(
        &mut conn,
        &seller.actor.pubky,
        region_fence::year_start(next_year),
        Scope::AllBuyers,
        None,
        now,
    )
    .await
    .expect("usage next year");
    assert_eq!(next, Usage::default());

    // An order can be left out of the count, so a payment is not counted
    // against itself.
    let first: uuid::Uuid = sqlx::query_scalar(
        "SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.seller_pubky = $1 AND p.state = 'confirmed' LIMIT 1",
    )
    .bind(&seller.actor.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("a paid order");
    let without = region_fence::usage(
        &mut conn,
        &seller.actor.pubky,
        region_fence::year_start(now),
        Scope::AllBuyers,
        Some(first),
        now,
    )
    .await
    .expect("usage without one order");
    assert_eq!(without.sales, 28);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_resident_seller_stops_at_2000_euro_converted_with_the_reference_rates(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    // BTCUSD 100000, BTCEUR 90000: one USD is 0.90 EUR.
    seed_usd_and_eur(&app).await;
    // 1,000.00 USD plus the 12.00 shipping is 910.80 EUR per order.
    let seller = Seller::priced(&app, &homeserver, Some("private"), "IE", 100_000).await;
    let buyer = new_actor(&app).await;
    let first = buy(&app, &buyer, &seller.actor, 1, Some("IE")).await;
    assert_accepted(&first);
    let second = buy(&app, &buyer, &seller.actor, 2, Some("US")).await;
    assert_accepted(&second);
    // Open orders carry no amount: both are accepted and neither is counted.
    let third = buy(&app, &buyer, &seller.actor, 3, Some("US")).await;
    assert_accepted(&third);
    assert_eq!(
        pay(&app, &buyer, &payment_id_of(&first), 1).await.0,
        StatusCode::OK
    );
    assert_eq!(
        pay(&app, &buyer, &payment_id_of(&second), 2).await.0,
        StatusCode::OK
    );
    // The third payment would take the paid total past 2,000 EUR: refused.
    assert_refused(
        &pay(&app, &buyer, &payment_id_of(&third), 3).await,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_amount_limit"),
    );
    // 3 x 910.80 = 2,732.40 EUR: a new order past two paid ones would pass 2,000.
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 4, Some("US")).await,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_amount_limit"),
    );
    assert_eq!(orders_of(&app, &seller.actor).await, 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_that_cannot_be_priced_in_euro_is_refused_not_waved_through(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "IE", 5).await;
    let buyer = new_actor(&app).await;
    // No FX reference samples at all.
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 1, Some("IE")).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "UPSTREAM_UNAVAILABLE",
        None,
    );
    assert_eq!(orders_of(&app, &seller.actor).await, 0);
}

// --- C3 for GB buyers (Consumer Contracts Regulations 2013) -----------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_gb_buyer_is_refused_by_business_and_undeclared_sellers_like_an_eu_buyer(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 5).await;
    let undeclared = Seller::new(&app, &homeserver, None, "US", 5).await;
    let nonsense = Seller::new(&app, &homeserver, Some("Private"), "US", 5).await;
    let no_shop = new_actor(&app).await;
    let (code, body) = execute(&app, &no_shop.token, &register_command(&no_shop.pubky, 5)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let buyer = new_actor(&app).await;

    let refused = buy(&app, &buyer, &trader.actor, 1, Some("GB")).await;
    assert_refused(
        &refused,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    assert_eq!(
        refused.1["error"]["message"],
        json!("This seller doesn't sell to buyers in the EU or UK.")
    );
    for seller in [&undeclared.actor, &nonsense.actor, &no_shop] {
        assert_refused(
            &buy(&app, &buyer, seller, 2, Some("GB")).await,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            Some("undeclared_seller"),
        );
    }
    for seller in [&trader.actor, &undeclared.actor, &nonsense.actor, &no_shop] {
        assert_eq!(orders_of(&app, seller).await, 0);
    }
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.created'"
        )
        .await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_gb_buyer_buys_from_a_private_seller_and_the_country_is_stored(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let private_ch = Seller::new(&app, &homeserver, Some("private"), "CH", 5).await;
    let private_gb = Seller::new(&app, &homeserver, Some("private"), "GB", 5).await;
    let buyer = new_actor(&app).await;
    assert_accepted(&buy(&app, &buyer, &private_ch.actor, 1, Some("GB")).await);
    assert_accepted(&buy(&app, &buyer, &private_gb.actor, 2, Some("GB")).await);
    assert_eq!(
        buyer_country_of_orders(&app, &private_ch.actor).await,
        vec![Some("GB".to_string())]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_gb_buyer_is_open_to_everyone_with_the_fence_off(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, false).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 5).await;
    let buyer = new_actor(&app).await;
    assert_accepted(&buy(&app, &buyer, &trader.actor, 1, Some("GB")).await);
    assert_eq!(
        buyer_country_of_orders(&app, &trader.actor).await,
        vec![Some("GB".to_string())]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_and_bids_from_gb_follow_the_same_gate(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let trader = Seller::new(&app, &homeserver, Some("trader"), "US", 5).await;
    let buyer = new_actor(&app).await;

    let mut offer = create_offer_command(&trader.actor.pubky, 1);
    offer["payload"]["buyer_country"] = json!("GB");
    assert_refused(
        &execute(&app, &buyer.token, &offer).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    let auction_seller = new_actor(&app).await;
    homeserver.put_shop_record(
        &auction_seller.pubky,
        shop_record(&auction_seller, Some("trader"), "US"),
    );
    let (code, body) = execute(
        &app,
        &auction_seller.token,
        &register_auction_command(&auction_seller.pubky),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let mut bid = place_bid_command(&auction_seller.pubky, 1, 7_000, 1);
    bid["payload"]["buyer_country"] = json!("GB");
    assert_refused(
        &execute(&app, &buyer.token, &bid).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_checkout_by_a_gb_buyer_is_refused_for_a_business_seller(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("trader"), "US", 100).await;
    let buyer = new_actor(&app).await;
    // The offer was made from a country the fence leaves open.
    let mut offer = create_offer_command(&seller.actor.pubky, 1);
    offer["payload"]["buyer_country"] = json!("US");
    assert_accepted(&execute(&app, &buyer.token, &offer).await);
    assert_accepted(
        &execute(
            &app,
            &seller.actor.token,
            &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001201"),
        )
        .await,
    );
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    let command = |country: &str, id: &str| {
        json!({
            "version": 1,
            "command_id": id,
            "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
            "expected_revision": 2,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.checkout",
            "payload": {
                "offer_id": OFFER_COMMAND_ID,
                "award_id": row.0.to_string(),
                "listing_aggregate_id": row.1,
                "listing_revision": row.2,
                "listing_record_sha256": row.3,
                "variant_id": row.4,
                "quantity": 1,
                "buyer_country": country,
                "delivery_address": {
                    "name": "Alice Buyer",
                    "line1": "1 Market Street",
                    "line2": "",
                    "city": "London",
                    "region": "",
                    "postal_code": "N1 9GU",
                    "country_code": "GB"
                },
            }
        })
    };
    assert_refused(
        &execute(
            &app,
            &buyer.token,
            &command("GB", "00000000-0000-4000-8000-000000001202"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("business_seller"),
    );
    assert_eq!(orders_of(&app, &seller.actor).await, 0);
    assert_accepted(
        &execute(
            &app,
            &buyer.token,
            &command("US", "00000000-0000-4000-8000-000000001203"),
        )
        .await,
    );
    assert_eq!(
        buyer_country_of_orders(&app, &seller.actor).await,
        vec![Some("US".to_string())]
    );
}

// --- C4: the private seller's re-confirmation -------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_private_seller_must_reconfirm_after_29_sales_to_eu_buyers(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    // Sales to buyers outside the EU never count toward the re-confirmation.
    fill(&app, &seller.actor, 1, 40, "US").await;
    fill(&app, &seller.actor, 100, 29, "DE").await;

    let buyer = new_actor(&app).await;
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 500, Some("DE")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("private_seller_reconfirmation_required"),
    );
    assert_accepted(&buy(&app, &buyer, &seller.actor, 501, Some("US")).await);

    // The seller re-confirms; the count restarts at the confirmation.
    app.clock.advance_seconds(3_600);
    seed_usd_and_eur(&app).await;
    let mut record = shop_record(&seller.actor, Some("private"), "CH");
    record["sellerStatusConfirmedAt"] = json!(app.clock.now().to_rfc3339());
    homeserver.put_shop_record(&seller.actor.pubky, record);
    app.clock.advance_seconds(60);
    assert_accepted(&buy(&app, &buyer, &seller.actor, 502, Some("DE")).await);

    // A confirmation dated in the future counts as none.
    let mut record = shop_record(&seller.actor, Some("private"), "CH");
    record["sellerStatusConfirmedAt"] = json!((app.clock.now() + Duration::days(1)).to_rfc3339());
    homeserver.put_shop_record(&seller.actor.pubky, record);
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 700, Some("DE")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("private_seller_reconfirmation_required"),
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sales_to_gb_buyers_never_count_toward_the_reconfirmation_point(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 200).await;
    // DAC7 is an EU directive: 40 paid sales to GB buyers are not EU sales.
    fill(&app, &seller.actor, 1, 40, "GB").await;
    let buyer = new_actor(&app).await;
    assert_accepted(&buy(&app, &buyer, &seller.actor, 500, Some("DE")).await);
    assert_accepted(&buy(&app, &buyer, &seller.actor, 501, Some("GB")).await);

    let (code, read) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{read}");
    assert_eq!(read["eu_buyer_sales"], json!(0));
    assert_eq!(read["reconfirmation_required"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_gb_buyer_is_not_stopped_by_a_private_sellers_reconfirmation_point(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &seller.actor, 100, 29, "DE").await;
    let buyer = new_actor(&app).await;
    assert_refused(
        &buy(&app, &buyer, &seller.actor, 500, Some("DE")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("private_seller_reconfirmation_required"),
    );
    assert_accepted(&buy(&app, &buyer, &seller.actor, 501, Some("GB")).await);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_eu_resident_sellers_annual_limit_still_counts_sales_to_gb_buyers(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 29, "GB").await;
    let buyer = new_actor(&app).await;
    for country in ["GB", "US"] {
        assert_refused(
            &buy(&app, &buyer, &seller.actor, 500, Some(country)).await,
            StatusCode::CONFLICT,
            "SELLER_ANNUAL_LIMIT_REACHED",
            Some("annual_sales_limit"),
        );
    }
}

// --- offer.checkout is counted in its own transaction -----------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_checkout_past_the_limit_is_refused_and_keeps_its_hold(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    app.clock.set(
        DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    let buyer = new_actor(&app).await;
    let mut offer = create_offer_command(&seller.actor.pubky, 1);
    offer["payload"]["buyer_country"] = json!("DE");
    assert_accepted(&execute(&app, &buyer.token, &offer).await);
    assert_accepted(
        &execute(
            &app,
            &seller.actor.token,
            &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
        )
        .await,
    );
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    // 29 other paid orders already count. They are on a second listing, so
    // the offered listing stays on the revision the offer was accepted on.
    let history_aggregate = format!("listing:{}_history_01", seller.actor.pubky);
    let mut history = common::register_listing_command(&seller.actor.pubky, "history_01", 100, 1);
    history["payload"]["unit_price"]["amount_minor"] = json!(1_000);
    assert_accepted(&execute(&app, &seller.actor.token, &history).await);
    let other = new_actor(&app).await;
    for number in 1..=29u64 {
        let mut order = checkout(&seller.actor, number, Some("DE"));
        order["payload"]["lines"][0]["listing_aggregate_id"] = json!(history_aggregate);
        let revision: i64 =
            sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(&history_aggregate)
                .fetch_one(&app.pool)
                .await
                .expect("history listing revision");
        order["payload"]["lines"][0]["expected_revision"] = json!(revision);
        let placed = execute(&app, &other.token, &order).await;
        assert_accepted(&placed);
        let paid = pay(&app, &other, &payment_id_of(&placed), number).await;
        assert_eq!(paid.0, StatusCode::OK, "paying order {number}: {}", paid.1);
    }
    assert_eq!(paid_events(&app, &seller.actor).await, 29);
    let command = |country: Option<&str>, id: &str| {
        let mut command = json!({
            "version": 1,
            "command_id": id,
            "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
            "expected_revision": 2,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.checkout",
            "payload": {
                "offer_id": OFFER_COMMAND_ID,
                "award_id": row.0.to_string(),
                "listing_aggregate_id": row.1,
                "listing_revision": row.2,
                "listing_record_sha256": row.3,
                "variant_id": row.4,
                "quantity": 1,
                "delivery_address": {
                    "name": "Alice Buyer",
                    "line1": "1 Market Street",
                    "line2": "",
                    "city": "Berlin",
                    "region": "",
                    "postal_code": "10115",
                    "country_code": "DE"
                },
            }
        });
        if let Some(country) = country {
            command["payload"]["buyer_country"] = json!(country);
        }
        command
    };
    let missing = execute(
        &app,
        &buyer.token,
        &command(None, "00000000-0000-4000-8000-000000001103"),
    )
    .await;
    assert_eq!(missing.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", missing.1);

    let refused = execute(
        &app,
        &buyer.token,
        &command(Some("DE"), "00000000-0000-4000-8000-000000001104"),
    )
    .await;
    assert_refused(
        &refused,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_sales_limit"),
    );
    let state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .fetch_one(&app.pool)
        .await
        .expect("offer state");
    assert_eq!(state, "accepted");
    let reservation: String =
        sqlx::query_scalar("SELECT status FROM reservations WHERE offer_award_id = $1")
            .bind(row.0)
            .fetch_one(&app.pool)
            .await
            .expect("reservation");
    assert_eq!(reservation, "active");

    // The seller moves out of the EU and re-confirms their status: no
    // residence, no limit, and the same buyer checks the offer out.
    app.clock.advance_seconds(5);
    let mut record = shop_record(&seller.actor, Some("private"), "CH");
    record["sellerStatusConfirmedAt"] = json!(app.clock.now().to_rfc3339());
    homeserver.put_shop_record(&seller.actor.pubky, record);
    app.clock.advance_seconds(5);
    let accepted = execute(
        &app,
        &buyer.token,
        &command(Some("DE"), "00000000-0000-4000-8000-000000001105"),
    )
    .await;
    assert_accepted(&accepted);
    let countries: Vec<Option<String>> =
        sqlx::query_scalar("SELECT buyer_country FROM orders WHERE buyer_pubky = $1")
            .bind(&buyer.pubky)
            .fetch_all(&app.pool)
            .await
            .expect("offer order country");
    assert_eq!(countries, vec![Some("DE".to_string())]);
}

// --- interleaving -----------------------------------------------------------

async fn concurrent_buys(
    app: &TestApp,
    seller: &TestActor,
    buyers: &[TestActor],
    first_number: u64,
    country: &str,
) -> Vec<(StatusCode, Value)> {
    let revision = listing_revision(app, seller).await;
    let mut tasks = tokio::task::JoinSet::new();
    for (index, buyer) in buyers.iter().enumerate() {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let mut command = checkout(seller, first_number + index as u64, Some(country));
        command["payload"]["lines"][0]["expected_revision"] = json!(revision);
        tasks.spawn(
            async move { send(router, "POST", "/v1/commands", Some(&token), &command).await },
        );
    }
    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        results.push(joined.expect("checkout task"));
    }
    results
}

async fn buyers(app: &TestApp, n: usize) -> Vec<TestActor> {
    // Sessions are issued before anything runs: `authenticate` moves the
    // shared test clock while it works.
    let mut buyers = Vec::new();
    for _ in 0..n {
        buyers.push(new_actor(app).await);
    }
    buyers
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_checkouts_all_succeed_because_open_orders_use_no_slot(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 28, "DE").await;
    let buyers = buyers(&app, 8).await;

    let results = concurrent_buys(&app, &seller.actor, &buyers, 1_000, "DE").await;
    for result in &results {
        assert_accepted(result);
    }
    assert_eq!(orders_of(&app, &seller.actor).await, 36);
    assert_eq!(paid_events(&app, &seller.actor).await, 28);
}

/// Several unpaid orders, each from its own buyer, then all paid at once.
async fn open_orders_from_distinct_buyers(
    app: &TestApp,
    seller: &TestActor,
    first_number: u64,
    n: usize,
    country: &str,
) -> Vec<(TestActor, String)> {
    let mut orders = Vec::new();
    for (index, buyer) in buyers(app, n).await.into_iter().enumerate() {
        let result = buy(
            app,
            &buyer,
            seller,
            first_number + index as u64,
            Some(country),
        )
        .await;
        assert_accepted(&result);
        let payment_id = payment_id_of(&result);
        orders.push((buyer, payment_id));
    }
    orders
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_payments_cannot_both_take_the_last_slot(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 28, "DE").await;
    let orders = open_orders_from_distinct_buyers(&app, &seller.actor, 1_000, 8, "DE").await;

    let mut tasks = tokio::task::JoinSet::new();
    for (index, (buyer, payment_id)) in orders.into_iter().enumerate() {
        let router = app.router.clone();
        let command = payment_command(&payment_id, 1, "confirmed", 1, 50_000 + index as u64);
        tasks.spawn(async move {
            send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await
        });
    }
    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        results.push(joined.expect("payment task"));
    }
    let accepted = results
        .iter()
        .filter(|(code, _)| *code == StatusCode::OK)
        .count();
    assert_eq!(
        accepted, 1,
        "exactly one payment takes the 29th slot: {results:?}"
    );
    for (code, body) in results.iter().filter(|(code, _)| *code != StatusCode::OK) {
        assert_eq!(*code, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], json!("SELLER_ANNUAL_LIMIT_REACHED"));
    }
    assert_eq!(paid_events(&app, &seller.actor).await, 29);
    // The seven refused payments took nothing: no receipt, order still open.
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE state = 'pending_payment'"
        )
        .await,
        7
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_payments_cannot_overshoot_the_private_sellers_reconfirmation(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &seller.actor, 1, 25, "DE").await;
    let orders = open_orders_from_distinct_buyers(&app, &seller.actor, 1_000, 8, "DE").await;

    let mut tasks = tokio::task::JoinSet::new();
    for (index, (buyer, payment_id)) in orders.into_iter().enumerate() {
        let router = app.router.clone();
        let command = payment_command(&payment_id, 1, "confirmed", 1, 60_000 + index as u64);
        tasks.spawn(async move {
            send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await
        });
    }
    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        results.push(joined.expect("payment task"));
    }
    let accepted = results
        .iter()
        .filter(|(code, _)| *code == StatusCode::OK)
        .count();
    assert_eq!(accepted, 4, "25 + 4 reaches 29 and no more: {results:?}");
    for (code, body) in results.iter().filter(|(code, _)| *code != StatusCode::OK) {
        assert_eq!(*code, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], json!("SELLER_UNAVAILABLE_IN_REGION"));
        assert_eq!(
            body["error"]["reason"],
            json!("private_seller_reconfirmation_required")
        );
    }
    assert_eq!(paid_events(&app, &seller.actor).await, 29);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_payment_after_the_reconfirmation_point_is_refused_until_the_seller_reconfirms(
    pool: PgPool,
) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &seller.actor, 1, 28, "DE").await;
    let orders = open_orders_from_distinct_buyers(&app, &seller.actor, 1_000, 3, "DE").await;
    let (first_buyer, first) = &orders[0];
    let (second_buyer, second) = &orders[1];
    let (third_buyer, third) = &orders[2];

    assert_eq!(
        pay(&app, first_buyer, first, 91_001).await.0,
        StatusCode::OK
    );
    assert_refused(
        &pay(&app, second_buyer, second, 91_002).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("private_seller_reconfirmation_required"),
    );
    assert_eq!(paid_events(&app, &seller.actor).await, 29);

    // The seller re-confirms; a payment that starts afterwards counts from
    // the new date and is taken.
    app.clock.advance_seconds(60);
    let mut record = shop_record(&seller.actor, Some("private"), "CH");
    record["sellerStatusConfirmedAt"] = json!(app.clock.now().to_rfc3339());
    homeserver.put_shop_record(&seller.actor.pubky, record);
    app.clock.advance_seconds(60);
    assert_eq!(
        pay(&app, third_buyer, third, 91_003).await.0,
        StatusCode::OK
    );
    assert_eq!(
        pay(&app, second_buyer, second, 91_004).await.0,
        StatusCode::OK
    );
    assert_eq!(paid_events(&app, &seller.actor).await, 31);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_payment_after_the_last_slot_is_refused_and_takes_nothing(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 28, "DE").await;
    let orders = open_orders_from_distinct_buyers(&app, &seller.actor, 1_000, 2, "DE").await;
    let (first_buyer, first) = &orders[0];
    let (second_buyer, second) = &orders[1];

    assert_eq!(
        pay(&app, first_buyer, first, 90_001).await.0,
        StatusCode::OK
    );
    assert_eq!(paid_events(&app, &seller.actor).await, 29);

    let refused = pay(&app, second_buyer, second, 90_002).await;
    assert_refused(
        &refused,
        StatusCode::CONFLICT,
        "SELLER_ANNUAL_LIMIT_REACHED",
        Some("annual_sales_limit"),
    );
    assert_eq!(paid_events(&app, &seller.actor).await, 29);
    let (payment_state, order_state): (String, String) = sqlx::query_as(
        "SELECT p.state, o.state FROM payments p JOIN orders o ON o.id = p.order_id WHERE p.id = $1",
    )
    .bind(uuid::Uuid::parse_str(second).expect("payment uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("second payment");
    assert_eq!(
        (payment_state.as_str(), order_state.as_str()),
        ("awaiting_entitlement", "pending_payment")
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM receipts").await,
        29,
        "no receipt for the refused payment"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_fence_off_the_30th_payment_is_taken(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, false).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    fill(&app, &seller.actor, 1, 29, "DE").await;
    let orders = open_orders_from_distinct_buyers(&app, &seller.actor, 1_000, 1, "DE").await;
    let (buyer, payment_id) = &orders[0];
    assert_eq!(pay(&app, buyer, payment_id, 90_001).await.0, StatusCode::OK);
    assert_eq!(paid_events(&app, &seller.actor).await, 30);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_with_no_usable_country_has_no_cap_at_checkout_or_payment(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    // An empty country, a country that is not a code, and no location at all.
    let empty = Seller::new(&app, &homeserver, Some("private"), "", 100).await;
    let unknown = Seller::new(&app, &homeserver, Some("private"), "XX1", 100).await;
    let actor = new_actor(&app).await;
    let mut record = shop_record(&actor, Some("private"), "DE");
    record.as_object_mut().expect("record").remove("location");
    homeserver.put_shop_record(&actor.pubky, record);
    let listing = register_command(&actor.pubky, 100);
    let (code, body) = execute(&app, &actor.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "register failed: {body}");

    for (index, seller) in [&empty.actor, &unknown.actor, &actor]
        .into_iter()
        .enumerate()
    {
        let base = 1 + index as u64 * 1_000;
        fill(&app, seller, base, 31, "US").await;
        let again = buy(&app, &new_actor(&app).await, seller, base + 500, Some("US")).await;
        assert_accepted(&again);
        assert_eq!(paid_events(&app, seller).await, 31, "seller {index}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unpaid_orders_do_not_move_a_private_seller_toward_reconfirmation(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &seller.actor, 1, 25, "DE").await;
    let buyers = buyers(&app, 8).await;
    let results = concurrent_buys(&app, &seller.actor, &buyers, 1_000, "DE").await;
    for result in &results {
        assert_accepted(result);
    }
    assert_eq!(orders_of(&app, &seller.actor).await, 33);
    // The re-confirmation point counts paid sales to EU buyers: four more
    // payments reach 29, and the next order waits for the seller to
    // re-confirm.
    fill(&app, &seller.actor, 2_000, 4, "DE").await;
    assert_refused(
        &buy(
            &app,
            &new_actor(&app).await,
            &seller.actor,
            3_000,
            Some("DE"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        Some("private_seller_reconfirmation_required"),
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn thirty_open_orders_leave_the_limit_untouched_and_the_counters_at_zero(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    open_orders(&app, &seller.actor, 1, 30, "DE").await;
    let (code, own) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{own}");
    assert_eq!(own["sales"], json!(0));
    assert_eq!(own["eur_cents"], json!(0));
    assert_eq!(own["limit_reached"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkouts_over_the_same_sellers_in_opposite_order_do_not_deadlock(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let first = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    let second = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    let two_lines = |number: u64, one: &TestActor, other: &TestActor| {
        let mut command = checkout(one, number, Some("DE"));
        command["payload"]["lines"] = json!([
            { "listing_aggregate_id": common::listing_aggregate(&one.pubky),
              "expected_revision": 1, "quantity": 1 },
            { "listing_aggregate_id": common::listing_aggregate(&other.pubky),
              "expected_revision": 1, "quantity": 1 },
        ]);
        command
    };
    // Sessions are issued before anything runs: `authenticate` moves the
    // shared test clock while it works.
    let mut buyers = Vec::new();
    for _ in 0..12 {
        buyers.push(new_actor(&app).await);
    }
    let mut tasks = tokio::task::JoinSet::new();
    for round in 0..6u64 {
        for (offset, (one, other)) in [(&first.actor, &second.actor), (&second.actor, &first.actor)]
            .into_iter()
            .enumerate()
        {
            let buyer = buyers.remove(0);
            let router = app.router.clone();
            let command = two_lines(round * 2 + offset as u64 + 1, one, other);
            tasks.spawn(async move {
                send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await
            });
        }
    }
    let mut accepted = 0;
    while let Some(joined) = tasks.join_next().await {
        let (code, body) = joined.expect("checkout task");
        assert_eq!(
            code,
            StatusCode::OK,
            "no deadlock or refusal expected: {body}"
        );
        accepted += 1;
    }
    assert_eq!(accepted, 12);
    assert_eq!(orders_of(&app, &first.actor).await, 12);
    assert_eq!(orders_of(&app, &second.actor).await, 12);
}

// --- the seller's own counters ----------------------------------------------

async fn annual_limit(app: &TestApp, token: &str, seller: &TestActor) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/sellers/{}/annual-limit", seller.pubky),
        Some(token),
        &json!(null),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_annual_limit_read_is_off_with_the_fence_off(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, false).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 5).await;
    let (code, body) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body, json!({ "enabled": false }));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_annual_limit_read_shows_counters_to_the_seller_and_a_flag_to_everyone(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 100).await;
    let buyer = new_actor(&app).await;
    fill(&app, &seller.actor, 1, 3, "US").await;

    let (code, own) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{own}");
    assert_eq!(own["enabled"], json!(true));
    assert_eq!(own["sales"], json!(3));
    assert_eq!(own["sales_limit"], json!(29));
    assert_eq!(own["eur_limit_cents"], json!(200_000));
    assert_eq!(own["resident_in_eu"], json!(true));
    assert_eq!(own["limit_reached"], json!(false));
    assert_eq!(own["reconfirmation_required"], json!(false));
    // Three 22.00 USD orders (10.00 plus 12.00 shipping) at 0.90 EUR per USD.
    assert_eq!(own["eur_cents"], json!(3 * 1_980));

    let (code, other) = annual_limit(&app, &buyer.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{other}");
    assert_eq!(
        other,
        json!({ "enabled": true, "year": 2026, "limit_reached": false })
    );

    fill(&app, &seller.actor, 100, 26, "US").await;
    let (_, other) = annual_limit(&app, &buyer.token, &seller.actor).await;
    assert_eq!(other["limit_reached"], json!(true));
    let (_, own) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(own["sales"], json!(29));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_annual_limit_read_tells_a_private_seller_when_to_reconfirm(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    seed_usd_and_eur(&app).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "CH", 100).await;
    fill(&app, &seller.actor, 1, 29, "FR").await;
    let (code, own) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::OK, "{own}");
    assert_eq!(own["resident_in_eu"], json!(false));
    assert_eq!(own["eu_buyer_sales"], json!(29));
    assert_eq!(own["reconfirmation_required"], json!(true));
    assert_eq!(own["limit_reached"], json!(false));
    assert!(own.get("sales").is_none());

    let buyer = new_actor(&app).await;
    let (_, other) = annual_limit(&app, &buyer.token, &seller.actor).await;
    assert!(other.get("reconfirmation_required").is_none(), "{other}");
    assert!(other.get("eu_buyer_sales").is_none(), "{other}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_annual_limit_read_fails_closed_without_the_shop_record(pool: PgPool) {
    let (app, homeserver) = app_with_fence(pool, true).await;
    let seller = Seller::new(&app, &homeserver, Some("private"), "DE", 5).await;
    homeserver.fail_shop_record(&seller.actor.pubky);
    let (code, body) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (code, body) = annual_limit(&app, &seller.actor.token, &seller.actor).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}
