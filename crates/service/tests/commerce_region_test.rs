//! Per-region commerce modes (`REGION_MODES`): a blocked region has no new
//! buying, no payment start and no selling, decided from the buyer's declared
//! country, the request's own country from the trusted edge header and the
//! seller's shop country, while everything that completes an existing order
//! stays open. Nuanced regions keep running the C3, C3b and C4 gates, and the
//! legacy flags behave as before when `REGION_MODES` is unset.

mod common;

use axum::http::StatusCode;
use marketplace_service::config::Config;
use marketplace_service::region_policy::RegionModes;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    as_digital_checkout, as_digital_locks_listing, bind_paypal_method, checkout_command,
    checkout_command_with_id, count, create_offer_command, create_paid_order, create_pending_order,
    execute, execute_from, indexed_command_id, lock_resource_for, new_actor, offer_action,
    order_command, payment_command, place_bid_command, post_completed_ipn, prepare_locks_command,
    reconfigured, register_auction_command, register_command, send, send_from,
    spawn_fake_homeserver, sync_command, sync_drop_command, test_app_with_homeserver,
    test_app_with_locks, test_app_with_pickup_and_paypal, test_app_with_shop_homeserver,
    with_shop_homeserver, FakeHomeserver, TestActor, TestApp, OFFER_COMMAND_ID,
};

const LAUNCH: &str = "EU=blocked,GB=blocked,US=blocked";
const COMMERCE_MESSAGE: &str = "Buying and selling aren't available in your region yet.";
const UNKNOWN_MESSAGE: &str = "We can't tell where you are, so buying and selling are paused.";
const SELLING_MESSAGE: &str = "Selling isn't available in your region yet.";
const SELLER_SHOP_MESSAGE: &str = "This seller can't sell on Pubky Market right now.";

fn with_modes(app: &TestApp, modes: &str) -> TestApp {
    reconfigured(app, |config| {
        config.region_modes = Some(RegionModes::parse(modes).expect("test modes parse"));
    })
}

fn launch(app: &TestApp) -> TestApp {
    with_modes(app, LAUNCH)
}

fn shop_record(seller: &TestActor, status: Option<&str>, country: &str) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "shop",
        "ownerPubky": seller.pubky,
        "name": "Region Shop",
        "location": { "countryCode": country },
    });
    if let Some(status) = status {
        record["sellerStatus"] = json!(status);
    }
    record
}

/// A seller with a shop in `country` and one registered listing, set up while
/// the policy is off.
async fn seller_in(
    app: &TestApp,
    homeserver: &FakeHomeserver,
    country: &str,
    status: Option<&str>,
) -> TestActor {
    let seller = new_actor(app).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, status, country));
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["unit_price"]["amount_minor"] = json!(1_000);
    let (code, body) = execute(app, &seller.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "register failed: {body}");
    seller
}

fn checkout(seller: &TestActor, number: u64, country: Option<&str>) -> Value {
    let mut command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7300, number));
    if let Some(country) = country {
        command["payload"]["buyer_country"] = json!(country);
    }
    command
}

async fn revision_of(app: &TestApp, seller: &TestActor) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE seller_pubky = $1")
        .bind(&seller.pubky)
        .fetch_one(&app.pool)
        .await
        .expect("listing revision")
}

/// A checkout against the listing's current revision.
async fn buy(
    app: &TestApp,
    buyer: &TestActor,
    seller: &TestActor,
    number: u64,
    country: Option<&str>,
    ip: Option<&str>,
) -> (StatusCode, Value) {
    let mut command = checkout(seller, number, country);
    command["payload"]["lines"][0]["expected_revision"] = json!(revision_of(app, seller).await);
    execute_from(app, &buyer.token, &command, ip).await
}

/// USD and EUR reference rates, so the seller limits can convert an order.
async fn seed_fx(app: &TestApp) {
    let now: chrono::DateTime<chrono::Utc> = common::NOW.parse().expect("test instant");
    for (currency, rate) in [("USD", "100000"), ("EUR", "90000")] {
        for back in 0..3 {
            let accepted = now - chrono::Duration::seconds(60 * back);
            let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
            let bucket = chrono::DateTime::from_timestamp(bucket_ts, 0).expect("bucket");
            sqlx::query(
                "INSERT INTO fx_rate_samples \
                 (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
                 VALUES ($1, $2::numeric, $3, $3, $4, 'blocktank')",
            )
            .bind(currency)
            .bind(rate)
            .bind(accepted)
            .bind(bucket)
            .execute(&app.pool)
            .await
            .expect("seed fx sample");
        }
    }
}

async fn orders(app: &TestApp) -> i64 {
    count(&app.pool, "SELECT COUNT(*) FROM orders").await
}

#[track_caller]
fn assert_refused(
    result: &(StatusCode, Value),
    status: StatusCode,
    code: &str,
    reason: &str,
    message: &str,
) {
    let (actual, body) = result;
    assert_eq!(*actual, status, "{body}");
    assert_eq!(body["ok"], json!(false), "{body}");
    assert_eq!(body["error"]["code"], json!(code), "{body}");
    assert_eq!(body["error"]["reason"], json!(reason), "{body}");
    assert_eq!(body["error"]["message"], json!(message), "{body}");
}

#[track_caller]
fn assert_commerce_refused(result: &(StatusCode, Value), reason: &str) {
    let message = if reason == "buyer_region_unknown" {
        UNKNOWN_MESSAGE
    } else {
        COMMERCE_MESSAGE
    };
    assert_refused(
        result,
        StatusCode::CONFLICT,
        "COMMERCE_UNAVAILABLE_IN_REGION",
        reason,
        message,
    );
}

#[track_caller]
fn assert_accepted(result: &(StatusCode, Value)) {
    assert_eq!(result.0, StatusCode::OK, "{}", result.1);
    assert_eq!(result.1["ok"], json!(true), "{}", result.1);
}

// --- checkout.create ---------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_is_refused_for_a_blocked_declared_country(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    for country in ["US", "DE", "GB", "FR", "IE", "GR"] {
        let result = execute_from(
            &app,
            &buyer.token,
            &checkout(&seller, 1, Some(country)),
            Some("CH"),
        )
        .await;
        assert_commerce_refused(&result, "buyer_region_blocked");
    }
    assert_eq!(orders(&app).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_is_refused_for_a_blocked_ip_whatever_the_buyer_declares(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    for ip in ["US", "DE", "GB", "SE"] {
        let result = execute_from(
            &app,
            &buyer.token,
            &checkout(&seller, 1, Some("CH")),
            Some(ip),
        )
        .await;
        assert_commerce_refused(&result, "buyer_ip_blocked");
    }
    assert_eq!(orders(&app).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_declared_blocked_country_is_reported_before_the_ip(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    let result = execute_from(
        &app,
        &buyer.token,
        &checkout(&seller, 1, Some("US")),
        Some("DE"),
    )
    .await;
    assert_commerce_refused(&result, "buyer_region_blocked");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_from_an_unknown_origin_is_refused_while_unknown_is_blocked(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    for ip in [
        None,
        Some("XX"),
        Some("T1"),
        Some(""),
        Some("usa"),
        Some("1A"),
    ] {
        let result = execute_from(&app, &buyer.token, &checkout(&seller, 1, Some("CH")), ip).await;
        assert_commerce_refused(&result, "buyer_region_unknown");
    }
    assert_eq!(orders(&app).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unknown_origin_passes_when_unknown_is_open(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = with_modes(&setup, &format!("{LAUNCH},UNKNOWN=open"));
    let buyer = new_actor(&app).await;

    assert_accepted(
        &execute_from(&app, &buyer.token, &checkout(&seller, 1, Some("CH")), None).await,
    );
    assert_eq!(orders(&app).await, 1);

    // A known blocked IP, and a blocked declared country, are still refused.
    let fresh = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    assert_commerce_refused(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&fresh, 2, Some("CH")),
            Some("US"),
        )
        .await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &checkout(&fresh, 3, Some("DE")), None).await,
        "buyer_region_blocked",
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_open_region_is_untouched(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    for (number, (declared, ip)) in [("CH", "CH"), ("NO", "NO"), ("CA", "JP"), ("CH", "AU")]
        .into_iter()
        .enumerate()
    {
        let seller = seller_in(&setup, &homeserver, "CH", Some("trader")).await;
        let result = execute_from(
            &app,
            &buyer.token,
            &checkout(&seller, number as u64, Some(declared)),
            Some(ip),
        )
        .await;
        assert_accepted(&result);
    }
    assert_eq!(orders(&app).await, 4);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_declared_country_is_optional_while_the_ip_decides_in_a_blocked_region(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", None).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &checkout(&seller, 1, None), Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &checkout(&seller, 1, None), None).await,
        "buyer_region_unknown",
    );
    assert_eq!(orders(&app).await, 0);
    assert_accepted(
        &execute_from(&app, &buyer.token, &checkout(&seller, 1, None), Some("CH")).await,
    );
    assert_eq!(orders(&app).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_is_answered_from_its_stored_result_before_the_gate(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    let command = checkout(&seller, 1, Some("CH"));
    assert_accepted(&execute_from(&app, &buyer.token, &command, Some("CH")).await);
    let replay = execute_from(&app, &buyer.token, &command, Some("US")).await;
    assert_accepted(&replay);
    assert_eq!(orders(&app).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refusal_stores_nothing_so_the_same_command_can_be_retried_from_an_open_region(
    pool: PgPool,
) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    let command = checkout(&seller, 1, Some("CH"));
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &command, Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM command_results").await,
        1
    );
    assert_accepted(&execute_from(&app, &buyer.token, &command, Some("CH")).await);
}

// --- the other buying commands ------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_create_is_refused_in_a_blocked_region_and_passes_in_an_open_one(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    let offer = |country: Option<&str>| {
        let mut command = create_offer_command(&seller.pubky, 1);
        if let Some(country) = country {
            command["payload"]["buyer_country"] = json!(country);
        }
        command
    };

    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer(Some("DE")), Some("CH")).await,
        "buyer_region_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer(Some("CH")), Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer(Some("CH")), None).await,
        "buyer_region_unknown",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer(None), Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    assert_accepted(&execute_from(&app, &buyer.token, &offer(None), Some("CH")).await);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_checkout_is_refused_in_a_blocked_region_and_passes_in_an_open_one(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let buyer = new_actor(&setup).await;
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["payload"]["buyer_country"] = json!("CH");
    assert_accepted(&execute(&setup, &buyer.token, &offer).await);
    assert_accepted(
        &execute(
            &setup,
            &seller.token,
            &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
        )
        .await,
    );
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&setup.pool)
    .await
    .expect("accepted offer row");
    let offer_checkout = |country: &str| {
        json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000001103",
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
                    "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                    "city": "Zurich", "region": "", "postal_code": "8001", "country_code": "CH"
                },
            }
        })
    };

    let app = launch(&setup);
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer_checkout("US"), Some("CH")).await,
        "buyer_region_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer_checkout("CH"), Some("GB")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &offer_checkout("CH"), None).await,
        "buyer_region_unknown",
    );
    assert_eq!(orders(&app).await, 0);

    assert_accepted(&execute_from(&app, &buyer.token, &offer_checkout("CH"), Some("CH")).await);
    assert_eq!(orders(&app).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bid_is_refused_in_a_blocked_region_and_passes_in_an_open_one(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&setup).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "CH"));
    let (code, body) = execute(
        &setup,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let app = launch(&setup);
    let bidder = new_actor(&app).await;
    let bid = |country: Option<&str>| {
        let mut command = place_bid_command(&seller.pubky, 1, 7_000, 1);
        if let Some(country) = country {
            command["payload"]["buyer_country"] = json!(country);
        }
        command
    };

    assert_commerce_refused(
        &execute_from(&app, &bidder.token, &bid(Some("US")), Some("CH")).await,
        "buyer_region_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &bidder.token, &bid(Some("CH")), Some("DE")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &bidder.token, &bid(Some("CH")), None).await,
        "buyer_region_unknown",
    );
    assert_commerce_refused(
        &execute_from(&app, &bidder.token, &bid(None), Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 0);

    assert_accepted(&execute_from(&app, &bidder.token, &bid(None), Some("CH")).await);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 1);
}

// --- the seller's shop country on a buyer command ------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_shop_in_a_blocked_country_cannot_sell_to_a_swiss_buyer(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;
    for (number, country) in ["US", "DE", "GB"].into_iter().enumerate() {
        let seller = seller_in(&setup, &homeserver, country, Some("private")).await;
        let number = number as u64 * 10;

        let result = execute_from(
            &app,
            &buyer.token,
            &checkout(&seller, number + 1, Some("CH")),
            Some("CH"),
        )
        .await;
        assert_refused(
            &result,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            "seller_region_blocked",
            SELLER_SHOP_MESSAGE,
        );

        let mut offer = create_offer_command(&seller.pubky, 1);
        offer["payload"]["buyer_country"] = json!("CH");
        assert_refused(
            &execute_from(&app, &buyer.token, &offer, Some("CH")).await,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            "seller_region_blocked",
            SELLER_SHOP_MESSAGE,
        );
    }
    assert_eq!(orders(&app).await, 0);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_with_no_shop_country_is_not_blocked_and_an_unreadable_shop_fails_closed(
    pool: PgPool,
) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let app = launch(&setup);
    let buyer = new_actor(&app).await;

    let bare = new_actor(&setup).await;
    let (code, body) = execute(&setup, &bare.token, &register_command(&bare.pubky, 5)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&bare, 1, Some("CH")),
            Some("CH"),
        )
        .await,
    );

    let seller = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    homeserver.fail_shop_record(&seller.pubky);
    let result = execute_from(
        &app,
        &buyer.token,
        &checkout(&seller, 2, Some("CH")),
        Some("CH"),
    )
    .await;
    assert_eq!(result.0, StatusCode::SERVICE_UNAVAILABLE, "{}", result.1);
    assert_eq!(result.1["error"]["code"], json!("UPSTREAM_UNAVAILABLE"));
    homeserver.heal_shop_record(&seller.pubky);
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&seller, 2, Some("CH")),
            Some("CH"),
        )
        .await,
    );
}

// --- starting a payment ---------------------------------------------------------

struct PaypalWorld {
    setup: TestApp,
    homeserver: FakeHomeserver,
}

async fn paypal_world(pool: PgPool) -> PaypalWorld {
    let homeserver = spawn_fake_homeserver().await;
    let setup = with_shop_homeserver(test_app_with_pickup_and_paypal(pool).await, &homeserver);
    PaypalWorld { setup, homeserver }
}

impl PaypalWorld {
    /// A seller in `country` with PayPal configured and one pending order from
    /// a buyer who declared `buyer_country`, made while the policy is off.
    async fn pending(
        &self,
        seller_country: &str,
        buyer_country: &str,
    ) -> (TestActor, TestActor, String) {
        let seller = new_actor(&self.setup).await;
        let buyer = new_actor(&self.setup).await;
        self.homeserver.put_shop_record(
            &seller.pubky,
            shop_record(&seller, Some("private"), seller_country),
        );
        let (code, body) = execute(
            &self.setup,
            &seller.token,
            &register_command(&seller.pubky, 5),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let (code, body) = send(
            self.setup.router.clone(),
            "PUT",
            "/v0/sellers/me/payment-config",
            Some(&seller.token),
            &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let mut command = checkout_command(&seller.pubky);
        command["payload"]["buyer_country"] = json!(buyer_country);
        let (code, body) = execute(&self.setup, &buyer.token, &command).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let order_id = body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string();
        (seller, buyer, order_id)
    }
}

async fn bind(
    app: &TestApp,
    buyer: &TestActor,
    order_id: &str,
    ip: Option<&str>,
) -> (StatusCode, Value) {
    send_from(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
        ip,
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn binding_a_payment_method_is_refused_in_a_blocked_region(pool: PgPool) {
    let world = paypal_world(pool).await;
    let (_seller, buyer, order_id) = world.pending("CH", "CH").await;
    let app = launch(&world.setup);

    assert_commerce_refused(
        &bind(&app, &buyer, &order_id, Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &bind(&app, &buyer, &order_id, None).await,
        "buyer_region_unknown",
    );
    assert_commerce_refused(
        &bind(&app, &buyer, &order_id, Some("XX")).await,
        "buyer_region_unknown",
    );
    let bound: Option<String> =
        sqlx::query_scalar("SELECT payment_method FROM orders WHERE id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(bound, None, "nothing was bound");

    let ok = bind(&app, &buyer, &order_id, Some("CH")).await;
    assert_eq!(ok.0, StatusCode::OK, "{}", ok.1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn binding_uses_the_country_the_buyer_declared_at_checkout(pool: PgPool) {
    let world = paypal_world(pool).await;
    let (_seller, buyer, order_id) = world.pending("CH", "DE").await;
    let app = launch(&world.setup);
    assert_commerce_refused(
        &bind(&app, &buyer, &order_id, Some("CH")).await,
        "buyer_region_blocked",
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn binding_is_refused_when_the_seller_moved_to_a_blocked_country_after_checkout(
    pool: PgPool,
) {
    let world = paypal_world(pool).await;
    let (seller, buyer, order_id) = world.pending("CH", "CH").await;
    world
        .homeserver
        .put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "US"));
    let app = launch(&world.setup);
    assert_refused(
        &bind(&app, &buyer, &order_id, Some("CH")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLER_SHOP_MESSAGE,
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn re_binding_the_method_already_bound_is_not_a_payment_start(pool: PgPool) {
    let world = paypal_world(pool).await;
    let (_seller, buyer, order_id) = world.pending("CH", "CH").await;
    let first = bind(&world.setup, &buyer, &order_id, None).await;
    assert_eq!(first.0, StatusCode::OK, "{}", first.1);
    let app = launch(&world.setup);
    let again = bind(&app, &buyer, &order_id, Some("US")).await;
    assert_eq!(again.0, StatusCode::OK, "{}", again.1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn preparing_a_locks_payment_is_refused_in_a_blocked_region(pool: PgPool) {
    let (setup, _fake) = test_app_with_locks(pool).await;
    let homeserver = spawn_fake_homeserver().await;
    let setup = with_shop_homeserver(setup, &homeserver);
    let seller = new_actor(&setup).await;
    let buyer = new_actor(&setup).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "CH"));
    let order = common::create_pending_locks_order(&setup, &seller, &buyer).await;
    sqlx::query("UPDATE orders SET buyer_country = 'CH' WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&setup.pool)
        .await
        .expect("declare the buyer's country");
    let app = launch(&setup);
    let prepare = |number: u64| prepare_locks_command(&order.payment_id, number);

    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &prepare(1), Some("US")).await,
        "buyer_ip_blocked",
    );
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &prepare(2), None).await,
        "buyer_region_unknown",
    );

    sqlx::query("UPDATE orders SET buyer_country = 'DE' WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&setup.pool)
        .await
        .expect("declare a blocked country");
    assert_commerce_refused(
        &execute_from(&app, &buyer.token, &prepare(3), Some("CH")).await,
        "buyer_region_blocked",
    );
    sqlx::query("UPDATE orders SET buyer_country = 'CH' WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&setup.pool)
        .await
        .expect("declare the buyer's country");

    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "US"));
    assert_refused(
        &execute_from(&app, &buyer.token, &prepare(4), Some("CH")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLER_SHOP_MESSAGE,
    );
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "CH"));

    assert_accepted(&execute_from(&app, &buyer.token, &prepare(5), Some("CH")).await);
}

// --- selling ------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn listing_register_is_refused_for_a_blocked_shop_country_or_ip(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let app = launch(&setup);

    for country in ["US", "DE", "GB"] {
        let seller = new_actor(&app).await;
        homeserver.put_shop_record(&seller.pubky, shop_record(&seller, None, country));
        let result = execute_from(
            &app,
            &seller.token,
            &register_command(&seller.pubky, 5),
            Some("CH"),
        )
        .await;
        assert_refused(
            &result,
            StatusCode::CONFLICT,
            "SELLER_UNAVAILABLE_IN_REGION",
            "seller_region_blocked",
            SELLING_MESSAGE,
        );
    }

    let swiss = new_actor(&app).await;
    homeserver.put_shop_record(&swiss.pubky, shop_record(&swiss, None, "CH"));
    let listing = register_command(&swiss.pubky, 5);
    assert_refused(
        &execute_from(&app, &swiss.token, &listing, Some("US")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_ip_blocked",
        SELLING_MESSAGE,
    );
    assert_commerce_refused(
        &execute_from(&app, &swiss.token, &listing, None).await,
        "buyer_region_unknown",
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM listings").await, 0);

    assert_accepted(&execute_from(&app, &swiss.token, &listing, Some("CH")).await);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM listings").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_with_no_shop_record_may_register_from_an_open_region(pool: PgPool) {
    let (setup, _homeserver) = test_app_with_shop_homeserver(pool).await;
    let app = launch(&setup);
    let seller = new_actor(&app).await;
    assert_accepted(
        &execute_from(
            &app,
            &seller.token,
            &register_command(&seller.pubky, 5),
            Some("NO"),
        )
        .await,
    );
}

fn listing_record(seller: &TestActor, state: &str) -> Value {
    let mut record = json!({
        "title": "Winter boots",
        "revision": 1,
        "state": state,
        "location": { "countryCode": "CH", "region": null },
        "media": [{
            "id": "media_01",
            "type": "image",
            "mimeType": "image/jpeg",
            "contentHash": "a".repeat(64),
            "byteSize": 999_533,
        }],
        "variants": [{ "id": "variant_0", "enabled": true, "quantity": 2, "sku": null, "priceOverride": null }],
        "shippingOptions": [{
            "id": "ship_flat",
            "pricing": "flat",
            "label": "Seller shipping",
            "price": { "amountMinor": 500, "currency": "USD", "exponent": 2 },
            "estimatedMinDays": 2,
            "estimatedMaxDays": 7,
        }],
        "sale": {
            "acceptsOffers": true,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    });
    record["ownerPubky"] = json!(seller.pubky);
    record
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn listing_sync_is_refused_when_it_would_publish_a_live_listing_for_a_blocked_seller(
    pool: PgPool,
) {
    let (setup, homeserver) = test_app_with_homeserver(pool).await;
    let app = launch(&setup);
    let us_seller = new_actor(&app).await;
    let swiss_seller = new_actor(&app).await;
    let visitor = new_actor(&app).await;
    homeserver.put_shop_record(&us_seller.pubky, shop_record(&us_seller, None, "US"));
    homeserver.put_shop_record(&swiss_seller.pubky, shop_record(&swiss_seller, None, "CH"));
    homeserver.put_record(
        &us_seller.pubky,
        "boots_01",
        listing_record(&us_seller, "active"),
    );
    homeserver.put_record(
        &swiss_seller.pubky,
        "boots_01",
        listing_record(&swiss_seller, "active"),
    );

    // The seller's own sync, and a visitor healing the listing, both refuse
    // on the shop country; the visitor's IP is not the seller's.
    assert_refused(
        &execute_from(
            &app,
            &us_seller.token,
            &sync_command(&us_seller.pubky, "boots_01", 1),
            Some("CH"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLING_MESSAGE,
    );
    assert_refused(
        &execute_from(
            &app,
            &visitor.token,
            &sync_command(&us_seller.pubky, "boots_01", 2),
            Some("CH"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLER_SHOP_MESSAGE,
    );

    // A Swiss seller syncing from a US IP is refused on the IP; a visitor
    // healing the same listing from there is not the seller.
    assert_refused(
        &execute_from(
            &app,
            &swiss_seller.token,
            &sync_command(&swiss_seller.pubky, "boots_01", 3),
            Some("US"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_ip_blocked",
        SELLING_MESSAGE,
    );
    assert_commerce_refused(
        &execute_from(
            &app,
            &swiss_seller.token,
            &sync_command(&swiss_seller.pubky, "boots_01", 4),
            None,
        )
        .await,
        "buyer_region_unknown",
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM listings").await, 0);

    let healed = execute_from(
        &app,
        &visitor.token,
        &sync_command(&swiss_seller.pubky, "boots_01", 5),
        Some("US"),
    )
    .await;
    assert_accepted(&healed);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM listings").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn listing_sync_that_takes_a_listing_down_is_never_refused(pool: PgPool) {
    let (setup, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&setup).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, None, "US"));
    homeserver.put_record(&seller.pubky, "boots_01", listing_record(&seller, "active"));
    assert_accepted(
        &execute(
            &setup,
            &seller.token,
            &sync_command(&seller.pubky, "boots_01", 1),
        )
        .await,
    );
    let app = launch(&setup);

    // The seller pauses, ends or removes the listing, from a blocked region
    // and from a blocked shop country: the sync is let through.
    for (number, state) in ["paused", "ended", "removed"].into_iter().enumerate() {
        homeserver.put_record(&seller.pubky, "boots_01", listing_record(&seller, state));
        let result = execute_from(
            &app,
            &seller.token,
            &sync_command(&seller.pubky, "boots_01", 10 + number as u64),
            Some("US"),
        )
        .await;
        assert_ne!(result.0, StatusCode::CONFLICT, "{state}: {}", result.1);
        assert_accepted(&result);
    }

    // The record is gone: the sync reaches the handler, which settles the
    // deletion; it is never a region refusal.
    homeserver.delete_record(&seller.pubky, "boots_01");
    let gone = execute_from(
        &app,
        &seller.token,
        &sync_command(&seller.pubky, "boots_01", 20),
        Some("US"),
    )
    .await;
    assert_ne!(
        gone.1["error"]["code"],
        json!("SELLER_UNAVAILABLE_IN_REGION"),
        "{}",
        gone.1
    );
    assert_ne!(
        gone.1["error"]["code"],
        json!("COMMERCE_UNAVAILABLE_IN_REGION"),
        "{}",
        gone.1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn drop_sync_is_refused_for_a_blocked_seller(pool: PgPool) {
    let (setup, homeserver) = test_app_with_homeserver(pool).await;
    let app = launch(&setup);
    let us_seller = new_actor(&app).await;
    let swiss_seller = new_actor(&app).await;
    homeserver.put_shop_record(&us_seller.pubky, shop_record(&us_seller, None, "US"));
    homeserver.put_shop_record(&swiss_seller.pubky, shop_record(&swiss_seller, None, "CH"));

    assert_refused(
        &execute_from(
            &app,
            &us_seller.token,
            &sync_drop_command(&us_seller.pubky, "winter_drop", 1),
            Some("CH"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLING_MESSAGE,
    );
    assert_refused(
        &execute_from(
            &app,
            &swiss_seller.token,
            &sync_drop_command(&swiss_seller.pubky, "winter_drop", 2),
            Some("DE"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_ip_blocked",
        SELLING_MESSAGE,
    );

    // Past the gate the handler answers for itself: the double has no drop.
    let open = execute_from(
        &app,
        &swiss_seller.token,
        &sync_drop_command(&swiss_seller.pubky, "winter_drop", 3),
        Some("CH"),
    )
    .await;
    assert_eq!(open.0, StatusCode::NOT_FOUND, "{}", open.1);
}

async fn put_config(app: &TestApp, token: &str, ip: Option<&str>) -> (StatusCode, Value) {
    send_from(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
        ip,
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_payment_setup_is_refused_for_a_blocked_shop_country_or_ip(pool: PgPool) {
    let world = paypal_world(pool).await;
    let app = launch(&world.setup);
    let us_seller = new_actor(&app).await;
    world
        .homeserver
        .put_shop_record(&us_seller.pubky, shop_record(&us_seller, None, "US"));
    assert_refused(
        &put_config(&app, &us_seller.token, Some("CH")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_region_blocked",
        SELLING_MESSAGE,
    );

    let swiss = new_actor(&app).await;
    world
        .homeserver
        .put_shop_record(&swiss.pubky, shop_record(&swiss, None, "CH"));
    assert_refused(
        &put_config(&app, &swiss.token, Some("US")).await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "seller_ip_blocked",
        SELLING_MESSAGE,
    );
    assert_commerce_refused(
        &put_config(&app, &swiss.token, None).await,
        "buyer_region_unknown",
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM seller_payment_configs").await,
        0
    );

    let ok = put_config(&app, &swiss.token, Some("CH")).await;
    assert_eq!(ok.0, StatusCode::OK, "{}", ok.1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_many_checks_the_shop_country_only_and_reads_no_ip(pool: PgPool) {
    let (setup, homeserver) = test_app_with_homeserver(pool).await;
    let app = launch(&setup);
    let us_seller = new_actor(&app).await;
    let swiss_seller = new_actor(&app).await;
    homeserver.put_shop_record(&us_seller.pubky, shop_record(&us_seller, None, "US"));
    homeserver.put_shop_record(&swiss_seller.pubky, shop_record(&swiss_seller, None, "CH"));
    homeserver.put_record(
        &us_seller.pubky,
        "boots_01",
        listing_record(&us_seller, "active"),
    );
    homeserver.put_record(
        &swiss_seller.pubky,
        "boots_01",
        listing_record(&swiss_seller, "active"),
    );

    // The automation key is the Swiss seller's own; there is no browser IP.
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&swiss_seller.token),
        &json!({"listings": [
            { "seller_pubky": swiss_seller.pubky, "listing_id": "boots_01" },
            { "seller_pubky": us_seller.pubky, "listing_id": "boots_01" },
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(200), "{body}");
    assert_eq!(body["results"][1]["status"], json!(409), "{body}");
    assert_eq!(
        body["results"][1]["result"]["error"]["code"],
        json!("SELLER_UNAVAILABLE_IN_REGION")
    );
    assert_eq!(
        body["results"][1]["result"]["error"]["reason"],
        json!("seller_region_blocked")
    );
}

// --- what a blocked region can still do ------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_blocked_region_can_still_finish_an_existing_order_and_read(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;

    // Four orders made while the policy was off, every seller in a country
    // that is blocked once the launch policy is on.
    let mut sellers = Vec::new();
    let mut buyers = Vec::new();
    for _ in 0..4 {
        let seller = new_actor(&setup).await;
        homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "US"));
        sellers.push(seller);
        buyers.push(new_actor(&setup).await);
    }
    let shipped = create_paid_order(&setup, &sellers[0], &buyers[0]).await;
    let returned = create_paid_order(&setup, &sellers[1], &buyers[1]).await;
    let cancelled = create_paid_order(&setup, &sellers[2], &buyers[2]).await;
    let unpaid = create_pending_order(&setup, &sellers[3], &buyers[3]).await;
    let app = launch(&setup);

    let from_us = |token: &str, command: Value| {
        let app = &app;
        let token = token.to_string();
        async move {
            let result = execute_from(app, &token, &command, Some("US")).await;
            assert_accepted(&result);
            result
        }
    };

    // Fulfillment, delivery confirmation and a review.
    from_us(
        &sellers[0].token,
        order_command(
            "fulfillment.ship",
            &shipped.order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
            3_001,
        ),
    )
    .await;
    from_us(
        &buyers[0].token,
        order_command(
            "fulfillment.confirm_delivery",
            &shipped.order_id,
            3,
            json!({}),
            3_002,
        ),
    )
    .await;
    from_us(
        &buyers[0].token,
        order_command(
            "review.create",
            &shipped.order_id,
            4,
            json!({ "rating": 5, "text": "Arrived." }),
            3_003,
        ),
    )
    .await;

    // Return and refund.
    from_us(
        &sellers[1].token,
        order_command(
            "fulfillment.ship",
            &returned.order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-2" }),
            3_010,
        ),
    )
    .await;
    from_us(
        &buyers[1].token,
        order_command(
            "fulfillment.confirm_delivery",
            &returned.order_id,
            3,
            json!({}),
            3_011,
        ),
    )
    .await;
    from_us(
        &buyers[1].token,
        order_command(
            "return.request",
            &returned.order_id,
            4,
            json!({ "reason": "Item not as described", "requested_amount_minor": returned.total_minor }),
            3_012,
        ),
    )
    .await;
    from_us(
        &sellers[1].token,
        order_command("return.approve", &returned.order_id, 5, json!({}), 3_013),
    )
    .await;
    from_us(
        &sellers[1].token,
        order_command("return.receive", &returned.order_id, 6, json!({}), 3_014),
    )
    .await;
    from_us(
        &sellers[1].token,
        order_command(
            "refund.record_external",
            &returned.order_id,
            7,
            json!({ "amount_minor": returned.total_minor, "transaction_id": "tx-refund-1" }),
            3_015,
        ),
    )
    .await;

    // Cancellation of a paid order.
    from_us(
        &buyers[2].token,
        order_command(
            "order.cancel_request",
            &cancelled.order_id,
            2,
            json!({ "reason": "Changed mind" }),
            3_020,
        ),
    )
    .await;
    from_us(
        &sellers[2].token,
        order_command(
            "order.cancel_approve",
            &cancelled.order_id,
            3,
            json!({}),
            3_021,
        ),
    )
    .await;

    // Payment confirmation of an order already started.
    from_us(
        &buyers[3].token,
        payment_command(&unpaid.payment_id, 1, "confirmed", 1, 3_030),
    )
    .await;

    // Reads.
    for (path, token) in [
        (format!("/v1/orders/{}", shipped.order_id), &buyers[0].token),
        ("/v1/orders".to_string(), &buyers[0].token),
        (
            format!("/v1/receipts/{}", shipped.receipt_id),
            &buyers[0].token,
        ),
        (
            format!("/v1/payments/{}", shipped.payment_id),
            &buyers[0].token,
        ),
        ("/v1/notifications".to_string(), &sellers[0].token),
    ] {
        for ip in [Some("US"), None] {
            let (status, body) = send_from(
                app.router.clone(),
                "GET",
                &path,
                Some(token),
                &Value::Null,
                ip,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{path} from {ip:?}: {body}");
        }
    }
    let states: Vec<String> = sqlx::query_scalar("SELECT state FROM orders ORDER BY created_at")
        .fetch_all(&app.pool)
        .await
        .expect("order states");
    assert_eq!(states.len(), 4);
    assert!(states.contains(&"cancelled".to_string()), "{states:?}");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM orders WHERE id = $1::uuid")
            .bind(&unpaid.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("unpaid order state"),
        "paid"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paypal_ipn_confirms_an_order_from_anywhere_while_regions_are_blocked(pool: PgPool) {
    let world = paypal_world(pool).await;
    let (seller, buyer, order_id) = world.pending("US", "CH").await;
    let total = bind_paypal_method(&world.setup, &seller, &buyer, &order_id).await;
    let app = launch(&world.setup);
    let status = post_completed_ipn(&app, &order_id, "txn-region-1", total).await;
    assert_eq!(status, StatusCode::OK);
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order state");
    assert_eq!(state, "paid");
}

// --- nuanced regions are today's behaviour -------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn nuanced_regions_run_c3_without_any_legacy_flag(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    seed_fx(&setup).await;
    let trader = seller_in(&setup, &homeserver, "CH", Some("trader")).await;
    let private = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced");
    assert!(!app.state.config.eu_fence_enabled);
    let buyer = new_actor(&app).await;

    assert_refused(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 1, Some("DE")),
            Some("DE"),
        )
        .await,
        StatusCode::CONFLICT,
        "SELLER_UNAVAILABLE_IN_REGION",
        "business_seller",
        "This seller doesn't sell to buyers in the EU or UK.",
    );
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&private, 2, Some("DE")),
            Some("DE"),
        )
        .await,
    );
    // C3 reads the declared country only: a Swiss buyer from a German IP buys
    // from a business, a nuanced IP is not a block.
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 3, Some("CH")),
            Some("DE"),
        )
        .await,
    );
    // The buyer's country is still required, and no header is needed.
    let missing = execute_from(&app, &buyer.token, &checkout(&trader, 4, None), None).await;
    assert_eq!(missing.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", missing.1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn nuanced_regions_hold_digital_items_without_any_legacy_flag(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&setup).await;
    homeserver.put_shop_record(&seller.pubky, shop_record(&seller, Some("private"), "CH"));
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    as_digital_locks_listing(&mut listing);
    assert_accepted(&execute(&setup, &seller.token, &listing).await);
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced");
    assert!(!app.state.config.digital_region_block_enabled);
    let buyer = new_actor(&app).await;
    let digital = |number: u64, country: &str| {
        let mut command = as_digital_checkout(checkout_command_with_id(
            &seller.pubky,
            &indexed_command_id(0x7400, number),
        ));
        command["payload"]["buyer_country"] = json!(country);
        command
    };

    let held = execute_from(&app, &buyer.token, &digital(1, "DE"), Some("CH")).await;
    assert_refused(
        &held,
        StatusCode::CONFLICT,
        "DIGITAL_UNAVAILABLE_IN_REGION",
        "eu_buyer",
        "Digital items aren't available to buyers in the EU or UK yet.",
    );
    let held = execute_from(&app, &buyer.token, &digital(2, "CH"), Some("GB")).await;
    assert_eq!(held.1["error"]["reason"], json!("gb_ip"), "{}", held.1);
    let held = execute_from(&app, &buyer.token, &digital(3, "CH"), None).await;
    assert_eq!(
        held.1["error"]["reason"],
        json!("buyer_region_unknown"),
        "{}",
        held.1
    );
    assert_accepted(&execute_from(&app, &buyer.token, &digital(4, "CH"), Some("CH")).await);

    // Another nuanced region is held too, with a message that names no region.
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced,CA=nuanced");
    let held = execute_from(&app, &buyer.token, &digital(5, "CA"), Some("CH")).await;
    assert_refused(
        &held,
        StatusCode::CONFLICT,
        "DIGITAL_UNAVAILABLE_IN_REGION",
        "region_buyer",
        "Digital items aren't available to buyers in your region yet.",
    );

    // An open unknown origin is let through the hold.
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced,UNKNOWN=open");
    assert_accepted(&execute_from(&app, &buyer.token, &digital(6, "CH"), None).await);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn nuanced_regions_cap_an_eu_resident_seller_without_any_legacy_flag(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    seed_fx(&setup).await;
    let seller = seller_in(&setup, &homeserver, "DE", Some("private")).await;
    let outside = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced");
    let buyer = new_actor(&app).await;

    for number in 0..29u64 {
        let ordered = buy(&app, &buyer, &seller, 100 + number, Some("CH"), Some("CH")).await;
        assert_accepted(&ordered);
        let payment_id = ordered.1["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id");
        let paid = execute_from(
            &app,
            &buyer.token,
            &payment_command(payment_id, 1, "confirmed", 1, 5_000 + number),
            Some("CH"),
        )
        .await;
        assert_accepted(&paid);
    }
    let refused = buy(&app, &buyer, &seller, 200, Some("CH"), Some("CH")).await;
    assert_eq!(refused.0, StatusCode::CONFLICT, "{}", refused.1);
    assert_eq!(
        refused.1["error"]["code"],
        json!("SELLER_ANNUAL_LIMIT_REACHED")
    );

    // A seller outside the EU has no cap.
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&outside, 201, Some("CH")),
            Some("CH"),
        )
        .await,
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_nuanced_set_is_configurable_and_the_counsel_outcome_is_a_value(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let trader = seller_in(&setup, &homeserver, "CH", Some("trader")).await;
    let private = seller_in(&setup, &homeserver, "CH", Some("private")).await;
    let buyer = new_actor(&setup).await;

    // EU nuanced with Germany open: a German buyer buys from a business, a
    // French one does not, and a nuanced non-EU country gets C3 too.
    let app = with_modes(&setup, "EU=nuanced,DE=open,US=nuanced");
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 1, Some("DE")),
            Some("DE"),
        )
        .await,
    );
    assert_eq!(
        execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 2, Some("FR")),
            Some("FR")
        )
        .await
        .1["error"]["reason"],
        json!("business_seller")
    );
    assert_eq!(
        execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 3, Some("US")),
            Some("US")
        )
        .await
        .1["error"]["reason"],
        json!("business_seller")
    );

    // The outcome where counsel accepts the nuanced design for the EU and the
    // UK while the US stays blocked.
    let app = with_modes(&setup, "EU=nuanced,GB=nuanced,US=blocked");
    assert_commerce_refused(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&private, 4, Some("US")),
            Some("US"),
        )
        .await,
        "buyer_region_blocked",
    );
    assert_accepted(
        &execute_from(
            &app,
            &buyer.token,
            &checkout(&private, 5, Some("GB")),
            Some("GB"),
        )
        .await,
    );
    assert_eq!(
        execute_from(
            &app,
            &buyer.token,
            &checkout(&trader, 6, Some("GB")),
            Some("GB")
        )
        .await
        .1["error"]["reason"],
        json!("business_seller")
    );
}

// --- the setting off, and the legacy flags ---------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn region_modes_none_opens_everything_whatever_the_legacy_flags_say(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let trader = seller_in(&setup, &homeserver, "US", Some("trader")).await;
    let app = reconfigured(&setup, |config| {
        config.region_modes = Some(RegionModes::parse("none").expect("none parses"));
        config.eu_fence_enabled = true;
        config.digital_region_block_enabled = true;
    });
    let buyer = new_actor(&app).await;
    homeserver.fail_shop_record(&trader.pubky);

    // No country is needed, no header is read, no shop record is read, and a
    // business seller sells to the EU, the UK and the US.
    for (number, country) in [None, Some("DE"), Some("GB"), Some("US")]
        .into_iter()
        .enumerate()
    {
        assert_accepted(&buy(&app, &buyer, &trader, number as u64 + 1, country, None).await);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_region_modes_the_legacy_flags_never_block_and_never_read_the_ip(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "US", Some("private")).await;
    let app = reconfigured(&setup, |config| {
        config.region_modes = None;
        config.eu_fence_enabled = true;
        config.digital_region_block_enabled = true;
    });
    let buyer = new_actor(&app).await;

    // A US buyer from a US IP, and an unknown origin, buy physical goods from
    // a US seller exactly as before.
    assert_accepted(&buy(&app, &buyer, &seller, 1, Some("US"), Some("US")).await);
    assert_accepted(&buy(&app, &buyer, &seller, 2, Some("US"), None).await);

    // The fence still applies to the EU.
    let trader = seller_in(&setup, &homeserver, "CH", Some("trader")).await;
    assert_eq!(
        buy(&app, &buyer, &trader, 3, Some("DE"), Some("DE"))
            .await
            .1["error"]["reason"],
        json!("business_seller")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_no_blocked_region_no_command_reads_the_region_or_the_shop(pool: PgPool) {
    let (setup, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = seller_in(&setup, &homeserver, "US", Some("trader")).await;
    homeserver.fail_shop_record(&seller.pubky);
    let buyer = new_actor(&setup).await;
    // The default configuration: nothing is refused for any country, header
    // or shop record.
    assert_accepted(&execute_from(&setup, &buyer.token, &checkout(&seller, 1, None), None).await);
}

#[test]
fn the_default_config_has_no_region_modes() {
    let config = Config::for_tests();
    assert!(config.region_modes.is_none());
    let policy = config.region_policy();
    assert!(!policy.fence_active());
    assert!(!policy.digital_active());
    assert!(!policy.commerce_active());
}
