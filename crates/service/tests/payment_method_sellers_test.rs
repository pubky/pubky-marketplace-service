//! `GET /v0/payment-methods/{method}/sellers`: the seller set behind the
//! marketplace's payment-method filter. Stored configuration only, never
//! Paykit readiness; seller keys only; only sellers with a live storefront.

mod common;

use axum::http::StatusCode;
use common::*;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::{MarketplaceAssets, MarketplaceContract};
use marketplace_service::payments::PaykitApi;
use serde_json::{json, Value};
use sqlx::PgPool;

async fn sellers(app: &TestApp, method: &str, query: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v0/payment-methods/{method}/sellers{query}"),
        None,
        &Value::Null,
    )
    .await
}

async fn configure(app: &TestApp, seller: &TestActor, body: Value) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment config: {body}");
}

async fn register_listing(app: &TestApp, seller: &TestActor, listing_id: &str) {
    let (status, body) = execute(
        app,
        &seller.token,
        &register_listing_command(&seller.pubky, listing_id, 2, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "listing registration: {body}");
}

/// A seller with a live listing and the given rails.
async fn seller_with(app: &TestApp, body: Value) -> TestActor {
    let seller = new_actor(app).await;
    configure(app, &seller, body).await;
    register_listing(app, &seller, "boots_01").await;
    seller
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

fn keys(body: &Value) -> Vec<String> {
    body["sellers"]
        .as_array()
        .expect("sellers is an array")
        .iter()
        .map(|key| key.as_str().expect("seller key is a string").to_string())
        .collect()
}

fn usdt_config() -> Config {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    config.paykit_marketplace_assets =
        MarketplaceAssets::parse(Some("BTC,USDT"), PaykitApi::Upstream).expect("assets parse");
    config.paykit_marketplace_contract = MarketplaceContract::MultiAsset;
    config
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn lists_sellers_by_their_stored_rails(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    let both = seller_with(
        &app,
        json!({ "bitcoin_enabled": true, "paypal_merchant_email": "both@example.com" }),
    )
    .await;
    let bitcoin_only = seller_with(&app, json!({ "bitcoin_enabled": true })).await;
    let paypal_only = seller_with(
        &app,
        json!({ "bitcoin_enabled": false, "paypal_merchant_email": "paypal@example.com" }),
    )
    .await;
    seller_with(&app, json!({ "bitcoin_enabled": false })).await;

    let (status, body) = sellers(&app, "bitcoin", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["method"], json!("bitcoin"));
    assert_eq!(
        keys(&body),
        sorted(vec![both.pubky.clone(), bitcoin_only.pubky])
    );
    assert_eq!(body["next_after"], Value::Null);

    let (status, body) = sellers(&app, "paypal", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), sorted(vec![both.pubky, paypal_only.pubky]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn answers_from_stored_config_whatever_paykit_says(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    // A claimed seller, then paykit-server stops answering past the stale
    // window: the public config fails closed to no Bitcoin, while the
    // filter still lists the seller from the stored config.
    let seller = seller_with(&app, json!({ "bitcoin_enabled": true })).await;
    paykit.set_claimed(&seller.pubky);
    paykit.fail_rail_health();
    paykit.fail_account_exists();
    paykit.fail_setup_status_with(503);
    app.clock.advance_seconds(120);

    let (status, body) = sellers(&app, "bitcoin", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), vec![seller.pubky.clone()]);
    let (_, config) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(config["bitcoin_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn names_only_sellers_with_a_live_storefront(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool.clone()).await;
    let no_listing = new_actor(&app).await;
    configure(&app, &no_listing, json!({ "bitcoin_enabled": true })).await;
    let sold_out = seller_with(&app, json!({ "bitcoin_enabled": true })).await;
    sqlx::query(
        "UPDATE listings SET state = 'sold', available_quantity = 0, \
         sold_quantity = total_quantity WHERE seller_pubky = $1",
    )
    .bind(&sold_out.pubky)
    .execute(&pool)
    .await
    .expect("sell out the listing");
    let live = seller_with(&app, json!({ "bitcoin_enabled": true })).await;

    let (status, body) = sellers(&app, "bitcoin", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), vec![live.pubky]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_whose_only_listing_was_deleted_is_not_listed(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        json!({
            "title": "Winter boots",
            "revision": 1,
            "location": { "countryCode": "US", "region": null },
            "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
            "variants": [{
                "id": "variant_1",
                "enabled": true,
                "quantity": 3,
                "sku": null,
                "options": [],
            }],
            "shippingOptions": [{ "pricing": "free" }],
            "fulfillmentMethods": ["shipping"],
            "sale": {
                "acceptsOffers": true,
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
            },
        }),
    );
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "registration: {body}");
    // This app has no payments runtime, so the stored row is written directly.
    sqlx::query(
        "INSERT INTO seller_payment_configs \
             (seller_pubky, bitcoin_enabled, paypal_merchant_email, created_at, updated_at) \
         VALUES ($1, FALSE, 'deleted@example.com', NOW(), NOW())",
    )
    .bind(&seller.pubky)
    .execute(&pool)
    .await
    .expect("seed the PayPal config");
    let (_, body) = sellers(&app, "paypal", "").await;
    assert_eq!(keys(&body), vec![seller.pubky.clone()]);

    homeserver.delete_record(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync after delete: {body}");
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));

    let (status, body) = sellers(&app, "paypal", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), Vec::<String>::new());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_body_carries_seller_keys_and_nothing_else(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    seller_with(
        &app,
        json!({ "bitcoin_enabled": true, "paypal_merchant_email": "private@example.com" }),
    )
    .await;

    let (status, body) = sellers(&app, "paypal", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let object = body.as_object().expect("an object");
    assert_eq!(
        sorted(object.keys().cloned().collect()),
        vec!["method", "next_after", "sellers"]
    );
    assert!(!body.to_string().contains("private@example.com"), "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn pages_by_seller_key(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    let mut expected = Vec::new();
    for _ in 0..3 {
        expected.push(
            seller_with(&app, json!({ "bitcoin_enabled": true }))
                .await
                .pubky,
        );
    }
    expected.sort();

    let (status, first) = sellers(&app, "bitcoin", "?limit=2").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(keys(&first), expected[..2].to_vec());
    assert_eq!(first["next_after"], json!(expected[1]));

    let (status, second) =
        sellers(&app, "bitcoin", &format!("?limit=2&after={}", expected[1])).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(keys(&second), expected[2..].to_vec());
    assert_eq!(second["next_after"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refuses_unknown_methods_cursors_and_limits(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    for (method, query, reason) in [
        ("stripe", "", "invalid_payment_method"),
        ("usdt", "", "invalid_payment_method"),
        ("bitcoin", "?after=not-a-pubky", "invalid_cursor"),
        ("bitcoin", "?limit=0", "invalid_limit"),
        ("bitcoin", "?limit=501", "invalid_limit"),
    ] {
        let (status, body) = sellers(&app, method, query).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{method}{query}: {body}"
        );
        assert_eq!(body["error"]["reason"], json!(reason), "{method}{query}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_needs_a_paykit_client_on_the_deployment(pool: PgPool) {
    let app = test_app(pool).await;
    let (status, body) = sellers(&app, "bitcoin", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), Vec::<String>::new());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_lists_opted_in_sellers_when_the_deployment_offers_it(pool: PgPool) {
    let (app, _paykit) =
        test_app_with_paykit_api_config(pool, usdt_config(), PaykitApi::Upstream).await;
    let opted_in = seller_with(
        &app,
        json!({ "bitcoin_enabled": true, "usdt_enabled": true }),
    )
    .await;
    seller_with(
        &app,
        json!({ "bitcoin_enabled": true, "usdt_enabled": false }),
    )
    .await;

    let (status, body) = sellers(&app, "usdt", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["method"], json!("usdt"));
    assert_eq!(keys(&body), vec![opted_in.pubky]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_is_empty_when_the_deployment_cannot_offer_it(pool: PgPool) {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    let (app, _paykit) = test_app_with_paykit_api_config(pool, config, PaykitApi::Upstream).await;
    seller_with(
        &app,
        json!({ "bitcoin_enabled": true, "usdt_enabled": true }),
    )
    .await;

    let (status, body) = sellers(&app, "usdt", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), Vec::<String>::new());
}
