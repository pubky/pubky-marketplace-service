//! The `/v1/connectors` surface: feature flag, seller authentication and
//! ownership, the fixed provider routes (launch, callbacks, webhooks), the
//! catalog and product selection, mappings, and the credential never leaving
//! the sealed column.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use common::connector_provider::*;
use common::{create_paid_order, new_actor, test_app};
use marketplace_service::connectors::keys::ConnectorKeys;
use marketplace_service::connectors::provider::{
    CatalogOption, CatalogProduct, CatalogVariant, Price,
};
use marketplace_service::connectors::store::{self, OpenError};
use marketplace_service::connectors::{self, ConnectorSettings};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

fn product(id: &str, variant: &str, stock: Option<i64>) -> CatalogProduct {
    CatalogProduct {
        provider_product_id: id.to_string(),
        title: format!("Product {id}"),
        description: Some("A boot".to_string()),
        url: Some(format!("https://{SHOP}/products/{id}")),
        options: vec![CatalogOption {
            name: "Size".to_string(),
            values: vec!["42".to_string()],
        }],
        image_urls: vec![format!("https://cdn.{SHOP}/{id}.jpg")],
        variants: vec![CatalogVariant {
            provider_variant_id: variant.to_string(),
            sku: Some("BOOT-42".to_string()),
            title: Some("42".to_string()),
            option_values: vec![],
            price: Price {
                amount_minor: 12_500,
                currency: "CHF".to_string(),
                exponent: 2,
            },
            stock,
            image_url: None,
        }],
    }
}

fn settings_with_return_url() -> ConnectorSettings {
    let mut settings = test_settings();
    settings.return_url = Some("https://shop.test/settings/connections".to_string());
    settings
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn connectors_are_off_by_default(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "connectors_disabled");
    let (status, body) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "shopify", "domain": SHOP }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "connectors_disabled");
    for (method, path) in [
        ("POST", "/v1/connectors/webhooks/shopify/john"),
        ("POST", "/v1/connectors/webhooks/woocommerce"),
        ("GET", "/v1/connectors/shopify/apps/john/launch"),
        ("GET", "/v1/connectors/shopify/apps/john/oauth/callback"),
        ("POST", "/v1/connectors/woocommerce/oauth/callback"),
    ] {
        let (status, _, _) = raw(app.router.clone(), method, path, &[], b"{}".to_vec()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
    }
    let (status, health) = api(&app, "GET", "/health", None, &json!(null)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(health.get("connectors").is_none(), "{health}");
    let (status, status_body) = api(
        &app,
        "GET",
        &format!("/v0/sellers/{}/connector-status", seller.pubky),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["stores"], json!([]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_reports_connectors_only_while_enabled(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let (_, health) = api(&app, "GET", "/health", None, &json!(null)).await;
    assert_eq!(health["connectors"]["available"], true);
    assert_eq!(
        health["connectors"]["providers"],
        json!(["shopify", "woocommerce"])
    );
    assert!(
        !health.to_string().contains("client-john"),
        "no app detail is public"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_seller_route_requires_a_session(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let id = Uuid::new_v4();
    let order = Uuid::new_v4();
    let routes: Vec<(&str, String)> = vec![
        ("GET", "/v1/connectors".to_string()),
        ("POST", "/v1/connectors".to_string()),
        ("DELETE", format!("/v1/connectors/{id}")),
        ("GET", format!("/v1/connectors/{id}/catalog")),
        ("GET", format!("/v1/connectors/{id}/mappings")),
        ("PUT", format!("/v1/connectors/{id}/mappings")),
        ("GET", format!("/v1/connectors/{id}/selection")),
        ("PUT", format!("/v1/connectors/{id}/selection")),
        ("GET", format!("/v1/connectors/{id}/pushes")),
        ("POST", format!("/v1/connectors/{id}/pushes/{order}/retry")),
        ("GET", "/v1/connectors/alerts".to_string()),
        ("POST", format!("/v1/connectors/alerts/{id}/resolve")),
    ];
    for (method, uri) in routes {
        let (status, _) = api(&app, method, &uri, None, &json!({})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
        let (status, _) = api(&app, method, &uri, Some("not-a-session"), &json!({})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri} bad token");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn another_seller_can_neither_read_nor_use_a_connection(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    store.set_catalog(vec![product("prod-1", "var-1", Some(3))]);
    let owner = new_actor(&app).await;
    let intruder = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &owner, SHOP).await;
    let order = create_paid_order(&app, &owner, &buyer).await;
    map_listing(&app, &owner, connection, "boots_01", "prod-1", "var-1").await;
    marketplace_service::connectors::worker::run_pass(&app.state)
        .await
        .expect("pass");
    store.catalog_calls.store(0, Ordering::SeqCst);

    let base = format!("/v1/connectors/{connection}");
    let attempts: Vec<(&str, String, Value)> = vec![
        ("GET", format!("{base}/catalog"), json!(null)),
        ("GET", format!("{base}/mappings"), json!(null)),
        ("PUT", format!("{base}/mappings"), json!({ "mappings": [] })),
        ("GET", format!("{base}/selection"), json!(null)),
        (
            "PUT",
            format!("{base}/selection"),
            json!({ "product_ids": [] }),
        ),
        ("GET", format!("{base}/pushes"), json!(null)),
        (
            "POST",
            format!("{base}/pushes/{}/retry", order.order_id),
            json!(null),
        ),
        ("DELETE", base.clone(), json!(null)),
    ];
    for (method, uri, body) in attempts {
        let (status, response) = api(&app, method, &uri, Some(&intruder.token), &body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}: {response}");
        assert_eq!(response["error"]["code"], "connection_not_found");
    }
    let (_, unknown) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{}/catalog", Uuid::new_v4()),
        Some(&intruder.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        unknown["error"]["code"], "connection_not_found",
        "same answer as unknown"
    );

    assert_eq!(
        store.catalog_calls.load(Ordering::SeqCst),
        0,
        "no provider call was made"
    );
    assert_eq!(store.disconnect_calls.load(Ordering::SeqCst), 0);
    let (_, list) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&intruder.token),
        &json!(null),
    )
    .await;
    assert_eq!(list["connections"], json!([]));
    let (_, alerts) = api(
        &app,
        "GET",
        "/v1/connectors/alerts",
        Some(&intruder.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"], json!([]));
    let (_, owner_list) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&owner.token),
        &json!(null),
    )
    .await;
    assert_eq!(owner_list["connections"][0]["status"], "connected");
    assert_eq!(owner_list["connections"][0]["mapping_count"], 1);
    assert_eq!(owner_list["connections"][0]["app_key"], APP_KEY);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn another_seller_cannot_resolve_an_alert_they_do_not_own(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    store.script(PushScript::Rejected("validation"));
    let owner = new_actor(&app).await;
    let intruder = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &owner, SHOP).await;
    create_paid_order(&app, &owner, &buyer).await;
    map_listing(&app, &owner, connection, "boots_01", "prod-1", "var-1").await;
    marketplace_service::connectors::worker::run_pass(&app.state)
        .await
        .expect("pass");
    let alert_id: Uuid = sqlx::query_scalar("SELECT id FROM connector_alerts")
        .fetch_one(&app.pool)
        .await
        .expect("alert");
    let url = format!("/v1/connectors/alerts/{alert_id}/resolve");
    let (status, _) = api(&app, "POST", &url, Some(&intruder.token), &json!(null)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        common::count(
            &app.pool,
            "SELECT COUNT(*) FROM connector_alerts WHERE resolved_at IS NULL"
        )
        .await,
        1
    );
    let (status, _) = api(&app, "POST", &url, Some(&owner.token), &json!(null)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        common::count(
            &app.pool,
            "SELECT COUNT(*) FROM connector_alerts WHERE resolved_at IS NULL"
        )
        .await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pending_connect_holds_a_one_time_nonce_for_thirty_minutes(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    for (body, code) in [
        (
            json!({ "provider": "etsy", "domain": SHOP }),
            "invalid_provider",
        ),
        (
            json!({ "provider": "shopify", "domain": "bad|shop" }),
            "invalid_domain",
        ),
        (
            json!({ "provider": "shopify", "domain": SHOP, "app_key": "nobody" }),
            "unknown_app",
        ),
    ] {
        let (status, response) =
            api(&app, "POST", "/v1/connectors", Some(&seller.token), &body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
        assert_eq!(response["error"]["code"], code);
    }
    let (status, started) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "shopify", "domain": SHOP }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    assert_eq!(started["connection"]["status"], "pending");
    assert_eq!(started["connection"]["domain"], SHOP);
    assert_eq!(started["connection"]["app_key"], Value::Null);
    assert_eq!(
        started["authorize_url"],
        Value::Null,
        "Shopify installs from the store's own admin"
    );
    let expires: chrono::DateTime<chrono::Utc> = started["connection"]["expires_at"]
        .as_str()
        .expect("expires_at")
        .parse()
        .expect("timestamp");
    let created: chrono::DateTime<chrono::Utc> = started["connection"]["created_at"]
        .as_str()
        .expect("created_at")
        .parse()
        .expect("timestamp");
    assert_eq!((expires - created).num_minutes(), 30);
    let hash: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT connect_state_hash FROM connector_connections")
            .fetch_one(&app.pool)
            .await
            .expect("state hash");
    assert_eq!(
        hash.expect("a hash is stored").len(),
        32,
        "only the hash of the nonce is kept"
    );

    // A named app is recorded straight away.
    let (_, named) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "shopify", "domain": "other.test-store.example", "app_key": "pilot-1" }),
    )
    .await;
    assert_eq!(named["connection"]["app_key"], "pilot-1");

    // After thirty minutes the pending connect is gone for a launch.
    app.clock.advance_seconds(30 * 60 + 1);
    let (status, _, body) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "no_pending_connect");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_shopify_launch_and_callback_follow_the_fixed_routes(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;

    // An install with no pending connect has nowhere to go yet.
    let (status, _, body) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "no_pending_connect");

    let connection = start_connect(&app, &first, SHOP).await;

    // The launch is verified with the app's own secret and app key.
    let (status, _, body) = launch(&app, APP_KEY, SHOP, "wrong-secret").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"]["code"], "invalid_launch");
    let (status, _, body) = launch(&app, "no-such-app", SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "unknown_app");
    let (status, _, _) = launch(&app, "pilot-1", SHOP, TEST_APP_SECRET).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "john's secret does not sign pilot-1's launch"
    );

    let (status, location, body) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::SEE_OTHER, "{body}");
    let location = location.expect("redirect target");
    assert!(location.starts_with(&format!("https://{SHOP}/admin/oauth/authorize?")));
    assert_eq!(query_param(&location, "client_id"), "client-john");
    assert_eq!(
        query_param(&location, "redirect_uri"),
        "https://service.test/v1/connectors/shopify/apps/john/oauth/callback"
    );
    let state = query_param(&location, "state");
    assert_eq!(
        store.last_app.lock().expect("app lock").as_deref(),
        Some(APP_KEY)
    );
    let bound: Option<String> =
        sqlx::query_scalar("SELECT app_key FROM connector_connections WHERE id = $1")
            .bind(connection)
            .fetch_one(&app.pool)
            .await
            .expect("app key");
    assert_eq!(
        bound.as_deref(),
        Some(APP_KEY),
        "the launch records the app"
    );

    let callback = |app_key: &str, query: &str| {
        let router = app.router.clone();
        let path = format!("/v1/connectors/shopify/apps/{app_key}/oauth/callback?{query}");
        async move { raw(router, "GET", &path, &[], Vec::new()).await }
    };
    // A wrong code leaves the pending connect usable; a forged state and the
    // wrong app are both refused as an invalid state.
    let (status, _, body) = callback(APP_KEY, &format!("state={state}&code=bad")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(as_json(&body)["error"]["code"], "connect_rejected");
    let (status, _, body) = callback(APP_KEY, "state=forged&code=good").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(as_json(&body)["error"]["code"], "invalid_state");
    let (status, _, body) = callback("pilot-1", &format!("state={state}&code=good")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(as_json(&body)["error"]["code"], "invalid_state");

    let (status, _, body) = callback(APP_KEY, &format!("state={state}&code=good")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let done = as_json(&body);
    assert_eq!(done["connection"]["status"], "connected");
    assert_eq!(done["connection"]["app_key"], APP_KEY);
    assert_eq!(
        done["connection"]["scopes"],
        json!(["read_products", "write_orders"])
    );
    let (status, _, _) = callback(APP_KEY, &format!("state={state}&code=good")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "the state is single use");

    // The same seller cannot connect it twice; a second seller can ask, and
    // the install is then refused because the store is taken.
    let (status, again) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&first.token),
        &json!({ "provider": "shopify", "domain": SHOP }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{again}");
    assert_eq!(again["error"]["code"], "already_connected");
    start_connect(&app, &second, SHOP).await;
    let (status, location, _) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let state = query_param(&location.expect("redirect"), "state");
    let (status, _, body) = callback(APP_KEY, &format!("state={state}&code=good")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(as_json(&body)["error"]["code"], "shop_in_use");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_sellers_pending_for_one_store_make_the_launch_ambiguous(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    start_connect(&app, &first, SHOP).await;
    start_connect(&app, &second, SHOP).await;
    let (status, _, body) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "ambiguous_connect");
    assert_eq!(
        common::count(
            &app.pool,
            "SELECT COUNT(*) FROM connector_connections WHERE app_key IS NOT NULL"
        )
        .await,
        0,
        "nothing was bound"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn browser_steps_return_to_the_shop_when_a_return_url_is_configured(pool: PgPool) {
    let (app, _store) = connector_app_with(pool, test_keys(), settings_with_return_url()).await;
    let seller = new_actor(&app).await;

    let (status, location, _) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let location = location.expect("redirect to the Shop");
    assert!(location.starts_with("https://shop.test/settings/connections?"));
    assert_eq!(query_param(&location, "status"), "install_unclaimed");
    assert_eq!(query_param(&location, "domain"), SHOP);
    assert_eq!(query_param(&location, "provider"), "shopify");

    let connection = start_connect(&app, &seller, SHOP).await;
    let (_, location, _) = launch(&app, APP_KEY, SHOP, TEST_APP_SECRET).await;
    let state = query_param(&location.expect("authorize"), "state");
    let (status, headers, _) = raw(
        app.router.clone(),
        "GET",
        &format!("/v1/connectors/shopify/apps/{APP_KEY}/oauth/callback?state={state}&code=good"),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let back = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("redirect to the Shop")
        .to_string();
    assert_eq!(query_param(&back, "status"), "connected");
    assert_eq!(query_param(&back, "connector"), connection.to_string());

    let (_, headers, _) = raw(
        app.router.clone(),
        "GET",
        &format!("/v1/connectors/shopify/apps/{APP_KEY}/oauth/callback?state=forged&code=good"),
        &[],
        Vec::new(),
    )
    .await;
    let failed = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("error redirect")
        .to_string();
    assert_eq!(query_param(&failed, "status"), "error");
    assert_eq!(query_param(&failed, "code"), "invalid_state");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn woocommerce_connects_through_its_own_callback_and_signs_with_a_store_secret(pool: PgPool) {
    let (app, stores) = connector_app_full(pool, test_keys(), test_settings()).await;
    let seller = new_actor(&app).await;
    let woo_shop = "https://shop.example.test".replace("https://", "");
    let (status, started) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "woocommerce", "domain": woo_shop }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let authorize = started["authorize_url"].as_str().expect("wc-auth url");
    assert!(authorize.contains("/wc-auth/v1/authorize"));
    assert_eq!(
        query_param(authorize, "callback_url"),
        "https://service.test/v1/connectors/woocommerce/oauth/callback"
    );
    let nonce = query_param(authorize, "user_id");
    let connection: Uuid = started["connection"]["id"]
        .as_str()
        .expect("id")
        .parse()
        .expect("uuid");

    // Keys posted with a forged nonce are refused; the real nonce connects.
    let post = |user_id: String| {
        let router = app.router.clone();
        async move {
            raw(
                router,
                "POST",
                "/v1/connectors/woocommerce/oauth/callback",
                &[("content-type", "application/json")],
                serde_json::to_vec(&json!({
                    "user_id": user_id,
                    "consumer_key": "ck_test",
                    "consumer_secret": "cs_test",
                }))
                .expect("body"),
            )
            .await
        }
    };
    let (status, _, _) = post("forged".to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _, body) = post(nonce.clone()).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(as_json(&body)["connection"]["status"], "connected");
    assert_eq!(as_json(&body)["connection"]["app_key"], Value::Null);
    let (status, _, _) = post(nonce).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "the nonce is single use");
    assert!(stores
        .woocommerce
        .last_app
        .lock()
        .expect("app lock")
        .is_none());

    // Webhooks have no app key and are signed with the store's own secret.
    let body = serde_json::to_vec(
        &json!({ "type": "stock", "product": "p", "variant": "", "available": 3 }),
    )
    .expect("body");
    let (status, accepted) = post_signed(
        &app,
        "/v1/connectors/webhooks/woocommerce",
        &woo_shop,
        "woo-evt-1",
        "product.updated",
        body.clone(),
        TEST_WEBHOOK_SECRET,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{accepted}");
    assert_eq!(accepted["status"], "accepted");
    let (status, _) = post_signed(
        &app,
        "/v1/connectors/webhooks/woocommerce",
        &woo_shop,
        "woo-evt-2",
        "product.updated",
        body,
        TEST_APP_SECRET,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let stored: String = sqlx::query_scalar(
        "SELECT provider FROM connector_inbound_events WHERE connection_id = $1",
    )
    .bind(connection)
    .fetch_one(&app.pool)
    .await
    .expect("inbound row");
    assert_eq!(stored, "woocommerce");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_is_limited_in_how_many_stores_they_connect(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    for index in 0..5 {
        let (status, body) = api(
            &app,
            "POST",
            "/v1/connectors",
            Some(&seller.token),
            &json!({ "provider": "shopify", "domain": format!("shop-{index}.test-store.example") }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    let (status, body) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "shopify", "domain": "one-more.test-store.example" }),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], "connection_limit");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_catalog_is_normalised_paged_marked_with_the_selection_and_free_of_credentials(
    pool: PgPool,
) {
    let (app, store) = connector_app(pool).await;
    store.set_catalog(vec![
        product("prod-1", "var-1", Some(3)),
        product("prod-2", "var-2", None),
    ]);
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let url = format!("/v1/connectors/{connection}/catalog");
    let (status, first) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["products"][0]["provider_product_id"], "prod-1");
    assert_eq!(
        first["products"][0]["variants"][0]["price"]["currency"],
        "CHF"
    );
    assert_eq!(first["products"][0]["variants"][0]["stock"], 3);
    assert_eq!(first["products"][0]["options"][0]["name"], "Size");
    assert_eq!(first["products"][0]["selected"], false);
    assert_eq!(first["next_page"], "1");
    let (_, second) = api(
        &app,
        "GET",
        &format!("{url}?page=1"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(second["products"][0]["provider_product_id"], "prod-2");
    assert_eq!(second["products"][0]["variants"][0]["stock"], Value::Null);
    assert_eq!(second["next_page"], Value::Null);

    // Choosing products to publish marks them in the catalog and filters it.
    let (status, chosen) = api(
        &app,
        "PUT",
        &format!("/v1/connectors/{connection}/selection"),
        Some(&seller.token),
        &json!({ "product_ids": ["prod-1"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{chosen}");
    let (_, marked) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(marked["products"][0]["selected"], true);
    let (_, only_selected) = api(
        &app,
        "GET",
        &format!("{url}?selected=true"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        only_selected["products"]
            .as_array()
            .expect("products")
            .len(),
        1
    );
    let (_, only_unselected) = api(
        &app,
        "GET",
        &format!("{url}?selected=false"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(only_unselected["products"], json!([]));
    let text = format!("{first}{second}{marked}");
    assert!(!text.contains(TEST_TOKEN) && !text.contains(TEST_WEBHOOK_SECRET));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_picks_which_store_products_to_publish(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    create_paid_order(&app, &seller, &buyer).await;
    let url = format!("/v1/connectors/{connection}/selection");

    let (_, empty) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(empty["product_ids"], json!([]));
    let (status, saved) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "product_ids": ["b-2", "a-1", "c-3"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["product_ids"], json!(["a-1", "b-2", "c-3"]));
    let (_, read) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(read["product_ids"], json!(["a-1", "b-2", "c-3"]));

    for (body, code) in [
        (json!({ "product_ids": ["a", "a"] }), "duplicate_product"),
        (json!({ "product_ids": [""] }), "invalid_selection"),
        (json!({ "product_ids": [" padded "] }), "invalid_selection"),
    ] {
        let (status, response) = api(&app, "PUT", &url, Some(&seller.token), &body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
        assert_eq!(response["error"]["code"], code);
    }
    let (_, unchanged) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(unchanged["product_ids"], json!(["a-1", "b-2", "c-3"]));

    // Mapping a product selects it, and a mapped product cannot be dropped.
    map_listing(&app, &seller, connection, "boots_01", "prod-9", "var-9").await;
    let (_, with_mapped) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert!(with_mapped["product_ids"]
        .as_array()
        .expect("ids")
        .contains(&json!("prod-9")));
    let (status, refused) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "product_ids": ["a-1"] }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], "product_mapped");
    assert_eq!(refused["error"]["product_id"], "prod-9");
    let (status, _) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "product_ids": ["a-1", "prod-9"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_rejected_credential_surfaces_as_a_conflict_and_an_alert(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    store.reject_credentials.store(true, Ordering::SeqCst);
    let (status, body) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{connection}/catalog"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "credentials_rejected");
    let (_, list) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(list["connections"][0]["status"], "error");
    let (_, alerts) = api(
        &app,
        "GET",
        "/v1/connectors/alerts",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"][0]["kind"], "credentials_rejected");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mappings_are_unique_in_both_directions_and_owned(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    create_paid_order(&app, &seller, &buyer).await;
    let other_connection = connect_shop(&app, &other, "other.test-store.example").await;
    let url = format!("/v1/connectors/{connection}/mappings");

    let (status, body) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": "42", "provider_product_id": "p1", "provider_variant_id": "v1" },
            { "listing_id": "boots_01", "variant_id": "42", "provider_product_id": "p2", "provider_variant_id": "v2" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "duplicate_mapping");
    let (status, body) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": "42", "provider_product_id": "p1", "provider_variant_id": "v1" },
            { "listing_id": "boots_01", "variant_id": "43", "provider_product_id": "p1", "provider_variant_id": "v1" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "duplicate_mapping");
    let (status, body) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [{ "listing_id": "not_mine", "provider_product_id": "p1" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "listing_not_found");
    assert_eq!(body["error"]["listing_id"], "not_mine");
    let (status, body) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [{ "listing_id": "", "provider_product_id": "p1" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "invalid_mapping");

    let (status, saved) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "provider_product_id": "p1", "provider_variant_id": "v1", "sku": "S" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["mappings"][0]["variant_id"], Value::Null);
    let (_, read) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(read["mappings"], saved["mappings"]);

    let (status, _) = api(
        &app,
        "PUT",
        &url,
        Some(&seller.token),
        &json!({ "mappings": [] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, read) = api(&app, "GET", &url, Some(&seller.token), &json!(null)).await;
    assert_eq!(read["mappings"], json!([]));

    let insert = "INSERT INTO connector_variant_map \
        (id, connection_id, listing_id, variant_id, provider_product_id, provider_variant_id, created_at, updated_at) \
        VALUES ($1, $2, $3, '', $4, $5, now(), now())";
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(connection)
        .bind("boots_01")
        .bind("p1")
        .bind("v1")
        .execute(&app.pool)
        .await
        .expect("first mapping");
    assert!(sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(connection)
        .bind("boots_01")
        .bind("p9")
        .bind("v9")
        .execute(&app.pool)
        .await
        .is_err());
    assert!(sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(connection)
        .bind("other_listing")
        .bind("p1")
        .bind("v1")
        .execute(&app.pool)
        .await
        .is_err());
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(other_connection)
        .bind("boots_01")
        .bind("p1")
        .bind("v1")
        .execute(&app.pool)
        .await
        .expect("another connection may map the same ids");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_credential_is_sealed_never_returned_and_deleted_on_disconnect(pool: PgPool) {
    common::install_log_capture();
    let (app, store) = connector_app(pool).await;
    store.set_catalog(vec![product("prod-1", "var-1", Some(1))]);
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let base = format!("/v1/connectors/{connection}");

    let sealed: Vec<u8> =
        sqlx::query_scalar("SELECT sealed FROM connector_credentials WHERE connection_id = $1")
            .bind(connection)
            .fetch_one(&app.pool)
            .await
            .expect("sealed credential");
    let haystack = String::from_utf8_lossy(&sealed).into_owned();
    assert!(!haystack.contains(TEST_TOKEN) && !haystack.contains("shpat"));
    assert!(sealed.len() > 24 + TEST_TOKEN.len());

    let mut everything = String::new();
    for (method, uri) in [
        ("GET", "/v1/connectors".to_string()),
        ("GET", format!("{base}/catalog")),
        ("GET", format!("{base}/mappings")),
        ("GET", format!("{base}/selection")),
        ("GET", format!("{base}/pushes")),
        ("GET", "/v1/connectors/alerts".to_string()),
    ] {
        let (_, body) = api(&app, method, &uri, Some(&seller.token), &json!(null)).await;
        everything.push_str(&body.to_string());
    }
    assert!(!everything.contains(TEST_TOKEN));
    assert!(!everything.contains(TEST_WEBHOOK_SECRET));
    assert!(!everything.contains(TEST_APP_SECRET));
    assert!(!everything.contains("sealed"));

    // A row moved to another connection does not open there: the associated
    // data binds seller, provider and shop.
    let other = new_actor(&app).await;
    let other_connection = connect_shop(&app, &other, "elsewhere.test-store.example").await;
    sqlx::query("UPDATE connector_credentials SET sealed = $2 WHERE connection_id = $1")
        .bind(other_connection)
        .bind(&sealed)
        .execute(&app.pool)
        .await
        .expect("transplant");
    let victim = store::get(&app.pool, other_connection)
        .await
        .expect("read")
        .expect("row");
    let keys = test_keys();
    assert!(matches!(
        store::open_connection(&app.pool, &keys, &victim).await,
        Err(OpenError::Unreadable)
    ));
    let original = store::get(&app.pool, connection)
        .await
        .expect("read")
        .expect("row");
    let runtime = app.state.connectors.as_ref().expect("runtime");
    let context = runtime
        .open(&app.pool, &original)
        .await
        .expect("the owner's credential opens");
    assert!(String::from_utf8_lossy(context.credentials.expose()).contains(TEST_TOKEN));
    assert_eq!(
        context.app.as_ref().map(|app| app.app_key.as_str()),
        Some(APP_KEY),
        "the connection's app rides with the credential"
    );
    assert_eq!(
        format!("{:?}", context.credentials),
        "CredentialSecret(<redacted>)"
    );
    assert!(!format!("{context:?}").contains(TEST_APP_SECRET));
    drop(context);

    let logs = common::captured_logs();
    assert!(!logs.contains(TEST_TOKEN), "credential reached the logs");
    assert!(
        !logs.contains(TEST_WEBHOOK_SECRET),
        "webhook secret reached the logs"
    );
    assert!(
        !logs.contains(TEST_APP_SECRET),
        "app secret reached the logs"
    );

    let (status, _) = api(&app, "DELETE", &base, Some(&seller.token), &json!(null)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(store.disconnect_calls.load(Ordering::SeqCst), 1);
    for table in ["connector_credentials", "connector_recipient_keys"] {
        assert_eq!(
            common::count(
                &app.pool,
                &format!("SELECT COUNT(*) FROM {table} WHERE connection_id = '{connection}'")
            )
            .await,
            0,
            "{table}"
        );
    }
    let (status, _) = api(&app, "DELETE", &base, Some(&seller.token), &json!(null)).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "disconnecting twice is harmless"
    );
    let (status, body) = api(
        &app,
        "GET",
        &format!("{base}/catalog"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "connection_not_connected");
    let (_, list) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(list["connections"], json!([]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_recipient_key_is_published_in_the_public_status_and_its_secret_never_is(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let (key_id, public_key, sealed_secret): (String, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT key_id, public_key, sealed_secret FROM connector_recipient_keys \
         WHERE connection_id = $1",
    )
    .bind(connection)
    .fetch_one(&app.pool)
    .await
    .expect("recipient key");
    assert_eq!(key_id.len(), 32);
    assert_eq!(public_key.len(), 32);

    // Anyone, with no session, can read the status a buyer's checkout needs.
    let path = format!("/v0/sellers/{}/connector-status", seller.pubky);
    let (status, headers, body) = raw(app.router.clone(), "GET", &path, &[], Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("max-age")));
    let published = as_json(&body);
    assert_eq!(published["seller_pubky"], seller.pubky);
    let store_entry = &published["stores"][0];
    assert_eq!(store_entry["provider"], "shopify");
    assert_eq!(store_entry["domain"], SHOP);
    let key = &store_entry["address_sharing"]["recipient_key"];
    assert_eq!(key["key_id"], key_id);
    assert_eq!(key["public_key"], URL_SAFE_NO_PAD.encode(&public_key));
    assert_eq!(key["algorithm"], "hpke-x25519-hkdf-sha256-chacha20poly1305");
    assert_eq!(
        key["envelope_context"],
        "pubky-marketplace/connector-address/v1"
    );
    assert_eq!(key["envelope_bytes"], 2096);
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(!text.contains(&URL_SAFE_NO_PAD.encode(&sealed_secret)));
    assert!(!text.contains(TEST_TOKEN) && !text.contains("sealed"));

    let (_, empty) = api(
        &app,
        "GET",
        &format!("/v0/sellers/{}/connector-status", stranger.pubky),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(empty["stores"], json!([]));

    // The secret half is sealed to its connection.
    let other = new_actor(&app).await;
    let other_connection = connect_shop(&app, &other, "elsewhere.test-store.example").await;
    let victim = store::get(&app.pool, other_connection)
        .await
        .expect("read")
        .expect("row");
    sqlx::query("UPDATE connector_recipient_keys SET sealed_secret = $2 WHERE connection_id = $1")
        .bind(other_connection)
        .bind(&sealed_secret)
        .execute(&app.pool)
        .await
        .expect("transplant");
    assert!(matches!(
        store::open_recipient_secret(&app.pool, &test_keys(), &victim, &key_id).await,
        Err(OpenError::Unreadable) | Ok(None)
    ));

    // A disconnect withdraws the key from the status.
    api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{connection}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    let (_, after) = api(&app, "GET", &path, None, &json!(null)).await;
    assert_eq!(after["stores"], json!([]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn rotating_the_credential_key_reseals_credentials_and_recipient_keys(pool: PgPool) {
    let (app, _store) = connector_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let row = store::get(&pool, connection)
        .await
        .expect("read")
        .expect("row");
    let (key_id,): (String,) =
        sqlx::query_as("SELECT key_id FROM connector_recipient_keys WHERE connection_id = $1")
            .bind(connection)
            .fetch_one(&pool)
            .await
            .expect("key id");

    let old = Arc::new(ConnectorKeys::from_hex(CONNECTOR_KEY, None).expect("old"));
    let rotated = Arc::new(
        ConnectorKeys::from_hex(CONNECTOR_PREVIOUS_KEY, Some(CONNECTOR_KEY)).expect("rotated"),
    );
    let new_only = Arc::new(ConnectorKeys::from_hex(CONNECTOR_PREVIOUS_KEY, None).expect("new"));

    assert!(matches!(
        store::open_connection(&pool, &new_only, &row).await,
        Err(OpenError::Unreadable)
    ));
    assert!(matches!(
        store::open_recipient_secret(&pool, &new_only, &row, &key_id).await,
        Err(OpenError::Unreadable)
    ));
    store::open_connection(&pool, &rotated, &row)
        .await
        .expect("previous key still opens");
    assert_eq!(
        connectors::assert_credentials_coherent(&pool, Some(&rotated))
            .await
            .expect("probe"),
        2,
        "a credential and a recipient key"
    );

    let progress = connectors::reseal_previous_key(&pool, &rotated, app.state.clock.now())
        .await
        .expect("reseal");
    assert_eq!(progress.resealed, 2);
    assert_eq!(progress.remaining_under_previous, 0);
    let again = connectors::reseal_previous_key(&pool, &rotated, app.state.clock.now())
        .await
        .expect("second pass");
    assert_eq!(again.resealed, 0);

    let context = store::open_connection(&pool, &new_only, &row)
        .await
        .expect("current key opens the resealed credential");
    assert!(String::from_utf8_lossy(context.credentials.expose()).contains(TEST_TOKEN));
    assert!(
        store::open_recipient_secret(&pool, &new_only, &row, &key_id)
            .await
            .expect("current key opens the recipient secret")
            .is_some()
    );
    assert!(matches!(
        store::open_connection(&pool, &old, &row).await,
        Err(OpenError::Unreadable)
    ));
    assert!(matches!(
        store::open_recipient_secret(&pool, &old, &row, &key_id).await,
        Err(OpenError::Unreadable)
    ));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_boot_probe_refuses_a_missing_or_wrong_key(pool: PgPool) {
    let (app, _store) = connector_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    connect_shop(&app, &seller, SHOP).await;
    let wrong = ConnectorKeys::from_hex(CONNECTOR_PREVIOUS_KEY, None).expect("wrong key");
    assert!(connectors::assert_credentials_coherent(&pool, Some(&wrong))
        .await
        .is_err());
    assert!(connectors::assert_credentials_coherent(&pool, None)
        .await
        .is_err());
    let right = ConnectorKeys::from_hex(CONNECTOR_KEY, None).expect("right key");
    assert_eq!(
        connectors::assert_credentials_coherent(&pool, Some(&right))
            .await
            .expect("probe"),
        2
    );
    for table in ["connector_credentials", "connector_recipient_keys"] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&pool)
            .await
            .expect("delete");
    }
    assert_eq!(
        connectors::assert_credentials_coherent(&pool, None)
            .await
            .expect("empty"),
        0
    );
}

#[test]
fn the_credential_key_must_differ_from_every_other_sealing_key() {
    use marketplace_service::connectors::keys::ensure_distinct_keys;
    let pickup = common::test_pickup_keys();
    let same_as_pickup =
        ConnectorKeys::from_hex(common::TEST_PICKUP_ENCRYPTION_KEY, None).expect("keys parse");
    assert!(ensure_distinct_keys(&same_as_pickup, None, Some(&pickup), None, None).is_err());
    let distinct = ConnectorKeys::from_hex(CONNECTOR_KEY, None).expect("keys parse");
    ensure_distinct_keys(&distinct, None, Some(&pickup), None, None).expect("distinct keys pass");
    ConnectorKeys::from_hex(CONNECTOR_KEY, Some(CONNECTOR_KEY)).expect_err("equal keys refused");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_pushes_route_pages_and_filters_the_ledger(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let mut orders = Vec::new();
    for offset in 0..3 {
        let order = Uuid::new_v4();
        orders.push(order);
        sqlx::query(
            "INSERT INTO connector_order_push (id, connection_id, seller_pubky, pubky_order_id, \
             provider, state, attempts, next_attempt_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, 'shopify', 'pending', 0, now(), \
                     now() + ($5 || ' seconds')::interval, now())",
        )
        .bind(Uuid::new_v4())
        .bind(connection)
        .bind(&seller.pubky)
        .bind(order)
        .bind(offset.to_string())
        .execute(&app.pool)
        .await
        .expect("ledger row");
    }
    let url = format!("/v1/connectors/{connection}/pushes");
    let (status, first) = api(
        &app,
        "GET",
        &format!("{url}?limit=2"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["pushes"].as_array().expect("pushes").len(), 2);
    assert_eq!(first["pushes"][0]["pubky_order_id"], orders[2].to_string());
    let cursor = first["next_cursor"].as_str().expect("cursor").to_string();
    let (_, second) = api(
        &app,
        "GET",
        &format!("{url}?limit=2&cursor={cursor}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(second["pushes"].as_array().expect("pushes").len(), 1);
    assert_eq!(second["pushes"][0]["pubky_order_id"], orders[0].to_string());
    assert_eq!(second["next_cursor"], Value::Null);
    let (_, none) = api(
        &app,
        "GET",
        &format!("{url}?state=created"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(none["pushes"], json!([]));
    for bad in ["?limit=0", "?limit=500", "?state=weird", "?cursor=nope"] {
        let (status, _) = api(
            &app,
            "GET",
            &format!("{url}{bad}"),
            Some(&seller.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}");
    }
}
