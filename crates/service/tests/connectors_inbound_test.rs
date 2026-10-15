//! Provider webhooks and the stock poll: signature, dedupe, and what each
//! event is allowed to do to Pubky.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::connector_provider::*;
use common::{create_paid_order, new_actor, TestActor, TestApp};
use marketplace_service::connectors::provider::{
    CatalogOption, CatalogProduct, CatalogVariant, Price,
};
use marketplace_service::connectors::worker;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

struct Shop {
    app: TestApp,
    store: Arc<TestStore>,
    seller: TestActor,
    connection: Uuid,
    order_id: String,
}

/// A connected, mapped store with one pushed (created) order.
async fn shop(pool: PgPool) -> Shop {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    Shop {
        app,
        store,
        seller,
        connection,
        order_id: order.order_id,
    }
}

async fn listing_available(app: &TestApp, seller: &str) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT available_quantity, total_quantity, server_revision FROM listings \
         WHERE seller_pubky = $1 AND listing_id = 'boots_01'",
    )
    .bind(seller)
    .fetch_one(&app.pool)
    .await
    .expect("listing")
}

async fn inbound(app: &TestApp, event_id: &str) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT state, outcome FROM connector_inbound_events WHERE provider_event_id = $1",
    )
    .bind(event_id)
    .fetch_one(&app.pool)
    .await
    .expect("inbound row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_the_right_secret_every_failure_looks_the_same(pool: PgPool) {
    let s = shop(pool).await;
    let body = serde_json::to_vec(&json!({ "type": "stock" })).expect("body");
    let (bad_status, bad) = signed_webhook(
        &s.app,
        SHOP,
        "evt-bad",
        "stock",
        body.clone(),
        "wrong-secret",
    )
    .await;
    let (unknown_status, unknown) = signed_webhook(
        &s.app,
        "nobody.test-store.example",
        "evt-unknown",
        "stock",
        body.clone(),
        "wrong-secret",
    )
    .await;
    // The pilot app's secret signs correctly for its own path, but the store
    // installed the other app.
    let (wrong_app_status, wrong_app) = post_signed(
        &s.app,
        "/v1/connectors/webhooks/shopify/pilot-1",
        SHOP,
        "evt-wrong-app",
        "stock",
        body.clone(),
        "pilot-secret",
    )
    .await;
    assert_eq!(bad_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unknown_status, StatusCode::UNAUTHORIZED);
    assert_eq!(wrong_app_status, StatusCode::UNAUTHORIZED);
    assert_eq!(bad, unknown, "the response does not say which stores exist");
    assert_eq!(bad, wrong_app);

    let (status, _, response) = raw(
        s.app.router.clone(),
        "POST",
        &format!("/v1/connectors/webhooks/shopify/{APP_KEY}"),
        &[("x-test-shop", SHOP), ("x-test-event-id", "evt-nosig")],
        body,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(as_json(&response), bad);
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_inbound_events").await,
        0
    );
    for path in [
        "/v1/connectors/webhooks/etsy",
        "/v1/connectors/webhooks/shopify/no-such-app",
    ] {
        let (status, _, _) = raw(s.app.router.clone(), "POST", path, &[], b"{}".to_vec()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_redelivered_webhook_is_acknowledged_and_applied_once(pool: PgPool) {
    let s = shop(pool).await;
    let body = json!({ "type": "stock", "product": "prod-1", "variant": "var-1", "available": 6 });
    let (status, first) = webhook(
        &s.app,
        SHOP,
        "evt-stock-1",
        "inventory_levels/update",
        &body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["status"], "accepted");
    let (status, second) = webhook(
        &s.app,
        SHOP,
        "evt-stock-1",
        "inventory_levels/update",
        &body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["status"], "duplicate");
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_inbound_events").await,
        1
    );

    let before = listing_available(&s.app, &s.seller.pubky).await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    let after = listing_available(&s.app, &s.seller.pubky).await;
    assert_eq!(after.0, 6, "available now matches the store");
    assert_eq!(after.2, before.2 + 1);
    assert_eq!(inbound(&s.app, "evt-stock-1").await.0, "applied");

    let (_, third) = webhook(
        &s.app,
        SHOP,
        "evt-stock-1",
        "inventory_levels/update",
        &body,
    )
    .await;
    assert_eq!(third["status"], "duplicate");
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await, after);
    assert_eq!(
        common::count(
            &s.app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'inventory.adjusted'"
        )
        .await,
        1
    );

    let (_, other) = webhook(
        &s.app,
        SHOP,
        "evt-stock-2",
        "inventory_levels/update",
        &body,
    )
    .await;
    assert_eq!(other["status"], "accepted");
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-stock-2").await,
        ("ignored".to_string(), Some("unchanged".to_string()))
    );
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await, after);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn stock_is_applied_to_mapped_variants_only(pool: PgPool) {
    let s = shop(pool).await;
    let before = listing_available(&s.app, &s.seller.pubky).await;
    let (status, _) = webhook(
        &s.app,
        SHOP,
        "evt-unmapped",
        "inventory_levels/update",
        &json!({ "type": "stock", "product": "prod-9", "variant": "var-9", "available": 50 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-unmapped").await,
        ("ignored".to_string(), Some("unmapped_variant".to_string()))
    );
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await, before);

    webhook(
        &s.app,
        SHOP,
        "evt-five",
        "inventory_levels/update",
        &json!({ "type": "stock", "product": "prod-1", "variant": "var-1", "available": 5 }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await.0, 5);
    webhook(
        &s.app,
        SHOP,
        "evt-negative",
        "inventory_levels/update",
        &json!({ "type": "stock", "product": "prod-1", "variant": "var-1", "available": -4 }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        listing_available(&s.app, &s.seller.pubky).await.0,
        0,
        "a negative store level clamps to zero"
    );

    let (status, _) = api(
        &s.app,
        "PUT",
        &format!("/v1/connectors/{}/mappings", s.connection),
        Some(&s.seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": "a", "provider_product_id": "prod-1", "provider_variant_id": "var-1" },
            { "listing_id": "boots_01", "variant_id": "b", "provider_product_id": "prod-1", "provider_variant_id": "var-2" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    webhook(
        &s.app,
        SHOP,
        "evt-multi",
        "inventory_levels/update",
        &json!({ "type": "stock", "product": "prod-1", "variant": "var-2", "available": 9 }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-multi").await,
        (
            "ignored".to_string(),
            Some("multi_variant_listing".to_string())
        )
    );
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await.0, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_fulfillment_webhook_ships_the_pubky_order_with_its_tracking(pool: PgPool) {
    let s = shop(pool).await;
    let (status, _) = webhook(
        &s.app,
        SHOP,
        "evt-ship-1",
        "fulfillments/create",
        &json!({ "type": "fulfillment", "order": "ext-1", "carrier": "DHL", "tracking": "JD0146000" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(inbound(&s.app, "evt-ship-1").await.0, "applied");
    let (state, shipment): (String, Value) =
        sqlx::query_as("SELECT state, shipment FROM orders WHERE id = $1::uuid")
            .bind(&s.order_id)
            .fetch_one(&s.app.pool)
            .await
            .expect("order");
    assert_eq!(state, "shipped");
    assert_eq!(shipment["carrier"], "DHL");
    assert_eq!(shipment["tracking_number"], "JD0146000");

    webhook(
        &s.app,
        SHOP,
        "evt-ship-2",
        "fulfillments/create",
        &json!({ "type": "fulfillment", "order": "ext-1", "carrier": "UPS", "tracking": "1Z999" }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-ship-2").await,
        (
            "ignored".to_string(),
            Some("order_not_shippable".to_string())
        )
    );
    let tracking: String =
        sqlx::query_scalar("SELECT shipment->>'tracking_number' FROM orders WHERE id = $1::uuid")
            .bind(&s.order_id)
            .fetch_one(&s.app.pool)
            .await
            .expect("tracking");
    assert_eq!(tracking, "JD0146000");

    webhook(
        &s.app,
        SHOP,
        "evt-ship-3",
        "fulfillments/create",
        &json!({ "type": "fulfillment", "order": "merchants-own-order", "carrier": "UPS", "tracking": "X" }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-ship-3").await,
        ("ignored".to_string(), Some("unknown_order".to_string()))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_in_the_store_alerts_the_seller_and_never_refunds(pool: PgPool) {
    let s = shop(pool).await;
    let (status, _) = webhook(
        &s.app,
        SHOP,
        "evt-cancel-1",
        "orders/cancelled",
        &json!({ "type": "cancelled", "order": "ext-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(inbound(&s.app, "evt-cancel-1").await.0, "applied");

    let (_, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts?unresolved=true",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"].as_array().expect("alerts").len(), 1);
    assert_eq!(alerts["alerts"][0]["kind"], "merchant_cancelled");
    assert_eq!(alerts["alerts"][0]["pubky_order_id"], s.order_id);
    assert!(alerts["alerts"][0]["detail"]
        .as_str()
        .expect("detail")
        .contains("Nothing was refunded"));

    let (state, refund, reason): (String, Option<Value>, Option<String>) = sqlx::query_as(
        "SELECT state, external_refund, cancellation_reason FROM orders WHERE id = $1::uuid",
    )
    .bind(&s.order_id)
    .fetch_one(&s.app.pool)
    .await
    .expect("order");
    assert_eq!((state.as_str(), refund, reason), ("paid", None, None));
    assert_eq!(
        common::count(
            &s.app.pool,
            "SELECT COUNT(*) FROM events WHERE kind LIKE 'refund.%' OR kind LIKE 'order.cancel%'"
        )
        .await,
        0
    );
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_order_actions").await,
        0
    );

    webhook(
        &s.app,
        SHOP,
        "evt-cancel-2",
        "orders/cancelled",
        &json!({ "type": "cancelled", "order": "ext-1" }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_alerts").await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_in_the_store_alerts_the_seller_and_moves_no_money(pool: PgPool) {
    let s = shop(pool).await;
    webhook(
        &s.app,
        SHOP,
        "evt-refund-1",
        "refunds/create",
        &json!({ "type": "refund", "order": "ext-1", "amount_minor": 12500 }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    let (_, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"][0]["kind"], "merchant_refunded");
    let refund: Option<Value> =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1::uuid")
            .bind(&s.order_id)
            .fetch_one(&s.app.pool)
            .await
            .expect("order");
    assert_eq!(refund, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn uninstalling_the_app_deletes_the_credential_and_the_recipient_key(pool: PgPool) {
    let s = shop(pool).await;
    let (status, _) = webhook(
        &s.app,
        SHOP,
        "evt-uninstall",
        "app/uninstalled",
        &json!({ "type": "uninstalled" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    for table in [
        "connector_credentials",
        "connector_recipient_keys",
        "connector_variant_map",
        "connector_product_selection",
    ] {
        assert_eq!(
            common::count(&s.app.pool, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }
    let (_, list) = api(
        &s.app,
        "GET",
        "/v1/connectors",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(list["connections"], json!([]));
    let (_, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"][0]["kind"], "store_uninstalled");
    let (status, _, _) = raw(
        s.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/connector-status", s.seller.pubky),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, status_body) = api(
        &s.app,
        "GET",
        &format!("/v0/sellers/{}/connector-status", s.seller.pubky),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(
        status_body["stores"],
        json!([]),
        "no key is published any more"
    );
    let _ = s.store;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_privacy_webhooks_are_honoured_even_after_the_store_disconnected(pool: PgPool) {
    let s = shop(pool).await;
    // Customer topics: nothing to return or erase, acknowledged and recorded.
    for (event, kind) in [
        ("evt-data", "data_request"),
        ("evt-customer", "customer_redact"),
    ] {
        let (status, body) = webhook(
            &s.app,
            SHOP,
            event,
            &format!("customers/{kind}"),
            &json!({ "type": kind }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "accepted");
    }
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-data").await,
        (
            "applied".to_string(),
            Some("no store customer data is held".to_string())
        )
    );
    assert_eq!(inbound(&s.app, "evt-customer").await.0, "applied");
    let (_, list) = api(
        &s.app,
        "GET",
        "/v1/connectors",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        list["connections"][0]["status"], "connected",
        "customer topics change nothing"
    );

    // The merchant uninstalls; 48 hours later Shopify sends shop/redact. The
    // credential is long gone, and the webhook is verified with the app
    // secret alone.
    webhook(
        &s.app,
        SHOP,
        "evt-uninstall",
        "app/uninstalled",
        &json!({ "type": "uninstalled" }),
    )
    .await;
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    s.app.clock.advance_seconds(48 * 3600);
    let (status, body) = webhook(
        &s.app,
        SHOP,
        "evt-redact",
        "shop/redact",
        &json!({ "type": "shop_redact" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "accepted");
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        inbound(&s.app, "evt-redact").await,
        (
            "applied".to_string(),
            Some("store data deleted".to_string())
        )
    );
    let leftover: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM connector_inbound_events WHERE event <> '{}'::jsonb AND provider_event_id <> 'evt-redact'",
    )
    .fetch_one(&s.app.pool)
    .await
    .expect("count");
    assert_eq!(leftover, 0, "stored event payloads are emptied");

    // A shop that was never connected, with a valid app signature, is
    // acknowledged and ignored; a bad signature is still refused.
    let (status, body) = webhook(
        &s.app,
        "never-connected.test-store.example",
        "evt-stranger",
        "shop/redact",
        &json!({ "type": "shop_redact" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ignored");
    let (status, _) = signed_webhook(
        &s.app,
        "never-connected.test-store.example",
        "evt-stranger-2",
        "shop/redact",
        b"{}".to_vec(),
        "wrong",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_but_signed_event_is_acknowledged_and_ignored(pool: PgPool) {
    let s = shop(pool).await;
    let (status, body) = signed_webhook(
        &s.app,
        SHOP,
        "evt-garbage",
        "stock",
        b"not json at all".to_vec(),
        TEST_APP_SECRET,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a 4xx would make the store retry for days"
    );
    assert_eq!(body["status"], "ignored");
    let (status, body) = webhook(
        &s.app,
        SHOP,
        "evt-topic",
        "customers/update",
        &json!({ "type": "customer" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "accepted");
    worker::drive_inbound(&s.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(inbound(&s.app, "evt-topic").await.0, "ignored");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_stock_poll_applies_catalog_stock_to_mapped_variants(pool: PgPool) {
    let s = shop(pool).await;
    let variant = |id: &str, stock: Option<i64>| CatalogVariant {
        provider_variant_id: id.to_string(),
        sku: None,
        title: None,
        option_values: vec![],
        price: Price {
            amount_minor: 1,
            currency: "USD".to_string(),
            exponent: 2,
        },
        stock,
        image_url: None,
    };
    let product = |id: &str, variants: Vec<CatalogVariant>| CatalogProduct {
        provider_product_id: id.to_string(),
        title: id.to_string(),
        description: None,
        url: None,
        options: Vec::<CatalogOption>::new(),
        image_urls: vec![],
        variants,
    };
    s.store.set_catalog(vec![
        product("prod-0", vec![variant("unmapped", Some(99))]),
        product("prod-1", vec![variant("var-1", Some(4))]),
    ]);
    let applied = worker::poll_stock(&s.app.state).await.expect("poll");
    assert_eq!(applied, 1);
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await.0, 4);
    assert_eq!(worker::poll_stock(&s.app.state).await.expect("poll"), 0);

    s.store
        .set_catalog(vec![product("prod-1", vec![variant("var-1", None)])]);
    assert_eq!(worker::poll_stock(&s.app.state).await.expect("poll"), 0);
    assert_eq!(listing_available(&s.app, &s.seller.pubky).await.0, 4);
}
