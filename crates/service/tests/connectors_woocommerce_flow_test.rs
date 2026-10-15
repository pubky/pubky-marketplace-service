//! The WooCommerce connector through the whole service, against PostgreSQL:
//! the routes, the framework's ledger and worker, and the real provider
//! talking to WooCommerce. The store side is the replay of exchanges and
//! webhook deliveries captured from a real WooCommerce (see
//! `tests/fixtures/woocommerce/README.md`); with `WOO_LIVE=1` the same tests
//! run against the Docker store and re-record them.

mod common;
mod woocommerce_support;

use axum::http::StatusCode;
use common::connector_provider::{api, as_json, raw};
use common::{create_paid_order, new_actor};
use marketplace_service::clock::Clock;
use marketplace_service::connectors::store;
use marketplace_service::connectors::worker;
use serde_json::{json, Value};
use sqlx::PgPool;
use woocommerce_support::flows::*;
use woocommerce_support::{post_delivery, woo_app, StoreSide};

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_merchant_connects_picks_products_and_gets_paid_orders_stock_and_tracking(pool: PgPool) {
    let side = StoreSide::open("flow_a").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (connection, secret) = connect_via_routes(&app, &seller, &side).await;

    // The catalog, in pages, with CHF prices in minor units.
    let products = read_catalog(&app, &seller, connection).await;
    let by_title = |title: &str| {
        products
            .iter()
            .find(|product| product["title"] == title)
            .unwrap_or_else(|| panic!("{title} in the catalog"))
    };
    let item = by_title("Pubky Test Item");
    assert_eq!(item["provider_product_id"], TEST_PRODUCT.to_string());
    assert_eq!(item["variants"][0]["price"]["currency"], "CHF");
    assert_eq!(item["variants"][0]["price"]["amount_minor"], 1250);
    assert_eq!(item["variants"][0]["stock"], 10);
    assert_eq!(item["categories"], json!(["Accessories"]));
    assert_eq!(item["attributes"][0]["name"], "Material");
    assert!(!item["image_urls"].as_array().expect("images").is_empty());
    let socks = by_title("Little Hodler Socks");
    assert_eq!(socks["options"][0]["name"], "Size");
    assert_eq!(socks["variants"].as_array().expect("variants").len(), 3);
    assert_eq!(socks["variants"][0]["price"]["amount_minor"], 1990);
    assert_eq!(socks["variants"][2]["price"]["amount_minor"], 2150);
    assert_eq!(socks["variants"][2]["stock"], 0);
    assert!(
        products
            .iter()
            .all(|p| p["title"] != "Logo Collection" && p["title"] != "WordPress Pennant"),
        "grouped and external products cannot be sold through a push"
    );

    // The seller picks products; the Shop publishes only those.
    let (status, selection) = api(
        &app,
        "PUT",
        &format!("/v1/connectors/{connection}/selection"),
        Some(&seller.token),
        &json!({ "product_ids": [TEST_PRODUCT.to_string(), SOCKS.to_string()] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{selection}");
    let (_, picked) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{connection}/catalog?selected=true"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    let picked_ids: Vec<&str> = picked["products"]
        .as_array()
        .expect("products")
        .iter()
        .map(|p| p["provider_product_id"].as_str().expect("id"))
        .collect();
    assert!(picked_ids.iter().all(|id| *id == "39" || *id == "35"));

    // A paid Pubky order becomes a paid, processing order in the store.
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_test_product(&app, &seller, connection).await;
    worker::run_pass(&app.state).await.expect("pass");
    let (state, external) = push_state(&app, &order.order_id).await;
    assert_eq!(state, "created");
    let external = external.expect("the store order id");
    if let Some(replay) = &side.replay {
        let created = replay.requests("POST", "/wp-json/wc/v3/orders");
        assert_eq!(created.len(), 1);
        let body = created[0].body.as_ref().expect("body");
        assert_eq!(body["status"], "processing");
        assert_eq!(body["set_paid"], true);
        assert_eq!(body["line_items"][0]["product_id"], TEST_PRODUCT);
        assert!(body.get("billing").is_none() && body.get("shipping").is_none());
        assert_eq!(body["meta_data"][0]["value"], order.order_id.as_str());
        assert_eq!(created[0].auth, "basic");
    } else {
        verify_live_order(side.live(), &external, &order.order_id, 9).await;
    }

    // The poll closes gaps a missed webhook leaves: the store is the truth.
    worker::poll_stock(&app.state).await.expect("poll");
    assert_eq!(listing_available(&app, &seller.pubky).await, 9);

    // Stock changed in the store reaches the listing.
    let stock = side
        .deliver(
            "stock_changed",
            &secret,
            async {
                side.live()
                    .rest(
                        "PUT",
                        &format!("/wc/v3/products/{TEST_PRODUCT}"),
                        Some(json!({"stock_quantity": 6})),
                    )
                    .await;
            },
            |topic, body| {
                topic == "product.updated"
                    && body["id"] == TEST_PRODUCT
                    && body["stock_quantity"] == 6
            },
        )
        .await;
    let (status, body) = post_delivery(&app, &stock).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("accepted"))
    );
    let (status, body) = post_delivery(&app, &stock).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("duplicate"))
    );
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(listing_available(&app, &seller.pubky).await, 6);

    // Completed in the store with tracking: the Pubky order ships.
    let shipped = side
        .deliver(
            "shipped",
            &secret,
            async {
                let live = side.live();
                live.rest(
                    "POST",
                    &format!("/wc/v3/orders/{external}/shipment-trackings"),
                    Some(json!({"tracking_provider": "Swiss Post", "tracking_number": "99.60.123456.12345678"})),
                )
                .await;
                live.rest("PUT", &format!("/wc/v3/orders/{external}"), Some(json!({"status": "completed"}))).await;
            },
            |topic, body| {
                topic == "order.updated" && body["id"].as_i64() == external.parse().ok() && body["status"] == "completed"
            },
        )
        .await;
    let (status, body) = post_delivery(&app, &shipped).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("accepted"))
    );
    worker::run_pass(&app.state).await.expect("pass");
    let (state, shipment): (String, Value) =
        sqlx::query_as("SELECT state, shipment FROM orders WHERE id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order");
    assert_eq!(state, "shipped");
    assert_eq!(
        shipment["carrier"], "swiss-post",
        "the tracking plugin's own provider value"
    );
    assert_eq!(shipment["tracking_number"], "99.60.123456.12345678");

    // WooCommerce's own unsigned ping is acknowledged; a bad signature is not.
    let (status, _, bytes) = raw(
        app.router.clone(),
        "POST",
        &woocommerce_support::webhook_path(connection),
        &[("content-type", "application/x-www-form-urlencoded")],
        b"webhook_id=3".to_vec(),
    )
    .await;
    assert_eq!(
        (
            status,
            as_json(&bytes)["status"].as_str().map(str::to_string)
        ),
        (StatusCode::OK, Some("ignored".into()))
    );
    let mut forged = stock.clone();
    forged
        .headers
        .insert("x-wc-webhook-signature".into(), "AAAA".into());
    let (status, body) = post_delivery(&app, &forged).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("invalid_webhook"))
    );
    // A delivery is only good at its own connection's URL: the same signed
    // delivery posted to another connection's URL is refused.
    let (status, body) =
        woocommerce_support::post_delivery_to(&app, uuid::Uuid::new_v4(), &stock).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("invalid_webhook"))
    );

    // Disconnecting deletes our webhooks from the store and our credential.
    let (status, _) = api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{connection}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let credentials: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM connector_credentials")
        .fetch_one(&app.pool)
        .await
        .expect("count");
    assert_eq!(credentials, 0);
    if let Some(replay) = &side.replay {
        let deleted = replay
            .matched()
            .iter()
            .filter(|seen| {
                seen.method == "DELETE" && seen.path.starts_with("/wp-json/wc/v3/webhooks/")
            })
            .count();
        assert!(
            deleted >= 3,
            "our webhooks are taken off the store: {deleted}"
        );
        replay.assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_or_delete_in_the_store_alerts_the_seller_and_refunds_nobody(pool: PgPool) {
    let shop = pushed_shop(pool, "flow_b").await;
    let external = shop.external.clone();
    let cancelled = shop
        .side
        .deliver(
            "store_cancelled",
            &shop.secret,
            async {
                shop.side
                    .live()
                    .rest(
                        "PUT",
                        &format!("/wc/v3/orders/{external}"),
                        Some(json!({"status": "cancelled"})),
                    )
                    .await;
            },
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && body["status"] == "cancelled"
            },
        )
        .await;
    let (status, body) = deliver_and_apply(&shop, &cancelled).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("accepted"))
    );
    let raised = alerts(&shop).await;
    assert_eq!(raised.len(), 1);
    assert_eq!(raised[0]["kind"], "merchant_cancelled");
    assert_eq!(raised[0]["pubky_order_id"], shop.order.order_id.as_str());
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&shop.order.order_id)
        .fetch_one(&shop.app.pool)
        .await
        .expect("order");
    assert_eq!(
        state, "paid",
        "a store-side cancel never cancels or refunds the Pubky order"
    );

    // Trashing the order afterwards is the same fact: no second alert.
    let deleted = shop
        .side
        .deliver(
            "store_deleted",
            &shop.secret,
            async {
                shop.side
                    .live()
                    .rest("DELETE", &format!("/wc/v3/orders/{external}"), None)
                    .await;
            },
            |topic, body| topic == "order.deleted" && body["id"].as_i64() == external.parse().ok(),
        )
        .await;
    let (status, _) = deliver_and_apply(&shop, &deleted).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(alerts(&shop).await.len(), 1);
    let outcomes = inbound_outcomes(&shop.app).await;
    assert!(
        outcomes.iter().all(|(_, state, _)| state == "applied"),
        "{outcomes:?}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_cancel_cancels_the_store_order_and_its_echo_raises_no_alert(pool: PgPool) {
    let shop = pushed_shop(pool, "flow_c").await;
    let external = shop.external.clone();
    cancel_in_pubky(&shop).await;
    worker::run_pass(&shop.app.state).await.expect("pass");
    let action: String =
        sqlx::query_scalar("SELECT state FROM connector_order_actions WHERE kind = 'cancel'")
            .fetch_one(&shop.app.pool)
            .await
            .expect("cancel action");
    assert_eq!(action, "done");
    if let Some(replay) = &shop.side.replay {
        let put = replay.requests("PUT", &format!("/wp-json/wc/v3/orders/{external}"));
        assert_eq!(put.len(), 1);
        let body = put[0].body.as_ref().expect("body");
        assert_eq!(body["status"], "cancelled");
        assert_eq!(body["meta_data"][0]["key"], "pubky_cancelled");
        assert!(
            replay
                .requests("POST", &format!("/wp-json/wc/v3/orders/{external}/refunds"))
                .is_empty(),
            "a cancel never refunds"
        );
    } else {
        let live = shop
            .side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{external}"), None)
            .await;
        assert_eq!(live["status"], "cancelled");
        assert_eq!(live["refunds"].as_array().expect("refunds").len(), 0);
        let item = shop
            .side
            .live()
            .rest("GET", &format!("/wc/v3/products/{TEST_PRODUCT}"), None)
            .await;
        assert_eq!(item["stock_quantity"], 10, "WooCommerce put the stock back");
    }
    let echo = shop
        .side
        .deliver(
            "pubky_cancel_echo",
            &shop.secret,
            async {},
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && body["status"] == "cancelled"
            },
        )
        .await;
    let (status, body) = deliver_and_apply(&shop, &echo).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("accepted"))
    );
    assert!(
        alerts(&shop).await.is_empty(),
        "our own cancel is not the merchant cancelling"
    );
    let outcomes = inbound_outcomes(&shop.app).await;
    assert!(
        outcomes
            .iter()
            .any(|(_, state, why)| state == "ignored"
                && why.as_deref() == Some("cancelled_by_pubky")),
        "{outcomes:?}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_refund_is_recorded_in_the_store_without_moving_money(pool: PgPool) {
    let shop = pushed_shop(pool, "flow_d").await;
    let external = shop.external.clone();
    refund_in_pubky(&shop).await;
    worker::run_pass(&shop.app.state).await.expect("pass");
    let action: String =
        sqlx::query_scalar("SELECT state FROM connector_order_actions WHERE kind = 'refund'")
            .fetch_one(&shop.app.pool)
            .await
            .expect("refund action");
    assert_eq!(action, "done");
    if let Some(replay) = &shop.side.replay {
        let posted = replay.requests("POST", &format!("/wp-json/wc/v3/orders/{external}/refunds"));
        assert_eq!(posted.len(), 1);
        let body = posted[0].body.as_ref().expect("body");
        assert_eq!(body["api_refund"], false);
        assert_eq!(body["amount"], "137.00");
        assert!(body["reason"]
            .as_str()
            .expect("reason")
            .starts_with("Pubky refund"));
    } else {
        let live = shop
            .side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{external}"), None)
            .await;
        assert_eq!(live["status"], "refunded");
        assert_eq!(live["refunds"][0]["total"], "-137.00");
    }
    let echo = shop
        .side
        .deliver(
            "pubky_refund_echo",
            &shop.secret,
            async {},
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && body["status"] == "refunded"
            },
        )
        .await;
    let (status, _) = deliver_and_apply(&shop, &echo).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        alerts(&shop).await.is_empty(),
        "our own refund record is not the merchant refunding"
    );
    let outcomes = inbound_outcomes(&shop.app).await;
    assert!(
        outcomes.iter().any(
            |(_, state, why)| state == "ignored" && why.as_deref() == Some("refunded_by_pubky")
        ),
        "{outcomes:?}"
    );
    if let Some(replay) = &shop.side.replay {
        replay.assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn pasted_keys_connect_a_pending_store_and_wrong_ones_do_not(pool: PgPool) {
    let side = StoreSide::open("flow_e").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (status, started) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "woocommerce", "domain": side.site() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let id = started["connection"]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let keys_path = format!("/v1/connectors/{id}/woocommerce/keys");

    // The merchant makes a key in WooCommerce > Settings > Advanced > REST API
    // (here: through the same approval, which mints one) and pastes it.
    let minted: Value = serde_json::from_slice(
        &side.approve(started["authorize_url"].as_str().expect("url"), "keys"),
    )
    .expect("keys JSON");
    let (key, secret) = (
        minted["consumer_key"].clone(),
        minted["consumer_secret"].clone(),
    );

    let (status, body) = api(
        &app,
        "PUT",
        &keys_path,
        Some(&seller.token),
        &json!({"consumer_key": "nope", "consumer_secret": "nope"}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid_keys"))
    );
    let wrong = json!({"consumer_key": format!("ck_{}", "e".repeat(40)), "consumer_secret": format!("cs_{}", "f".repeat(40))});
    let (status, body) = api(&app, "PUT", &keys_path, Some(&seller.token), &wrong).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("credentials_rejected")
        ),
        "{body}"
    );
    let (status, body) = api(
        &app,
        "PUT",
        &keys_path,
        Some(&stranger.token),
        &json!({"consumer_key": key, "consumer_secret": secret}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::NOT_FOUND, Some("connection_not_found"))
    );

    let (status, body) = api(
        &app,
        "PUT",
        &keys_path,
        Some(&seller.token),
        &json!({"consumer_key": key, "consumer_secret": secret}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["connection"]["status"], "connected");
    assert!(!body.to_string().contains("ck_") && !body.to_string().contains("cs_"));
    let (status, again) = api(
        &app,
        "PUT",
        &keys_path,
        Some(&seller.token),
        &json!({"consumer_key": key, "consumer_secret": secret}),
    )
    .await;
    assert_eq!(
        (status, again["connection"]["status"].as_str()),
        (StatusCode::OK, Some("connected")),
        "keys on a connected store replace its keys"
    );

    let (status, catalog) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{id}/catalog"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{catalog}");
    assert!(!catalog["products"].as_array().expect("products").is_empty());
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refused_connect_leaves_no_draft_behind_and_never_hits_the_limit(pool: PgPool) {
    let side = StoreSide::open("flow_f").await;
    let _restore = side
        .live
        .as_ref()
        .map(woocommerce_support::live::RestoreStore);
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let drafts = |app: &common::TestApp| {
        let pool = app.pool.clone();
        async move {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM connector_connections")
                .fetch_one(&pool)
                .await
                .expect("count");
            count
        }
    };

    // WooCommerce is not active: the probe refuses. The limit is five
    // connections per seller, so six refusals in a row would block a seller
    // if each left its draft behind.
    if side.is_live() {
        side.live().woocommerce_active(false);
    }
    for attempt in 1..=6 {
        let (status, body) = api(
            &app,
            "POST",
            "/v1/connectors",
            Some(&seller.token),
            &json!({ "provider": "woocommerce", "domain": side.site() }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "attempt {attempt}: {body}"
        );
        assert!(body["error"]["message"]
            .as_str()
            .expect("message")
            .contains("WooCommerce was not found"));
        assert_eq!(drafts(&app).await, 0, "attempt {attempt} left a draft");
    }
    // A malformed address is refused before any draft exists.
    let (status, body) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "woocommerce", "domain": "https://user:pass@dezentralshop.ch" }),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid_domain"))
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .contains("username or password"),
        "{body}"
    );
    assert_eq!(drafts(&app).await, 0);

    // With WooCommerce back, the same seller connects; a restart of the same
    // store reuses its draft rather than adding another.
    if side.is_live() {
        side.live().woocommerce_active(true);
    }
    for _ in 0..2 {
        let (status, body) = api(
            &app,
            "POST",
            "/v1/connectors",
            Some(&seller.token),
            &json!({ "provider": "woocommerce", "domain": side.site() }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(drafts(&app).await, 1);
    }
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_shops_credentials_form_connects_a_woocommerce_store_with_its_key_pair(pool: PgPool) {
    let side = StoreSide::open("flow_g").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (status, started) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "woocommerce", "domain": side.site() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let id = started["connection"]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let path = format!("/v1/connectors/{id}/credentials");
    let minted: Value = serde_json::from_slice(
        &side.approve(started["authorize_url"].as_str().expect("url"), "keys"),
    )
    .expect("keys JSON");
    let right = json!({"consumer_key": minted["consumer_key"], "consumer_secret": minted["consumer_secret"]});

    // The Shopify body is not for this provider.
    let (status, body) = api(
        &app,
        "PUT",
        &path,
        Some(&seller.token),
        &json!({"client_id": "a", "client_secret": "b"}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("credentials_not_supported")
        )
    );
    let (status, body) = api(
        &app,
        "PUT",
        &path,
        Some(&seller.token),
        &json!({"consumer_key": "nope", "consumer_secret": "nope"}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid_keys"))
    );
    let wrong = json!({"consumer_key": format!("ck_{}", "e".repeat(40)), "consumer_secret": format!("cs_{}", "f".repeat(40))});
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &wrong).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("credentials_rejected")
        ),
        "{body}"
    );
    let (status, body) = api(&app, "PUT", &path, Some(&stranger.token), &right).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::NOT_FOUND, Some("connection_not_found"))
    );

    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &right).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "connector_connected");
    assert_eq!(body["connection"]["status"], "connected");
    assert!(!body.to_string().contains("ck_") && !body.to_string().contains("cs_"));
    let (status, again) = api(&app, "PUT", &path, Some(&seller.token), &right).await;
    assert_eq!(
        (status, again["connection"]["status"].as_str()),
        (StatusCode::OK, Some("connected")),
        "keys on a connected store replace its keys"
    );
    let (status, catalog) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{id}/catalog"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{catalog}");
    assert!(!catalog["products"].as_array().expect("products").is_empty());
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

fn tracking_numbers(body: &Value) -> Vec<String> {
    body["meta_data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["key"] == "_wc_shipment_tracking_items")
        .flat_map(|entry| entry["value"].as_array().cloned().unwrap_or_default())
        .filter_map(|item| item["tracking_number"].as_str().map(str::to_string))
        .collect()
}

async fn shipment_of(shop: &Shop) -> (String, Value) {
    let (state, shipment): (String, Option<Value>) =
        sqlx::query_as("SELECT state, shipment FROM orders WHERE id = $1::uuid")
            .bind(&shop.order.order_id)
            .fetch_one(&shop.app.pool)
            .await
            .expect("order");
    (state, shipment.unwrap_or(Value::Null))
}

async fn last_outcome(shop: &Shop) -> (String, Option<String>) {
    let outcomes = inbound_outcomes(&shop.app).await;
    let (_, state, outcome) = outcomes.last().expect("an inbound event").clone();
    (state, outcome)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn completed_without_tracking_ships_as_other_and_later_tracking_follows(pool: PgPool) {
    let shop = pushed_shop(pool, "flow_h").await;
    let external = shop.external.clone();

    // Completed in the store with no tracking.
    let completed = shop
        .side
        .deliver(
            "completed_untracked",
            &shop.secret,
            async {
                shop.side
                    .live()
                    .rest(
                        "PUT",
                        &format!("/wc/v3/orders/{external}"),
                        Some(json!({"status": "completed"})),
                    )
                    .await;
            },
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && body["status"] == "completed"
                    && tracking_numbers(body).is_empty()
            },
        )
        .await;
    shop.app.clock.advance_seconds(5);
    let (status, _) = deliver_and_apply(&shop, &completed).await;
    assert_eq!(status, StatusCode::OK);
    let (state, shipment) = shipment_of(&shop).await;
    assert_eq!(state, "shipped");
    assert_eq!(shipment["carrier"], "Other");
    assert!(shipment["tracking_number"].is_null());
    assert_eq!(last_outcome(&shop).await.1.as_deref(), Some("shipped"));

    // A tracking plugin adds a number later: the same order, updated.
    let added = shop
        .side
        .deliver(
            "tracking_added",
            &shop.secret,
            async {
                shop.side
                    .live()
                    .rest(
                        "POST",
                        &format!("/wc/v3/orders/{external}/shipment-trackings"),
                        Some(json!({"tracking_provider": "Swiss Post", "tracking_number": "99.60.123456.12345678"})),
                    )
                    .await;
            },
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && tracking_numbers(body) == ["99.60.123456.12345678"]
            },
        )
        .await;
    shop.app.clock.advance_seconds(5);
    let (status, _) = deliver_and_apply(&shop, &added).await;
    assert_eq!(status, StatusCode::OK);
    let (state, shipment) = shipment_of(&shop).await;
    assert_eq!(state, "shipped");
    assert_eq!(shipment["tracking_number"], "99.60.123456.12345678");
    assert_eq!(shipment["carrier"], "swiss-post");
    assert_eq!(
        last_outcome(&shop).await.1.as_deref(),
        Some("tracking_updated")
    );

    // The same delivery again changes nothing.
    let (status, again) = post_delivery(&shop.app, &added).await;
    assert_eq!(
        (status, again["status"].as_str()),
        (StatusCode::OK, Some("duplicate"))
    );

    // The number is changed: the newest item counts.
    let changed = shop
        .side
        .deliver(
            "tracking_changed",
            &shop.secret,
            async {
                shop.side
                    .live()
                    .rest(
                        "POST",
                        &format!("/wc/v3/orders/{external}/shipment-trackings"),
                        Some(
                            json!({"tracking_provider": "DHL", "tracking_number": "JD0146000555"}),
                        ),
                    )
                    .await;
            },
            |topic, body| {
                topic == "order.updated"
                    && body["id"].as_i64() == external.parse().ok()
                    && tracking_numbers(body).last().map(String::as_str) == Some("JD0146000555")
            },
        )
        .await;
    shop.app.clock.advance_seconds(5);
    let (status, _) = deliver_and_apply(&shop, &changed).await;
    assert_eq!(status, StatusCode::OK);
    let (state, shipment) = shipment_of(&shop).await;
    assert_eq!(state, "shipped");
    assert_eq!(shipment["tracking_number"], "JD0146000555");
    assert_eq!(
        shipment["carrier"]
            .as_str()
            .map(str::to_lowercase)
            .as_deref(),
        Some("dhl")
    );
    assert_eq!(
        shipment["tracking_history"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(
        last_outcome(&shop).await.1.as_deref(),
        Some("tracking_updated")
    );
    assert!(
        alerts(&shop).await.is_empty(),
        "a shipment is never an alert"
    );
    if let Some(replay) = &shop.side.replay {
        replay.assert_no_misses();
    }
}

/// A wc-auth URL for the store, to mint a key pair as the merchant would.
fn authorize_url(side: &StoreSide, scope: &str) -> String {
    let origin = if side.is_live() {
        "https://localhost:8443".to_string()
    } else {
        format!("http://{}", side.site())
    };
    let mut url = url::Url::parse(&format!("{origin}/wc-auth/v1/authorize")).expect("url");
    url.query_pairs_mut()
        .append_pair("app_name", "Pubky Shop")
        .append_pair("scope", scope)
        .append_pair(
            "user_id",
            &format!("rotate-{scope}-{}", uuid::Uuid::new_v4().simple()),
        )
        .append_pair("return_url", woocommerce_support::RETURN_URL)
        .append_pair("callback_url", woocommerce_support::CALLBACK_URL);
    url.into()
}

fn minted(side: &StoreSide, scope: &str, name: &str) -> Value {
    let body = side.approve(&authorize_url(side, scope), name);
    let keys: Value = serde_json::from_slice(&body).expect("keys JSON");
    json!({"consumer_key": keys["consumer_key"], "consumer_secret": keys["consumer_secret"]})
}

async fn sealed_credential(app: &common::TestApp, connection: uuid::Uuid) -> Vec<u8> {
    sqlx::query_scalar("SELECT sealed FROM connector_credentials WHERE connection_id = $1")
        .bind(connection)
        .fetch_one(&app.pool)
        .await
        .expect("sealed credential")
}

async fn webhook_status(
    side: &StoreSide,
    app: &common::TestApp,
    secret: &str,
) -> (StatusCode, Option<String>) {
    let source = if side.is_live() {
        "https://localhost:8443/".to_string()
    } else {
        format!("http://{}/", side.site())
    };
    let delivery = woocommerce_support::resign(
        &woocommerce_support::replay::Delivery::load("flow_a.stock_changed"),
        secret,
        &source,
    );
    let (status, body) = post_delivery(app, &delivery).await;
    (status, body["status"].as_str().map(str::to_string))
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn replacing_the_keys_of_a_connected_store_swaps_them_or_changes_nothing(pool: PgPool) {
    let side = StoreSide::open("flow_i").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (connection, first_secret) = connect_via_routes(&app, &seller, &side).await;
    let path = format!("/v1/connectors/{connection}/credentials");
    let before = sealed_credential(&app, connection).await;
    assert_eq!(
        webhook_status(&side, &app, &first_secret).await.0,
        StatusCode::OK
    );

    // Refusals change nothing: wrong shape, another seller's connection, keys the
    // store does not know, a read-only key.
    let (status, body) = api(
        &app,
        "PUT",
        &path,
        Some(&seller.token),
        &json!({"consumer_key": "nope", "consumer_secret": "nope"}),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid_keys"))
    );
    let unknown = json!({"consumer_key": format!("ck_{}", "e".repeat(40)), "consumer_secret": format!("cs_{}", "f".repeat(40))});
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &unknown).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("credentials_rejected")
        ),
        "{body}"
    );
    let read_only = minted(&side, "read", "keys_read_only");
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &read_only).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("keys_read_only")),
        "{body}"
    );
    let good = minted(&side, "read_write", "keys_new");
    let (status, body) = api(&app, "PUT", &path, Some(&stranger.token), &good).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::NOT_FOUND, Some("connection_not_found"))
    );
    assert_eq!(
        sealed_credential(&app, connection).await,
        before,
        "the old keys are still sealed"
    );
    assert_eq!(webhook_secret(&app, connection).await, first_secret);
    assert_eq!(
        webhook_status(&side, &app, &first_secret).await.0,
        StatusCode::OK,
        "the old webhooks still work with the old secret"
    );

    // The new pair replaces the old: same connection, new sealed credential, new
    // webhook secret, our webhooks swapped on the store.
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &good).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "connector_connected");
    assert_eq!(body["connection"]["id"], connection.to_string());
    assert_eq!(body["connection"]["status"], "connected");
    assert!(!body.to_string().contains("ck_") && !body.to_string().contains("cs_"));
    assert_ne!(sealed_credential(&app, connection).await, before);
    let second_secret = webhook_secret(&app, connection).await;
    assert_ne!(second_secret, first_secret);
    assert_eq!(
        webhook_status(&side, &app, &first_secret).await.0,
        StatusCode::UNAUTHORIZED,
        "the old secret no longer verifies"
    );
    assert_eq!(
        webhook_status(&side, &app, &second_secret).await.0,
        StatusCode::OK
    );
    let products = read_catalog(&app, &seller, connection).await;
    assert!(!products.is_empty(), "the new keys read the catalog");
    if side.is_live() {
        let hooks = side
            .live()
            .rest("GET", "/wc/v3/webhooks?per_page=100", None)
            .await;
        let ours = hooks
            .as_array()
            .expect("hooks")
            .iter()
            .filter(|hook| hook["delivery_url"] == woocommerce_support::webhook_url(connection))
            .count();
        assert_eq!(ours, 3, "the old webhooks are gone, the new ones are there");
    }

    // A store that needs attention is brought back by the same call.
    store::set_attention(
        &app.pool,
        connection,
        "webhook_registration_failed",
        app.clock.now(),
    )
    .await
    .expect("attention");
    let again = minted(&side, "read_write", "keys_again");
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &again).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["connection"]["status"], "connected");
    assert!(body["connection"]["last_error"].is_null());
    assert_ne!(webhook_secret(&app, connection).await, second_secret);

    // A disconnected connection takes no keys.
    let (status, _) = api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{connection}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &again).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("not_pending"))
    );
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

/// The webhooks a live store holds: `(id, delivery_url, secret)`.
async fn live_hooks(side: &StoreSide) -> Vec<(i64, String, String)> {
    side.live()
        .rest("GET", "/wc/v3/webhooks?per_page=100", None)
        .await
        .as_array()
        .expect("hooks")
        .iter()
        .map(|hook| {
            (
                hook["id"].as_i64().expect("id"),
                hook["delivery_url"].as_str().expect("url").to_string(),
                hook["secret"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn hooks_at(
    hooks: &[(i64, String, String)],
    connection: uuid::Uuid,
) -> Vec<&(i64, String, String)> {
    let url = woocommerce_support::webhook_url(connection);
    hooks.iter().filter(|(_, hook, _)| *hook == url).collect()
}

/// Webhook creates the replay saw that were registrations (not the write check),
/// and webhook deletes.
fn replayed_webhook_writes(side: &StoreSide) -> (usize, usize) {
    let replay = side.replay.as_ref().expect("replay");
    let created = replay
        .matched()
        .iter()
        .filter(|seen| {
            seen.method == "POST"
                && seen.path == "/wp-json/wc/v3/webhooks"
                && seen
                    .body
                    .as_ref()
                    .is_some_and(|body| body["topic"] != "pubky.access_check")
        })
        .count();
    let deleted = replay
        .matched()
        .iter()
        .filter(|seen| seen.method == "DELETE" && seen.path.starts_with("/wp-json/wc/v3/webhooks/"))
        .count();
    (created, deleted)
}

async fn webhook_status_at(
    side: &StoreSide,
    app: &common::TestApp,
    connection: uuid::Uuid,
    secret: &str,
) -> StatusCode {
    let source = if side.is_live() {
        "https://localhost:8443/".to_string()
    } else {
        format!("http://{}/", side.site())
    };
    let delivery = woocommerce_support::resign(
        &woocommerce_support::replay::Delivery::load("flow_a.stock_changed"),
        secret,
        &source,
    );
    woocommerce_support::post_delivery_to(app, connection, &delivery)
        .await
        .0
}

fn spawn_replace(
    app: &common::TestApp,
    token: &str,
    connection: uuid::Uuid,
    keys: &Value,
) -> tokio::task::JoinHandle<(StatusCode, Value)> {
    let router = app.router.clone();
    let token = token.to_string();
    let keys = keys.clone();
    tokio::spawn(async move {
        common::send(
            router,
            "PUT",
            &format!("/v1/connectors/{connection}/credentials"),
            Some(&token),
            &keys,
        )
        .await
    })
}

async fn credential_rows(app: &common::TestApp, connection: uuid::Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM connector_credentials WHERE connection_id = $1")
        .bind(connection)
        .fetch_one(&app.pool)
        .await
        .expect("count")
}

async fn status_of(app: &common::TestApp, connection: uuid::Uuid) -> String {
    store::get(&app.pool, connection)
        .await
        .expect("lookup")
        .expect("connection")
        .status
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_competing_connect_for_a_connected_store_cannot_touch_its_webhooks(pool: PgPool) {
    let side = StoreSide::open("flow_j").await;
    let app = woo_app(pool, side.provider(7)).await;
    let owner = new_actor(&app).await;
    let rival = new_actor(&app).await;
    let (connection, secret) = connect_via_routes(&app, &owner, &side).await;

    // Registered after the credential was stored, at this connection's own URL.
    if side.is_live() {
        let hooks = live_hooks(&side).await;
        assert_eq!(hooks.len(), 3);
        assert_eq!(hooks_at(&hooks, connection).len(), 3);
    } else {
        assert_eq!(replayed_webhook_writes(&side), (3, 0));
    }

    // Another seller approves keys for the same store. It is refused after the
    // keys were verified, and nothing of the first connection was touched.
    let (status, body) = api(
        &app,
        "POST",
        "/v1/connectors",
        Some(&rival.token),
        &json!({ "provider": "woocommerce", "domain": side.site() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let rival_connection: uuid::Uuid = body["connection"]["id"]
        .as_str()
        .expect("id")
        .parse()
        .expect("uuid");
    let callback = side.approve(
        body["authorize_url"].as_str().expect("url"),
        "callback_rival",
    );
    let (status, _, bytes) = raw(
        app.router.clone(),
        "POST",
        "/v1/connectors/woocommerce/oauth/callback",
        &[("content-type", "application/json;charset=UTF-8")],
        callback,
    )
    .await;
    let refused = as_json(&bytes);
    assert_eq!(
        (status, refused["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("shop_in_use")),
        "{refused}"
    );
    assert_eq!(status_of(&app, rival_connection).await, "draft");
    assert_eq!(credential_rows(&app, rival_connection).await, 0);
    assert_eq!(status_of(&app, connection).await, "connected");
    assert_eq!(webhook_secret(&app, connection).await, secret);
    assert_eq!(
        webhook_status_at(&side, &app, connection, &secret).await,
        StatusCode::OK,
        "the first connection's webhooks still verify"
    );
    if side.is_live() {
        let hooks = live_hooks(&side).await;
        assert_eq!(
            hooks.len(),
            3,
            "the rival's attempt created and removed nothing"
        );
        assert_eq!(hooks_at(&hooks, connection).len(), 3);
        assert!(hooks_at(&hooks, rival_connection).is_empty());
    } else {
        assert_eq!(
            replayed_webhook_writes(&side),
            (3, 0),
            "the rival created and deleted no webhook"
        );
        side.replay.as_ref().expect("replay").assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_disconnect_while_keys_are_being_replaced_wins_and_the_keys_are_not_stored(pool: PgPool) {
    let side = StoreSide::open("flow_k").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let (connection, _) = connect_via_routes(&app, &seller, &side).await;
    let good = minted(&side, "read_write", "keys_new");

    // The store is slow to answer the new keys' write check.
    side.delay_next("probe", std::time::Duration::from_secs(4));
    let replacing = spawn_replace(&app, &seller.token, connection, &good);
    side.wait_until_held("probe", 2).await;

    // The replacement holds no lock on the connection while it waits, so the
    // disconnect goes straight through.
    let started = std::time::Instant::now();
    let (status, _) = api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{connection}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "the disconnect was not blocked by the replacement: {:?}",
        started.elapsed()
    );

    // The replacement then finds the connection gone and stores nothing.
    let (status, body) = replacing.await.expect("replace finished");
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("not_pending")),
        "{body}"
    );
    assert_eq!(status_of(&app, connection).await, "disconnected");
    assert_eq!(credential_rows(&app, connection).await, 0);
    if side.is_live() {
        assert!(
            hooks_at(&live_hooks(&side).await, connection).is_empty(),
            "no webhook is left"
        );
    } else {
        assert_eq!(
            replayed_webhook_writes(&side),
            (3, 3),
            "only the connect's three were created, and the disconnect removed them"
        );
        side.replay.as_ref().expect("replay").assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_disconnect_between_the_swap_and_the_registration_leaves_no_keys_and_no_webhooks(
    pool: PgPool,
) {
    let side = StoreSide::open("flow_l").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let (connection, _) = connect_via_routes(&app, &seller, &side).await;
    let before = sealed_credential(&app, connection).await;
    let good = minted(&side, "read_write", "keys_new");

    // The keys verify and are swapped in; the store is slow to create the first
    // of the new webhooks, and the seller disconnects meanwhile.
    side.delay_next("register", std::time::Duration::from_secs(4));
    let replacing = spawn_replace(&app, &seller.token, connection, &good);
    side.wait_until_held("register", 2).await;
    assert_ne!(
        sealed_credential(&app, connection).await,
        before,
        "the swap had happened"
    );
    let (status, _) = api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{connection}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // The registration lands after the disconnect removed the webhooks; it sees
    // that and removes what it created.
    let (status, body) = replacing.await.expect("replace finished");
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("not_pending")),
        "{body}"
    );
    assert_eq!(status_of(&app, connection).await, "disconnected");
    assert_eq!(credential_rows(&app, connection).await, 0);
    if side.is_live() {
        assert!(
            hooks_at(&live_hooks(&side).await, connection).is_empty(),
            "no webhook of the disconnected connection is left"
        );
    } else {
        assert_eq!(
            replayed_webhook_writes(&side),
            (6, 6),
            "three at connect and three by the late registration, all removed"
        );
        side.replay.as_ref().expect("replay").assert_no_misses();
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_replacements_of_the_same_keys_never_both_apply(pool: PgPool) {
    let side = StoreSide::open("flow_m").await;
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let (connection, first_secret) = connect_via_routes(&app, &seller, &side).await;
    let slow = minted(&side, "read_write", "keys_slow");
    let quick = minted(&side, "read_write", "keys_quick");
    let path = format!("/v1/connectors/{connection}/credentials");

    side.delay_next("probe", std::time::Duration::from_secs(5));
    let slow_one = spawn_replace(&app, &seller.token, connection, &slow);
    side.wait_until_held("probe", 2).await;

    let (status, body) = api(&app, "PUT", &path, Some(&seller.token), &quick).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let winner_secret = webhook_secret(&app, connection).await;
    assert_ne!(winner_secret, first_secret);

    // The slow one verified its keys against the credential it read at the start;
    // that is no longer the stored one, so it writes nothing.
    let (status, body) = slow_one.await.expect("replace finished");
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("credentials_changed")),
        "{body}"
    );
    assert_eq!(webhook_secret(&app, connection).await, winner_secret);
    assert_eq!(
        webhook_status_at(&side, &app, connection, &winner_secret).await,
        StatusCode::OK
    );
    assert_eq!(
        webhook_status_at(&side, &app, connection, &first_secret).await,
        StatusCode::UNAUTHORIZED
    );
    if side.is_live() {
        let hooks = live_hooks(&side).await;
        let ours = hooks_at(&hooks, connection);
        assert_eq!(ours.len(), 3);
        for (id, _, _) in &ours {
            assert_eq!(
                side.live().webhook_secret(*id),
                winner_secret,
                "the store's webhooks carry the stored secret"
            );
        }
    } else {
        assert_eq!(replayed_webhook_writes(&side), (6, 3));
        side.replay.as_ref().expect("replay").assert_no_misses();
    }
}
