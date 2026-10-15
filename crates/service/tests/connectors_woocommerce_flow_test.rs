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
                topic == "order.updated" && body["id"].to_string() == external && body["status"] == "completed"
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
        woocommerce_support::WEBHOOK_PATH,
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
                    && body["id"].to_string() == external
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
            |topic, body| topic == "order.deleted" && body["id"].to_string() == external,
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
                    && body["id"].to_string() == external
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
                    && body["id"].to_string() == external
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
        (status, again["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("not_pending"))
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
