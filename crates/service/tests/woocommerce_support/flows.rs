//! The scenarios the WooCommerce tests run. Each is written once and runs in
//! two modes (see `StoreSide::open`): against the Docker WooCommerce while
//! recording, and against the replay of what it answered.

use axum::http::StatusCode;
use marketplace_service::connectors::provider::ConnectionContext;
use marketplace_service::connectors::worker;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::connector_provider::{api, as_json, raw};
use crate::common::{
    create_paid_order, execute, new_actor, order_command, PaidOrder, TestActor, TestApp,
};

use super::live::LiveStack;
use super::{post_delivery, secret_of, woo_app, StoreSide};

pub const TEST_PRODUCT: i64 = 39;
pub const SOCKS: i64 = 35;
pub const SOCKS_S: i64 = 36;

/// A known starting point in the live store before a flow: stock of the
/// products the flows use.
pub async fn reset_store(live: &LiveStack) {
    for (path, quantity) in [
        (format!("/wc/v3/products/{TEST_PRODUCT}"), 10),
        (format!("/wc/v3/products/{SOCKS}/variations/36"), 5),
        (format!("/wc/v3/products/{SOCKS}/variations/37"), 3),
        (format!("/wc/v3/products/{SOCKS}/variations/38"), 0),
    ] {
        live.rest(
            "PUT",
            &path,
            Some(json!({"manage_stock": true, "stock_quantity": quantity})),
        )
        .await;
    }
}

pub async fn read_catalog(app: &TestApp, seller: &TestActor, connection: Uuid) -> Vec<Value> {
    let mut products = Vec::new();
    let mut page: Option<String> = None;
    for _ in 0..20 {
        let uri = match &page {
            Some(token) => format!("/v1/connectors/{connection}/catalog?page={token}"),
            None => format!("/v1/connectors/{connection}/catalog"),
        };
        let (status, body) = api(app, "GET", &uri, Some(&seller.token), &json!(null)).await;
        assert_eq!(status, StatusCode::OK, "catalog: {body}");
        products.extend(
            body["products"]
                .as_array()
                .expect("products")
                .iter()
                .cloned(),
        );
        match body["next_page"].as_str() {
            Some(next) => page = Some(next.to_string()),
            None => return products,
        }
    }
    panic!("the catalog never ended");
}

/// The seller starts a WooCommerce connection, the merchant approves, and
/// WooCommerce posts the keys to the callback.
pub async fn connect_via_routes(
    app: &TestApp,
    seller: &TestActor,
    side: &StoreSide,
) -> (Uuid, String) {
    let (status, body) = api(
        app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({ "provider": "woocommerce", "domain": side.site() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "connect: {body}");
    assert_eq!(body["connection"]["status"], "draft");
    let id: Uuid = body["connection"]["id"]
        .as_str()
        .expect("id")
        .parse()
        .expect("uuid");
    let authorize =
        url::Url::parse(body["authorize_url"].as_str().expect("authorize_url")).expect("url");
    assert_eq!(authorize.path(), "/wc-auth/v1/authorize");
    let query: std::collections::BTreeMap<String, String> = authorize
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert_eq!(query["app_name"], "Pubky Shop");
    assert_eq!(query["scope"], "read_write");
    assert_eq!(query["return_url"], super::RETURN_URL);
    assert_eq!(query["callback_url"], super::CALLBACK_URL);
    assert!(query["user_id"].len() >= 40, "the nonce travels as user_id");

    let callback = side.approve(authorize.as_str(), "callback");
    let (status, _, bytes) = raw(
        app.router.clone(),
        "POST",
        "/v1/connectors/woocommerce/oauth/callback",
        &[("content-type", "application/json;charset=UTF-8")],
        callback,
    )
    .await;
    let connected = as_json(&bytes);
    assert_eq!(status, StatusCode::OK, "callback: {connected}");
    assert_eq!(connected["kind"], "connector_connected");
    assert_eq!(connected["connection"]["status"], "connected");
    assert_eq!(connected["connection"]["scopes"], json!(["read_write"]));
    let text = connected.to_string();
    assert!(
        !text.contains("ck_") && !text.contains("cs_"),
        "no key in a response"
    );
    let secret = webhook_secret(app, id).await;
    (id, secret)
}

pub async fn webhook_secret(app: &TestApp, connection: Uuid) -> String {
    let runtime = app.state.connectors.as_ref().expect("connectors enabled");
    let row = marketplace_service::connectors::store::get(&app.pool, connection)
        .await
        .expect("lookup")
        .expect("connection");
    let context: ConnectionContext = runtime
        .open(&app.pool, &row)
        .await
        .expect("credential opens");
    secret_of(&context.credentials)
}

pub async fn map_test_product(app: &TestApp, seller: &TestActor, connection: Uuid) {
    let (status, body) = api(
        app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [{
            "listing_id": "boots_01",
            "provider_product_id": TEST_PRODUCT.to_string(),
            "provider_variant_id": TEST_PRODUCT.to_string(),
            "sku": "PUBKY-TEST",
        }] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "map: {body}");
}

pub async fn push_state(app: &TestApp, order_id: &str) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT state, external_id FROM connector_order_push WHERE pubky_order_id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("push row")
}

pub async fn listing_available(app: &TestApp, seller: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT available_quantity FROM listings WHERE seller_pubky = $1 AND listing_id = 'boots_01'",
    )
    .bind(seller)
    .fetch_one(&app.pool)
    .await
    .expect("listing")
}

pub struct Shop {
    pub side: StoreSide,
    pub app: TestApp,
    pub seller: TestActor,
    pub buyer: TestActor,
    pub connection: Uuid,
    pub secret: String,
    pub order: PaidOrder,
    pub external: String,
}

/// Connects, maps the test product and pushes one paid order.
pub async fn pushed_shop(pool: PgPool, scenario: &str) -> Shop {
    let side = StoreSide::open(scenario).await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let app = woo_app(pool, side.provider(7)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (connection, secret) = connect_via_routes(&app, &seller, &side).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_test_product(&app, &seller, connection).await;
    worker::run_pass(&app.state).await.expect("pass");
    let (state, external) = push_state(&app, &order.order_id).await;
    assert_eq!(state, "created");
    Shop {
        side,
        app,
        seller,
        buyer,
        connection,
        secret,
        order,
        external: external.expect("external id"),
    }
}

pub async fn alerts(shop: &Shop) -> Vec<Value> {
    let (_, body) = api(
        &shop.app,
        "GET",
        "/v1/connectors/alerts",
        Some(&shop.seller.token),
        &json!(null),
    )
    .await;
    body["alerts"].as_array().expect("alerts").clone()
}

pub async fn inbound_outcomes(app: &TestApp) -> Vec<(String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT topic, state, outcome FROM connector_inbound_events ORDER BY received_at, id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("inbound rows")
}

pub async fn deliver_and_apply(
    shop: &Shop,
    delivery: &super::replay::Delivery,
) -> (StatusCode, Value) {
    let result = post_delivery(&shop.app, delivery).await;
    worker::run_pass(&shop.app.state).await.expect("pass");
    result
}

/// Pubky cancel (buyer asks, seller approves) at the revisions of a paid order.
pub async fn cancel_in_pubky(shop: &Shop) {
    let order_id = shop.order.order_id.as_str();
    let (status, body) = execute(
        &shop.app,
        &shop.buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &shop.app,
        &shop.seller.token,
        &order_command("order.cancel_approve", order_id, 3, json!({}), 1_402),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// Ship, deliver, return, approve, receive and record the refund, as the
/// framework's own refund test does.
pub async fn refund_in_pubky(shop: &Shop) {
    let order_id = shop.order.order_id.as_str();
    for (seller, kind, revision, payload, number) in [
        (
            true,
            "fulfillment.ship",
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "T-1" }),
            1_210,
        ),
        (false, "fulfillment.confirm_delivery", 3, json!({}), 1_211),
        (
            false,
            "return.request",
            4,
            json!({ "reason": "wrong size", "requested_amount_minor": shop.order.total_minor }),
            1_212,
        ),
        (true, "return.approve", 5, json!({}), 1_213),
        (true, "return.receive", 6, json!({}), 1_214),
        (
            true,
            "refund.record_external",
            7,
            json!({ "amount_minor": shop.order.total_minor, "transaction_id": "bitcoin-tx-1" }),
            1_215,
        ),
    ] {
        let token = if seller {
            &shop.seller.token
        } else {
            &shop.buyer.token
        };
        let (status, body) = execute(
            &shop.app,
            token,
            &order_command(kind, order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }
}

/// What a live store really holds for the order a push created.
pub async fn verify_live_order(
    live: &LiveStack,
    external: &str,
    pubky_order_id: &str,
    stock_after: i64,
) {
    let order = live
        .rest("GET", &format!("/wc/v3/orders/{external}"), None)
        .await;
    assert_eq!(order["status"], "processing", "created paid and processing");
    assert!(order["date_paid"].is_string(), "paid");
    assert_eq!(order["billing"]["email"], "", "no billing email");
    assert_eq!(order["billing"]["first_name"], "");
    assert!(order["customer_note"]
        .as_str()
        .expect("note")
        .contains(pubky_order_id));
    let meta = order["meta_data"].as_array().expect("meta");
    assert!(meta
        .iter()
        .any(|m| m["key"] == "pubky_order_id" && m["value"] == pubky_order_id));
    assert_eq!(order["line_items"][0]["product_id"], TEST_PRODUCT);
    let product = live
        .rest("GET", &format!("/wc/v3/products/{TEST_PRODUCT}"), None)
        .await;
    assert_eq!(
        product["stock_quantity"], stock_after,
        "WooCommerce took the stock"
    );
}

pub fn provider_for(
    side: &StoreSide,
) -> marketplace_service::connectors::providers::woocommerce::WooCommerceProvider {
    side.provider(7)
}
