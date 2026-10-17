//! The Shopify provider inside the connector framework: a merchant's custom
//! app installs through launch and OAuth, paid Pubky orders reach the store
//! once, cancels and refunds are mirrored, and what the store reports back
//! raises alerts instead of moving money. The store is the local fake in
//! `common/shopify_fake.rs`, which checks every request against Shopify's
//! published 2026-10 schema.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::StatusCode;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::connector_provider::{
    api, as_json, connection_for, map_listing, push_state, query_param, raw, rebuild,
    save_credentials, start_connect, test_keys, test_settings,
};
use common::shopify_fake::{FakeShopify, Fault, ACCESS_TOKEN, GOOD_CODE};
use common::{create_paid_order, execute, new_actor, order_command, TestActor, TestApp};
use hmac::{Hmac, Mac};
use marketplace_service::connectors::provider::ProviderRegistry;
use marketplace_service::connectors::providers::shopify::client::Origin;
use marketplace_service::connectors::providers::shopify::ShopifyProvider;
use marketplace_service::connectors::stock::{StockOutcome, VariantInventory, VariantStockRequest};
use marketplace_service::connectors::{worker, ConnectorRuntime};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

const SHOP: &str = "dezentral.myshopify.com";
const CLIENT_ID: &str = "dev-dashboard-client-id";
const APP_SECRET: &str = "shpss_merchants_own_app_secret";
const PRODUCT: &str = "gid://shopify/Product/9001";
const VARIANT: &str = "gid://shopify/ProductVariant/50001";

struct Rig {
    app: TestApp,
    fake: FakeShopify,
}

async fn rig_with(pool: PgPool, variant_inventory: Option<Arc<dyn VariantInventory>>) -> Rig {
    let fake = FakeShopify::start().await;
    fake.expect_client_secret(APP_SECRET);
    let provider = Arc::new(ShopifyProvider::with_origin(Origin::Fixed(
        fake.base_url.clone(),
    )));
    let mut runtime = ConnectorRuntime::new(
        test_keys(),
        ProviderRegistry::new().with(provider),
        test_settings(),
    );
    if let Some(inventory) = variant_inventory {
        runtime = runtime.with_variant_inventory(inventory);
    }
    let base = common::test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(Arc::new(runtime)));
    Rig {
        app: rebuild(base, state),
        fake,
    }
}

async fn rig(pool: PgPool) -> Rig {
    rig_with(pool, None).await
}

fn signed_query(secret: &str, shop: &str, timestamp: i64, extra: &[(&str, &str)]) -> String {
    let mut pairs: Vec<String> = vec![format!("shop={shop}"), format!("timestamp={timestamp}")];
    pairs.extend(extra.iter().map(|(key, value)| format!("{key}={value}")));
    pairs.sort();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("key");
    mac.update(pairs.join("&").as_bytes());
    format!(
        "{}&hmac={}",
        pairs.join("&"),
        hex::encode(mac.finalize().into_bytes())
    )
}

impl Rig {
    /// Shopify stamps its signed requests with real time, not the test clock.
    fn now(&self) -> i64 {
        chrono::Utc::now().timestamp()
    }

    async fn launch(&self, connection: Uuid, secret: &str) -> (StatusCode, Option<String>, Value) {
        let (status, headers, body) = raw(
            self.app.router.clone(),
            "GET",
            &format!(
                "/v1/connectors/shopify/c/{connection}/launch?{}",
                signed_query(secret, SHOP, self.now(), &[("host", "YWJj")])
            ),
            &[],
            Vec::new(),
        )
        .await;
        let location = headers
            .get("location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        (status, location, as_json(&body))
    }

    async fn callback(
        &self,
        connection: Uuid,
        secret: &str,
        state: &str,
        code: &str,
    ) -> (StatusCode, Value) {
        let (status, _, body) = raw(
            self.app.router.clone(),
            "GET",
            &format!(
                "/v1/connectors/shopify/c/{connection}/oauth/callback?{}",
                signed_query(
                    secret,
                    SHOP,
                    self.now(),
                    &[("code", code), ("state", state)]
                )
            ),
            &[],
            Vec::new(),
        )
        .await;
        (status, as_json(&body))
    }

    /// The merchant's whole install: connect request, saved app credentials,
    /// Shopify's launch to the App URL, and the OAuth callback.
    async fn install(&self, seller: &TestActor) -> Uuid {
        let id = start_connect(&self.app, seller, SHOP).await;
        let (status, body) = save_credentials(&self.app, seller, id, CLIENT_ID, APP_SECRET).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, location, body) = self.launch(id, APP_SECRET).await;
        assert_eq!(status, StatusCode::SEE_OTHER, "{body}");
        let state = query_param(&location.expect("authorize redirect"), "state");
        let (status, body) = self.callback(id, APP_SECRET, &state, GOOD_CODE).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        id
    }

    async fn webhook(
        &self,
        connection: Uuid,
        event_id: &str,
        topic: &str,
        body: &Value,
        secret: &str,
    ) -> (StatusCode, Value) {
        let bytes = serde_json::to_vec(body).expect("body");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("key");
        mac.update(&bytes);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        let (status, _, response) = raw(
            self.app.router.clone(),
            "POST",
            &format!("/v1/connectors/shopify/c/{connection}/webhooks"),
            &[
                ("content-type", "application/json"),
                ("x-shopify-hmac-sha256", &signature),
                ("x-shopify-topic", topic),
                ("x-shopify-event-id", event_id),
                ("x-shopify-shop-domain", SHOP),
            ],
            bytes,
        )
        .await;
        (status, as_json(&response))
    }

    async fn pass(&self) {
        worker::run_pass(&self.app.state).await.expect("pass");
    }

    async fn alerts(&self, seller: &TestActor) -> Vec<Value> {
        let (_, body) = api(
            &self.app,
            "GET",
            "/v1/connectors/alerts?unresolved=true",
            Some(&seller.token),
            &json!(null),
        )
        .await;
        body["alerts"].as_array().cloned().unwrap_or_default()
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_merchant_installs_their_own_custom_app_and_the_store_is_wired_up(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let id = r.install(&seller).await;

    let token_request = r.fake.lock().token_requests[0].clone();
    assert_eq!(token_request["code"], GOOD_CODE);
    assert_eq!(token_request["client_id"], CLIENT_ID);
    assert_eq!(token_request["client_secret"], APP_SECRET);

    let (status, view) = api(
        &r.app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    let connection = &view["connections"][0];
    assert_eq!(connection["status"], "connected");
    assert_eq!(connection["missing_scopes"], json!([]));
    assert_eq!(connection["variant_mode"], "native");
    let rendered = view.to_string();
    for secret in [ACCESS_TOKEN, APP_SECRET] {
        assert!(!rendered.contains(secret), "{secret} left the service");
    }

    // The six operational topics were subscribed to this connection's own
    // webhooks URL.
    let url = format!("https://service.test/v1/connectors/shopify/c/{id}/webhooks");
    let mut topics: Vec<String> = r
        .fake
        .lock()
        .webhooks
        .iter()
        .filter(|(_, _, uri)| *uri == url)
        .map(|(_, topic, _)| topic.clone())
        .collect();
    topics.sort();
    assert_eq!(
        topics,
        [
            "APP_UNINSTALLED",
            "FULFILLMENTS_CREATE",
            "FULFILLMENTS_UPDATE",
            "INVENTORY_ITEMS_UPDATE",
            "INVENTORY_LEVELS_UPDATE",
            "ORDERS_CANCELLED",
            "REFUNDS_CREATE"
        ]
    );

    let (status, check) = api(
        &r.app,
        "GET",
        &format!("/v1/connectors/{id}/check"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{check}");
    assert_eq!(check["shop_name"], "Dezentral Goods");
    assert_eq!(check["location_count"], 1);
    assert_eq!(check["catalog_readable"], true);

    let (status, catalog) = api(
        &r.app,
        "GET",
        &format!("/v1/connectors/{id}/catalog"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{catalog}");
    assert_eq!(catalog["products"][0]["provider_product_id"], PRODUCT);
    assert_eq!(
        catalog["products"][0]["variants"]
            .as_array()
            .expect("variants")
            .len(),
        3
    );
    assert_eq!(
        catalog["products"][0]["variants"][0]["price"]["amount_minor"],
        12500
    );
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_grant_missing_scopes_needs_attention_and_names_them(pool: PgPool) {
    let r = rig(pool).await;
    r.fake.lock().granted_scopes = vec!["read_products".into(), "write_orders".into()];
    let seller = new_actor(&r.app).await;
    let id = r.install(&seller).await;
    let (_, view) = api(
        &r.app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    let connection = view["connections"]
        .as_array()
        .expect("connections")
        .iter()
        .find(|connection| connection["id"] == id.to_string())
        .expect("connection");
    assert_eq!(connection["status"], "needs_attention");
    assert_eq!(
        connection["missing_scopes"],
        json!(["read_inventory", "read_locations", "read_fulfillments"])
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn launch_and_callback_must_be_signed_with_this_connections_app_secret(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let id = start_connect(&r.app, &seller, SHOP).await;
    save_credentials(&r.app, &seller, id, CLIENT_ID, APP_SECRET).await;

    let (status, _, body) = r.launch(id, "someone-elses-secret").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let (status, location, body) = r.launch(id, APP_SECRET).await;
    assert_eq!(status, StatusCode::SEE_OTHER, "{body}");
    let location = location.expect("redirect");
    assert!(location.starts_with(&format!("https://{SHOP}/admin/oauth/authorize?")));
    assert_eq!(query_param(&location, "client_id"), CLIENT_ID);
    assert_eq!(
        query_param(&location, "scope"),
        "read_products,read_inventory,read_locations,read_fulfillments,write_orders"
    );
    assert!(!location.contains(APP_SECRET));
    let state = query_param(&location, "state");

    let (status, _) = r
        .callback(id, "someone-elses-secret", &state, GOOD_CODE)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        r.fake.lock().token_requests.is_empty(),
        "an unverified callback never reaches Shopify's token endpoint"
    );
    let (status, _) = r
        .callback(id, APP_SECRET, &state, "a-code-shopify-refuses")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = r.callback(id, APP_SECRET, &state, GOOD_CODE).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a failed exchange does not burn the install"
    );
}

struct Shop {
    r: Rig,
    seller: TestActor,
    buyer: TestActor,
    connection: Uuid,
    order_id: String,
    total_minor: i64,
}

/// An installed store, a listing mapped to a Shopify variant and one paid
/// order waiting to be pushed.
async fn shop(pool: PgPool) -> Shop {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    let order = create_paid_order(&r.app, &seller, &buyer).await;
    map_listing(&r.app, &seller, connection, "boots_01", PRODUCT, VARIANT).await;
    Shop {
        r,
        seller,
        buyer,
        connection,
        order_id: order.order_id,
        total_minor: order.total_minor,
    }
}

impl Shop {
    async fn push(&self) -> (String, Option<String>, Option<String>, i32) {
        push_state(&self.r.app.pool, &self.order_id).await
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paid_order_reaches_shopify_once_paid_and_without_the_buyer(pool: PgPool) {
    let s = shop(pool).await;
    s.r.pass().await;
    let (state, external, reason, _) = s.push().await;
    assert_eq!((state.as_str(), reason), ("created", None), "{external:?}");
    let orders = s.r.fake.orders();
    assert_eq!(orders.len(), 1);
    assert_eq!(external.as_deref(), Some(orders[0].id.as_str()));
    let input = &orders[0].input;
    assert_eq!(input["financialStatus"], "PAID");
    assert_eq!(input["lineItems"][0]["variantId"], VARIANT);
    assert!(input["tags"]
        .as_array()
        .expect("tags")
        .iter()
        .any(|tag| *tag == format!("pubky-order-{}", s.order_id)));
    let paid = input["transactions"][0]["amountSet"]["shopMoney"]["amount"]
        .as_str()
        .expect("amount");
    assert_eq!(
        paid,
        format!("{}.{:02}", s.total_minor / 100, s.total_minor % 100)
    );
    assert!(input.get("shippingAddress").is_none());
    let sent = format!("{:?}", s.r.fake.calls("OrderCreate"));
    assert!(
        !sent.contains(&s.buyer.pubky),
        "no buyer identity reaches the store"
    );
    assert!(!sent.contains(&s.seller.pubky));
    s.r.fake.assert_clean();

    s.r.app.clock.advance_seconds(700);
    s.r.pass().await;
    assert_eq!(
        s.r.fake.orders().len(),
        1,
        "a later pass pushes nothing more"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_lost_create_answer_is_found_through_the_lookup_not_created_twice(pool: PgPool) {
    let s = shop(pool).await;
    s.r.fake
        .fault("OrderCreate", vec![Fault::AppliedThenServerError]);
    s.r.fake.lock().search_lags = true;
    s.r.pass().await;
    assert_eq!(
        s.r.fake.orders().len(),
        1,
        "the store applied the first push"
    );
    assert_ne!(s.push().await.0, "created", "the answer was lost");
    s.r.app.clock.advance_seconds(700);
    s.r.pass().await;
    let (state, external, _, _) = s.push().await;
    assert_eq!(state, "created");
    assert_eq!(external.as_deref(), Some(s.r.fake.orders()[0].id.as_str()));
    assert_eq!(
        s.r.fake.orders().len(),
        1,
        "the recent-orders scan found it although the search index lags"
    );
    s.r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_in_another_currency_quarantines_the_push_with_an_alert(pool: PgPool) {
    let s = shop(pool).await;
    s.r.fake.lock().shop_currency = "EUR".into();
    s.r.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert!(reason.is_some());
    assert!(s.r.fake.orders().is_empty());
    assert_eq!(s.r.alerts(&s.seller).await.len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_cancel_cancels_the_store_order_with_restock_and_no_refund(pool: PgPool) {
    let s = shop(pool).await;
    s.r.pass().await;
    assert_eq!(s.push().await.0, "created");
    execute(
        &s.r.app,
        &s.buyer.token,
        &order_command(
            "order.cancel_request",
            &s.order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
    )
    .await;
    let (status, body) = execute(
        &s.r.app,
        &s.seller.token,
        &order_command("order.cancel_approve", &s.order_id, 3, json!({}), 1_402),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    s.r.pass().await;
    let calls = s.r.fake.calls("OrderCancel");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].variables["restock"], true);
    assert!(calls[0].variables.get("refundMethod").is_none());
    let order = &s.r.fake.orders()[0];
    assert!(order.cancelled && order.refunds.is_empty());

    // Shopify reports the cancel it just made back to the service: an echo,
    // not the merchant acting, so no alert.
    let body = json!({ "id": order.id.rsplit('/').next().expect("id").parse::<u64>().expect("number"),
        "cancel_reason": "other", "tags": format!("pubky, pubky-order-{}", s.order_id) });
    let (status, _) =
        s.r.webhook(
            s.connection,
            "evt-echo",
            "orders/cancelled",
            &body,
            APP_SECRET,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    worker::drive_inbound(&s.r.app.state, 10)
        .await
        .expect("drive");
    assert!(
        s.r.alerts(&s.seller).await.is_empty(),
        "an echo raises no alert"
    );
    s.r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_refund_is_recorded_in_the_store_once(pool: PgPool) {
    let s = shop(pool).await;
    s.r.pass().await;
    assert_eq!(s.push().await.0, "created");
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
            json!({ "reason": "wrong size", "requested_amount_minor": s.total_minor }),
            1_212,
        ),
        (true, "return.approve", 5, json!({}), 1_213),
        (true, "return.receive", 6, json!({}), 1_214),
        (
            true,
            "refund.record_external",
            7,
            json!({ "amount_minor": s.total_minor, "transaction_id": "bitcoin-tx-1" }),
            1_215,
        ),
    ] {
        let token = if seller {
            &s.seller.token
        } else {
            &s.buyer.token
        };
        let (status, body) = execute(
            &s.r.app,
            token,
            &order_command(kind, &s.order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }
    s.r.pass().await;
    let order = &s.r.fake.orders()[0];
    assert_eq!(order.refunds.len(), 1);
    let amount = order.refunds[0]["transactions"][0]["amount"]
        .as_str()
        .expect("amount")
        .to_string();
    assert_eq!(
        amount,
        format!("{}.{:02}", s.total_minor / 100, s.total_minor % 100)
    );
    sqlx::query("UPDATE connector_event_cursor SET sequence = 0 WHERE name = 'events'")
        .execute(&s.r.app.pool)
        .await
        .expect("rewind");
    s.r.app.clock.advance_seconds(700);
    s.r.pass().await;
    assert_eq!(
        s.r.fake.orders()[0].refunds.len(),
        1,
        "a replay refunds nothing twice"
    );
    s.r.fake.assert_clean();
}

fn list_available_sql() -> &'static str {
    "SELECT available_quantity FROM listings WHERE seller_pubky = $1 AND listing_id = 'boots_01'"
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_inventory_webhook_resolves_its_variant_and_sets_the_listings_stock(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    let buyer = new_actor(&r.app).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    map_listing(
        &r.app,
        &seller,
        connection,
        "boots_01",
        PRODUCT,
        "gid://shopify/ProductVariant/50003",
    )
    .await;

    let (status, body) = r
        .webhook(
            connection,
            "evt-inv-1",
            "inventory_levels/update",
            &json!({ "inventory_item_id": 4242, "location_id": 1001, "available": 1 }),
            APP_SECRET,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    let available: i64 = sqlx::query_scalar(list_available_sql())
        .bind(&seller.pubky)
        .fetch_one(&r.app.pool)
        .await
        .expect("listing");
    assert_eq!(
        available, 3,
        "the store's total for the variant, not the webhook's per-location figure"
    );
    let state: String = sqlx::query_scalar(
        "SELECT state FROM connector_inbound_events WHERE provider_event_id = 'evt-inv-1'",
    )
    .fetch_one(&r.app.pool)
    .await
    .expect("inbound row");
    assert_eq!(state, "applied");

    // An inventory item that is no mapped variant changes nothing.
    r.webhook(
        connection,
        "evt-inv-2",
        "inventory_levels/update",
        &json!({ "inventory_item_id": 1, "available": 99 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    let available: i64 = sqlx::query_scalar(list_available_sql())
        .bind(&seller.pubky)
        .fetch_one(&r.app.pool)
        .await
        .expect("listing");
    assert_eq!(available, 3);
    r.fake.assert_clean();
}

async fn listing_stock(r: &Rig, seller: &str) -> (i64, i64, i64, String, bool) {
    sqlx::query_as(
        "SELECT available_quantity, sold_quantity, total_quantity, state, made_to_order \
         FROM listings WHERE seller_pubky = $1 AND listing_id = 'boots_01'",
    )
    .bind(seller)
    .fetch_one(&r.app.pool)
    .await
    .expect("listing")
}

/// A second buyer's paid checkout of `quantity` units of `boots_01`.
async fn buy(r: &Rig, seller: &TestActor, quantity: i64, n: u64) -> String {
    let buyer = new_actor(&r.app).await;
    let revision: i64 = sqlx::query_scalar(
        "SELECT server_revision FROM listings WHERE seller_pubky = $1 AND listing_id = 'boots_01'",
    )
    .bind(&seller.pubky)
    .fetch_one(&r.app.pool)
    .await
    .expect("revision");
    let mut command =
        common::checkout_command_with_id(&seller.pubky, &common::indexed_command_id(0x6d70, n));
    command["payload"]["lines"][0]["expected_revision"] = json!(revision);
    command["payload"]["lines"][0]["quantity"] = json!(quantity);
    let (status, body) = execute(&r.app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        &r.app,
        &buyer.token,
        &common::payment_command(&payment_id, 1, "confirmed", 1, 6_000 + n),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    order_id
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_print_on_demand_variant_stays_orderable_and_flips_back_when_shopify_tracks_it(
    pool: PgPool,
) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    let first_buyer = new_actor(&r.app).await;
    create_paid_order(&r.app, &seller, &first_buyer).await;
    map_listing(
        &r.app,
        &seller,
        connection,
        "boots_01",
        PRODUCT,
        "gid://shopify/ProductVariant/50003",
    )
    .await;
    assert_eq!(
        listing_stock(&r, &seller.pubky).await,
        (0, 1, 1, "sold".to_string(), false),
        "the one unit imported is sold"
    );

    // The merchant's print-on-demand variant: Shopify does not track it.
    r.fake.lock().inventory_item_tracking = (false, "DENY");
    let (status, body) = r
        .webhook(
            connection,
            "evt-items-1",
            "inventory_items/update",
            &json!({ "id": 4242, "tracked": false,
                     "admin_graphql_api_id": "gid://shopify/InventoryItem/4242" }),
            APP_SECRET,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        listing_stock(&r, &seller.pubky).await,
        (0, 1, 1, "available".to_string(), true),
        "made to order: orderable again without inventing a count"
    );

    // Buyers keep ordering it; every sale still reaches Shopify.
    let second = buy(&r, &seller, 2, 1).await;
    let third = buy(&r, &seller, 3, 2).await;
    assert_eq!(
        listing_stock(&r, &seller.pubky).await,
        (0, 6, 6, "available".to_string(), true)
    );
    r.pass().await;
    for order in [&second, &third] {
        assert_eq!(push_state(&r.app.pool, order).await.0, "created", "{order}");
    }
    let quantities: Vec<i64> = r
        .fake
        .orders()
        .iter()
        .map(|order| {
            order.input["lineItems"][0]["quantity"]
                .as_i64()
                .expect("quantity")
        })
        .collect();
    assert_eq!(quantities, [1, 2, 3]);

    // A seller adjustment has no count to move.
    let revision: i64 = sqlx::query_scalar(
        "SELECT server_revision FROM listings WHERE seller_pubky = $1 AND listing_id = 'boots_01'",
    )
    .bind(&seller.pubky)
    .fetch_one(&r.app.pool)
    .await
    .expect("revision");
    let adjust = json!({
        "schema_version": 1,
        "kind": "inventory.adjust",
        "aggregate_id": common::listing_aggregate(&seller.pubky),
        "listing_id": "boots_01",
        "expected_revision": revision,
        "delta": 5,
        "idempotency_key": "00000000-0000-4000-8000-00000000d701",
    });
    let (status, body) = common::send(
        r.app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(&seller.token),
        &adjust,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "stock_not_tracked");

    // The merchant turns tracking on with 3 in stock: the next level counts.
    r.fake.lock().inventory_item_tracking = (true, "DENY");
    r.webhook(
        connection,
        "evt-levels-1",
        "inventory_levels/update",
        &json!({ "inventory_item_id": 4242, "location_id": 1001, "available": 3 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        listing_stock(&r, &seller.pubky).await,
        (3, 6, 9, "available".to_string(), false),
        "Shopify's count is on sale again; the sold units stay sold"
    );

    // Selling on at zero (CONTINUE) is made to order too.
    r.fake.lock().inventory_item_tracking = (true, "CONTINUE");
    r.webhook(
        connection,
        "evt-items-2",
        "inventory_items/update",
        &json!({ "id": 4242, "tracked": true }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        listing_stock(&r, &seller.pubky).await,
        (0, 6, 6, "available".to_string(), true)
    );
    // A replay of the same state changes nothing.
    r.webhook(
        connection,
        "evt-items-3",
        "inventory_items/update",
        &json!({ "id": 4242, "tracked": true }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    let outcome: Option<String> = sqlx::query_scalar(
        "SELECT outcome FROM connector_inbound_events WHERE provider_event_id = 'evt-items-3'",
    )
    .fetch_one(&r.app.pool)
    .await
    .expect("inbound row");
    assert_eq!(outcome.as_deref(), Some("unchanged"));
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn webhooks_need_the_connections_secret_and_are_deduplicated_by_event_id(pool: PgPool) {
    let s = shop(pool).await;
    let body = json!({ "inventory_item_id": 1 });
    let (status, _) =
        s.r.webhook(
            s.connection,
            "evt-bad",
            "inventory_levels/update",
            &body,
            "wrong",
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    for _ in 0..2 {
        let (status, _) =
            s.r.webhook(
                s.connection,
                "evt-dup",
                "inventory_levels/update",
                &body,
                APP_SECRET,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM connector_inbound_events WHERE provider_event_id = 'evt-dup'",
    )
    .fetch_one(&s.r.app.pool)
    .await
    .expect("count");
    assert_eq!(count, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_cancel_raises_an_alert_whose_action_is_the_existing_pubky_flow(pool: PgPool) {
    let s = shop(pool).await;
    s.r.pass().await;
    let store_order = s.r.fake.orders()[0].id.clone();
    let number: u64 = store_order
        .rsplit('/')
        .next()
        .expect("id")
        .parse()
        .expect("number");
    let cancelled = |event: &str| json!({ "id": number, "cancel_reason": "customer", "tags": format!("pubky, pubky-order-{}", s.order_id), "event": event });

    // A paid order has no seller cancel command: the alert shows, with no
    // one-click action, and nothing is cancelled.
    s.r.webhook(
        s.connection,
        "evt-c1",
        "orders/cancelled",
        &cancelled("1"),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&s.r.app.state, 10)
        .await
        .expect("drive");
    let alerts = s.r.alerts(&s.seller).await;
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["kind"], "merchant_cancelled");
    assert_eq!(alerts[0]["pubky_order_id"], s.order_id);
    assert_eq!(alerts[0]["action"], Value::Null);
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&s.order_id)
        .fetch_one(&s.r.app.pool)
        .await
        .expect("order");
    assert_eq!(
        state, "paid",
        "a store cancel never cancels the Pubky order"
    );

    // Once the buyer has asked to cancel, the alert carries the one-click
    // approval; it is not taken automatically.
    execute(
        &s.r.app,
        &s.buyer.token,
        &order_command(
            "order.cancel_request",
            &s.order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_501,
        ),
    )
    .await;
    s.r.webhook(
        s.connection,
        "evt-c2",
        "orders/cancelled",
        &cancelled("2"),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&s.r.app.state, 10)
        .await
        .expect("drive");
    let alerts = s.r.alerts(&s.seller).await;
    let with_action = alerts
        .iter()
        .find(|alert| alert["action"] != Value::Null)
        .expect("an alert with an action");
    assert_eq!(with_action["action"]["kind"], "approve_cancellation");
    assert_eq!(with_action["action"]["command"], "order.cancel_approve");
    assert_eq!(with_action["action"]["order_id"], s.order_id);
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&s.order_id)
        .fetch_one(&s.r.app.pool)
        .await
        .expect("order");
    assert_eq!(state, "cancel_requested");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_refund_on_a_cancelled_order_offers_to_record_the_refund(pool: PgPool) {
    let s = shop(pool).await;
    s.r.pass().await;
    let store_order = s.r.fake.orders()[0].id.clone();
    let number: u64 = store_order
        .rsplit('/')
        .next()
        .expect("id")
        .parse()
        .expect("number");
    execute(
        &s.r.app,
        &s.buyer.token,
        &order_command(
            "order.cancel_request",
            &s.order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_601,
        ),
    )
    .await;
    execute(
        &s.r.app,
        &s.seller.token,
        &order_command("order.cancel_approve", &s.order_id, 3, json!({}), 1_602),
    )
    .await;
    let body = json!({ "id": 5, "order_id": number, "transactions": [
        { "kind": "refund", "status": "success", "amount": "137.00", "currency": "USD" } ] });
    s.r.webhook(s.connection, "evt-r1", "refunds/create", &body, APP_SECRET)
        .await;
    worker::drive_inbound(&s.r.app.state, 10)
        .await
        .expect("drive");
    let alerts = s.r.alerts(&s.seller).await;
    let refund = alerts
        .iter()
        .find(|alert| alert["kind"] == "merchant_refunded")
        .expect("refund alert");
    assert_eq!(refund["action"]["kind"], "record_refund");
    assert_eq!(refund["action"]["command"], "refund.record_external");
    let recorded: Option<Value> =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1::uuid")
            .bind(&s.order_id)
            .fetch_one(&s.r.app.pool)
            .await
            .expect("order");
    assert_eq!(recorded, None, "no refund is recorded automatically");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn uninstalling_the_app_from_shopify_deletes_the_credential(pool: PgPool) {
    let s = shop(pool).await;
    s.r.webhook(
        s.connection,
        "evt-u1",
        "app/uninstalled",
        &json!({ "id": 1 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&s.r.app.state, 10)
        .await
        .expect("drive");
    let rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM connector_credentials WHERE connection_id = $1")
            .bind(s.connection)
            .fetch_one(&s.r.app.pool)
            .await
            .expect("count");
    assert_eq!(rows, 0);
    let _ = connection_for(&s.r.app, SHOP).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_privacy_webhooks_are_acknowledged_with_the_apps_secret(pool: PgPool) {
    let s = shop(pool).await;
    for (id, topic) in [
        ("evt-p1", "customers/data_request"),
        ("evt-p2", "customers/redact"),
        ("evt-p3", "shop/redact"),
    ] {
        let (status, body) =
            s.r.webhook(
                s.connection,
                id,
                topic,
                &json!({ "shop_domain": SHOP }),
                APP_SECRET,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{topic}: {body}");
    }
    let (status, _) =
        s.r.webhook(
            s.connection,
            "evt-p4",
            "shop/redact",
            &json!({ "shop_domain": SHOP }),
            "wrong",
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// === Variant mode ============================================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn split_mode_maps_each_store_variant_to_its_own_listing(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    let settings = format!("/v1/connectors/{connection}/settings");
    let mappings = format!("/v1/connectors/{connection}/mappings");
    let two_variants = json!({ "mappings": [
        { "listing_id": "boots_01", "variant_id": "a", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50001" },
        { "listing_id": "boots_01", "variant_id": "b", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50002" },
    ] });
    let (status, body) = api(&r.app, "PUT", &mappings, Some(&seller.token), &two_variants).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "native mode allows several variants per listing: {body}"
    );

    let (status, body) = api(
        &r.app,
        "PUT",
        &settings,
        Some(&seller.token),
        &json!({ "variant_mode": "split" }),
    )
    .await;
    // No circular refusal: the switch unmaps the combined listing, says so in
    // the response and in an alert, and the seller re-imports or ends it.
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["connection"]["variant_mode"], "split");
    assert_eq!(
        body["unmapped_listings"],
        json!([{ "listing_id": "boots_01", "mappings_removed": 2 }])
    );
    assert_eq!(body["connection"]["mapping_count"], 0);
    let alerts = r.alerts(&seller).await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["kind"], "listings_unmapped");
    let detail = alerts[0]["detail"].as_str().expect("detail");
    assert!(
        detail.contains("boots_01") && detail.contains("re-import"),
        "{detail}"
    );
    let (_, listed) = api(&r.app, "GET", &mappings, Some(&seller.token), &json!(null)).await;
    assert_eq!(listed["mappings"], json!([]));
    // Switching again with nothing combined unmaps nothing and raises nothing more.
    let (status, again) = api(
        &r.app,
        "PUT",
        &settings,
        Some(&seller.token),
        &json!({ "variant_mode": "split" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["unmapped_listings"], json!([]));
    assert_eq!(r.alerts(&seller).await.len(), 1);
    let (status, body) = api(&r.app, "PUT", &mappings, Some(&seller.token), &two_variants).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "split_mode_listing_shared");
    let one_each = json!({ "mappings": [
        { "listing_id": "boots_01", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50001" },
    ] });
    let (status, body) = api(&r.app, "PUT", &mappings, Some(&seller.token), &one_each).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = api(
        &r.app,
        "PUT",
        &settings,
        Some(&seller.token),
        &json!({ "variant_mode": "bogus" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// listing, Pubky variant, available, event id
type Seen = (String, String, i64, Option<String>);

#[derive(Default)]
struct Recording {
    seen: Mutex<Vec<Seen>>,
}

#[async_trait]
impl VariantInventory for Recording {
    async fn apply_variant_stock(
        &self,
        _state: &AppState,
        request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error> {
        self.seen.lock().expect("lock").push((
            request.mapping.listing_id.clone(),
            request.mapping.variant_id.clone(),
            request.available,
            request.event_id.map(str::to_string),
        ));
        Ok(StockOutcome::Applied { delta: 0 })
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn native_mode_stock_for_a_multi_variant_listing_goes_through_the_variant_interface(
    pool: PgPool,
) {
    let recording = Arc::new(Recording::default());
    let r = rig_with(pool, Some(recording.clone())).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    let (status, body) = api(&r.app, "PUT", &format!("/v1/connectors/{connection}/mappings"), Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": "a", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50003" },
            { "listing_id": "boots_01", "variant_id": "b", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50002" },
        ] })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    r.webhook(
        connection,
        "evt-v1",
        "inventory_levels/update",
        &json!({ "inventory_item_id": 4242 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    assert_eq!(
        recording.seen.lock().expect("lock").clone(),
        vec![(
            "boots_01".to_string(),
            "a".to_string(),
            3,
            Some("evt-v1".to_string())
        )]
    );
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_per_variant_inventory_a_multi_variant_listing_is_left_alone(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    api(&r.app, "PUT", &format!("/v1/connectors/{connection}/mappings"), Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": "a", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50003" },
            { "listing_id": "boots_01", "variant_id": "b", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50002" },
        ] })).await;
    let before: i64 = sqlx::query_scalar(list_available_sql())
        .bind(&seller.pubky)
        .fetch_one(&r.app.pool)
        .await
        .expect("listing");
    r.webhook(
        connection,
        "evt-v2",
        "inventory_levels/update",
        &json!({ "inventory_item_id": 4242 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    let after: i64 = sqlx::query_scalar(list_available_sql())
        .bind(&seller.pubky)
        .fetch_one(&r.app.pool)
        .await
        .expect("listing");
    assert_eq!(before, after);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_connection_says_how_to_install_the_app_in_the_dev_dashboard(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let id = start_connect(&r.app, &seller, SHOP).await;
    let (status, saved) = save_credentials(&r.app, &seller, id, CLIENT_ID, APP_SECRET).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    for connection in [
        &saved["connection"],
        &api(
            &r.app,
            "GET",
            "/v1/connectors",
            Some(&seller.token),
            &json!(null),
        )
        .await
        .1["connections"][0],
    ] {
        // Shopify's docs do not say the authorize URL installs an app the
        // store does not have yet, so there is no service-built link: the
        // Dev Dashboard path is returned instead.
        assert_eq!(connection["install_url"], Value::Null);
        let install = &connection["install"];
        assert_eq!(install["method"], "dev_dashboard");
        assert!(install["where"]
            .as_str()
            .expect("where")
            .contains("Installs card"));
        assert!(install["where"]
            .as_str()
            .expect("where")
            .contains("Generate link"));
        assert_eq!(install["app_url"], connection["urls"]["launch"]);
        assert_eq!(
            install["redirect_url"],
            connection["urls"]["oauth_callback"]
        );
        assert!(!install.to_string().contains(APP_SECRET));
    }
    let (status, _) = api(
        &r.app,
        "DELETE",
        &format!("/v1/connectors/{id}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

// === Per-variant stock through the seam ======================================

/// A seam implementation that always answers a stale mapping.
struct StaleMapping;

#[async_trait]
impl VariantInventory for StaleMapping {
    async fn apply_variant_stock(
        &self,
        _state: &AppState,
        _request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error> {
        Ok(StockOutcome::MappingError("variant_assertion_mismatch"))
    }
}

async fn two_variant_mapping(r: &Rig, seller: &TestActor, connection: Uuid, first_variant: &str) {
    let (status, body) = api(
        &r.app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": first_variant, "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50003" },
            { "listing_id": "boots_01", "variant_id": "other", "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50002" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn inventory_event(r: &Rig, connection: Uuid, id: &str) -> (String, Option<String>) {
    r.webhook(
        connection,
        id,
        "inventory_levels/update",
        &json!({ "inventory_item_id": 4242 }),
        APP_SECRET,
    )
    .await;
    worker::drive_inbound(&r.app.state, 10)
        .await
        .expect("drive");
    sqlx::query_as(
        "SELECT state, outcome FROM connector_inbound_events WHERE provider_event_id = $1",
    )
    .bind(id)
    .fetch_one(&r.app.pool)
    .await
    .expect("inbound row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_stale_variant_mapping_settles_as_an_error_alerts_once_and_is_never_retried(
    pool: PgPool,
) {
    let r = rig_with(pool, Some(Arc::new(StaleMapping))).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    two_variant_mapping(&r, &seller, connection, "a").await;
    for id in ["evt-m1", "evt-m2"] {
        let (state, outcome) = inventory_event(&r, connection, id).await;
        assert_eq!(state, "ignored", "a permanent mapping error is not retried");
        assert_eq!(
            outcome.as_deref(),
            Some("variant_mapping_error: variant_assertion_mismatch")
        );
    }
    let attempts: i32 = sqlx::query_scalar("SELECT MAX(attempts) FROM connector_inbound_events")
        .fetch_one(&r.app.pool)
        .await
        .expect("attempts");
    assert_eq!(attempts, 1);
    let alerts = r.alerts(&seller).await;
    assert_eq!(
        alerts.len(),
        1,
        "one alert per variant, however many events: {alerts:?}"
    );
    assert_eq!(alerts[0]["kind"], "variant_mapping_error");
    assert!(alerts[0]["detail"]
        .as_str()
        .expect("detail")
        .contains("Re-map"));
}

/// Converts the listing to per-variant rows the way migration 0070's tables
/// hold them: one keyed row per variant replaces the implicit row, and the
/// listing's committed units stay on the first variant so the roll-up keeps
/// the listing's totals balanced.
async fn convert_to_variant_rows(r: &Rig, aggregate: &str, rows: &[(&str, i64)]) {
    let mut tx = r.app.pool.begin().await.expect("tx");
    let (reserved, sold): (i64, i64) = sqlx::query_as(
        "SELECT reserved_quantity, sold_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(aggregate)
    .fetch_one(&mut *tx)
    .await
    .expect("listing");
    for (index, (variant, available)) in rows.iter().enumerate() {
        let (held, gone) = if index == 0 { (reserved, sold) } else { (0, 0) };
        sqlx::query(
            "INSERT INTO listing_variant_inventory (listing_aggregate_id, variant_id, position, \
             total_quantity, available_quantity, reserved_quantity, sold_quantity) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(aggregate)
        .bind(variant)
        .bind(index as i32)
        .bind(available + held + gone)
        .bind(available)
        .bind(held)
        .bind(gone)
        .execute(&mut *tx)
        .await
        .expect("variant row");
    }
    sqlx::query(
        "DELETE FROM listing_variant_inventory WHERE listing_aggregate_id = $1 AND variant_id = ''",
    )
    .bind(aggregate)
    .execute(&mut *tx)
    .await
    .expect("drop the implicit row");
    tx.commit().await.expect("commit");
}

async fn one_variant_mapping(r: &Rig, seller: &TestActor, connection: Uuid, variant: &str) {
    let (status, body) = api(
        &r.app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "variant_id": variant, "provider_product_id": PRODUCT, "provider_variant_id": "gid://shopify/ProductVariant/50003" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn one_mapped_variant_on_a_listing_that_tracks_variants_moves_that_variant_only(
    pool: PgPool,
) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    let aggregate = format!("listing:{}_boots_01", seller.pubky);
    convert_to_variant_rows(&r, &aggregate, &[("a", 3), ("other", 9)]).await;

    // Only size `a` is mapped. The store says 3 and `a` shows 3: nothing is
    // sent, although the listing as a whole has 12 available.
    one_variant_mapping(&r, &seller, connection, "a").await;
    let (state, outcome) = inventory_event(&r, connection, "evt-one-same").await;
    assert_eq!(
        (state.as_str(), outcome.as_deref()),
        ("ignored", Some("unchanged"))
    );

    // `a` drops to 1: the change is judged against `a`, and the other size
    // is untouched whatever the service answers.
    sqlx::query(
        "UPDATE listing_variant_inventory SET available_quantity = available_quantity - 2, \
         total_quantity = total_quantity - 2 WHERE listing_aggregate_id = $1 AND variant_id = 'a'",
    )
    .bind(&aggregate)
    .execute(&r.app.pool)
    .await
    .expect("less stock");
    let (state, outcome) = inventory_event(&r, connection, "evt-one-adjust").await;
    assert_ne!(state, "pending", "{outcome:?}");
    assert!(
        !outcome
            .as_deref()
            .is_some_and(|text| text.starts_with("variant_mapping_error")),
        "{outcome:?}"
    );
    let other: i64 = sqlx::query_scalar(
        "SELECT available_quantity FROM listing_variant_inventory \
         WHERE listing_aggregate_id = $1 AND variant_id = 'other'",
    )
    .bind(&aggregate)
    .fetch_one(&r.app.pool)
    .await
    .expect("other size");
    assert_eq!(other, 9);

    // A single mapping that names no variant cannot be addressed on such a
    // listing, and says so instead of moving the listing total.
    one_variant_mapping(&r, &seller, connection, "").await;
    let (_, outcome) = inventory_event(&r, connection, "evt-one-empty").await;
    assert_eq!(
        outcome.as_deref(),
        Some("variant_mapping_error: variant_required")
    );
    assert!(r
        .alerts(&seller)
        .await
        .iter()
        .all(|alert| alert["kind"] == "variant_mapping_error"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_default_seam_reads_the_variants_own_available_and_refuses_what_it_cannot_address(
    pool: PgPool,
) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let buyer = new_actor(&r.app).await;
    let connection = r.install(&seller).await;
    create_paid_order(&r.app, &seller, &buyer).await;
    two_variant_mapping(&r, &seller, connection, "a").await;

    // A listing that has not converted to per-variant rows yet holds only its
    // implicit row: it is skipped, as before, and the seller gets no alert.
    let (state, outcome) = inventory_event(&r, connection, "evt-skip").await;
    assert_eq!(
        (state.as_str(), outcome.as_deref()),
        ("ignored", Some("multi_variant_listing"))
    );
    assert!(r.alerts(&seller).await.is_empty());

    // With them: the store says 3 and the variant already shows 3, so nothing is sent.
    let aggregate = format!("listing:{}_boots_01", seller.pubky);
    convert_to_variant_rows(&r, &aggregate, &[("a", 3), ("other", 9)]).await;
    let (state, outcome) = inventory_event(&r, connection, "evt-same").await;
    assert_eq!(
        (state.as_str(), outcome.as_deref()),
        ("ignored", Some("unchanged"))
    );

    // A mapping to a variant the listing no longer has is a mapping error.
    two_variant_mapping(&r, &seller, connection, "gone").await;
    let (state, outcome) = inventory_event(&r, connection, "evt-gone").await;
    assert_eq!(state, "ignored");
    assert_eq!(
        outcome.as_deref(),
        Some("variant_mapping_error: variant_not_found")
    );

    // A mapping with no Pubky variant id cannot be addressed at all.
    two_variant_mapping(&r, &seller, connection, "").await;
    let (_, outcome) = inventory_event(&r, connection, "evt-empty").await;
    assert_eq!(
        outcome.as_deref(),
        Some("variant_mapping_error: variant_required")
    );
    let kinds: Vec<String> = r
        .alerts(&seller)
        .await
        .iter()
        .filter_map(|alert| alert["kind"].as_str().map(str::to_string))
        .collect();
    assert_eq!(kinds, ["variant_mapping_error", "variant_mapping_error"]);

    // A change is judged against that variant's available (3), not the
    // listing's: the target 3 moves the variant by the difference. The
    // adjustment goes to the service, which answers; whatever it says the
    // event settles and does not loop.
    two_variant_mapping(&r, &seller, connection, "a").await;
    sqlx::query(
        "UPDATE listing_variant_inventory SET available_quantity = available_quantity - 2, \
         total_quantity = total_quantity - 2 WHERE variant_id = 'a'",
    )
    .execute(&r.app.pool)
    .await
    .expect("less stock");
    let (state, outcome) = inventory_event(&r, connection, "evt-adjust").await;
    assert_ne!(state, "pending", "{outcome:?}");
}
