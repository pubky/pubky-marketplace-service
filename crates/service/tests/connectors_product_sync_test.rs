//! Store product changes queued for the seller's Shop: a linked Shopify
//! product is based on first sight, a store edit becomes one pending change
//! per field group (a newer edit replaces its proposal), the Shop resolves it
//! with the proposal's hash, and directions per connection and per listing
//! decide what is queued. The store is the local fake in
//! `common/shopify_fake.rs`, which checks every request against Shopify's
//! published 2026-10 schema.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::connector_provider::{
    api, as_json, map_listing, query_param, raw, rebuild, save_credentials, start_connect,
    test_keys, test_settings,
};
use common::shopify_fake::{FakeShopify, Fault, GOOD_CODE};
use common::{execute, new_actor, register_command, register_listing_command, TestActor, TestApp};
use hmac::{Hmac, Mac};
use marketplace_service::connectors::provider::ProviderRegistry;
use marketplace_service::connectors::providers::shopify::client::Origin;
use marketplace_service::connectors::providers::shopify::ShopifyProvider;
use marketplace_service::connectors::{worker, ConnectorRuntime};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use uuid::Uuid;

const SHOP: &str = "dezentral.myshopify.com";
const CLIENT_ID: &str = "dev-dashboard-client-id";
const APP_SECRET: &str = "shpss_merchants_own_app_secret";
const BOOTS: &str = "gid://shopify/Product/9001";
const BOOTS_VARIANT: &str = "gid://shopify/ProductVariant/50001";
const MUG: &str = "gid://shopify/Product/9002";
const MUG_VARIANT: &str = "gid://shopify/ProductVariant/50010";

struct Rig {
    app: TestApp,
    fake: FakeShopify,
    seller: TestActor,
    connection: Uuid,
}

fn signed_query(secret: &str, timestamp: i64, extra: &[(&str, &str)]) -> String {
    let mut pairs: Vec<String> = vec![format!("shop={SHOP}"), format!("timestamp={timestamp}")];
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

/// An installed store with the boots listing linked to its first variant.
async fn rig(pool: PgPool) -> Rig {
    let fake = FakeShopify::start().await;
    fake.expect_client_secret(APP_SECRET);
    let provider = Arc::new(ShopifyProvider::with_origin(Origin::Fixed(
        fake.base_url.clone(),
    )));
    let runtime = ConnectorRuntime::new(
        test_keys(),
        ProviderRegistry::new().with(provider),
        test_settings(),
    );
    let base = common::test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(Arc::new(runtime)));
    let app = rebuild(base, state);
    let seller = new_actor(&app).await;
    let connection = start_connect(&app, &seller, SHOP).await;
    let (status, body) = save_credentials(&app, &seller, connection, CLIENT_ID, APP_SECRET).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let now = chrono::Utc::now().timestamp();
    let (status, headers, body) = raw(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/connectors/shopify/c/{connection}/launch?{}",
            signed_query(APP_SECRET, now, &[("host", "YWJj")])
        ),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER, "{}", as_json(&body));
    let location = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("authorize redirect")
        .to_string();
    let state_param = query_param(&location, "state");
    let (status, _, body) = raw(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/connectors/shopify/c/{connection}/oauth/callback?{}",
            signed_query(
                APP_SECRET,
                now,
                &[("code", GOOD_CODE), ("state", &state_param)]
            )
        ),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", as_json(&body));
    for command in [
        register_command(&seller.pubky, 3),
        register_listing_command(&seller.pubky, "mug_01", 3, 1),
    ] {
        let (status, body) = execute(&app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "register: {body}");
    }
    map_listing(&app, &seller, connection, "boots_01", BOOTS, BOOTS_VARIANT).await;
    Rig {
        app,
        fake,
        seller,
        connection,
    }
}

impl Rig {
    /// Links the mug listing too (a mapping save replaces the whole set).
    async fn link_mug(&self) {
        let (status, body) = self
            .call(
                "PUT",
                "/mappings",
                &json!({ "mappings": [
                    { "listing_id": "boots_01", "provider_product_id": BOOTS,
                      "provider_variant_id": BOOTS_VARIANT },
                    { "listing_id": "mug_01", "provider_product_id": MUG,
                      "provider_variant_id": MUG_VARIANT },
                ] }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    async fn poll(&self) {
        worker::poll_stock(&self.app.state).await.expect("poll");
    }

    async fn pass(&self) {
        worker::run_pass(&self.app.state).await.expect("pass");
    }

    async fn call(&self, method: &str, path: &str, body: &Value) -> (StatusCode, Value) {
        api(
            &self.app,
            method,
            &format!("/v1/connectors/{}{path}", self.connection),
            Some(&self.seller.token),
            body,
        )
        .await
    }

    async fn changes(&self) -> Vec<Value> {
        let (status, body) = self.call("GET", "/changes", &json!(null)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["kind"], "connector_product_changes");
        body["changes"].as_array().cloned().expect("changes")
    }

    async fn resolve(&self, change: &Value, resolution: &str) -> (StatusCode, Value) {
        self.call(
            "POST",
            &format!("/changes/{}/resolve", change["id"].as_str().expect("id")),
            &json!({
                "resolution": resolution,
                "proposed_hash": change["proposed_hash"],
            }),
        )
        .await
    }

    async fn sync(&self, groups: Value) -> Value {
        let (status, body) = self
            .call("PUT", "/sync", &json!({ "groups": groups }))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    fn edit(&self, product: &str, fields: Value) {
        self.fake
            .lock()
            .product_edits
            .insert(product.into(), fields);
    }

    fn price(&self, variant: &str, price: &str) {
        self.fake
            .lock()
            .variant_prices
            .insert(variant.into(), price.into());
    }

    async fn webhook(&self, event_id: &str, topic: &str, body: &Value) {
        let bytes = serde_json::to_vec(body).expect("body");
        let mut mac = Hmac::<Sha256>::new_from_slice(APP_SECRET.as_bytes()).expect("key");
        mac.update(&bytes);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        let (status, _, response) = raw(
            self.app.router.clone(),
            "POST",
            &format!("/v1/connectors/shopify/c/{}/webhooks", self.connection),
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
        assert_eq!(status, StatusCode::OK, "{}", as_json(&response));
    }
}

fn only<'a>(changes: &'a [Value], group: &str) -> &'a Value {
    let matching: Vec<&Value> = changes
        .iter()
        .filter(|change| change["field_group"] == group)
        .collect();
    assert_eq!(matching.len(), 1, "one {group} change: {changes:?}");
    matching[0]
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_edit_is_queued_once_replaced_by_a_newer_one_and_withdrawn_on_revert(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "first sight is the base"
    );

    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(changes.len(), 1, "{changes:?}");
    let first = only(&changes, "content");
    assert_eq!(first["provider_product_id"], BOOTS);
    assert_eq!(first["base"]["title"], "Trail Boots");
    assert_eq!(first["proposed"]["title"], "Trail Boots II");
    assert_eq!(first["proposed"]["image_urls"], first["base"]["image_urls"]);
    assert_eq!(
        first["listings"],
        json!([{
            "listing_id": "boots_01",
            "direction": "from_store",
            "variants": [{ "variant_id": "", "provider_variant_id": BOOTS_VARIANT }],
        }])
    );

    r.edit(BOOTS, json!({ "title": "Trail Boots III" }));
    r.poll().await;
    let changes = r.changes().await;
    let replaced = only(&changes, "content");
    assert_eq!(
        replaced["id"], first["id"],
        "the pending change is updated, not duplicated"
    );
    assert_eq!(replaced["base"]["title"], "Trail Boots");
    assert_eq!(replaced["proposed"]["title"], "Trail Boots III");
    assert_ne!(replaced["proposed_hash"], first["proposed_hash"]);

    r.fake.lock().product_edits.clear();
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "the store is back at the base"
    );
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn applying_moves_the_base_and_a_dismissed_value_is_not_raised_again(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;

    r.price(BOOTS_VARIANT, "130.00");
    r.poll().await;
    let changes = r.changes().await;
    let price = only(&changes, "price");
    let proposed: Vec<&Value> = price["proposed"]["variants"]
        .as_array()
        .expect("variants")
        .iter()
        .filter(|variant| variant["provider_variant_id"] == BOOTS_VARIANT)
        .collect();
    assert_eq!(proposed[0]["amount_minor"], 13000);
    assert_eq!(proposed[0]["currency"], "USD");

    let mut wrong = price.clone();
    wrong["proposed_hash"] = json!("00".repeat(32));
    let (status, body) = r.resolve(&wrong, "applied").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "change_stale");

    let (status, body) = r.resolve(price, "applied").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let (status, body) = r.resolve(price, "applied").await;
    assert_eq!(status, StatusCode::CONFLICT, "a resolved change: {body}");
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "the base is the applied price"
    );

    r.price(BOOTS_VARIANT, "140.00");
    r.poll().await;
    let changes = r.changes().await;
    let dismissed = only(&changes, "price").clone();
    assert_eq!(
        dismissed["base"]["variants"], price["proposed"]["variants"],
        "the new base is what was applied"
    );
    let (status, body) = r.resolve(&dismissed, "dismissed").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "a dismissed value stays dismissed"
    );

    r.price(BOOTS_VARIANT, "150.00");
    r.poll().await;
    let changes = r.changes().await;
    let again = only(&changes, "price");
    assert_ne!(again["id"], dismissed["id"]);
    assert_eq!(
        again["base"]["variants"], price["proposed"]["variants"],
        "dismissing kept the base"
    );

    let stranger = new_actor(&r.app).await;
    let (status, _) = api(
        &r.app,
        "POST",
        &format!(
            "/v1/connectors/{}/changes/{}/resolve",
            r.connection,
            again["id"].as_str().expect("id")
        ),
        Some(&stranger.token),
        &json!({ "resolution": "applied", "proposed_hash": again["proposed_hash"] }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "another seller's connection");
    let (status, body) = r
        .call(
            "POST",
            &format!("/changes/{}/resolve", Uuid::new_v4()),
            &json!({ "resolution": "applied", "proposed_hash": again["proposed_hash"] }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "change_not_found");
    let (status, body) = r
        .call(
            "POST",
            &format!("/changes/{}/resolve", again["id"].as_str().expect("id")),
            &json!({ "resolution": "merged", "proposed_hash": again["proposed_hash"] }),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_product_webhook_reads_the_product_again_and_a_covered_delivery_is_skipped(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.app.clock.advance_seconds(5);
    r.edit(BOOTS, json!({ "description": "Waterproof now." }));
    let body = json!({ "id": 9001, "admin_graphql_api_id": BOOTS, "title": "stale body" });
    // A burst: two deliveries arrive before the worker reads the product.
    r.webhook("evt-product-1", "products/update", &body).await;
    r.webhook("evt-product-2", "products/update", &body).await;
    r.app.clock.advance_seconds(5);
    r.pass().await;
    assert_eq!(
        r.fake.calls("Product").len(),
        1,
        "the second delivery is covered by the read made for the first"
    );
    let changes = r.changes().await;
    let content = only(&changes, "content");
    assert_eq!(content["proposed"]["description"], "Waterproof now.");
    assert_eq!(
        content["proposed"]["title"], "Trail Boots",
        "the store's product, not the delivery's body"
    );

    // A delivery received at the same instant as a read may describe a
    // change that read missed, so it reads again.
    r.webhook("evt-product-2b", "products/update", &body).await;
    r.pass().await;
    assert_eq!(r.fake.calls("Product").len(), 2);

    r.app.clock.advance_seconds(5);
    r.edit(BOOTS, json!({ "description": "Waterproof and warm." }));
    r.webhook("evt-product-3", "products/update", &body).await;
    r.pass().await;
    assert_eq!(r.fake.calls("Product").len(), 3);
    let changes = r.changes().await;
    assert_eq!(
        only(&changes, "content")["proposed"]["description"],
        "Waterproof and warm."
    );

    // A product linked to no listing is not read at all.
    r.webhook(
        "evt-product-4",
        "products/update",
        &json!({ "id": 9002, "admin_graphql_api_id": MUG }),
    )
    .await;
    r.pass().await;
    assert_eq!(r.fake.calls("Product").len(), 3);
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_product_the_store_stops_selling_is_queued_whatever_the_directions(pool: PgPool) {
    let r = rig(pool).await;
    r.link_mug().await;
    r.poll().await;
    r.sync(json!({ "content": "off", "price": "off", "variants": "off" }))
        .await;

    r.fake.lock().products_gone.insert(BOOTS.into(), None);
    r.webhook("evt-delete-1", "products/delete", &json!({ "id": 9001 }))
        .await;
    r.pass().await;
    let changes = r.changes().await;
    let status = only(&changes, "status");
    assert_eq!(status["provider_product_id"], BOOTS);
    assert_eq!(status["base"], json!({ "available": true }));
    assert_eq!(status["proposed"], json!({ "available": false }));
    assert_eq!(status["listings"][0]["direction"], "always");

    // The catalog walk finds an archived product missing once it reaches the
    // last page.
    r.fake
        .lock()
        .products_gone
        .insert(MUG.into(), Some("ARCHIVED"));
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(changes.len(), 2, "{changes:?}");
    assert!(changes
        .iter()
        .any(|change| change["provider_product_id"] == MUG
            && change["proposed"] == json!({ "available": false })));
    r.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_catalog_walk_cut_short_records_no_removal(pool: PgPool) {
    let r = rig(pool).await;
    r.link_mug().await;
    r.poll().await;
    r.fake.lock().products_gone.insert(MUG.into(), None);
    r.fake.fault("ProductsPage", vec![Fault::Status(500)]);
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "the walk proved nothing"
    );
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(only(&changes, "status")["provider_product_id"], MUG);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn turning_a_group_off_cancels_its_change_and_turning_it_on_takes_the_store_as_it_is(
    pool: PgPool,
) {
    let r = rig(pool).await;
    r.poll().await;
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    assert_eq!(r.changes().await.len(), 1);

    let view = r.sync(json!({ "content": "off" })).await;
    assert_eq!(view["kind"], "connector_sync");
    assert_eq!(
        view["groups"],
        json!([
            { "field_group": "content", "direction": "off", "default": "from_store" },
            { "field_group": "price", "direction": "from_store", "default": "from_store" },
            { "field_group": "variants", "direction": "from_store", "default": "from_store" },
        ])
    );
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "cancelled with the direction"
    );
    r.edit(BOOTS, json!({ "title": "Trail Boots III" }));
    r.poll().await;
    assert_eq!(r.changes().await, Vec::<Value>::new(), "off queues nothing");

    r.sync(json!({ "content": "from_store" })).await;
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "what changed while off is not replayed"
    );
    r.edit(BOOTS, json!({ "title": "Trail Boots IV" }));
    r.poll().await;
    let changes = r.changes().await;
    let content = only(&changes, "content");
    assert_eq!(content["base"]["title"], "Trail Boots III");
    assert_eq!(content["proposed"]["title"], "Trail Boots IV");

    let (status, body) = r
        .call("PUT", "/sync", &json!({ "groups": { "status": "off" } }))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "invalid_field_group");
    let (status, body) = r
        .call(
            "PUT",
            "/sync",
            &json!({ "groups": { "price": "to_store" } }),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "invalid_direction");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_override_wins_over_the_connection_in_both_directions(pool: PgPool) {
    let r = rig(pool).await;
    r.link_mug().await;
    r.poll().await;
    r.sync(json!({ "price": "off" })).await;
    let (status, view) = r
        .call(
            "PUT",
            "/sync/listings/mug_01",
            &json!({ "groups": { "price": "from_store" } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(
        view["overrides"],
        json!([{ "listing_id": "mug_01", "field_group": "price", "direction": "from_store" }])
    );
    let (status, _) = r
        .call(
            "PUT",
            "/sync/listings/boots_01",
            &json!({ "groups": { "content": "off" } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    // A changed direction takes the store as it stands at the next read.
    r.poll().await;
    assert_eq!(r.changes().await, Vec::<Value>::new());

    r.price(BOOTS_VARIANT, "130.00");
    r.price(MUG_VARIANT, "20.00");
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.edit(MUG, json!({ "title": "Enamel Mug XL" }));
    r.poll().await;
    let changes = r.changes().await;
    let summary: Vec<(String, String)> = changes
        .iter()
        .map(|change| {
            (
                change["provider_product_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                change["field_group"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (MUG.to_string(), "content".to_string()),
            (MUG.to_string(), "price".to_string()),
        ],
        "boots: price off for the connection, content off for the listing; mug: \
         price on for the listing, content follows the connection"
    );

    let (status, _) = r
        .call(
            "PUT",
            "/sync/listings/mug_01",
            &json!({ "groups": { "price": null } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let changes = r.changes().await;
    assert_eq!(only(&changes, "content")["provider_product_id"], MUG);
    assert!(
        changes
            .iter()
            .all(|change| change["field_group"] != "price"),
        "back to the connection's off: the price change is cancelled"
    );

    let (status, body) = r
        .call(
            "PUT",
            "/sync/listings/not_linked",
            &json!({ "groups": { "price": "off" } }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "listing_not_linked");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_resolve_racing_a_newer_store_value_never_loses_it(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    for round in 1..=12 {
        let applied_title = format!("Boots v{round}");
        let newer_title = format!("Boots v{round} newer");
        r.edit(BOOTS, json!({ "title": applied_title }));
        r.poll().await;
        let changes = r.changes().await;
        let change = only(&changes, "content").clone();
        assert_eq!(change["proposed"]["title"], applied_title);

        r.edit(BOOTS, json!({ "title": newer_title }));
        let (resolved, ()) = tokio::join!(r.resolve(&change, "applied"), r.poll());
        let changes = r.changes().await;
        let pending = only(&changes, "content");
        assert_eq!(
            pending["proposed"]["title"], newer_title,
            "round {round}: the newer store value is still pending"
        );
        match resolved.0 {
            StatusCode::NO_CONTENT => assert_eq!(
                pending["base"]["title"], applied_title,
                "round {round}: applied first, so the newer value is measured from it"
            ),
            StatusCode::CONFLICT => assert_eq!(
                pending["id"], change["id"],
                "round {round}: replaced first, so the stale resolve changed nothing"
            ),
            other => panic!("round {round}: {other} {}", resolved.1),
        }
        // Settle the round: apply what is pending now.
        let (status, body) = r.resolve(pending, "applied").await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        r.poll().await;
        assert_eq!(r.changes().await, Vec::<Value>::new());
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unlinked_product_drops_its_pending_changes(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    assert_eq!(r.changes().await.len(), 1);
    let (status, body) = r.call("PUT", "/mappings", &json!({ "mappings": [] })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "nothing to apply it to"
    );
}
