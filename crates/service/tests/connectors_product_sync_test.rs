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
use common::shopify_fake::{FakeShopify, Fault, ACCESS_TOKEN, GOOD_CODE};
use common::{execute, new_actor, register_command, register_listing_command, TestActor, TestApp};
use hmac::{Hmac, Mac};
use marketplace_service::connectors::product_sync;
use marketplace_service::connectors::provider::{
    CatalogProduct, ConnectionContext, ConnectorProvider, CredentialSecret, ProviderRegistry,
};
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

    /// The product as the store has it now, read the way the worker reads it.
    async fn store_product(&self, product: &str) -> CatalogProduct {
        let provider = ShopifyProvider::with_origin(Origin::Fixed(self.fake.base_url.clone()));
        let context = ConnectionContext {
            connection_id: self.connection,
            seller_pubky: self.seller.pubky.clone(),
            shop: SHOP.into(),
            scopes: Vec::new(),
            credentials: CredentialSecret::new(ACCESS_TOKEN.as_bytes().to_vec()),
            app: None,
        };
        provider
            .fetch_product(&context, product)
            .await
            .expect("read")
            .expect("on sale")
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
    let kept: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM connector_product_changes \
         WHERE connection_id = $1 AND provider_product_id = $2 AND field_group = 'price'",
    )
    .bind(r.connection)
    .bind(BOOTS)
    .fetch_one(&r.app.pool)
    .await
    .expect("count");
    assert_eq!(kept, 2, "the pending change and the latest resolution only");
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
async fn unlinking_forgets_the_product_its_changes_and_its_listings_overrides(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    let (status, _) = r
        .call(
            "PUT",
            "/sync/listings/boots_01",
            &json!({ "groups": { "price": "off" } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    let changes = r.changes().await;
    let old = only(&changes, "content").clone();

    let (status, body) = r.call("PUT", "/mappings", &json!({ "mappings": [] })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(r.changes().await, Vec::<Value>::new());
    let (status, body) = r.resolve(&old, "applied").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the change went with the link: {body}"
    );
    let (_, view) = r.call("GET", "/sync", &json!(null)).await;
    assert_eq!(
        view["overrides"],
        json!([]),
        "the listing links nothing any more"
    );
    let rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM connector_product_snapshots WHERE connection_id = $1 \
                   AND (content IS NOT NULL OR price IS NOT NULL OR variants IS NOT NULL \
                        OR status IS NOT NULL OR unlinked_at IS NULL)) \
              + (SELECT COUNT(*) FROM connector_product_changes WHERE connection_id = $1)",
    )
    .bind(r.connection)
    .fetch_one(&r.app.pool)
    .await
    .expect("count");
    assert_eq!(rows, 0, "no base and no change; only the unlink's fence");

    map_listing(
        &r.app,
        &r.seller,
        r.connection,
        "boots_01",
        BOOTS,
        BOOTS_VARIANT,
    )
    .await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "no stale proposal comes back"
    );
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "relinked, the product is based afresh"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_read_older_than_the_products_last_read_is_dropped(pool: PgPool) {
    let r = rig(pool).await;
    let slow_read_started = marketplace_service::clock::Clock::now(&*r.app.clock);
    r.app.clock.advance_seconds(10);
    r.poll().await;
    // A read that started before that poll finishes after it, carrying
    // values that differ from the store's in every group.
    let stale = CatalogProduct {
        provider_product_id: BOOTS.into(),
        handle: None,
        title: "Old Boots".into(),
        description: None,
        url: None,
        options: vec![],
        image_urls: vec![],
        variants: vec![],
        categories: vec![],
        attributes: vec![],
    };
    let observed = product_sync::observe(&r.app.pool, slow_read_started, r.connection, &stale)
        .await
        .expect("observe");
    assert!(observed.stale, "{observed:?}");
    assert_eq!(r.changes().await, Vec::<Value>::new());
    let fresh = product_sync::observe(
        &r.app.pool,
        marketplace_service::clock::Clock::now(&*r.app.clock),
        r.connection,
        &stale,
    )
    .await
    .expect("observe");
    assert!(!fresh.stale);
    assert_eq!(
        fresh.queued, 3,
        "the same values from a current read are changes"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_delete_delivery_does_not_override_a_newer_read(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.app.clock.advance_seconds(5);
    r.webhook("evt-late-delete", "products/delete", &json!({ "id": 9001 }))
        .await;
    r.app.clock.advance_seconds(5);
    // The store sells the product again (the delivery is older than this read).
    r.poll().await;
    r.pass().await;
    assert_eq!(r.changes().await, Vec::<Value>::new());

    // Delivered and applied before any newer read, it is queued.
    r.app.clock.advance_seconds(5);
    r.fake.lock().products_gone.insert(BOOTS.into(), None);
    r.webhook("evt-delete", "products/delete", &json!({ "id": 9001 }))
        .await;
    r.pass().await;
    let changes = r.changes().await;
    assert_eq!(
        only(&changes, "status")["proposed"],
        json!({ "available": false })
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_direction_change_waits_for_a_first_read_in_flight_and_drops_its_base(pool: PgPool) {
    let r = rig(pool).await;
    // A first observation of the boots, its bases written but not yet
    // committed.
    let mut first_read = r.app.pool.begin().await.expect("begin");
    sqlx::query(
        "INSERT INTO connector_product_snapshots \
         (connection_id, provider_product_id, content, price, variants, status, checked_at, \
          created_at) \
         VALUES ($1, $2, '{\"title\":\"Trail Boots\"}', '{}', '{}', \
                 '{\"available\":true}', now(), now()) \
         ON CONFLICT (connection_id, provider_product_id) DO UPDATE SET \
             content = EXCLUDED.content, price = EXCLUDED.price, \
             variants = EXCLUDED.variants, status = EXCLUDED.status, \
             checked_at = EXCLUDED.checked_at",
    )
    .bind(r.connection)
    .bind(BOOTS)
    .execute(&mut *first_read)
    .await
    .expect("insert");
    let router = r.app.router.clone();
    let token = r.seller.token.clone();
    let connection = r.connection;
    let turning_off = tokio::spawn(async move {
        common::send(
            router,
            "PUT",
            &format!("/v1/connectors/{connection}/sync"),
            Some(&token),
            &json!({ "groups": { "content": "off" } }),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !turning_off.is_finished(),
        "the direction change waits for the first read's snapshot"
    );
    first_read.commit().await.expect("commit");
    let (status, body) = turning_off.await.expect("task");
    assert_eq!(status, StatusCode::OK, "{body}");
    let content: Option<Value> = sqlx::query_scalar(
        "SELECT content FROM connector_product_snapshots \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(r.connection)
    .bind(BOOTS)
    .fetch_one(&r.app.pool)
    .await
    .expect("snapshot");
    assert_eq!(content, None, "the base the first read wrote was dropped");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_read_that_started_before_a_direction_change_cannot_base_the_dropped_group(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.sync(json!({ "content": "off" })).await;
    r.app.clock.advance_seconds(5);
    // A read starts while content is off and is delayed.
    let slow_read_started = marketplace_service::clock::Clock::now(&*r.app.clock);
    let old = r.store_product(BOOTS).await;
    r.app.clock.advance_seconds(5);
    // Meanwhile the store changes and content is turned back on.
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.sync(json!({ "content": "from_store" })).await;
    let late = product_sync::observe(&r.app.pool, slow_read_started, r.connection, &old)
        .await
        .expect("observe");
    assert!(
        !late.stale,
        "newer than the last read, so not dropped as a whole"
    );
    assert_eq!(
        late.based, 0,
        "but it may not base the group the change dropped"
    );
    r.app.clock.advance_seconds(5);
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "the store as it stood after the change is the base, not a change"
    );
    r.edit(BOOTS, json!({ "title": "Trail Boots III" }));
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(only(&changes, "content")["base"]["title"], "Trail Boots II");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_override_change_racing_an_unlink_never_deadlocks(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    let mappings = json!({ "mappings": [
        { "listing_id": "boots_01", "provider_product_id": BOOTS,
          "provider_variant_id": BOOTS_VARIANT },
    ] });
    for round in 0..12 {
        let (status, body) = r.call("PUT", "/mappings", &mappings).await;
        assert_eq!(status, StatusCode::OK, "relink {round}: {body}");
        r.poll().await;
        let direction = if round % 2 == 0 { "off" } else { "from_store" };
        let override_body = json!({ "groups": { "price": direction } });
        let unlink_body = json!({ "mappings": [] });
        let (overridden, unlinked) = tokio::join!(
            r.call("PUT", "/sync/listings/boots_01", &override_body),
            r.call("PUT", "/mappings", &unlink_body),
        );
        assert_eq!(unlinked.0, StatusCode::OK, "round {round}: {}", unlinked.1);
        assert!(
            matches!(overridden.0, StatusCode::OK | StatusCode::NOT_FOUND),
            "round {round}: either side may go first, neither may fail: {} {}",
            overridden.0,
            overridden.1
        );
        let (_, view) = r.call("GET", "/sync", &json!(null)).await;
        assert_eq!(
            view["overrides"],
            json!([]),
            "round {round}: an override left behind by the race went with the unlink"
        );
    }
}

impl Rig {
    /// Links the mug listing to the boots' second variant, so both listings
    /// share one store product.
    async fn link_mug_to_boots(&self) {
        let (status, body) = self
            .call(
                "PUT",
                "/mappings",
                &json!({ "mappings": [
                    { "listing_id": "boots_01", "provider_product_id": BOOTS,
                      "provider_variant_id": BOOTS_VARIANT },
                    { "listing_id": "mug_01", "provider_product_id": BOOTS,
                      "provider_variant_id": BOOTS_SECOND_VARIANT },
                ] }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    async fn override_groups(&self, listing: &str, groups: Value) {
        let (status, body) = self
            .call(
                "PUT",
                &format!("/sync/listings/{listing}"),
                &json!({ "groups": groups }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        marketplace_service::clock::Clock::now(&*self.app.clock)
    }
}

const BOOTS_SECOND_VARIANT: &str = "gid://shopify/ProductVariant/50002";

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn turning_one_listing_off_keeps_the_change_another_listing_of_the_product_follows(
    pool: PgPool,
) {
    let r = rig(pool).await;
    r.link_mug_to_boots().await;
    r.poll().await;
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    let changes = r.changes().await;
    let pending = only(&changes, "content").clone();

    r.override_groups("boots_01", json!({ "content": "off" }))
        .await;
    let changes = r.changes().await;
    assert_eq!(
        only(&changes, "content")["id"],
        pending["id"],
        "the mug still follows the store's content"
    );
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    let kept = only(&changes, "content");
    assert_eq!(kept["id"], pending["id"]);
    assert_eq!(kept["base"]["title"], "Trail Boots", "the base was kept");

    // Back on for the boots: the product was followed throughout.
    r.override_groups("boots_01", json!({ "content": null }))
        .await;
    assert_eq!(only(&r.changes().await, "content")["id"], pending["id"]);

    // Off for both: nothing follows the content any more.
    r.override_groups("boots_01", json!({ "content": "off" }))
        .await;
    r.override_groups("mug_01", json!({ "content": "off" }))
        .await;
    assert_eq!(r.changes().await, Vec::<Value>::new());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn turning_the_connection_off_keeps_the_change_a_listing_override_follows(pool: PgPool) {
    let r = rig(pool).await;
    r.link_mug_to_boots().await;
    r.override_groups("mug_01", json!({ "content": "from_store" }))
        .await;
    r.poll().await;
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.poll().await;
    let changes = r.changes().await;
    let pending = only(&changes, "content").clone();

    r.sync(json!({ "content": "off" })).await;
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    let kept = only(&changes, "content");
    assert_eq!(
        kept["id"], pending["id"],
        "the mug's own direction still follows the store"
    );
    assert_eq!(kept["base"]["title"], "Trail Boots");

    r.override_groups("mug_01", json!({ "content": null }))
        .await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "no listing of the product follows the content now"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_read_from_before_an_unlink_is_dropped_after_the_product_is_linked_again(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.app.clock.advance_seconds(5);
    // A read starts while the boots are linked and is delayed.
    let slow_read_started = r.now();
    let old = r.store_product(BOOTS).await;
    r.app.clock.advance_seconds(5);
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    let (status, body) = r.call("PUT", "/mappings", &json!({ "mappings": [] })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    map_listing(
        &r.app,
        &r.seller,
        r.connection,
        "boots_01",
        BOOTS,
        BOOTS_VARIANT,
    )
    .await;

    let late = product_sync::observe(&r.app.pool, slow_read_started, r.connection, &old)
        .await
        .expect("observe");
    assert!(late.stale, "a read of the previous link: {late:?}");
    let late_removal =
        product_sync::observe_removed(&r.app.pool, slow_read_started, r.connection, BOOTS)
            .await
            .expect("observe");
    assert!(late_removal.stale, "{late_removal:?}");
    assert_eq!(r.changes().await, Vec::<Value>::new());

    r.app.clock.advance_seconds(5);
    r.poll().await;
    assert_eq!(
        r.changes().await,
        Vec::<Value>::new(),
        "the store as it stands after the relink is the base"
    );
    r.edit(BOOTS, json!({ "title": "Trail Boots III" }));
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(only(&changes, "content")["base"]["title"], "Trail Boots II");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_direction_change_in_one_group_leaves_a_read_valid_for_another(pool: PgPool) {
    let r = rig(pool).await;
    r.poll().await;
    r.sync(json!({ "content": "off" })).await;
    r.app.clock.advance_seconds(5);
    r.sync(json!({ "content": "from_store" })).await;
    r.app.clock.advance_seconds(5);
    // A read starts after content was turned back on and is delayed.
    let read_started = r.now();
    let product = r.store_product(BOOTS).await;
    r.app.clock.advance_seconds(5);
    r.sync(json!({ "price": "off" })).await;

    let late = product_sync::observe(&r.app.pool, read_started, r.connection, &product)
        .await
        .expect("observe");
    assert!(!late.stale, "{late:?}");
    assert_eq!(
        late.based, 1,
        "content is based by a read that started after its own change; price is not"
    );
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    let content = only(&changes, "content");
    assert_eq!(content["base"]["title"], "Trail Boots");
    assert_eq!(content["proposed"]["title"], "Trail Boots II");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_removal_is_queued_although_a_direction_changed_after_it_was_seen(pool: PgPool) {
    let r = rig(pool).await;
    r.fake.lock().products_gone.insert(BOOTS.into(), None);
    // The delivery saying the product is gone arrives first ...
    let delivered = r.now();
    r.app.clock.advance_seconds(5);
    // ... then a direction change, before the worker applies it.
    r.sync(json!({ "content": "off" })).await;
    let observed = product_sync::observe_removed(&r.app.pool, delivered, r.connection, BOOTS)
        .await
        .expect("observe");
    assert_eq!(observed.queued, 1, "{observed:?}");
    let changes = r.changes().await;
    assert_eq!(
        only(&changes, "status")["proposed"],
        json!({ "available": false })
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0097_gives_each_group_the_shared_rebase_instant_and_reruns(pool: PgPool) {
    const MIGRATION_0097: &str =
        include_str!("../migrations/0097_connector_product_sync_fences.sql");
    let r = rig(pool).await;
    r.link_mug().await;
    r.poll().await;
    // A product linked before 0097 may have no snapshot yet.
    sqlx::query(
        "DELETE FROM connector_product_snapshots \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(r.connection)
    .bind(MUG)
    .execute(&r.app.pool)
    .await
    .expect("no mug snapshot");
    let shared = r.now() + chrono::Duration::seconds(30);
    let own = r.now() + chrono::Duration::seconds(60);
    sqlx::query(
        "UPDATE connector_product_snapshots \
         SET rebased_at = $3, content_rebased_at = NULL, price_rebased_at = $4, \
             variants_rebased_at = NULL \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(r.connection)
    .bind(BOOTS)
    .bind(shared)
    .bind(own)
    .execute(&r.app.pool)
    .await
    .expect("a row from before 0097");
    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION_0097)
            .execute(&r.app.pool)
            .await
            .expect("0097 runs");
    }
    type Instants = (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let instants: Instants = sqlx::query_as(
        "SELECT content_rebased_at, price_rebased_at, variants_rebased_at, unlinked_at \
         FROM connector_product_snapshots WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(r.connection)
    .bind(BOOTS)
    .fetch_one(&r.app.pool)
    .await
    .expect("snapshot");
    assert_eq!(instants, (Some(shared), Some(own), Some(shared), None));
    let mug_based: Option<bool> = sqlx::query_scalar(
        "SELECT content IS NOT NULL OR price IS NOT NULL OR variants IS NOT NULL \
                OR status IS NOT NULL \
         FROM connector_product_snapshots WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(r.connection)
    .bind(MUG)
    .fetch_optional(&r.app.pool)
    .await
    .expect("mug snapshot");
    assert_eq!(
        mug_based,
        Some(false),
        "a linked product gets an empty snapshot row"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_first_read_from_before_an_unlink_is_dropped_after_the_product_is_linked_again(
    pool: PgPool,
) {
    let r = rig(pool).await;
    // The boots' first read starts before the service has seen them and is
    // delayed past an unlink and a relink.
    let slow_read_started = r.now();
    let old = r.store_product(BOOTS).await;
    r.app.clock.advance_seconds(5);
    r.edit(BOOTS, json!({ "title": "Trail Boots II" }));
    let (status, body) = r.call("PUT", "/mappings", &json!({ "mappings": [] })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    map_listing(
        &r.app,
        &r.seller,
        r.connection,
        "boots_01",
        BOOTS,
        BOOTS_VARIANT,
    )
    .await;
    let late_removal =
        product_sync::observe_removed(&r.app.pool, slow_read_started, r.connection, BOOTS)
            .await
            .expect("observe");
    assert!(late_removal.stale, "{late_removal:?}");
    let late = product_sync::observe(&r.app.pool, slow_read_started, r.connection, &old)
        .await
        .expect("observe");
    assert!(late.stale, "a read of the previous link: {late:?}");
    assert_eq!(r.changes().await, Vec::<Value>::new());

    r.app.clock.advance_seconds(5);
    r.poll().await;
    r.edit(BOOTS, json!({ "title": "Trail Boots III" }));
    r.app.clock.advance_seconds(5);
    r.poll().await;
    let changes = r.changes().await;
    assert_eq!(only(&changes, "content")["base"]["title"], "Trail Boots II");
}
