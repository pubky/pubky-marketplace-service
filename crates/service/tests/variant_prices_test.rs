//! Per-variant prices (migration 0075): a variant whose record carries a
//! `priceOverride` sells at that price, so checkout charges what the Shop
//! shows in the cart, and a listing that sells everything at one price is
//! untouched. Every request drives the real router, executor and PostgreSQL;
//! the interleaving tests race real requests.

mod common;

use axum::http::StatusCode;
use common::{
    execute, indexed_command_id, listing_aggregate, new_actor, register_command, send,
    sync_command, test_app, test_app_with_homeserver, TestActor, TestApp,
};
use serde_json::{json, Value};
use sqlx::PgPool;

const BASE: i64 = 12_500;
const SHIPPING: i64 = 1_200;
const LARGE: i64 = 15_000;

fn money(amount_minor: i64) -> Value {
    json!({ "amount_minor": amount_minor, "currency": "USD", "exponent": 2 })
}

/// A `listing.register` of size S, M and L at `listing_revision`; L (and M
/// when `medium` is set) sell at their own price.
fn register_sizes(
    seller: &str,
    listing_revision: i64,
    expected_revision: i64,
    large: Option<i64>,
    medium: Option<i64>,
) -> Value {
    let mut command = register_command(seller, 9);
    command["command_id"] = json!(indexed_command_id(0x7000, listing_revision as u64));
    command["expected_revision"] = json!(expected_revision);
    command["payload"]["listing_revision"] = json!(listing_revision);
    let size = |id: &str, price: Option<i64>| {
        let mut variant = json!({ "id": id, "quantity": 3 });
        if let Some(price) = price {
            variant["unit_price"] = money(price);
        }
        variant
    };
    command["payload"]["variants"] = json!([
        size("size_s", None),
        size("size_m", medium),
        size("size_l", large)
    ]);
    command
}

fn checkout(seller: &str, n: u64, variant: &str, expected_revision: i64, quantity: i64) -> Value {
    let mut command = common::checkout_command_with_id(seller, &indexed_command_id(0x7100, n));
    let line = &mut command["payload"]["lines"][0];
    line["expected_revision"] = json!(expected_revision);
    line["quantity"] = json!(quantity);
    line["variant_id"] = json!(variant);
    command
}

async fn revision(pool: &PgPool, seller: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

async fn stored_prices(pool: &PgPool, seller: &str) -> Option<Value> {
    sqlx::query_scalar("SELECT variant_prices FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

/// The order a checkout of `quantity` units of `variant` creates:
/// `(total, line unit price, line subtotal)` in minor units.
async fn buy(
    app: &TestApp,
    buyer: &TestActor,
    seller: &str,
    n: u64,
    variant: &str,
    quantity: i64,
) -> (i64, i64, i64) {
    let current = revision(&app.pool, seller).await;
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout(seller, n, variant, current, quantity),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order = &body["result"]["orders"][0];
    let line = &order["lines"][0];
    (
        order["total"]["amount_minor"].as_i64().expect("total"),
        line["unit_price"]["amount_minor"].as_i64().expect("unit"),
        line["subtotal"]["amount_minor"].as_i64().expect("subtotal"),
    )
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_charges_each_variants_own_price(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 1, "size_s", 1).await,
        (BASE + SHIPPING, BASE, BASE),
        "an unpriced size sells at the listing price"
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 2, "size_l", 1).await,
        (LARGE + SHIPPING, LARGE, LARGE),
        "a priced size sells at its own price"
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 3, "size_l", 2).await,
        (2 * LARGE + SHIPPING, LARGE, 2 * LARGE),
        "the price is per unit and shipping is charged once per line"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cheaper_variant_price_is_charged_too(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), Some(900)),
    )
    .await;
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 1, "size_m", 1).await,
        (900 + SHIPPING, 900, 900),
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_without_variant_prices_is_unchanged(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["result"]["listing"].get("variant_prices").is_none(),
        "the projection omits the key when every variant sells at the listing price: {body}"
    );
    assert_eq!(stored_prices(&app.pool, &seller.pubky).await, None);
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 1, "size_l", 1).await,
        (BASE + SHIPPING, BASE, BASE)
    );

    let mut plain = register_command(&seller.pubky, 2);
    plain["command_id"] = json!(indexed_command_id(0x7000, 90));
    plain["payload"]["listing_id"] = json!("plain_01");
    plain["aggregate_id"] = json!(format!("listing:{}_plain_01", seller.pubky));
    let (status, body) = execute(&app, &seller.token, &plain).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["result"]["listing"].get("variant_prices").is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_projection_serves_the_priced_variants_only(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["listing"]["variant_prices"],
        json!([{ "variant_id": "size_l", "unit_price": money(LARGE) }])
    );
    let (status, read) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{}", listing_aggregate(&seller.pubky)),
        Some(&new_actor(&app).await.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(
        read["variant_prices"],
        json!([{ "variant_id": "size_l", "unit_price": money(LARGE) }])
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_edit_reprices_the_variant_and_a_stale_checkout_is_refused(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), None),
    )
    .await;
    let seen = revision(&app.pool, &seller.pubky).await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 2, seen, Some(16_000), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, 1, "size_l", seen, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("REVISION_CONFLICT"),
        "a buyer who saw the old price is never charged the new one"
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 2, "size_l", 1).await,
        (16_000 + SHIPPING, 16_000, 16_000)
    );

    let now = revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 3, now, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        stored_prices(&app.pool, &seller.pubky).await,
        None,
        "dropping every override returns the listing to one price"
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 3, "size_l", 1).await,
        (BASE + SHIPPING, BASE, BASE)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_variant_price_in_another_asset_is_refused(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let mut command = register_sizes(&seller.pubky, 1, 0, Some(LARGE), None);
    command["payload"]["variants"][2]["unit_price"]["currency"] = json!("EUR");
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert!(
        body["error"]["issues"]
            .as_array()
            .expect("issues")
            .iter()
            .any(|issue| issue["path"] == json!("payload.variants.2.unit_price")),
        "{body}"
    );
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM listings").await,
        0,
        "a refused registration writes nothing"
    );
}

/// Buyers racing across sizes: every order is priced by the size it names,
/// whatever the interleaving, and no order mixes two sizes' prices.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_checkouts_across_sizes_are_each_priced_by_their_size(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), None),
    )
    .await;
    let mut buyers = Vec::new();
    for _ in 0..6 {
        buyers.push(new_actor(&app).await);
    }
    let mut handles = Vec::new();
    for (index, buyer) in buyers.into_iter().enumerate() {
        let router = app.router.clone();
        let seller_pubky = seller.pubky.clone();
        let variant = if index % 2 == 0 { "size_l" } else { "size_s" };
        handles.push(tokio::spawn(async move {
            // The listing revision moves with every checkout, so a loser
            // re-reads it from the conflict and retries.
            let mut expected = 1i64;
            for attempt in 0..50u64 {
                let command = checkout(
                    &seller_pubky,
                    (index as u64 + 1) * 100 + attempt,
                    variant,
                    expected,
                    1,
                );
                let (_, body) = send(
                    router.clone(),
                    "POST",
                    "/v1/commands",
                    Some(&buyer.token),
                    &command,
                )
                .await;
                if body["ok"] == json!(true) {
                    let line = &body["result"]["orders"][0]["lines"][0];
                    return (
                        variant,
                        line["unit_price"]["amount_minor"].as_i64().unwrap(),
                        body["result"]["orders"][0]["total"]["amount_minor"]
                            .as_i64()
                            .unwrap(),
                    );
                }
                match body["error"]["code"].as_str() {
                    Some("REVISION_CONFLICT") => {
                        expected = body["error"]["current_revision"]
                            .as_i64()
                            .expect("a conflict carries the current revision");
                    }
                    other => panic!("unexpected refusal {other:?}: {body}"),
                }
            }
            panic!("checkout did not settle in 50 attempts");
        }));
    }
    for handle in handles {
        let (variant, unit, total) = handle.await.expect("checkout task");
        let expected_unit = if variant == "size_l" { LARGE } else { BASE };
        assert_eq!(unit, expected_unit, "{variant}");
        assert_eq!(total, expected_unit + SHIPPING, "{variant}");
    }
}

/// A seller's reprice racing a buyer's checkout of the same size. Both orders
/// of the interleaving are legal; what is never legal is the new price under
/// the old revision, or a total that is neither price plus shipping.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_reprice_racing_a_checkout_never_charges_a_price_the_buyer_did_not_see(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_sizes(&seller.pubky, 1, 0, Some(LARGE), None),
    )
    .await;
    let mut buyers = Vec::new();
    for _ in 0..8 {
        buyers.push(new_actor(&app).await);
    }
    let seen = revision(&app.pool, &seller.pubky).await;

    let reprice = {
        let router = app.router.clone();
        let token = seller.token.clone();
        let command = register_sizes(&seller.pubky, 2, seen, Some(16_000), None);
        tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        })
    };
    let mut handles = Vec::new();
    for (index, buyer) in buyers.into_iter().enumerate() {
        let router = app.router.clone();
        let command = checkout(&seller.pubky, index as u64 + 1, "size_l", seen, 1);
        handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await
        }));
    }
    let (_, reprice_body) = reprice.await.expect("reprice task");
    assert_eq!(reprice_body["ok"], json!(true), "{reprice_body}");
    for handle in handles {
        let (_, body) = handle.await.expect("checkout task");
        if body["ok"] == json!(true) {
            let order = &body["result"]["orders"][0];
            assert_eq!(
                order["total"]["amount_minor"],
                json!(LARGE + SHIPPING),
                "a checkout that won the race charges the price the buyer saw: {body}"
            );
            assert_eq!(
                order["lines"][0]["unit_price"]["amount_minor"],
                json!(LARGE)
            );
        } else {
            assert_eq!(
                body["error"]["code"],
                json!("REVISION_CONFLICT"),
                "a checkout that lost the race is told the listing moved: {body}"
            );
        }
    }
    assert_eq!(
        stored_prices(&app.pool, &seller.pubky).await,
        Some(json!([{ "variant_id": "size_l", "unit_price": money(16_000) }]))
    );
}

fn homeserver_record(revision: i64, large: Option<i64>) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "title": "Winter boots",
        "revision": revision,
        "media": [{
            "id": "media_01",
            "type": "image",
            "mimeType": "image/jpeg",
            "contentHash": "a".repeat(64),
            "byteSize": 1,
        }],
        "variants": [
            { "id": "size_s", "enabled": true, "quantity": 3, "sku": null, "priceOverride": null },
            {
                "id": "size_l",
                "enabled": true,
                "quantity": 3,
                "sku": null,
                "priceOverride": large.map(|amount| json!({
                    "amountMinor": amount, "currency": "USD", "exponent": 2
                })),
            },
        ],
        "shippingOptions": [{ "pricing": "free" }],
        "sale": {
            "acceptsOffers": false,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": BASE, "currency": "USD", "exponent": 2 },
        },
    })
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sync_derives_the_variant_prices_from_the_record(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(&seller.pubky, "boots_01", homeserver_record(1, Some(LARGE)));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["listing"]["variant_prices"],
        json!([{ "variant_id": "size_l", "unit_price": money(LARGE) }])
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 1, "size_l", 1).await,
        (LARGE, LARGE, LARGE)
    );
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 2, "size_s", 1).await,
        (BASE, BASE, BASE)
    );
}

/// A listing registered before the column existed holds no variant prices
/// while its record, at the same revision, carries them. An equal-revision
/// sync fills them in, moves the revision so a stale checkout is refused, and
/// touches neither stock nor the listing price.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_equal_revision_sync_heals_a_listing_registered_without_its_prices(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(&seller.pubky, "boots_01", homeserver_record(1, None));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(stored_prices(&app.pool, &seller.pubky).await, None);
    let before = revision(&app.pool, &seller.pubky).await;

    // The record at the same revision now prices size L.
    homeserver.put_record(&seller.pubky, "boots_01", homeserver_record(1, Some(LARGE)));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let after = revision(&app.pool, &seller.pubky).await;
    assert_eq!(after, before + 1, "the heal is a write of the listing");
    assert_eq!(
        stored_prices(&app.pool, &seller.pubky).await,
        Some(json!([{ "variant_id": "size_l", "unit_price": money(LARGE) }]))
    );
    let (total, available): (i64, i64) = sqlx::query_as(
        "SELECT total_quantity, available_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing totals");
    assert_eq!((total, available), (6, 6), "stock is untouched");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, 1, "size_l", before, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 2, "size_l", 1).await,
        (LARGE, LARGE, LARGE)
    );

    // Syncing the same record again changes nothing.
    let settled = revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(revision(&app.pool, &seller.pubky).await, settled);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sole_enabled_variant_keeps_its_price_on_a_single_variant_listing(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut record = homeserver_record(1, Some(LARGE));
    record["variants"] = json!([{
        "id": "size_l",
        "enabled": true,
        "quantity": 3,
        "sku": null,
        "priceOverride": { "amountMinor": LARGE, "currency": "USD", "exponent": 2 },
    }]);
    homeserver.put_record(&seller.pubky, "boots_01", record);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        buy(&app, &buyer, &seller.pubky, 1, "size_l", 1).await,
        (LARGE, LARGE, LARGE)
    );
}
