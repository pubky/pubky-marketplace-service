//! Zone-priced shipping: a listing references a seller's shipping profile (or
//! carries its own zones) and the buyer's destination country and rate price
//! each order. Checkout, offers and auctions price from the record the seller
//! signed, pinned by the profile's SHA-256; a destination in a blocked region
//! is refused when the order is placed and again when its payment starts.
//! Every request drives the real router, executor and PostgreSQL, and the
//! profile and shop records are served by the fake homeserver over HTTP.

mod common;

use axum::http::StatusCode;
use marketplace_service::config::Config;
use marketplace_service::region_policy::RegionModes;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command_with_id, close_auction_command, count, counter_offer_command,
    create_offer_command, execute, execute_from, indexed_command_id, listing_aggregate, new_actor,
    offer_action, place_bid_command, reconfigured, register_auction_command, register_command,
    send, send_from, spawn_fake_homeserver, test_app, test_app_with_homeserver,
    test_app_with_pickup_and_paypal, test_app_without_homeserver, with_shop_homeserver,
    FakeHomeserver, TestActor, TestApp, OFFER_COMMAND_ID,
};

const PROFILE_ID: &str = "pf_world";
const LISTING_STOCK: i64 = 50;

fn flat(rate_id: &str, label: &str, amount_minor: i64) -> Value {
    json!({
        "pricing": "flat",
        "rateId": rate_id,
        "label": label,
        "amountMinor": amount_minor,
        "estimatedMinDays": 5,
        "estimatedMaxDays": 9,
    })
}

fn per_item(rate_id: &str, label: &str, first_minor: i64, additional_minor: i64) -> Value {
    json!({
        "pricing": "per_item",
        "rateId": rate_id,
        "label": label,
        "firstMinor": first_minor,
        "additionalMinor": additional_minor,
        "estimatedMinDays": 7,
        "estimatedMaxDays": 14,
    })
}

/// Europe (DE, FR) ships flat; the United States ships per item.
fn two_zones() -> Value {
    json!([
        {
            "zoneId": "eu",
            "name": "Europe",
            "countries": ["DE", "FR"],
            "rates": [flat("standard", "Standard", 800), flat("express", "Express", 2_000)],
        },
        {
            "zoneId": "us",
            "name": "United States",
            "countries": ["US"],
            "rates": [per_item("ground", "Ground", 500, 200)],
        },
    ])
}

fn profile_record(seller: &TestActor, revision: i64, excluded: &[&str], zones: Value) -> Value {
    json!({
        "schemaVersion": 1,
        "recordType": "shipping_profile",
        "ownerPubky": seller.pubky,
        "revision": revision,
        "createdAt": "2026-08-19T21:00:00.000Z",
        "updatedAt": "2026-08-19T21:00:00.000Z",
        "profileId": PROFILE_ID,
        "name": "Worldwide",
        "currency": "USD",
        "exponent": 2,
        "origins": ["US"],
        "excludedCountries": excluded,
        "zones": zones,
    })
}

struct World {
    app: TestApp,
    homeserver: FakeHomeserver,
    seller: TestActor,
    buyer: TestActor,
    /// SHA-256 of the profile bytes the buyer saw.
    sha: String,
}

/// A seller with a published two-zone profile and one listing that
/// references it, and a buyer.
async fn world(pool: PgPool) -> World {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let sha = homeserver.put_shipping_profile(
        &seller.pubky,
        PROFILE_ID,
        &profile_record(&seller, 1, &[], two_zones()),
    );
    let mut register = register_command(&seller.pubky, LISTING_STOCK);
    register["payload"]["shipping_profile_id"] = json!(PROFILE_ID);
    homeserver.publish_registration(&register);
    let (status, body) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    World {
        app,
        homeserver,
        seller,
        buyer,
        sha,
    }
}

fn choice(seller: &TestActor, rate_id: &str, sha: &str) -> Value {
    json!({
        "seller_pubky": seller.pubky,
        "profile_id": PROFILE_ID,
        "rate_id": rate_id,
        "profile_sha256": sha,
    })
}

/// A checkout of `quantity` units to `destination` at `rate`, pinned to `sha`.
fn checkout(
    world: &World,
    number: u64,
    quantity: i64,
    destination: Option<&str>,
    rate: Option<&str>,
    sha: &str,
) -> Value {
    let mut command =
        checkout_command_with_id(&world.seller.pubky, &indexed_command_id(0x7a00, number));
    command["payload"]["lines"][0]["quantity"] = json!(quantity);
    if let Some(destination) = destination {
        command["payload"]["destination_country"] = json!(destination);
    }
    if let Some(rate) = rate {
        command["payload"]["shipping_choices"] = json!([choice(&world.seller, rate, sha)]);
    }
    command
}

async fn buy(world: &World, command: &Value) -> (StatusCode, Value) {
    execute(&world.app, &world.buyer.token, command).await
}

fn order_of(body: &Value) -> &Value {
    &body["result"]["orders"][0]
}

fn assert_error(
    result: &(StatusCode, Value),
    status: StatusCode,
    code: &str,
    reason: Option<&str>,
) {
    assert_eq!(result.0, status, "{}", result.1);
    assert_eq!(result.1["error"]["code"], json!(code), "{}", result.1);
    if let Some(reason) = reason {
        assert_eq!(result.1["error"]["reason"], json!(reason), "{}", result.1);
    }
}

async fn orders_of(world: &World) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM orders WHERE buyer_pubky = $1")
        .bind(&world.buyer.pubky)
        .fetch_one(&world.app.pool)
        .await
        .expect("order count")
}

async fn assert_nothing_written(world: &World) {
    for table in ["orders", "payments", "reservations"] {
        assert_eq!(
            count(&world.app.pool, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }
}

/// The order row's destination, charged shipping and quotes.
async fn stored_order(world: &World, order_id: &str) -> (Option<String>, i64, Option<Value>) {
    sqlx::query_as(
        "SELECT destination_country, shipping_minor, shipping_quotes FROM orders \
         WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&world.app.pool)
    .await
    .expect("order row")
}

// --- checkout ---------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_zones_price_an_order_each_by_its_destination_and_rate(pool: PgPool) {
    let world = world(pool).await;

    let (status, body) = buy(
        &world,
        &checkout(&world, 1, 2, Some("DE"), Some("standard"), &world.sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let europe = order_of(&body);
    assert_eq!(europe["shipping"]["amount_minor"], json!(800), "{europe}");
    assert_eq!(
        europe["subtotal"]["amount_minor"],
        json!(25_000),
        "{europe}"
    );
    assert_eq!(europe["total"]["amount_minor"], json!(25_800), "{europe}");
    assert_eq!(europe["destination_country"], json!("DE"));
    let quote = &europe["shipping_quotes"][0];
    assert_eq!(quote["zone_id"], json!("eu"), "{quote}");
    assert_eq!(quote["rate_id"], json!("standard"), "{quote}");
    assert_eq!(quote["profile_id"], json!(PROFILE_ID), "{quote}");
    assert_eq!(quote["profile_sha256"], json!(world.sha), "{quote}");
    assert_eq!(quote["units"], json!(2), "{quote}");
    assert_eq!(
        body["result"]["payments"][0]["amount"]["amount_minor"],
        json!(25_800)
    );
    let id = europe["id"].as_str().expect("order id").to_string();
    let (destination, shipping, quotes) = stored_order(&world, &id).await;
    assert_eq!(destination.as_deref(), Some("DE"));
    assert_eq!(shipping, 800);
    assert_eq!(
        quotes.expect("stored quotes")[0]["amount_minor"],
        json!(800)
    );

    // Per item: 500 for the first unit and 200 for each further one.
    let (status, body) = buy(
        &world,
        &checkout(&world, 2, 3, Some("US"), Some("ground"), &world.sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let america = order_of(&body);
    assert_eq!(america["shipping"]["amount_minor"], json!(900), "{america}");
    assert_eq!(america["total"]["amount_minor"], json!(37_500 + 900));
    assert_eq!(america["shipping_quotes"][0]["zone_id"], json!("us"));

    // The same destination at the dearer rate.
    let (status, body) = buy(
        &world,
        &checkout(&world, 3, 1, Some("FR"), Some("express"), &world.sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(2_000));
    assert_eq!(orders_of(&world).await, 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_zone_priced_listing_needs_a_destination_and_a_rate(pool: PgPool) {
    let world = world(pool).await;

    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, None, Some("standard"), &world.sha),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        None,
    );
    assert_error(
        &buy(&world, &checkout(&world, 2, 1, None, None, &world.sha)).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_required"),
    );
    assert_error(
        &buy(
            &world,
            &checkout(&world, 3, 1, Some("DE"), None, &world.sha),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("shipping_choice_required"),
    );
    assert_error(
        &buy(
            &world,
            &checkout(&world, 4, 1, Some("DE"), Some("ground"), &world.sha),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("unknown_shipping_rate"),
    );
    assert_nothing_written(&world).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_country_no_zone_serves_or_the_profile_excludes_is_not_shipped(pool: PgPool) {
    let world = world(pool).await;
    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, Some("BR"), Some("standard"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE_TO_COUNTRY",
        None,
    );

    // An exclusion wins over the zone that lists the country. The seller
    // publishes a new revision, and the buyer pins what they saw.
    let sha = world.homeserver.put_shipping_profile(
        &world.seller.pubky,
        PROFILE_ID,
        &profile_record(&world.seller, 2, &["DE"], two_zones()),
    );
    assert_error(
        &buy(
            &world,
            &checkout(&world, 2, 1, Some("DE"), Some("standard"), &sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE_TO_COUNTRY",
        None,
    );
    let (status, body) = buy(
        &world,
        &checkout(&world, 3, 1, Some("FR"), Some("standard"), &sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(orders_of(&world).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_shop_ships_to_list_limits_every_destination(pool: PgPool) {
    let world = world(pool).await;
    world.homeserver.put_shop_record(
        &world.seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": world.seller.pubky,
            "name": "Worldwide Shop",
            "shipsToCountries": ["US"],
        }),
    );
    // Germany is in a zone but not in the shop's list.
    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, Some("DE"), Some("standard"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE_TO_COUNTRY",
        None,
    );
    let (status, body) = buy(
        &world,
        &checkout(&world, 2, 1, Some("US"), Some("ground"), &world.sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A shop record that cannot be read refuses rather than shipping anywhere.
    world.homeserver.fail_shop_record(&world.seller.pubky);
    assert_error(
        &buy(
            &world,
            &checkout(&world, 3, 1, Some("US"), Some("ground"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("shop_unreadable"),
    );
    assert_eq!(orders_of(&world).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_profile_the_seller_changed_since_the_buyer_looked_is_shipping_changed(pool: PgPool) {
    let world = world(pool).await;
    let old_sha = world.sha.clone();
    let new_sha = world.homeserver.put_shipping_profile(
        &world.seller.pubky,
        PROFILE_ID,
        &profile_record(
            &world.seller,
            2,
            &[],
            json!([{
                "zoneId": "eu",
                "name": "Europe",
                "countries": ["DE", "FR"],
                "rates": [flat("standard", "Standard", 4_000)],
            }]),
        ),
    );
    assert_ne!(old_sha, new_sha);

    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, Some("DE"), Some("standard"), &old_sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_CHANGED",
        None,
    );
    assert_nothing_written(&world).await;

    let (status, body) = buy(
        &world,
        &checkout(&world, 2, 1, Some("DE"), Some("standard"), &new_sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(4_000));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_profile_that_is_gone_or_unreadable_refuses_the_order(pool: PgPool) {
    let world = world(pool).await;

    world
        .homeserver
        .fail_shipping_profile(&world.seller.pubky, PROFILE_ID);
    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, Some("DE"), Some("standard"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("shipping_profile_unreadable"),
    );

    world
        .homeserver
        .heal_record(&world.seller.pubky, PROFILE_ID);
    world
        .homeserver
        .delete_shipping_profile(&world.seller.pubky, PROFILE_ID);
    assert_error(
        &buy(
            &world,
            &checkout(&world, 2, 1, Some("DE"), Some("standard"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("shipping_profile_missing"),
    );
    assert_nothing_written(&world).await;
}

/// A profile priced in another asset than the listing cannot price it.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_profile_in_another_currency_refuses_the_order(pool: PgPool) {
    let world = world(pool).await;
    let mut record = profile_record(&world.seller, 2, &[], two_zones());
    record["currency"] = json!("EUR");
    let sha = world
        .homeserver
        .put_shipping_profile(&world.seller.pubky, PROFILE_ID, &record);
    assert_error(
        &buy(
            &world,
            &checkout(&world, 1, 1, Some("DE"), Some("standard"), &sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("shipping_asset_mismatch"),
    );
    assert_nothing_written(&world).await;
}

/// Two listings of one seller that share a profile are one shipping group:
/// a flat rate is charged once and per-item units add up across the lines.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn lines_sharing_a_profile_are_charged_as_one_group(pool: PgPool) {
    let world = world(pool).await;
    let mut second = common::register_listing_command(&world.seller.pubky, "mug_01", 20, 11);
    second["payload"]["shipping_profile_id"] = json!(PROFILE_ID);
    world.homeserver.publish_registration(&second);
    let (status, body) = execute(&world.app, &world.seller.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mug = format!("listing:{}_mug_01", world.seller.pubky);

    let build = |number: u64, destination: &str, rate: &str| {
        let mut command = checkout(&world, number, 2, Some(destination), Some(rate), &world.sha);
        command["payload"]["lines"] = json!([
            {
                "listing_aggregate_id": listing_aggregate(&world.seller.pubky),
                "expected_revision": 1,
                "quantity": 2,
            },
            { "listing_aggregate_id": mug, "expected_revision": 1, "quantity": 1 },
        ]);
        command
    };

    let (status, body) = buy(&world, &build(1, "DE", "standard")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(800));
    assert_eq!(order_of(&body)["shipping_quotes"][0]["units"], json!(3));

    // Three units per item: 500 + 2 * 200.
    let (status, body) = buy(&world, &build(2, "US", "ground")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(900));
    assert_eq!(
        body["result"]["orders"].as_array().map(Vec::len),
        Some(1),
        "one seller and one profile make one order"
    );

    // One choice names the group; a second one for it is refused.
    let mut doubled = build(3, "DE", "standard");
    doubled["payload"]["shipping_choices"] = json!([
        choice(&world.seller, "standard", &world.sha),
        choice(&world.seller, "express", &world.sha),
    ]);
    assert_error(
        &buy(&world, &doubled).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        None,
    );
}

/// A listing that references no profile keeps its flat rate, and a destination
/// on it is held to the shop's ships-to list.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_flat_listing_keeps_its_rate_and_honours_the_shop_list(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let flat_listing = register_command(&seller.pubky, 5);
    homeserver.publish_registration(&flat_listing);
    let (status, body) = execute(&app, &seller.token, &flat_listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let plain = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 1));
    let (status, body) = execute(&app, &buyer.token, &plain).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(1_200));
    assert!(order_of(&body).get("destination_country").is_none());

    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Flat Shop",
            "shipsToCountries": ["US"],
        }),
    );
    let mut abroad = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 2));
    abroad["payload"]["destination_country"] = json!("DE");
    assert_error(
        &execute(&app, &buyer.token, &abroad).await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE_TO_COUNTRY",
        None,
    );
    let mut home = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 3));
    home["payload"]["destination_country"] = json!("US");
    let (status, body) = execute(&app, &buyer.token, &home).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_of(&body)["shipping"]["amount_minor"], json!(1_200));
    assert_eq!(order_of(&body)["destination_country"], json!("US"));

    // A shop that limits where it ships needs the buyer to say where.
    let silent = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 4));
    assert_error(
        &execute(&app, &buyer.token, &silent).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_required"),
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM orders").await,
        2,
        "the refused checkout wrote nothing"
    );

    // With the shop record unreadable a destination refuses; an order that
    // names none is not stopped by an outage it cannot be checked against.
    homeserver.fail_shop_record(&seller.pubky);
    let mut named = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 5));
    named["payload"]["destination_country"] = json!("US");
    assert_error(
        &execute(&app, &buyer.token, &named).await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("shop_unreadable"),
    );
    let unnamed = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a01, 6));
    let (status, body) = execute(&app, &buyer.token, &unnamed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(order_of(&body).get("destination_country").is_none());
}

// --- registration ------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn registering_stores_the_profile_or_the_zones_and_a_re_register_switches_them(pool: PgPool) {
    let world = world(pool).await;
    let aggregate = listing_aggregate(&world.seller.pubky);
    let row = |aggregate: String| {
        let pool = world.app.pool.clone();
        async move {
            sqlx::query_as::<_, (Option<String>, Option<Value>)>(
                "SELECT shipping_profile_id, shipping_zones FROM listings WHERE aggregate_id = $1",
            )
            .bind(aggregate)
            .fetch_one(&pool)
            .await
            .expect("listing row")
        }
    };
    assert_eq!(
        row(aggregate.clone()).await,
        (Some(PROFILE_ID.to_string()), None)
    );

    let mut inline = register_command(&world.seller.pubky, LISTING_STOCK);
    inline["command_id"] = json!(indexed_command_id(0x7a02, 1));
    inline["expected_revision"] = json!(1);
    inline["payload"]["shipping_zones"] = two_zones();
    inline["payload"]["listing_revision"] = json!(2);
    world.homeserver.publish_registration(&inline);
    let (status, body) = execute(&world.app, &world.seller.token, &inline).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (profile, zones) = row(aggregate.clone()).await;
    assert_eq!(profile, None);
    assert_eq!(zones.expect("zones")[0]["zoneId"], json!("eu"));

    // A profile and zones together are not one shipping term.
    let mut both = inline.clone();
    both["command_id"] = json!(indexed_command_id(0x7a02, 2));
    both["expected_revision"] = json!(2);
    both["payload"]["shipping_profile_id"] = json!(PROFILE_ID);
    assert_error(
        &execute(&world.app, &world.seller.token, &both).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        None,
    );
    // Overlapping zones are refused by the format's own validation.
    let mut overlapping = inline.clone();
    overlapping["command_id"] = json!(indexed_command_id(0x7a02, 3));
    overlapping["expected_revision"] = json!(2);
    overlapping["payload"]["shipping_zones"] = json!([
        { "zoneId": "a", "name": "A", "countries": ["DE"], "rates": [flat("r", "R", 1)] },
        { "zoneId": "b", "name": "B", "countries": ["DE"], "rates": [flat("r", "R", 1)] },
    ]);
    assert_error(
        &execute(&world.app, &world.seller.token, &overlapping).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        None,
    );
}

// --- concurrency -------------------------------------------------------------

/// Whatever the interleaving, an order is priced by the profile it was
/// pinned to: checkouts run against the listing while the seller moves it to
/// another profile, and each stored quote names the profile and the hash the
/// buyer chose, or the checkout is refused with nothing written for it.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkouts_racing_a_profile_switch_never_mix_the_two_profiles(pool: PgPool) {
    let world = world(pool).await;
    let aggregate = listing_aggregate(&world.seller.pubky);
    let mut other = profile_record(&world.seller, 1, &[], two_zones());
    other["profileId"] = json!("pf_other");
    other["zones"][0]["rates"][0] = flat("standard", "Standard", 9_999);
    let other_sha = world
        .homeserver
        .put_shipping_profile(&world.seller.pubky, "pf_other", &other);
    world
        .homeserver
        .set_delay(std::time::Duration::from_millis(20));

    let buyers = [
        new_actor(&world.app).await,
        new_actor(&world.app).await,
        new_actor(&world.app).await,
        new_actor(&world.app).await,
    ];
    let mut switch = register_command(&world.seller.pubky, LISTING_STOCK);
    switch["command_id"] = json!(indexed_command_id(0x7a03, 1));
    switch["expected_revision"] = json!(1);
    switch["payload"]["shipping_profile_id"] = json!("pf_other");
    switch["payload"]["listing_revision"] = json!(2);
    world.homeserver.publish_registration(&switch);

    // One order is placed before the race so the old profile is known to
    // price: the race cannot pass with every checkout refused.
    let before = buy(
        &world,
        &checkout(&world, 99, 1, Some("DE"), Some("standard"), &world.sha),
    )
    .await;
    assert_eq!(before.0, StatusCode::OK, "{}", before.1);

    let commands: Vec<Value> = (0..buyers.len())
        .map(|index| {
            checkout(
                &world,
                100 + index as u64,
                1,
                Some("DE"),
                Some("standard"),
                &world.sha,
            )
        })
        .collect();
    let (switched, first, second, third, fourth) = tokio::join!(
        execute(&world.app, &world.seller.token, &switch),
        execute(&world.app, &buyers[0].token, &commands[0]),
        execute(&world.app, &buyers[1].token, &commands[1]),
        execute(&world.app, &buyers[2].token, &commands[2]),
        execute(&world.app, &buyers[3].token, &commands[3]),
    );
    let results = [first, second, third, fourth];
    assert_eq!(switched.0, StatusCode::OK, "{}", switched.1);

    // Each checkout either priced by the profile it was pinned to or was
    // refused for the switch: the listing moved to revision 2 under it, the
    // profile moved (shipping changed or unavailable), or its choice no
    // longer names the listing's profile.
    let mut placed = 1;
    for (status, body) in &results {
        match *status {
            StatusCode::OK => {
                placed += 1;
                let quote = &order_of(body)["shipping_quotes"][0];
                assert_eq!(quote["profile_id"], json!(PROFILE_ID), "{body}");
                assert_eq!(quote["profile_sha256"], json!(world.sha), "{body}");
                assert_eq!(quote["amount_minor"], json!(800), "{body}");
            }
            StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY => {
                let code = body["error"]["code"].as_str().unwrap_or_default();
                let reason = body["error"]["reason"].as_str().unwrap_or_default();
                assert!(
                    matches!(
                        code,
                        "SHIPPING_CHANGED" | "SHIPPING_UNAVAILABLE" | "REVISION_CONFLICT"
                    ) || (code == "INVALID_COMMAND" && reason == "shipping_choice_required"),
                    "refused for an unexpected reason: {body}"
                );
            }
            other => panic!("unexpected outcome {other}: {body}"),
        }
    }
    assert_eq!(
        count(&world.app.pool, "SELECT COUNT(*) FROM orders").await,
        placed,
        "one order per accepted checkout, none for a refused one"
    );
    assert_eq!(
        count(
            &world.app.pool,
            "SELECT COUNT(*) FROM orders WHERE shipping_minor NOT IN (800)"
        )
        .await,
        0,
        "no order carries the other profile's price"
    );

    // After the switch the listing is priced by the other profile only.
    let (profile,): (Option<String>,) =
        sqlx::query_as("SELECT shipping_profile_id FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate)
            .fetch_one(&world.app.pool)
            .await
            .expect("listing row");
    assert_eq!(profile.as_deref(), Some("pf_other"));

    // The listing is now priced by the other profile only: a choice pinned to
    // the old one is refused, and a choice pinned to the new one prices it.
    let mut stale = checkout(&world, 200, 1, Some("DE"), Some("standard"), &world.sha);
    stale["payload"]["lines"][0]["expected_revision"] = json!(2);
    let stale = buy(&world, &stale).await;
    assert_eq!(stale.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", stale.1);
    assert_eq!(stale.1["error"]["code"], json!("INVALID_COMMAND"));
    let mut fresh = checkout(&world, 201, 1, Some("DE"), None, &world.sha);
    fresh["payload"]["lines"][0]["expected_revision"] = json!(2);
    fresh["payload"]["shipping_choices"] = json!([{
        "seller_pubky": world.seller.pubky,
        "profile_id": "pf_other",
        "rate_id": "standard",
        "profile_sha256": other_sha,
    }]);
    let fresh = buy(&world, &fresh).await;
    assert_eq!(fresh.0, StatusCode::OK, "{}", fresh.1);
    assert_eq!(
        order_of(&fresh.1)["shipping_quotes"][0]["amount_minor"],
        json!(9_999),
        "{}",
        fresh.1
    );
}

/// The seller rewrites the profile's rates while checkouts pinned to the old
/// bytes are in flight. Each is priced by the bytes it pinned or refused as
/// changed; none is charged a rate it did not pin.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkouts_racing_a_profile_rewrite_are_priced_by_the_bytes_they_pinned(pool: PgPool) {
    let world = world(pool).await;
    world
        .homeserver
        .set_delay(std::time::Duration::from_millis(30));
    let buyers = [
        new_actor(&world.app).await,
        new_actor(&world.app).await,
        new_actor(&world.app).await,
        new_actor(&world.app).await,
    ];
    let mut rewritten = profile_record(&world.seller, 2, &[], two_zones());
    rewritten["zones"][0]["rates"][0] = flat("standard", "Standard", 9_999);
    // One order is placed before the rewrite so the old bytes are known to
    // price: the race cannot pass with every checkout refused.
    let before = buy(
        &world,
        &checkout(&world, 299, 1, Some("DE"), Some("standard"), &world.sha),
    )
    .await;
    assert_eq!(before.0, StatusCode::OK, "{}", before.1);
    let commands: Vec<Value> = (0..buyers.len())
        .map(|index| {
            checkout(
                &world,
                300 + index as u64,
                1,
                Some("DE"),
                Some("standard"),
                &world.sha,
            )
        })
        .collect();
    let (new_sha, first, second, third, fourth) = tokio::join!(
        async {
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            world
                .homeserver
                .put_shipping_profile(&world.seller.pubky, PROFILE_ID, &rewritten)
        },
        execute(&world.app, &buyers[0].token, &commands[0]),
        execute(&world.app, &buyers[1].token, &commands[1]),
        execute(&world.app, &buyers[2].token, &commands[2]),
        execute(&world.app, &buyers[3].token, &commands[3]),
    );
    let mut placed = 1;
    for (status, body) in [first, second, third, fourth] {
        match status {
            StatusCode::OK => {
                placed += 1;
                let quote = &order_of(&body)["shipping_quotes"][0];
                assert_eq!(quote["profile_sha256"], json!(world.sha), "{body}");
                assert_eq!(quote["amount_minor"], json!(800), "{body}");
            }
            StatusCode::CONFLICT => {
                assert_eq!(body["error"]["code"], json!("SHIPPING_CHANGED"), "{body}");
            }
            other => panic!("unexpected outcome {other}: {body}"),
        }
    }
    assert_eq!(
        count(&world.app.pool, "SELECT COUNT(*) FROM orders").await,
        placed
    );
    assert_eq!(
        count(
            &world.app.pool,
            "SELECT COUNT(*) FROM orders WHERE shipping_minor <> 800"
        )
        .await,
        0
    );

    // Once the rewrite is served the old pin is refused and the new one prices.
    assert_error(
        &buy(
            &world,
            &checkout(&world, 310, 1, Some("DE"), Some("standard"), &world.sha),
        )
        .await,
        StatusCode::CONFLICT,
        "SHIPPING_CHANGED",
        None,
    );
    let (status, body) = buy(
        &world,
        &checkout(&world, 311, 1, Some("DE"), Some("standard"), &new_sha),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        order_of(&body)["shipping_quotes"][0]["amount_minor"],
        json!(9_999),
        "{body}"
    );
}

/// The same checkout sent twice at once creates one order, priced once.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn one_checkout_sent_twice_at_once_makes_one_priced_order(pool: PgPool) {
    let world = world(pool).await;
    world
        .homeserver
        .set_delay(std::time::Duration::from_millis(20));
    let command = checkout(&world, 7, 2, Some("US"), Some("ground"), &world.sha);
    let results = tokio::join!(
        buy(&world, &command),
        buy(&world, &command),
        buy(&world, &command),
        buy(&world, &command),
    );
    let results = [results.0, results.1, results.2, results.3];
    for (status, body) in &results {
        assert_eq!(*status, StatusCode::OK, "{body}");
        assert_eq!(order_of(body)["shipping"]["amount_minor"], json!(700));
    }
    assert_eq!(orders_of(&world).await, 1);
}

// --- offers ------------------------------------------------------------------

/// The seller's published record, as the Shop writes it, referencing the
/// profile.
fn listing_record(seller: &TestActor) -> Value {
    json!({
        "revision": 1,
        "state": "active",
        "title": "Cast iron pan",
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
            "acceptsOffers": true,
        },
        "variants": [{ "id": "boots_01", "options": [], "quantity": LISTING_STOCK, "enabled": true }],
        "fulfillmentMethods": ["shipping"],
        "shippingProfileId": PROFILE_ID,
        "shippingOptions": [{ "pricing": "free" }],
        "ownerPubky": seller.pubky,
    })
}

fn offer(world: &World, quantity: i64, destination: Option<&str>, rate: Option<&str>) -> Value {
    let mut command = create_offer_command(&world.seller.pubky, quantity);
    if let Some(destination) = destination {
        command["payload"]["destination_country"] = json!(destination);
    }
    if let Some(rate) = rate {
        command["payload"]["shipping_choice"] = choice(&world.seller, rate, &world.sha);
    }
    command
}

async fn publish_listing_record(world: &World) {
    world.homeserver.put_record(
        &world.seller.pubky,
        "boots_01",
        listing_record(&world.seller),
    );
}

async fn accept(world: &World) -> (StatusCode, Value) {
    execute(
        &world.app,
        &world.seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000a101"),
    )
    .await
}

fn offer_id() -> uuid::Uuid {
    uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_is_priced_for_its_destination_and_converts_to_a_matching_order(pool: PgPool) {
    let world = world(pool).await;
    publish_listing_record(&world).await;
    world.app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );

    // The buyer must say where the goods go and which rate they take.
    assert_error(
        &execute(
            &world.app,
            &world.buyer.token,
            &offer(&world, 3, None, None),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_required"),
    );
    let (status, body) = execute(
        &world.app,
        &world.buyer.token,
        &offer(&world, 3, Some("US"), Some("ground")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (destination, choice_json): (Option<String>, Option<Value>) =
        sqlx::query_as("SELECT destination_country, shipping_choice FROM offers WHERE id = $1")
            .bind(offer_id())
            .fetch_one(&world.app.pool)
            .await
            .expect("offer row");
    assert_eq!(destination.as_deref(), Some("US"));
    assert_eq!(choice_json.expect("choice")["rate_id"], json!("ground"));

    // Acceptance prices the accepted quantity: 500 + 2 * 200.
    let (status, body) = accept(&world).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (shipping, total, quotes): (i64, i64, Option<Value>) = sqlx::query_as(
        "SELECT accepted_shipping_minor, accepted_total_minor, accepted_shipping_quotes \
         FROM offers WHERE id = $1",
    )
    .bind(offer_id())
    .fetch_one(&world.app.pool)
    .await
    .expect("accepted offer");
    assert_eq!((shipping, total), (900, 3 * 10_000 + 900));
    assert_eq!(
        quotes.expect("quotes")[0]["profile_sha256"],
        json!(world.sha)
    );
    let award = &body["result"]["offer"]["award"];
    assert_eq!(award["destination_country"], json!("US"), "{award}");

    // The award converts to an order for the same destination and charge.
    let (award_id, revision, sha, variant): (uuid::Uuid, i64, String, String) = sqlx::query_as(
        "SELECT award_id, accepted_listing_revision, accepted_listing_record_sha256, \
         accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(offer_id())
    .fetch_one(&world.app.pool)
    .await
    .expect("award");
    let mut convert = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-00000000a102",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": award_id,
            "listing_aggregate_id": listing_aggregate(&world.seller.pubky),
            "listing_revision": revision,
            "listing_record_sha256": sha,
            "variant_id": variant,
            "quantity": 3,
            "delivery_address": {
                "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                "city": "New York", "region": "NY", "postal_code": "10001", "country_code": "US",
            },
            "destination_country": "DE",
            "guarantee_policy_version": 1,
        },
    });
    assert_error(
        &execute(&world.app, &world.buyer.token, &convert).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_mismatch"),
    );
    convert["payload"]["destination_country"] = json!("US");
    let (status, body) = execute(&world.app, &world.buyer.token, &convert).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = &body["result"]["order"];
    assert_eq!(order["shipping"]["amount_minor"], json!(900), "{order}");
    assert_eq!(order["destination_country"], json!("US"), "{order}");
    assert_eq!(
        order["shipping_quotes"][0]["zone_id"],
        json!("us"),
        "{order}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn accepting_after_the_seller_changed_the_profile_is_shipping_changed(pool: PgPool) {
    let world = world(pool).await;
    publish_listing_record(&world).await;
    world.app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let (status, body) = execute(
        &world.app,
        &world.buyer.token,
        &offer(&world, 1, Some("DE"), Some("standard")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    world.homeserver.put_shipping_profile(
        &world.seller.pubky,
        PROFILE_ID,
        &profile_record(&world.seller, 2, &[], two_zones()),
    );
    assert_error(
        &accept(&world).await,
        StatusCode::CONFLICT,
        "SHIPPING_CHANGED",
        None,
    );
    let state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(offer_id())
        .fetch_one(&world.app.pool)
        .await
        .expect("offer state");
    assert_eq!(
        state, "pending",
        "the offer stays open for the buyer to renew"
    );
}

// --- auctions ----------------------------------------------------------------

fn bid_with(command: &mut Value, seller: &TestActor, destination: &str, rate: &str) {
    command["payload"]["destination_country"] = json!(destination);
    command["payload"]["shipping_choice"] = json!({
        "seller_pubky": seller.pubky,
        "listing_aggregate_id": listing_aggregate(&seller.pubky),
        "rate_id": rate,
    });
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_winning_bid_prices_the_auction_order(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let leader = new_actor(&app).await;
    let runner_up = new_actor(&app).await;
    let mut register = register_auction_command(&seller.pubky);
    register["payload"]["shipping_zones"] = two_zones();
    let (status, body) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");

    // A bid says where the item would go.
    let (status, body) = execute(
        &app,
        &leader.token,
        &place_bid_command(&seller.pubky, 19, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("destination_country_required")
    );

    let mut first = place_bid_command(&seller.pubky, 20, 10_000, 1);
    bid_with(&mut first, &seller, "US", "ground");
    let (status, body) = execute(&app, &leader.token, &first).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut second = place_bid_command(&seller.pubky, 21, 8_000, 2);
    bid_with(&mut second, &seller, "DE", "express");
    let (status, body) = execute(&app, &runner_up.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored: (Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT destination_country, shipping_quote FROM bids WHERE bidder_pubky = $1",
    )
    .bind(&leader.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("bid row");
    assert_eq!(stored.0.as_deref(), Some("US"));
    assert_eq!(stored.1.expect("quote")["amount_minor"], json!(500));

    app.clock.advance_seconds(11 * 60);
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 950),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {body}");
    let order = &body["result"]["order"];
    assert_eq!(order["buyer_pubky"], json!(leader.pubky));
    assert_eq!(order["subtotal"]["amount_minor"], json!(8_500), "{order}");
    assert_eq!(order["shipping"]["amount_minor"], json!(500), "{order}");
    assert_eq!(order["total"]["amount_minor"], json!(9_000), "{order}");
    assert_eq!(order["destination_country"], json!("US"), "{order}");
    let (destination, shipping): (Option<String>, i64) = sqlx::query_as(
        "SELECT destination_country, shipping_minor FROM orders WHERE auction_aggregate_id IS NOT NULL",
    )
    .fetch_one(&app.pool)
    .await
    .expect("auction order");
    assert_eq!((destination.as_deref(), shipping), (Some("US"), 500));
}

// --- blocked destination -----------------------------------------------------

async fn bind(app: &TestApp, buyer: &TestActor, order_id: &str) -> (StatusCode, Value) {
    send_from(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
        Some("CH"),
    )
    .await
}

fn with_modes(app: &TestApp, modes: &str) -> TestApp {
    reconfigured(app, |config: &mut Config| {
        config.region_modes = Some(RegionModes::parse(modes).expect("test modes parse"));
    })
}

/// A seller whose listing carries its own zones, with PayPal configured.
async fn paypal_seller(pool: PgPool) -> (TestApp, FakeHomeserver, TestActor, TestActor) {
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(test_app_with_pickup_and_paypal(pool).await, &homeserver);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut register = register_command(&seller.pubky, LISTING_STOCK);
    register["payload"]["shipping_zones"] = two_zones();
    let (status, body) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment config failed: {body}");
    (app, homeserver, seller, buyer)
}

fn inline_checkout(seller: &TestActor, number: u64, destination: &str, rate: &str) -> Value {
    let mut command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7a04, number));
    command["payload"]["destination_country"] = json!(destination);
    command["payload"]["shipping_choices"] = json!([{
        "seller_pubky": seller.pubky,
        "listing_aggregate_id": listing_aggregate(&seller.pubky),
        "rate_id": rate,
    }]);
    command["payload"]["buyer_country"] = json!("CH");
    command
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_destination_in_a_blocked_region_is_refused_at_checkout(pool: PgPool) {
    let (setup, _homeserver, seller, buyer) = paypal_seller(pool).await;
    let app = with_modes(&setup, "DE=blocked");

    let (status, body) = execute_from(
        &app,
        &buyer.token,
        &inline_checkout(&seller, 1, "DE", "standard"),
        Some("CH"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("COMMERCE_UNAVAILABLE_IN_REGION")
    );
    assert_eq!(body["error"]["reason"], json!("destination_region_blocked"));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);

    let (status, body) = execute_from(
        &app,
        &buyer.token,
        &inline_checkout(&seller, 2, "FR", "standard"),
        Some("CH"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn starting_a_payment_rechecks_the_orders_destination(pool: PgPool) {
    let (setup, _homeserver, seller, buyer) = paypal_seller(pool).await;
    let (status, body) = execute(
        &setup,
        &buyer.token,
        &inline_checkout(&seller, 1, "DE", "standard"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = order_of(&body)["id"]
        .as_str()
        .expect("order id")
        .to_string();

    // Germany becomes a blocked region after the order was placed.
    let app = with_modes(&setup, "DE=blocked");
    let (status, body) = bind(&app, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("destination_region_blocked"));
    let bound: Option<String> =
        sqlx::query_scalar("SELECT payment_method FROM orders WHERE id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(bound, None, "no payment started");

    let open = with_modes(&setup, "BR=blocked");
    let (status, body) = bind(&open, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

// --- offers priced without a destination, and a record ahead of the row -----

/// A flat-rate listing's offer, accepted, with the checkout payload for it.
async fn accepted_flat_offer(
    app: &TestApp,
    homeserver: &FakeHomeserver,
    seller: &TestActor,
    buyer: &TestActor,
) -> impl Fn(Option<&str>) -> Value {
    let flat_listing = register_command(&seller.pubky, 5);
    homeserver.publish_registration(&flat_listing);
    let (status, body) = execute(app, &seller.token, &flat_listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let (status, body) = execute(app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000a101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    move |destination: Option<&str>| {
        let mut command = json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-00000000a102",
            "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
            "expected_revision": 2,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.checkout",
            "payload": {
                "offer_id": OFFER_COMMAND_ID,
                "award_id": row.0.to_string(),
                "listing_aggregate_id": row.1,
                "listing_revision": row.2,
                "listing_record_sha256": row.3,
                "variant_id": row.4,
                "quantity": 1,
                "delivery_address": {
                    "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                    "city": "New York", "region": "NY", "postal_code": "10001",
                    "country_code": "US",
                },
                "guarantee_policy_version": 1,
            },
        });
        if let Some(destination) = destination {
            command["payload"]["destination_country"] = json!(destination);
        }
        command
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_priced_without_a_destination_cannot_gain_one_at_checkout(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let convert = accepted_flat_offer(&app, &homeserver, &seller, &buyer).await;

    // The seller limits where the shop ships after the offer was accepted.
    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Flat Shop",
            "shipsToCountries": ["US"],
        }),
    );
    assert_error(
        &execute(&app, &buyer.token, &convert(Some("DE"))).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_mismatch"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);

    let (status, body) = execute(&app, &buyer.token, &convert(None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["result"]["order"].get("destination_country").is_none());
}

/// An offer is priced from the listing record the homeserver serves. While
/// the registered row still names the previous profile, the profile the
/// record names is the one read and the one the buyer pins.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_reads_the_profile_the_listing_record_names_not_the_registered_row(pool: PgPool) {
    let world = world(pool).await;
    world.app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let mut other = profile_record(&world.seller, 1, &[], two_zones());
    other["profileId"] = json!("pf_other");
    other["zones"][0]["rates"][0] = flat("standard", "Standard", 9_999);
    let other_sha = world
        .homeserver
        .put_shipping_profile(&world.seller.pubky, "pf_other", &other);
    let mut record = listing_record(&world.seller);
    record["shippingProfileId"] = json!("pf_other");
    world
        .homeserver
        .put_record(&world.seller.pubky, "boots_01", record);
    let (profile,): (Option<String>,) =
        sqlx::query_as("SELECT shipping_profile_id FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&world.seller.pubky))
            .fetch_one(&world.app.pool)
            .await
            .expect("listing row");
    assert_eq!(
        profile.as_deref(),
        Some(PROFILE_ID),
        "the registered row has not synced"
    );

    let mut command = create_offer_command(&world.seller.pubky, 1);
    command["payload"]["destination_country"] = json!("DE");
    command["payload"]["shipping_choice"] = json!({
        "seller_pubky": world.seller.pubky,
        "profile_id": "pf_other",
        "rate_id": "standard",
        "profile_sha256": other_sha,
    });
    let (status, body) = execute(&world.app, &world.buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored: Option<Value> =
        sqlx::query_scalar("SELECT shipping_choice FROM offers WHERE id = $1")
            .bind(offer_id())
            .fetch_one(&world.app.pool)
            .await
            .expect("offer row");
    assert_eq!(stored.expect("choice")["profile_id"], json!("pf_other"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_with_no_destination_is_not_accepted_once_the_shop_limits_where_it_ships(
    pool: PgPool,
) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let flat_listing = register_command(&seller.pubky, 5);
    homeserver.publish_registration(&flat_listing);
    let (status, body) = execute(&app, &seller.token, &flat_listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1,
            "recordType": "shop",
            "ownerPubky": seller.pubky,
            "name": "Flat Shop",
            "shipsToCountries": ["US"],
        }),
    );
    let accept = offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000a101");
    assert_error(
        &execute(&app, &seller.token, &accept).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("destination_country_required"),
    );
    let state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(offer_id())
        .fetch_one(&app.pool)
        .await
        .expect("offer state");
    assert_eq!(state, "pending", "the offer stays open for the buyer");
}

/// The registered row prices an offer when no listing record can be read. A
/// listing that does not ship has no shipping to charge, whatever flat rate it
/// still carries.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_registered_row_charges_shipping_only_to_a_listing_that_ships(pool: PgPool) {
    use marketplace_service::shipping::LineShipping;

    let app = test_app(pool).await;
    for (methods, expected) in [
        (json!(["pickup"]), LineShipping::Flat(0)),
        (json!(["shipping"]), LineShipping::Flat(1_200)),
        (json!(["shipping", "pickup"]), LineShipping::Flat(1_200)),
    ] {
        let seller = new_actor(&app).await;
        let mut listing = register_command(&seller.pubky, 5);
        listing["payload"]["fulfillment_methods"] = methods.clone();
        let (status, body) = execute(&app, &seller.token, &listing).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let row: marketplace_service::model::ListingRow = sqlx::query_as(&format!(
            "SELECT {} FROM listings WHERE aggregate_id = $1",
            marketplace_service::handlers::LISTING_COLUMNS
        ))
        .bind(listing_aggregate(&seller.pubky))
        .fetch_one(&app.pool)
        .await
        .expect("listing row");
        assert_eq!(row.shipping_minor, 1_200, "{methods}");
        assert_eq!(
            LineShipping::from_row(&row).expect("priced"),
            expected,
            "{methods}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_offer_on_row_only_zones_waits_for_the_listing_record(pool: PgPool) {
    let app = test_app_without_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut listing = register_command(&seller.pubky, 5);
    listing["payload"]["shipping_zones"] = two_zones();
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["payload"]["destination_country"] = json!("DE");
    offer["payload"]["shipping_choice"] = json!({
        "seller_pubky": seller.pubky,
        "listing_aggregate_id": listing_aggregate(&seller.pubky),
        "rate_id": "standard",
    });
    assert_error(
        &execute(&app, &buyer.token, &offer).await,
        StatusCode::CONFLICT,
        "SHIPPING_UNAVAILABLE",
        Some("listing_record_unavailable"),
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);
}

/// A seller republishes inline zones, keeping a rate id and raising its
/// amount, and counters the buyer's offer. The buyer's choice names no record
/// hash, so the counter may not move the pin to the new record.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_counter_cannot_move_an_inline_zone_offer_to_a_republished_record(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut listing = register_command(&seller.pubky, 5);
    listing["payload"]["shipping_zones"] = two_zones();
    homeserver.publish_registration(&listing);
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap(),
    );
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["payload"]["destination_country"] = json!("DE");
    offer["payload"]["shipping_choice"] = json!({
        "seller_pubky": seller.pubky,
        "listing_aggregate_id": listing_aggregate(&seller.pubky),
        "rate_id": "standard",
    });
    let (status, body) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let mut republished = two_zones();
    republished[0]["rates"][0] = flat("standard", "Standard", 9_000);
    let mut changed = register_command(&seller.pubky, 5);
    changed["payload"]["shipping_zones"] = republished;
    changed["payload"]["listing_revision"] = json!(2);
    homeserver.publish_registration(&changed);

    assert_error(
        &execute(&app, &seller.token, &counter_offer_command(1)).await,
        StatusCode::CONFLICT,
        "SHIPPING_CHANGED",
        None,
    );
    let (revision, state): (i64, String) =
        sqlx::query_as("SELECT revision, state FROM offers WHERE id = $1")
            .bind(offer_id())
            .fetch_one(&app.pool)
            .await
            .expect("offer row");
    assert_eq!((revision, state.as_str()), (1, "pending"));
}
