//! Migration 0095: the zone-priced shipping columns. A listing references a
//! profile or carries its own zones, never both; the destination country of an
//! order, offer or bid is an uppercase ISO 3166-1 alpha-2 code or NULL; every
//! check is added NOT VALID so no table is scanned under the ALTER's lock; and
//! the file reruns as a no-op that keeps every stored column.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

const MIGRATION_0095: &str = include_str!("../migrations/0095_shipping_zones.sql");

async fn constraint_of(result: Result<sqlx::postgres::PgQueryResult, sqlx::Error>) -> String {
    let error = result.expect_err("the row is refused");
    error
        .as_database_error()
        .and_then(|error| error.constraint())
        .expect("a named constraint refused it")
        .to_string()
}

async fn listing_aggregate(pool: &PgPool) -> String {
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let (status, body) = common::execute(
        &app,
        &seller.token,
        &common::register_command(&seller.pubky, 3),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    common::listing_aggregate(&seller.pubky)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_has_a_profile_or_zones_not_both(pool: PgPool) {
    let aggregate = listing_aggregate(&pool).await;
    let (profile, zones): (Option<String>, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT shipping_profile_id, shipping_zones FROM listings WHERE aggregate_id = $1",
    )
    .bind(&aggregate)
    .fetch_one(&pool)
    .await
    .expect("listing row");
    assert_eq!(
        (profile, zones),
        (None, None),
        "existing listings keep the flat rate"
    );

    sqlx::query("UPDATE listings SET shipping_profile_id = 'pf_world' WHERE aggregate_id = $1")
        .bind(&aggregate)
        .execute(&pool)
        .await
        .expect("a profile alone");
    sqlx::query("UPDATE listings SET shipping_profile_id = NULL, shipping_zones = $2 WHERE aggregate_id = $1")
        .bind(&aggregate)
        .bind(json!([{ "zoneId": "eu" }]))
        .execute(&pool)
        .await
        .expect("zones alone");
    let both =
        sqlx::query("UPDATE listings SET shipping_profile_id = 'pf_world' WHERE aggregate_id = $1")
            .bind(&aggregate)
            .execute(&pool)
            .await;
    assert_eq!(
        constraint_of(both).await,
        "listings_shipping_profile_or_zones"
    );
}

async fn offer_and_bid_rows(app: &common::TestApp) -> (Uuid, Uuid) {
    let seller = common::new_actor(app).await;
    let buyer = common::new_actor(app).await;
    let (status, body) = common::execute(
        app,
        &seller.token,
        &common::register_command(&seller.pubky, 3),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let (status, body) = common::execute(
        app,
        &buyer.token,
        &common::create_offer_command(&seller.pubky, 1),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");

    let auctioneer = common::new_actor(app).await;
    let bidder = common::new_actor(app).await;
    let (status, body) = common::execute(
        app,
        &auctioneer.token,
        &common::register_auction_command(&auctioneer.pubky),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let (status, body) = common::execute(
        app,
        &bidder.token,
        &common::place_bid_command(&auctioneer.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");

    let offer: Uuid = sqlx::query_scalar("SELECT id FROM offers")
        .fetch_one(&app.pool)
        .await
        .expect("offer row");
    let bid: Uuid = sqlx::query_scalar("SELECT id FROM bids")
        .fetch_one(&app.pool)
        .await
        .expect("bid row");
    (offer, bid)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_destination_country_is_an_uppercase_code_or_null(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let buyer = common::new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    let (offer_id, bid_id) = offer_and_bid_rows(&app).await;

    for (table, id) in [("orders", order_id), ("offers", offer_id), ("bids", bid_id)] {
        let update = format!("UPDATE {table} SET destination_country = $2 WHERE id = $1");
        for country in [Some("DE"), None] {
            sqlx::query(&update)
                .bind(id)
                .bind(country)
                .execute(&pool)
                .await
                .unwrap_or_else(|error| panic!("{table} accepts {country:?}: {error}"));
        }
        for country in ["de", "DEU", "D", "1A", ""] {
            let refused = sqlx::query(&update)
                .bind(id)
                .bind(country)
                .execute(&pool)
                .await;
            assert_eq!(
                constraint_of(refused).await,
                format!("{table}_destination_country_format"),
                "{table} {country:?}"
            );
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_check_is_added_not_valid_to_the_table_it_guards(pool: PgPool) {
    let mut checks: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT conname::text, conrelid::regclass::text, convalidated FROM pg_constraint \
         WHERE conname IN ('listings_shipping_profile_or_zones', \
                           'orders_destination_country_format', \
                           'offers_destination_country_format', \
                           'bids_destination_country_format') \
         ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("constraints");
    checks.sort();
    assert_eq!(
        checks,
        vec![
            (
                "bids_destination_country_format".into(),
                "bids".into(),
                false
            ),
            (
                "listings_shipping_profile_or_zones".into(),
                "listings".into(),
                false
            ),
            (
                "offers_destination_country_format".into(),
                "offers".into(),
                false
            ),
            (
                "orders_destination_country_format".into(),
                "orders".into(),
                false
            ),
        ]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_every_stored_column(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let buyer = common::new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    let (offer_id, bid_id) = offer_and_bid_rows(&app).await;
    let aggregate = common::listing_aggregate(&seller.pubky);

    let quotes = json!([{ "zone_id": "eu", "rate_id": "ground", "amount_minor": 500 }]);
    sqlx::query("UPDATE listings SET shipping_profile_id = 'pf_world' WHERE aggregate_id = $1")
        .bind(&aggregate)
        .execute(&pool)
        .await
        .expect("a profile");
    sqlx::query("UPDATE orders SET destination_country = 'DE', shipping_quotes = $2 WHERE id = $1")
        .bind(order_id)
        .bind(&quotes)
        .execute(&pool)
        .await
        .expect("order columns");
    sqlx::query(
        "UPDATE offers SET destination_country = 'FR', shipping_choice = $2, \
         accepted_shipping_quotes = $3 WHERE id = $1",
    )
    .bind(offer_id)
    .bind(json!({ "profile_id": "pf_world", "rate_id": "ground" }))
    .bind(&quotes)
    .execute(&pool)
    .await
    .expect("offer columns");
    sqlx::query("UPDATE bids SET destination_country = 'US', shipping_quote = $2 WHERE id = $1")
        .bind(bid_id)
        .bind(json!({ "zone_id": "us", "rate_id": "ground", "amount_minor": 900 }))
        .execute(&pool)
        .await
        .expect("bid columns");

    let snapshot = || async {
        let listing: (Option<String>, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT shipping_profile_id, shipping_zones FROM listings WHERE aggregate_id = $1",
        )
        .bind(&aggregate)
        .fetch_one(&pool)
        .await
        .expect("listing");
        let order: (Option<String>, Option<serde_json::Value>) =
            sqlx::query_as("SELECT destination_country, shipping_quotes FROM orders WHERE id = $1")
                .bind(order_id)
                .fetch_one(&pool)
                .await
                .expect("order");
        let offer: (
            Option<String>,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
        ) = sqlx::query_as(
            "SELECT destination_country, shipping_choice, accepted_shipping_quotes \
             FROM offers WHERE id = $1",
        )
        .bind(offer_id)
        .fetch_one(&pool)
        .await
        .expect("offer");
        let bid: (Option<String>, Option<serde_json::Value>) =
            sqlx::query_as("SELECT destination_country, shipping_quote FROM bids WHERE id = $1")
                .bind(bid_id)
                .fetch_one(&pool)
                .await
                .expect("bid");
        (listing, order, offer, bid)
    };
    let before = snapshot().await;
    assert_eq!(before.0 .0.as_deref(), Some("pf_world"));
    assert_eq!(before.1 .0.as_deref(), Some("DE"));
    assert_eq!(before.2 .0.as_deref(), Some("FR"));
    assert_eq!(before.3 .0.as_deref(), Some("US"));

    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0095)
        .execute(&mut *tx)
        .await
        .expect("0095 must be directly rerunnable");
    tx.commit().await.expect("commit");
    assert_eq!(snapshot().await, before);

    let refused = sqlx::query("UPDATE bids SET destination_country = 'us' WHERE id = $1")
        .bind(bid_id)
        .execute(&pool)
        .await;
    assert_eq!(
        constraint_of(refused).await,
        "bids_destination_country_format",
        "the rerun keeps the checks enforcing"
    );
}
