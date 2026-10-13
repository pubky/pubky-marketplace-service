//! `GET /v0/listings/{seller}/{listing_id}/availability`: public, state-only
//! read of one listing's stock state, so a viewer without a session still
//! sees another buyer's hold.

mod common;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use marketplace_service::clock::Clock;
use marketplace_service::http::build_router;
use marketplace_service::listing_availability::AvailabilityLimiter;
use marketplace_service::workers::run_once;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tower::util::ServiceExt;

use common::{
    execute, listing_aggregate, new_actor, register_command, reserve_command, sync_command,
    test_app, test_app_with_homeserver, TestApp,
};

fn router_for(app: &TestApp, limiter: Option<AvailabilityLimiter>) -> Router {
    let mut state = app.state.clone();
    if let Some(limiter) = limiter {
        state = state.with_listing_availability(Arc::new(limiter));
    }
    build_router(state).layer(MockConnectInfo(
        "127.0.0.1:41000".parse::<std::net::SocketAddr>().unwrap(),
    ))
}

async fn availability_for(
    router: &Router,
    seller_pubky: &str,
    listing_id: &str,
    client: Option<&str>,
    bearer: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut request = Request::builder().method("GET").uri(format!(
        "/v0/listings/{seller_pubky}/{listing_id}/availability"
    ));
    if let Some(client) = client {
        request = request.header("x-real-ip", client);
    }
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, headers, value)
}

async fn availability(
    router: &Router,
    seller_pubky: &str,
    listing_id: &str,
) -> (StatusCode, HeaderMap, Value) {
    availability_for(router, seller_pubky, listing_id, None, None).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_available_listing_answers_without_a_bearer_with_state_only(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    let router = router_for(&app, None);

    let (status, headers, body) = availability(&router, &seller.pubky, "boots_01").await;

    assert_eq!(status, StatusCode::OK, "unexpected: {body}");
    assert_eq!(body["state"], json!("available"));
    assert_eq!(
        body["aggregate_id"],
        json!(listing_aggregate(&seller.pubky))
    );
    assert_eq!(
        body["server_time"],
        json!(marketplace_service::clock::format_timestamp(
            app.clock.now()
        ))
    );
    let mut keys: Vec<&str> = body
        .as_object()
        .expect("body is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["aggregate_id", "server_time", "state"]);
    assert_eq!(headers.get("cache-control").unwrap(), "public, max-age=5");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_held_listing_reads_reserved_and_restocks_when_the_hold_lapses(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let router = router_for(&app, None);

    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::OK, "unexpected: {body}");

    let (status, _, body) = availability(&router, &seller.pubky, "boots_01").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], json!("reserved"));
    let rendered = body.to_string();
    assert!(!rendered.contains(&buyer.pubky), "buyer leaked: {rendered}");
    assert!(
        !rendered.contains("quantity") && !rendered.contains("expires"),
        "stock or deadline leaked: {rendered}"
    );

    app.clock.advance_seconds(601);
    let holder = uuid::Uuid::new_v4();
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    let (_, _, body) = availability(&router, &seller.pubky, "boots_01").await;
    assert_eq!(body["state"], json!("available"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sold_listing_reads_sold(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    sqlx::query(
        "UPDATE listings SET state = 'sold', available_quantity = 0, sold_quantity = 1 \
         WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&app.pool)
    .await
    .expect("mark sold");
    let router = router_for(&app, None);

    let (status, _, body) = availability(&router, &seller.pubky, "boots_01").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], json!("sold"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unknown_listings_are_not_found(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let router = router_for(&app, None);

    let (status, headers, body) = availability(&router, &seller.pubky, "nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");

    let (status, _, _) = availability(&router, "someoneelse", "boots_01").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_read_is_a_500_that_is_never_cached(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let router = router_for(&app, None);
    pool.close().await;

    let (status, headers, body) = availability(&router, "someoneelse", "boots_01").await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected: {body}"
    );
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_its_seller_deleted_is_not_found(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        json!({
            "title": "Winter boots",
            "revision": 1,
            "location": { "countryCode": "US", "region": null },
            "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
            "variants": [{
                "id": "variant_1",
                "enabled": true,
                "quantity": 3,
                "sku": null,
                "options": [],
            }],
            "shippingOptions": [{ "pricing": "free" }],
            "fulfillmentMethods": ["shipping"],
            "sale": {
                "acceptsOffers": true,
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
            },
        }),
    );
    let mut register = register_command(&seller.pubky, 3);
    register["command_id"] = json!(uuid::Uuid::new_v4());
    let (status, body) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");
    let router = router_for(&app, None);
    let (status, _, _) = availability(&router, &seller.pubky, "boots_01").await;
    assert_eq!(status, StatusCode::OK);

    homeserver.delete_record(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync after delete: {body}");
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));

    let (status, _, body) = availability(&router, &seller.pubky, "boots_01").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bearer_is_neither_required_nor_consulted(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let router = router_for(&app, None);

    let (status, _, body) = availability_for(
        &router,
        &seller.pubky,
        "boots_01",
        None,
        Some("not-a-real-token"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn each_client_has_its_own_rate_limit_with_retry_after(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let limiter =
        AvailabilityLimiter::new(Some(axum::http::HeaderName::from_static("x-real-ip")), 60);
    let router = router_for(&app, Some(limiter));

    for attempt in 0..30 {
        let (status, _, body) = availability_for(
            &router,
            &seller.pubky,
            "boots_01",
            Some("203.0.113.7"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "attempt {attempt}: {body}");
    }
    let (status, headers, body) = availability_for(
        &router,
        &seller.pubky,
        "boots_01",
        Some("203.0.113.7"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("rate_limited"));
    let retry_after: u64 = headers
        .get("retry-after")
        .expect("retry-after header")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(retry_after >= 1);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");

    let (status, _, _) = availability_for(
        &router,
        &seller.pubky,
        "boots_01",
        Some("203.0.113.8"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "another client is unaffected");

    app.clock.advance_seconds(retry_after as i64 + 1);
    let (status, _, _) = availability_for(
        &router,
        &seller.pubky,
        "boots_01",
        Some("203.0.113.7"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the bucket refills");
}
