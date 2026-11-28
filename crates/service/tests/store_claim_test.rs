//! Single-seller first-boot claim, unclaimed fail-closed behaviour, boot
//! binding transitions and the empty-store release helper.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderName, Request, StatusCode};
use base64::Engine;
use common::{
    cancel_drop_command, checkout_command_with_id, close_auction_command, counter_offer_command,
    create_offer_command, execute, listing_aggregate, new_actor, offer_action, order_command,
    payment_command, place_bid_command, prepare_locks_command, register_command,
    register_locks_command, release_drop_command, reserve_command, send, send_with_headers,
    sync_command, sync_drop_command, test_app_with_config, TestActor, TestApp, OFFER_COMMAND_ID,
    TEST_BUNDLE_ID,
};
use http_body_util::BodyExt;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::grant_session::GrantSessionConfig;
use marketplace_service::store_claim::{ClaimCode, StoreLimiters, CLAIM_CODE_SHAPE_MESSAGE};
use marketplace_service::store_seller::{
    other_seller_rows, require_claim_grant_sessions, resolve_store, resolve_worker_store,
    route_scope, store_mode_from_lookup, take_binding_lock, Active, RouteScope, StoreBinding,
    StoreMode, ROUTES, STORE_BINDING_LOCK_KEY,
};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use tower::ServiceExt;
use uuid::Uuid;

const CODE: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const WRONG_CODE: &str = "ZYXWVTSRQPNMKJHGFEDCBA9876543210";

fn claim_config(code: &str) -> Config {
    let mut config = Config::for_tests();
    config.store_mode = StoreMode::SingleSeller { pinned: None };
    config.store_claim_code = Some(ClaimCode::parse(code).expect("valid fixture code"));
    config
}

async fn unclaimed(pool: PgPool) -> TestApp {
    test_app_with_config(pool, claim_config(CODE)).await
}

async fn grant_session(app: &TestApp, actor: &TestActor) {
    sqlx::query(
        "UPDATE auth_sessions SET source_grant_id = 'grant-fixture', \
         source_client_id = 'client-fixture', created_at = $2 WHERE pubky = $1",
    )
    .bind(&actor.pubky)
    .bind(app.clock.now())
    .execute(&app.pool)
    .await
    .expect("grant-session fixture");
}

async fn claim(app: &TestApp, actor: &TestActor, code: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/store/claim",
        Some(&actor.token),
        &json!({ "claim_code": code }),
    )
    .await
}

fn app_with_state(app: &TestApp, state: AppState) -> TestApp {
    TestApp {
        router: marketplace_service::http::build_router(state.clone()),
        pool: app.pool.clone(),
        clock: app.clock.clone(),
        state,
    }
}

async fn insert_binding(pool: &PgPool, seller: &str, bound_by: &str) {
    sqlx::query(
        "INSERT INTO store_seller_binding (singleton, seller_pubky, bound_by) \
         VALUES (TRUE, $1, $2)",
    )
    .bind(seller)
    .bind(bound_by)
    .execute(pool)
    .await
    .expect("binding fixture");
}

async fn send_any(
    router: axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = router
        .oneshot(request.body(Body::from("{}")).expect("request builds"))
        .await
        .expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, body)
}

async fn get_store_from(router: axum::Router, address: &str) -> (StatusCode, HeaderMap, Value) {
    let request = Request::builder()
        .method("GET")
        .uri("/v1/store")
        .header("x-test-client", address)
        .body(Body::empty())
        .expect("request builds");
    let response = router.oneshot(request).await.expect("request executes");
    let status = response.status();
    let headers = response.headers().clone();
    let body = serde_json::from_slice(
        &response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes(),
    )
    .expect("store response is JSON");
    (status, headers, body)
}

fn concrete_path(path: &str, pubky: &str) -> String {
    path.replace("{pubky}", pubky)
        .replace("{seller_pubky}", pubky)
        .replace("{seller}", pubky)
        .replace("{aggregate_id}", &format!("listing:{pubky}_boots_01"))
        .replace("{listing_id}", "boots_01")
        .replace("{line_index}", "0")
        .replace("{connection_id}", &Uuid::new_v4().to_string())
        .replace("{change_id}", &Uuid::new_v4().to_string())
        .replace("{flow_id}", &Uuid::new_v4().to_string())
        .replace("{drop_id}", "winter_drop")
        .replace("{method}", "paypal")
        .replace("{order}", &Uuid::new_v4().to_string())
        .replace("{id}", &Uuid::new_v4().to_string())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unclaimed_routes_fail_closed_and_claim_needs_no_restart(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    for rule in ROUTES {
        let path = concrete_path(rule.path, &actor.pubky);
        let session_admin = rule.path.starts_with("/v1/auth/sessions/");
        let disposable = if session_admin {
            Some(new_actor(&app).await)
        } else {
            None
        };
        let caller = disposable.as_ref().unwrap_or(&actor);
        let (status, body) =
            send_any(app.router.clone(), rule.method, &path, Some(&caller.token)).await;
        if matches!(rule.scope, RouteScope::Public(_))
            || rule.path == "/v1/store/claim"
            || session_admin
        {
            assert_ne!(
                body["error"]["code"],
                json!("STORE_UNCLAIMED"),
                "{} {}",
                rule.method,
                rule.path
            );
        } else {
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{} {}: {body}",
                rule.method,
                rule.path
            );
            assert_eq!(
                body["error"]["code"],
                json!("STORE_UNCLAIMED"),
                "{} {}",
                rule.method,
                rule.path
            );
        }
    }
    assert!(
        other_seller_rows(&pool, None)
            .await
            .expect("seller rows")
            .is_empty(),
        "no public or guarded route can create a seller row while unclaimed"
    );
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&actor.token),
        &json!({ "listings": [{ "seller_pubky": actor.pubky, "listing_id": "boots_01" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let spent: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = 'listing.sync_many'",
    )
    .bind(&actor.pubky)
    .fetch_one(&pool)
    .await
    .expect("limiter rows");
    assert_eq!(spent, 0, "the whole batch is refused before its limiter");

    let (status, store) = send(app.router.clone(), "GET", "/v1/store", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{store}");
    assert_eq!(
        store,
        json!({ "mode": "single_seller", "state": "unclaimed", "seller": null })
    );
    let (_, health) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(health["store"], store);

    grant_session(&app, &actor).await;
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["store"]["seller"], json!(actor.pubky));
    assert!(body["store"]["claimed_at"].is_string());
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("STORE_ALREADY_CLAIMED"));
    assert_eq!(body["seller"], json!(actor.pubky));

    let (status, body) = execute(&app, &actor.token, &register_command(&actor.pubky, 1)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the same AppState serves immediately: {body}"
    );
    let (status, body) = execute(
        &app,
        &actor.token,
        &sync_command(&actor.pubky, "boots_01", 2),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the same AppState serves listing.sync immediately: {body}"
    );
    let other = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &other.token,
        &sync_command(&other.pubky, "boots_01", 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("SELLER_NOT_SERVED"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn claim_rejects_code_session_source_capability_and_staleness(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_session_source"));

    grant_session(&app, &actor).await;
    let (status, body) = claim(&app, &actor, WRONG_CODE).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_code_invalid"));

    let capabilities: String =
        sqlx::query_scalar("SELECT capabilities FROM auth_sessions WHERE pubky = $1")
            .bind(&actor.pubky)
            .fetch_one(&pool)
            .await
            .expect("session capabilities");
    sqlx::query("UPDATE auth_sessions SET capabilities = '' WHERE pubky = $1")
        .bind(&actor.pubky)
        .execute(&pool)
        .await
        .expect("capability fixture");
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_session_capability"));
    sqlx::query("UPDATE auth_sessions SET capabilities = $2, created_at = $3 WHERE pubky = $1")
        .bind(&actor.pubky)
        .bind(capabilities)
        .bind(app.clock.now() - chrono::Duration::seconds(901))
        .execute(&pool)
        .await
        .expect("stale fixture");
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_session_stale"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn eight_parallel_claims_have_one_winner_and_replicas_converge(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;
    let replica_state = AppState::new(pool.clone(), app.clock.clone(), claim_config(CODE));
    let replica = TestApp {
        router: marketplace_service::http::build_router(replica_state.clone()),
        pool: pool.clone(),
        clock: app.clock.clone(),
        state: replica_state,
    };
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let router = app.router.clone();
        let token = actor.token.clone();
        tasks.spawn(async move {
            send(
                router,
                "POST",
                "/v1/store/claim",
                Some(&token),
                &json!({ "claim_code": CODE }),
            )
            .await
            .0
        });
    }
    let mut statuses = Vec::new();
    while let Some(status) = tasks.join_next().await {
        statuses.push(status.expect("claim task"));
    }
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        7,
        "{statuses:?}"
    );

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    let (status, body) = send(
        replica.router.clone(),
        "GET",
        "/v1/store",
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], json!("claimed"));
    assert_eq!(body["seller"], json!(actor.pubky));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn claim_limit_is_per_address_and_authentication_precedes_mode(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;
    for _ in 0..10 {
        let (status, _) = claim(&app, &actor, WRONG_CODE).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (status, headers, _) = send_with_headers(
        app.router.clone(),
        "POST",
        "/v1/store/claim",
        Some(&actor.token),
        &json!({ "claim_code": WRONG_CODE }),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(header::RETRY_AFTER));
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");

    let marketplace = common::test_app(pool.clone()).await;
    let (status, _) = send(
        marketplace.router.clone(),
        "POST",
        "/v1/store/claim",
        None,
        &json!({ "claim_code": CODE }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn pinned_boot_writes_the_binding_and_marketplace_guard_rolls_back(pool: PgPool) {
    let seller = marketplace_domain::pubky::encode_pubky(&[21; 32]);
    let mistyped = marketplace_domain::pubky::encode_pubky(&[22; 32]);
    let pinned = StoreMode::SingleSeller {
        pinned: Some(seller.clone()),
    };
    let store = resolve_store(&pool, &pinned, None)
        .await
        .expect("pinned boot");
    assert_eq!(store.active(), Active::Claimed(seller.clone()));
    let error = resolve_store(&pool, &StoreMode::Marketplace, None)
        .await
        .expect_err("marketplace guard");
    assert!(
        error
            .to_string()
            .contains("this database is bound to a single-seller store"),
        "{error}"
    );
    let error = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(mistyped.clone()),
        },
        None,
    )
    .await
    .expect_err("mismatched pin");
    let message = error.to_string();
    assert!(message.contains("does not match"), "{message}");
    assert!(message.contains(&seller[..8]), "{message}");
    assert!(message.contains(&mistyped[..8]), "{message}");
    let rebound = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect("the corrected claim-mode boot sees the env binding");
    assert_eq!(rebound.active(), Active::Claimed(seller));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mistyped_pin_on_populated_database_commits_nothing(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let actor = new_actor(&app).await;
    let (status, body) = execute(&app, &actor.token, &register_command(&actor.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mistyped = marketplace_domain::pubky::encode_pubky(&[31; 32]);
    let error = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(mistyped.clone()),
        },
        None,
    )
    .await
    .expect_err("other seller rows refuse the pin");
    assert!(error.to_string().contains("other sellers"), "{error}");
    assert!(marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding read")
        .is_none());
    let corrected = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(actor.pubky.clone()),
        },
        None,
    )
    .await
    .expect("the corrected pin boots");
    assert_eq!(corrected.active(), Active::Claimed(actor.pubky));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_requires_confirmation_and_an_empty_store(pool: PgPool) {
    let seller = marketplace_domain::pubky::encode_pubky(&[41; 32]);
    resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller.clone()),
        },
        None,
    )
    .await
    .expect("pinned boot");
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("single admin connection");
    let second = PgConnection::connect_with(&admin.connect_options())
        .await
        .expect("explicit second connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &seller[..8])
        .await
        .expect_err("another open service connection refuses release");
    assert!(
        error.to_string().contains("another service connection"),
        "{error}"
    );
    drop(second);
    let error = marketplace_service::store_binding_admin::release(&admin, "xxxxxxxx")
        .await
        .expect_err("wrong confirmation");
    assert!(error.to_string().contains("does not match"), "{error}");
    let released = marketplace_service::store_binding_admin::release(&admin, &seller[..8])
        .await
        .expect("empty store releases");
    assert_eq!(released, seller);
    let fresh_boot = resolve_store(
        &admin,
        &StoreMode::SingleSeller { pinned: None },
        Some(ClaimCode::parse(CODE).unwrap()),
    )
    .await
    .expect("fresh boot is unclaimed");
    assert_eq!(fresh_boot.active(), Active::Unclaimed);
    let fresh = unclaimed(admin.clone()).await;
    let actor = new_actor(&fresh).await;
    grant_session(&fresh, &actor).await;
    let (status, body) = claim(&fresh, &actor, CODE).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a fresh claim follows release: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn env_pinned_claim_is_already_claimed(pool: PgPool) {
    let base = common::test_app(pool.clone()).await;
    let actor = new_actor(&base).await;
    let mut config = Config::for_tests();
    config.store_mode = StoreMode::SingleSeller {
        pinned: Some(actor.pubky.clone()),
    };
    let state = AppState::new(pool, base.clock.clone(), config);
    let pinned = TestApp {
        router: marketplace_service::http::build_router(state.clone()),
        pool: base.pool.clone(),
        clock: base.clock.clone(),
        state,
    };
    let (status, body) = claim(&pinned, &actor, CODE).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("STORE_ALREADY_CLAIMED"));
}

#[test]
fn active_values_are_explicit_and_terminal_by_construction() {
    for (active, expected) in [
        (Active::Off, "off"),
        (Active::Unclaimed, "unclaimed"),
        (
            Active::Claimed(marketplace_domain::pubky::encode_pubky(&[9; 32])),
            "claimed",
        ),
    ] {
        let binding = StoreBinding::new(active.clone(), None);
        assert_eq!(binding.active(), active, "{expected}");
    }
}

fn fresh(mut command: Value) -> Value {
    command["command_id"] = json!(Uuid::new_v4().to_string());
    command
}

fn parsed_command_kinds() -> BTreeSet<String> {
    let source = include_str!("../../domain/src/commands.rs");
    let arms = source
        .split_once("let payload = match envelope.kind.as_str() {")
        .expect("parse_command dispatch in commands.rs")
        .1
        .split_once("_ => return Err(")
        .expect("parse_command fallback arm")
        .0;
    arms.lines()
        .filter_map(|line| {
            let (kind, _) = line.trim().strip_prefix('"')?.split_once("\" =>")?;
            Some(kind.to_string())
        })
        .collect()
}

/// One parseable envelope for every command in the domain catalog. None of
/// these subjects needs to exist: the unclaimed guard runs before resolution.
fn unclaimed_command_cases(seller: &str) -> BTreeMap<String, Value> {
    let order = Uuid::new_v4().to_string();
    let payment = Uuid::new_v4().to_string();
    let listing = listing_aggregate(seller);
    let on_order = |kind: &str, payload: Value| fresh(order_command(kind, &order, 1, payload, 1));
    let on_listing = |kind: &str, payload: Value| {
        json!({
            "version": 1,
            "command_id": Uuid::new_v4().to_string(),
            "aggregate_id": listing,
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": kind,
            "payload": payload,
        })
    };
    let mut cases = BTreeMap::new();
    let mut add = |command: Value| {
        let kind = command["kind"].as_str().expect("kind").to_string();
        assert!(
            cases.insert(kind.clone(), command).is_none(),
            "{kind} twice"
        );
    };
    add(fresh(register_command(seller, 1)));
    add(fresh(sync_command(seller, "boots_01", 1)));
    add(fresh(sync_drop_command(seller, "winter_drop", 1)));
    add(fresh(cancel_drop_command(seller, "winter_drop", 1, 1)));
    add(fresh(release_drop_command(seller, "winter_drop", 1, 1)));
    add(fresh(reserve_command(seller, 1, 1, 1)));
    add(checkout_command_with_id(
        seller,
        &Uuid::new_v4().to_string(),
    ));
    add(fresh(create_offer_command(seller, 1)));
    add(fresh(counter_offer_command(1)));
    for kind in ["offer.accept", "offer.reject", "offer.withdraw"] {
        add(offer_action(kind, 1, &Uuid::new_v4().to_string()));
    }
    add(json!({
        "version": 1,
        "command_id": Uuid::new_v4().to_string(),
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": Uuid::new_v4().to_string(),
            "listing_aggregate_id": listing,
            "listing_revision": 1,
            "listing_record_sha256": "a".repeat(64),
            "variant_id": "boots_01",
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Example",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    }));
    add(fresh(place_bid_command(seller, 1, 7_000, 1)));
    add(fresh(close_auction_command(seller, 1, 1)));
    add(fresh(payment_command(&payment, 2, "confirmed", 1, 1)));
    add(fresh(prepare_locks_command(&payment, 1)));
    add(fresh(register_locks_command(
        &payment,
        2,
        TEST_BUNDLE_ID,
        "",
        1,
    )));
    add(on_order(
        "order.cancel_request",
        json!({ "reason": "Changed mind" }),
    ));
    add(on_order("order.cancel_approve", json!({})));
    add(on_order(
        "fulfillment.ship",
        json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
    ));
    add(on_order(
        "fulfillment.update_tracking",
        json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-2" }),
    ));
    add(on_order("fulfillment.confirm_delivery", json!({})));
    add(on_order(
        "order.set_delivery_email",
        json!({ "delivery_email": "buyer@example.com" }),
    ));
    add(on_order(
        "fulfillment.deliver_digital",
        json!({ "channel": "email" }),
    ));
    add(on_order("fulfillment.release_digital", json!({})));
    add(on_order("fulfillment.mark_ready", json!({})));
    add(on_order("fulfillment.confirm_pickup", json!({})));
    add(on_order(
        "return.request",
        json!({ "reason": "Item not as described", "requested_amount_minor": 100 }),
    ));
    add(on_order("return.approve", json!({})));
    add(on_order("return.cancel", json!({})));
    add(on_order("return.receive", json!({})));
    add(on_order(
        "refund.record_external",
        json!({ "amount_minor": 100, "transaction_id": "tx-refund-1" }),
    ));
    add(on_order(
        "refund.confirm_destination",
        json!({ "address": format!("0x{}", "ab".repeat(20)) }),
    ));
    add(on_order(
        "review.create",
        json!({ "rating": 5, "text": "Great" }),
    ));
    add(on_order(
        "review.update",
        json!({ "rating": 4, "text": "Good" }),
    ));
    add(on_order("review.withdraw", json!({})));
    add(on_listing(
        "pickup_details.set",
        json!({
            "expected_version": 0,
            "details": {
                "kind": "spot",
                "spot": "Main Station",
                "instructions": "Ask for the blue backpack.",
                "availability": {
                    "windows": [{ "day": "sat", "start": "10:00", "end": "14:00" }],
                    "zone": "Europe/Berlin",
                },
            },
        }),
    ));
    add(on_listing(
        "pickup_details.clear",
        json!({ "expected_version": 0 }),
    ));
    add(on_listing(
        "digital_delivery.set",
        json!({ "expected_version": 0, "delivery": { "kind": "text", "text": "LICENCE" } }),
    ));
    add(on_listing(
        "digital_delivery.clear",
        json!({ "expected_version": 0 }),
    ));
    add(json!({
        "version": 1,
        "command_id": Uuid::new_v4().to_string(),
        "aggregate_id": format!("seller_settings:{seller}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "attestation.set_band_consent",
        "payload": { "allows_amount_band": true },
    }));
    cases
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_command_kind_is_refused_while_unclaimed(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    let cases = unclaimed_command_cases(&actor.pubky);
    assert_eq!(
        cases.keys().cloned().collect::<BTreeSet<_>>(),
        parsed_command_kinds(),
        "one unclaimed case per command kind in the domain catalog"
    );
    for (kind, command) in cases {
        let (status, body) = execute(&app, &actor.token, &command).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{kind}: {body}");
        assert_eq!(body["error"]["code"], json!("STORE_UNCLAIMED"), "{kind}");
    }
    assert!(
        other_seller_rows(&pool, None)
            .await
            .expect("seller rows")
            .is_empty(),
        "no command can create a seller row while unclaimed"
    );
}

fn router_routes() -> BTreeSet<(String, String)> {
    let source = include_str!("../src/http.rs");
    let body = source
        .split_once("pub fn build_router(")
        .expect("build_router in http.rs")
        .1;
    let mut routes = BTreeSet::new();
    for (start, _) in body.match_indices(".route(") {
        let args_start = start + ".route(".len();
        let mut depth = 1;
        let mut end = None;
        for (offset, character) in body[args_start..].char_indices() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(args_start + offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let args = &body[args_start..end.expect(".route( is closed")];
        let path = args
            .split('"')
            .nth(1)
            .expect(".route( names a path literal");
        let handlers = &args[args.find(',').expect(".route( has handlers")..];
        let before = routes.len();
        for method in ["get", "post", "put", "patch", "delete"] {
            for (index, _) in handlers.match_indices(&format!("{method}(")) {
                let preceded = handlers[..index]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == ':');
                if !preceded {
                    routes.insert((method.to_ascii_uppercase(), path.to_string()));
                }
            }
        }
        assert!(routes.len() > before, "no method found for route {path}");
    }
    routes
}

#[test]
fn route_classification_is_complete_and_store_routes_have_their_specified_scopes() {
    let router = router_routes();
    let listed: BTreeSet<(String, String)> = ROUTES
        .iter()
        .map(|rule| (rule.method.to_string(), rule.path.to_string()))
        .collect();
    assert_eq!(router, listed, "every router route needs a ROUTES row");
    assert!(matches!(
        route_scope("GET", "/v1/store"),
        Some(RouteScope::Public(_))
    ));
    assert_eq!(
        route_scope("POST", "/v1/store/claim"),
        Some(RouteScope::Open(
            "signed in; the actor claims the unclaimed store for itself"
        ))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn populated_unclaimed_and_claimed_boots_refuse_other_seller_rows(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let actor = new_actor(&app).await;
    let (status, body) = execute(&app, &actor.token, &register_command(&actor.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let rows = other_seller_rows(&pool, None)
        .await
        .expect("all-seller scan");
    assert_eq!(
        rows.listings, 1,
        "the empty pubky sentinel counts every seller"
    );
    assert_eq!(rows.sellers.len(), 1);
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("a populated database cannot become an open claim");
    assert_eq!(
        error.to_string(),
        "single-seller mode has no seller but the database holds rows of 1 sellers; \
         set STORE_SELLER_PUBKY or use an empty database"
    );

    let bound = marketplace_domain::pubky::encode_pubky(&[51; 32]);
    insert_binding(&pool, &bound, "claim").await;
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("state 2 refuses another seller's rows");
    assert!(error.to_string().contains("other sellers"), "{error}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ui_claim_marketplace_guard_covers_removed_and_explicit_modes(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for configured in [None, Some("marketplace")] {
        let mode = store_mode_from_lookup(&|name| {
            (name == "STORE_MODE")
                .then(|| configured.map(str::to_string))
                .flatten()
        })
        .expect("marketplace configuration parses");
        let error = resolve_store(&pool, &mode, None)
            .await
            .expect_err("a UI-claimed database cannot boot as a marketplace");
        assert!(
            error.to_string().contains(&actor.pubky[..8]),
            "guard names the bound seller: {error}"
        );
    }
}

#[test]
fn claim_code_boot_shapes_use_the_config_env_lookup() {
    let mode = StoreMode::SingleSeller { pinned: None };
    let grouped = CODE
        .as_bytes()
        .chunks(4)
        .map(|group| std::str::from_utf8(group).expect("ASCII"))
        .collect::<Vec<_>>()
        .join("-");
    let lowercase_hex = "0123456789abcdef".repeat(4);
    for accepted in [grouped, lowercase_hex.clone(), lowercase_hex.to_uppercase()] {
        let code = marketplace_service::store_claim::claim_code_from_lookup(&mode, &|name| {
            (name == "STORE_CLAIM_TOKEN").then(|| accepted.clone())
        })
        .expect("accepted boot code");
        assert!(code.is_some());
    }
    for refused in [
        CODE[..26].to_string(),
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        "A".repeat(32),
        format!("{}!", &CODE[..31]),
    ] {
        let error = marketplace_service::store_claim::claim_code_from_lookup(&mode, &|name| {
            (name == "STORE_CLAIM_TOKEN").then(|| refused.clone())
        })
        .expect_err("invalid boot code shape");
        assert_eq!(error.to_string(), CLAIM_CODE_SHAPE_MESSAGE);
    }
}

#[test]
fn claim_mode_requires_grant_sessions_with_a_client_id() {
    let mode = StoreMode::SingleSeller { pinned: None };
    let expected = "single-seller claim mode requires MARKETPLACE_GRANT_SESSION_ENABLED=true \
                    and at least one MARKETPLACE_GRANT_SESSION_CLIENT_IDS entry";
    assert_eq!(
        require_claim_grant_sessions(&mode, None)
            .expect_err("disabled grant sessions refuse claim mode")
            .to_string(),
        expected
    );
    let empty = GrantSessionConfig {
        audience: "https://svc.test".to_string(),
        client_ids: Vec::new(),
        max_proof_age_seconds: 120,
    };
    assert_eq!(
        require_claim_grant_sessions(&mode, Some(&empty))
            .expect_err("an empty client list refuses claim mode")
            .to_string(),
        expected
    );
    let enabled = GrantSessionConfig::new("https://svc.test", "shop.pubky.app", 120)
        .expect("grant configuration");
    require_claim_grant_sessions(&mode, Some(&enabled)).expect("claim mode boots when configured");
    require_claim_grant_sessions(&StoreMode::Marketplace, None)
        .expect("the prerequisite is specific to claim mode");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn grant_flow_settlement_session_cannot_claim(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let (keypair, pubky) = common::random_keypair();
    let bearer = [73u8; 32];
    let now = app.clock.now();
    // This is the grant.rs settlement insert: unlike the grant-session mint,
    // it deliberately records neither source_grant_id nor source_client_id.
    sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(marketplace_service::auth::hash_token(&bearer))
    .bind(&pubky)
    .bind("/:rw")
    .bind(now)
    .bind(now + chrono::Duration::seconds(3_600))
    .execute(&pool)
    .await
    .expect("grant-flow session fixture");
    let actor = TestActor {
        keypair,
        pubky,
        token: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bearer),
    };
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_session_source"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn connector_worker_waits_for_a_claim_and_enforces_marketplace_guard(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;
    let worker_pool = pool.clone();
    let mut worker = tokio::spawn(async move {
        resolve_worker_store(
            &worker_pool,
            &StoreMode::SingleSeller { pinned: None },
            None,
            Duration::from_millis(10),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(60), &mut worker)
            .await
            .is_err(),
        "the worker stays alive and waits while unclaimed"
    );
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let store = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("the worker notices the claim")
        .expect("worker task")
        .expect("worker boots");
    assert_eq!(store.active(), Active::Claimed(actor.pubky));

    let error = resolve_worker_store(
        &pool,
        &StoreMode::Marketplace,
        None,
        Duration::from_millis(10),
    )
    .await
    .expect_err("worker marketplace guard");
    assert!(error.to_string().contains("bound to a single-seller store"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_many_refusal_leaves_the_exact_limiter_budget_untouched(pool: PgPool) {
    let mut config = claim_config(CODE);
    config.automation_rate_limit_per_minute = 1;
    config.automation_rate_limit_burst_multiplier = 1;
    let app = test_app_with_config(pool.clone(), config).await;
    let actor = new_actor(&app).await;
    let batch = json!({
        "listings": [
            { "seller_pubky": actor.pubky, "listing_id": "boots_01" },
            { "seller_pubky": actor.pubky, "listing_id": "boots_02" }
        ]
    });
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&actor.token),
        &batch,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.get("results").is_none(), "the batch is refused whole");
    let buckets: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = 'listing.sync_many'",
    )
    .bind(&actor.pubky)
    .fetch_one(&pool)
    .await
    .expect("limiter rows");
    assert_eq!(buckets, 0, "not even the request admission token was spent");

    grant_session(&app, &actor).await;
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let one_item = json!({
        "listings": [{ "seller_pubky": actor.pubky, "listing_id": "boots_01" }]
    });
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&actor.token),
        &one_item,
    )
    .await;
    assert_ne!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&actor.token),
        &one_item,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "the full one-token budget was still available after the claim"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn store_read_cache_and_per_address_limit_and_marketplace_404(pool: PgPool) {
    let base = unclaimed(pool.clone()).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let actor = new_actor(&app).await;
    for index in 0..30 {
        let (status, headers, body) = get_store_from(app.router.clone(), "203.0.113.10").await;
        assert_eq!(status, StatusCode::OK, "read {index}: {body}");
        if index == 0 {
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        }
    }
    let (status, headers, _) = get_store_from(app.router.clone(), "203.0.113.10").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(header::RETRY_AFTER));
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    let (status, _, body) = get_store_from(app.router.clone(), "203.0.113.11").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a second address has its own bucket: {body}"
    );

    grant_session(&app, &actor).await;
    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, headers, body) = get_store_from(app.router.clone(), "203.0.113.11").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "public, max-age=30");
    assert_eq!(body["state"], json!("claimed"));

    let marketplace = common::test_app(pool.clone()).await;
    let (status, _) = send(
        marketplace.router.clone(),
        "GET",
        "/v1/store",
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_store_body_stays_compatible_outside_claim_mode(pool: PgPool) {
    let marketplace = common::test_app(pool.clone()).await;
    let (_, health) = send(
        marketplace.router.clone(),
        "GET",
        "/health",
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(
        health["store"],
        json!({ "mode": "marketplace", "seller": null })
    );

    let seller = marketplace_domain::pubky::encode_pubky(&[62; 32]);
    let mut config = Config::for_tests();
    config.store_mode = StoreMode::SingleSeller {
        pinned: Some(seller.clone()),
    };
    let state = AppState::new(pool.clone(), marketplace.clock.clone(), config);
    let pinned = app_with_state(&marketplace, state);
    let (_, health) = send(pinned.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(
        health["store"],
        json!({ "mode": "single_seller", "seller": seller })
    );
    assert!(health["store"].get("state").is_none());

    let unclaimed = unclaimed(pool.clone()).await;
    let (_, health) = send(
        unclaimed.router.clone(),
        "GET",
        "/health",
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(
        health["store"],
        json!({ "mode": "single_seller", "state": "unclaimed", "seller": null })
    );
}

async fn seller_row_refuses_unclaimed_boot_and_release(pool: PgPool, insert: &str) {
    let other = marketplace_domain::pubky::encode_pubky(&[80; 32]);
    sqlx::query(insert)
        .bind(&other)
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("seller row fixture");
    assert!(!other_seller_rows(&pool, None)
        .await
        .expect("seller rows")
        .is_empty());
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("state 3 refuses a populated unclaimed boot");
    assert!(
        error
            .to_string()
            .contains("database holds rows of 1 sellers"),
        "{error}"
    );

    let bound = marketplace_domain::pubky::encode_pubky(&[81; 32]);
    insert_binding(&pool, &bound, "claim").await;
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("admin connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &bound[..8])
        .await
        .expect_err("release refuses seller rows");
    assert!(
        error.to_string().contains("database holds seller rows"),
        "{error}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shipping_config_row_refuses_unclaimed_boot_and_release(pool: PgPool) {
    seller_row_refuses_unclaimed_boot_and_release(
        pool,
        "INSERT INTO shipping_configs (seller_pubky, created_at, updated_at) \
         VALUES ($1, $2, $2)",
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn drop_row_refuses_unclaimed_boot_and_release(pool: PgPool) {
    seller_row_refuses_unclaimed_boot_and_release(
        pool,
        "INSERT INTO drops \
             (aggregate_id, seller_pubky, drop_id, record_revision, revision, state, format, \
              starts_at, total_quantity, per_buyer_limit, remaining_quantity, paid_quantity, \
              stock_display, listing_ids, created_at, updated_at) \
         VALUES ('drop:' || $1 || '_setup', $1, 'setup', 1, 1, 'announced', 'fcfs', $2, \
                 1, 1, 1, 0, 'hidden', '[]'::jsonb, $2, $2)",
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_stat_attestation_row_refuses_unclaimed_boot_and_release(pool: PgPool) {
    seller_row_refuses_unclaimed_boot_and_release(
        pool,
        "INSERT INTO seller_stat_attestations \
             (id, seller_pubky, period_from, period_to, body, jws, created_at) \
         VALUES (gen_random_uuid(), $1, DATE '2026-01-01', DATE '2026-01-07', \
                 '{}'::jsonb, 'fixture', $2)",
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paykit_known_seller_row_refuses_boot_and_release(pool: PgPool) {
    let seller_a = marketplace_domain::pubky::encode_pubky(&[85; 32]);
    let seller_b = marketplace_domain::pubky::encode_pubky(&[86; 32]);
    sqlx::query("INSERT INTO paykit_known_sellers (seller_pubky, first_ready_at) VALUES ($1, $2)")
        .bind(&seller_a)
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("paykit_known_sellers fixture");

    let rows = other_seller_rows(&pool, None)
        .await
        .expect("all-seller scan");
    assert_eq!(rows.paykit_known_sellers, 1);
    assert_eq!(rows.sellers, vec![format!("{}…", &seller_a[..8])]);

    // Unclaimed boot refuses because durable Paykit seller ownership exists.
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("unclaimed boot refuses paykit_known_sellers row");
    assert!(
        error
            .to_string()
            .contains("database holds rows of 1 sellers"),
        "{error}"
    );

    // Pinned boot for another seller refuses with count and seller prefix and commits nothing.
    let error = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller_b.clone()),
        },
        None,
    )
    .await
    .expect_err("pinned boot for seller B refuses seller A paykit_known_sellers row");
    let message = error.to_string();
    assert!(message.contains("1 known Paykit sellers"), "{message}");
    assert!(
        message.contains(&format!("{}…", &seller_a[..8])),
        "{message}"
    );
    assert!(
        marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none(),
        "failed pin must not insert a binding"
    );

    // Pinned boot for Seller A itself succeeds and binds Seller A.
    resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller_a.clone()),
        },
        None,
    )
    .await
    .expect("pinned boot for seller A accepts its own paykit_known_sellers row");

    let mut database = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    database.set_path(&format!(
        "/{}",
        pool.connect_options()
            .get_database()
            .expect("test database name")
    ));
    let options = (*pool.connect_options()).clone();
    pool.close().await;

    // Library release refuses and preserves Seller A's binding.
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options.clone())
        .await
        .expect("admin connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &seller_a[..8])
        .await
        .expect_err("release refuses paykit_known_sellers row");
    let message = error.to_string();
    assert!(message.contains("database holds seller rows"), "{message}");
    assert!(message.contains("1 known Paykit sellers"), "{message}");
    assert!(
        message.contains(&format!("{}…", &seller_a[..8])),
        "{message}"
    );
    assert_eq!(
        marketplace_service::store_seller::read_binding(&admin)
            .await
            .expect("binding check")
            .map(|b| b.seller_pubky),
        Some(seller_a.clone())
    );
    admin.close().await;

    // CLI binary `store-binding-admin release` exits non-zero and preserves Seller A's binding.
    let refusal = std::process::Command::new(env!("CARGO_BIN_EXE_store-binding-admin"))
        .env("DATABASE_URL", database.as_str())
        .args(["release", "--confirm", &seller_a[..8]])
        .output()
        .expect("release binary runs");
    assert!(!refusal.status.success(), "{refusal:?}");
    let stderr = String::from_utf8(refusal.stderr).expect("stderr is text");
    assert!(stderr.contains("database holds seller rows"), "{stderr}");
    assert!(stderr.contains("1 known Paykit sellers"), "{stderr}");
    assert!(stderr.contains(&format!("{}…", &seller_a[..8])), "{stderr}");

    let verify = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("verify connection");
    assert_eq!(
        marketplace_service::store_seller::read_binding(&verify)
            .await
            .expect("binding remains after CLI refusal")
            .map(|b| b.seller_pubky),
        Some(seller_a)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paykit_known_seller_row_refuses_claim(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let seller_a = new_actor(&app).await;
    let seller_b = new_actor(&app).await;
    grant_session(&app, &seller_a).await;
    grant_session(&app, &seller_b).await;

    sqlx::query("INSERT INTO paykit_known_sellers (seller_pubky, first_ready_at) VALUES ($1, $2)")
        .bind(&seller_a.pubky)
        .bind(app.clock.now())
        .execute(&pool)
        .await
        .expect("paykit_known_sellers fixture");

    for claimant in [&seller_b, &seller_a] {
        let (status, body) = claim(&app, claimant, CODE).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "STORE_ALREADY_CLAIMED", "{body}");
        assert_eq!(
            body["error"]["message"], "The database is not empty; the store cannot be claimed.",
            "{body}"
        );
    }

    assert!(
        marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none(),
        "refused claim must leave store_seller_binding empty"
    );
    let (status, _, body) = get_store_from(app.router.clone(), "203.0.113.44").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], "unclaimed", "{body}");
}

fn moderation_decision_fixture(
    recipient: &str,
    restriction: &str,
    decided_at: chrono::DateTime<chrono::Utc>,
) -> marketplace_service::moderation_decisions::Decision {
    let target_uri = match restriction {
        "listing_removed" => {
            format!("pubky://{recipient}/pub/pubky.app/marketplace/v1/listings/lst_1")
        }
        "shop_suspended" => format!("pubky://{recipient}/pub/pubky.app/profile.json"),
        _ => format!("pubky://{recipient}/pub/pubky.app/marketplace/v1/reviews/rev_1"),
    };
    marketplace_service::moderation_decisions::Decision::validate(
        marketplace_service::moderation_decisions::DecisionInput {
            restriction: restriction.into(),
            target_uri,
            territorial_scope: "Every region where Pubky Market is offered".into(),
            ends_at: None,
            decided_at,
            basis: "notice".into(),
            facts: "Fixture moderation statement for single-seller emptiness coverage.".into(),
            automated_detection: false,
            automated_decision: false,
            automated_means: None,
            ground: "terms".into(),
            ground_reference: "Pubky Terms of Service".into(),
            explanation: "Fixture explanation matching the Terms clause.".into(),
        },
        decided_at,
    )
    .expect("valid moderation decision fixture")
}

async fn seller_moderation_restriction_refuses_boot_claim_and_release(
    pool: PgPool,
    restriction: &str,
) {
    let app = unclaimed(pool.clone()).await;
    let seller_a = new_actor(&app).await;
    let seller_b = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    grant_session(&app, &seller_a).await;
    grant_session(&app, &seller_b).await;

    // Buyer review moderation decisions (`review_text_hidden` and `review_excluded`)
    // are identity/buyer state and must stay allowed.
    for buyer_restriction in ["review_text_hidden", "review_excluded"] {
        let decision =
            moderation_decision_fixture(&buyer.pubky, buyer_restriction, app.clock.now());
        marketplace_service::moderation_decisions::record(
            &pool,
            &decision,
            "mod-fixture",
            app.clock.now(),
        )
        .await
        .expect("record buyer review moderation decision");
    }
    assert!(
        other_seller_rows(&pool, None)
            .await
            .expect("all-seller scan with buyer review decisions")
            .is_empty(),
        "buyer review moderation decisions must not count as seller rows"
    );

    // Record the standalone seller-targeted moderation decision with zero listings/orders.
    let seller_decision =
        moderation_decision_fixture(&seller_a.pubky, restriction, app.clock.now());
    marketplace_service::moderation_decisions::record(
        &pool,
        &seller_decision,
        "mod-fixture",
        app.clock.now(),
    )
    .await
    .expect("record seller moderation decision");

    let rows = other_seller_rows(&pool, None)
        .await
        .expect("all-seller scan");
    assert_eq!(rows.seller_moderation_decisions, 1, "{restriction}");
    assert_eq!(
        rows.sellers,
        vec![format!("{}…", &seller_a.pubky[..8])],
        "{restriction}"
    );

    // Unclaimed boot refuses.
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("unclaimed boot refuses seller moderation decision");
    assert!(
        error
            .to_string()
            .contains("database holds rows of 1 sellers"),
        "{restriction}: {error}"
    );

    // Pinned boot for Seller B refuses with diagnostic count and seller prefix, committing nothing.
    let error = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller_b.pubky.clone()),
        },
        None,
    )
    .await
    .expect_err("pinned boot for seller B refuses seller A moderation decision");
    let message = error.to_string();
    assert!(
        message.contains("1 seller moderation decisions"),
        "{restriction}: {message}"
    );
    assert!(
        message.contains(&format!("{}…", &seller_a.pubky[..8])),
        "{restriction}: {message}"
    );
    assert!(
        marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none(),
        "{restriction}: failed pin must not insert a binding"
    );

    // Claim refuses for both Seller B and Seller A and leaves store_seller_binding empty.
    for claimant in [&seller_b, &seller_a] {
        let (status, body) = claim(&app, claimant, CODE).await;
        assert_eq!(status, StatusCode::CONFLICT, "{restriction}: {body}");
        assert_eq!(
            body["error"]["code"], "STORE_ALREADY_CLAIMED",
            "{restriction}: {body}"
        );
        assert_eq!(
            body["error"]["message"], "The database is not empty; the store cannot be claimed.",
            "{restriction}: {body}"
        );
    }
    assert!(
        marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none(),
        "{restriction}: refused claim must leave store_seller_binding empty"
    );

    // Pinned boot for Seller A itself succeeds (buyer review decisions are still present).
    resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller_a.pubky.clone()),
        },
        None,
    )
    .await
    .expect("pinned boot for seller A accepts its own moderation decision");

    let mut database = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    database.set_path(&format!(
        "/{}",
        pool.connect_options()
            .get_database()
            .expect("test database name")
    ));
    let options = (*pool.connect_options()).clone();
    pool.close().await;

    // Library release refuses and preserves Seller A's binding.
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options.clone())
        .await
        .expect("admin connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &seller_a.pubky[..8])
        .await
        .expect_err("release refuses seller moderation decision");
    let message = error.to_string();
    assert!(
        message.contains("database holds seller rows"),
        "{restriction}: {message}"
    );
    assert!(
        message.contains("1 seller moderation decisions"),
        "{restriction}: {message}"
    );
    assert!(
        message.contains(&format!("{}…", &seller_a.pubky[..8])),
        "{restriction}: {message}"
    );
    admin.close().await;

    // CLI binary `store-binding-admin release` exits non-zero and preserves Seller A's binding.
    let refusal = std::process::Command::new(env!("CARGO_BIN_EXE_store-binding-admin"))
        .env("DATABASE_URL", database.as_str())
        .args(["release", "--confirm", &seller_a.pubky[..8]])
        .output()
        .expect("release binary runs");
    assert!(!refusal.status.success(), "{restriction}: {refusal:?}");
    let stderr = String::from_utf8(refusal.stderr).expect("stderr is text");
    assert!(
        stderr.contains("database holds seller rows"),
        "{restriction}: {stderr}"
    );
    assert!(
        stderr.contains("1 seller moderation decisions"),
        "{restriction}: {stderr}"
    );

    let verify = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("verify connection");
    assert_eq!(
        marketplace_service::store_seller::read_binding(&verify)
            .await
            .expect("binding remains")
            .map(|b| b.seller_pubky),
        Some(seller_a.pubky)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn moderation_shop_suspended_refuses_boot_claim_and_release(pool: PgPool) {
    seller_moderation_restriction_refuses_boot_claim_and_release(pool, "shop_suspended").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn moderation_listing_removed_refuses_boot_claim_and_release(pool: PgPool) {
    seller_moderation_restriction_refuses_boot_claim_and_release(pool, "listing_removed").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn moderation_buyer_review_decision_allows_boot_claim_and_release(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    grant_session(&app, &seller).await;

    let decision = moderation_decision_fixture(&buyer.pubky, "review_text_hidden", app.clock.now());
    marketplace_service::moderation_decisions::record(
        &pool,
        &decision,
        "mod-fixture",
        app.clock.now(),
    )
    .await
    .expect("record buyer review decision");

    // Unclaimed boot, claim, and release all succeed with a buyer review decision present.
    resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect("unclaimed boot allows buyer review moderation decision");

    let (status, body) = claim(&app, &seller, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("admin connection");
    let released = marketplace_service::store_binding_admin::release(&admin, &seller.pubky[..8])
        .await
        .expect("release allows buyer review moderation decision");
    assert_eq!(released, seller.pubky);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn gateway_refund_inbox_orphan_row_refuses_boot_claim_and_release(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let seller_a = new_actor(&app).await;
    grant_session(&app, &seller_a).await;

    sqlx::query(
        "INSERT INTO gateway_refund_inbox \
             (txn_id, parent_txn_id, payment_status, reason, order_id, fields, received_at) \
         VALUES ($1, $2, 'Refunded', 'unknown_parent', NULL, $3, $4)",
    )
    .bind("ORPHAN_REFUND_TXN_1")
    .bind("UNKNOWN_PARENT_TXN_1")
    .bind(json!({
        "receiver_email": "merchant@example.com",
        "mc_gross": "-10.00",
        "mc_currency": "USD"
    }))
    .bind(app.clock.now())
    .execute(&pool)
    .await
    .expect("orphan gateway_refund_inbox fixture");

    // All-seller and pinned-seller scans both count the unattributed row (fail-closed).
    let all_rows = other_seller_rows(&pool, None)
        .await
        .expect("all-seller scan");
    assert_eq!(all_rows.gateway_refund_inbox, 1);
    assert!(all_rows.sellers.is_empty());
    let pinned_rows = other_seller_rows(&pool, Some(&seller_a.pubky))
        .await
        .expect("pinned-seller scan");
    assert_eq!(pinned_rows.gateway_refund_inbox, 1);

    // Unclaimed boot refuses with unattributed diagnostic (not "0 sellers").
    let error = resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("unclaimed boot refuses orphan gateway_refund_inbox row");
    let message = error.to_string();
    assert!(message.contains("unattributed seller rows"), "{message}");
    assert!(message.contains("1 gateway refund inbox rows"), "{message}");

    // Pinned boot for Seller A refuses fail-closed and commits nothing.
    let error = resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(seller_a.pubky.clone()),
        },
        None,
    )
    .await
    .expect_err("pinned boot refuses orphan gateway_refund_inbox row");
    let message = error.to_string();
    assert!(message.contains("1 gateway refund inbox rows"), "{message}");
    assert!(message.contains("(unattributed)"), "{message}");
    assert!(
        marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none(),
        "failed pin must not insert a binding"
    );

    // Claim refuses with 409 and leaves store_seller_binding empty.
    let (status, body) = claim(&app, &seller_a, CODE).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "STORE_ALREADY_CLAIMED", "{body}");
    assert_eq!(
        body["error"]["message"], "The database is not empty; the store cannot be claimed.",
        "{body}"
    );
    assert!(marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding check")
        .is_none());

    // With Seller A bound, both library and CLI release refuse and preserve the binding.
    insert_binding(&pool, &seller_a.pubky, "claim").await;
    let mut database = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    database.set_path(&format!(
        "/{}",
        pool.connect_options()
            .get_database()
            .expect("test database name")
    ));
    let options = (*pool.connect_options()).clone();
    pool.close().await;

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options.clone())
        .await
        .expect("admin connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &seller_a.pubky[..8])
        .await
        .expect_err("release refuses orphan gateway_refund_inbox row");
    let message = error.to_string();
    assert!(message.contains("database holds seller rows"), "{message}");
    assert!(message.contains("1 gateway refund inbox rows"), "{message}");
    assert!(message.contains("unattributed"), "{message}");
    admin.close().await;

    let refusal = std::process::Command::new(env!("CARGO_BIN_EXE_store-binding-admin"))
        .env("DATABASE_URL", database.as_str())
        .args(["release", "--confirm", &seller_a.pubky[..8]])
        .output()
        .expect("release binary runs");
    assert!(!refusal.status.success(), "{refusal:?}");
    let stderr = String::from_utf8(refusal.stderr).expect("stderr is text");
    assert!(stderr.contains("database holds seller rows"), "{stderr}");
    assert!(stderr.contains("1 gateway refund inbox rows"), "{stderr}");

    let verify = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("verify connection");
    assert_eq!(
        marketplace_service::store_seller::read_binding(&verify)
            .await
            .expect("binding remains")
            .map(|b| b.seller_pubky),
        Some(seller_a.pubky)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unclaimed_paypal_ipn_is_refused_without_writing_gateway_refund_inbox(pool: PgPool) {
    let (app, _paykit, _ipn) =
        common::test_app_with_payments_config(pool.clone(), claim_config(CODE)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    grant_session(&app, &seller).await;

    // A verified PayPal Refunded IPN against an unclaimed store must be
    // refused (503) before verification and write zero inbox rows.
    let ipn_body = "txn_id=UNCLAIMED_REFUND_1&parent_txn_id=UNKNOWN_PARENT_1\
                    &payment_status=Refunded&mc_gross=-10.00&mc_currency=USD\
                    &receiver_email=merchant@example.com";
    let (status, _) = common::send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        ipn_body.as_bytes().to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let inbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateway_refund_inbox")
        .fetch_one(&pool)
        .await
        .expect("inbox count");
    assert_eq!(
        inbox_count, 0,
        "unclaimed IPN must not write orphan gateway_refund_inbox rows"
    );

    // The store remains clean and claimable, and matched inbox rows for the
    // store seller are allowed on pinned boot while other sellers are refused.
    let (status, body) = claim(&app, &seller, CODE).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    let order_uuid = Uuid::parse_str(&order.order_id).expect("order uuid");
    sqlx::query(
        "INSERT INTO gateway_refund_inbox \
             (txn_id, parent_txn_id, payment_status, reason, order_id, fields, received_at) \
         VALUES ($1, $2, 'Refunded', 'receiver_mismatch', $3, $4, $5)",
    )
    .bind("MATCHED_REFUND_TXN_1")
    .bind("MATCHED_PARENT_TXN_1")
    .bind(order_uuid)
    .bind(json!({ "mc_gross": "-10.00", "mc_currency": "USD" }))
    .bind(app.clock.now())
    .execute(&pool)
    .await
    .expect("matched gateway_refund_inbox fixture");

    assert_eq!(
        other_seller_rows(&pool, Some(&seller.pubky))
            .await
            .expect("own seller scan")
            .gateway_refund_inbox,
        0,
        "a matched inbox row for the store seller's own order is allowed"
    );
    let other_seller = marketplace_domain::pubky::encode_pubky(&[88; 32]);
    let other_rows = other_seller_rows(&pool, Some(&other_seller))
        .await
        .expect("other seller scan");
    assert_eq!(other_rows.gateway_refund_inbox, 1);
    assert_eq!(other_rows.orders, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn empty_seller_pubky_row_refuses_unclaimed_boot_claim_and_release(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;

    sqlx::query("INSERT INTO paykit_known_sellers (seller_pubky, first_ready_at) VALUES ('', $1)")
        .bind(app.clock.now())
        .execute(&pool)
        .await
        .expect("empty seller_pubky fixture");

    let rows = other_seller_rows(&pool, None)
        .await
        .expect("all-seller scan");
    assert_eq!(rows.paykit_known_sellers, 1);
    assert!(!rows.is_empty());

    resolve_store(&pool, &StoreMode::SingleSeller { pinned: None }, None)
        .await
        .expect_err("unclaimed boot refuses empty-string seller_pubky row");

    let (status, body) = claim(&app, &actor, CODE).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "STORE_ALREADY_CLAIMED", "{body}");

    insert_binding(&pool, &actor.pubky, "claim").await;
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("admin connection");
    marketplace_service::store_binding_admin::release(&admin, &actor.pubky[..8])
        .await
        .expect_err("release refuses empty-string seller_pubky row");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn store_binding_admin_binary_logs_release_and_exits_nonzero_on_refusal(pool: PgPool) {
    let seller = marketplace_domain::pubky::encode_pubky(&[82; 32]);
    insert_binding(&pool, &seller, "claim").await;
    let mut database = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    database.set_path(&format!(
        "/{}",
        pool.connect_options()
            .get_database()
            .expect("test database name")
    ));
    pool.close().await;

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_store-binding-admin"))
        .env("DATABASE_URL", database.as_str())
        .args(["release", "--confirm", &seller[..8]])
        .output()
        .expect("release binary runs");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("stdout is text");
    let stderr = String::from_utf8(output.stderr).expect("stderr is text");
    let expected = format!("store.binding.released seller={}", &seller[..8]);
    assert_eq!(
        format!("{stdout}{stderr}")
            .lines()
            .filter(|line| line.contains("store.binding.released"))
            .count(),
        1,
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stderr.lines().any(|line| line == expected),
        "stdout: {stdout}\nstderr: {stderr}"
    );

    let refusal = std::process::Command::new(env!("CARGO_BIN_EXE_store-binding-admin"))
        .env("DATABASE_URL", database.as_str())
        .args(["release", "--confirm", &seller[..8]])
        .output()
        .expect("refused release binary runs");
    assert!(!refusal.status.success(), "{refusal:?}");
    let stderr = String::from_utf8(refusal.stderr).expect("stderr is text");
    assert!(
        stderr.contains("the store has no seller binding to release"),
        "{stderr}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_refuses_when_any_seller_row_exists(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let actor = new_actor(&app).await;
    let (status, body) = execute(&app, &actor.token, &register_command(&actor.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    resolve_store(
        &pool,
        &StoreMode::SingleSeller {
            pinned: Some(actor.pubky.clone()),
        },
        None,
    )
    .await
    .expect("the only seller can be pinned");
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("admin connection");
    let error = marketplace_service::store_binding_admin::release(&admin, &actor.pubky[..8])
        .await
        .expect_err("seller rows make release unsafe");
    assert!(
        error.to_string().contains("database holds seller rows"),
        "{error}"
    );
    assert!(marketplace_service::store_seller::read_binding(&admin)
        .await
        .expect("binding remains")
        .is_some());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn boot_waits_for_release_lock_then_boots_unclaimed(pool: PgPool) {
    let seller = marketplace_domain::pubky::encode_pubky(&[71; 32]);
    insert_binding(&pool, &seller, "claim").await;
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options.clone())
        .await
        .expect("admin connection");
    let pending = marketplace_service::store_binding_admin::begin_release(&admin, &seller[..8])
        .await
        .expect("release holds the shared lock");
    let boot_pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("boot connection");
    let mut boot = tokio::spawn(async move {
        resolve_store(&boot_pool, &StoreMode::SingleSeller { pinned: None }, None).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(60), &mut boot)
            .await
            .is_err(),
        "the boot waits while the helper holds the advisory lock"
    );
    assert_eq!(pending.commit().await.expect("release commits"), seller);
    let store = tokio::time::timeout(Duration::from_secs(2), boot)
        .await
        .expect("boot finishes after release")
        .expect("boot task")
        .expect("boot succeeds");
    assert_eq!(store.active(), Active::Unclaimed);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_after_an_earlier_boot_lock_refuses_its_open_connection(pool: PgPool) {
    let seller = marketplace_domain::pubky::encode_pubky(&[72; 32]);
    insert_binding(&pool, &seller, "claim").await;
    let options = (*pool.connect_options()).clone();
    pool.close().await;
    let boot_pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options.clone())
        .await
        .expect("boot connection");
    let mut boot_tx = boot_pool.begin().await.expect("boot transaction");
    take_binding_lock(&mut boot_tx).await.expect("boot lock");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect_with(options)
        .await
        .expect("admin connection");
    let confirm = seller[..8].to_string();
    let release = tokio::spawn(async move {
        marketplace_service::store_binding_admin::release(&admin, &confirm).await
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    boot_tx.commit().await.expect("boot transaction commits");
    let error = tokio::time::timeout(Duration::from_secs(2), release)
        .await
        .expect("release finishes")
        .expect("release task")
        .expect_err("the boot pool remains open");
    assert!(
        error.to_string().contains("another service connection"),
        "{error}"
    );
}

async fn open_parked_claim(
    address: std::net::SocketAddr,
    token: &str,
    client_ip: &str,
    body_len: usize,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to test server");
    let head = format!(
        "POST /v1/store/claim HTTP/1.1\r\n\
         Host: localhost\r\n\
         Authorization: Bearer {token}\r\n\
         Content-Type: application/json\r\n\
         X-Test-Client: {client_ip}\r\n\
         Content-Length: {body_len}\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write request headers");
    stream.flush().await.expect("flush request headers");
    stream
}

async fn read_http_response(mut stream: tokio::net::TcpStream) -> (u16, String) {
    use tokio::io::AsyncReadExt;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read http response");
    let text = String::from_utf8(raw).expect("http response is utf-8");
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("malformed HTTP status line: {text}"));
    (status, text)
}

async fn send_claim_from(
    router: axum::Router,
    token: &str,
    client_ip: &str,
    raw_body: &str,
) -> (StatusCode, HeaderMap, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/store/claim")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("x-test-client", client_ip)
        .body(Body::from(raw_body.to_string()))
        .expect("claim request builds");
    let response = router.oneshot(request).await.expect("request executes");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, headers, body)
}

async fn wait_until_in_flight(app: &TestApp, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let current = app.state.store_limiters.claim_in_flight_total();
        if current == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for in_flight == {expected} (last {current})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn parked_concurrent_claims_cannot_exceed_ten_failures_and_use_fresh_clock(pool: PgPool) {
    use tokio::io::AsyncWriteExt;

    let base = unclaimed(pool.clone()).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let actor = new_actor(&app).await;
    grant_session(&app, &actor).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener addr");
    let server = tokio::spawn({
        let router = app.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });

    let wrong_body = serde_json::to_string(&json!({ "claim_code": WRONG_CODE })).unwrap();
    let attacker_ip = "203.0.113.10";
    let other_ip = "203.0.113.11";

    // Open 10 concurrent requests from one address and park them in `to_bytes`.
    let mut parked = Vec::new();
    for _ in 0..10 {
        parked.push(open_parked_claim(address, &actor.token, attacker_ip, wrong_body.len()).await);
    }
    wait_until_in_flight(&app, 10).await;

    // While all 10 bodies remain parked, 5 additional attempts from the same
    // address are rejected immediately with 429, while a distinct address is
    // unaffected.
    for _ in 0..5 {
        let (status, headers, _) =
            send_claim_from(app.router.clone(), &actor.token, attacker_ip, &wrong_body).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(headers.contains_key(header::RETRY_AFTER));
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }
    let (other_status, _, other_body) =
        send_claim_from(app.router.clone(), &actor.token, other_ip, &wrong_body).await;
    assert_eq!(other_status, StatusCode::FORBIDDEN, "{other_body}");
    assert_eq!(other_body["error"]["reason"], json!("claim_code_invalid"));

    // Advance the serving clock by 120 s while the 10 bodies are still parked,
    // then complete all 10 with wrong codes. Failures must be recorded at the
    // fresh clock (`T0 + 120s`), not at the pre-await timestamp `T0`.
    app.clock.advance_seconds(120);
    let mut responses = Vec::new();
    for mut stream in parked {
        stream
            .write_all(wrong_body.as_bytes())
            .await
            .expect("write parked body");
        stream.flush().await.expect("flush parked body");
        responses.push(tokio::spawn(read_http_response(stream)));
    }
    for task in responses {
        let (status, raw) = task.await.expect("response task");
        assert_eq!(status, 403, "{raw}");
        assert!(raw.contains("claim_code_invalid"), "{raw}");
    }

    // Immediately after those 10 failures at `T0 + 120s`, an 11th attempt must
    // still be rate-limited (no 120 s refill credited from stale `T0`).
    let (status, headers, _) =
        send_claim_from(app.router.clone(), &actor.token, attacker_ip, &wrong_body).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(header::RETRY_AFTER));

    server.abort();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn stalled_and_cancelled_claim_bodies_time_out_and_release_in_flight_slots(pool: PgPool) {
    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();
    let wrong_body = serde_json::to_string(&json!({ "claim_code": WRONG_CODE })).unwrap();

    // 1. Body-read timeout (408) releases every in-flight slot without
    // spending failure tokens.
    {
        let base = unclaimed(pool.clone()).await;
        let state = base.state.clone().with_store_limiters(Arc::new(
            StoreLimiters::new(Some(HeaderName::from_static("x-test-client")))
                .with_claim_timeouts(Duration::from_millis(120), Duration::from_secs(30)),
        ));
        let app = app_with_state(&base, state);
        let actor = new_actor(&app).await;
        grant_session(&app, &actor).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("listener addr");
        let server = tokio::spawn({
            let router = app.router.clone();
            async move {
                let _ = axum::serve(listener, router).await;
            }
        });

        let ip = "203.0.113.20";
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let stream = open_parked_claim(address, &actor.token, ip, valid_body.len()).await;
            tasks.push(tokio::spawn(read_http_response(stream)));
        }
        for task in tasks {
            let (status, raw) = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("timed-out request finishes")
                .expect("response task");
            assert_eq!(status, 408, "{raw}");
            assert!(raw.contains("REQUEST_TIMEOUT"), "{raw}");
            assert!(
                raw.to_ascii_lowercase().contains("cache-control: no-store"),
                "{raw}"
            );
        }

        // All 10 slots were released without spending failure tokens: one wrong
        // code returns 403 (not 429), then the valid code claims with 200.
        let (status, _, body) =
            send_claim_from(app.router.clone(), &actor.token, ip, &wrong_body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["reason"], json!("claim_code_invalid"));
        let (status, _, body) =
            send_claim_from(app.router.clone(), &actor.token, ip, &valid_body).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        server.abort();
    }

    // 2. Route-level timeout (408) also drops the in-flight guard cleanly.
    {
        let mut tx = pool.begin().await.expect("reset tx");
        sqlx::query("SET LOCAL marketplace.release_store_binding = 'on'")
            .execute(&mut *tx)
            .await
            .expect("enable release guc");
        sqlx::query("DELETE FROM store_seller_binding")
            .execute(&mut *tx)
            .await
            .expect("clear binding");
        tx.commit().await.expect("commit reset");

        let base = unclaimed(pool.clone()).await;
        let state = base.state.clone().with_store_limiters(Arc::new(
            StoreLimiters::new(Some(HeaderName::from_static("x-test-client")))
                .with_claim_timeouts(Duration::from_secs(10), Duration::from_millis(120)),
        ));
        let app = app_with_state(&base, state);
        let actor = new_actor(&app).await;
        grant_session(&app, &actor).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("listener addr");
        let server = tokio::spawn({
            let router = app.router.clone();
            async move {
                let _ = axum::serve(listener, router).await;
            }
        });

        let ip = "203.0.113.21";
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let stream = open_parked_claim(address, &actor.token, ip, valid_body.len()).await;
            tasks.push(tokio::spawn(read_http_response(stream)));
        }
        for task in tasks {
            let (status, raw) = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("route-timed-out request finishes")
                .expect("response task");
            assert_eq!(status, 408, "{raw}");
            assert!(raw.contains("REQUEST_TIMEOUT"), "{raw}");
        }

        // Wait until the route timeout has returned and dropped the inner
        // handler future, then confirm every slot is free.
        wait_until_in_flight(&app, 0).await;
        let (status, _, body) =
            send_claim_from(app.router.clone(), &actor.token, ip, &valid_body).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        server.abort();
    }

    // 3. Abrupt client disconnect while parked in `to_bytes` drops the guard
    // and releases all 10 slots cleanly.
    {
        let mut tx = pool.begin().await.expect("reset tx");
        sqlx::query("SET LOCAL marketplace.release_store_binding = 'on'")
            .execute(&mut *tx)
            .await
            .expect("enable release guc");
        sqlx::query("DELETE FROM store_seller_binding")
            .execute(&mut *tx)
            .await
            .expect("clear binding");
        tx.commit().await.expect("commit reset");

        let base = unclaimed(pool.clone()).await;
        let state = base
            .state
            .clone()
            .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
                "x-test-client",
            )))));
        let app = app_with_state(&base, state);
        let actor = new_actor(&app).await;
        grant_session(&app, &actor).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("listener addr");
        let server = tokio::spawn({
            let router = app.router.clone();
            async move {
                let _ = axum::serve(listener, router).await;
            }
        });

        let ip = "203.0.113.22";
        let mut parked = Vec::new();
        for _ in 0..10 {
            parked.push(open_parked_claim(address, &actor.token, ip, valid_body.len()).await);
        }
        wait_until_in_flight(&app, 10).await;
        let (status, _, _) =
            send_claim_from(app.router.clone(), &actor.token, ip, &valid_body).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        drop(parked);
        wait_until_in_flight(&app, 0).await;
        let (status, _, body) =
            send_claim_from(app.router.clone(), &actor.token, ip, &valid_body).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        server.abort();
    }
}

fn token_hash_for_bearer(bearer: &str) -> Vec<u8> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(bearer)
        .expect("valid base64url bearer");
    marketplace_service::auth::hash_token(&raw)
}

async fn mark_session_as_grant(
    pool: &PgPool,
    bearer: &str,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    client_id: &str,
) -> Uuid {
    sqlx::query_scalar(
        "UPDATE auth_sessions \
         SET source_grant_id = 'grant-fixture', \
             source_client_id = $2, \
             created_at = $3, \
             expires_at = $4 \
         WHERE token_hash = $1 \
         RETURNING session_id",
    )
    .bind(token_hash_for_bearer(bearer))
    .bind(client_id)
    .bind(created_at)
    .bind(expires_at)
    .fetch_one(pool)
    .await
    .expect("grant-session fixture row")
}

async fn wait_for_advisory_lock_waiter(pool: &PgPool, key: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for advisory lock waiter on key {key}"
        );
        let waiting: bool = tokio::time::timeout(
            remaining,
            sqlx::query_scalar(
                "SELECT EXISTS ( \
                     SELECT 1 FROM pg_locks \
                     WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                       AND locktype = 'advisory' \
                       AND objsubid = 1 \
                       AND classid = (($1::bigint >> 32) & x'ffffffff'::bigint)::oid \
                       AND objid = ($1::bigint & x'ffffffff'::bigint)::oid \
                       AND NOT granted \
                 )",
            )
            .bind(key)
            .fetch_one(pool),
        )
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for advisory lock waiter on key {key}"))
        .expect("pg_locks query");
        if waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_revoke_lock_waiter(pool: &PgPool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for revoke_session to block on auth_sessions row lock"
        );
        let waiting: bool = tokio::time::timeout(
            remaining,
            sqlx::query_scalar(
                "SELECT EXISTS ( \
                     SELECT 1 FROM pg_stat_activity \
                     WHERE datname = current_database() \
                       AND wait_event_type = 'Lock' \
                       AND query LIKE 'UPDATE auth_sessions SET revoked_at%' \
                 )",
            )
            .fetch_one(pool),
        )
        .await
        .expect("timed out waiting for revoke_session to block on auth_sessions row lock")
        .expect("pg_stat_activity query");
        if waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spawn_claim_from(
    router: axum::Router,
    token: &str,
    client_ip: &'static str,
    raw_body: &str,
) -> tokio::task::JoinHandle<(StatusCode, HeaderMap, Value)> {
    let token = token.to_string();
    let raw_body = raw_body.to_string();
    tokio::spawn(async move { send_claim_from(router, &token, client_ip, &raw_body).await })
}

async fn assert_full_ten_failure_budget_intact(
    app: &TestApp,
    actor: &TestActor,
    client_ip: &str,
    client_id: &str,
) {
    tokio::time::sleep(Duration::from_millis(2)).await;
    let fresh_token = common::authenticate(app, &actor.keypair).await;
    let now = app.clock.now();
    mark_session_as_grant(
        &app.pool,
        &fresh_token,
        now,
        now + chrono::Duration::seconds(3_600),
        client_id,
    )
    .await;
    let wrong_body = serde_json::to_string(&json!({ "claim_code": WRONG_CODE })).unwrap();
    for attempt in 0..10 {
        let (status, _, body) =
            send_claim_from(app.router.clone(), &fresh_token, client_ip, &wrong_body).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "attempt {attempt} should still have failure budget: {body}"
        );
        assert_eq!(body["error"]["reason"], json!("claim_code_invalid"));
    }
    let (status, headers, _) =
        send_claim_from(app.router.clone(), &fresh_token, client_ip, &wrong_body).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(header::RETRY_AFTER));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn parked_claim_crossing_max_age_on_binding_lock_rejects_without_consuming_failure_slot(
    pool: PgPool,
) {
    let mut config = claim_config(CODE);
    config.store_claim_session_max_age_seconds = 1;
    let base = test_app_with_config(pool.clone(), config).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let actor = new_actor(&app).await;
    let t0 = app.clock.now();
    mark_session_as_grant(
        &pool,
        &actor.token,
        t0,
        t0 + chrono::Duration::seconds(3_600),
        "client-fixture",
    )
    .await;

    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();
    let client_ip = "203.0.113.51";

    let mut lock_tx = pool.begin().await.expect("lock tx");
    take_binding_lock(&mut lock_tx)
        .await
        .expect("hold binding lock");

    let claim_task = spawn_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body);
    wait_for_advisory_lock_waiter(&pool, STORE_BINDING_LOCK_KEY).await;

    // Cross STORE_CLAIM_SESSION_MAX_AGE_SECONDS=1 by 500 ms (proving both
    // post-lock revalidation and sub-second precision, not integer truncation).
    app.clock.set(t0 + chrono::Duration::milliseconds(1_500));
    lock_tx.commit().await.expect("release binding lock");

    let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
        .await
        .expect("claim finishes")
        .expect("claim task");
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(body["error"]["reason"], json!("claim_session_stale"));

    assert!(marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding check")
        .is_none());
    let (store_status, _, store_body) = get_store_from(app.router.clone(), client_ip).await;
    assert_eq!(store_status, StatusCode::OK, "{store_body}");
    assert_eq!(store_body["state"], json!("unclaimed"));
    assert_eq!(app.state.store_limiters.claim_in_flight_total(), 0);

    // Pre-body check also uses sub-second precision (1.5 s > 1 s).
    let (status, _, body) =
        send_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("claim_session_stale"));

    assert_full_ten_failure_budget_intact(&app, &actor, client_ip, "client-fixture").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn parked_claim_crossing_session_expiry_on_binding_lock_rejects_with_401(pool: PgPool) {
    let base = unclaimed(pool.clone()).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let actor = new_actor(&app).await;
    let t0 = app.clock.now();
    // Expires in 10 s while STORE_CLAIM_SESSION_MAX_AGE_SECONDS is 900 s, so
    // advancing to t0 + 10 s crosses only auth_sessions.expires_at.
    mark_session_as_grant(
        &pool,
        &actor.token,
        t0,
        t0 + chrono::Duration::seconds(10),
        "client-fixture",
    )
    .await;

    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();
    let client_ip = "203.0.113.52";

    let mut lock_tx = pool.begin().await.expect("lock tx");
    take_binding_lock(&mut lock_tx)
        .await
        .expect("hold binding lock");

    let claim_task = spawn_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body);
    wait_for_advisory_lock_waiter(&pool, STORE_BINDING_LOCK_KEY).await;

    app.clock.set(t0 + chrono::Duration::seconds(10));
    lock_tx.commit().await.expect("release binding lock");

    let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
        .await
        .expect("claim finishes")
        .expect("claim task");
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("The session is invalid or expired.")
    );

    assert!(marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding check")
        .is_none());
    let (store_status, _, store_body) = get_store_from(app.router.clone(), client_ip).await;
    assert_eq!(store_status, StatusCode::OK, "{store_body}");
    assert_eq!(store_body["state"], json!("unclaimed"));
    assert_eq!(app.state.store_limiters.claim_in_flight_total(), 0);

    assert_full_ten_failure_budget_intact(&app, &actor, client_ip, "client-fixture").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_session_revocation_while_claim_waits_rejects_with_401(pool: PgPool) {
    let base = unclaimed(pool.clone()).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let actor = new_actor(&app).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let admin_token = common::authenticate(&app, &actor.keypair).await;

    let t0 = app.clock.now();
    let claimant_session_id = mark_session_as_grant(
        &pool,
        &actor.token,
        t0,
        t0 + chrono::Duration::seconds(3_600),
        "client-fixture",
    )
    .await;
    sqlx::query("UPDATE auth_sessions SET created_at = $2, expires_at = $3 WHERE token_hash = $1")
        .bind(token_hash_for_bearer(&admin_token))
        .bind(t0)
        .bind(t0 + chrono::Duration::seconds(3_600))
        .execute(&pool)
        .await
        .expect("align admin session window");

    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();
    let client_ip = "203.0.113.53";

    let mut lock_tx = pool.begin().await.expect("lock tx");
    take_binding_lock(&mut lock_tx)
        .await
        .expect("hold binding lock");

    let claim_task = spawn_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body);
    wait_for_advisory_lock_waiter(&pool, STORE_BINDING_LOCK_KEY).await;

    // Another session for the same actor revokes the claimant session and
    // receives 204 while the valid-code claim waits on the binding lock.
    let (revoke_status, revoke_body) = tokio::time::timeout(
        Duration::from_secs(5),
        send(
            app.router.clone(),
            "DELETE",
            &format!("/v1/auth/sessions/{claimant_session_id}"),
            Some(&admin_token),
            &Value::Null,
        ),
    )
    .await
    .expect("revoke finishes without blocking on binding lock");
    assert_eq!(revoke_status, StatusCode::NO_CONTENT, "{revoke_body}");

    lock_tx.commit().await.expect("release binding lock");

    let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
        .await
        .expect("claim finishes")
        .expect("claim task");
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("The session is invalid or expired.")
    );

    assert!(marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding check")
        .is_none());
    let (store_status, _, store_body) = get_store_from(app.router.clone(), client_ip).await;
    assert_eq!(store_status, StatusCode::OK, "{store_body}");
    assert_eq!(store_body["state"], json!("unclaimed"));
    assert_eq!(app.state.store_limiters.claim_in_flight_total(), 0);

    assert_full_ten_failure_budget_intact(&app, &actor, client_ip, "client-fixture").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn claim_row_lock_on_auth_sessions_serializes_concurrent_revocation_through_commit(
    pool: PgPool,
) {
    const TEST_PAUSE_LOCK_KEY: i64 = 4_713_371_337_999;

    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let admin_token = common::authenticate(&app, &actor.keypair).await;

    let t0 = app.clock.now();
    let claimant_session_id = mark_session_as_grant(
        &pool,
        &actor.token,
        t0,
        t0 + chrono::Duration::seconds(3_600),
        "client-fixture",
    )
    .await;
    sqlx::query("UPDATE auth_sessions SET created_at = $2, expires_at = $3 WHERE token_hash = $1")
        .bind(token_hash_for_bearer(&admin_token))
        .bind(t0)
        .bind(t0 + chrono::Duration::seconds(3_600))
        .execute(&pool)
        .await
        .expect("align admin session window");

    // Pause the claim transaction inside INSERT INTO store_seller_binding
    // (after auth_sessions FOR UPDATE and before COMMIT) using a test-only
    // trigger on the isolated test database.
    sqlx::query(&format!(
        "CREATE FUNCTION test_pause_store_binding_insert() RETURNS trigger AS $$ \
         BEGIN \
             PERFORM pg_advisory_xact_lock({TEST_PAUSE_LOCK_KEY}); \
             RETURN NEW; \
         END; \
         $$ LANGUAGE plpgsql;"
    ))
    .execute(&pool)
    .await
    .expect("create pause function");
    sqlx::query(
        "CREATE TRIGGER test_pause_store_binding_insert_trg \
         BEFORE INSERT ON store_seller_binding \
         FOR EACH ROW EXECUTE FUNCTION test_pause_store_binding_insert();",
    )
    .execute(&pool)
    .await
    .expect("create pause trigger");

    let mut pause_tx = pool.begin().await.expect("pause tx");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(TEST_PAUSE_LOCK_KEY)
        .execute(&mut *pause_tx)
        .await
        .expect("hold pause lock");

    let claim_task = tokio::spawn({
        let router = app.router.clone();
        let token = actor.token.clone();
        async move {
            send(
                router,
                "POST",
                "/v1/store/claim",
                Some(&token),
                &json!({ "claim_code": CODE }),
            )
            .await
        }
    });
    wait_for_advisory_lock_waiter(&pool, TEST_PAUSE_LOCK_KEY).await;

    // While claim_store holds auth_sessions FOR UPDATE before committing the
    // binding, concurrent revocation must block rather than returning 204
    // ahead of the claim commit.
    let mut revoke_task = tokio::spawn({
        let router = app.router.clone();
        let token = admin_token.clone();
        let uri = format!("/v1/auth/sessions/{claimant_session_id}");
        async move { send(router, "DELETE", &uri, Some(&token), &Value::Null).await }
    });
    wait_for_revoke_lock_waiter(&pool).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut revoke_task)
            .await
            .is_err(),
        "DELETE /v1/auth/sessions/{{id}} must block until claim_store releases auth_sessions FOR UPDATE on commit"
    );

    pause_tx.commit().await.expect("release pause lock");

    let (claim_status, claim_body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
        .await
        .expect("claim finishes")
        .expect("claim task");
    assert_eq!(claim_status, StatusCode::OK, "{claim_body}");
    assert_eq!(claim_body["store"]["seller"], json!(actor.pubky));

    let (revoke_status, revoke_body) = tokio::time::timeout(Duration::from_secs(5), revoke_task)
        .await
        .expect("revoke finishes after claim commit")
        .expect("revoke task");
    assert_eq!(revoke_status, StatusCode::NO_CONTENT, "{revoke_body}");

    let binding = marketplace_service::store_seller::read_binding(&pool)
        .await
        .expect("binding check")
        .expect("store is claimed");
    assert_eq!(binding.seller_pubky, actor.pubky);
    assert_eq!(binding.claim_session_id, Some(claimant_session_id));

    let revoked_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT revoked_at FROM auth_sessions WHERE session_id = $1")
            .bind(claimant_session_id)
            .fetch_one(&pool)
            .await
            .expect("revoked_at check");
    assert!(
        revoked_at.is_some(),
        "session is revoked after claim commit"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn parked_claim_revalidates_grant_source_and_capability_after_binding_lock(pool: PgPool) {
    let base = unclaimed(pool.clone()).await;
    let grant_config = Arc::new(
        GrantSessionConfig::new("https://svc.test", "shop.pubky.app", 120)
            .expect("grant session config"),
    );
    let state = base
        .state
        .clone()
        .with_grant_session(Some(grant_config))
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);
    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();

    // 1. Capability narrowed while parked on the binding lock answers 403 claim_session_capability.
    {
        let actor = new_actor(&app).await;
        let t0 = app.clock.now();
        mark_session_as_grant(
            &pool,
            &actor.token,
            t0,
            t0 + chrono::Duration::seconds(3_600),
            "shop.pubky.app",
        )
        .await;
        let client_ip = "203.0.113.54";
        let mut lock_tx = pool.begin().await.expect("lock tx");
        take_binding_lock(&mut lock_tx)
            .await
            .expect("hold binding lock");
        let claim_task = spawn_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body);
        wait_for_advisory_lock_waiter(&pool, STORE_BINDING_LOCK_KEY).await;

        sqlx::query("UPDATE auth_sessions SET capabilities = '' WHERE token_hash = $1")
            .bind(token_hash_for_bearer(&actor.token))
            .execute(&pool)
            .await
            .expect("narrow capability");
        lock_tx.commit().await.expect("release binding lock");

        let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
            .await
            .expect("claim finishes")
            .expect("claim task");
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["reason"], json!("claim_session_capability"));
        assert!(marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none());
    }

    // 2. Grant source client_id invalidated while parked on the binding lock answers 403 claim_session_source.
    {
        let actor = new_actor(&app).await;
        let t0 = app.clock.now();
        mark_session_as_grant(
            &pool,
            &actor.token,
            t0,
            t0 + chrono::Duration::seconds(3_600),
            "shop.pubky.app",
        )
        .await;
        let client_ip = "203.0.113.55";
        let mut lock_tx = pool.begin().await.expect("lock tx");
        take_binding_lock(&mut lock_tx)
            .await
            .expect("hold binding lock");
        let claim_task = spawn_claim_from(app.router.clone(), &actor.token, client_ip, &valid_body);
        wait_for_advisory_lock_waiter(&pool, STORE_BINDING_LOCK_KEY).await;

        sqlx::query(
            "UPDATE auth_sessions SET source_client_id = 'other.pubky.app' WHERE token_hash = $1",
        )
        .bind(token_hash_for_bearer(&actor.token))
        .execute(&pool)
        .await
        .expect("change source_client_id");
        lock_tx.commit().await.expect("release binding lock");

        let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), claim_task)
            .await
            .expect("claim finishes")
            .expect("claim task");
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["reason"], json!("claim_session_source"));
        assert!(marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none());
        assert_full_ten_failure_budget_intact(&app, &actor, client_ip, "shop.pubky.app").await;
    }
}

async fn open_parked_claim_with_prefix(
    address: std::net::SocketAddr,
    token: &str,
    client_ip: &str,
    total_body_len: usize,
    prefix: &[u8],
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;

    let mut stream = open_parked_claim(address, token, client_ip, total_body_len).await;
    stream
        .write_all(prefix)
        .await
        .expect("write initial body byte");
    stream.flush().await.expect("flush initial body byte");
    stream
}

fn router_notifying_first_body_frame(
    router: axum::Router,
    entered: Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let entered = Arc::clone(&entered);
            async move {
                let request = request.map(move |body| {
                    Body::new(body.map_frame(move |frame| {
                        if let Some(tx) = entered.lock().expect("body gate lock").take() {
                            let _ = tx.send(());
                        }
                        frame
                    }))
                });
                next.run(request).await
            }
        },
    ))
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn parked_claim_in_body_read_crossing_max_age_or_revocation_rejects(pool: PgPool) {
    use tokio::io::AsyncWriteExt;

    let mut config = claim_config(CODE);
    config.store_claim_session_max_age_seconds = 1;
    let base = test_app_with_config(pool.clone(), config).await;
    let state = base
        .state
        .clone()
        .with_store_limiters(Arc::new(StoreLimiters::new(Some(HeaderName::from_static(
            "x-test-client",
        )))));
    let app = app_with_state(&base, state);

    let entered_slot: Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>> =
        Arc::new(std::sync::Mutex::new(None));
    let observed_router =
        router_notifying_first_body_frame(app.router.clone(), Arc::clone(&entered_slot));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener addr");
    let server = tokio::spawn({
        let router = observed_router;
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });

    let valid_body = serde_json::to_string(&json!({ "claim_code": CODE })).unwrap();
    let wrong_body = serde_json::to_string(&json!({ "claim_code": WRONG_CODE })).unwrap();

    // 1. Session passes pre-body check at t0, parks in to_bytes after the
    // first body byte is consumed, and crosses STORE_CLAIM_SESSION_MAX_AGE_SECONDS=1
    // by 500 ms before the rest of the body arrives.
    {
        let actor = new_actor(&app).await;
        let t0 = app.clock.now();
        mark_session_as_grant(
            &pool,
            &actor.token,
            t0,
            t0 + chrono::Duration::seconds(3_600),
            "client-fixture",
        )
        .await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        *entered_slot.lock().expect("body gate lock") = Some(entered_tx);
        let mut stream = open_parked_claim_with_prefix(
            address,
            &actor.token,
            "203.0.113.56",
            valid_body.len(),
            &valid_body.as_bytes()[..1],
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .expect("claim reached body read after pre-body check")
            .expect("body frame signal");
        assert_eq!(app.state.store_limiters.claim_in_flight_total(), 1);

        app.clock.set(t0 + chrono::Duration::milliseconds(1_500));
        stream
            .write_all(&valid_body.as_bytes()[1..])
            .await
            .expect("write remaining valid body");
        stream.flush().await.expect("flush valid body");
        let (status, raw) =
            tokio::time::timeout(Duration::from_secs(5), read_http_response(stream))
                .await
                .expect("response arrives");
        assert_eq!(status, 403, "{raw}");
        assert!(raw.contains("claim_session_stale"), "{raw}");
        assert!(marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none());
        wait_until_in_flight(&app, 0).await;
    }

    // 2. Session passes pre-body check, parks in to_bytes, and another session
    // for the same actor revokes it (204) before the valid body finishes,
    // answering 401 without claiming.
    {
        let actor = new_actor(&app).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let admin_token = common::authenticate(&app, &actor.keypair).await;
        let t1 = app.clock.now();
        let claimant_session_id = mark_session_as_grant(
            &pool,
            &actor.token,
            t1,
            t1 + chrono::Duration::seconds(3_600),
            "client-fixture",
        )
        .await;
        sqlx::query(
            "UPDATE auth_sessions SET created_at = $2, expires_at = $3 WHERE token_hash = $1",
        )
        .bind(token_hash_for_bearer(&admin_token))
        .bind(t1)
        .bind(t1 + chrono::Duration::seconds(3_600))
        .execute(&pool)
        .await
        .expect("align admin session window");

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        *entered_slot.lock().expect("body gate lock") = Some(entered_tx);
        let mut stream = open_parked_claim_with_prefix(
            address,
            &actor.token,
            "203.0.113.57",
            valid_body.len(),
            &valid_body.as_bytes()[..1],
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .expect("claim reached body read after pre-body check")
            .expect("body frame signal");
        assert_eq!(app.state.store_limiters.claim_in_flight_total(), 1);

        let (revoke_status, revoke_body) = tokio::time::timeout(
            Duration::from_secs(5),
            send(
                app.router.clone(),
                "DELETE",
                &format!("/v1/auth/sessions/{claimant_session_id}"),
                Some(&admin_token),
                &Value::Null,
            ),
        )
        .await
        .expect("revoke finishes");
        assert_eq!(revoke_status, StatusCode::NO_CONTENT, "{revoke_body}");

        stream
            .write_all(&valid_body.as_bytes()[1..])
            .await
            .expect("write remaining valid body");
        stream.flush().await.expect("flush valid body");
        let (status, raw) =
            tokio::time::timeout(Duration::from_secs(5), read_http_response(stream))
                .await
                .expect("response arrives");
        assert_eq!(status, 401, "{raw}");
        assert!(raw.contains("The session is invalid or expired."), "{raw}");
        assert!(marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none());
        wait_until_in_flight(&app, 0).await;
    }

    // 3. A revoked parked request sending a wrong claim code also answers 401
    // before claim-code verification (no post-revocation code oracle and no
    // wrong-code failure slot consumed).
    {
        let actor = new_actor(&app).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let admin_token = common::authenticate(&app, &actor.keypair).await;
        let t2 = app.clock.now();
        let claimant_session_id = mark_session_as_grant(
            &pool,
            &actor.token,
            t2,
            t2 + chrono::Duration::seconds(3_600),
            "client-fixture",
        )
        .await;
        sqlx::query(
            "UPDATE auth_sessions SET created_at = $2, expires_at = $3 WHERE token_hash = $1",
        )
        .bind(token_hash_for_bearer(&admin_token))
        .bind(t2)
        .bind(t2 + chrono::Duration::seconds(3_600))
        .execute(&pool)
        .await
        .expect("align admin session window");

        let client_ip = "203.0.113.58";
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        *entered_slot.lock().expect("body gate lock") = Some(entered_tx);
        let mut stream = open_parked_claim_with_prefix(
            address,
            &actor.token,
            client_ip,
            wrong_body.len(),
            &wrong_body.as_bytes()[..1],
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .expect("claim reached body read after pre-body check")
            .expect("body frame signal");
        assert_eq!(app.state.store_limiters.claim_in_flight_total(), 1);

        let (revoke_status, revoke_body) = tokio::time::timeout(
            Duration::from_secs(5),
            send(
                app.router.clone(),
                "DELETE",
                &format!("/v1/auth/sessions/{claimant_session_id}"),
                Some(&admin_token),
                &Value::Null,
            ),
        )
        .await
        .expect("revoke finishes");
        assert_eq!(revoke_status, StatusCode::NO_CONTENT, "{revoke_body}");

        stream
            .write_all(&wrong_body.as_bytes()[1..])
            .await
            .expect("write remaining wrong body");
        stream.flush().await.expect("flush wrong body");
        let (status, raw) =
            tokio::time::timeout(Duration::from_secs(5), read_http_response(stream))
                .await
                .expect("response arrives");
        assert_eq!(status, 401, "{raw}");
        assert!(raw.contains("The session is invalid or expired."), "{raw}");
        assert!(marketplace_service::store_seller::read_binding(&pool)
            .await
            .expect("binding check")
            .is_none());
        wait_until_in_flight(&app, 0).await;
        assert_full_ten_failure_budget_intact(&app, &actor, client_ip, "client-fixture").await;
    }

    server.abort();
}
