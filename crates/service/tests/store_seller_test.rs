//! Single-seller mode (`STORE_SELLER_PUBKY`): every command and every
//! signed-in route that names another seller is refused through the one
//! check in `store_seller`, with a refusal-audit row; the store seller, its
//! buyers and sessions for any identity are served; unset, nothing changes.
//!
//! The command and route sets are discovered from the source, not listed
//! here: the command kinds from `parse_command` in the domain crate and the
//! routes from `build_router` in `http.rs`. A kind or route added there
//! without a refusal case here, or without a scope in
//! `store_seller::ROUTES`, fails these tests.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use axum::http::StatusCode;
use marketplace_service::homeserver::HttpHomeserverClient;
use marketplace_service::http::build_router;
use marketplace_service::listing_moderation::{HttpListingModerationClient, ListingModerationGate};
use marketplace_service::refusal_audit::{
    AuditKeys, CommandKind, RefusalAuditRuntime, RefusalKind, SurfaceKind,
};
use marketplace_service::store_seller::{
    other_seller_rows, refuse_other_sellers, route_scope, OtherSellerRows, RouteScope, StoreMode,
    ROUTES,
};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    cancel_drop_command, checkout_command, checkout_command_with_id, close_auction_command,
    counter_offer_command, create_offer_command, drop_aggregate, execute, listing_aggregate,
    new_actor, offer_action, order_command, payment_command, place_bid_command,
    prepare_locks_command, register_command, register_locks_command, release_drop_command,
    reserve_command, send, sync_command, sync_drop_command, test_app, TestActor, TestApp,
    OFFER_COMMAND_ID,
};

/// `(METHOD, path)` for every `.route(..)` in `build_router`.
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
        assert!(
            routes.len() > before,
            "no method found for route {path}: the http.rs parser needs updating"
        );
    }
    routes
}

/// Every kind `parse_command` accepts.
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

#[test]
fn every_router_route_has_a_store_seller_scope_and_no_other_is_listed() {
    let router = router_routes();
    assert!(router.len() > 80, "parsed {} routes", router.len());
    let listed: BTreeSet<(String, String)> = ROUTES
        .iter()
        .map(|rule| (rule.method.to_string(), rule.path.to_string()))
        .collect();
    let unscoped: Vec<_> = router.difference(&listed).collect();
    assert!(
        unscoped.is_empty(),
        "routes without a scope in store_seller::ROUTES: {unscoped:?}"
    );
    let stale: Vec<_> = listed.difference(&router).collect();
    assert!(
        stale.is_empty(),
        "store_seller::ROUTES lists routes the router does not have: {stale:?}"
    );
}

struct Writer {
    runtime: Arc<RefusalAuditRuntime>,
    _pool: PgPool,
    _login: common::RefusalAuditWriterLogin,
}

async fn writer(pool: &PgPool) -> Writer {
    let login = common::claim_refusal_audit_writer_login(pool).await;
    let writer_pool = common::pool_with_limit(
        &login.url,
        marketplace_service::refusal_audit::WRITER_POOL_MAX_CONNECTIONS,
    )
    .await;
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
    let keys = AuditKeys::parse(&root, "1", None, None).expect("keys");
    let runtime = Arc::new(RefusalAuditRuntime::spawn(writer_pool.clone(), keys));
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(15);
    while !runtime.is_ready() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "refusal-audit writer never became ready"
        );
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    Writer {
        runtime,
        _pool: writer_pool,
        _login: login,
    }
}

/// `app` on the same pool and clock, serving only `store`.
fn single_seller(app: &TestApp, store: &str, audit: Option<&Writer>) -> TestApp {
    let mut config = (*app.state.config).clone();
    config.store_mode = StoreMode::SingleSeller(store.to_string());
    let mut state: AppState = app.state.clone();
    state.config = Arc::new(config);
    if let Some(audit) = audit {
        state = state.with_refusal_audit(audit.runtime.clone());
    }
    TestApp {
        router: build_router(state.clone()),
        pool: app.pool.clone(),
        clock: app.clock.clone(),
        state,
    }
}

async fn settle(writer: &Writer, refusals: u64) {
    tokio::time::timeout(
        StdDuration::from_secs(60),
        writer.runtime.wait_until_envelopes_settled(refusals),
    )
    .await
    .expect("refusal audit settles");
    let metrics = writer.runtime.metrics();
    assert_eq!(metrics.delivered, refusals, "{metrics:?}");
}

/// `(command_kind, occurrences)` of the `seller_not_served` buckets on
/// `surface`.
async fn audited(pool: &PgPool, surface: SurfaceKind) -> BTreeMap<i16, i64> {
    sqlx::query_as::<_, (i16, i64)>(
        "SELECT command_kind, SUM(occurrence_count)::BIGINT \
         FROM command_refusal_audit_buckets \
         WHERE surface_kind = $1 AND refusal_kind = $2 \
         GROUP BY command_kind",
    )
    .bind(surface as i16)
    .bind(RefusalKind::SellerNotServed as i16)
    .fetch_all(pool)
    .await
    .expect("refusal buckets")
    .into_iter()
    .collect()
}

async fn domain_rows(pool: &PgPool) -> (i64, i64) {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM command_results)")
        .fetch_one(pool)
        .await
        .expect("domain row counts")
}

/// A seller (`bob`) with a listing, an open offer, and a paid order with its
/// payment and receipt, all made in marketplace mode.
struct OtherSeller {
    bob: TestActor,
    buyer: TestActor,
    order_id: String,
    payment_id: String,
    receipt_id: String,
}

async fn other_seller(app: &TestApp) -> OtherSeller {
    let bob = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = execute(app, &bob.token, &register_command(&bob.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    let (status, body) = execute(app, &buyer.token, &create_offer_command(&bob.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&bob.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 9_001),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment: {body}");
    let receipt_id = body["result"]["receipt"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    OtherSeller {
        bob,
        buyer,
        order_id,
        payment_id,
        receipt_id,
    }
}

fn fresh(mut command: Value) -> Value {
    command["command_id"] = json!(Uuid::new_v4().to_string());
    command
}

/// One envelope per command kind, each naming `bob` or one of his records.
fn refusal_cases(other: &OtherSeller) -> BTreeMap<String, Value> {
    let bob = other.bob.pubky.as_str();
    let order = other.order_id.as_str();
    let listing = listing_aggregate(bob);
    let on_order = |kind: &str, payload: Value| fresh(order_command(kind, order, 1, payload, 1));
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
        let kind = command["kind"].as_str().unwrap().to_string();
        assert!(
            cases.insert(kind.clone(), command).is_none(),
            "{kind} twice"
        );
    };
    add(fresh(register_command(bob, 1)));
    add(fresh(sync_command(bob, "boots_01", 1)));
    add(fresh(sync_drop_command(bob, "winter_drop", 1)));
    add(fresh(cancel_drop_command(bob, "winter_drop", 1, 1)));
    add(fresh(release_drop_command(bob, "winter_drop", 1, 1)));
    add(fresh(reserve_command(bob, 1, 1, 1)));
    add(checkout_command_with_id(bob, &Uuid::new_v4().to_string()));
    add(fresh(create_offer_command(bob, 1)));
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
                "name": "Alice Buyer",
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
    add(fresh(place_bid_command(bob, 1, 7_000, 1)));
    add(fresh(close_auction_command(bob, 1, 1)));
    add(fresh(payment_command(
        &other.payment_id,
        2,
        "confirmed",
        1,
        1,
    )));
    add(fresh(prepare_locks_command(&other.payment_id, 1)));
    add(fresh(register_locks_command(
        &other.payment_id,
        2,
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
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
        "aggregate_id": format!("seller_settings:{bob}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "attestation.set_band_consent",
        "payload": { "allows_amount_band": true },
    }));
    cases
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_command_naming_another_seller_is_refused_and_audited(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let other = other_seller(&base).await;
    let store = new_actor(&base).await;
    let audit = writer(&pool).await;
    let app = single_seller(&base, &store.pubky, Some(&audit));

    let cases = refusal_cases(&other);
    assert_eq!(
        cases.keys().cloned().collect::<BTreeSet<_>>(),
        parsed_command_kinds(),
        "one refusal case per command kind parse_command accepts"
    );
    let before = domain_rows(&pool).await;
    for (kind, command) in &cases {
        // The store seller acting on another seller's records is refused
        // as well as that seller's buyer.
        for actor in [&other.buyer, &store] {
            let (status, body) = execute(&app, &actor.token, command).await;
            assert_eq!(status, StatusCode::CONFLICT, "{kind}: {body}");
            assert_eq!(body["error"]["code"], json!("SELLER_NOT_SERVED"), "{kind}");
            assert_eq!(
                body["error"]["message"],
                json!("This service sells only its store seller's listings."),
                "{kind}"
            );
        }
    }
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&store.token),
        &json!({ "listings": [{ "seller_pubky": other.bob.pubky, "listing_id": "boots_01" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(409), "{body}");
    assert_eq!(
        body["results"][0]["result"]["error"]["code"],
        json!("SELLER_NOT_SERVED"),
        "{body}"
    );
    assert_eq!(
        domain_rows(&pool).await,
        before,
        "no command wrote anything"
    );

    let refusals = cases.len() as u64 * 2 + 1;
    settle(&audit, refusals).await;
    let buckets = audited(&pool, SurfaceKind::V1Command).await;
    let mut expected: BTreeMap<i16, i64> = cases
        .values()
        .map(|command| (command_kind(command) as i16, 2))
        .collect();
    assert_eq!(
        expected.len(),
        cases.len(),
        "one audit kind per command kind"
    );
    *expected
        .get_mut(&(CommandKind::SyncListing as i16))
        .expect("listing.sync case") += 1;
    assert_eq!(buckets, expected);
    assert!(audited(&pool, SurfaceKind::SellerRoute).await.is_empty());
}

fn command_kind(command: &Value) -> CommandKind {
    let parsed = marketplace_domain::commands::parse_command(command)
        .unwrap_or_else(|issues| panic!("fixture does not parse: {issues:?} {command}"));
    marketplace_service::executor::command_kind(&parsed.payload)
}

fn concrete(path: &str, scope: RouteScope, other: &OtherSeller) -> String {
    let bob = other.bob.pubky.as_str();
    let id = match scope {
        RouteScope::Order(_) => other.order_id.clone(),
        RouteScope::Payment(_) => other.payment_id.clone(),
        RouteScope::Receipt(_) => other.receipt_id.clone(),
        _ => Uuid::new_v4().to_string(),
    };
    let aggregate = if path.starts_with("/v1/drops") {
        drop_aggregate(bob, "winter_drop")
    } else {
        listing_aggregate(bob)
    };
    path.replace("{seller_pubky}", bob)
        .replace("{seller}", bob)
        .replace("{pubky}", bob)
        .replace("{aggregate_id}", &aggregate)
        .replace("{listing_id}", "boots_01")
        .replace("{line_index}", "0")
        .replace("{id}", &id)
        .replace("{change_id}", &Uuid::new_v4().to_string())
}

/// [`send`] for a route whose body may not be JSON (CSV, HTML, empty): the
/// text is returned as a JSON string.
async fn request(app: &TestApp, method: &str, uri: &str, token: &str) -> (StatusCode, Value) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::from("{}"))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
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

fn names_a_seller(scope: RouteScope) -> bool {
    !matches!(
        scope,
        RouteScope::Commands | RouteScope::Open(_) | RouteScope::Public(_)
    )
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_signed_in_route_naming_another_seller_is_refused_and_audited(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let other = other_seller(&base).await;
    let store = new_actor(&base).await;
    let audit = writer(&pool).await;
    let app = single_seller(&base, &store.pubky, Some(&audit));

    let before = domain_rows(&pool).await;
    let mut refused = 0u64;
    for rule in ROUTES.iter().filter(|rule| names_a_seller(rule.scope)) {
        let uri = concrete(rule.path, rule.scope, &other);
        let (status, body) = request(&app, rule.method, &uri, &other.bob.token).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "{} {uri}: {body}",
            rule.method
        );
        assert_eq!(
            body["error"]["code"],
            json!("SELLER_NOT_SERVED"),
            "{} {uri}",
            rule.method
        );
        refused += 1;
    }
    assert!(refused > 40, "{refused} seller-scoped routes");

    // The store seller's own seller routes, and every route that names no
    // seller, pass the guard (the handler answers as it does today).
    for rule in ROUTES
        .iter()
        .filter(|rule| matches!(rule.scope, RouteScope::StoreSeller | RouteScope::Open(_)))
    {
        let uri = concrete(rule.path, rule.scope, &other);
        let actor = if rule.scope == RouteScope::StoreSeller {
            &store
        } else {
            &other.bob
        };
        let (status, body) = request(&app, rule.method, &uri, &actor.token).await;
        assert_ne!(
            body["error"]["code"],
            json!("SELLER_NOT_SERVED"),
            "{} {uri}: {status} {body}",
            rule.method
        );
    }
    assert_eq!(
        domain_rows(&pool).await,
        before,
        "no route wrote a domain fact"
    );

    settle(&audit, refused).await;
    let buckets = audited(&pool, SurfaceKind::SellerRoute).await;
    assert_eq!(
        buckets,
        BTreeMap::from([(CommandKind::SellerRoute as i16, refused as i64)])
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_route_but_the_public_ones_needs_a_session_and_so_meets_the_guard(pool: PgPool) {
    let base = test_app(pool).await;
    let other = other_seller(&base).await;
    let app = single_seller(&base, &other.buyer.pubky, None);
    for rule in ROUTES
        .iter()
        .filter(|rule| !matches!(rule.scope, RouteScope::Public(_)))
    {
        let uri = concrete(rule.path, rule.scope, &other);
        let (status, body) = send(app.router.clone(), rule.method, &uri, None, &json!({})).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{} {uri} answered without a session: {body}",
            rule.method
        );
    }
    assert_eq!(
        route_scope("GET", "/v1/not-a-route"),
        None,
        "an unlisted route has no scope and the guard refuses it"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_store_seller_and_its_buyers_are_served(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let app = single_seller(&base, &encode_store(), None);

    // Sessions are issued to the store seller and to any other identity.
    let store = common::actor_from_secret(&app, STORE_SECRET).await;
    let buyer = new_actor(&app).await;
    assert_eq!(store.pubky, encode_store());

    let (status, body) = execute(&app, &store.token, &register_command(&store.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&store.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Changed mind" }),
            1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel request: {body}");

    for (token, uri) in [
        (&buyer.token, format!("/v1/orders/{order_id}")),
        (&store.token, format!("/v1/orders/{order_id}")),
        (&buyer.token, "/v1/orders".to_string()),
        (&store.token, "/v0/sellers/me/payment-config".to_string()),
        (
            &store.token,
            format!("/v1/listings/{}", listing_aggregate(&store.pubky)),
        ),
    ] {
        let (status, body) = send(app.router.clone(), "GET", &uri, Some(token), &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    }

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&buyer.token),
        &json!({ "listings": [{ "seller_pubky": store.pubky, "listing_id": "boots_01" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_ne!(
        body["results"][0]["result"]["error"]["code"],
        json!("SELLER_NOT_SERVED"),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn marketplace_mode_serves_every_seller(pool: PgPool) {
    let app = test_app(pool).await;
    assert_eq!(app.state.config.store_mode, StoreMode::Marketplace);
    let other = other_seller(&app).await;
    let alice = new_actor(&app).await;
    let (status, body) = execute(&app, &alice.token, &register_command(&alice.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for (token, uri) in [
        (&other.buyer.token, format!("/v1/orders/{}", other.order_id)),
        (&other.bob.token, format!("/v1/orders/{}", other.order_id)),
        (
            &other.buyer.token,
            format!("/v1/receipts/{}", other.receipt_id),
        ),
    ] {
        let (status, body) = send(app.router.clone(), "GET", &uri, Some(token), &json!({})).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_command_stored_before_the_switch_replays_its_result(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let bob = new_actor(&base).await;
    let register = register_command(&bob.pubky, 1);
    let (status, first) = execute(&base, &bob.token, &register).await;
    assert_eq!(status, StatusCode::OK, "{first}");

    let store = new_actor(&base).await;
    let app = single_seller(&base, &store.pubky, None);
    let (status, replay) = execute(&app, &bob.token, &register).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, first);

    let (status, body) = execute(&app, &bob.token, &fresh(register)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("SELLER_NOT_SERVED"));
}

async fn health(app: &TestApp) -> Value {
    let (status, body) = send(app.router.clone(), "GET", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_reports_the_mode_resolution_moderation_and_fx_source(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let body = health(&base).await;
    assert_eq!(
        body["store"],
        json!({ "mode": "marketplace", "seller": null })
    );
    assert_eq!(body["homeserver_resolution"], json!(null), "a test double");
    assert_eq!(body["listing_moderation"], json!("off"), "no gate");
    assert_eq!(
        body["fx_source"],
        json!({ "provider": "blocktank", "pinned": true })
    );

    let store = encode_store();
    let single = single_seller(&base, &store, None);
    let body = health(&single).await;
    assert_eq!(
        body["store"],
        json!({ "mode": "single_seller", "seller": store })
    );

    let fixed = common::test_app_with_homeserver_client(
        pool.clone(),
        Arc::new(HttpHomeserverClient::new("http://127.0.0.1:9").expect("client")),
    )
    .await;
    let gated = TestApp {
        router: build_router(fixed.state.clone().with_listing_moderation(Some(Arc::new(
            ListingModerationGate::new(Arc::new(
                HttpListingModerationClient::new("http://127.0.0.1:9").expect("client"),
            )),
        )))),
        ..fixed
    };
    let body = health(&gated).await;
    assert_eq!(body["homeserver_resolution"], json!("fixed"));
    assert_eq!(body["listing_moderation"], json!("on"));

    let unpinned = common::reconfigured(&base, |config| {
        config.fx_feed_url = "http://127.0.0.1:9/fx".to_string();
    });
    assert_eq!(health(&unpinned).await["fx_source"]["pinned"], json!(false));
}

async fn server_revision(pool: &PgPool, seller: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(pool)
        .await
        .expect("listing revision")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn single_seller_mode_refuses_to_start_on_another_sellers_rows(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let store = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let single = StoreMode::SingleSeller(store.pubky.clone());

    // The store seller's own listing, hold and order: it starts.
    let (status, body) = execute(&app, &store.token, &register_command(&store.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&store.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let revision = server_revision(&pool, &store.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve_command(&store.pubky, 1, 1, revision),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reserve: {body}");
    refuse_other_sellers(&pool, &single)
        .await
        .expect("only the store seller's rows");

    // Another seller's listing, hold and order: it refuses, naming them.
    let bob = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &bob.token, &register_command(&bob.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    let error = refuse_other_sellers(&pool, &single)
        .await
        .expect_err("another seller's listing");
    assert!(
        error
            .to_string()
            .contains("holds 1 listings, 0 orders and 0 holds of other sellers"),
        "{error}"
    );
    let (status, body) = execute(&app, &other_buyer.token, &checkout_command(&bob.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let revision = server_revision(&pool, &bob.pubky).await;
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &reserve_command(&bob.pubky, 2, 1, revision),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reserve: {body}");
    assert_eq!(
        other_seller_rows(&pool, &store.pubky).await.expect("rows"),
        OtherSellerRows {
            listings: 1,
            orders: 1,
            holds: 1,
            sellers: vec![format!("{}…", &bob.pubky[..8])],
        }
    );
    let error = refuse_other_sellers(&pool, &single)
        .await
        .expect_err("another seller's listing, hold and order")
        .to_string();
    assert_eq!(
        error,
        format!(
            "STORE_SELLER_PUBKY is set, but the database holds 1 listings, 1 orders and 1 holds \
             of other sellers ({}…); single-seller mode needs a database that only its store \
             seller has used; refusing to start",
            &bob.pubky[..8]
        )
    );
    assert!(!error.contains(&bob.pubky), "{error}");

    // Marketplace mode serves every seller and checks nothing.
    refuse_other_sellers(&pool, &StoreMode::Marketplace)
        .await
        .expect("marketplace mode");
}

const STORE_SECRET: &str = "0505050505050505050505050505050505050505050505050505050505050505";

fn encode_store() -> String {
    pubky_common::crypto::Keypair::from_secret(&[5u8; 32])
        .public_key()
        .z32()
}
