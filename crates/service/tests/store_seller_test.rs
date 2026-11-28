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
use marketplace_service::http::{build_router, routes, serve};
use marketplace_service::listing_moderation::{HttpListingModerationClient, ListingModerationGate};
use marketplace_service::refusal_audit::{
    AuditKeys, CommandKind, RefusalAuditRuntime, RefusalKind, SurfaceKind,
};
use marketplace_service::store_seller::{
    other_seller_rows, refuse_other_sellers, route_scope, OtherSellerRows, RouteScope, StoreMode,
    CONDITIONALLY_SCANNED_SELLER_TABLES, ROUTES, SCANNED_SELLER_TABLES,
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
    OFFER_COMMAND_ID, TEST_BUNDLE_ID,
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
    config.store_mode = StoreMode::SingleSeller {
        pinned: Some(store.to_string()),
    };
    let mut state: AppState = app.state.clone();
    state.store = Arc::new(marketplace_service::store_seller::StoreBinding::from_mode(
        &config.store_mode,
        None,
    ));
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

/// The kinds whose payload names nothing their aggregate does not: the
/// listing (inventory, offers, bids, pickup and digital details), the drop,
/// or the actor's own seller settings. Every other kind names a seller or a
/// record in its payload.
const AGGREGATE_ONLY: &[&str] = &[
    "drop.cancel",
    "drop.release_listings",
    "inventory.reserve",
    "offer.create",
    "auction.place_bid",
    "auction.close",
    "pickup_details.set",
    "pickup_details.clear",
    "digital_delivery.set",
    "digital_delivery.clear",
    "attestation.set_band_consent",
];

/// `command` under an aggregate that names nothing of `bob`'s: `store`'s
/// own listing or drop, or a fresh id of the same kind. A checkout's
/// aggregate names nothing already.
fn neutral(mut command: Value, bob: &str, store: &str) -> Value {
    let aggregate = command["aggregate_id"].as_str().unwrap().to_string();
    let (kind, rest) = aggregate.split_once(':').expect("kind:id aggregate");
    command["aggregate_id"] = json!(match kind {
        "listing" | "drop" => format!("{kind}:{}", rest.replacen(bob, store, 1)),
        "checkout" => aggregate.clone(),
        _ => format!("{kind}:{}", Uuid::new_v4()),
    });
    fresh(command)
}

/// For every kind not in [`AGGREGATE_ONLY`], the refusal case under a
/// neutral aggregate, so only the payload names `bob` or his records. An
/// offer checkout names the offer and the listing, so it has a case for
/// each.
fn payload_cases(other: &OtherSeller, store: &str) -> BTreeMap<String, Value> {
    let bob = other.bob.pubky.as_str();
    let mut cases: BTreeMap<String, Value> = refusal_cases(other)
        .into_iter()
        .filter(|(kind, _)| !AGGREGATE_ONLY.contains(&kind.as_str()))
        .map(|(kind, command)| (kind, neutral(command, bob, store)))
        .collect();
    let checkout = cases.remove("offer.checkout").expect("offer.checkout case");
    let mut by_offer = fresh(checkout.clone());
    by_offer["payload"]["listing_aggregate_id"] = json!(listing_aggregate(store));
    let mut by_listing = fresh(checkout);
    by_listing["payload"]["offer_id"] = json!(Uuid::new_v4().to_string());
    cases.insert("offer.checkout (offer)".to_string(), by_offer);
    cases.insert("offer.checkout (listing)".to_string(), by_listing);
    cases
}

/// Each payload subject in `store_seller::command_subjects` is needed on
/// its own: with the aggregate naming nothing of the other seller's, the
/// payload alone is refused.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_payload_naming_another_seller_is_refused_under_a_neutral_aggregate(pool: PgPool) {
    let base = test_app(pool.clone()).await;
    let other = other_seller(&base).await;
    let store = new_actor(&base).await;
    let app = single_seller(&base, &store.pubky, None);

    let cases = payload_cases(&other, &store.pubky);
    let kinds: BTreeSet<String> = cases
        .values()
        .map(|command| command["kind"].as_str().unwrap().to_string())
        .collect();
    let aggregate_only: BTreeSet<String> = AGGREGATE_ONLY.iter().map(ToString::to_string).collect();
    assert!(aggregate_only.is_subset(&parsed_command_kinds()));
    assert_eq!(
        kinds
            .union(&aggregate_only)
            .cloned()
            .collect::<BTreeSet<_>>(),
        parsed_command_kinds(),
        "a kind either names something in its payload or is listed in AGGREGATE_ONLY"
    );
    let bobs = [
        other.bob.pubky.as_str(),
        other.order_id.as_str(),
        other.payment_id.as_str(),
        OFFER_COMMAND_ID,
    ];
    let before = domain_rows(&pool).await;
    for (case, command) in &cases {
        let aggregate = command["aggregate_id"].as_str().unwrap();
        assert!(
            bobs.iter().all(|bob| !aggregate.contains(bob)),
            "{case}: {aggregate} is not neutral"
        );
        for actor in [&other.buyer, &store] {
            let (status, body) = execute(&app, &actor.token, command).await;
            assert_eq!(status, StatusCode::CONFLICT, "{case}: {body}");
            assert_eq!(
                body["error"]["code"],
                json!("SELLER_NOT_SERVED"),
                "{case}: {body}"
            );
        }
    }
    assert_eq!(
        domain_rows(&pool).await,
        before,
        "no command wrote anything"
    );
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

/// `(endpoint_class, tokens, updated_at)` of a seller's inventory buckets.
async fn inventory_buckets(pool: &PgPool, seller: &str) -> Vec<(String, f64, String)> {
    sqlx::query_as(
        "SELECT endpoint_class, tokens, updated_at::TEXT FROM inventory_actor_rate_limits \
         WHERE actor_pubky = $1 ORDER BY endpoint_class",
    )
    .bind(seller)
    .fetch_all(pool)
    .await
    .expect("inventory buckets")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn inventory_routes_serve_the_store_seller_and_refuse_others_before_their_budget(
    pool: PgPool,
) {
    let base = test_app(pool.clone()).await;
    let app = single_seller(&base, &encode_store(), None);
    let store = common::actor_from_secret(&app, STORE_SECRET).await;
    let bob = new_actor(&app).await;
    let (status, body) = execute(&app, &store.token, &register_command(&store.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    let aggregate = listing_aggregate(&store.pubky);
    let projection = format!("/v1/inventory/listings/{aggregate}");
    let adjust = |revision: i64| {
        json!({
            "schema_version": 1,
            "kind": "inventory.adjust",
            "aggregate_id": aggregate,
            "listing_id": "boots_01",
            "expected_revision": revision,
            "delta": 1,
            "idempotency_key": Uuid::new_v4(),
        })
    };

    // The store seller reads and adjusts its own inventory on its own
    // per-seller budget.
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &projection,
        Some(&store.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "store read: {body}");
    assert_eq!(body["seller_pubky"], json!(store.pubky));
    assert_eq!(body["stock"]["total"], json!(3));
    let revision = server_revision(&pool, &store.pubky).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(&store.token),
        &adjust(revision),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "store adjust: {body}");
    let store_buckets = inventory_buckets(&pool, &store.pubky).await;
    assert_eq!(
        store_buckets
            .iter()
            .map(|(class, _, _)| class.as_str())
            .collect::<Vec<_>>(),
        ["inventory.adjust", "inventory.read"],
        "{store_buckets:?}"
    );

    // Another seller is refused by the guard before the handler, so it
    // neither opens a bucket of its own nor spends the store seller's.
    let revision = server_revision(&pool, &store.pubky).await;
    for (method, body) in [("GET", Value::Null), ("POST", adjust(revision))] {
        let uri = if method == "GET" {
            projection.as_str()
        } else {
            "/v1/inventory/adjust"
        };
        let (status, body) = send(app.router.clone(), method, uri, Some(&bob.token), &body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method} {uri}: {body}");
        assert_eq!(body["error"]["code"], json!("SELLER_NOT_SERVED"));
    }
    assert!(inventory_buckets(&pool, &bob.pubky).await.is_empty());
    assert_eq!(inventory_buckets(&pool, &store.pubky).await, store_buckets);
    assert_eq!(server_revision(&pool, &store.pubky).await, revision);

    // Signing in spends the identity's own `session.create` budget; those
    // buckets are not another seller's records and do not stop a restart.
    let session_buckets: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM automation_rate_limits \
         WHERE endpoint_class = 'session.create' AND seller_pubky IN ($1, $2)",
    )
    .bind(&store.pubky)
    .bind(&bob.pubky)
    .fetch_one(&pool)
    .await
    .expect("session buckets");
    assert_eq!(session_buckets, 2);
    refuse_other_sellers(&pool, &app.state.config.store_mode)
        .await
        .expect("rate buckets are not another seller's rows");
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
    let single = StoreMode::SingleSeller {
        pinned: Some(store.pubky.clone()),
    };

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
            .contains("holds 1 listings, 0 orders, 0 holds, 0 store connections"),
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
        other_seller_rows(&pool, Some(&store.pubky))
            .await
            .expect("rows"),
        OtherSellerRows {
            listings: 1,
            orders: 1,
            holds: 1,
            sellers: vec![format!("{}…", &bob.pubky[..8])],
            ..OtherSellerRows::default()
        }
    );
    let error = refuse_other_sellers(&pool, &single)
        .await
        .expect_err("another seller's listing, hold and order")
        .to_string();
    assert_eq!(
        error,
        format!(
            "STORE_SELLER_PUBKY is set, but the database holds 1 listings, 1 orders, 1 holds, \
             0 store connections, 0 payment configurations, 0 known Paykit sellers, 0 webhook \
             endpoints, 0 seller settings, 0 shipping configurations, 0 drops, 0 seller stat \
             attestations, 0 seller moderation decisions and 0 gateway refund inbox rows of \
             other sellers ({}…); single-seller mode needs a database that only its store seller \
             has used; refusing to start",
            &bob.pubky[..8]
        )
    );
    assert!(!error.contains(&bob.pubky), "{error}");

    // Marketplace mode serves every seller and checks nothing.
    refuse_other_sellers(&pool, &StoreMode::Marketplace)
        .await
        .expect("marketplace mode");
}

async fn unlisted() -> axum::Json<Value> {
    axum::Json(json!({ "served": true }))
}

/// One layer over the whole router refuses, in single-seller mode, a route
/// missing from `store_seller::ROUTES` however it is mounted: nested,
/// merged, or routed in place, on no guarded router and with no session.
/// Listed routes on the same router are served, and marketplace mode
/// serves the unlisted ones too.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_route_missing_from_the_scope_table_is_refused_wherever_it_is_mounted(pool: PgPool) {
    use axum::routing::{get, post};
    let base = test_app(pool.clone()).await;
    let store = new_actor(&base).await;
    let audit = writer(&pool).await;
    let single = single_seller(&base, &store.pubky, Some(&audit));
    let with_unlisted = |state: &AppState| {
        serve(
            routes(state)
                .nest(
                    "/v1/nested",
                    axum::Router::new().route("/{id}", get(unlisted)),
                )
                .merge(axum::Router::new().route("/v1/merged", post(unlisted)))
                .route("/v1/inline", get(unlisted)),
            state.clone(),
        )
    };
    let added = [
        ("GET", "/v1/nested/{id}", "/v1/nested/7"),
        ("POST", "/v1/merged", "/v1/merged"),
        ("GET", "/v1/inline", "/v1/inline"),
    ];
    for (method, path, _) in added {
        assert_eq!(route_scope(method, path), None, "{method} {path} is listed");
    }

    let marketplace = with_unlisted(&base.state);
    let router = with_unlisted(&single.state);
    for (method, path, uri) in added {
        let (status, body) = send(marketplace.clone(), method, uri, None, &json!({})).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "marketplace {method} {path}: {body}"
        );
        assert_eq!(body, json!({ "served": true }));
        for token in [None, Some(store.token.as_str())] {
            let (status, body) = send(router.clone(), method, uri, token, &json!({})).await;
            assert_eq!(status, StatusCode::CONFLICT, "{method} {path}: {body}");
            assert_eq!(
                body["error"]["code"],
                json!("SELLER_NOT_SERVED"),
                "{method} {path}"
            );
        }
    }
    let (status, body) = send(router.clone(), "GET", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "a listed public route: {body}");
    let (status, body) = send(
        router.clone(),
        "GET",
        "/v1/connectors",
        Some(&store.token),
        &json!({}),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::CONFLICT,
        "a listed seller route: {body}"
    );
    // A method the path does not serve is not in the table either: refused
    // in single-seller mode, the router's 405 in marketplace mode. A path
    // no route matches stays the router's 404.
    let (status, _) = send(marketplace.clone(), "PUT", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let (status, body) = send(router.clone(), "PUT", "/health", None, &json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    for router in [&marketplace, &router] {
        let (status, _) = send(router.clone(), "GET", "/v1/nowhere", None, &json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // The audit tags an authenticated actor: only the refusals that carried
    // the store seller's session are written.
    let refusals = added.len() as u64;
    settle(&audit, refusals).await;
    assert_eq!(
        audited(&pool, SurfaceKind::SellerRoute).await,
        BTreeMap::from([(CommandKind::SellerRoute as i16, refusals as i64)])
    );
}

/// One seller-keyed record the boot check counts besides listings, orders
/// and holds: its table, an insert binding `$1` (seller) and `$2` (time),
/// and the count it adds.
struct Category {
    table: &'static str,
    insert: &'static str,
    counted: fn(&mut OtherSellerRows) -> &mut i64,
}

const CATEGORIES: &[Category] = &[
    Category {
        table: "connector_connections",
        insert: "INSERT INTO connector_connections \
                     (id, seller_pubky, provider, shop, status, created_at, updated_at) \
                 VALUES (gen_random_uuid(), $1, 'shopify', left($1, 20) || '.myshopify.com', \
                         'disconnected', $2, $2)",
        counted: |rows| &mut rows.connector_connections,
    },
    Category {
        table: "seller_payment_configs",
        insert: "INSERT INTO seller_payment_configs \
                     (seller_pubky, bitcoin_enabled, created_at, updated_at) \
                 VALUES ($1, TRUE, $2, $2)",
        counted: |rows| &mut rows.payment_configs,
    },
    Category {
        table: "seller_accepted_payment_options",
        insert: "INSERT INTO seller_accepted_payment_options \
                     (seller_pubky, option_id, enabled, updated_at) \
                 VALUES ($1, 'paykit.usdt.arbitrum-one', FALSE, $2)",
        counted: |rows| &mut rows.payment_configs,
    },
    Category {
        table: "paykit_known_sellers",
        insert: "INSERT INTO paykit_known_sellers (seller_pubky, first_ready_at) \
                 VALUES ($1, $2)",
        counted: |rows| &mut rows.paykit_known_sellers,
    },
    Category {
        table: "webhook_endpoints",
        insert: "INSERT INTO webhook_endpoints \
                     (id, seller_pubky, endpoint_url, key_id, signing_key, created_at, \
                      updated_at, deleted_at) \
                 VALUES (gen_random_uuid(), $1, 'https://hooks.example/' || $1, \
                         gen_random_uuid(), decode(repeat('ab', 32), 'hex'), $2, $2, $2)",
        counted: |rows| &mut rows.webhook_endpoints,
    },
    Category {
        table: "attestation_band_consents",
        insert: "INSERT INTO attestation_band_consents \
                     (seller_pubky, allows_amount_band, revision, updated_at) \
                 VALUES ($1, FALSE, 1, $2)",
        counted: |rows| &mut rows.seller_settings,
    },
    Category {
        table: "shipping_configs",
        insert: "INSERT INTO shipping_configs (seller_pubky, created_at, updated_at) \
                 VALUES ($1, $2, $2)",
        counted: |rows| &mut rows.shipping_configs,
    },
    Category {
        table: "drops",
        insert: "INSERT INTO drops \
                     (aggregate_id, seller_pubky, drop_id, record_revision, revision, state, \
                      format, starts_at, total_quantity, per_buyer_limit, remaining_quantity, \
                      paid_quantity, stock_display, listing_ids, created_at, updated_at) \
                 VALUES ('drop:' || $1 || '_setup', $1, 'setup', 1, 1, 'announced', 'fcfs', \
                         $2, 1, 1, 1, 0, 'hidden', '[]'::jsonb, $2, $2)",
        counted: |rows| &mut rows.drops,
    },
    Category {
        table: "seller_stat_attestations",
        insert: "INSERT INTO seller_stat_attestations \
                     (id, seller_pubky, period_from, period_to, body, jws, created_at) \
                 VALUES (gen_random_uuid(), $1, DATE '2026-01-01', DATE '2026-01-07', \
                         '{}'::jsonb, 'fixture', $2)",
        counted: |rows| &mut rows.seller_stat_attestations,
    },
];

/// A seller with no listing, order or hold still keeps single-seller mode
/// from starting through a store connection, a payment configuration or
/// accepted option, a webhook endpoint or seller settings. Each counts on
/// its own, whatever its state (a disconnected connection, a disabled
/// option and a deleted endpoint are seeded).
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn single_seller_mode_refuses_to_start_on_another_sellers_settings_and_integrations(
    pool: PgPool,
) {
    let store = marketplace_domain::pubky::encode_pubky(&[11; 32]);
    let carol = marketplace_domain::pubky::encode_pubky(&[12; 32]);
    let single = StoreMode::SingleSeller {
        pinned: Some(store.clone()),
    };
    let at = chrono::Utc::now();
    let seed = |category: &Category, seller: &str| {
        sqlx::query(category.insert)
            .bind(seller.to_string())
            .bind(at)
            .execute(&pool)
    };

    let mut tested_scanned: BTreeSet<&str> = CATEGORIES.iter().map(|c| c.table).collect();
    tested_scanned.insert("listings");
    tested_scanned.insert("orders");
    let declared_scanned: BTreeSet<&str> = SCANNED_SELLER_TABLES.iter().copied().collect();
    assert_eq!(
        tested_scanned, declared_scanned,
        "every table in SCANNED_SELLER_TABLES must have a live row-seeding test"
    );

    for category in CATEGORIES {
        seed(category, &store).await.expect(category.table);
    }
    assert_eq!(
        other_seller_rows(&pool, Some(&store)).await.expect("rows"),
        OtherSellerRows::default()
    );
    refuse_other_sellers(&pool, &single)
        .await
        .expect("only the store seller's rows");

    for category in CATEGORIES {
        seed(category, &carol).await.expect(category.table);
        let mut expected = OtherSellerRows {
            sellers: vec![format!("{}…", &carol[..8])],
            ..OtherSellerRows::default()
        };
        *(category.counted)(&mut expected) = 1;
        assert_eq!(
            other_seller_rows(&pool, Some(&store)).await.expect("rows"),
            expected,
            "{}",
            category.table
        );
        refuse_other_sellers(&pool, &single)
            .await
            .expect_err(category.table);
        sqlx::query(&format!(
            "DELETE FROM {} WHERE seller_pubky = $1",
            category.table
        ))
        .bind(&carol)
        .execute(&pool)
        .await
        .expect("removed");
        refuse_other_sellers(&pool, &single)
            .await
            .unwrap_or_else(|error| panic!("{} removed: {error}", category.table));
    }

    for category in CATEGORIES {
        seed(category, &carol).await.expect(category.table);
    }
    let error = refuse_other_sellers(&pool, &single)
        .await
        .expect_err("all of them")
        .to_string();
    assert_eq!(
        error,
        format!(
            "STORE_SELLER_PUBKY is set, but the database holds 0 listings, 0 orders, 0 holds, \
             1 store connections, 1 payment configurations, 1 known Paykit sellers, 1 webhook \
             endpoints, 1 seller settings, 1 shipping configurations, 1 drops, 1 seller stat \
             attestations, 0 seller moderation decisions and 0 gateway refund inbox rows of \
             other sellers ({}…); single-seller mode needs a database that only its store seller \
             has used; refusing to start",
            &carol[..8]
        ),
        "a configuration and an accepted option of one seller are one payment configuration"
    );
    assert!(!error.contains(&carol), "{error}");
}

/// `connector-worker` on `pool`'s database, connectors on, serving `store`.
fn connector_worker(pool: &PgPool, store: &str) -> std::process::Command {
    let mut database = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    database.set_path(&format!(
        "/{}",
        pool.connect_options()
            .get_database()
            .expect("test database name")
    ));
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
    let mut worker = std::process::Command::new(env!("CARGO_BIN_EXE_connector-worker"));
    worker
        .env_clear()
        .env("DATABASE_URL", database.as_str())
        .env(
            "REFUSAL_AUDIT_DATABASE_URL",
            "postgres://marketplace_refusal_audit_writer_login@audit.example/refusal",
        )
        .env(
            "REFUSAL_AUDIT_RETENTION_DATABASE_URL",
            "postgres://marketplace_refusal_audit_retention@audit.example/refusal",
        )
        .env("REFUSAL_AUDIT_HMAC_ROOT_B64", root)
        .env("REFUSAL_AUDIT_HMAC_KEY_EPOCH", "1")
        .env("REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED", "true")
        .env("REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED", "true")
        .env("REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED", "true")
        .env("CONNECTORS_ENABLED", "true")
        .env("CONNECTOR_PUBLIC_BASE_URL", "https://market.example")
        .env("CONNECTOR_RETURN_URL", "https://market.example/connectors")
        .env("CONNECTOR_CREDENTIAL_ENCRYPTION_KEY", "55".repeat(32))
        .env("STORE_SELLER_PUBKY", store);
    worker
}

/// The connector worker runs the same boot check as the service: it refuses
/// to start in single-seller mode on another seller's rows, and starts once
/// they are the store seller's.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_connector_worker_refuses_to_start_on_another_sellers_rows(pool: PgPool) {
    // `TEST_MIGRATOR` wraps the catalog in test-only lock versions that the
    // worker's migrator does not know; leave the ledger production holds.
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (0, 9000000001)")
        .execute(&pool)
        .await
        .expect("drop test-only ledger rows");
    let store = marketplace_domain::pubky::encode_pubky(&[11; 32]);
    let carol = marketplace_domain::pubky::encode_pubky(&[12; 32]);
    sqlx::query(CATEGORIES[0].insert)
        .bind(&carol)
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("carol's store connection");

    match boot(connector_worker(&pool, &store)) {
        Boot::Started => panic!("the worker started on another seller's store connection"),
        Boot::Refused(stderr) => assert!(
            stderr.contains(&format!(
                "STORE_SELLER_PUBKY is set, but the database holds 0 listings, 0 orders, 0 \
                 holds, 1 store connections, 0 payment configurations, 0 known Paykit sellers, 0 \
                 webhook endpoints, 0 seller settings, 0 shipping configurations, 0 drops, 0 \
                 seller stat attestations, 0 seller moderation decisions and 0 gateway refund \
                 inbox rows of other sellers ({}…)",
                &carol[..8]
            )),
            "{stderr}"
        ),
    }
    match boot(connector_worker(&pool, &carol)) {
        Boot::Started => {}
        Boot::Refused(stderr) => panic!("the worker refused its store seller's rows: {stderr}"),
    }
}

enum Boot {
    /// Exited before it started work, with what it wrote to stderr.
    Refused(String),
    /// Logged that it is starting; it has been stopped.
    Started,
}

/// Runs `worker` until it exits or logs that it is starting, for at most a
/// minute.
fn boot(mut worker: std::process::Command) -> Boot {
    use std::io::{BufRead, Read};
    let mut child = worker
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the worker runs");
    let mut stderr = child.stderr.take().expect("stderr");
    let stderr = std::thread::spawn(move || {
        let mut text = String::new();
        stderr.read_to_string(&mut text).expect("stderr is text");
        text
    });
    let stdout = child.stdout.take().expect("stdout");
    let (lines, logged) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = std::time::Instant::now() + StdDuration::from_secs(60);
    loop {
        if logged
            .recv_timeout(StdDuration::from_millis(100))
            .is_ok_and(|line| line.contains("connector worker starting"))
        {
            child.kill().expect("stop the worker");
            child.wait().expect("the worker stops");
            return Boot::Started;
        }
        if let Some(status) = child.try_wait().expect("worker status") {
            assert!(!status.success(), "the worker exited cleanly");
            return Boot::Refused(stderr.join().expect("stderr reader"));
        }
        if std::time::Instant::now() > deadline {
            child.kill().expect("stop the worker");
            child.wait().expect("the worker stops");
            panic!("the worker neither started nor exited within a minute");
        }
    }
}

const STORE_SECRET: &str = "0505050505050505050505050505050505050505050505050505050505050505";

fn encode_store() -> String {
    pubky_common::crypto::Keypair::from_secret(&[5u8; 32])
        .public_key()
        .z32()
}

/// Strict foreign-key edges in `public` read directly from `pg_catalog`:
/// grouped by constraint `oid` so every column of a composite foreign key must
/// be `NOT NULL` (`bool_and(att.attnotnull)`), and `ON DELETE SET NULL` (`n`)
/// / `ON DELETE SET DEFAULT` (`d`) constraints are excluded.
async fn strict_fk_edges<'e, E>(executor: E) -> Vec<(String, String)>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_as(
        "SELECT DISTINCT child.relname::text, parent.relname::text \
         FROM pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class child ON child.oid = con.conrelid \
         JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace \
         JOIN pg_catalog.pg_class parent ON parent.oid = con.confrelid \
         JOIN pg_catalog.pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace \
         JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE \
         JOIN pg_catalog.pg_attribute att \
           ON att.attrelid = con.conrelid AND att.attnum = k.attnum \
         WHERE con.contype = 'f' \
           AND child_ns.nspname = 'public' \
           AND parent_ns.nspname = 'public' \
           AND con.confdeltype NOT IN ('n', 'd') \
         GROUP BY con.oid, child.relname, parent.relname \
         HAVING bool_and(att.attnotnull)",
    )
    .fetch_all(executor)
    .await
    .expect("strict pg_constraint foreign keys")
}

/// Guards against future migrations adding ANY table that escapes the store
/// emptiness audit. Every base table in `public` (excluding `_sqlx_migrations`),
/// with zero column-name regex pre-filtering, must belong to exactly one
/// audited bucket: directly scanned roots (`SCANNED_SELLER_TABLES`),
/// conditionally scanned mixed tables (`CONDITIONALLY_SCANNED_SELLER_TABLES`),
/// strict `NOT NULL` foreign-key descendants of `SCANNED_SELLER_TABLES`
/// (verified transitively via `pg_catalog.pg_constraint`), application-scoped
/// children of never-hard-deleted scanned parents, or explicitly audited
/// non-seller tables.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn schema_exhaustiveness_partitions_every_public_base_table(pool: PgPool) {
    // Tables reachable via strict NOT NULL, non-SET-NULL/SET-DEFAULT Postgres
    // foreign keys (direct or transitive) to `SCANNED_SELLER_TABLES`.
    const FK_DESCENDANT_TABLES: &[&str] = &[
        "bids",
        "connector_alerts",
        "connector_app_credentials",
        "connector_credentials",
        "connector_inbound_events",
        "connector_order_actions",
        "connector_order_push",
        "connector_product_changes",
        "connector_product_selection",
        "connector_product_snapshots",
        "connector_recipient_keys",
        "connector_sync_overrides",
        "connector_sync_settings",
        "connector_variant_map",
        "drop_listings",
        "drop_purchases",
        "inventory_adjustment_conflicts",
        "inventory_adjustment_results",
        "inventory_external_refs",
        "listing_auction_reserves",
        "listing_variant_conversion_blocks",
        "listing_variant_inventory",
        "offers",
        "order_delivery_emails",
        "order_digital_access",
        "order_digital_pins",
        "order_gateway_refunds",
        "order_refund_destinations",
        "paykit_manual_resolutions",
        "paykit_resolve_outbox",
        "paykit_seller_confirmations",
        "paykit_superseded_attempts",
        "payment_locks_correlations",
        "payment_locks_observations",
        "payments",
        "pickup_handovers",
        "pickup_line_snapshots",
        "receipts",
        "reservations",
        "review_attestations",
        "review_notifications",
        "reviews",
        "webhook_deliveries",
    ];

    // Child tables without a DB-level FOREIGN KEY whose service writers
    // require an existing row in the named parent table (neither `listings`,
    // `orders` nor `webhook_endpoints` is ever hard-deleted).
    const APP_SCOPED_CHILD_TABLES: &[(&str, &str)] = &[
        ("attestation_annotations", "orders"),
        ("listing_deletion_cursors", "listings"),
        ("listing_digital_counters", "listings"),
        ("listing_digital_versions", "listings"),
        ("listing_pickup_details", "listings"),
        ("listing_pickup_version_counters", "listings"),
        ("listing_revival_checks", "listings"),
        ("payment_locks_binding_outcomes", "orders"),
        ("payment_locks_checkout_snapshots", "orders"),
        ("webhook_dead_letters", "webhook_endpoints"),
    ];

    // Non-seller tables: singleton binding, shared actor/identity/buyer tables,
    // rate-limit buckets, refusal-audit subsystem and global service state.
    const NON_SELLER_TABLES: &[&str] = &[
        "attestor_publish_attempts",
        "attestor_publications",
        "auth_sessions",
        "auth_token_uses",
        "automation_rate_limits",
        "command_refusal_audit_access_buckets",
        "command_refusal_audit_access_limits",
        "command_refusal_audit_bucket_limits",
        "command_refusal_audit_buckets",
        "command_refusal_audit_epoch_inventory",
        "command_refusal_audit_loss_gap",
        "command_refusal_command_kinds",
        "command_refusal_kinds",
        "command_refusal_surface_kinds",
        "command_results",
        "connector_event_cursor",
        "digital_read_rate_limits",
        "events",
        "fx_rate_samples",
        "grant_flows",
        "grant_rate_limits",
        "grant_result_nonces",
        "grant_service_requests",
        "grant_session_challenges",
        "inventory_actor_rate_limits",
        "inventory_rate_limits",
        "notifications",
        "outbox",
        "store_seller_binding",
        "user_priv_key_custody_releases",
        "user_priv_keys",
        "worker_leases",
    ];

    let live_tables: BTreeSet<String> = sqlx::query_scalar(
        "SELECT table_name \
         FROM information_schema.tables \
         WHERE table_schema = 'public' \
           AND table_type = 'BASE TABLE' \
           AND table_name <> '_sqlx_migrations' \
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .expect("live public base tables")
    .into_iter()
    .collect();

    let scanned: BTreeSet<&str> = SCANNED_SELLER_TABLES.iter().copied().collect();
    let conditional: BTreeSet<&str> = CONDITIONALLY_SCANNED_SELLER_TABLES
        .iter()
        .copied()
        .collect();
    let fk_descendants: BTreeSet<&str> = FK_DESCENDANT_TABLES.iter().copied().collect();
    let app_children: BTreeSet<&str> = APP_SCOPED_CHILD_TABLES.iter().map(|(t, _)| *t).collect();
    let non_seller: BTreeSet<&str> = NON_SELLER_TABLES.iter().copied().collect();

    assert_eq!(
        scanned.len(),
        SCANNED_SELLER_TABLES.len(),
        "duplicate in SCANNED_SELLER_TABLES"
    );
    assert_eq!(
        conditional.len(),
        CONDITIONALLY_SCANNED_SELLER_TABLES.len(),
        "duplicate in CONDITIONALLY_SCANNED_SELLER_TABLES"
    );
    assert_eq!(
        fk_descendants.len(),
        FK_DESCENDANT_TABLES.len(),
        "duplicate in FK_DESCENDANT_TABLES"
    );
    assert_eq!(
        app_children.len(),
        APP_SCOPED_CHILD_TABLES.len(),
        "duplicate in APP_SCOPED_CHILD_TABLES"
    );
    assert_eq!(
        non_seller.len(),
        NON_SELLER_TABLES.len(),
        "duplicate in NON_SELLER_TABLES"
    );

    let buckets: [(&str, &BTreeSet<&str>); 5] = [
        ("SCANNED_SELLER_TABLES", &scanned),
        ("CONDITIONALLY_SCANNED_SELLER_TABLES", &conditional),
        ("FK_DESCENDANT_TABLES", &fk_descendants),
        ("APP_SCOPED_CHILD_TABLES", &app_children),
        ("NON_SELLER_TABLES", &non_seller),
    ];
    for i in 0..buckets.len() {
        for j in (i + 1)..buckets.len() {
            let overlap: Vec<&str> = buckets[i].1.intersection(buckets[j].1).copied().collect();
            assert!(
                overlap.is_empty(),
                "overlap between {} and {}: {overlap:?}",
                buckets[i].0,
                buckets[j].0
            );
        }
    }

    let mut classified: BTreeSet<String> = BTreeSet::new();
    for (_, set) in buckets {
        for table in set {
            classified.insert((*table).to_string());
        }
    }

    let unclassified: Vec<&String> = live_tables.difference(&classified).collect();
    assert!(
        unclassified.is_empty(),
        "unclassified base tables in migrated public schema (add to other_seller_rows_on and \
         SCANNED_SELLER_TABLES / CONDITIONALLY_SCANNED_SELLER_TABLES or explicitly classify): \
         {unclassified:?}"
    );
    let stale: Vec<&String> = classified.difference(&live_tables).collect();
    assert!(
        stale.is_empty(),
        "stale table names in schema exhaustiveness classification: {stale:?}"
    );

    // Every directly scanned root table must actually hold a NOT NULL
    // `seller_pubky` column in `pg_attribute`.
    let not_null_seller_cols: BTreeSet<String> = sqlx::query_scalar(
        "SELECT cls.relname::text \
         FROM pg_catalog.pg_class cls \
         JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace \
         JOIN pg_catalog.pg_attribute att ON att.attrelid = cls.oid \
         WHERE ns.nspname = 'public' \
           AND cls.relkind = 'r' \
           AND att.attname = 'seller_pubky' \
           AND att.attnotnull \
           AND NOT att.attisdropped",
    )
    .fetch_all(&pool)
    .await
    .expect("NOT NULL seller_pubky columns")
    .into_iter()
    .collect();
    for table in SCANNED_SELLER_TABLES {
        assert!(
            not_null_seller_cols.contains(*table),
            "SCANNED_SELLER_TABLES entry `{table}` must have a NOT NULL `seller_pubky` column"
        );
    }

    // Every app-scoped child must point to a directly scanned root table.
    for (child, parent) in APP_SCOPED_CHILD_TABLES {
        assert!(
            scanned.contains(parent),
            "APP_SCOPED_CHILD_TABLES entry `{child}` references non-scanned parent `{parent}`"
        );
    }

    // Compute transitive closure of strict pg_constraint FKs to
    // SCANNED_SELLER_TABLES and assert every FK_DESCENDANT_TABLES entry is
    // strictly anchored to a scanned root.
    let mut fk_parents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (child, parent) in strict_fk_edges(&pool).await {
        fk_parents.entry(child).or_default().insert(parent);
    }
    let mut anchored: BTreeSet<String> = scanned.iter().map(|t| (*t).to_string()).collect();
    loop {
        let mut added = false;
        for (child, parents) in &fk_parents {
            if !anchored.contains(child) && parents.iter().any(|p| anchored.contains(p)) {
                anchored.insert(child.clone());
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    for descendant in FK_DESCENDANT_TABLES {
        assert!(
            anchored.contains(*descendant),
            "FK_DESCENDANT_TABLES entry `{descendant}` lacks a strict NOT NULL, non-SET-NULL / \
             non-SET-DEFAULT foreign-key chain to SCANNED_SELLER_TABLES (parents: {:?})",
            fk_parents.get(*descendant)
        );
    }

    // Synthetic soundness proof for `strict_fk_edges`: a composite MATCH SIMPLE
    // FK with one nullable column, an ON DELETE SET NULL FK, and an ON DELETE
    // SET DEFAULT FK must all be rejected, while an all-NOT-NULL composite FK
    // must be accepted. Rolled back so the scratch schema stays clean.
    let mut tx = pool.begin().await.expect("synthetic fk tx");
    sqlx::raw_sql(
        "CREATE TABLE _fk_audit_parent ( \
             k1 INT NOT NULL, \
             k2 INT NOT NULL, \
             UNIQUE (k1, k2) \
         ); \
         CREATE TABLE _fk_audit_comp_nullable ( \
             k1 INT NOT NULL, \
             k2 INT NULL, \
             FOREIGN KEY (k1, k2) REFERENCES _fk_audit_parent (k1, k2) \
         ); \
         CREATE TABLE _fk_audit_set_null ( \
             k1 INT NOT NULL, \
             k2 INT NOT NULL, \
             FOREIGN KEY (k1, k2) REFERENCES _fk_audit_parent (k1, k2) ON DELETE SET NULL \
         ); \
         CREATE TABLE _fk_audit_set_default ( \
             k1 INT NOT NULL DEFAULT 0, \
             k2 INT NOT NULL DEFAULT 0, \
             FOREIGN KEY (k1, k2) REFERENCES _fk_audit_parent (k1, k2) ON DELETE SET DEFAULT \
         ); \
         CREATE TABLE _fk_audit_comp_strict ( \
             k1 INT NOT NULL, \
             k2 INT NOT NULL, \
             FOREIGN KEY (k1, k2) REFERENCES _fk_audit_parent (k1, k2) \
         );",
    )
    .execute(&mut *tx)
    .await
    .expect("create synthetic fk fixtures");

    let synthetic_edges: BTreeSet<(String, String)> =
        strict_fk_edges(&mut *tx).await.into_iter().collect();
    assert!(
        !synthetic_edges.contains(&(
            "_fk_audit_comp_nullable".to_string(),
            "_fk_audit_parent".to_string()
        )),
        "composite FK with a nullable column must be rejected"
    );
    assert!(
        !synthetic_edges.contains(&(
            "_fk_audit_set_null".to_string(),
            "_fk_audit_parent".to_string()
        )),
        "ON DELETE SET NULL FK must be rejected"
    );
    assert!(
        !synthetic_edges.contains(&(
            "_fk_audit_set_default".to_string(),
            "_fk_audit_parent".to_string()
        )),
        "ON DELETE SET DEFAULT FK must be rejected"
    );
    assert!(
        synthetic_edges.contains(&(
            "_fk_audit_comp_strict".to_string(),
            "_fk_audit_parent".to_string()
        )),
        "all-NOT-NULL composite FK must be accepted"
    );
    tx.rollback().await.expect("rollback synthetic fk fixtures");
}
