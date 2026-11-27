//! Single-seller first-boot claim, unclaimed fail-closed behaviour, boot
//! binding transitions and the empty-store release helper.

mod common;

use std::time::Duration;

use axum::http::StatusCode;
use common::{
    execute, new_actor, register_command, send, test_app_with_config, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::store_claim::ClaimCode;
use marketplace_service::store_seller::{
    resolve_store, Active, RouteScope, StoreBinding, StoreMode, ROUTES,
};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unclaimed_routes_fail_closed_and_claim_needs_no_restart(pool: PgPool) {
    let app = unclaimed(pool.clone()).await;
    let actor = new_actor(&app).await;
    for rule in ROUTES
        .iter()
        .filter(|rule| !matches!(rule.scope, RouteScope::Public(_)))
    {
        let path = rule
            .path
            .replace("{pubky}", &actor.pubky)
            .replace("{seller_pubky}", &actor.pubky)
            .replace("{seller}", &actor.pubky)
            .replace(
                "{aggregate_id}",
                &format!("listing:{}_boots_01", actor.pubky),
            )
            .replace("{listing_id}", "boots_01")
            .replace("{line_index}", "0")
            .replace("{connection_id}", &Uuid::new_v4().to_string())
            .replace("{change_id}", &Uuid::new_v4().to_string())
            .replace("{flow_id}", &Uuid::new_v4().to_string())
            .replace("{id}", &Uuid::new_v4().to_string());
        let session_admin = rule.path.starts_with("/v1/auth/sessions/");
        let disposable = if session_admin {
            Some(new_actor(&app).await)
        } else {
            None
        };
        let caller = disposable.as_ref().unwrap_or(&actor);
        let (status, body) = send(
            app.router.clone(),
            rule.method,
            &path,
            Some(&caller.token),
            &json!({}),
        )
        .await;
        if rule.path == "/v1/store/claim" || session_admin {
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
    let (status, _) = claim(&app, &actor, WRONG_CODE).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

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
            pinned: Some(mistyped),
        },
        None,
    )
    .await
    .expect_err("mismatched pin");
    assert!(error.to_string().contains("does not match"), "{error}");
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
    let error = marketplace_service::store_binding_admin::release(&admin, "xxxxxxxx")
        .await
        .expect_err("wrong confirmation");
    assert!(error.to_string().contains("does not match"), "{error}");
    let released = marketplace_service::store_binding_admin::release(&admin, &seller[..8])
        .await
        .expect("empty store releases");
    assert_eq!(released, seller);
    let unclaimed = resolve_store(
        &admin,
        &StoreMode::SingleSeller { pinned: None },
        Some(ClaimCode::parse(CODE).unwrap()),
    )
    .await
    .expect("fresh boot is unclaimed");
    assert_eq!(unclaimed.active(), Active::Unclaimed);
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
