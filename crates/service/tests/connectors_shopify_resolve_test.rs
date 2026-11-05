//! A Shopify store's own domain resolved to its `*.myshopify.com` domain:
//! the provider lookup against loopback stand-ins for the storefront and for
//! Shopify's own host, and `POST /v1/connectors/resolve` over HTTP.
//!
//! The storefront and the store host answer `/meta.json` the way Shopify
//! documents it (`myshopify_domain`, `domain`); the stand-ins are scripted per
//! test and count their requests.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use common::connector_provider::{api, rebuild, test_keys, test_settings};
use common::{new_actor, TestApp};
use marketplace_service::connectors::provider::{
    ConnectorProvider, ProviderError, ProviderRegistry, ResolvedShop,
};
use marketplace_service::connectors::providers::shopify::client::Origin;
use marketplace_service::connectors::providers::shopify::resolve::MAX_CONCURRENT;
use marketplace_service::connectors::providers::shopify::ShopifyProvider;
use marketplace_service::connectors::ConnectorRuntime;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Notify;

const SHOP: &str = "example-goods.myshopify.com";

#[derive(Clone)]
enum Reply {
    Json(Value),
    Raw(Vec<u8>),
    Status(u16),
    Redirect(String),
    RedirectToItself,
    Hold(Arc<Notify>),
}

struct Stub {
    host_port: String,
    hits: Arc<AtomicUsize>,
}

impl Stub {
    async fn start(reply: Reply) -> Stub {
        let hits = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .fallback(answer)
            .with_state((reply, hits.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the stand-in");
        let host_port = listener.local_addr().expect("address").to_string();
        tokio::spawn(async move {
            axum::serve(listener, router).await.ok();
        });
        Stub { host_port, hits }
    }

    fn base(&self) -> String {
        format!("http://{}", self.host_port)
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

async fn answer(
    State((reply, hits)): State<(Reply, Arc<AtomicUsize>)>,
    headers: axum::http::HeaderMap,
) -> Response {
    hits.fetch_add(1, Ordering::SeqCst);
    match reply {
        Reply::Json(body) => (StatusCode::OK, axum::Json(body)).into_response(),
        Reply::Raw(body) => (StatusCode::OK, body).into_response(),
        Reply::Status(code) => StatusCode::from_u16(code).expect("status").into_response(),
        Reply::Redirect(to) => {
            (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, to)]).into_response()
        }
        Reply::RedirectToItself => {
            let host = headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .expect("host header");
            (
                StatusCode::FOUND,
                [(header::LOCATION, format!("http://{host}/again"))],
            )
                .into_response()
        }
        Reply::Hold(release) => {
            release.notified().await;
            StatusCode::OK.into_response()
        }
    }
}

fn meta(shop: &str, domain: &str) -> Reply {
    Reply::Json(json!({
        "id": 1,
        "name": "Example Goods",
        "domain": domain,
        "url": format!("https://{domain}"),
        "myshopify_domain": shop,
    }))
}

fn provider_for(store: &Stub) -> ShopifyProvider {
    ShopifyProvider::with_origin(Origin::Fixed(store.base())).with_local_resolver()
}

fn bare(stub: &Stub) -> &str {
    stub.host_port.split(':').next().expect("host")
}

fn not_shopify(error: ProviderError) {
    match error {
        ProviderError::Rejected { code, .. } => assert_eq!(code, "not_a_shopify_store"),
        other => panic!("expected not_a_shopify_store, got {other:?}"),
    }
}

#[tokio::test]
async fn the_canonical_shop_and_the_admin_address_need_no_lookup() {
    let store = Stub::start(Reply::Status(500)).await;
    let provider = provider_for(&store);
    for raw in [
        SHOP,
        "https://Example-Goods.myshopify.com/admin",
        "admin.shopify.com/store/example-goods",
    ] {
        assert_eq!(
            provider.resolve_shop(raw).await.expect(raw),
            ResolvedShop {
                shop: SHOP.into(),
                domain: None
            }
        );
    }
    assert_eq!(store.hits(), 0, "nothing was fetched");
}

#[tokio::test]
async fn a_custom_domain_resolves_to_the_store_that_names_it_as_its_own() {
    let storefront_domain = "127.0.0.1";
    let store = Stub::start(meta(SHOP, storefront_domain)).await;
    let storefront = Stub::start(meta(SHOP, storefront_domain)).await;
    let provider = provider_for(&store);

    for raw in [
        storefront.host_port.clone(),
        format!(
            "http://{}/collections/all?utm_source=x#top",
            storefront.host_port
        ),
    ] {
        let resolved = provider.resolve_shop(&raw).await.expect(&raw);
        assert_eq!(resolved.shop, SHOP);
        assert_eq!(resolved.domain.as_deref(), Some(storefront_domain));
    }
    assert_eq!(storefront.hits(), 2);
    assert_eq!(
        store.hits(),
        2,
        "the claim was asked on the store's own host"
    );
}

#[tokio::test]
async fn a_redirect_to_the_stores_main_domain_is_followed_and_checked_there() {
    let main = Stub::start(meta(SHOP, "localhost")).await;
    let store = Stub::start(meta(SHOP, "www.localhost")).await;
    let apex = Stub::start(Reply::Redirect(format!(
        "http://localhost:{}/anything",
        main.host_port.rsplit(':').next().expect("port")
    )))
    .await;
    let provider = provider_for(&store);

    let resolved = provider
        .resolve_shop(&apex.host_port)
        .await
        .expect("resolves");
    assert_eq!(resolved.shop, SHOP);
    assert_eq!(
        resolved.domain.as_deref(),
        Some("127.0.0.1"),
        "the address the merchant typed"
    );
    assert_eq!((apex.hits(), main.hits(), store.hits()), (1, 1, 1));
}

#[tokio::test]
async fn a_site_cannot_claim_a_store_whose_main_domain_is_elsewhere() {
    let store = Stub::start(meta(SHOP, "the-real-shop.example")).await;
    let impostor = Stub::start(meta(SHOP, "the-real-shop.example")).await;
    let error = provider_for(&store)
        .resolve_shop(&impostor.host_port)
        .await
        .expect_err("refused");
    match error {
        ProviderError::Rejected { code, message } => {
            assert_eq!(code, "not_primary_domain");
            assert!(message.contains("the-real-shop.example"), "{message}");
        }
        other => panic!("expected not_primary_domain, got {other:?}"),
    }
}

#[tokio::test]
async fn a_claim_the_store_does_not_confirm_is_refused() {
    let storefront = Stub::start(meta(SHOP, "127.0.0.1")).await;
    for store_reply in [
        meta("another-shop.myshopify.com", "127.0.0.1"),
        Reply::Status(404),
        Reply::Status(302),
        Reply::Raw(b"<html>not json</html>".to_vec()),
        Reply::Json(json!({"myshopify_domain": SHOP})),
        Reply::Json(json!([SHOP])),
    ] {
        let store = Stub::start(store_reply).await;
        let error = provider_for(&store)
            .resolve_shop(&storefront.host_port)
            .await
            .expect_err("refused");
        not_shopify(error);
    }
}

#[tokio::test]
async fn a_site_that_is_not_a_shopify_storefront_is_refused() {
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let provider = provider_for(&store);
    for reply in [
        Reply::Status(404),
        Reply::Status(500),
        Reply::Raw(b"<html>a blog</html>".to_vec()),
        Reply::Json(json!({"name": "no shop here"})),
        Reply::Json(json!({"myshopify_domain": "evil.example"})),
        Reply::Json(json!({"myshopify_domain": "evil.example/.myshopify.com"})),
        Reply::Json(json!({"myshopify_domain": "a.b.myshopify.com"})),
        Reply::Json(json!({"myshopify_domain": 7})),
        Reply::Json(json!(null)),
    ] {
        let site = Stub::start(reply).await;
        not_shopify(
            provider
                .resolve_shop(&site.host_port)
                .await
                .expect_err("refused"),
        );
    }
    assert_eq!(
        store.hits(),
        0,
        "an unconfirmed claim never reaches the store"
    );
}

#[tokio::test]
async fn redirects_are_bounded() {
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let provider = provider_for(&store);

    let looping = Stub::start(Reply::RedirectToItself).await;
    not_shopify(
        provider
            .resolve_shop(&looping.host_port)
            .await
            .expect_err("refused"),
    );
    assert_eq!(looping.hits(), 4, "the first request and three redirects");

    let no_target = Stub::start(Reply::Status(301)).await;
    not_shopify(
        provider
            .resolve_shop(&no_target.host_port)
            .await
            .expect_err("refused"),
    );
}

#[tokio::test]
async fn a_huge_answer_is_cut_off() {
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let site = Stub::start(Reply::Raw(vec![b' '; 512 * 1024])).await;
    let error = provider_for(&store)
        .resolve_shop(&site.host_port)
        .await
        .expect_err("refused");
    assert!(matches!(error, ProviderError::Unknown(_)), "{error:?}");
}

#[tokio::test]
async fn a_closed_port_is_unreachable_not_a_verdict() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve a port")
        .local_addr()
        .expect("address")
        .port();
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let error = provider_for(&store)
        .resolve_shop(&format!("127.0.0.1:{port}"))
        .await
        .expect_err("refused");
    assert!(matches!(error, ProviderError::Unknown(_)), "{error:?}");
}

#[tokio::test]
async fn production_rules_refuse_addresses_inside_the_network() {
    let provider = ShopifyProvider::new();
    for raw in [
        "127.0.0.1",
        "http://127.0.0.1:8080/meta.json",
        "169.254.169.254",
        "10.1.2.3",
        "[::1]",
        "localhost",
        "intranet",
        "shop.example:8443",
    ] {
        assert!(
            matches!(
                provider.resolve_shop(raw).await,
                Err(ProviderError::Invalid(_))
            ),
            "{raw:?}"
        );
    }
}

#[tokio::test]
async fn lookups_are_capped_and_a_full_house_is_told_to_wait() {
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let release = Arc::new(Notify::new());
    let hanging = Stub::start(Reply::Hold(release.clone())).await;
    let provider = Arc::new(provider_for(&store));

    let mut running = Vec::new();
    for _ in 0..MAX_CONCURRENT {
        let provider = provider.clone();
        let target = hanging.host_port.clone();
        running.push(tokio::spawn(
            async move { provider.resolve_shop(&target).await },
        ));
    }
    for _ in 0..200 {
        if hanging.hits() == MAX_CONCURRENT {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(hanging.hits(), MAX_CONCURRENT, "every slot is taken");

    let overflow = provider.resolve_shop(&hanging.host_port).await;
    assert!(
        matches!(overflow, Err(ProviderError::RateLimited { .. })),
        "{overflow:?}"
    );

    release.notify_waiters();
    for lookup in running {
        not_shopify(
            lookup
                .await
                .expect("task")
                .expect_err("held answers are empty"),
        );
    }
    let after = provider.resolve_shop(&hanging.host_port).await;
    assert!(
        !matches!(after, Err(ProviderError::RateLimited { .. })),
        "slots were given back"
    );
}

struct Rig {
    app: TestApp,
    store: Stub,
}

async fn rig(pool: PgPool) -> Rig {
    let store = Stub::start(meta(SHOP, "127.0.0.1")).await;
    let runtime = ConnectorRuntime::new(
        test_keys(),
        ProviderRegistry::new().with(Arc::new(provider_for(&store))),
        test_settings(),
    );
    let base = common::test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(Arc::new(runtime)));
    Rig {
        app: rebuild(base, state),
        store,
    }
}

const RESOLVE: &str = "/v1/connectors/resolve";

fn resolve_body(provider: &str, domain: &str) -> Value {
    json!({"provider": provider, "domain": domain})
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_route_answers_the_shop_for_a_custom_domain_and_for_the_canonical_one(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let storefront = Stub::start(meta(SHOP, "127.0.0.1")).await;

    let (status, body) = api(
        &r.app,
        "POST",
        RESOLVE,
        Some(&seller.token),
        &resolve_body("shopify", &storefront.host_port),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shop"], SHOP);
    assert_eq!(body["domain"], bare(&storefront));
    assert_eq!(body["provider"], "shopify");

    let (status, body) = api(
        &r.app,
        "POST",
        RESOLVE,
        Some(&seller.token),
        &resolve_body("shopify", "https://Example-Goods.myshopify.com"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shop"], SHOP);
    assert_eq!(body["domain"], Value::Null);

    // The resolved shop is what the connect request takes.
    let (status, body) = api(
        &r.app,
        "POST",
        "/v1/connectors",
        Some(&seller.token),
        &json!({"provider": "shopify", "domain": SHOP}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["connection"]["domain"], SHOP);
    assert!(r.store.hits() >= 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_route_refuses_clearly_and_needs_a_session(pool: PgPool) {
    let r = rig(pool).await;
    let seller = new_actor(&r.app).await;
    let blog = Stub::start(Reply::Status(404)).await;

    let (status, body) = api(
        &r.app,
        "POST",
        RESOLVE,
        None,
        &resolve_body("shopify", "shop.example"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    let cases = [
        (
            resolve_body("shopify", &blog.host_port),
            "not_a_shopify_store",
        ),
        (
            resolve_body("shopify", "user:pass@shop.example"),
            "invalid_domain",
        ),
        (resolve_body("shopify", ""), "invalid_domain"),
        (resolve_body("shopify", &"a".repeat(300)), "invalid_domain"),
        (resolve_body("magento", "shop.example"), "invalid_provider"),
    ];
    for (request, code) in cases {
        let (status, body) = api(&r.app, "POST", RESOLVE, Some(&seller.token), &request).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{request}: {body}"
        );
        assert_eq!(body["error"]["code"], code, "{request}: {body}");
    }

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve a port")
        .local_addr()
        .expect("address")
        .port();
    let (status, body) = api(
        &r.app,
        "POST",
        RESOLVE,
        Some(&seller.token),
        &resolve_body("shopify", &format!("127.0.0.1:{port}")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert_eq!(body["error"]["code"], "provider_unavailable");

    let (status, body) = api(
        &r.app,
        "POST",
        RESOLVE,
        Some(&seller.token),
        &json!({"provider": "shopify"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "invalid_domain");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_route_is_absent_where_connectors_are_off(pool: PgPool) {
    let app = common::test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = api(
        &app,
        "POST",
        RESOLVE,
        Some(&seller.token),
        &resolve_body("shopify", "shop.example"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "connectors_disabled");
}
