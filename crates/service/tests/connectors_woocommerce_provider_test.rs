//! The WooCommerce provider against WooCommerce, with no database: what
//! connecting accepts and refuses, how the key pair is sent, the catalog, and
//! the order operations (create, find, cancel, record a refund) with their
//! idempotency. The store side is the replay of exchanges captured from a real
//! WooCommerce (`tests/fixtures/woocommerce/README.md`); with `WOO_LIVE=1` the
//! same tests run against the Docker store and re-record them.

mod common;
mod woocommerce_support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use marketplace_service::connectors::provider::*;
use marketplace_service::connectors::providers::woocommerce::creds::{AuthMode, StoredCredentials};
use marketplace_service::connectors::providers::woocommerce::{WooCommerceProvider, WooOptions};
use serde_json::{json, Value};
use uuid::Uuid;
use woocommerce_support::flows::*;
use woocommerce_support::live::RestoreStore;
use woocommerce_support::replay::Delivery;
use woocommerce_support::{connect_context, webhook_url, StoreSide, CALLBACK_URL, RETURN_URL};

fn request(side_site: &str, provider: &WooCommerceProvider) -> ConnectRequest {
    let connection_id = Uuid::new_v4();
    ConnectRequest {
        connection_id,
        seller_pubky: "seller".into(),
        shop: provider.normalize_shop(side_site).expect("shop"),
        state: format!("nonce-{}", Uuid::new_v4().simple()),
        app: None,
        callback_url: CALLBACK_URL.into(),
        webhook_url: webhook_url(connection_id),
        params: json!({}),
    }
}

fn invalid(error: ProviderError) -> String {
    match error {
        ProviderError::Invalid(message) => message,
        other => panic!("expected a refusal with a reason, got {other:?}"),
    }
}

fn webhook_posts(side: &StoreSide) -> Vec<Value> {
    side.replay
        .as_ref()
        .expect("replay")
        .requests("POST", "/wp-json/wc/v3/webhooks")
        .into_iter()
        .map(|seen| seen.body.expect("webhook body"))
        .collect()
}

/// Our webhooks on a live store: `(id, delivery_url)`.
async fn live_webhooks(side: &StoreSide) -> Vec<(i64, String)> {
    side.live()
        .rest("GET", "/wc/v3/webhooks?per_page=100", None)
        .await
        .as_array()
        .expect("hooks")
        .iter()
        .map(|hook| {
            (
                hook["id"].as_i64().expect("id"),
                hook["delivery_url"].as_str().expect("url").to_string(),
            )
        })
        .collect()
}

fn at_url(hooks: &[(i64, String)], url: &str) -> Vec<i64> {
    hooks
        .iter()
        .filter(|(_, hook)| hook == url)
        .map(|(id, _)| *id)
        .collect()
}

fn webhook_deletes(side: &StoreSide) -> usize {
    side.replay
        .as_ref()
        .expect("replay")
        .matched()
        .iter()
        .filter(|seen| seen.method == "DELETE" && seen.path.starts_with("/wp-json/wc/v3/webhooks/"))
        .count()
}

#[tokio::test]
async fn connecting_changes_nothing_on_the_store_and_registering_adds_our_webhooks() {
    let side = StoreSide::open("p_connect").await;
    let provider = side.provider(7);
    let request = request(&side.site(), &provider);
    let started = provider.connect(&request).await.expect("connect");
    let body = side.approve(&started.authorize_url.expect("url"), "callback");
    let completed = provider
        .complete_connect(&CompleteRequest {
            pending: PendingConnection {
                connection_id: request.connection_id,
                seller_pubky: "seller".into(),
                shop: request.shop.clone(),
                state: request.state.clone(),
            },
            app: None,
            callback: CallbackParams {
                body,
                ..CallbackParams::default()
            },
            webhook_url: request.webhook_url.clone(),
        })
        .await
        .expect("complete connect");
    assert_eq!(completed.scopes, vec!["read_write"]);
    // Verifying the keys (reads, and a write check that saves nothing) created no
    // webhook: that waits until the credential is stored.
    if let Some(replay) = &side.replay {
        let created: Vec<Value> = webhook_posts(&side)
            .into_iter()
            .filter(|post| post["topic"] != "pubky.access_check")
            .collect();
        assert!(
            created.is_empty(),
            "no webhook before the credential is stored"
        );
        assert_eq!(webhook_deletes(&side), 0);
        let _ = replay;
    } else {
        assert!(at_url(&live_webhooks(&side).await, &request.webhook_url).is_empty());
    }
    let context = ConnectionContext {
        connection_id: request.connection_id,
        seller_pubky: "seller".into(),
        shop: request.shop.clone(),
        scopes: completed.scopes,
        credentials: completed.credentials,
        app: None,
    };
    let stored = StoredCredentials::open(&context.credentials).expect("credentials");
    assert_eq!(stored.auth, AuthMode::Basic);
    assert_eq!(stored.webhook_secret.len(), 64);

    // The framework's post-commit hook registers them at this connection's URL.
    provider
        .register_webhooks(&context, &request.webhook_url)
        .await
        .expect("register");
    let own_url = webhook_url(request.connection_id);
    if let Some(replay) = &side.replay {
        let created: Vec<Value> = webhook_posts(&side)
            .into_iter()
            .filter(|post| post["topic"] != "pubky.access_check")
            .collect();
        assert_eq!(created.len(), 3);
        let topics: Vec<&str> = created
            .iter()
            .map(|p| p["topic"].as_str().expect("topic"))
            .collect();
        assert_eq!(
            topics,
            ["product.updated", "order.updated", "order.deleted"]
        );
        for post in &created {
            assert_eq!(post["delivery_url"], own_url);
            assert_eq!(post["status"], "active");
            assert_eq!(post["secret"].as_str().expect("secret").len(), 64);
            assert!(post["name"]
                .as_str()
                .expect("name")
                .starts_with("Pubky Shop"));
        }
        assert!(replay
            .matched()
            .iter()
            .all(|seen| seen.auth == "basic" || seen.auth == "none"));
    } else {
        assert_eq!(at_url(&live_webhooks(&side).await, &own_url).len(), 3);
    }

    // Registering again (replaced keys) swaps them: three new, the earlier three gone.
    provider
        .register_webhooks(&context, &request.webhook_url)
        .await
        .expect("register again");
    if side.is_live() {
        assert_eq!(at_url(&live_webhooks(&side).await, &own_url).len(), 3);
    } else {
        assert_eq!(webhook_deletes(&side), 3, "the earlier three are removed");
    }

    // Disconnecting takes this connection's webhooks off the store.
    provider.disconnect(&context).await.expect("disconnect");
    if side.is_live() {
        assert!(at_url(&live_webhooks(&side).await, &own_url).is_empty());
    }
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn one_connections_webhooks_are_never_touched_by_another_connection() {
    let side = StoreSide::open("p_webhook_isolation").await;
    let provider = side.provider(7);
    let (first, _) = connect_context(&side, &provider, "callback").await;
    let (second, _) = connect_context(&side, &provider, "callback2").await;
    let first_url = webhook_url(first.connection_id);
    let second_url = webhook_url(second.connection_id);
    let ours_at = |hooks: &[(i64, String)], url: &str| {
        hooks
            .iter()
            .filter(|(_, hook)| hook == url)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
    };
    let mut before = Vec::new();
    if side.is_live() {
        let hooks = live_webhooks(&side).await;
        assert_eq!(ours_at(&hooks, &first_url).len(), 3);
        assert_eq!(ours_at(&hooks, &second_url).len(), 3);
        before = ours_at(&hooks, &second_url);
    }
    let deletes_at_start = if side.is_live() {
        0
    } else {
        webhook_deletes(&side)
    };
    assert_eq!(deletes_at_start, 0, "connecting twice deletes nothing");

    // A registration that fails (a read-only key cannot create webhooks) removes
    // nothing of either connection.
    let read_only_body = {
        let started = provider
            .connect(&request(&side.site(), &provider))
            .await
            .expect("connect");
        let mut authorize = url::Url::parse(&started.authorize_url.expect("url")).expect("url");
        let kept: Vec<(String, String)> = authorize
            .query_pairs()
            .map(|(k, v)| {
                let value = if k == "scope" {
                    "read".to_string()
                } else {
                    v.into_owned()
                };
                (k.into_owned(), value)
            })
            .collect();
        authorize.query_pairs_mut().clear().extend_pairs(kept);
        side.approve(authorize.as_str(), "callback_read")
    };
    let keys: Value = serde_json::from_slice(&read_only_body).expect("keys");
    let read_only = ConnectionContext {
        connection_id: Uuid::new_v4(),
        seller_pubky: "seller".into(),
        shop: first.shop.clone(),
        scopes: vec![],
        credentials: StoredCredentials::new(
            keys["consumer_key"].as_str().expect("key").to_string(),
            keys["consumer_secret"]
                .as_str()
                .expect("secret")
                .to_string(),
            AuthMode::Basic,
            "a".repeat(64),
        )
        .into_secret(),
        app: None,
    };
    let refused = provider
        .register_webhooks(&read_only, &webhook_url(read_only.connection_id))
        .await;
    assert!(
        matches!(refused, Err(ProviderError::Rejected { ref code, .. }) if code == "keys_read_only"),
        "{refused:?}"
    );
    let read_only_disconnect = provider.disconnect(&read_only).await;
    let _ = read_only_disconnect;
    if side.is_live() {
        let hooks = live_webhooks(&side).await;
        assert_eq!(ours_at(&hooks, &first_url).len(), 3);
        assert_eq!(ours_at(&hooks, &second_url), before);
    } else {
        assert_eq!(
            webhook_deletes(&side),
            0,
            "a failed registration deleted nothing"
        );
    }

    // Replacing the first connection's webhooks leaves the second's as they were.
    provider
        .register_webhooks(&first, &first_url)
        .await
        .expect("replace the first's");
    if side.is_live() {
        let hooks = live_webhooks(&side).await;
        assert_eq!(ours_at(&hooks, &first_url).len(), 3);
        assert_eq!(ours_at(&hooks, &second_url), before);
    } else {
        assert_eq!(
            webhook_deletes(&side),
            3,
            "only the first's earlier three go"
        );
    }

    // Disconnecting the first leaves the second's.
    provider.disconnect(&first).await.expect("disconnect first");
    if side.is_live() {
        let hooks = live_webhooks(&side).await;
        assert!(ours_at(&hooks, &first_url).is_empty());
        assert_eq!(ours_at(&hooks, &second_url), before);
    } else {
        assert_eq!(webhook_deletes(&side), 6);
    }
    provider
        .disconnect(&second)
        .await
        .expect("disconnect second");
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn webhook_lists_are_read_in_full_across_pages() {
    let side = StoreSide::open("p_webhook_pages").await;
    let provider = WooCommerceProvider::with_options(
        if side.is_live() {
            WooOptions::local_testing(false, vec![woocommerce_support::live::ca_certificate()])
        } else {
            WooOptions::local_testing(true, Vec::new())
        }
        .with_list_page_size(2),
        Some(RETURN_URL.to_string()),
    );
    let (context, _) = connect_context(&side, &provider, "callback").await;
    // Three webhooks over pages of two: registering again must find all three to
    // replace, and disconnecting must remove all three.
    provider
        .register_webhooks(&context, &webhook_url(context.connection_id))
        .await
        .expect("register again");
    let own_url = webhook_url(context.connection_id);
    if side.is_live() {
        assert_eq!(at_url(&live_webhooks(&side).await, &own_url).len(), 3);
    } else {
        assert_eq!(webhook_deletes(&side), 3);
    }
    provider.disconnect(&context).await.expect("disconnect");
    if side.is_live() {
        assert!(at_url(&live_webhooks(&side).await, &own_url).is_empty());
    } else {
        assert_eq!(webhook_deletes(&side), 6);
        let listed = side
            .replay
            .as_ref()
            .expect("replay")
            .requests("GET", "/wp-json/wc/v3/webhooks");
        assert!(
            listed.iter().any(|seen| seen
                .query
                .iter()
                .any(|(name, value)| name == "page" && value == "2")),
            "the second page was read"
        );
        side.replay.as_ref().expect("replay").assert_no_misses();
    }
}

#[tokio::test]
async fn a_host_that_strips_the_authorization_header_is_served_with_the_key_in_the_query() {
    let side = StoreSide::open("p_stripped_header").await;
    if side.is_live() {
        side.live().strip_authorization(true);
    }
    let provider = side.provider(7);
    let (context, _) = connect_context(&side, &provider, "callback").await;
    if side.is_live() {
        side.live().strip_authorization(false);
    }
    let stored = StoredCredentials::open(&context.credentials).expect("credentials");
    assert_eq!(
        stored.auth,
        AuthMode::Query,
        "Basic was refused, the query string worked"
    );

    let page = provider
        .list_catalog(&context, None)
        .await
        .expect("catalog over query auth");
    assert!(!page.products.is_empty());
    if let Some(replay) = &side.replay {
        let reads = replay.requests("GET", "/wp-json/wc/v3/products");
        assert!(!reads.is_empty() && reads.iter().all(|seen| seen.auth == "query"));
        let probes: Vec<String> = replay
            .matched()
            .iter()
            .filter(|seen| seen.path.ends_with("woocommerce_currency"))
            .map(|seen| seen.auth.clone())
            .collect();
        assert_eq!(
            &probes[..2],
            ["basic", "query"],
            "Basic is tried first, then the query string"
        );
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn keys_the_store_does_not_accept_are_rejected_after_both_ways_fail() {
    let side = StoreSide::open("p_bad_keys").await;
    let provider = side.provider(7);
    let started = provider
        .connect(&request(&side.site(), &provider))
        .await
        .expect("connect");
    assert!(started.authorize_url.is_some());
    let completed = provider
        .complete_connect(&CompleteRequest {
            pending: PendingConnection {
                connection_id: Uuid::new_v4(),
                seller_pubky: "seller".into(),
                shop: provider.normalize_shop(&side.site()).expect("shop"),
                state: "n".into(),
            },
            app: None,
            callback: CallbackParams {
                body: serde_json::to_vec(&json!({
                    "user_id": "n",
                    "consumer_key": format!("ck_{}", "e".repeat(40)),
                    "consumer_secret": format!("cs_{}", "f".repeat(40)),
                    "key_permissions": "read_write",
                }))
                .expect("json"),
                ..CallbackParams::default()
            },
            webhook_url: webhook_url(Uuid::nil()),
        })
        .await;
    assert!(
        matches!(completed, Err(ProviderError::Rejected { ref code, .. }) if code == "credentials_rejected")
    );
    if let Some(replay) = &side.replay {
        let tries: Vec<String> = replay
            .matched()
            .iter()
            .filter(|seen| seen.path.ends_with("woocommerce_currency"))
            .map(|seen| seen.auth.clone())
            .collect();
        assert_eq!(tries, ["basic", "query"]);
    }
}

#[tokio::test]
async fn a_read_only_key_cannot_connect() {
    let side = StoreSide::open("p_read_only").await;
    let provider = side.provider(7);
    let connect = request(&side.site(), &provider);
    let started = provider.connect(&connect).await.expect("connect");
    let mut authorize = url::Url::parse(&started.authorize_url.expect("url")).expect("url");
    let kept: Vec<(String, String)> = authorize
        .query_pairs()
        .map(|(k, v)| {
            let value = if k == "scope" {
                "read".to_string()
            } else {
                v.into_owned()
            };
            (k.into_owned(), value)
        })
        .collect();
    authorize.query_pairs_mut().clear().extend_pairs(kept);
    let body = side.approve(authorize.as_str(), "callback");
    let pending = |state: &str| PendingConnection {
        connection_id: connect.connection_id,
        seller_pubky: "seller".into(),
        shop: connect.shop.clone(),
        state: state.into(),
    };

    // WooCommerce says what it granted; a read key is refused before anything is sent.
    let refused = provider
        .complete_connect(&CompleteRequest {
            pending: pending("n"),
            app: None,
            callback: CallbackParams {
                body: body.clone(),
                ..CallbackParams::default()
            },
            webhook_url: webhook_url(Uuid::nil()),
        })
        .await;
    assert!(
        matches!(refused, Err(ProviderError::Rejected { ref code, .. }) if code == "keys_not_read_write")
    );

    // A pasted key carries no permission claim; the store refuses to create webhooks with it.
    let mut pasted: Value = serde_json::from_slice(&body).expect("json");
    pasted
        .as_object_mut()
        .expect("object")
        .remove("key_permissions");
    let refused = provider
        .complete_connect(&CompleteRequest {
            pending: pending("n"),
            app: None,
            callback: CallbackParams {
                body: serde_json::to_vec(&pasted).expect("json"),
                ..CallbackParams::default()
            },
            webhook_url: webhook_url(Uuid::nil()),
        })
        .await;
    assert!(
        matches!(refused, Err(ProviderError::Rejected { ref code, .. }) if code == "keys_read_only"),
        "{refused:?}"
    );
}

#[tokio::test]
async fn a_store_that_cannot_connect_is_refused_with_a_reason() {
    // Plain HTTP and addresses that are not public names never reach the network.
    let production =
        WooCommerceProvider::with_options(WooOptions::default(), Some(RETURN_URL.into()));
    let message = invalid(
        production
            .normalize_shop("http://dezentralshop.ch")
            .unwrap_err(),
    );
    assert!(message.contains("HTTPS"), "{message}");
    for refused in ["127.0.0.1", "localhost", "10.0.0.5", "intranet"] {
        assert!(production.normalize_shop(refused).is_err(), "{refused}");
    }
    assert_eq!(
        production
            .normalize_shop("https://DezentralShop.ch/")
            .expect("shop"),
        "dezentralshop.ch"
    );

    // A closed port is "could not be reached", not a refusal of the address.
    let local = WooCommerceProvider::with_options(
        WooOptions::local_testing(true, Vec::new()),
        Some(RETURN_URL.into()),
    );
    let unreachable = local.connect(&request("127.0.0.1:9", &local)).await;
    assert!(
        matches!(unreachable, Err(ProviderError::Unknown(_))),
        "{unreachable:?}"
    );

    // The store itself: plain permalinks, no WooCommerce.
    let side = StoreSide::open("p_refused").await;
    let provider = side.provider(7);
    let _restore = side.live.as_ref().map(RestoreStore);
    if side.is_live() {
        side.live().plain_permalinks(true);
    }
    let plain = provider.connect(&request(&side.site(), &provider)).await;
    if side.is_live() {
        side.live().plain_permalinks(false);
        side.live().woocommerce_active(false);
    }
    let message = invalid(plain.unwrap_err());
    assert!(
        message.contains("Permalinks") && message.contains("Post name"),
        "{message}"
    );

    let without = provider.connect(&request(&side.site(), &provider)).await;
    if side.is_live() {
        side.live().woocommerce_active(true);
    }
    let message = invalid(without.unwrap_err());
    assert!(message.contains("WooCommerce was not found"), "{message}");
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn a_store_reporting_another_address_is_refused_naming_the_right_one() {
    let side = StoreSide::open_with("p_home_mismatch", false).await;
    let provider = side.provider(7);
    let site = if side.is_live() {
        "127.0.0.1:8443".to_string()
    } else {
        side.site()
    };
    let message = invalid(
        provider
            .connect(&request(&site, &provider))
            .await
            .unwrap_err(),
    );
    assert!(message.contains("localhost:8443"), "{message}");
}

#[tokio::test]
async fn the_catalog_is_read_in_pages_with_variations_stock_images_and_chf_prices() {
    let side = StoreSide::open("p_catalog").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = side.provider(7);
    let (context, _) = connect_context(&side, &provider, "callback").await;

    let mut pages = 0;
    let mut products: Vec<CatalogProduct> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = provider
            .list_catalog(&context, token.as_deref())
            .await
            .expect("page");
        pages += 1;
        products.extend(page.products);
        match page.next_page {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert!(pages >= 3, "twenty products at seven a page: {pages} pages");
    let titles: Vec<&str> = products.iter().map(|p| p.title.as_str()).collect();
    for sold in [
        "Pubky Test Item",
        "Little Hodler Socks",
        "V-Neck T-Shirt",
        "Hoodie",
        "Beanie",
    ] {
        assert!(titles.contains(&sold), "{sold} in {titles:?}");
    }
    for skipped in ["Logo Collection", "WordPress Pennant"] {
        assert!(
            !titles.contains(&skipped),
            "{skipped} is grouped or external"
        );
    }
    assert!(products.iter().all(|p| p
        .variants
        .iter()
        .all(|v| v.price.currency == "CHF" && v.price.exponent == 2)));

    let socks = products
        .iter()
        .find(|p| p.title == "Little Hodler Socks")
        .expect("socks");
    assert_eq!(socks.options.len(), 1);
    assert_eq!(socks.options[0].values, ["S", "M", "L"]);
    let variants: Vec<(String, i64, Option<i64>)> = socks
        .variants
        .iter()
        .map(|v| {
            (
                v.sku.clone().unwrap_or_default(),
                v.price.amount_minor,
                v.stock,
            )
        })
        .collect();
    assert_eq!(
        variants,
        [
            ("PUBKY-SOCKS-S".to_string(), 1990, Some(5)),
            ("PUBKY-SOCKS-M".to_string(), 1990, Some(3)),
            ("PUBKY-SOCKS-L".to_string(), 2150, Some(0)),
        ]
    );
    assert_eq!(socks.variants[0].option_values[0].value, "S");
    assert!(socks.image_urls.len() >= 2, "product image and gallery");
    assert_eq!(socks.categories, ["Socks"]);
    assert_eq!(
        socks.attributes.len(),
        1,
        "Material does not pick a variant"
    );
    assert_eq!(socks.attributes[0].name, "Material");
    assert_eq!(socks.attributes[0].values, ["Merino wool", "Cotton"]);
    assert_eq!(socks.options.len(), 1, "only Size picks a variant");

    let item = products
        .iter()
        .find(|p| p.title == "Pubky Test Item")
        .expect("item");
    assert_eq!(item.categories, ["Accessories"]);
    assert_eq!(item.attributes[0].name, "Material");
    assert_eq!(item.attributes[0].values, ["Cotton"]);
    assert_eq!(item.variants.len(), 1);
    assert_eq!(
        item.variants[0].provider_variant_id,
        item.provider_product_id
    );
    assert_eq!(
        item.variants[0].stock,
        Some(10),
        "exact when WooCommerce manages stock"
    );
    let beanie = products
        .iter()
        .find(|p| p.title == "Beanie")
        .expect("beanie");
    assert_eq!(
        beanie.variants[0].stock, None,
        "not tracked when the store does not manage stock"
    );
    let hoodie = products
        .iter()
        .find(|p| p.title == "Hoodie")
        .expect("hoodie");
    assert!(hoodie.variants.len() >= 2, "variable sample product");
    let tee = products
        .iter()
        .find(|p| p.title == "V-Neck T-Shirt")
        .expect("tee");
    assert!(!tee.options.is_empty());
    assert_eq!(tee.categories, ["Tshirts"]);
    assert!(tee.attributes.is_empty(), "Color and Size pick variants");

    assert!(matches!(
        provider.list_catalog(&context, Some("nonsense")).await,
        Err(ProviderError::Invalid(_))
    ));
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
    }
}

fn order(id: &str, address: bool) -> OrderPush {
    OrderPush {
        pubky_order_id: Uuid::parse_str(id).expect("uuid"),
        currency: "CHF".into(),
        exponent: 2,
        lines: vec![
            OrderPushLine {
                provider_product_id: TEST_PRODUCT.to_string(),
                provider_variant_id: TEST_PRODUCT.to_string(),
                sku: Some("PUBKY-TEST".into()),
                title: "Pubky Test Item".into(),
                quantity: 2,
                unit_price_minor: 1250,
            },
            OrderPushLine {
                provider_product_id: SOCKS.to_string(),
                provider_variant_id: SOCKS_S.to_string(),
                sku: Some("PUBKY-SOCKS-S".into()),
                title: "Little Hodler Socks".into(),
                quantity: 1,
                unit_price_minor: 1990,
            },
        ],
        subtotal_minor: 4490,
        shipping_minor: 600,
        total_minor: 5090,
        payment_method: "bitcoin".into(),
        paid_at: "2026-10-15T12:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("time"),
        shipping_address: address.then(|| ShippingAddress {
            name: "Anna Muster".into(),
            line1: "Bahnhofstrasse 1".into(),
            line2: "c/o Pubky".into(),
            city: "Z\u{fc}rich".into(),
            region: "ZH".into(),
            postal_code: "8001".into(),
            country_code: "CH".into(),
        }),
    }
}

fn refund_record(id: &str, order: &OrderPush, recorded_total: i64) -> RefundRecord {
    RefundRecord {
        refund_id: id.into(),
        pubky_order_id: order.pubky_order_id,
        amount_minor: recorded_total,
        recorded_total_minor: recorded_total,
        transaction_id: None,
        order_total_minor: order.total_minor,
        order_currency: order.currency.clone(),
        order_exponent: order.exponent,
        full: recorded_total >= order.total_minor,
        basis: None,
    }
}

/// What the worker does for a refund: ask the store what it shows, take the
/// framework's difference, and record only that. `false` when there was
/// nothing left to mirror.
async fn mirror(
    provider: &WooCommerceProvider,
    context: &ConnectionContext,
    store_order: &ExternalOrder,
    order: &OrderPush,
    id: &str,
    recorded_total: i64,
) -> Result<bool, ProviderError> {
    let shown = provider.refunded_total(context, store_order).await?;
    let Some(amount) = marketplace_service::connectors::refund::amount_to_mirror(
        recorded_total,
        &order.currency,
        &shown,
    )?
    else {
        return Ok(false);
    };
    let record = RefundRecord {
        amount_minor: amount,
        ..refund_record(id, order, recorded_total)
    };
    provider
        .record_refund(context, store_order, &record)
        .await?;
    Ok(true)
}

const ORDER_ONE: &str = "11111111-2222-8333-9444-555555555551";
const ORDER_TWO: &str = "11111111-2222-8333-9444-555555555552";
const ORDER_THREE: &str = "11111111-2222-8333-9444-555555555553";
const ORDER_FOUR: &str = "11111111-2222-8333-9444-555555555554";
const ORDER_FIVE: &str = "11111111-2222-8333-9444-555555555555";
const ORDER_SIX: &str = "11111111-2222-8333-9444-555555555556";
const ORDER_SEVEN: &str = "11111111-2222-8333-9444-555555555557";
const ORDER_UNKNOWN: &str = "11111111-2222-8333-9444-5555555555ff";

#[tokio::test]
async fn orders_are_pushed_found_cancelled_and_refunded_without_ever_doing_a_thing_twice() {
    let side = StoreSide::open("p_orders").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = Arc::new(side.provider(7));
    let (context, _) = connect_context(&side, &provider, "callback").await;

    // A paid order with the buyer's opted-in address.
    let one = order(ORDER_ONE, true);
    let created = provider.push_order(&context, &one).await.expect("push");
    if side.is_live() {
        let live = side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{}", created.id), None)
            .await;
        assert_eq!(live["status"], "processing");
        assert!(live["date_paid"].is_string());
        assert_eq!(
            live["total"], "50.90",
            "the store has Swiss VAT on, and still the total is what was paid"
        );
        assert_eq!(live["total_tax"], "0.00");
        assert_eq!(live["shipping_tax"], "0.00");
        assert_eq!(live["shipping"]["first_name"], "Anna");
        assert_eq!(live["shipping"]["last_name"], "Muster");
        assert_eq!(live["shipping"]["address_1"], "Bahnhofstrasse 1");
        assert_eq!(live["shipping"]["postcode"], "8001");
        assert_eq!(live["shipping"]["country"], "CH");
        assert_eq!(live["billing"]["email"], "");
        assert_eq!(live["billing"]["address_1"], "");
        assert_eq!(live["line_items"][1]["variation_id"], SOCKS_S);
        assert!(live["customer_note"]
            .as_str()
            .expect("note")
            .contains(ORDER_ONE));
        let item = side
            .live()
            .rest("GET", &format!("/wc/v3/products/{TEST_PRODUCT}"), None)
            .await;
        assert_eq!(item["stock_quantity"], 8, "two units taken");
        let variation = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/products/{SOCKS}/variations/{SOCKS_S}"),
                None,
            )
            .await;
        assert_eq!(variation["stock_quantity"], 4);
    }
    if let Some(replay) = &side.replay {
        let posted = replay.requests("POST", "/wp-json/wc/v3/orders");
        let body = posted[0].body.as_ref().expect("body");
        assert_eq!(body["shipping"]["last_name"], "Muster");
        assert_eq!(body["shipping"]["address_2"], "c/o Pubky");
        assert!(body.get("billing").is_none());
        assert_eq!(body["payment_method_title"], "Pubky (Bitcoin)");
    }

    // The same order is found again, by its id, without a search index.
    let found = provider
        .find_pushed_order(&context, one.pubky_order_id)
        .await
        .expect("find");
    assert_eq!(
        found.as_ref().map(|o| o.id.as_str()),
        Some(created.id.as_str())
    );
    let unknown = provider
        .find_pushed_order(&context, Uuid::parse_str(ORDER_UNKNOWN).expect("uuid"))
        .await
        .expect("find");
    assert_eq!(unknown, None);

    // A second order, cancelled: the store puts its stock back, twice is fine,
    // and an order that no longer exists is not an error.
    let two = order(ORDER_TWO, false);
    let second = provider.push_order(&context, &two).await.expect("push");
    let cancel = CancelOrder {
        pubky_order_id: two.pubky_order_id,
        reason: "buyer cancelled".into(),
    };
    provider
        .cancel_order(&context, &second, &cancel)
        .await
        .expect("cancel");
    provider
        .cancel_order(&context, &second, &cancel)
        .await
        .expect("cancel again");
    provider
        .cancel_order(
            &context,
            &ExternalOrder {
                id: "99999999".into(),
            },
            &cancel,
        )
        .await
        .expect("a missing order is already cancelled");
    if side.is_live() {
        let live = side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{}", second.id), None)
            .await;
        assert_eq!(live["status"], "cancelled");
        assert!(live["meta_data"]
            .as_array()
            .expect("meta")
            .iter()
            .any(|m| m["key"] == "pubky_cancelled"));
        assert_eq!(
            live["refunds"].as_array().expect("refunds").len(),
            0,
            "a cancel never records a refund"
        );
        let item = side
            .live()
            .rest("GET", &format!("/wc/v3/products/{TEST_PRODUCT}"), None)
            .await;
        assert_eq!(
            item["stock_quantity"], 8,
            "the cancelled order's unit is back"
        );
    }

    // Refunds are mirrored the way the framework drives a provider: read what
    // the store shows (`refunded_total`), work out what is still to mirror
    // from Pubky's recorded total, and record only that.
    assert!(mirror(&provider, &context, &created, &one, "refund-1", 500)
        .await
        .expect("5.00"));
    assert!(
        !mirror(&provider, &context, &created, &one, "refund-1b", 500)
            .await
            .expect("the same total again"),
        "the store already shows it"
    );
    assert!(
        mirror(&provider, &context, &created, &one, "refund-2", 1660)
            .await
            .expect("a later, larger total"),
        "adds only the difference"
    );
    assert!(
        !mirror(&provider, &context, &created, &one, "refund-3", 1000)
            .await
            .expect("a smaller later total"),
    );
    assert!(
        matches!(
            mirror(&provider, &context, &created, &one, "refund-4", 1_000_000).await,
            Err(ProviderError::Rejected { ref code, .. }) if code == "refund_exceeds_payment"
        ),
        "never above the payment"
    );
    assert!(
        mirror(&provider, &context, &created, &one, "refund-5", 5090)
            .await
            .expect("the rest")
    );
    assert!(
        !mirror(&provider, &context, &created, &one, "refund-5b", 5090)
            .await
            .expect("nothing left")
    );
    // A refund id already written is not written again, whatever the amount.
    let again = RefundRecord {
        amount_minor: 100,
        ..refund_record("refund-5", &one, 5090)
    };
    provider
        .record_refund(&context, &created, &again)
        .await
        .expect("the same refund id again writes nothing");
    if side.is_live() {
        let live = side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{}", created.id), None)
            .await;
        assert_eq!(live["status"], "refunded");
        let refunds = live["refunds"].as_array().expect("refunds");
        let mut totals: Vec<&str> = refunds
            .iter()
            .map(|r| r["total"].as_str().expect("total"))
            .collect();
        totals.sort();
        assert_eq!(
            totals,
            ["-11.60", "-34.30", "-5.00"],
            "5.00, then 16.60 in all (11.60 more), then the rest of the payment (50.90 in all)"
        );
        assert!(refunds.iter().all(|r| r["reason"]
            .as_str()
            .expect("reason")
            .starts_with("Pubky refund")));
    }
    if let Some(replay) = &side.replay {
        let refunds = replay.requests(
            "POST",
            &format!("/wp-json/wc/v3/orders/{}/refunds", created.id),
        );
        let amounts: Vec<&str> = refunds
            .iter()
            .map(|posted| {
                let body = posted.body.as_ref().expect("body");
                assert_eq!(body["api_refund"], false, "records only, never moves money");
                body["amount"].as_str().expect("amount")
            })
            .collect();
        assert_eq!(amounts, ["5.00", "11.60", "34.30"]);
    }

    // A refund the merchant already made in the store counts: Pubky's record
    // of 10.00 adds only the 6.00 that is missing, and a record that the store
    // already covers adds nothing.
    let three = order(ORDER_THREE, false);
    let third = provider
        .push_order(&context, &three)
        .await
        .expect("push the third order");
    if side.is_live() {
        side.live()
            .rest(
                "POST",
                &format!("/wc/v3/orders/{}/refunds", third.id),
                Some(json!({"amount": "4.00", "reason": "Damaged box", "api_refund": false})),
            )
            .await;
    }
    let by_hand = provider
        .refunded_total(&context, &third)
        .await
        .expect("what the store shows");
    assert_eq!(
        by_hand.refunded_minor, 400,
        "a refund made by hand, with no marker of ours, counts"
    );
    assert!(
        mirror(&provider, &context, &third, &three, "refund-7", 1000)
            .await
            .expect("10.00 total")
    );
    assert!(
        !mirror(&provider, &context, &third, &three, "refund-8", 300)
            .await
            .expect("covered")
    );
    assert!(
        mirror(&provider, &context, &third, &three, "refund-9", 5090)
            .await
            .expect("the rest")
    );
    if side.is_live() {
        let live = side
            .live()
            .rest("GET", &format!("/wc/v3/orders/{}", third.id), None)
            .await;
        assert_eq!(live["status"], "refunded");
        let mut totals: Vec<&str> = live["refunds"]
            .as_array()
            .expect("refunds")
            .iter()
            .map(|r| r["total"].as_str().expect("total"))
            .collect();
        totals.sort();
        assert_eq!(totals, ["-4.00", "-40.90", "-6.00"]);
    }
    if let Some(replay) = &side.replay {
        let refunds = replay.requests(
            "POST",
            &format!("/wp-json/wc/v3/orders/{}/refunds", third.id),
        );
        let amounts: Vec<&str> = refunds
            .iter()
            .map(|posted| {
                posted.body.as_ref().expect("body")["amount"]
                    .as_str()
                    .expect("amount")
            })
            .collect();
        assert_eq!(amounts, ["6.00", "40.90"]);
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn a_real_webhook_signature_verifies_and_a_changed_body_does_not() {
    let delivery = Delivery::load("flow_a.stock_changed");
    let secret = delivery
        .webhook_secret
        .clone()
        .expect("the recording kept its throwaway secret");
    let provider =
        WooCommerceProvider::with_options(WooOptions::local_testing(true, Vec::new()), None);
    let credentials = StoredCredentials::new(
        format!("ck_{}", "a".repeat(40)),
        format!("cs_{}", "b".repeat(40)),
        AuthMode::Basic,
        secret,
    )
    .into_secret();
    let context = |credentials: CredentialSecret| WebhookContext {
        app: None,
        connection: Some(ConnectionContext {
            connection_id: Uuid::new_v4(),
            seller_pubky: "seller".into(),
            shop: "localhost:8443".into(),
            scopes: vec![],
            credentials,
            app: None,
        }),
    };
    let headers = WebhookHeaders::from_pairs(
        delivery
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone())),
    );
    assert_eq!(
        provider
            .webhook_shop(&headers, &delivery.body())
            .expect("shop"),
        "localhost:8443"
    );
    let verified = provider
        .verify_webhook(&context(credentials), &delivery.body(), &headers)
        .expect("real signature verifies");
    assert_eq!(verified.topic, "product.updated");
    match provider
        .parse_inbound(&verified, &delivery.body())
        .expect("parse")
    {
        InboundEvent::StockLevel {
            provider_product_id,
            provider_variant_id,
            available,
        } => {
            assert_eq!(
                (
                    provider_product_id.as_str(),
                    provider_variant_id.as_str(),
                    available
                ),
                ("39", "39", 6)
            );
        }
        other => panic!("{other:?}"),
    }
    let mut changed = delivery.body();
    changed.extend_from_slice(b" ");
    let other_secret = StoredCredentials::new(
        format!("ck_{}", "a".repeat(40)),
        format!("cs_{}", "b".repeat(40)),
        AuthMode::Basic,
        delivery.webhook_secret.clone().expect("secret"),
    )
    .into_secret();
    assert!(provider
        .verify_webhook(&context(other_secret), &changed, &headers)
        .is_err());
    let wrong = StoredCredentials::new(
        format!("ck_{}", "a".repeat(40)),
        format!("cs_{}", "b".repeat(40)),
        AuthMode::Basic,
        "someone else's secret".into(),
    )
    .into_secret();
    assert!(provider
        .verify_webhook(&context(wrong), &delivery.body(), &headers)
        .is_err());
}

#[tokio::test]
async fn replacing_keys_for_a_store_that_answers_at_another_address_is_refused() {
    // Valid keys, but the store at this address says it is somewhere else:
    // they are not the keys of the store the connection is for.
    let side = StoreSide::open_with("p_rotate_other_store", false).await;
    let provider = side.provider(7);
    let origin = if side.is_live() {
        "https://localhost:8443".to_string()
    } else {
        format!("http://{}", side.site())
    };
    let mut authorize = url::Url::parse(&format!("{origin}/wc-auth/v1/authorize")).expect("url");
    authorize
        .query_pairs_mut()
        .append_pair("app_name", "Pubky Shop")
        .append_pair("scope", "read_write")
        .append_pair("user_id", "rotate-other-store")
        .append_pair("return_url", RETURN_URL)
        .append_pair("callback_url", CALLBACK_URL);
    let mut keys: Value =
        serde_json::from_slice(&side.approve(authorize.as_str(), "callback")).expect("keys");
    keys["rotate"] = json!(true);
    let site = if side.is_live() {
        "127.0.0.1:8443".to_string()
    } else {
        side.site()
    };
    let refused = provider
        .complete_connect(&CompleteRequest {
            pending: PendingConnection {
                connection_id: Uuid::new_v4(),
                seller_pubky: "seller".into(),
                shop: provider.normalize_shop(&site).expect("shop"),
                state: String::new(),
            },
            app: None,
            callback: CallbackParams {
                body: serde_json::to_vec(&keys).expect("json"),
                ..CallbackParams::default()
            },
            webhook_url: webhook_url(Uuid::nil()),
        })
        .await;
    match refused {
        Err(ProviderError::Rejected { code, message }) => {
            assert_eq!(code, "wrong_store");
            assert!(message.contains("localhost:8443"), "{message}");
        }
        other => panic!("expected wrong_store, got {other:?}"),
    }
    if let Some(replay) = &side.replay {
        assert!(
            replay.matched().iter().all(|seen| seen.auth == "none"),
            "no key was sent to a store that is not the connection's"
        );
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn a_refund_applied_late_after_the_client_gave_up_is_found_and_never_written_twice() {
    let side = StoreSide::open("p_refund_late_apply").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = Arc::new(side.provider(7));
    let patient = side.provider_with_timeout(Duration::from_millis(1200));
    let (context, _) = connect_context(&side, &provider, "callback").await;
    let order = order(ORDER_FOUR, false);
    let created = provider.push_order(&context, &order).await.expect("push");

    // The first attempt: the store takes the refund but answers three seconds
    // late, and the client has given up after about one.
    let delay = Duration::from_secs(3);
    side.delay_next("refund", delay);
    let first = mirror(&patient, &context, &created, &order, "refund-late", 1000).await;
    assert!(matches!(first, Err(ProviderError::Unknown(_))), "{first:?}");
    side.let_it_land(delay).await;

    // A successor that read the store's total before the refund landed (so it
    // still thinks 10.00 is to be written) finds the marker and writes nothing.
    let stale = refund_record("refund-late", &order, 1000);
    provider
        .record_refund(&context, &created, &stale)
        .await
        .expect("the earlier attempt is found");
    // One that reads it afterwards has nothing left to mirror.
    assert!(
        !mirror(&provider, &context, &created, &order, "refund-late", 1000)
            .await
            .expect("the store already shows it")
    );
    if side.is_live() {
        let live = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/orders/{}/refunds", created.id),
                None,
            )
            .await;
        let refunds = live.as_array().expect("refunds");
        assert_eq!(refunds.len(), 1, "exactly one refund in the store");
        assert_eq!(refunds[0]["amount"], "10.00");
        assert!(refunds[0]["reason"]
            .as_str()
            .expect("reason")
            .ends_with("[pubky:refund-late]"));
        assert!(refunds[0]["meta_data"]
            .as_array()
            .expect("meta")
            .iter()
            .any(|m| m["key"] == "pubky_refund_id" && m["value"] == "refund-late"));
    }
    if let Some(replay) = &side.replay {
        let refunds = replay.requests(
            "POST",
            &format!("/wp-json/wc/v3/orders/{}/refunds", created.id),
        );
        assert_eq!(refunds.len(), 1, "the successor wrote nothing");
    }

    // A second refund with the same marker (a late apply landing between a
    // successor's read and its write) is settled back to one.
    if side.is_live() {
        side.live()
            .rest(
                "POST",
                &format!("/wc/v3/orders/{}/refunds", created.id),
                Some(json!({
                    "amount": "10.00",
                    "reason": "Pubky refund [pubky:refund-late]",
                    "api_refund": false,
                    "meta_data": [{"key": "pubky_refund_id", "value": "refund-late"}],
                })),
            )
            .await;
    }
    provider
        .record_refund(&context, &created, &stale)
        .await
        .expect("the duplicate is removed");
    // And a refund the merchant made by hand counts.
    if side.is_live() {
        let live = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/orders/{}/refunds", created.id),
                None,
            )
            .await;
        assert_eq!(live.as_array().expect("refunds").len(), 1, "settled to one");
        side.live()
            .rest(
                "POST",
                &format!("/wc/v3/orders/{}/refunds", created.id),
                Some(json!({"amount": "2.00", "reason": "By hand", "api_refund": false})),
            )
            .await;
    }
    let shown = provider
        .refunded_total(&context, &created)
        .await
        .expect("what the store shows");
    assert_eq!(shown.refunded_minor, 1200, "ours and the one made by hand");
    if let Some(replay) = &side.replay {
        let deleted = replay
            .matched()
            .iter()
            .filter(|seen| seen.method == "DELETE" && seen.path.contains("/refunds/"))
            .count();
        assert_eq!(deleted, 1);
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn an_order_applied_late_after_the_client_gave_up_is_never_pushed_twice() {
    let side = StoreSide::open("p_push_late_apply").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = Arc::new(side.provider(7));
    let patient = side.provider_with_timeout(Duration::from_millis(1200));
    let (context, _) = connect_context(&side, &provider, "callback").await;
    let push = order(ORDER_FIVE, false);

    // The first push is applied five seconds late; the client gave up at one.
    let delay = Duration::from_secs(5);
    side.delay_next("order", delay);
    let first = patient.push_order(&context, &push).await;
    assert!(matches!(first, Err(ProviderError::Unknown(_))), "{first:?}");

    // The successor looks at the wrong moment: the store has not applied it yet.
    let early = provider
        .find_pushed_order(&context, push.pubky_order_id)
        .await
        .expect("lookup");
    assert_eq!(early, None);
    side.let_it_land(delay).await;

    // It then pushes (as the framework does after finding nothing). The late
    // order now exists, so this one is the duplicate: it is cancelled and the
    // push resolves to the older order.
    let kept = provider.push_order(&context, &push).await.expect("push");
    let found = provider
        .find_pushed_order(&context, push.pubky_order_id)
        .await
        .expect("lookup");
    assert_eq!(found.as_ref(), Some(&kept), "the push is the oldest order");
    if side.is_live() {
        let orders = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/orders?search={}", push.pubky_order_id),
                None,
            )
            .await;
        let orders = orders.as_array().expect("orders");
        assert_eq!(orders.len(), 2);
        let mut by_id: Vec<(i64, String)> = orders
            .iter()
            .map(|o| {
                (
                    o["id"].as_i64().expect("id"),
                    o["status"].as_str().expect("status").to_string(),
                )
            })
            .collect();
        by_id.sort();
        assert_eq!(by_id[0].0.to_string(), kept.id, "the late one is the push");
        assert_eq!(by_id[0].1, "processing");
        assert_eq!(by_id[1].1, "cancelled", "the duplicate is cancelled");
        let item = side
            .live()
            .rest("GET", &format!("/wc/v3/products/{TEST_PRODUCT}"), None)
            .await;
        assert_eq!(item["stock_quantity"], 8, "stock was taken once");
    }
    if let Some(replay) = &side.replay {
        let cancelled = replay.matched();
        let cancelled: Vec<_> = cancelled
            .iter()
            .filter(|seen| seen.method == "PUT" && seen.path.starts_with("/wp-json/wc/v3/orders/"))
            .collect();
        assert_eq!(cancelled.len(), 1, "one duplicate cancelled");
        let body = cancelled[0].body.as_ref().expect("body");
        assert_eq!(body["status"], "cancelled");
        assert_eq!(body["meta_data"][0]["value"], "duplicate push");
        assert!(
            !cancelled[0].path.ends_with(&format!("/{}", kept.id)),
            "not the kept order"
        );
        replay.assert_no_misses();
    }
}

fn paged_provider(side: &StoreSide, per_page: u32) -> WooCommerceProvider {
    let options = if side.is_live() {
        WooOptions::local_testing(false, vec![woocommerce_support::live::ca_certificate()])
    } else {
        WooOptions::local_testing(true, Vec::new())
    }
    .with_catalog_page_size(7)
    .with_list_page_size(per_page);
    WooCommerceProvider::with_options(options, Some(RETURN_URL.to_string()))
}

#[tokio::test]
async fn an_orders_refunds_are_read_in_full_across_pages_before_deciding_to_write() {
    let side = StoreSide::open("p_refund_pages").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = paged_provider(&side, 2);
    let (context, _) = connect_context(&side, &provider, "callback").await;
    let push = order(ORDER_SIX, false);
    let created = provider.push_order(&context, &push).await.expect("push");

    // Three refunds over pages of two; the oldest is on the second page.
    for (id, amount) in [("refund-a", 1000), ("refund-b", 500), ("refund-c", 300)] {
        let record = RefundRecord {
            amount_minor: amount,
            ..refund_record(id, &push, amount)
        };
        provider
            .record_refund(&context, &created, &record)
            .await
            .expect("record");
    }
    // The same action again must find its marker on the last page and write nothing.
    let again = RefundRecord {
        amount_minor: 1000,
        ..refund_record("refund-a", &push, 1000)
    };
    provider
        .record_refund(&context, &created, &again)
        .await
        .expect("found, not written");
    if side.is_live() {
        let refunds = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/orders/{}/refunds", created.id),
                None,
            )
            .await;
        assert_eq!(refunds.as_array().expect("refunds").len(), 3);
    } else {
        let replay = side.replay.as_ref().expect("replay");
        let posts = replay.requests(
            "POST",
            &format!("/wp-json/wc/v3/orders/{}/refunds", created.id),
        );
        assert_eq!(posts.len(), 3, "the repeated refund was not written again");
        let listed = replay.requests(
            "GET",
            &format!("/wp-json/wc/v3/orders/{}/refunds", created.id),
        );
        assert!(
            listed.iter().any(|seen| seen
                .query
                .iter()
                .any(|(name, value)| name == "page" && value == "2")),
            "the second page was read"
        );
        replay.assert_no_misses();
    }
}

#[tokio::test]
async fn a_duplicate_order_that_could_not_be_cancelled_is_cancelled_by_the_next_lookup() {
    let side = StoreSide::open("p_duplicate_retry").await;
    if side.is_live() {
        reset_store(side.live()).await;
    }
    let provider = paged_provider(&side, 1);
    let (context, _) = connect_context(&side, &provider, "callback").await;
    let push = order(ORDER_SEVEN, false);
    let kept = provider.push_order(&context, &push).await.expect("push");

    // A second order for the same Pubky order (a late apply that landed after
    // the first push), made behind the connector's back.
    if side.is_live() {
        let body = marketplace_service::connectors::providers::woocommerce::orders::order_body(
            &push, None,
        )
        .expect("body");
        side.live().rest("POST", "/wc/v3/orders", Some(body)).await;
    }

    // Cancelling it fails once. The lookup fails with it, so whoever retries
    // is the one that settles it: the duplicate is never abandoned.
    side.fail_next_cancel();
    let first = provider
        .find_pushed_order(&context, push.pubky_order_id)
        .await;
    assert!(matches!(first, Err(ProviderError::Unknown(_))), "{first:?}");
    let retried = provider
        .find_pushed_order(&context, push.pubky_order_id)
        .await
        .expect("the retry settles it");
    assert_eq!(
        retried.as_ref(),
        Some(&kept),
        "the oldest order is the push"
    );
    if side.is_live() {
        let orders = side
            .live()
            .rest(
                "GET",
                &format!("/wc/v3/orders?search={}", push.pubky_order_id),
                None,
            )
            .await;
        let mut states: Vec<(i64, String)> = orders
            .as_array()
            .expect("orders")
            .iter()
            .map(|o| {
                (
                    o["id"].as_i64().expect("id"),
                    o["status"].as_str().expect("status").to_string(),
                )
            })
            .collect();
        states.sort();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].1, "processing");
        assert_eq!(states[1].1, "cancelled");
    } else {
        let replay = side.replay.as_ref().expect("replay");
        let puts: Vec<_> = replay
            .matched()
            .into_iter()
            .filter(|seen| seen.method == "PUT" && seen.path.starts_with("/wp-json/wc/v3/orders/"))
            .collect();
        assert_eq!(puts.len(), 2, "one failed cancel and the retry that worked");
        let searches = replay.requests("GET", "/wp-json/wc/v3/orders");
        assert!(
            searches.iter().any(|seen| seen
                .query
                .iter()
                .any(|(name, value)| name == "page" && value == "2")),
            "the second page of the search was read"
        );
        replay.assert_no_misses();
    }
}
