//! The WooCommerce provider against WooCommerce, with no database: what
//! connecting accepts and refuses, how the key pair is sent, the catalog, and
//! the order operations (create, find, cancel, record a refund) with their
//! idempotency. The store side is the replay of exchanges captured from a real
//! WooCommerce (`tests/fixtures/woocommerce/README.md`); with `WOO_LIVE=1` the
//! same tests run against the Docker store and re-record them.

mod common;
mod woocommerce_support;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use marketplace_service::connectors::provider::*;
use marketplace_service::connectors::providers::woocommerce::creds::{AuthMode, StoredCredentials};
use marketplace_service::connectors::providers::woocommerce::{WooCommerceProvider, WooOptions};
use serde_json::{json, Value};
use uuid::Uuid;
use woocommerce_support::flows::*;
use woocommerce_support::live::RestoreStore;
use woocommerce_support::replay::Delivery;
use woocommerce_support::{connect_context, StoreSide, CALLBACK_URL, RETURN_URL, WEBHOOK_URL};

fn request(side_site: &str, provider: &WooCommerceProvider) -> ConnectRequest {
    ConnectRequest {
        connection_id: Uuid::new_v4(),
        seller_pubky: "seller".into(),
        shop: provider.normalize_shop(side_site).expect("shop"),
        state: format!("nonce-{}", Uuid::new_v4().simple()),
        app: None,
        callback_url: CALLBACK_URL.into(),
        webhook_url: WEBHOOK_URL.into(),
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

#[tokio::test]
async fn connecting_registers_our_webhooks_and_replaces_an_earlier_connection() {
    let side = StoreSide::open("p_connect").await;
    let provider = side.provider(7);
    let (first, first_secret) = connect_context(&side, &provider, "callback").await;
    let stored = StoredCredentials::open(&first.credentials).expect("credentials");
    assert_eq!(stored.auth, AuthMode::Basic);
    assert_eq!(first.scopes, vec!["read_write"]);
    assert_eq!(first_secret.len(), 64);

    // Connecting again (new keys) removes the old webhooks before adding new ones.
    let (second, second_secret) = connect_context(&side, &provider, "callback2").await;
    assert_ne!(
        first_secret, second_secret,
        "a fresh webhook secret per connection"
    );

    if let Some(replay) = &side.replay {
        let posts = webhook_posts(&side);
        assert_eq!(posts.len(), 6);
        let topics: Vec<&str> = posts[..3]
            .iter()
            .map(|p| p["topic"].as_str().expect("topic"))
            .collect();
        assert_eq!(
            topics,
            ["product.updated", "order.updated", "order.deleted"]
        );
        for post in &posts {
            assert_eq!(post["delivery_url"], WEBHOOK_URL);
            assert_eq!(post["status"], "active");
            assert_eq!(post["secret"].as_str().expect("secret").len(), 64);
            assert!(post["name"]
                .as_str()
                .expect("name")
                .starts_with("Pubky Shop"));
        }
        let deleted = replay
            .matched()
            .iter()
            .filter(|seen| {
                seen.method == "DELETE" && seen.path.starts_with("/wp-json/wc/v3/webhooks/")
            })
            .count();
        assert!(
            deleted >= 3,
            "the earlier connection's webhooks are removed first: {deleted}"
        );
        assert!(replay
            .matched()
            .iter()
            .all(|seen| seen.auth == "basic" || seen.auth == "none"));
    }

    // Disconnecting takes our webhooks off the store.
    provider.disconnect(&second).await.expect("disconnect");
    if side.is_live() {
        let hooks = side
            .live()
            .rest("GET", "/wc/v3/webhooks?per_page=100", None)
            .await;
        assert!(
            hooks
                .as_array()
                .expect("hooks")
                .iter()
                .all(|hook| hook["delivery_url"] != WEBHOOK_URL),
            "no webhook of ours is left"
        );
    }
    let _ = first;
    if let Some(replay) = &side.replay {
        replay.assert_no_misses();
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
            webhook_url: WEBHOOK_URL.into(),
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
            webhook_url: WEBHOOK_URL.into(),
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
            webhook_url: WEBHOOK_URL.into(),
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

fn refund(id: &str, order: &OrderPush, amount: Option<i64>, full: bool) -> RefundRecord {
    RefundRecord {
        refund_id: id.into(),
        pubky_order_id: order.pubky_order_id,
        amount_minor: amount,
        amount_unit: order.currency.to_lowercase(),
        transaction_id: None,
        order_total_minor: order.total_minor,
        order_currency: order.currency.clone(),
        order_exponent: order.exponent,
        full,
    }
}

const ORDER_ONE: &str = "11111111-2222-8333-9444-555555555551";
const ORDER_TWO: &str = "11111111-2222-8333-9444-555555555552";
const ORDER_THREE: &str = "11111111-2222-8333-9444-555555555553";
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

    // Refunds are recorded in the store's books, never as money moved. Each
    // Pubky record is a refund total in the order currency; what is added is
    // the difference from what the store already shows refunded.
    let first = refund("refund-1", &one, Some(500), false);
    provider
        .record_refund(&context, &created, &first)
        .await
        .expect("5.00");
    provider
        .record_refund(&context, &created, &first)
        .await
        .expect("the same record again adds nothing");
    let larger = refund("refund-2", &one, Some(1660), false);
    provider
        .record_refund(&context, &created, &larger)
        .await
        .expect("a later, larger total adds only the difference");
    let smaller = refund("refund-3", &one, Some(1000), false);
    provider
        .record_refund(&context, &created, &smaller)
        .await
        .expect("a smaller later total adds nothing");
    let beyond = refund("refund-4", &one, Some(1_000_000), false);
    provider
        .record_refund(&context, &created, &beyond)
        .await
        .expect("a total above the payment is capped at the payment");
    let full = refund("refund-5", &one, None, true);
    provider
        .record_refund(&context, &created, &full)
        .await
        .expect("nothing is left to refund");
    assert!(matches!(
        provider
            .record_refund(&context, &created, &refund("refund-6", &one, None, false))
            .await,
        Err(ProviderError::Rejected { ref code, .. }) if code == "refund_amount_missing"
    ));
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
    provider
        .record_refund(
            &context,
            &third,
            &refund("refund-7", &three, Some(1000), false),
        )
        .await
        .expect("10.00 total, 4.00 already refunded by the merchant");
    provider
        .record_refund(
            &context,
            &third,
            &refund("refund-8", &three, Some(300), false),
        )
        .await
        .expect("the store already shows more than this");
    provider
        .record_refund(&context, &third, &refund("refund-9", &three, None, true))
        .await
        .expect("the rest");
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
