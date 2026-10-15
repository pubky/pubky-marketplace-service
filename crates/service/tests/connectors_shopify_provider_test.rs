//! The Shopify provider against a local stand-in for Shopify's Admin API
//! (see `common/shopify_fake.rs`): what it sends, how it reads answers, and
//! how it maps throttling, refusals and unknown outcomes. No database.

mod common;

use std::time::Duration;

use chrono::TimeZone;
use common::shopify_fake::{FakeShopify, Fault, ACCESS_TOKEN, GOOD_CODE};
use marketplace_service::connectors::provider::*;
use marketplace_service::connectors::providers::shopify::client::Origin;
use marketplace_service::connectors::providers::shopify::ShopifyProvider;
use serde_json::{json, Value};
use uuid::Uuid;

const SHOP: &str = "dezentral.myshopify.com";
const HOOK: &str = "https://service.test/v1/connectors/shopify/c/abc/webhooks";

async fn setup() -> (FakeShopify, ShopifyProvider, ConnectionContext) {
    let fake = FakeShopify::start().await;
    let provider = ShopifyProvider::with_origin(Origin::Fixed(fake.base_url.clone()));
    let connection = ConnectionContext {
        connection_id: Uuid::new_v4(),
        seller_pubky: "seller".into(),
        shop: SHOP.into(),
        scopes: Vec::new(),
        credentials: CredentialSecret::new(ACCESS_TOKEN.as_bytes().to_vec()),
        app: Some(app()),
    };
    (fake, provider, connection)
}

fn app() -> AppContext {
    AppContext {
        connection_id: Uuid::nil(),
        client_id: "client-id".into(),
        client_secret: CredentialSecret::new(b"app-secret".to_vec()),
    }
}

fn order_id() -> Uuid {
    Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("uuid")
}

fn push(total_extra: i64) -> OrderPush {
    OrderPush {
        pubky_order_id: order_id(),
        currency: "USD".into(),
        exponent: 2,
        lines: vec![OrderPushLine {
            provider_product_id: "gid://shopify/Product/9001".into(),
            provider_variant_id: "gid://shopify/ProductVariant/50001".into(),
            sku: Some("BOOT-40".into()),
            title: "Trail Boots".into(),
            quantity: 2,
            unit_price_minor: 12500,
        }],
        subtotal_minor: 25000,
        shipping_minor: 1200,
        total_minor: 26200 + total_extra,
        payment_method: "paykit".into(),
        paid_at: chrono::Utc
            .with_ymd_and_hms(2026, 10, 15, 9, 30, 0)
            .unwrap(),
        shipping_address: None,
    }
}

/// A Pubky refund record: `amount` is what the Pubky books show refunded in
/// total, in the order currency's minor units.
fn refund(id: &str, amount: Option<i64>, unit: &str) -> RefundRecord {
    RefundRecord {
        refund_id: id.into(),
        pubky_order_id: order_id(),
        amount_minor: amount,
        amount_unit: unit.into(),
        transaction_id: None,
        order_total_minor: 26200,
        order_currency: "USD".into(),
        order_exponent: 2,
        full: amount == Some(26200),
        basis: Some("Amount recorded in USD, the order currency; no currency conversion. Paid with Bitcoin. Order quote: BTCUSD 98123.45 from feed-x at 2026-10-15T08:00:00.000Z.".into()),
    }
}

#[tokio::test]
async fn the_catalog_is_paged_with_whole_products_and_normalised() {
    let (fake, provider, connection) = setup().await;
    let first = provider
        .list_catalog(&connection, None)
        .await
        .expect("page 1");
    assert_eq!(first.next_page.as_deref(), Some("cursor-page-1"));
    let boots = &first.products[0];
    assert_eq!(boots.provider_product_id, "gid://shopify/Product/9001");
    assert_eq!(boots.options[0].name, "Size");
    assert_eq!(
        boots.image_urls,
        ["https://cdn.shopify.com/s/files/boots-1.jpg"]
    );
    assert_eq!(
        boots
            .variants
            .iter()
            .map(|v| v.provider_variant_id.as_str())
            .collect::<Vec<_>>(),
        [
            "gid://shopify/ProductVariant/50001",
            "gid://shopify/ProductVariant/50002",
            "gid://shopify/ProductVariant/50003"
        ],
        "the variants that did not fit the first page are read before the page is returned"
    );
    assert_eq!(boots.variants[0].price.amount_minor, 12500);
    assert_eq!(boots.variants[0].stock, Some(7));
    assert_eq!(boots.variants[1].stock, Some(0));
    assert_eq!(boots.variants[2].price.amount_minor, 12999);
    assert_eq!(boots.variants[2].option_values[0].value, "42");
    assert_eq!(fake.calls("ProductVariantsPage").len(), 1);
    assert_eq!(
        fake.calls("ProductVariantsPage")[0].variables["after"],
        "variants-cursor-1"
    );

    let second = provider
        .list_catalog(&connection, first.next_page.as_deref())
        .await
        .expect("page 2");
    assert_eq!(second.next_page, None);
    let mug = &second.products[0];
    assert!(
        mug.options.is_empty(),
        "the Default Title placeholder is not an option"
    );
    assert_eq!(
        mug.variants[0].stock, None,
        "an untracked variant has no stock"
    );
    assert_eq!(mug.variants[0].sku, None);
    assert_eq!(
        fake.calls("ProductsPage")[1].variables["after"],
        "cursor-page-1"
    );
    fake.assert_clean();
}

#[tokio::test]
async fn the_catalog_prices_follow_the_stores_currency() {
    let (fake, provider, connection) = setup().await;
    fake.lock().shop_currency = "JPY".into();
    let page = provider
        .list_catalog(&connection, None)
        .await
        .expect("page");
    let price = &page.products[0].variants[0].price;
    assert_eq!(
        (price.amount_minor, price.exponent, price.currency.as_str()),
        (125, 0, "JPY")
    );
}

#[tokio::test]
async fn an_inventory_item_resolves_to_its_variant_and_total_stock() {
    let (fake, provider, connection) = setup().await;
    let stock = provider
        .resolve_inventory(&connection, "gid://shopify/InventoryItem/4242")
        .await
        .expect("resolved")
        .expect("a variant");
    assert_eq!(stock.provider_product_id, "gid://shopify/Product/9001");
    assert_eq!(
        stock.provider_variant_id,
        "gid://shopify/ProductVariant/50003"
    );
    assert_eq!(stock.available, Some(3));
    assert!(provider
        .resolve_inventory(&connection, "gid://shopify/InventoryItem/1")
        .await
        .expect("resolved")
        .is_none());
    fake.assert_clean();
}

#[tokio::test]
async fn a_connection_check_reports_scopes_locations_and_a_readable_catalog() {
    let (fake, provider, connection) = setup().await;
    let check = provider.check_connection(&connection).await.expect("check");
    assert_eq!(check.shop_name, "Dezentral Goods");
    assert_eq!(
        check.location_count, 1,
        "inactive locations are not counted"
    );
    assert!(check.catalog_readable);
    assert_eq!(check.granted_scopes.len(), 5);
    fake.fault("ProductsPage", vec![Fault::AccessDenied]);
    let check = provider.check_connection(&connection).await.expect("check");
    assert!(!check.catalog_readable);
    fake.assert_clean();
}

#[tokio::test]
async fn a_pushed_order_is_paid_tagged_and_carries_no_buyer() {
    let (fake, provider, connection) = setup().await;
    let created = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    assert!(created.id.starts_with("gid://shopify/Order/"));
    let orders = fake.orders();
    assert_eq!(orders.len(), 1);
    let order = &orders[0];
    assert_eq!(order.input["financialStatus"], "PAID");
    assert_eq!(
        order.input["tags"],
        json!(["pubky", "pubky-order-11111111-2222-4333-8444-555555555555"])
    );
    assert_eq!(
        order.input["customAttributes"][0]["value"],
        order_id().to_string()
    );
    assert_eq!(
        order.input["lineItems"][0]["variantId"],
        "gid://shopify/ProductVariant/50001"
    );
    assert_eq!(order.input["transactions"][0]["gateway"], "Pubky Paykit");
    assert_eq!(
        order.input["transactions"][0]["amountSet"]["shopMoney"]["amount"],
        "262.00"
    );
    assert_eq!(
        order.options["inventoryBehaviour"],
        "DECREMENT_IGNORING_POLICY"
    );
    assert_eq!(order.options["sendReceipt"], false);
    assert!(order.input.get("shippingAddress").is_none());
    assert!(order.input.get("customer").is_none() && order.input.get("email").is_none());
    fake.assert_clean();
}

#[tokio::test]
async fn an_address_the_buyer_shared_reaches_the_store_order() {
    let (fake, provider, connection) = setup().await;
    let mut order = push(0);
    order.shipping_address = Some(ShippingAddress {
        name: "Ada Lovelace".into(),
        line1: "1 Main St".into(),
        line2: "Flat 2".into(),
        city: "London".into(),
        region: "LND".into(),
        postal_code: "N1 1AA".into(),
        country_code: "GB".into(),
    });
    provider
        .push_order(&connection, &order)
        .await
        .expect("pushed");
    let address = &fake.orders()[0].input["shippingAddress"];
    assert_eq!(address["address1"], "1 Main St");
    assert_eq!(address["address2"], "Flat 2");
    assert_eq!(address["countryCode"], "GB");
    fake.assert_clean();
}

#[tokio::test]
async fn an_order_in_another_currency_or_that_does_not_add_up_is_refused_unsent() {
    let (fake, provider, connection) = setup().await;
    let mut euro = push(0);
    euro.currency = "EUR".into();
    let error = provider
        .push_order(&connection, &euro)
        .await
        .expect_err("refused");
    assert!(matches!(error, ProviderError::Rejected { code, .. } if code == "currency_mismatch"));
    let error = provider
        .push_order(&connection, &push(5))
        .await
        .expect_err("refused");
    assert!(matches!(error, ProviderError::Rejected { code, .. } if code == "total_mismatch"));
    let mut odd = push(0);
    odd.exponent = 0;
    let error = provider
        .push_order(&connection, &odd)
        .await
        .expect_err("refused");
    assert!(matches!(error, ProviderError::Rejected { code, .. } if code == "exponent_mismatch"));
    assert!(fake.calls("OrderCreate").is_empty());
    fake.assert_clean();
}

#[tokio::test]
async fn a_lookup_finds_an_order_the_search_index_has_not_caught_up_on() {
    let (fake, provider, connection) = setup().await;
    assert!(provider
        .find_pushed_order(&connection, order_id())
        .await
        .expect("find")
        .is_none());
    let created = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    fake.lock().search_lags = true;
    let found = provider
        .find_pushed_order(&connection, order_id())
        .await
        .expect("find")
        .expect("found through the recent-orders scan");
    assert_eq!(found, created);
    assert_eq!(fake.calls("OrdersByTag").len(), 2);
    assert!(provider
        .find_pushed_order(&connection, Uuid::new_v4())
        .await
        .expect("find")
        .is_none());
    fake.lock().search_lags = false;
    assert_eq!(
        provider
            .find_pushed_order(&connection, order_id())
            .await
            .expect("find"),
        Some(created)
    );
    fake.assert_clean();
}

#[tokio::test]
async fn a_create_whose_answer_was_lost_is_unknown_and_then_found_not_repeated() {
    let (fake, provider, connection) = setup().await;
    fake.fault("OrderCreate", vec![Fault::AppliedThenServerError]);
    let error = provider
        .push_order(&connection, &push(0))
        .await
        .expect_err("unknown");
    assert!(matches!(error, ProviderError::Unknown(_)), "{error:?}");
    assert_eq!(fake.orders().len(), 1, "the store applied it");
    let found = provider
        .find_pushed_order(&connection, order_id())
        .await
        .expect("find");
    assert_eq!(
        found.map(|order| order.id),
        Some(fake.orders()[0].id.clone())
    );
    fake.assert_clean();
}

#[tokio::test]
async fn a_cancel_restocks_never_refunds_and_is_safe_to_repeat() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    let cancel = CancelOrder {
        pubky_order_id: order_id(),
        reason: "buyer changed their mind".into(),
    };
    provider
        .cancel_order(&connection, &order, &cancel)
        .await
        .expect("cancel");
    provider
        .cancel_order(&connection, &order, &cancel)
        .await
        .expect("repeat cancel");
    let calls = fake.calls("OrderCancel");
    assert_eq!(calls.len(), 1, "a cancelled order is not cancelled again");
    let variables = &calls[0].variables;
    assert_eq!(variables["restock"], true);
    assert_eq!(variables["reason"], "OTHER");
    assert_eq!(variables["notifyCustomer"], false);
    assert!(
        variables.get("refundMethod").is_none(),
        "money never moved through the store"
    );
    assert!(variables["staffNote"]
        .as_str()
        .expect("note")
        .contains("buyer changed their mind"));
    assert!(fake.orders()[0].refunds.is_empty());

    let missing = ExternalOrder {
        id: "gid://shopify/Order/1".into(),
    };
    let error = provider
        .cancel_order(&connection, &missing, &cancel)
        .await
        .expect_err("missing");
    assert!(matches!(error, ProviderError::Rejected { code, .. } if code == "order_not_found"));
    fake.assert_clean();
}

#[tokio::test]
async fn a_refund_is_recorded_once_per_pubky_refund_with_a_manual_transaction() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    let partial = refund("refund-a", Some(5000), "usd");
    provider
        .record_refund(&connection, &order, &partial)
        .await
        .expect("refund");
    provider
        .record_refund(&connection, &order, &partial)
        .await
        .expect("replayed refund");
    assert_eq!(
        fake.orders()[0].refunds.len(),
        1,
        "a replay finds the refund already in the store and sends nothing"
    );
    let input = &fake.orders()[0].refunds[0];
    assert_eq!(input["transactions"][0]["kind"], "REFUND");
    assert_eq!(input["transactions"][0]["amount"], "50.00");
    assert_eq!(input["transactions"][0]["gateway"], "Pubky Paykit");
    assert!(input["transactions"][0]["parentId"]
        .as_str()
        .expect("parent")
        .contains("OrderTransaction"));
    assert_eq!(input["notify"], false);
    assert!(
        input.get("refundLineItems").is_none(),
        "a Pubky refund does not restock"
    );
    let note = input["note"].as_str().expect("note");
    assert!(
        note.contains("no currency conversion") && note.contains("BTCUSD 98123.45"),
        "{note}"
    );
    assert!(note.ends_with("pubky-refund:refund-a:5000:USD"), "{note}");
    fake.assert_clean();
}

#[tokio::test]
async fn a_full_refund_after_a_partial_one_refunds_only_the_difference() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    provider
        .record_refund(&connection, &order, &refund("refund-a", Some(5000), "usd"))
        .await
        .expect("partial");
    provider
        .record_refund(&connection, &order, &refund("refund-b", Some(26200), "usd"))
        .await
        .expect("full after partial");
    let refunds = fake.orders()[0].refunds.clone();
    assert_eq!(refunds.len(), 2);
    assert_eq!(refunds[1]["transactions"][0]["amount"], "212.00");
    assert_eq!(
        fake.orders()[0].refunded_minor(),
        26200,
        "the store never shows more than was paid"
    );
    let keys: Vec<String> = fake
        .calls("RefundCreate")
        .iter()
        .map(|call| {
            call.document
                .split("@idempotent(key: \"")
                .nth(1)
                .expect("key")
                .split('"')
                .next()
                .expect("key")
                .to_string()
        })
        .collect();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
    // The same full record again is already reflected: nothing is sent.
    provider
        .record_refund(&connection, &order, &refund("refund-b", Some(26200), "usd"))
        .await
        .expect("replayed full");
    assert_eq!(fake.calls("RefundCreate").len(), 2);
    fake.assert_clean();
}

#[tokio::test]
async fn a_refund_the_merchant_already_made_in_the_store_is_not_made_twice() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    fake.merchant_refund(&order.id, 10000);
    provider
        .record_refund(&connection, &order, &refund("refund-a", Some(26200), "usd"))
        .await
        .expect("difference");
    assert_eq!(fake.orders()[0].refunded_minor(), 26200);
    assert_eq!(
        fake.orders()[0].refunds[1]["transactions"][0]["amount"],
        "162.00"
    );
    provider
        .record_refund(&connection, &order, &refund("refund-c", Some(20000), "usd"))
        .await
        .expect("already covered");
    assert_eq!(
        fake.calls("RefundCreate").len(),
        1,
        "a record the store already covers sends nothing"
    );
    fake.assert_clean();
}

#[tokio::test]
async fn a_refund_that_cannot_be_stated_in_the_store_currency_is_refused() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    for (record, wanted) in [
        (
            refund("refund-sat", Some(40_000), "sat"),
            "refund_amount_unconvertible",
        ),
        (refund("refund-none", None, "usd"), "refund_amount_missing"),
        (
            refund("refund-big", Some(99_999), "usd"),
            "refund_exceeds_payment",
        ),
    ] {
        let error = provider
            .record_refund(&connection, &order, &record)
            .await
            .expect_err("refused");
        assert!(
            matches!(&error, ProviderError::Rejected { code, .. } if code == wanted),
            "{wanted}: {error:?}"
        );
    }
    let mut euro = refund("refund-eur", Some(1000), "eur");
    euro.order_currency = "EUR".into();
    let error = provider
        .record_refund(&connection, &order, &euro)
        .await
        .expect_err("the store order is in USD");
    assert!(matches!(error, ProviderError::Rejected { code, .. } if code == "currency_mismatch"));
    assert!(fake.calls("RefundCreate").is_empty());
    fake.assert_clean();
}

#[tokio::test]
async fn a_throttled_call_waits_and_retries_before_it_gives_up() {
    let (fake, provider, connection) = setup().await;
    fake.fault("ProductsPage", vec![Fault::Throttled, Fault::Throttled]);
    provider
        .list_catalog(&connection, None)
        .await
        .expect("succeeds on the third attempt");
    assert_eq!(fake.calls("ProductsPage").len(), 3);

    fake.fault(
        "ShopCurrency",
        vec![Fault::Throttled, Fault::Throttled, Fault::Throttled],
    );
    let error = provider
        .push_order(&connection, &push(0))
        .await
        .expect_err("rate limited");
    assert!(
        matches!(error, ProviderError::RateLimited { retry_after: Some(wait) } if wait < Duration::from_secs(1)),
        "{error:?}"
    );
    assert!(fake.orders().is_empty(), "a throttled call applied nothing");

    fake.fault("ShopCurrency", vec![Fault::TooManyRequests; 3]);
    let error = provider
        .push_order(&connection, &push(0))
        .await
        .expect_err("429");
    assert!(
        matches!(error, ProviderError::RateLimited { .. }),
        "{error:?}"
    );
    fake.assert_clean();
}

#[tokio::test]
async fn store_failures_map_to_the_contracts_outcomes() {
    let (fake, provider, connection) = setup().await;
    fake.fault("OrderCreate", vec![Fault::Status(503)]);
    assert!(matches!(
        provider.push_order(&connection, &push(0)).await,
        Err(ProviderError::Unknown(_))
    ));
    fake.fault("OrderCreate", vec![Fault::Status(401)]);
    assert!(matches!(
        provider.push_order(&connection, &push(0)).await,
        Err(ProviderError::Unauthorized)
    ));
    fake.fault("OrderCreate", vec![Fault::AccessDenied]);
    assert!(matches!(
        provider.push_order(&connection, &push(0)).await,
        Err(ProviderError::Rejected { code, .. }) if code == "access_denied"
    ));
    fake.fault("OrderCreate", vec![Fault::Status(422)]);
    assert!(matches!(
        provider.push_order(&connection, &push(0)).await,
        Err(ProviderError::Rejected { code, .. }) if code == "http_422"
    ));
    let wrong = ConnectionContext {
        credentials: CredentialSecret::new(b"revoked-token".to_vec()),
        ..connection_clone(&connection)
    };
    assert!(matches!(
        provider.list_catalog(&wrong, None).await,
        Err(ProviderError::Unauthorized)
    ));
    fake.assert_clean();
}

fn connection_clone(connection: &ConnectionContext) -> ConnectionContext {
    ConnectionContext {
        connection_id: connection.connection_id,
        seller_pubky: connection.seller_pubky.clone(),
        shop: connection.shop.clone(),
        scopes: connection.scopes.clone(),
        credentials: CredentialSecret::new(connection.credentials.expose().to_vec()),
        app: Some(app()),
    }
}

#[tokio::test]
async fn webhooks_are_subscribed_once_and_stale_service_subscriptions_are_replaced() {
    let (fake, provider, connection) = setup().await;
    {
        let mut inner = fake.lock();
        inner.webhooks.push((
            "gid://shopify/WebhookSubscription/1".into(),
            "ORDERS_CANCELLED".into(),
            "https://old.service.test/v1/connectors/webhooks/shopify/old/hook".into(),
        ));
        inner.webhooks.push((
            "gid://shopify/WebhookSubscription/2".into(),
            "ORDERS_CANCELLED".into(),
            "https://merchant.example/their-own-hook".into(),
        ));
    }
    provider
        .register_webhooks(&connection, HOOK)
        .await
        .expect("registered");
    let mut topics: Vec<String> = fake
        .lock()
        .webhooks
        .iter()
        .filter(|(_, _, uri)| uri == HOOK)
        .map(|(_, topic, _)| topic.clone())
        .collect();
    topics.sort();
    assert_eq!(
        topics,
        [
            "APP_UNINSTALLED",
            "FULFILLMENTS_CREATE",
            "FULFILLMENTS_UPDATE",
            "INVENTORY_LEVELS_UPDATE",
            "ORDERS_CANCELLED",
            "REFUNDS_CREATE"
        ]
    );
    let uris: Vec<String> = fake
        .lock()
        .webhooks
        .iter()
        .map(|(_, _, uri)| uri.clone())
        .collect();
    assert!(
        !uris.iter().any(|uri| uri.contains("/old/")),
        "the stale subscription is gone"
    );
    assert!(
        uris.contains(&"https://merchant.example/their-own-hook".to_string()),
        "the merchant's own subscription is untouched"
    );
    let creates = fake.calls("WebhookSubscriptionCreate").len();
    provider
        .register_webhooks(&connection, HOOK)
        .await
        .expect("registered again");
    assert_eq!(
        fake.calls("WebhookSubscriptionCreate").len(),
        creates,
        "nothing is created twice"
    );
    let created = fake.calls("WebhookSubscriptionCreate");
    assert_eq!(
        created[0].variables["webhookSubscription"]["format"],
        "JSON"
    );
    fake.assert_clean();
}

#[tokio::test]
async fn disconnecting_uninstalls_the_app() {
    let (fake, provider, connection) = setup().await;
    provider
        .disconnect(&connection)
        .await
        .expect("disconnected");
    assert!(fake.lock().app_uninstalled);
    fake.assert_clean();
}

#[tokio::test]
async fn the_recent_order_scan_pages_until_it_finds_the_order_or_leaves_the_horizon() {
    let (fake, provider, connection) = setup().await;
    let created = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    fake.lock().search_lags = true;
    // 120 newer orders push ours to the third page of 50.
    fake.add_other_orders(120);
    let found = provider
        .find_pushed_order(&connection, order_id())
        .await
        .expect("find")
        .expect("found on the third page");
    assert_eq!(found, created);
    let pages: Vec<Value> = fake
        .calls("RecentOrders")
        .iter()
        .map(|call| call.variables["after"].clone())
        .collect();
    assert_eq!(
        pages,
        [Value::Null, json!("recent-50"), json!("recent-100")]
    );

    // An order that is not there ends the scan at the first order older than
    // the horizon, not at the end of the store's history.
    fake.age_orders(48);
    fake.lock().orders.retain(|order| order.id == created.id);
    let before = fake.calls("RecentOrders").len();
    assert!(provider
        .find_pushed_order(&connection, Uuid::new_v4())
        .await
        .expect("find")
        .is_none());
    assert_eq!(fake.calls("RecentOrders").len() - before, 1);
    fake.assert_clean();
}

#[tokio::test]
async fn a_scan_that_hits_its_bound_inside_the_horizon_says_it_cannot_tell() {
    let (fake, provider, connection) = setup().await;
    fake.lock().search_lags = true;
    // More than 10 pages of 50 orders created within the last day.
    fake.add_other_orders(520);
    let error = provider
        .find_pushed_order(&connection, order_id())
        .await
        .expect_err("cannot rule the order out");
    assert!(
        matches!(&error, ProviderError::Unknown(message) if message.contains("could not be ruled out")),
        "{error:?}"
    );
    assert_eq!(fake.calls("RecentOrders").len(), 10);
    fake.assert_clean();
}

#[tokio::test]
async fn subscriptions_are_read_page_by_page_and_a_lost_race_is_not_a_failure() {
    let (fake, provider, connection) = setup().await;
    {
        let mut inner = fake.lock();
        inner.subscription_page_size = 2;
        for number in 0..5 {
            inner.webhooks.push((
                format!("gid://shopify/WebhookSubscription/{number}"),
                "PRODUCTS_UPDATE".into(),
                format!("https://merchant.example/hook-{number}"),
            ));
        }
        inner.webhooks.push((
            "gid://shopify/WebhookSubscription/99".into(),
            "REFUNDS_CREATE".into(),
            "https://old.service.test/v1/connectors/webhooks/shopify/old/hook".into(),
        ));
    }
    provider
        .register_webhooks(&connection, HOOK)
        .await
        .expect("registered");
    assert!(
        !fake
            .lock()
            .webhooks
            .iter()
            .any(|(_, _, uri)| uri.contains("/old/")),
        "a stale subscription on the last of three pages is still found and removed"
    );
    assert_eq!(fake.calls("WebhookSubscriptions").len(), 3);

    // Another registration wins the race for one topic between this one's
    // listing and its create: the create answers with a user error, and a
    // re-read shows the subscription, so it is a success.
    let (fake, provider, connection) = setup().await;
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    fake.fault(
        "WebhookSubscriptionCreate",
        vec![Fault::Hold {
            entered: entered.clone(),
            release: release.clone(),
        }],
    );
    let racing = tokio::spawn({
        let connection = connection_clone(&connection);
        async move { provider.register_webhooks(&connection, HOOK).await }
    });
    entered.notified().await;
    fake.lock().webhooks.push((
        "gid://shopify/WebhookSubscription/7".into(),
        "INVENTORY_LEVELS_UPDATE".into(),
        HOOK.into(),
    ));
    release.notify_one();
    racing
        .await
        .expect("task")
        .expect("the lost race is not a failure");
    let topics = fake
        .lock()
        .webhooks
        .iter()
        .filter(|(_, _, uri)| uri == HOOK)
        .count();
    assert_eq!(topics, 6, "one subscription per topic, none twice");
    fake.assert_clean();
}

#[tokio::test]
async fn a_refused_cancel_is_judged_by_the_order_not_by_the_message() {
    let (fake, provider, connection) = setup().await;
    let order = provider
        .push_order(&connection, &push(0))
        .await
        .expect("pushed");
    let cancel = CancelOrder {
        pubky_order_id: order_id(),
        reason: "buyer".into(),
    };
    // Refused and not cancelled: not applied yet, so it is retried later, not
    // quarantined.
    fake.lock().cancel_refusal = Some(("INVALID".into(), false));
    let error = provider
        .cancel_order(&connection, &order, &cancel)
        .await
        .expect_err("not accepted yet");
    assert!(matches!(error, ProviderError::Unknown(_)), "{error:?}");
    // Refused because the first cancel's job got there first: the order is
    // cancelled, so this is a success whatever the wording.
    fake.lock().cancel_refusal = Some(("INVALID".into(), true));
    provider
        .cancel_order(&connection, &order, &cancel)
        .await
        .expect("already cancelled by the first job");
    fake.assert_clean();
}

fn signed(secret: &str, shop: &str, timestamp: i64, extra: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut pairs = vec![format!("shop={shop}"), format!("timestamp={timestamp}")];
    pairs.extend(
        extra
            .split('&')
            .filter(|part| !part.is_empty())
            .map(str::to_string),
    );
    pairs.sort();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).expect("key");
    mac.update(pairs.join("&").as_bytes());
    format!(
        "{}&hmac={}",
        pairs.join("&"),
        hex::encode(mac.finalize().into_bytes())
    )
}

fn callback(raw_query: &str) -> CallbackParams {
    CallbackParams {
        raw_query: raw_query.to_string(),
        query: url::form_urlencoded::parse(raw_query.as_bytes())
            .into_owned()
            .collect(),
        ..CallbackParams::default()
    }
}

fn complete(raw_query: &str) -> CompleteRequest {
    CompleteRequest {
        pending: PendingConnection {
            connection_id: Uuid::nil(),
            seller_pubky: "seller".into(),
            shop: SHOP.into(),
            state: "s".into(),
        },
        app: Some(app()),
        callback: callback(raw_query),
        webhook_url: HOOK.into(),
    }
}

#[tokio::test]
async fn a_launch_is_verified_with_the_apps_secret_and_sent_to_authorize() {
    let (_fake, provider, _connection) = setup().await;
    let now = chrono::Utc::now().timestamp();
    let launch = callback(&signed("app-secret", SHOP, now, "host=YWJj"));
    assert_eq!(
        provider.verify_launch(&app(), &launch).expect("verified"),
        SHOP
    );
    let wrong = callback(&signed("other-secret", SHOP, now, ""));
    assert!(provider.verify_launch(&app(), &wrong).is_err());
    let stale = callback(&signed("app-secret", SHOP, now - 7200, ""));
    assert!(provider.verify_launch(&app(), &stale).is_err());
    let foreign = callback(&signed("app-secret", "evil.example.com", now, ""));
    assert!(provider.verify_launch(&app(), &foreign).is_err());

    let url = provider
        .authorize_url(&AuthorizeRequest {
            app: app(),
            shop: SHOP.into(),
            state: "nonce-1".into(),
            callback_url: "https://service.test/v1/connectors/shopify/c/abc/oauth/callback".into(),
        })
        .expect("authorize url");
    let parsed = url::Url::parse(&url).expect("url");
    assert_eq!(parsed.host_str(), Some(SHOP));
    assert_eq!(parsed.path(), "/admin/oauth/authorize");
    let query: std::collections::BTreeMap<_, _> = parsed.query_pairs().into_owned().collect();
    assert_eq!(query["client_id"], "client-id");
    assert_eq!(query["state"], "nonce-1");
    assert_eq!(
        query["scope"],
        "read_products,read_inventory,read_locations,read_fulfillments,write_orders"
    );
    assert!(!url.contains("app-secret"));
}

#[tokio::test]
async fn the_authorization_code_is_exchanged_for_an_offline_token_after_the_callback_verifies() {
    let (fake, provider, _connection) = setup().await;
    fake.expect_client_secret("app-secret");
    let now = chrono::Utc::now().timestamp();
    let good = signed(
        "app-secret",
        SHOP,
        now,
        &format!("code={GOOD_CODE}&state=s"),
    );
    let done = provider
        .complete_connect(&complete(&good))
        .await
        .expect("completed");
    assert_eq!(done.credentials.expose(), ACCESS_TOKEN.as_bytes());
    assert_eq!(done.scopes.len(), 5);
    let request = fake.lock().token_requests[0].clone();
    assert_eq!(request["code"], GOOD_CODE);
    assert_eq!(request["client_id"], "client-id");
    assert_eq!(request["client_secret"], "app-secret");

    let before = fake.lock().token_requests.len();
    for (query, why) in [
        (
            signed(
                "other-secret",
                SHOP,
                now,
                &format!("code={GOOD_CODE}&state=s"),
            ),
            "wrong secret",
        ),
        (
            signed(
                "app-secret",
                SHOP,
                now - 7200,
                &format!("code={GOOD_CODE}&state=s"),
            ),
            "stale",
        ),
        (
            signed(
                "app-secret",
                "other.myshopify.com",
                now,
                &format!("code={GOOD_CODE}&state=s"),
            ),
            "another store",
        ),
        (signed("app-secret", SHOP, now, "state=s"), "no code"),
    ] {
        assert!(
            provider.complete_connect(&complete(&query)).await.is_err(),
            "{why}"
        );
    }
    assert_eq!(
        fake.lock().token_requests.len(),
        before,
        "nothing unverified reaches Shopify"
    );
    let refused = signed("app-secret", SHOP, now, "code=used-up&state=s");
    let error = provider
        .complete_connect(&complete(&refused))
        .await
        .expect_err("refused code");
    assert!(
        matches!(error, ProviderError::Rejected { code, .. } if code == "token_exchange_refused")
    );
}

#[test]
fn the_privacy_topics_are_named_for_the_merchant_guide() {
    use marketplace_service::connectors::providers::shopify::webhooks::{
        PRIVACY_TOPICS, SUBSCRIBED_TOPICS,
    };
    assert_eq!(
        PRIVACY_TOPICS,
        ["customers/data_request", "customers/redact", "shop/redact"]
    );
    assert_eq!(SUBSCRIBED_TOPICS.len(), 6);
    let _ = Value::Null;
}

#[tokio::test]
async fn a_pushed_order_asks_for_no_tax_and_its_total_is_what_the_buyer_paid() {
    let (fake, provider, connection) = setup().await;
    let order = push(0);
    let created = provider
        .push_order(&connection, &order)
        .await
        .expect("pushed");
    let sent = &fake.orders()[0].input;
    assert_eq!(sent["taxesIncluded"], false);
    assert_eq!(sent["taxLines"], json!([]));
    assert_eq!(sent["lineItems"][0]["taxable"], false);
    assert_eq!(sent["shippingLines"][0]["taxLines"], json!([]));
    assert_eq!(
        provider
            .verify_pushed_order(&connection, &created, &order)
            .await
            .expect("verified"),
        None,
        "no tax, and the total is what was paid"
    );

    // A store that adds tax whatever the order says is reported, in words a
    // seller can act on, and the order is left alone.
    fake.lock().forced_tax_bps = 1000;
    let message = provider
        .verify_pushed_order(&connection, &created, &order)
        .await
        .expect("verified")
        .expect("a mismatch");
    assert!(
        message.contains("288.20 USD") && message.contains("262.00 USD"),
        "{message}"
    );
    assert!(message.contains("added 26.20 USD tax"), "{message}");
    assert_eq!(fake.orders().len(), 1);
    fake.assert_clean();
}
