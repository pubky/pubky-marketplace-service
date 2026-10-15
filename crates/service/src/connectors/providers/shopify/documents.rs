//! The GraphQL documents the provider sends. Each is validated against
//! Shopify's published 2026-10 schema by `tests/shopify_graphql_test.rs`.

pub const APP_UNINSTALL: &str = include_str!("graphql/app_uninstall.graphql");
pub const INVENTORY_ITEM: &str = include_str!("graphql/inventory_item.graphql");
pub const ORDER_CANCEL: &str = include_str!("graphql/order_cancel.graphql");
pub const ORDER_CREATE: &str = include_str!("graphql/order_create.graphql");
pub const ORDER_TRANSACTIONS: &str = include_str!("graphql/order_transactions.graphql");
pub const ORDERS_BY_TAG: &str = include_str!("graphql/orders_by_tag.graphql");
pub const PRODUCT_VARIANTS_PAGE: &str = include_str!("graphql/product_variants_page.graphql");
pub const PRODUCTS_PAGE: &str = include_str!("graphql/products_page.graphql");
pub const RECENT_ORDERS: &str = include_str!("graphql/recent_orders.graphql");
pub const REFUND_CREATE: &str = include_str!("graphql/refund_create.graphql");
pub const SHOP_CURRENCY: &str = include_str!("graphql/shop_currency.graphql");
pub const SHOP_OVERVIEW: &str = include_str!("graphql/shop_overview.graphql");
pub const WEBHOOK_SUBSCRIPTION_CREATE: &str =
    include_str!("graphql/webhook_subscription_create.graphql");
pub const WEBHOOK_SUBSCRIPTION_DELETE: &str =
    include_str!("graphql/webhook_subscription_delete.graphql");
pub const WEBHOOK_SUBSCRIPTIONS: &str = include_str!("graphql/webhook_subscriptions.graphql");

/// Every document with its file name, for the schema validation test.
pub const ALL: [(&str, &str); 15] = [
    ("app_uninstall", APP_UNINSTALL),
    ("inventory_item", INVENTORY_ITEM),
    ("order_cancel", ORDER_CANCEL),
    ("order_create", ORDER_CREATE),
    ("order_transactions", ORDER_TRANSACTIONS),
    ("orders_by_tag", ORDERS_BY_TAG),
    ("product_variants_page", PRODUCT_VARIANTS_PAGE),
    ("products_page", PRODUCTS_PAGE),
    ("recent_orders", RECENT_ORDERS),
    ("refund_create", REFUND_CREATE),
    ("shop_currency", SHOP_CURRENCY),
    ("shop_overview", SHOP_OVERVIEW),
    ("webhook_subscription_create", WEBHOOK_SUBSCRIPTION_CREATE),
    ("webhook_subscription_delete", WEBHOOK_SUBSCRIPTION_DELETE),
    ("webhook_subscriptions", WEBHOOK_SUBSCRIPTIONS),
];
