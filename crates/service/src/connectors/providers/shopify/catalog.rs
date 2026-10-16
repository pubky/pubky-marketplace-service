//! The store's catalog, normalised into the trait's catalog types.
//!
//! Products come a few at a time (`PRODUCT_PAGE_SIZE`, sized so a page stays
//! under Shopify's 1000-point single-query cap with 25 variants and 5 images
//! each). A product with more than 25 variants has its remaining variants read
//! through the product's own variant connection before the page is returned,
//! so the framework always sees whole products. The page cursor is Shopify's
//! own `endCursor`, opaque to the framework.
//!
//! Only `status:active` products are listed. A variant's stock is the
//! variant's total across the store's locations (`inventoryQuantity`); a
//! variant whose inventory item is not tracked has `stock: None`.

use serde_json::{json, Value};

use super::client::Client;
use super::documents;
use super::money::{decimal_to_minor, exponent};
use crate::connectors::provider::{
    CatalogOption, CatalogOptionValue, CatalogPage, CatalogProduct, CatalogVariant, Price,
    ProviderError, ResolvedStock,
};

pub const PRODUCT_PAGE_SIZE: i64 = 8;
const MAX_VARIANT_PAGES: usize = 100;

fn malformed(what: &str) -> ProviderError {
    ProviderError::Unknown(format!("shopify returned an unexpected {what}"))
}

fn text(value: &Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|text| !text.is_empty())
}

pub async fn page(
    client: &Client,
    shop: &str,
    token: &str,
    after: Option<&str>,
) -> Result<CatalogPage, ProviderError> {
    let data = client
        .execute(
            shop,
            token,
            documents::PRODUCTS_PAGE,
            json!({ "first": PRODUCT_PAGE_SIZE, "after": after }),
        )
        .await?;
    let currency = data
        .pointer("/shop/currencyCode")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("shop currency"))?
        .to_string();
    let connection = data
        .get("products")
        .ok_or_else(|| malformed("products page"))?;
    let nodes = connection
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("products page"))?;
    let mut products = Vec::with_capacity(nodes.len());
    for node in nodes {
        let mut variants: Vec<Value> = node
            .pointer("/variants/nodes")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| malformed("variants page"))?;
        let product_id = text(node, "id").ok_or_else(|| malformed("product id"))?;
        let mut more = node
            .pointer("/variants/pageInfo")
            .map(cursor_of)
            .unwrap_or_default();
        let mut pages = 0;
        while let Some(cursor) = more {
            pages += 1;
            if pages > MAX_VARIANT_PAGES {
                return Err(ProviderError::Rejected {
                    code: "too_many_variants".into(),
                    message: format!("product {product_id} has more variants than can be read"),
                });
            }
            let rest = client
                .execute(
                    shop,
                    token,
                    documents::PRODUCT_VARIANTS_PAGE,
                    json!({ "id": product_id, "after": cursor }),
                )
                .await?;
            let connection = rest
                .pointer("/product/variants")
                .ok_or_else(|| malformed("variants page"))?;
            variants.extend(
                connection
                    .get("nodes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| malformed("variants page"))?
                    .iter()
                    .cloned(),
            );
            more = connection
                .get("pageInfo")
                .map(cursor_of)
                .unwrap_or_default();
        }
        products.push(product(node, &variants, &currency)?);
    }
    Ok(CatalogPage {
        products,
        next_page: connection
            .get("pageInfo")
            .map(cursor_of)
            .unwrap_or_default(),
    })
}

/// The `endCursor` when `hasNextPage`.
fn cursor_of(page_info: &Value) -> Option<String> {
    if page_info.get("hasNextPage").and_then(Value::as_bool) == Some(true) {
        text(page_info, "endCursor")
    } else {
        None
    }
}

fn product(
    node: &Value,
    variants: &[Value],
    currency: &str,
) -> Result<CatalogProduct, ProviderError> {
    let mut options: Vec<CatalogOption> = node
        .get("options")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|option| {
                    Some(CatalogOption {
                        name: text(option, "name")?,
                        values: option
                            .get("values")
                            .and_then(Value::as_array)
                            .map(|values| {
                                values
                                    .iter()
                                    .filter_map(|value| value.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    // A product with no real options carries Shopify's placeholder
    // "Title: Default Title"; it is one variant, not a choice.
    let placeholder =
        options.len() == 1 && options[0].name == "Title" && options[0].values == ["Default Title"];
    if placeholder {
        options.clear();
    }
    let variants = variants
        .iter()
        .map(|variant| catalog_variant(variant, currency, placeholder))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CatalogProduct {
        provider_product_id: text(node, "id").ok_or_else(|| malformed("product id"))?,
        title: text(node, "title").unwrap_or_default(),
        description: text(node, "description"),
        url: text(node, "onlineStoreUrl"),
        options,
        image_urls: node
            .pointer("/images/nodes")
            .and_then(Value::as_array)
            .map(|images| {
                images
                    .iter()
                    .filter_map(|image| text(image, "url"))
                    .collect()
            })
            .unwrap_or_default(),
        variants,
        categories: categories(node),
        attributes: attributes(node),
    })
}

/// The store's own classification of the product: its taxonomy category's full
/// name and its product type, each once.
fn categories(node: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for name in [
        node.pointer("/category/fullName")
            .and_then(Value::as_str)
            .map(str::to_string),
        text(node, "productType"),
    ]
    .into_iter()
    .flatten()
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Descriptive attributes that do not pick a variant: the vendor.
fn attributes(node: &Value) -> Vec<CatalogOption> {
    text(node, "vendor")
        .map(|vendor| {
            vec![CatalogOption {
                name: "Vendor".to_string(),
                values: vec![vendor],
            }]
        })
        .unwrap_or_default()
}

fn catalog_variant(
    variant: &Value,
    currency: &str,
    placeholder: bool,
) -> Result<CatalogVariant, ProviderError> {
    let price = variant
        .get("price")
        .and_then(Value::as_str)
        .and_then(|price| decimal_to_minor(price, exponent(currency)))
        .ok_or_else(|| malformed("variant price"))?;
    let tracked = variant
        .pointer("/inventoryItem/tracked")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let stock = tracked.then(|| {
        variant
            .get("inventoryQuantity")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .max(0)
    });
    let option_values = if placeholder {
        Vec::new()
    } else {
        variant
            .get("selectedOptions")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|option| {
                        Some(CatalogOptionValue {
                            name: text(option, "name")?,
                            value: text(option, "value")?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    Ok(CatalogVariant {
        provider_variant_id: text(variant, "id").ok_or_else(|| malformed("variant id"))?,
        sku: text(variant, "sku"),
        title: if placeholder {
            None
        } else {
            text(variant, "title")
        },
        option_values,
        price: Price {
            amount_minor: price,
            currency: currency.to_string(),
            exponent: exponent(currency),
        },
        stock,
        image_url: variant
            .pointer("/image/url")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// The variant behind an inventory item and what it can now sell.
pub async fn resolve(
    client: &Client,
    shop: &str,
    token: &str,
    inventory_item_id: &str,
) -> Result<Option<ResolvedStock>, ProviderError> {
    let data = client
        .execute(
            shop,
            token,
            documents::INVENTORY_ITEM,
            json!({ "id": inventory_item_id }),
        )
        .await?;
    let Some(item) = data.get("inventoryItem").filter(|item| !item.is_null()) else {
        return Ok(None);
    };
    let Some(variant) = item.get("variant").filter(|variant| !variant.is_null()) else {
        return Ok(None);
    };
    let tracked = item
        .get("tracked")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(Some(ResolvedStock {
        provider_product_id: variant
            .pointer("/product/id")
            .and_then(Value::as_str)
            .ok_or_else(|| malformed("inventory item"))?
            .to_string(),
        provider_variant_id: text(variant, "id").ok_or_else(|| malformed("inventory item"))?,
        available: tracked.then(|| {
            variant
                .get("inventoryQuantity")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                .max(0)
        }),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(id: &str, price: &str, tracked: bool, quantity: i64) -> Value {
        json!({
            "id": id, "title": "Large", "sku": "BOOT-L", "price": price,
            "selectedOptions": [{"name": "Size", "value": "Large"}],
            "image": null, "inventoryQuantity": quantity,
            "inventoryItem": {"tracked": tracked}
        })
    }

    #[test]
    fn variants_are_normalised_to_minor_units_and_stock() {
        let v = catalog_variant(
            &variant("gid://shopify/ProductVariant/1", "125.50", true, 7),
            "USD",
            false,
        )
        .expect("variant");
        assert_eq!(v.price.amount_minor, 12550);
        assert_eq!(v.price.exponent, 2);
        assert_eq!(v.stock, Some(7));
        assert_eq!(v.option_values[0].name, "Size");
        let yen = catalog_variant(
            &variant("gid://shopify/ProductVariant/1", "1500", true, 3),
            "JPY",
            false,
        )
        .expect("variant");
        assert_eq!((yen.price.amount_minor, yen.price.exponent), (1500, 0));
    }

    #[test]
    fn untracked_inventory_has_no_stock_and_negative_stock_is_zero() {
        let untracked =
            catalog_variant(&variant("gid://x/1", "1.00", false, 9), "USD", false).expect("v");
        assert_eq!(untracked.stock, None);
        let oversold =
            catalog_variant(&variant("gid://x/1", "1.00", true, -4), "USD", false).expect("v");
        assert_eq!(oversold.stock, Some(0));
    }

    #[test]
    fn a_price_that_is_not_a_decimal_is_refused() {
        assert!(catalog_variant(&variant("gid://x/1", "free", true, 1), "USD", false).is_err());
    }

    #[test]
    fn the_default_title_option_is_not_a_choice() {
        let node = json!({
            "id": "gid://shopify/Product/9", "title": "Mug", "description": "",
            "onlineStoreUrl": null,
            "options": [{"name": "Title", "values": ["Default Title"]}],
            "images": {"nodes": []}
        });
        let single = json!({
            "id": "gid://shopify/ProductVariant/2", "title": "Default Title", "sku": "",
            "price": "9.00", "selectedOptions": [{"name": "Title", "value": "Default Title"}],
            "image": null, "inventoryQuantity": 1, "inventoryItem": {"tracked": true}
        });
        let product = product(&node, &[single], "USD").expect("product");
        assert!(product.options.is_empty());
        assert!(product.variants[0].option_values.is_empty());
        assert_eq!(product.variants[0].title, None);
        assert_eq!(product.variants[0].sku, None);
        assert_eq!(product.description, None);
    }

    #[test]
    fn a_cursor_is_returned_only_while_more_pages_remain() {
        assert_eq!(
            cursor_of(&json!({"hasNextPage": true, "endCursor": "abc"})),
            Some("abc".into())
        );
        assert_eq!(
            cursor_of(&json!({"hasNextPage": false, "endCursor": "abc"})),
            None
        );
    }
}
