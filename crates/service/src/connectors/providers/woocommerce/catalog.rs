//! WooCommerce REST v3 products into the connector's normalised catalog.
//!
//! Pure functions over the JSON WooCommerce returns, so they are tested
//! against real captured responses. The mapping:
//!
//! * a **simple** product is one variant whose id is the product id;
//! * a **variable** product has one variant per published variation, with the
//!   variation id as the variant id and the variation's attributes as its
//!   option values;
//! * grouped, external, draft, pending and private-draft products are not
//!   sellable through a push and are skipped, as are variations without a
//!   price;
//! * prices are the store's current price (sale price while a sale runs) in
//!   minor units of the store currency;
//! * stock is exact when WooCommerce manages it (the variation's own count, or
//!   the parent's when the variation defers to it), `0` for an unmanaged
//!   product marked out of stock, and `None` (not tracked) otherwise.

use serde_json::Value;

use crate::connectors::provider::{
    CatalogOption, CatalogOptionValue, CatalogProduct, CatalogVariant, Price,
};

use super::money;
use super::text;

const MAX_DESCRIPTION_CHARS: usize = 20_000;
const MAX_IMAGES: usize = 10;

fn string(value: &Value, key: &str) -> String {
    match value.get(key) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        _ => String::new(),
    }
}

pub fn id_of(value: &Value) -> Option<i64> {
    match value.get("id")? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn quantity(value: &Value) -> Option<i64> {
    match value.get("stock_quantity")? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|float| float.floor() as i64)),
        Value::String(text) => text
            .trim()
            .parse::<f64>()
            .ok()
            .map(|float| float.floor() as i64),
        _ => None,
    }
}

/// `true`, `false`, or the string `"parent"` a variation uses to defer to its
/// product.
#[derive(PartialEq, Eq)]
enum Managed {
    Yes,
    No,
    Parent,
}

fn managed(value: &Value) -> Managed {
    match value.get("manage_stock") {
        Some(Value::Bool(true)) => Managed::Yes,
        Some(Value::String(text)) if text == "parent" => Managed::Parent,
        _ => Managed::No,
    }
}

/// The units a product or variation can sell, per the rules in the module
/// documentation. `parent` is the variable product for a variation.
pub fn stock_of(item: &Value, parent: Option<&Value>) -> Option<i64> {
    let exact = |source: &Value| Some(quantity(source).unwrap_or(0).max(0));
    match managed(item) {
        Managed::Yes => return exact(item),
        Managed::Parent => {
            if let Some(parent) = parent {
                if managed(parent) == Managed::Yes {
                    return exact(parent);
                }
            }
        }
        Managed::No => {}
    }
    (string(item, "stock_status") == "outofstock").then_some(0)
}

fn price_of(item: &Value, currency: &str, exponent: i32) -> Option<Price> {
    let amount_minor = money::parse_minor(&string(item, "price"), exponent)?;
    Some(Price {
        amount_minor,
        currency: currency.to_string(),
        exponent,
    })
}

fn image_urls(product: &Value) -> Vec<String> {
    product
        .get("images")
        .and_then(Value::as_array)
        .map(|images| {
            images
                .iter()
                .filter_map(|image| image.get("src")?.as_str())
                .filter(|src| src.starts_with("https://") || src.starts_with("http://"))
                .take(MAX_IMAGES)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn variation_attributes(variation: &Value) -> Vec<CatalogOptionValue> {
    variation
        .get("attributes")
        .and_then(Value::as_array)
        .map(|attributes| {
            attributes
                .iter()
                .filter_map(|attribute| {
                    let name = text::title(attribute.get("name")?.as_str()?);
                    let value = text::title(attribute.get("option")?.as_str()?);
                    (!name.is_empty() && !value.is_empty())
                        .then_some(CatalogOptionValue { name, value })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn variation_options(product: &Value) -> Vec<CatalogOption> {
    product
        .get("attributes")
        .and_then(Value::as_array)
        .map(|attributes| {
            attributes
                .iter()
                .filter(|attribute| {
                    attribute.get("variation").and_then(Value::as_bool) == Some(true)
                })
                .filter_map(|attribute| {
                    let name = text::title(attribute.get("name")?.as_str()?);
                    let values: Vec<String> = attribute
                        .get("options")?
                        .as_array()?
                        .iter()
                        .filter_map(|value| value.as_str().map(text::title))
                        .filter(|value| !value.is_empty())
                        .collect();
                    (!name.is_empty() && !values.is_empty())
                        .then_some(CatalogOption { name, values })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn description(product: &Value) -> Option<String> {
    let full = text::plain_text(&string(product, "description"));
    let chosen = if full.is_empty() {
        text::plain_text(&string(product, "short_description"))
    } else {
        full
    };
    (!chosen.is_empty()).then(|| chosen.chars().take(MAX_DESCRIPTION_CHARS).collect())
}

fn sku(item: &Value) -> Option<String> {
    let sku = string(item, "sku");
    let sku = sku.trim();
    (!sku.is_empty()).then(|| sku.chars().take(100).collect())
}

/// Whether a product listing entry can be offered at all (its variations are
/// fetched only if it can).
pub fn is_sellable_type(product: &Value) -> bool {
    matches!(string(product, "type").as_str(), "simple" | "variable")
        && matches!(string(product, "status").as_str(), "publish" | "private")
}

pub fn is_variable(product: &Value) -> bool {
    string(product, "type") == "variable"
}

/// The normalised product, or `None` when it has nothing sellable (no price,
/// no published variation, not a simple or variable product).
pub fn normalize_product(
    product: &Value,
    variations: &[Value],
    currency: &str,
    exponent: i32,
) -> Option<CatalogProduct> {
    if !is_sellable_type(product) {
        return None;
    }
    let id = id_of(product)?;
    let images = image_urls(product);
    let variants: Vec<CatalogVariant> = if is_variable(product) {
        variations
            .iter()
            .filter(|variation| string(variation, "status") == "publish")
            .filter_map(|variation| {
                let variation_id = id_of(variation)?;
                let price = price_of(variation, currency, exponent)?;
                let option_values = variation_attributes(variation);
                let title = (!option_values.is_empty()).then(|| {
                    option_values
                        .iter()
                        .map(|option| option.value.as_str())
                        .collect::<Vec<_>>()
                        .join(" / ")
                });
                Some(CatalogVariant {
                    provider_variant_id: variation_id.to_string(),
                    sku: sku(variation),
                    title,
                    option_values,
                    price,
                    stock: stock_of(variation, Some(product)),
                    image_url: variation
                        .get("image")
                        .and_then(|image| image.get("src")?.as_str())
                        .filter(|src| src.starts_with("https://") || src.starts_with("http://"))
                        .map(str::to_string),
                })
            })
            .collect()
    } else {
        let price = price_of(product, currency, exponent)?;
        vec![CatalogVariant {
            provider_variant_id: id.to_string(),
            sku: sku(product),
            title: None,
            option_values: Vec::new(),
            price,
            stock: stock_of(product, None),
            image_url: images.first().cloned(),
        }]
    };
    if variants.is_empty() {
        return None;
    }
    let title = text::title(&string(product, "name"));
    Some(CatalogProduct {
        provider_product_id: id.to_string(),
        title: if title.is_empty() {
            format!("Product {id}")
        } else {
            title
        },
        description: description(product),
        url: Some(string(product, "permalink")).filter(|url| url.starts_with("http")),
        options: if is_variable(product) {
            variation_options(product)
        } else {
            Vec::new()
        },
        image_urls: images,
        variants,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn simple() -> Value {
        json!({
            "id": 39, "name": "Pubky Test Item &amp; Co", "type": "simple", "status": "publish",
            "permalink": "https://shop.example/product/pubky-test-item/",
            "description": "<p>A <b>test</b> item.</p>", "short_description": "",
            "sku": "PUBKY-TEST", "price": "12.50", "manage_stock": true, "stock_quantity": 10,
            "stock_status": "instock",
            "images": [{"id": 1, "src": "https://shop.example/a.png"}, {"src": "javascript:alert(1)"}],
            "attributes": [], "variations": []
        })
    }

    #[test]
    fn a_simple_product_is_one_variant_keyed_by_the_product_id() {
        let product = normalize_product(&simple(), &[], "CHF", 2).unwrap();
        assert_eq!(product.provider_product_id, "39");
        assert_eq!(product.title, "Pubky Test Item & Co");
        assert_eq!(product.description.as_deref(), Some("A test item."));
        assert_eq!(product.image_urls, vec!["https://shop.example/a.png"]);
        assert_eq!(product.variants.len(), 1);
        let variant = &product.variants[0];
        assert_eq!(variant.provider_variant_id, "39");
        assert_eq!(variant.price.amount_minor, 1250);
        assert_eq!(variant.price.currency, "CHF");
        assert_eq!(variant.stock, Some(10));
        assert_eq!(variant.sku.as_deref(), Some("PUBKY-TEST"));
        assert_eq!(
            variant.image_url.as_deref(),
            Some("https://shop.example/a.png")
        );
    }

    #[test]
    fn stock_follows_what_woocommerce_manages() {
        let mut item = simple();
        item["stock_quantity"] = json!(-3);
        assert_eq!(
            stock_of(&item, None),
            Some(0),
            "backorders never go below zero"
        );
        item["manage_stock"] = json!(false);
        assert_eq!(
            stock_of(&item, None),
            None,
            "unmanaged and in stock: not tracked"
        );
        item["stock_status"] = json!("outofstock");
        assert_eq!(stock_of(&item, None), Some(0));
        item["stock_status"] = json!("onbackorder");
        assert_eq!(stock_of(&item, None), None);

        let parent = json!({"manage_stock": true, "stock_quantity": 7});
        let deferring =
            json!({"manage_stock": "parent", "stock_quantity": null, "stock_status": "instock"});
        assert_eq!(stock_of(&deferring, Some(&parent)), Some(7));
        let unmanaged_parent = json!({"manage_stock": false});
        assert_eq!(stock_of(&deferring, Some(&unmanaged_parent)), None);
    }

    #[test]
    fn a_variable_product_has_a_variant_per_priced_published_variation() {
        let product = json!({
            "id": 35, "name": "Little Hodler Socks", "type": "variable", "status": "publish",
            "permalink": "https://shop.example/product/socks/",
            "description": "", "short_description": "<p>Warm.</p>",
            "attributes": [
                {"name": "Size", "variation": true, "options": ["S", "M", "L"]},
                {"name": "Material", "variation": false, "options": ["Wool"]}
            ],
            "images": [{"src": "https://shop.example/socks.png"}]
        });
        let variations = vec![
            json!({"id": 36, "status": "publish", "sku": "S", "price": "19.90",
                   "manage_stock": true, "stock_quantity": 5,
                   "attributes": [{"name": "Size", "option": "S"}],
                   "image": {"src": "https://shop.example/socks-s.png"}}),
            json!({"id": 38, "status": "publish", "sku": "", "price": "21.50",
                   "manage_stock": true, "stock_quantity": 0,
                   "attributes": [{"name": "Size", "option": "L"}]}),
            json!({"id": 99, "status": "publish", "price": "",
                   "attributes": [{"name": "Size", "option": "XL"}]}),
            json!({"id": 98, "status": "private", "price": "5.00",
                   "attributes": [{"name": "Size", "option": "XS"}]}),
        ];
        let normalized = normalize_product(&product, &variations, "CHF", 2).unwrap();
        assert_eq!(normalized.description.as_deref(), Some("Warm."));
        assert_eq!(normalized.options.len(), 1);
        assert_eq!(normalized.options[0].name, "Size");
        assert_eq!(normalized.options[0].values, vec!["S", "M", "L"]);
        let ids: Vec<&str> = normalized
            .variants
            .iter()
            .map(|variant| variant.provider_variant_id.as_str())
            .collect();
        assert_eq!(ids, ["36", "38"]);
        assert_eq!(normalized.variants[0].title.as_deref(), Some("S"));
        assert_eq!(normalized.variants[0].option_values[0].name, "Size");
        assert_eq!(normalized.variants[0].stock, Some(5));
        assert_eq!(normalized.variants[1].stock, Some(0));
        assert_eq!(normalized.variants[1].sku, None);
        assert_eq!(normalized.variants[1].image_url, None);
    }

    #[test]
    fn what_cannot_be_sold_is_skipped() {
        for (kind, status) in [
            ("grouped", "publish"),
            ("external", "publish"),
            ("simple", "draft"),
            ("simple", "pending"),
        ] {
            let mut product = simple();
            product["type"] = json!(kind);
            product["status"] = json!(status);
            assert!(
                normalize_product(&product, &[], "CHF", 2).is_none(),
                "{kind} {status}"
            );
        }
        let mut unpriced = simple();
        unpriced["price"] = json!("");
        assert!(normalize_product(&unpriced, &[], "CHF", 2).is_none());
        let mut variable = simple();
        variable["type"] = json!("variable");
        assert!(
            normalize_product(&variable, &[], "CHF", 2).is_none(),
            "no variations"
        );
    }

    #[test]
    fn zero_decimal_currencies_keep_whole_units() {
        let mut product = simple();
        product["price"] = json!("1200");
        let normalized = normalize_product(&product, &[], "JPY", 0).unwrap();
        assert_eq!(normalized.variants[0].price.amount_minor, 1200);
        assert_eq!(normalized.variants[0].price.exponent, 0);
    }
}
