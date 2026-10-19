//! Countries the legal launch plan fences on (EU-27) and the seller
//! declaration vocabulary.
//!
//! The list is the member states of the European Union as ISO 3166-1
//! alpha-2 codes (Greece is `GR`, not the VAT prefix `EL`). The United
//! Kingdom, Switzerland, Norway and the other EEA or neighbouring states are
//! not in it.

use serde_json::Value;

/// The 27 EU member states, ISO 3166-1 alpha-2, sorted.
pub const EU_27: [&str; 27] = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IT",
    "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
];

/// Whether `country_code` (uppercase ISO 3166-1 alpha-2) is an EU member.
pub fn is_eu_country(country_code: &str) -> bool {
    EU_27.binary_search(&country_code).is_ok()
}

/// What a seller declared about themselves in their own shop record
/// (`sellerStatus`). Synonym only reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellerStatus {
    /// The seller declared they sell as a private individual.
    Private,
    /// The seller declared they sell as a business (trader).
    Trader,
    /// No usable declaration: the field is absent or holds anything else.
    /// Treated as a business seller wherever the status matters.
    Undeclared,
}

impl SellerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SellerStatus::Private => "private",
            SellerStatus::Trader => "trader",
            SellerStatus::Undeclared => "undeclared",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "private" => Some(SellerStatus::Private),
            "trader" => Some(SellerStatus::Trader),
            "undeclared" => Some(SellerStatus::Undeclared),
            _ => None,
        }
    }

    /// Reads `sellerStatus` from a shop record; anything but the two
    /// published values reads as [`SellerStatus::Undeclared`].
    pub fn from_shop_record(record: &Value) -> Self {
        match record.get("sellerStatus").and_then(Value::as_str) {
            Some("private") => SellerStatus::Private,
            Some("trader") => SellerStatus::Trader,
            _ => SellerStatus::Undeclared,
        }
    }

    /// Whether the seller sells as a business for the fence: a declared
    /// trader or no declaration.
    pub fn is_business(self) -> bool {
        !matches!(self, SellerStatus::Private)
    }
}

/// The seller's shop country: `location.countryCode` of the shop record,
/// when it is an uppercase ISO 3166-1 alpha-2 code.
pub fn shop_country_from_record(record: &Value) -> Option<String> {
    let code = record.get("location")?.get("countryCode")?.as_str()?;
    (code.len() == 2 && code.bytes().all(|byte| byte.is_ascii_uppercase()))
        .then(|| code.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_eu_list_is_sorted_and_has_27_members() {
        let mut sorted = EU_27;
        sorted.sort_unstable();
        assert_eq!(sorted, EU_27);
        assert_eq!(EU_27.len(), 27);
    }

    #[test]
    fn members_and_non_members() {
        for code in ["DE", "FR", "GR", "IE", "SE", "CY", "MT"] {
            assert!(is_eu_country(code), "{code}");
        }
        for code in ["GB", "CH", "NO", "IS", "LI", "US", "EL", "UK", "de", ""] {
            assert!(!is_eu_country(code), "{code}");
        }
    }

    #[test]
    fn a_shop_record_declares_a_status_or_nothing() {
        assert_eq!(
            SellerStatus::from_shop_record(&json!({ "sellerStatus": "private" })),
            SellerStatus::Private
        );
        assert_eq!(
            SellerStatus::from_shop_record(&json!({ "sellerStatus": "trader" })),
            SellerStatus::Trader
        );
        for record in [
            json!({}),
            json!({ "sellerStatus": "Private" }),
            json!({ "sellerStatus": "business" }),
            json!({ "sellerStatus": 1 }),
            json!({ "sellerStatus": null }),
            json!([]),
        ] {
            assert_eq!(
                SellerStatus::from_shop_record(&record),
                SellerStatus::Undeclared,
                "{record}"
            );
        }
        assert!(SellerStatus::Trader.is_business());
        assert!(SellerStatus::Undeclared.is_business());
        assert!(!SellerStatus::Private.is_business());
    }

    #[test]
    fn the_shop_country_must_be_an_uppercase_code() {
        assert_eq!(
            shop_country_from_record(&json!({ "location": { "countryCode": "DE" } })),
            Some("DE".to_string())
        );
        for record in [
            json!({}),
            json!({ "location": {} }),
            json!({ "location": { "countryCode": "de" } }),
            json!({ "location": { "countryCode": "DEU" } }),
            json!({ "location": { "countryCode": 5 } }),
        ] {
            assert_eq!(shop_country_from_record(&record), None, "{record}");
        }
    }
}
