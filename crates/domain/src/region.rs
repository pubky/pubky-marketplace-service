//! Countries the legal launch plan fences on and the seller declaration
//! vocabulary.
//!
//! [`EU_27`] is the member states of the European Union as ISO 3166-1
//! alpha-2 codes (Greece is `GR`, not the VAT prefix `EL`). The United
//! Kingdom, Switzerland, Norway and the other EEA or neighbouring states are
//! not in it.
//!
//! Two rules use different sets:
//!
//! - **C3, the private-seller rule** ([`is_private_seller_only_country`]):
//!   the EU-27 plus [`GB`]. A buyer in one of these countries buys only from
//!   a seller who declared they are private (UK consumers buying from a
//!   business have a 14-day withdrawal right under the Consumer Contracts
//!   Regulations 2013 that a "final sale" label cannot remove).
//! - **C4, the DAC7 guard** ([`is_eu_country`]): the EU-27 only, because
//!   DAC7 is an EU directive. The annual limit, the re-confirmation point and
//!   the count behind it never include GB.

use serde_json::Value;

/// The 27 EU member states, ISO 3166-1 alpha-2, sorted.
pub const EU_27: [&str; 27] = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IT",
    "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
];

/// The United Kingdom, ISO 3166-1 alpha-2.
pub const GB: &str = "GB";

/// Whether `country_code` (uppercase ISO 3166-1 alpha-2) is an EU member.
pub fn is_eu_country(country_code: &str) -> bool {
    EU_27.binary_search(&country_code).is_ok()
}

/// Whether a buyer in `country_code` (uppercase ISO 3166-1 alpha-2) can buy
/// only from a seller who declared they are private (C3): the EU-27 and GB.
/// This is the one definition of that set; C4 and DAC7 use
/// [`is_eu_country`].
pub fn is_private_seller_only_country(country_code: &str) -> bool {
    is_eu_country(country_code) || country_code == GB
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
/// trimmed and uppercased, when it is then an ISO 3166-1 alpha-2 code. A
/// seller who typed `de` or ` DE ` is resident in Germany all the same, so the
/// annual cap must not skip them.
pub fn shop_country_from_record(record: &Value) -> Option<String> {
    let code = record
        .get("location")?
        .get("countryCode")?
        .as_str()?
        .trim()
        .to_ascii_uppercase();
    (code.len() == 2 && code.bytes().all(|byte| byte.is_ascii_uppercase())).then_some(code)
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
    fn the_private_seller_rule_covers_the_eu_27_and_gb_only() {
        for code in EU_27.iter().copied().chain([GB]) {
            assert!(is_private_seller_only_country(code), "{code}");
        }
        for code in ["CH", "NO", "IS", "LI", "US", "EL", "UK", "gb", "de", ""] {
            assert!(!is_private_seller_only_country(code), "{code}");
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
    fn the_shop_country_is_trimmed_and_uppercased() {
        for (written, expected) in [("DE", "DE"), ("de", "DE"), (" DE ", "DE"), ("\tch\n", "CH")] {
            assert_eq!(
                shop_country_from_record(&json!({ "location": { "countryCode": written } })),
                Some(expected.to_string()),
                "{written:?}"
            );
        }
    }

    #[test]
    fn the_shop_country_must_be_a_two_letter_code() {
        assert_eq!(
            shop_country_from_record(&json!({ "location": { "countryCode": "DE" } })),
            Some("DE".to_string())
        );
        for record in [
            json!({}),
            json!({ "location": {} }),
            json!({ "location": { "countryCode": "DEU" } }),
            json!({ "location": { "countryCode": "" } }),
            json!({ "location": { "countryCode": "D E" } }),
            json!({ "location": { "countryCode": 5 } }),
        ] {
            assert_eq!(shop_country_from_record(&record), None, "{record}");
        }
    }
}
