//! Decimal prices as WooCommerce writes them (`"19.90"`) to and from minor
//! units, without floating point.

/// ISO 4217 minor-unit exponent. The currencies Pubky lists in are JPY (0)
/// and 2-decimal ones; the rest follow ISO so a store in another currency is
/// read correctly even though the Shop will refuse to list it.
pub fn exponent_for(currency: &str) -> i32 {
    match currency.to_ascii_uppercase().as_str() {
        "BIF" | "CLP" | "DJF" | "GNF" | "ISK" | "JPY" | "KMF" | "KRW" | "PYG" | "RWF" | "UGX"
        | "UYI" | "VND" | "VUV" | "XAF" | "XOF" | "XPF" => 0,
        "BHD" | "IQD" | "JOD" | "KWD" | "LYD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

/// `"19.90"` with exponent 2 is `1990`. More decimals than the exponent round
/// half up. Refuses negatives, blanks, signs, exponents and anything that is
/// not plain digits with at most one point.
pub fn parse_minor(text: &str, exponent: i32) -> Option<i64> {
    let text = text.trim();
    if text.is_empty() || exponent < 0 {
        return None;
    }
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (text, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let exponent = exponent as usize;
    let mut digits = String::from(if whole.is_empty() { "0" } else { whole });
    let kept: String = fraction.chars().take(exponent).collect();
    digits.push_str(&kept);
    for _ in kept.len()..exponent {
        digits.push('0');
    }
    let mut value: i64 = digits.parse().ok()?;
    let round_up = fraction
        .chars()
        .nth(exponent)
        .is_some_and(|digit| digit >= '5');
    if round_up {
        value = value.checked_add(1)?;
    }
    Some(value)
}

/// `1990` with exponent 2 is `"19.90"`; negative amounts keep their sign.
pub fn format_minor(minor: i64, exponent: i32) -> String {
    let exponent = exponent.max(0) as u32;
    let sign = if minor < 0 { "-" } else { "" };
    let magnitude = minor.unsigned_abs();
    if exponent == 0 {
        return format!("{sign}{magnitude}");
    }
    let divisor = 10u64.pow(exponent);
    format!(
        "{sign}{}.{:0width$}",
        magnitude / divisor,
        magnitude % divisor,
        width = exponent as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prices_into_minor_units() {
        assert_eq!(parse_minor("19.90", 2), Some(1990));
        assert_eq!(parse_minor("19.9", 2), Some(1990));
        assert_eq!(parse_minor("18", 2), Some(1800));
        assert_eq!(parse_minor("0.05", 2), Some(5));
        assert_eq!(parse_minor(".5", 2), Some(50));
        assert_eq!(parse_minor("1200", 0), Some(1200));
        assert_eq!(parse_minor("12.345", 3), Some(12_345));
    }

    #[test]
    fn rounds_half_up_beyond_the_exponent() {
        assert_eq!(parse_minor("19.995", 2), Some(2000));
        assert_eq!(parse_minor("19.994", 2), Some(1999));
        assert_eq!(parse_minor("1199.5", 0), Some(1200));
    }

    #[test]
    fn refuses_what_is_not_a_plain_price() {
        for bad in ["", " ", "-1.00", "+1", "1e3", "1,50", "abc", "1.2.3", "."] {
            assert_eq!(parse_minor(bad, 2), None, "{bad:?}");
        }
        assert_eq!(parse_minor("99999999999999999999.00", 2), None);
    }

    #[test]
    fn formats_minor_units_for_woocommerce() {
        assert_eq!(format_minor(1990, 2), "19.90");
        assert_eq!(format_minor(5, 2), "0.05");
        assert_eq!(format_minor(-250, 2), "-2.50");
        assert_eq!(format_minor(1200, 0), "1200");
        assert_eq!(format_minor(0, 2), "0.00");
        assert_eq!(format_minor(12_345, 3), "12.345");
    }

    #[test]
    fn exponents_follow_iso_4217() {
        assert_eq!(exponent_for("CHF"), 2);
        assert_eq!(exponent_for("jpy"), 0);
        assert_eq!(exponent_for("KWD"), 3);
        assert_eq!(exponent_for("EUR"), 2);
    }
}
