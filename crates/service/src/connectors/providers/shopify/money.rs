//! Shopify amounts are decimal strings in a currency's major units
//! (`"125.00"`, `"1500"` for yen); Pubky amounts are integers in minor units.

/// Decimal places of an ISO 4217 currency's minor unit.
pub fn exponent(currency: &str) -> i32 {
    match currency.to_ascii_uppercase().as_str() {
        "BIF" | "CLP" | "DJF" | "GNF" | "ISK" | "JPY" | "KMF" | "KRW" | "PYG" | "RWF" | "UGX"
        | "UYI" | "VND" | "VUV" | "XAF" | "XOF" | "XPF" => 0,
        "BHD" | "IQD" | "JOD" | "KWD" | "LYD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

/// `12500` at exponent 2 is `"125.00"`; at exponent 0 it is `"12500"`.
pub fn minor_to_decimal(minor: i64, exponent: i32) -> String {
    let exponent = exponent.max(0) as u32;
    let negative = minor < 0;
    let digits = minor.unsigned_abs().to_string();
    let body = if exponent == 0 {
        digits
    } else {
        let padded = format!("{digits:0>width$}", width = exponent as usize + 1);
        let (whole, fraction) = padded.split_at(padded.len() - exponent as usize);
        format!("{whole}.{fraction}")
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}

/// `"125.00"` at exponent 2 is `12500`. Digits beyond the exponent are
/// rounded half up. `None` for anything that is not a plain decimal number.
pub fn decimal_to_minor(text: &str, exponent: i32) -> Option<i64> {
    let text = text.trim();
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let exponent = exponent.max(0) as usize;
    let mut kept: String = fraction.chars().take(exponent).collect();
    while kept.len() < exponent {
        kept.push('0');
    }
    let round_up = fraction
        .as_bytes()
        .get(exponent)
        .is_some_and(|digit| *digit >= b'5');
    let combined = format!("{}{}", if whole.is_empty() { "0" } else { whole }, kept);
    let mut value: i64 = combined.parse().ok()?;
    if round_up {
        value = value.checked_add(1)?;
    }
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minor_units_become_decimal_strings() {
        assert_eq!(minor_to_decimal(12_500, 2), "125.00");
        assert_eq!(minor_to_decimal(5, 2), "0.05");
        assert_eq!(minor_to_decimal(0, 2), "0.00");
        assert_eq!(minor_to_decimal(1_500, 0), "1500");
        assert_eq!(minor_to_decimal(1_234, 3), "1.234");
        assert_eq!(minor_to_decimal(-250, 2), "-2.50");
    }

    #[test]
    fn decimal_strings_become_minor_units() {
        assert_eq!(decimal_to_minor("125.00", 2), Some(12_500));
        assert_eq!(decimal_to_minor("125", 2), Some(12_500));
        assert_eq!(decimal_to_minor("0.05", 2), Some(5));
        assert_eq!(decimal_to_minor("1500", 0), Some(1_500));
        assert_eq!(decimal_to_minor("1.005", 2), Some(101));
        assert_eq!(decimal_to_minor(".5", 2), Some(50));
        assert_eq!(decimal_to_minor("-2.50", 2), Some(-250));
        for bad in ["", "abc", "1.2.3", "1e5", "$5", "--1"] {
            assert_eq!(decimal_to_minor(bad, 2), None, "{bad}");
        }
    }

    #[test]
    fn exponents_follow_the_currency() {
        assert_eq!(exponent("JPY"), 0);
        assert_eq!(exponent("jpy"), 0);
        assert_eq!(exponent("KWD"), 3);
        for code in ["USD", "EUR", "CHF", "GBP", "CAD", "AUD"] {
            assert_eq!(exponent(code), 2);
        }
    }
}
