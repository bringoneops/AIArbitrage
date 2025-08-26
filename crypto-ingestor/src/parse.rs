use rust_decimal::Decimal;

/// Parse a decimal string into a normalized representation.
///
/// The value is rounded to 28 decimal places and trailing zeros are removed.
pub fn parse_decimal_str(s: &str) -> Option<String> {
    s.parse::<Decimal>()
        .ok()
        .map(|d| d.round_dp(28).normalize().to_string())
}
