/// Utilities for canonicalizing exchange-specific trading pairs.
///
/// The `CanonicalService` converts symbols from supported exchanges into a
/// standard `BASE-QUOTE` format in uppercase. Binance symbols such as
/// `btcusdt` are converted to `BTC-USDT`, while Coinbase symbols already in
/// `BASE-QUOTE` form are normalized to uppercase.
///
/// Additional exchanges can be supported by extending `canonical_pair`.

pub struct CanonicalService;

impl CanonicalService {
    /// Convert `pair` as used by `exchange` into the canonical `BASE-QUOTE`
    /// representation. Returns `None` if the exchange is unknown or the pair
    /// cannot be parsed.
    pub fn canonical_pair(exchange: &str, pair: &str) -> Option<String> {
        match exchange.to_lowercase().as_str() {
            "binance" => Self::canonicalize_binance(pair),
            "coinbase" => Some(Self::canonicalize_coinbase(pair)),
            _ => None,
        }
    }

    fn canonicalize_binance(symbol: &str) -> Option<String> {
        let lower = symbol.to_lowercase();
        // Common quote assets on Binance US.
        const QUOTES: [&str; 7] = ["usdt", "usdc", "busd", "usd", "btc", "eth", "bnb"];
        for q in QUOTES {
            if lower.ends_with(q) {
                let base = &lower[..lower.len() - q.len()];
                if base.is_empty() {
                    return None;
                }
                return Some(format!("{}-{}", base.to_uppercase(), q.to_uppercase()));
            }
        }
        None
    }

    fn canonicalize_coinbase(symbol: &str) -> String {
        let lower = symbol.to_lowercase().replace('_', "-");

        if let Some((base, quote)) = lower.split_once('-') {
            return format!("{}-{}", base.to_uppercase(), quote.to_uppercase());
        }

        // Attempt to detect a known quote asset when no separator is present.
        const QUOTES: [&str; 6] = ["usdt", "usdc", "usd", "btc", "eth", "eur"];
        for q in QUOTES {
            if lower.ends_with(q) {
                let base = &lower[..lower.len() - q.len()];
                if !base.is_empty() {
                    return format!("{}-{}", base.to_uppercase(), q.to_uppercase());
                }
            }
        }

        lower.to_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalService;

    #[test]
    fn binance_pairs_are_canonicalized() {
        assert_eq!(
            CanonicalService::canonical_pair("binance", "btcusdt"),
            Some("BTC-USDT".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("binance", "ethbtc"),
            Some("ETH-BTC".to_string())
        );
    }

    #[test]
    fn coinbase_pairs_are_canonicalized() {
        assert_eq!(
            CanonicalService::canonical_pair("coinbase", "btc-usd"),
            Some("BTC-USD".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("coinbase", "ETH-USD"),
            Some("ETH-USD".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("coinbase", "btc_usd"),
            Some("BTC-USD".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("coinbase", "btcusd"),
            Some("BTC-USD".to_string())
        );
    }

    #[test]
    fn unknown_exchange_returns_none() {
        assert_eq!(CanonicalService::canonical_pair("kraken", "btcusd"), None);
    }
}
