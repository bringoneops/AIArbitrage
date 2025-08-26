//! Utilities for canonicalizing exchange-specific trading pairs.
//!
//! The [`CanonicalService`] converts symbols from supported exchanges into a
//! standard `BASE-QUOTE` format in uppercase. Binance symbols such as
//! `btcusdt` are converted to `BTC-USDT`, while Coinbase symbols already in
//! `BASE-QUOTE` form are normalized to uppercase.
//!
//! ## SSL Certificate Verification
//!
//! Requests to Binance's `exchangeInfo` endpoint use an HTTP client built by
//! [`http_client::builder`]. Certificate verification is enabled by default.
//! To accept invalid (e.g., self-signed) certificates during development, set
//! the `BINANCE_ACCEPT_INVALID_CERTS` environment variable to a truthy value
//! (`1`, `true`, `yes`). Disabling certificate verification is strongly
//! discouraged for production use.
//!
//! Additional exchanges can be supported by extending
//! [`CanonicalService::canonical_pair`].

pub mod events;
mod http_client;
pub mod events;

pub use events::{OptionChain, OptionGreeks, OptionQuote};
pub mod onchain;

use std::collections::HashSet;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use tracing::warn;
use serde::{Serialize, Deserialize};

/// Canonicalized candle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Candle interval (e.g., `1m`).
    #[serde(rename = "i")]
    pub interval: String,
    /// Open price.
    #[serde(rename = "o")]
    pub open: String,
    /// High price.
    #[serde(rename = "h")]
    pub high: String,
    /// Low price.
    #[serde(rename = "l")]
    pub low: String,
    /// Close price.
    #[serde(rename = "c")]
    pub close: String,
    /// Traded volume.
    #[serde(rename = "v")]
    pub volume: String,
    /// Candle close timestamp (ms).
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Canonical 24h ticker event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Last traded price.
    #[serde(rename = "p")]
    pub price: String,
    /// 24h volume.
    #[serde(rename = "v")]
    pub volume: String,
    /// Event timestamp (ms).
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

pub struct CanonicalService;

/// Cached list of Binance quote assets. Populated at startup via [`init`].
static BINANCE_QUOTES: OnceLock<Vec<String>> = OnceLock::new();

impl CanonicalService {
    /// Initialise any resources required by the service. Currently this loads
    /// the list of Binance quote assets from the public `exchangeInfo` endpoint
    /// (unless provided via the `BINANCE_QUOTES` environment variable).
    ///
    /// Network errors are logged and fall back to a small built-in list.
    pub async fn init() {
        if BINANCE_QUOTES.get().is_some() {
            return;
        }

        if let Ok(env) = std::env::var("BINANCE_QUOTES") {
            let quotes = Self::parse_env_quotes(&env);
            let _ = BINANCE_QUOTES.set(quotes);
            return;
        }

        match Self::fetch_binance_quotes().await {
            Ok(quotes) if !quotes.is_empty() => {
                let _ = BINANCE_QUOTES.set(quotes);
            }
            Ok(_) => {
                let _ = BINANCE_QUOTES.set(Self::default_binance_quotes());
            }
            Err(e) => {
                warn!("failed to fetch Binance quotes: {}", e);
                let _ = BINANCE_QUOTES.set(Self::default_binance_quotes());
            }
        }
    }

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

    fn binance_quotes() -> &'static Vec<String> {
        BINANCE_QUOTES.get_or_init(Self::default_binance_quotes)
    }

    async fn fetch_binance_quotes() -> Result<Vec<String>, reqwest::Error> {
        let client = http_client::builder().build()?;
        let v: serde_json::Value = client
            .get("https://api.binance.us/api/v3/exchangeInfo")
            .send()
            .await?
            .json()
            .await?;
        let mut set = HashSet::new();
        if let Some(symbols) = v.get("symbols").and_then(|s| s.as_array()) {
            for sym in symbols {
                if let Some(q) = sym.get("quoteAsset").and_then(|q| q.as_str()) {
                    set.insert(q.to_lowercase());
                }
            }
        }
        let mut quotes: Vec<String> = set.into_iter().collect();
        quotes.sort_by(|a, b| b.len().cmp(&a.len()));
        Ok(quotes)
    }

    fn parse_env_quotes(env: &str) -> Vec<String> {
        let mut quotes: Vec<String> = env
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        quotes.sort_by(|a, b| b.len().cmp(&a.len()));
        quotes
    }

    fn default_binance_quotes() -> Vec<String> {
        const DEFAULT: [&str; 7] = ["usdt", "usdc", "busd", "usd", "btc", "eth", "bnb"];
        let mut quotes: Vec<String> = DEFAULT.iter().map(|q| q.to_string()).collect();
        quotes.sort_by(|a, b| b.len().cmp(&a.len()));
        quotes
    }

    fn canonicalize_binance(symbol: &str) -> Option<String> {
        let lower = symbol.to_lowercase();
        for q in Self::binance_quotes() {
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

    #[cfg(test)]
    pub fn set_binance_quotes(quotes: Vec<&str>) {
        let mut qs: Vec<String> = quotes.into_iter().map(|s| s.to_lowercase()).collect();
        qs.sort_by(|a, b| b.len().cmp(&a.len()));
        let _ = BINANCE_QUOTES.set(qs);
    }
}

/// Canonical representation of an incremental level-2 order book update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Diff {
    pub agent: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "s")]
    pub symbol: String,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

impl L2Diff {
    pub fn new(agent: &str, symbol: &str, bids: Vec<[String; 2]>, asks: Vec<[String; 2]>, ts: i64) -> Self {
        let sym = CanonicalService::canonical_pair(agent, symbol).unwrap_or_else(|| symbol.to_string());
        Self {
            agent: agent.to_string(),
            event_type: "l2_diff".to_string(),
            symbol: sym,
            bids,
            asks,
            timestamp: ts,
        }
    }

    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Canonical representation of a full order book snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub agent: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "s")]
    pub symbol: String,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

impl Snapshot {
    pub fn new(agent: &str, symbol: &str, bids: Vec<[String; 2]>, asks: Vec<[String; 2]>, ts: i64) -> Self {
        let sym = CanonicalService::canonical_pair(agent, symbol).unwrap_or_else(|| symbol.to_string());
        Self {
            agent: agent.to_string(),
            event_type: "snapshot".to_string(),
            symbol: sym,
            bids,
            asks,
            timestamp: ts,
        }
    }

    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalService;
    use std::sync::Once;

    fn setup() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            CanonicalService::set_binance_quotes(vec!["usdt", "btc", "eth"]);
        });
    }

    #[test]
    fn binance_pairs_are_canonicalized() {
        setup();
        assert_eq!(
            CanonicalService::canonical_pair("binance", "btcusdt"),
            Some("BTC-USDT".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("binance", "ethbtc"),
            Some("ETH-BTC".to_string())
        );
        assert_eq!(
            CanonicalService::canonical_pair("binance", "bnbeth"),
            Some("BNB-ETH".to_string())
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
