use serde::{Deserialize, Serialize};

/// Funding rate update from an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Funding {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Funding rate as a string.
    #[serde(rename = "r")]
    pub rate: String,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Open interest update for a symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenInterest {
    pub agent: String,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Open interest quantity.
    #[serde(rename = "oi")]
    pub open_interest: String,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Futures term structure data, typically the basis between spot and futures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermStructure {
    pub agent: String,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Basis value or similar metric.
    #[serde(rename = "b")]
    pub basis: String,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Liquidation event from the derivatives market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidation {
    pub agent: String,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Price at which liquidation occurred.
    #[serde(rename = "p")]
    pub price: String,
    /// Quantity liquidated.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Side of the position being liquidated (BUY/SELL).
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "ts")]
    pub timestamp: i64,
}
