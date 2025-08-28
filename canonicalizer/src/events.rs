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

/// Candlestick bar (open-high-low-close-volume) for a trading pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    /// Source exchange name.
    pub agent: String,
    /// Event type, always `"ohlcv"`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Bar interval in seconds.
    #[serde(rename = "i")]
    pub interval: u64,
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
    /// Traded volume during the interval.
    #[serde(rename = "v")]
    pub volume: String,
    /// Start timestamp of the bar in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}
/// Greeks associated with an option contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptionGreeks {
    /// Delta of the option.
    pub delta: Option<f64>,
    /// Gamma of the option.
    pub gamma: Option<f64>,
    /// Theta of the option.
    pub theta: Option<f64>,
    /// Vega of the option.
    pub vega: Option<f64>,
}

/// Quoted data for a single option contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptionQuote {
    /// Strike price of the contract.
    pub strike: f64,
    /// Contract type: "CALL" or "PUT".
    pub kind: String,
    /// Bid price.
    pub bid: Option<f64>,
    /// Ask price.
    pub ask: Option<f64>,
    /// Last traded price.
    pub last: Option<f64>,
    /// Implied volatility as a ratio (e.g. 0.55 == 55%).
    pub iv: Option<f64>,
    /// Associated greeks for this option.
    pub greeks: Option<OptionGreeks>,
}

/// Point on an implied volatility surface (strike \times expiry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptionSurfacePoint {
    /// Strike price for the quote.
    pub strike: f64,
    /// Expiration timestamp associated with this point.
    pub expiry: i64,
    /// Implied volatility value.
    pub iv: f64,
}

/// Normalised representation of an option chain for a single expiry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptionChain {
    /// Source agent or exchange.
    pub agent: String,
    /// Event type, always `"option_chain"` for this structure.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Canonical underlying symbol (e.g. `BTC-USDT`).
    pub s: String,
    /// Expiration timestamp (seconds since Unix epoch).
    pub expiry: i64,
    /// Collection of option quotes at this expiry.
    pub options: Vec<OptionQuote>,
    /// Implied volatility surface points for this chain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub surface: Vec<OptionSurfacePoint>,
}

/// Order update representing state changes on an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Exchange-assigned order identifier.
    #[serde(rename = "id")]
    pub order_id: String,
    /// Side of the order, e.g. BUY or SELL.
    #[serde(rename = "side")]
    pub side: String,
    /// Current status of the order.
    #[serde(rename = "st")]
    pub status: String,
    /// Order price as a string.
    #[serde(rename = "p")]
    pub price: String,
    /// Order quantity as a string.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Fill event associated with an order execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Exchange-assigned order identifier.
    #[serde(rename = "oid")]
    pub order_id: String,
    /// Exchange-assigned trade identifier.
    #[serde(rename = "tid")]
    pub trade_id: String,
    /// Fill price as a string.
    #[serde(rename = "p")]
    pub price: String,
    /// Fill quantity as a string.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Position or balance update for an asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Source exchange name.
    pub agent: String,
    /// Asset or canonical symbol associated with the position.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Free balance quantity.
    #[serde(rename = "f")]
    pub free: String,
    /// Locked or reserved quantity.
    #[serde(rename = "l")]
    pub locked: String,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_chain_serialises() {
        let chain = OptionChain {
            agent: "binance".into(),
            r#type: "option_chain".into(),
            s: "BTC-USD".into(),
            expiry: 1_700_000_000,
            options: vec![OptionQuote {
                strike: 30000.0,
                kind: "CALL".into(),
                bid: Some(10.0),
                ask: Some(11.0),
                last: Some(10.5),
                iv: Some(0.55),
                greeks: Some(OptionGreeks {
                    delta: Some(0.5),
                    gamma: Some(0.1),
                    theta: Some(-0.01),
                    vega: Some(0.2),
                }),
            }],
            surface: vec![OptionSurfacePoint {
                strike: 30000.0,
                expiry: 1_700_000_000,
                iv: 0.55,
            }],
        };

        let json = serde_json::to_string(&chain).expect("serialize");
        let back: OptionChain = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, chain);
    }
}
