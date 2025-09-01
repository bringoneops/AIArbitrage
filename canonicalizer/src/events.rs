use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

fn ser_uppercase<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_uppercase())
}

fn de_uppercase<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.to_uppercase())
}

/// Canonical `BASE-QUOTE` symbol broken into base and quote components.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
}

/// Side of the trade.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
    Unknown,
}

/// Event type describing the origin of the message.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Trade,
    BookUpdate,
    Ticker,
    Heartbeat,
}

/// Fee information associated with a trade.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fee {
    pub asset: String,
    pub amount: String,
}

/// Additional metadata for a trade event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeMeta {
    pub maker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fees: Vec<Fee>,
    #[serde(flatten, default)]
    pub extra: HashMap<String, Value>,
}

/// Normalised representation of a trade across exchanges.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trade {
    #[serde(deserialize_with = "de_uppercase", serialize_with = "ser_uppercase")]
    pub exchange: String,
    pub symbol: Symbol,
    pub trade_id: Option<String>,
    pub price: String,
    pub quantity: String,
    pub side: Side,
    pub timestamp: DateTime<Utc>,
    pub timestamp_ms: i64,
    pub event_type: EventType,
    pub agent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TradeMeta>,
    #[serde(flatten, default)]
    pub extra: HashMap<String, Value>,
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

/// Listing information for a tradable symbol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Listing {
    /// Source exchange name.
    pub agent: String,
    /// Event type, always `"listing"`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Base asset of the market.
    pub base: String,
    /// Quote asset of the market.
    pub quote: String,
    /// Lot size or quantity increment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lot_size: Option<String>,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// Fee tier information for a market or exchange.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeeTier {
    /// Volume threshold for this tier.
    pub volume: f64,
    /// Maker fee rate (e.g. 0.001 for 0.1%).
    pub maker: f64,
    /// Taker fee rate.
    pub taker: f64,
}

/// Fee schedule describing maker/taker fees across tiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeeSchedule {
    /// Source exchange name.
    pub agent: String,
    /// Event type, always `"fee_schedule"`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Optional symbol this schedule applies to.
    #[serde(rename = "s", skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Ordered fee tiers.
    pub tiers: Vec<FeeTier>,
    /// Event timestamp in milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;
    use std::collections::HashMap;

    #[test]
    fn trade_serialises_and_deserialises() {
        let ts = chrono::Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let trade = Trade {
            exchange: "binance".into(),
            symbol: Symbol {
                base: "BTC".into(),
                quote: "USDT".into(),
            },
            trade_id: Some("123".into()),
            price: "30000".into(),
            quantity: "0.01".into(),
            side: Side::Buy,
            timestamp: ts,
            timestamp_ms: ts.timestamp_millis(),
            event_type: EventType::Trade,
            agent: "ingestor".into(),
            meta: Some(TradeMeta {
                maker: true,
                taker_order_id: Some("abc".into()),
                fees: vec![Fee {
                    asset: "USDT".into(),
                    amount: "0.1".into(),
                }],
                extra: HashMap::new(),
            }),
            extra: HashMap::new(),
        };

        let json = serde_json::to_string(&trade).expect("serialize");
        let v: Value = serde_json::from_str(&json).expect("json");
        assert_eq!(v["exchange"], "BINANCE");
        assert_eq!(v["side"], "BUY");
        assert_eq!(v["event_type"], "TRADE");

        let back: Trade = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.exchange, "BINANCE");
        assert_eq!(back.side, Side::Buy);
        assert_eq!(back.event_type, EventType::Trade);
    }

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
