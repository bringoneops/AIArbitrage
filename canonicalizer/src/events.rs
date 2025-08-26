use serde::{Deserialize, Serialize};

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
        };

        let json = serde_json::to_string(&chain).expect("serialize");
        let back: OptionChain = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, chain);
    }
}

