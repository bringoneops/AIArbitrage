use std::collections::HashMap;
use std::str::FromStr;

use canonicalizer::Bar;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Trade {
    agent: String,
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "ts")]
    timestamp: i64,
}

struct RunningBar {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    start: i64,
}

pub struct BarAggregator {
    interval: u64,
    current: HashMap<(String, String), RunningBar>,
}

impl BarAggregator {
    pub fn new(interval: u64) -> Self {
        Self {
            interval,
            current: HashMap::new(),
        }
    }

    /// Process a JSON line containing a trade event.
    /// Returns a completed [`Bar`] if the previous interval closed.
    pub fn process_line(&mut self, line: &str) -> Option<Bar> {
        let trade: Trade = serde_json::from_str(line).ok()?;
        if trade.r#type != "trade" {
            return None;
        }
        let price = Decimal::from_str(&trade.price).ok()?;
        let qty = Decimal::from_str(&trade.qty).ok()?;
        let interval_ms = (self.interval * 1000) as i64;
        let start = (trade.timestamp / interval_ms) * interval_ms;
        let key = (trade.agent.clone(), trade.symbol.clone());
        if let Some(rb) = self.current.get_mut(&key) {
            if rb.start == start {
                rb.close = price;
                if price > rb.high {
                    rb.high = price;
                }
                if price < rb.low {
                    rb.low = price;
                }
                rb.volume += qty;
                None
            } else if start > rb.start {
                let bar = Bar {
                    agent: key.0.clone(),
                    r#type: "ohlcv".into(),
                    symbol: key.1.clone(),
                    interval: self.interval,
                    open: rb.open.to_string(),
                    high: rb.high.to_string(),
                    low: rb.low.to_string(),
                    close: rb.close.to_string(),
                    volume: rb.volume.to_string(),
                    timestamp: rb.start,
                };
                *rb = RunningBar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: qty,
                    start,
                };
                Some(bar)
            } else {
                // Ignore out-of-order trades
                None
            }
        } else {
            self.current.insert(
                key,
                RunningBar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: qty,
                    start,
                },
            );
            None
        }
    }

    /// Drain and return all currently tracked bars.
    pub fn drain(&mut self) -> Vec<Bar> {
        let mut out = Vec::new();
        for ((agent, symbol), rb) in self.current.drain() {
            out.push(Bar {
                agent,
                r#type: "ohlcv".into(),
                symbol,
                interval: self.interval,
                open: rb.open.to_string(),
                high: rb.high.to_string(),
                low: rb.low.to_string(),
                close: rb.close.to_string(),
                volume: rb.volume.to_string(),
                timestamp: rb.start,
            });
        }
        out
    }
}

