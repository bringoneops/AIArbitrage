use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

pub mod monitor;
pub use monitor::{
    spawn_metrics, AnalyticsMetrics, BridgeEvent, ExchangeFlows, StablecoinMonitorEvent,
    ValidatorStats,
};

/// Trade record consumed by the analytics service.
#[derive(Debug, Deserialize)]
pub struct Trade {
    /// Source exchange name.
    pub agent: String,
    /// Canonical `BASE-QUOTE` symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Trade price as string.
    #[serde(rename = "p")]
    pub price: String,
}

/// Event emitted when a spread exceeds the configured threshold.
#[derive(Debug, Clone, Serialize)]
pub struct SpreadEvent {
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub spread: f64,
    pub timestamp: i64,
}

/// Spawn the analytics task.
///
/// Returns a [`mpsc::Sender`] accepting [`Trade`] messages and a
/// [`broadcast::Receiver`] yielding [`SpreadEvent`] notifications.
pub fn spawn(threshold: f64) -> (mpsc::Sender<Trade>, broadcast::Receiver<SpreadEvent>) {
    let (tx, mut rx) = mpsc::channel::<Trade>(100);
    let (event_tx, event_rx) = broadcast::channel(100);

    tokio::spawn(async move {
        let mut prices: HashMap<String, HashMap<String, f64>> = HashMap::new();

        while let Some(trade) = rx.recv().await {
            let Trade {
                agent: exch,
                symbol: sym,
                price: price_str,
            } = trade;
            if let Ok(price) = price_str.parse::<f64>() {
                let entry = prices.entry(sym.clone()).or_default();
                entry.insert(exch, price);

                if entry.len() >= 2 {
                    let mut best_buy: Option<(&String, f64)> = None;
                    let mut best_sell: Option<(&String, f64)> = None;
                    for (e, p) in entry.iter() {
                        if best_buy.as_ref().map_or(true, |(_, bp)| p < bp) {
                            best_buy = Some((e, *p));
                        }
                        if best_sell.as_ref().map_or(true, |(_, sp)| p > sp) {
                            best_sell = Some((e, *p));
                        }
                    }
                    if let (Some((buy_ex, buy_p)), Some((sell_ex, sell_p))) = (best_buy, best_sell)
                    {
                        let spread = sell_p - buy_p;
                        if spread >= threshold {
                            let event = SpreadEvent {
                                symbol: sym,
                                buy_exchange: buy_ex.clone(),
                                sell_exchange: sell_ex.clone(),
                                spread,
                                timestamp: Utc::now().timestamp_millis(),
                            };
                            let _ = event_tx.send(event.clone());
                            info!(?event, "arbitrage opportunity");
                        }
                    }
                }
            }
        }
    });

    (tx, event_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emits_spread_events() {
        let (tx, mut rx) = spawn(10.0);
        tx.send(Trade {
            agent: "a".into(),
            symbol: "BTC-USD".into(),
            price: "100".into(),
        })
        .await
        .unwrap();
        tx.send(Trade {
            agent: "b".into(),
            symbol: "BTC-USD".into(),
            price: "115".into(),
        })
        .await
        .unwrap();
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.symbol, "BTC-USD");
        assert_eq!(ev.buy_exchange, "a");
        assert_eq!(ev.sell_exchange, "b");
        assert!(ev.spread >= 15.0 - 1e-6);
    }

    #[tokio::test]
    async fn emits_stablecoin_monitor_events() {
        let (_state, mut rx) = spawn_metrics(std::time::Duration::from_millis(10));
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.stablecoin, "USDC");
        assert!(ev.supply > 0.0);
    }
}
