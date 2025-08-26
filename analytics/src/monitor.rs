use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use serde::Serialize;
use tokio::sync::{broadcast, Mutex};

/// Basic validator statistics.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatorStats {
    pub total: u64,
    pub active: u64,
}

/// Representation of a bridge event.
#[derive(Debug, Clone, Serialize)]
pub struct BridgeEvent {
    pub bridge: String,
    pub volume: f64,
}

/// Mapping of exchange name to net wallet flow.
pub type ExchangeFlows = HashMap<String, f64>;

/// Event emitted with current stablecoin information.
#[derive(Debug, Clone, Serialize)]
pub struct StablecoinMonitorEvent {
    pub stablecoin: String,
    pub supply: f64,
    pub price: f64,
    pub deviation: f64,
    pub timestamp: i64,
}

/// Aggregated analytics metrics stored for alerting.
#[derive(Default)]
pub struct AnalyticsMetrics {
    pub validator: Option<ValidatorStats>,
    pub bridges: Vec<BridgeEvent>,
    pub exchange_flows: ExchangeFlows,
    pub stablecoin: Option<StablecoinMonitorEvent>,
}

/// Spawn periodic tasks collecting various on-chain metrics.
///
/// Returns a shared state containing the latest metrics and a broadcast
/// receiver yielding [`StablecoinMonitorEvent`] updates.
pub fn spawn_metrics(
    interval: Duration,
) -> (
    Arc<Mutex<AnalyticsMetrics>>,
    broadcast::Receiver<StablecoinMonitorEvent>,
) {
    let state = Arc::new(Mutex::new(AnalyticsMetrics::default()));
    let (tx, rx) = broadcast::channel(100);
    let state_task = state.clone();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let validator = fetch_validator_stats().await;
            let bridges = fetch_bridge_events().await;
            let flows = fetch_exchange_flows().await;
            let (supply, price) = fetch_stablecoin_data().await;
            let event = StablecoinMonitorEvent {
                stablecoin: "USDC".to_string(),
                supply,
                price,
                deviation: price - 1.0,
                timestamp: Utc::now().timestamp_millis(),
            };
            {
                let mut st = state_task.lock().await;
                st.validator = Some(validator);
                st.bridges = bridges;
                st.exchange_flows = flows;
                st.stablecoin = Some(event.clone());
            }
            let _ = tx.send(event);
        }
    });

    (state, rx)
}

async fn fetch_validator_stats() -> ValidatorStats {
    // Placeholder implementation. Real code would query blockchain RPC.
    ValidatorStats {
        total: 1000,
        active: 950,
    }
}

async fn fetch_bridge_events() -> Vec<BridgeEvent> {
    // Placeholder for pulling bridge activity.
    vec![BridgeEvent {
        bridge: "ExampleBridge".into(),
        volume: 1234.5,
    }]
}

async fn fetch_exchange_flows() -> ExchangeFlows {
    // Placeholder for exchange wallet flows.
    let mut map = HashMap::new();
    map.insert("Binance".into(), 100.0);
    map.insert("Coinbase".into(), -50.0);
    map
}

async fn fetch_stablecoin_data() -> (f64, f64) {
    // Placeholder returning mocked supply and price.
    (1_000_000.0, 0.998)
}
