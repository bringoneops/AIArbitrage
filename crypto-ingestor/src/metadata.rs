use chrono::Utc;
use serde::Serialize;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::{
    error::IngestorError,
    http_client,
    metrics::{METADATA_FETCH_INCIDENTS, METADATA_FETCH_LATENCY},
    sink::DynSink,
};

#[derive(Serialize)]
pub struct ReferenceData {
    pub agent: &'static str,
    #[serde(rename = "type")]
    pub event_type: &'static str,
    pub contracts: serde_json::Value,
    pub limits: serde_json::Value,
    pub maintenance: serde_json::Value,
    pub ts: i64,
}

/// Periodically fetch reference data for supported exchanges and
/// emit `ReferenceData` events via the provided `sink`.
pub async fn run(mut shutdown: tokio::sync::watch::Receiver<bool>, sink: DynSink) {
    // fetch immediately on startup
    fetch_all(&sink).await;

    // refresh every hour
    let mut ticker = interval(Duration::from_secs(60 * 60));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = ticker.tick() => {
                fetch_all(&sink).await;
            }
        }
    }
}

async fn fetch_all(sink: &DynSink) {
    for exch in ["binance", "coinbase"] {
        if let Err(e) = fetch_and_emit(exch, sink).await {
            tracing::error!(exchange=%exch, error=%e, "metadata fetch failed");
        }
    }
}

async fn fetch_and_emit(exchange: &'static str, sink: &DynSink) -> Result<(), IngestorError> {
    let (exchange_info_url, status_url, fees_url) = match exchange {
        "binance" => (
            "https://api.binance.us/api/v3/exchangeInfo",
            "https://api.binance.us/wapi/v3/systemStatus.html",
            "https://api.binance.us/api/v3/account",
        ),
        "coinbase" => (
            "https://api.exchange.coinbase.com/products",
            "https://api.exchange.coinbase.com/system/status",
            "https://api.exchange.coinbase.com/fees",
        ),
        _ => return Ok(()),
    };

    let client = http_client::builder().build().map_err(|e| IngestorError::Http {
        source: e,
        exchange,
        symbol: None,
    })?;

    let exchange_info = fetch_json(&client, exchange, "exchangeInfo", exchange_info_url).await?;
    let status = fetch_json(&client, exchange, "status", status_url).await?;
    let fees = fetch_json(&client, exchange, "fees", fees_url).await?;

    let event = ReferenceData {
        agent: exchange,
        event_type: "reference_data",
        contracts: exchange_info,
        limits: fees,
        maintenance: status,
        ts: Utc::now().timestamp_millis(),
    };

    let line = serde_json::to_string(&event).map_err(|e| IngestorError::Other(e.to_string()))?;
    sink.send(&line).await?;
    Ok(())
}

async fn fetch_json(
    client: &reqwest::Client,
    exchange: &'static str,
    endpoint: &'static str,
    url: &str,
) -> Result<serde_json::Value, IngestorError> {
    let start = std::time::Instant::now();
    let resp = client.get(url).send().await;
    let latency = start.elapsed().as_millis() as i64;
    METADATA_FETCH_LATENCY.with_label_values(&[exchange, endpoint]).set(latency);

    match resp {
        Ok(r) => r
            .json::<serde_json::Value>()
            .await
            .map_err(|e| {
                METADATA_FETCH_INCIDENTS.with_label_values(&[exchange, endpoint]).inc();
                IngestorError::Http { source: e, exchange, symbol: None }
            }),
        Err(e) => {
            METADATA_FETCH_INCIDENTS.with_label_values(&[exchange, endpoint]).inc();
            Err(IngestorError::Http { source: e, exchange, symbol: None })
        }
    }
}

