use std::time::Duration;

use canonicalizer::{Bar, CanonicalService};
use futures_util::future::join_all;
use tokio::sync::mpsc;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

pub struct CoinbaseOhlcvAgent {
    symbols: Vec<String>,
    intervals: Vec<u64>,
    poll_interval_secs: u64,
}

impl CoinbaseOhlcvAgent {
    pub fn new(symbols: Vec<String>, intervals: Vec<u64>, cfg: &Settings) -> Self {
        Self {
            symbols,
            intervals,
            poll_interval_secs: cfg.coinbase_ohlcv_poll_interval_secs,
        }
    }
}

fn val_to_string(v: &serde_json::Value) -> String {
    v.as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| v.to_string())
}

pub async fn fetch_bar(client: &reqwest::Client, symbol: &str, interval: u64) -> Option<Bar> {
    let url = format!(
        "https://api.exchange.coinbase.com/products/{}/candles?granularity={}&limit=1",
        symbol, interval
    );
    let mut delay = Duration::from_millis(500);
    for _ in 0..3 {
        match client.get(&url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    if let Ok(v) = resp.json::<serde_json::Value>().await {
                        if let Some(bar) = parse_bar(symbol, interval, &v) {
                            return Some(bar);
                        }
                    }
                    break;
                } else if status.as_u16() == 429 || status.is_server_error() {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                } else {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    None
}

pub fn parse_bar(symbol: &str, interval: u64, v: &serde_json::Value) -> Option<Bar> {
    let first = v.as_array()?.get(0)?.as_array()?;
    let ts = first.get(0)?.as_i64()? * 1000; // seconds to ms
    let low = val_to_string(first.get(1)?);
    let high = val_to_string(first.get(2)?);
    let open = val_to_string(first.get(3)?);
    let close = val_to_string(first.get(4)?);
    let volume = val_to_string(first.get(5)?);
    let sym =
        CanonicalService::canonical_pair("coinbase", symbol).unwrap_or_else(|| symbol.to_string());
    Some(Bar {
        agent: "coinbase".into(),
        r#type: "ohlcv".into(),
        symbol: sym,
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp: ts,
    })
}

#[async_trait::async_trait]
impl Agent for CoinbaseOhlcvAgent {
    fn name(&self) -> &'static str {
        "coinbase_ohlcv"
    }

    async fn run(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        tx: mpsc::Sender<String>,
    ) -> Result<(), IngestorError> {
        let client = http_client::builder()
            .build()
            .map_err(|e| IngestorError::Http {
                source: e,
                exchange: "coinbase",
                symbol: None,
            })?;
        loop {
            let mut futs = Vec::new();
            for s in &self.symbols {
                for &i in &self.intervals {
                    let client = client.clone();
                    let symbol = s.clone();
                    let tx = tx.clone();
                    futs.push(async move {
                        if let Some(bar) = fetch_bar(&client, &symbol, i).await {
                            let _ = tx.send(serde_json::to_string(&bar).unwrap()).await;
                        }
                    });
                }
            }
            join_all(futs).await;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(self.poll_interval_secs)) => {},
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
        Ok(())
    }
}

pub struct CoinbaseOhlcvFactory;

#[async_trait::async_trait]
impl super::super::AgentFactory for CoinbaseOhlcvFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let symbols = if spec.trim().is_empty() || spec.eq_ignore_ascii_case("all") {
            match super::fetch_all_symbols().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error=%e, "failed to fetch coinbase symbols");
                    return None;
                }
            }
        } else {
            spec.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };
        if cfg.coinbase_ohlcv_intervals.is_empty() {
            tracing::error!("no coinbase ohlcv intervals specified");
            return None;
        }
        let agent = CoinbaseOhlcvAgent::new(symbols, cfg.coinbase_ohlcv_intervals.clone(), cfg);
        Some(Box::new(agent))
    }
}
