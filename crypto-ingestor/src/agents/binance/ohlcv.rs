use std::time::Duration;

use canonicalizer::{Bar, CanonicalService};
use futures_util::future::join_all;
use tokio::sync::mpsc;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

pub struct BinanceOhlcvAgent {
    symbols: Vec<String>,
    intervals: Vec<u64>,
    poll_interval_secs: u64,
}

impl BinanceOhlcvAgent {
    pub fn new(symbols: Vec<String>, intervals: Vec<u64>, cfg: &Settings) -> Self {
        Self {
            symbols,
            intervals,
            poll_interval_secs: cfg.binance_ohlcv_poll_interval_secs,
        }
    }
}

fn interval_str(secs: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    const WEEK: u64 = 7 * DAY;
    if secs % WEEK == 0 {
        format!("{}w", secs / WEEK)
    } else if secs % DAY == 0 {
        format!("{}d", secs / DAY)
    } else if secs % HOUR == 0 {
        format!("{}h", secs / HOUR)
    } else if secs % MINUTE == 0 {
        format!("{}m", secs / MINUTE)
    } else {
        format!("{}s", secs)
    }
}

pub async fn fetch_bar(client: &reqwest::Client, symbol: &str, interval: u64) -> Option<Bar> {
    let url = format!(
        "https://api.binance.us/api/v3/klines?symbol={}&interval={}&limit=1",
        symbol.to_uppercase(),
        interval_str(interval)
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
    let ts = first.get(0)?.as_i64()?;
    let open = first.get(1)?.as_str()?.to_string();
    let high = first.get(2)?.as_str()?.to_string();
    let low = first.get(3)?.as_str()?.to_string();
    let close = first.get(4)?.as_str()?.to_string();
    let volume = first.get(5)?.as_str()?.to_string();
    let sym =
        CanonicalService::canonical_pair("binance", symbol).unwrap_or_else(|| symbol.to_string());
    Some(Bar {
        agent: "binance".into(),
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
impl Agent for BinanceOhlcvAgent {
    fn name(&self) -> &'static str {
        "binance_ohlcv"
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
                exchange: "binance",
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

pub struct BinanceOhlcvFactory;

#[async_trait::async_trait]
impl super::super::AgentFactory for BinanceOhlcvFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let symbols = if spec.trim().is_empty() || spec.eq_ignore_ascii_case("all") {
            match super::fetch_all_symbols().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error=%e, "failed to fetch binance symbols");
                    return None;
                }
            }
        } else {
            spec.split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        };
        if cfg.binance_ohlcv_intervals.is_empty() {
            tracing::error!("no binance ohlcv intervals specified");
            return None;
        }
        let agent = BinanceOhlcvAgent::new(symbols, cfg.binance_ohlcv_intervals.clone(), cfg);
        Some(Box::new(agent))
    }
}
