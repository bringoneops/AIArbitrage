use std::time::Duration;

use canonicalizer::{CanonicalService, OptionChain, OptionGreeks, OptionQuote};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

pub struct BinanceOptionsAgent {
    symbols: Vec<String>,
    expiries: Vec<String>,
    rest_url: String,
    poll_interval_secs: u64,
}

impl BinanceOptionsAgent {
    pub fn new(symbols: Vec<String>, expiries: Vec<String>, cfg: &Settings) -> Self {
        Self {
            symbols,
            expiries,
            rest_url: cfg.binance_options_rest_url.clone(),
            poll_interval_secs: cfg.binance_options_poll_interval_secs,
        }
    }
}

#[async_trait::async_trait]
impl Agent for BinanceOptionsAgent {
    fn name(&self) -> &'static str {
        "binance_options"
    }

    fn event_types(&self) -> Vec<crate::agent::EventType> {
        Vec::new()
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
            for sym in &self.symbols {
                for exp in &self.expiries {
                    let url = format!("{}/optionChain?symbol={}&expiry={}", self.rest_url, sym, exp);
                    match client.get(&url).send().await {
                        Ok(resp) => match resp.json::<Value>().await {
                            Ok(v) => {
                                if let Some(event) = parse_chain(sym, exp, &v) {
                                    if tx
                                        .send(serde_json::to_string(&event).unwrap())
                                        .await
                                        .is_err()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(error=%e, "failed to decode option chain");
                            }
                        },
                        Err(e) => {
                            tracing::error!(error=%e, "binance option chain request failed");
                        }
                    }
                }
            }

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

fn parse_chain(symbol: &str, expiry: &str, v: &Value) -> Option<OptionChain> {
    let canon = CanonicalService::canonical_pair("binance", symbol)?;
    let expiry_ts = parse_expiry(expiry)?;

    let mut options = Vec::new();
    if let Some(arr) = v.get("data").and_then(|d| d.as_array()).or_else(|| v.as_array()) {
        for item in arr {
            let strike = as_f64(item, "strike").or_else(|| as_f64(item, "strikePrice"))?;
            if let Some(call) = item.get("call") {
                if let Some(q) = parse_side(strike, "CALL", call) {
                    options.push(q);
                }
            }
            if let Some(put) = item.get("put") {
                if let Some(q) = parse_side(strike, "PUT", put) {
                    options.push(q);
                }
            }
        }
    }

    Some(OptionChain {
        agent: "binance".to_string(),
        r#type: "option_chain".to_string(),
        s: canon,
        expiry: expiry_ts,
        options,
    })
}

fn parse_side(strike: f64, kind: &str, v: &Value) -> Option<OptionQuote> {
    let bid = as_f64(v, "bid");
    let ask = as_f64(v, "ask");
    let last = as_f64(v, "lastPrice").or_else(|| as_f64(v, "last"));
    let iv = as_f64(v, "iv").or_else(|| as_f64(v, "impliedVol"));
    let greeks = {
        let delta = as_f64(v, "delta");
        let gamma = as_f64(v, "gamma");
        let theta = as_f64(v, "theta");
        let vega = as_f64(v, "vega");
        if delta.is_some() || gamma.is_some() || theta.is_some() || vega.is_some() {
            Some(OptionGreeks {
                delta,
                gamma,
                theta,
                vega,
            })
        } else {
            None
        }
    };

    Some(OptionQuote {
        strike,
        kind: kind.to_string(),
        bid,
        ask,
        last,
        iv,
        greeks,
    })
}

fn as_f64(v: &Value, key: &str) -> Option<f64> {
    v.get(key)
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}

fn parse_expiry(exp: &str) -> Option<i64> {
    use chrono::{NaiveDate, TimeZone, Utc};
    let d = NaiveDate::parse_from_str(exp, "%Y-%m-%d").ok()?;
    let dt = d.and_hms_opt(0, 0, 0)?;
    Some(Utc.from_utc_datetime(&dt).timestamp())
}

pub struct BinanceOptionsFactory;

#[async_trait::async_trait]
impl crate::agents::AgentFactory for BinanceOptionsFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let mut symbols: Vec<String> = if spec.trim().is_empty() {
            cfg.binance_options_symbols.clone()
        } else {
            spec.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };
        if symbols.is_empty() {
            tracing::error!("no binance option symbols specified");
            return None;
        }
        let expiries = if cfg.binance_options_expiries.is_empty() {
            tracing::error!("no binance option expiries specified");
            return None;
        } else {
            cfg.binance_options_expiries.clone()
        };
        let agent = BinanceOptionsAgent::new(symbols.drain(..).collect(), expiries, cfg);
        Some(Box::new(agent))
    }
}

