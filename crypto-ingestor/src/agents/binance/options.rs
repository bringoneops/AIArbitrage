use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use canonicalizer::{CanonicalService, OptionChain, OptionGreeks, OptionQuote, OptionSurfacePoint};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

pub struct BinanceOptionsAgent {
    symbols: Vec<String>,
    rest_url: String,
    poll_interval_secs: u64,
}

impl BinanceOptionsAgent {
    pub fn new(symbols: Vec<String>, cfg: &Settings) -> Self {
        Self {
            symbols,
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

        let mut last: HashMap<(String, i64), OptionChain> = HashMap::new();

        loop {
            for sym in &self.symbols {
                let expiries = fetch_expiries(&client, &self.rest_url, sym).await;
                for exp in expiries {
                    let url = format!(
                        "{}/optionChain?symbol={}&expiry={}",
                        self.rest_url, sym, exp
                    );
                    match client.get(&url).send().await {
                        Ok(resp) => match resp.json::<Value>().await {
                            Ok(v) => {
                                if let Some(chain) = parse_chain(sym, &exp, &v) {
                                    let key = (sym.clone(), chain.expiry);
                                    if last.get(&key) != Some(&chain) {
                                        if tx
                                            .send(serde_json::to_string(&chain).unwrap())
                                            .await
                                            .is_err()
                                        {
                                            return Ok(());
                                        }
                                        last.insert(key, chain);
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

async fn fetch_expiries(client: &reqwest::Client, base: &str, symbol: &str) -> Vec<String> {
    let url = format!("{}/optionInfo?symbol={}", base, symbol);
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Value>().await {
            if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
                let mut set = HashSet::new();
                for item in arr {
                    if let Some(exp) = item
                        .get("expiryDate")
                        .or_else(|| item.get("expiry"))
                        .or_else(|| item.get("expiration"))
                        .and_then(|e| e.as_str())
                    {
                        set.insert(exp.to_string());
                    }
                }
                let mut expiries: Vec<String> = set.into_iter().collect();
                expiries.sort();
                return expiries;
            }
        }
    }
    Vec::new()
}

fn parse_chain(symbol: &str, expiry: &str, v: &Value) -> Option<OptionChain> {
    let canon = CanonicalService::canonical_pair("binance", symbol)?;
    let expiry_ts = parse_expiry(expiry)?;

    let mut options = Vec::new();
    if let Some(arr) = v
        .get("data")
        .and_then(|d| d.as_array())
        .or_else(|| v.as_array())
    {
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

    let surface = options
        .iter()
        .filter_map(|q| {
            q.iv.map(|iv| OptionSurfacePoint {
                strike: q.strike,
                expiry: expiry_ts,
                iv,
            })
        })
        .collect();

    Some(OptionChain {
        agent: "binance".to_string(),
        r#type: "option_chain".to_string(),
        s: canon,
        expiry: expiry_ts,
        options,
        surface,
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
    v.get(key).and_then(|x| {
        x.as_f64()
            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
    })
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
        let agent = BinanceOptionsAgent::new(symbols.drain(..).collect(), cfg);
        Some(Box::new(agent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_chain_builds_surface() {
        let v = serde_json::json!({
            "data": [{
                "strike": "30000",
                "call": {"bid": "10", "ask": "11", "lastPrice": "10.5", "iv": "0.55"},
                "put": {"bid": "9", "ask": "10", "last": "9.5", "iv": "0.60"}
            }]
        });
        let chain = parse_chain("btcusdt", "2023-09-01", &v).expect("chain");
        assert_eq!(chain.options.len(), 2);
        assert_eq!(chain.surface.len(), 2);
        assert!(chain.surface.iter().any(|p| (p.iv - 0.55).abs() < 1e-6));
        assert!(chain.surface.iter().any(|p| (p.iv - 0.60).abs() < 1e-6));
    }
}
