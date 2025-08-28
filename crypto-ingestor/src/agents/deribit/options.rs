use std::{collections::HashMap, time::Duration};

use canonicalizer::{CanonicalService, OptionChain, OptionGreeks, OptionQuote, OptionSurfacePoint};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

pub struct DeribitOptionsAgent {
    symbols: Vec<String>,
    rest_url: String,
    poll_interval_secs: u64,
}

impl DeribitOptionsAgent {
    pub fn new(symbols: Vec<String>, cfg: &Settings) -> Self {
        Self {
            symbols,
            rest_url: cfg.deribit_options_rest_url.clone(),
            poll_interval_secs: cfg.deribit_options_poll_interval_secs,
        }
    }
}

#[async_trait::async_trait]
impl Agent for DeribitOptionsAgent {
    fn name(&self) -> &'static str {
        "deribit_options"
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
                exchange: "deribit",
                symbol: None,
            })?;
        let mut last: HashMap<(String, i64), OptionChain> = HashMap::new();
        loop {
            for sym in &self.symbols {
                let url = format!(
                    "{}/public/get_book_summary_by_currency?currency={}&kind=option",
                    self.rest_url, sym
                );
                match client.get(&url).send().await {
                    Ok(resp) => match resp.json::<Value>().await {
                        Ok(v) => {
                            for chain in parse_chains(sym, &v) {
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
                            tracing::error!(error=%e, "failed to decode deribit chain");
                        }
                    },
                    Err(e) => {
                        tracing::error!(error=%e, "deribit option chain request failed");
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(self.poll_interval_secs)) => {},
                _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
            }
        }
        Ok(())
    }
}

fn parse_chains(symbol: &str, v: &Value) -> Vec<OptionChain> {
    let canon = CanonicalService::canonical_pair("coinbase", &format!("{}-USD", symbol))
        .unwrap_or_else(|| format!("{}-USD", symbol.to_uppercase()));
    let mut map: HashMap<i64, Vec<OptionQuote>> = HashMap::new();
    if let Some(arr) = v
        .get("result")
        .and_then(|r| r.as_array())
        .or_else(|| v.as_array())
    {
        for item in arr {
            if let Some((expiry, quote)) = parse_quote(item) {
                map.entry(expiry).or_default().push(quote);
            }
        }
    }
    map.into_iter()
        .map(|(expiry, options)| {
            let surface = options
                .iter()
                .filter_map(|q| {
                    q.iv.map(|iv| OptionSurfacePoint {
                        strike: q.strike,
                        expiry,
                        iv,
                    })
                })
                .collect();
            OptionChain {
                agent: "deribit".into(),
                r#type: "option_chain".into(),
                s: canon.clone(),
                expiry,
                options,
                surface,
            }
        })
        .collect()
}

fn parse_quote(v: &Value) -> Option<(i64, OptionQuote)> {
    let name = v.get("instrument_name")?.as_str()?;
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    let strike: f64 = parts[2].parse().ok()?;
    let kind = if parts[3].eq_ignore_ascii_case("C") {
        "CALL"
    } else {
        "PUT"
    }
    .to_string();
    let expiry = v
        .get("expiration_timestamp")
        .and_then(|e| e.as_i64())
        .map(|ts| ts / 1000)
        .or_else(|| parse_deribit_expiry(parts[1]))?;
    let bid = v.get("bid_price").and_then(|x| x.as_f64());
    let ask = v.get("ask_price").and_then(|x| x.as_f64());
    let last = v.get("last_price").and_then(|x| x.as_f64());
    let iv = v.get("mark_iv").and_then(|x| x.as_f64());
    let delta = v.get("delta").and_then(|x| x.as_f64());
    let gamma = v.get("gamma").and_then(|x| x.as_f64());
    let theta = v.get("theta").and_then(|x| x.as_f64());
    let vega = v.get("vega").and_then(|x| x.as_f64());
    let greeks = if delta.is_some() || gamma.is_some() || theta.is_some() || vega.is_some() {
        Some(OptionGreeks {
            delta,
            gamma,
            theta,
            vega,
        })
    } else {
        None
    };
    Some((
        expiry,
        OptionQuote {
            strike,
            kind,
            bid,
            ask,
            last,
            iv,
            greeks,
        },
    ))
}

fn parse_deribit_expiry(code: &str) -> Option<i64> {
    use chrono::{NaiveDate, TimeZone, Utc};
    let d = NaiveDate::parse_from_str(code, "%d%b%y").ok()?;
    let dt = d.and_hms_opt(8, 0, 0)?;
    Some(Utc.from_utc_datetime(&dt).timestamp())
}

pub struct DeribitOptionsFactory;

#[async_trait::async_trait]
impl crate::agents::AgentFactory for DeribitOptionsFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let mut symbols: Vec<String> = if spec.trim().is_empty() {
            cfg.deribit_options_symbols.clone()
        } else {
            spec.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };
        if symbols.is_empty() {
            tracing::error!("no deribit option symbols specified");
            return None;
        }
        Some(Box::new(DeribitOptionsAgent::new(
            symbols.drain(..).collect(),
            cfg,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quote_extracts_iv() {
        let v = serde_json::json!({
            "instrument_name": "BTC-30JUN23-30000-C",
            "mark_iv": 0.5,
            "bid_price": 1.0,
            "ask_price": 2.0,
            "last_price": 1.5,
            "expiration_timestamp": 1_600_000_000_000i64
        });
        let (expiry, quote) = parse_quote(&v).expect("quote");
        assert_eq!(expiry, 1_600_000_000);
        assert_eq!(quote.iv.unwrap(), 0.5);
    }
}
