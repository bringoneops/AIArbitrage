use std::collections::HashMap;

use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Generic macroeconomic metric emitted by the service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroMetric {
    pub category: String,
    pub symbol: String,
    pub value: f64,
    pub timestamp: i64,
}

/// Index values calculated from cryptocurrency market data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoIndex {
    pub name: String,
    pub value: f64,
    pub timestamp: i64,
}

/// Spawn background tasks fetching macro data and crypto indices.
///
/// Returns [`broadcast::Receiver`]s yielding [`MacroMetric`] and [`CryptoIndex`] events.
pub fn spawn(
    shutdown: CancellationToken,
) -> (
    broadcast::Receiver<MacroMetric>,
    broadcast::Receiver<CryptoIndex>,
) {
    let (macro_tx, macro_rx) = broadcast::channel(100);
    let (crypto_tx, crypto_rx) = broadcast::channel(100);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");

    tokio::spawn(run_fx_fetcher(
        client.clone(),
        macro_tx.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run_rates_fetcher(
        client.clone(),
        macro_tx.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run_commodity_fetcher(
        client.clone(),
        macro_tx.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run_equity_fetcher(
        client.clone(),
        macro_tx.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run_event_fetcher(
        client.clone(),
        macro_tx.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run_crypto_indices_fetcher(
        client,
        crypto_tx.clone(),
        shutdown,
    ));

    (macro_rx, crypto_rx)
}

async fn run_fx_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<MacroMetric>,
    shutdown: CancellationToken,
) {
    let mut intv = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                match client
                    .get("https://api.exchangerate.host/latest?base=USD&symbols=EUR,JPY,GBP")
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) => match resp.text().await {
                        Ok(body) => match parse_exchangerate_host(&body) {
                            Ok(metrics) => metrics.into_iter().for_each(|m| {
                                let _ = tx.send(m);
                            }),
                            Err(e) => info!("fx parse error: {}", e),
                        },
                        Err(e) => info!("fx body error: {}", e),
                    },
                    Err(e) => info!("fx fetch error: {}", e),
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

async fn run_rates_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<MacroMetric>,
    shutdown: CancellationToken,
) {
    let api_key = match std::env::var("FRED_API_KEY") {
        Ok(k) => k,
        Err(_) => {
            info!("FRED_API_KEY not set; disabling rate fetcher");
            return;
        }
    };
    let mut intv = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                let url = format!("https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&sort_order=desc&limit=1&api_key={api_key}&file_type=json");
                match client.get(&url).timeout(Duration::from_secs(10)).send().await {
                    Ok(resp) => match resp.text().await {
                        Ok(body) => match parse_fred_rate(&body) {
                            Ok(Some(val)) => {
                                let metric = MacroMetric {
                                    category: "rate".into(),
                                    symbol: "US10Y".into(),
                                    value: val,
                                    timestamp: Utc::now().timestamp_millis(),
                                };
                                let _ = tx.send(metric);
                            }
                            Ok(None) => info!("no rate data"),
                            Err(e) => info!("rate parse error: {}", e),
                        },
                        Err(e) => info!("rate body error: {}", e),
                    },
                    Err(e) => info!("rate fetch error: {}", e),
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

async fn run_commodity_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<MacroMetric>,
    shutdown: CancellationToken,
) {
    let mut intv = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                for (symbol, name) in [("gc.f", "GOLD"), ("cl.f", "WTI")] {
                    if let Ok(resp) = client
                        .get(format!("https://stooq.com/q/l/?s={}&i=d", symbol))
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                    {
                        if let Ok(body) = resp.text().await {
                            if let Some(price) = parse_stooq_price(&body) {
                                let metric = MacroMetric {
                                    category: "commodity".into(),
                                    symbol: name.into(),
                                    value: price,
                                    timestamp: Utc::now().timestamp_millis(),
                                };
                                let _ = tx.send(metric);
                            }
                        }
                    }
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

async fn run_equity_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<MacroMetric>,
    shutdown: CancellationToken,
) {
    let mut intv = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                for (symbol, name) in [("^spx", "SPX"), ("^ndq", "NDQ"), ("^dji", "DJI")] {
                    if let Ok(resp) = client
                        .get(format!("https://stooq.com/q/l/?s={}&i=d", symbol))
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                    {
                        if let Ok(body) = resp.text().await {
                            if let Some(level) = parse_stooq_price(&body) {
                                let metric = MacroMetric {
                                    category: "equity".into(),
                                    symbol: name.into(),
                                    value: level,
                                    timestamp: Utc::now().timestamp_millis(),
                                };
                                let _ = tx.send(metric);
                            }
                        }
                    }
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

async fn run_event_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<MacroMetric>,
    shutdown: CancellationToken,
) {
    let mut intv = interval(Duration::from_secs(86400));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                match client
                    .get("https://date.nager.at/api/v3/NextPublicHolidays/US")
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) => match resp.text().await {
                        Ok(body) => match parse_nager_events(&body) {
                            Ok(events) => events.into_iter().for_each(|e| {
                                let _ = tx.send(e);
                            }),
                            Err(e) => info!("event parse error: {}", e),
                        },
                        Err(e) => info!("event body error: {}", e),
                    },
                    Err(e) => info!("event fetch error: {}", e),
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

async fn run_crypto_indices_fetcher(
    client: reqwest::Client,
    tx: broadcast::Sender<CryptoIndex>,
    shutdown: CancellationToken,
) {
    let mut intv = interval(Duration::from_secs(300));
    loop {
        tokio::select! {
            _ = intv.tick() => {
                match client
                    .get("https://api.coingecko.com/api/v3/global")
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) => match resp.text().await {
                        Ok(body) => match parse_coingecko_global(&body) {
                            Ok(indices) => indices.into_iter().for_each(|i| {
                                let _ = tx.send(i);
                            }),
                            Err(e) => info!("crypto index parse error: {}", e),
                        },
                        Err(e) => info!("crypto index body error: {}", e),
                    },
                    Err(e) => info!("crypto index fetch error: {}", e),
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

fn parse_exchangerate_host(data: &str) -> Result<Vec<MacroMetric>, serde_json::Error> {
    #[derive(Deserialize)]
    struct Resp {
        base: String,
        rates: HashMap<String, f64>,
    }
    let resp: Resp = serde_json::from_str(data)?;
    let ts = Utc::now().timestamp_millis();
    Ok(resp
        .rates
        .into_iter()
        .map(|(sym, rate)| MacroMetric {
            category: "fx".into(),
            symbol: format!("{}{}", resp.base, sym),
            value: rate,
            timestamp: ts,
        })
        .collect())
}

fn parse_coingecko_global(data: &str) -> Result<Vec<CryptoIndex>, serde_json::Error> {
    #[derive(Deserialize)]
    struct Data {
        market_cap_percentage: HashMap<String, f64>,
    }
    #[derive(Deserialize)]
    struct Resp {
        data: Data,
    }
    let resp: Resp = serde_json::from_str(data)?;
    let ts = Utc::now().timestamp_millis();
    let mut res = Vec::new();
    if let Some(btc) = resp.data.market_cap_percentage.get("btc") {
        res.push(CryptoIndex {
            name: "btc_dominance".into(),
            value: *btc,
            timestamp: ts,
        });
    }
    if let Some(eth) = resp.data.market_cap_percentage.get("eth") {
        res.push(CryptoIndex {
            name: "eth_dominance".into(),
            value: *eth,
            timestamp: ts,
        });
    }
    Ok(res)
}

fn parse_fred_rate(data: &str) -> Result<Option<f64>, serde_json::Error> {
    #[derive(Deserialize)]
    struct Obs {
        value: String,
    }
    #[derive(Deserialize)]
    struct Resp {
        observations: Vec<Obs>,
    }
    let resp: Resp = serde_json::from_str(data)?;
    if let Some(o) = resp.observations.get(0) {
        if let Ok(v) = o.value.parse::<f64>() {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn parse_stooq_price(data: &str) -> Option<f64> {
    data.split(',').nth(6)?.parse().ok()
}

fn parse_nager_events(data: &str) -> Result<Vec<MacroMetric>, serde_json::Error> {
    #[derive(Deserialize)]
    struct Holiday {
        date: String,
        name: String,
    }
    let holidays: Vec<Holiday> = serde_json::from_str(data)?;
    let mut res = Vec::new();
    for h in holidays {
        if let Ok(d) = NaiveDate::parse_from_str(&h.date, "%Y-%m-%d") {
            let ts = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
            res.push(MacroMetric {
                category: "event".into(),
                symbol: h.name,
                value: 1.0,
                timestamp: ts,
            });
        }
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fx_rates() {
        let json = r#"{"base":"USD","rates":{"EUR":0.9,"JPY":110.0}}"#;
        let metrics = parse_exchangerate_host(json).unwrap();
        assert_eq!(metrics.len(), 2);
        assert!(metrics
            .iter()
            .any(|m| m.symbol == "USDEUR" && (m.value - 0.9).abs() < 1e-6));
        assert!(metrics
            .iter()
            .any(|m| m.symbol == "USDJPY" && (m.value - 110.0).abs() < 1e-6));
    }

    #[test]
    fn parses_crypto_indices() {
        let json = r#"{"data":{"market_cap_percentage":{"btc":51.0,"eth":18.0}}}"#;
        let indices = parse_coingecko_global(json).unwrap();
        assert!(indices
            .iter()
            .any(|i| i.name == "btc_dominance" && (i.value - 51.0).abs() < 1e-6));
        assert!(indices
            .iter()
            .any(|i| i.name == "eth_dominance" && (i.value - 18.0).abs() < 1e-6));
    }

    #[test]
    fn parses_stooq() {
        let line = "^SPX,20250825,230000,6457.67,6466.89,6438.06,6439.32,2506639696,";
        let price = parse_stooq_price(line).unwrap();
        assert!((price - 6439.32).abs() < 1e-6);
    }

    #[test]
    fn parses_events() {
        let json = r#"[{"date":"2025-09-01","name":"Labor Day"}]"#;
        let events = parse_nager_events(json).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "Labor Day");
        assert_eq!(events[0].category, "event");
    }
}
