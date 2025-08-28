//! Historical open interest backfill for Binance futures.
//!
//! Data is sourced from the `/fapi/v1/openInterestHist` endpoint. Each request
//! has a weight of `1` with an approximate limit of 1200 requests per minute.
//! We request pages of up to 500 entries (5 minute granularity) and pause
//! briefly between requests.  Rate limited (`429`) or failed requests are retried
//! with exponential backoff up to five attempts.
//!
//! Retrieved records are normalised into canonical [`OpenInterest`] events and
//! emitted so downstream consumers receive a continuous history prior to live
//! streaming.

use std::time::Duration;

use canonicalizer::{events::OpenInterest, CanonicalService};
use tokio::sync::mpsc;

use crate::{http_client, parse::parse_decimal_str};

const LIMIT: usize = 500;
const PERIOD: &str = "5m";

/// Backfill historical open interest for `symbols` and publish events via `tx`.
///
/// `symbols` should be lowercase Binance symbols (e.g. `btcusdt`).
pub async fn backfill(symbols: &[String], tx: mpsc::Sender<String>) {
    let client = match http_client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error=%e, "binance open interest history http client");
            return;
        }
    };

    for sym in symbols {
        if let Err(e) = backfill_symbol(&client, sym, &tx).await {
            tracing::error!(symbol=%sym, error=%e, "open interest history backfill failed");
        }
    }
}

async fn backfill_symbol(
    client: &reqwest::Client,
    symbol: &str,
    tx: &mpsc::Sender<String>,
) -> Result<(), reqwest::Error> {
    let mut start: i64 = 0;
    loop {
        let url = format!(
            "https://fapi.binance.com/fapi/v1/openInterestHist?symbol={}&period={}&limit={}&startTime={}",
            symbol.to_uppercase(),
            PERIOD,
            LIMIT,
            start
        );

        let mut delay = Duration::from_millis(500);
        let resp = loop {
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => break resp,
                Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    continue;
                }
                Ok(resp) => break resp,
                Err(e) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    if delay > Duration::from_secs(8) {
                        return Err(e);
                    }
                }
            }
        };

        let text = resp.text().await?;
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(symbol=%symbol, error=%e, body=%text, "open interest history parse failed");
                return Ok(());
            }
        };

        let data = match value {
            serde_json::Value::Array(arr) => arr,
            serde_json::Value::Object(obj) => {
                let code = obj.get("code").and_then(|v| v.as_i64()).unwrap_or_default();
                let msg = obj.get("msg").and_then(|v| v.as_str()).unwrap_or("unknown");
                tracing::error!(symbol=%symbol, code=code, msg=%msg, "open interest history returned error response");
                return Ok(());
            }
            _ => {
                tracing::error!(symbol=%symbol, body=%text, "unexpected open interest history response");
                return Ok(());
            }
        };

        if data.is_empty() {
            break;
        }

        for item in &data {
            let ts = item
                .get("timestamp")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            let oi = item
                .get("sumOpenInterest")
                .or_else(|| item.get("openInterest"))
                .and_then(|v| v.as_str())
                .and_then(parse_decimal_str)
                .unwrap_or_else(|| "?".to_string());
            let canon = CanonicalService::canonical_pair("binance", symbol)
                .unwrap_or_else(|| symbol.to_string());
            let event = OpenInterest {
                agent: "binance".into(),
                symbol: canon,
                open_interest: oi,
                timestamp: ts,
            };
            let line = serde_json::to_string(&event).unwrap();
            if tx.send(line).await.is_err() {
                return Ok(());
            }
        }

        if data.len() < LIMIT {
            break;
        }

        if let Some(last) = data
            .last()
            .and_then(|v| v.get("timestamp").and_then(|x| x.as_i64()))
        {
            start = last + 1;
        } else {
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}
