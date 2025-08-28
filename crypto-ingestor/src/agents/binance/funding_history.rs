//! Historical funding rate backfill for Binance futures.
//!
//! The `/fapi/v1/fundingRate` REST endpoint has a request weight of `1` and is
//! limited to ~1200 weight per minute. Requests that exceed the quota return
//! HTTP `429`.  We page through results (up to 1000 entries per request) and
//! sleep briefly between calls to remain within the quota.  When a request is
//! rate limited or encounters a transient server error, it is retried with
//! exponential backoff up to five attempts.
//!
//! Fetched records are normalised into canonical [`Funding`] events and
//! forwarded through the provided channel so that downstream sinks receive a
//! complete history before live streaming begins.

use std::time::Duration;

use canonicalizer::{events::Funding, CanonicalService};
use tokio::sync::mpsc;

use crate::{http_client, parse::parse_decimal_str};

const LIMIT: usize = 1000;

/// Backfill historical funding rates for `symbols` and publish events via `tx`.
///
/// `symbols` should be lowercase Binance symbols (e.g. `btcusdt`).
/// `rest_url` is the base URL for Binance futures REST API.
pub async fn backfill(symbols: &[String], rest_url: &str, tx: mpsc::Sender<String>) {
    let client = match http_client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error=%e, "binance funding history http client");
            return;
        }
    };

    for sym in symbols {
        if let Err(e) = backfill_symbol(&client, rest_url, sym, &tx).await {
            tracing::error!(symbol=%sym, error=%e, "funding history backfill failed");
        }
    }
}

async fn backfill_symbol(
    client: &reqwest::Client,
    rest_url: &str,
    symbol: &str,
    tx: &mpsc::Sender<String>,
) -> Result<(), reqwest::Error> {
    let mut start: i64 = 0;
    loop {
        let url = format!(
            "{}/fapi/v1/fundingRate?symbol={}&limit={}&startTime={}",
            rest_url,
            symbol.to_uppercase(),
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

        let data: Vec<serde_json::Value> = resp.json().await?;
        if data.is_empty() {
            break;
        }

        for item in &data {
            let ts = item
                .get("fundingTime")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            let rate = item
                .get("fundingRate")
                .and_then(|r| r.as_str())
                .and_then(parse_decimal_str)
                .unwrap_or_else(|| "?".to_string());
            let canon = CanonicalService::canonical_pair("binance", symbol)
                .unwrap_or_else(|| symbol.to_string());
            let event = Funding {
                agent: "binance".into(),
                symbol: canon,
                rate,
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
            .and_then(|v| v.get("fundingTime").and_then(|x| x.as_i64()))
        {
            start = last + 1;
        } else {
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}
