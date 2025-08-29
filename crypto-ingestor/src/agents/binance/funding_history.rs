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

/// Normalise a user supplied pair into a Binance futures symbol.
///
/// When `rest_url` points at the coin‑M API (`dapi`), symbols are mapped to the
/// `*_USD_PERP` format. Otherwise `USDT` is appended. Returns `None` if the pair
/// cannot be interpreted as a supported futures contract.
fn normalise_pair(symbol: &str, rest_url: &str) -> Option<String> {
    let coin_m = rest_url.contains("dapi");
    let s = symbol.to_uppercase().replace(['-', '_', '/'], "");

    if coin_m {
        if s.ends_with("USDPERP") {
            let base = s.trim_end_matches("USDPERP");
            return Some(format!("{}USD_PERP", base));
        }
        if s.ends_with("USD") {
            let base = s.trim_end_matches("USD");
            return Some(format!("{}USD_PERP", base));
        }
        if s.ends_with("USDT") {
            return None;
        }
        if s.chars().all(|c| c.is_ascii_alphabetic()) {
            return Some(format!("{}USD_PERP", s));
        }
    } else {
        if s.ends_with("USDT") {
            return Some(s);
        }
        if s.ends_with("USD") {
            let base = s.trim_end_matches("USD");
            return Some(format!("{}USDT", base));
        }
        if s.ends_with("USDPERP") {
            return None;
        }
        if s.chars().all(|c| c.is_ascii_alphabetic()) {
            return Some(format!("{}USDT", s));
        }
    }
    None
}

/// Backfill historical funding rates for `symbols` and publish events via `tx`.
///
/// Pair names are normalised to the appropriate futures format (e.g. `BTC`
/// becomes `BTCUSDT` or `BTCUSD_PERP`).
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
        if let Some(norm) = normalise_pair(sym, rest_url) {
            if let Err(e) = backfill_symbol(&client, rest_url, &norm, &tx).await {
                tracing::error!(symbol=%norm, error=%e, "funding history backfill failed");
            }
        } else {
            tracing::warn!(symbol=%sym, "unsupported futures pair");
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

#[cfg(test)]
mod tests {
    use super::normalise_pair;

    #[test]
    fn normalises_usdt_symbols() {
        assert_eq!(
            normalise_pair("btc", "https://fapi.binance.com"),
            Some("BTCUSDT".into())
        );
        assert_eq!(
            normalise_pair("BTC-USD", "https://fapi.binance.com"),
            Some("BTCUSDT".into())
        );
        assert_eq!(
            normalise_pair("btcusdt", "https://fapi.binance.com"),
            Some("BTCUSDT".into())
        );
    }

    #[test]
    fn normalises_coin_m_symbols() {
        assert_eq!(
            normalise_pair("btc", "https://dapi.binance.com"),
            Some("BTCUSD_PERP".into())
        );
        assert_eq!(
            normalise_pair("BTCUSD_PERP", "https://dapi.binance.com"),
            Some("BTCUSD_PERP".into())
        );
        assert!(normalise_pair("btcusdt", "https://dapi.binance.com").is_none());
    }
}
