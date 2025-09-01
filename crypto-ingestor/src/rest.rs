use crate::{error::IngestorError, http_client, parse::parse_decimal_str, sink::DynSink};
use canonicalizer::{CanonicalService, FeeSchedule, FeeTier, Listing};
use chrono::Utc;
use serde::Serialize;

/// Fetch tradable listings from Binance US REST API and optionally stream them to a sink.
pub async fn fetch_binance_listings() -> Result<Vec<Listing>, IngestorError> {
    let client = http_client::builder()
        .build()
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;
    let resp: serde_json::Value = client
        .get("https://api.binance.us/api/v3/exchangeInfo")
        .send()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?
        .json()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;

    let now = Utc::now().timestamp_millis();
    let mut listings = Vec::new();
    if let Some(symbols) = resp.get("symbols").and_then(|s| s.as_array()) {
        for sym in symbols {
            if sym.get("status").and_then(|s| s.as_str()) != Some("TRADING") {
                continue;
            }
            let raw = match sym.get("symbol").and_then(|s| s.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let base = sym
                .get("baseAsset")
                .and_then(|b| b.as_str())
                .unwrap_or("")
                .to_string();
            let quote = sym
                .get("quoteAsset")
                .and_then(|q| q.as_str())
                .unwrap_or("")
                .to_string();
            let lot_size = sym
                .get("filters")
                .and_then(|f| f.as_array())
                .and_then(|arr| {
                    arr.iter().find_map(|flt| {
                        if flt.get("filterType").and_then(|ft| ft.as_str()) == Some("LOT_SIZE") {
                            flt.get("stepSize").and_then(|s| s.as_str()).and_then(parse_decimal_str)
                        } else {
                            None
                        }
                    })
                });
            let canon = CanonicalService::canonical_pair("binance", raw)
                .unwrap_or_else(|| raw.to_string());
            listings.push(Listing {
                agent: "binance".into(),
                r#type: "listing".into(),
                symbol: canon,
                base,
                quote,
                lot_size,
                timestamp: now,
            });
        }
    }
    Ok(listings)
}

/// Fetch tradable listings from Coinbase REST API.
pub async fn fetch_coinbase_listings() -> Result<Vec<Listing>, IngestorError> {
    let client = http_client::builder()
        .build()
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?;
    let resp: serde_json::Value = client
        .get("https://api.exchange.coinbase.com/products")
        .send()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?
        .json()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?;

    let now = Utc::now().timestamp_millis();
    let mut listings = Vec::new();
    if let Some(arr) = resp.as_array() {
        for prod in arr {
            if prod.get("trading_disabled").and_then(|v| v.as_bool()) == Some(true) {
                continue;
            }
            let id = match prod.get("id").and_then(|s| s.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let base = prod
                .get("base_currency")
                .and_then(|b| b.as_str())
                .unwrap_or("")
                .to_string();
            let quote = prod
                .get("quote_currency")
                .and_then(|q| q.as_str())
                .unwrap_or("")
                .to_string();
            let lot_size = prod
                .get("base_increment")
                .and_then(|s| s.as_str())
                .and_then(parse_decimal_str);
            let canon = CanonicalService::canonical_pair("coinbase", id)
                .unwrap_or_else(|| id.to_string());
            listings.push(Listing {
                agent: "coinbase".into(),
                r#type: "listing".into(),
                symbol: canon,
                base,
                quote,
                lot_size,
                timestamp: now,
            });
        }
    }
    Ok(listings)
}

/// Placeholder fee schedule fetch for Binance using public tier information.
pub async fn fetch_binance_fee_schedule() -> Result<FeeSchedule, IngestorError> {
    let tiers = vec![FeeTier {
        volume: 0.0,
        maker: 0.001,
        taker: 0.001,
    }];
    Ok(FeeSchedule {
        agent: "binance".into(),
        r#type: "fee_schedule".into(),
        symbol: None,
        tiers,
        timestamp: Utc::now().timestamp_millis(),
    })
}

/// Placeholder fee schedule fetch for Coinbase.
pub async fn fetch_coinbase_fee_schedule() -> Result<FeeSchedule, IngestorError> {
    let tiers = vec![FeeTier {
        volume: 0.0,
        maker: 0.004,
        taker: 0.006,
    }];
    Ok(FeeSchedule {
        agent: "coinbase".into(),
        r#type: "fee_schedule".into(),
        symbol: None,
        tiers,
        timestamp: Utc::now().timestamp_millis(),
    })
}

/// Stream serializable events to the provided sink.
async fn stream_json<T: Serialize>(value: &T, sink: &DynSink) -> Result<(), IngestorError> {
    let line = serde_json::to_string(value).map_err(|e| IngestorError::Other(e.to_string()))?;
    sink.send(&line).await
}

/// Stream a list of listings to the sink.
pub async fn stream_listings(list: &[Listing], sink: &DynSink) -> Result<(), IngestorError> {
    for l in list {
        stream_json(l, sink).await?;
    }
    Ok(())
}

/// Stream a fee schedule to the sink.
pub async fn stream_fee_schedule(fees: &FeeSchedule, sink: &DynSink) -> Result<(), IngestorError> {
    stream_json(fees, sink).await
}

