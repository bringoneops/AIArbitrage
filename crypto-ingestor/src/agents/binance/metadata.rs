use std::collections::HashMap;

use canonicalizer::{CanonicalService, FeeSchedule, FeeTier, Listing};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::{error::IngestorError, http_client, sink::DynSink};

/// Poll Binance REST endpoints for listing and fee metadata and emit canonical events.
pub async fn run(mut shutdown: tokio::sync::watch::Receiver<bool>, sink: DynSink) {
    let mut prev_listings: HashMap<String, Listing> = HashMap::new();
    let mut prev_fee: Option<FeeSchedule> = None;

    if let Ok((listings, fee)) = fetch().await {
        for listing in listings.values() {
            if let Ok(line) = serde_json::to_string(listing) {
                let _ = sink.send(&line).await;
            }
        }
        if let Ok(line) = serde_json::to_string(&fee) {
            let _ = sink.send(&line).await;
        }
        prev_listings = listings;
        prev_fee = Some(fee);
    }

    let mut ticker = interval(Duration::from_secs(60 * 60 * 24));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = ticker.tick() => {
                match fetch().await {
                    Ok((listings, fee)) => {
                        for (sym, listing) in &listings {
                            if prev_listings.get(sym) != Some(listing) {
                                if let Ok(line) = serde_json::to_string(listing) {
                                    let _ = sink.send(&line).await;
                                }
                            }
                        }
                        if prev_fee.as_ref() != Some(&fee) {
                            if let Ok(line) = serde_json::to_string(&fee) {
                                let _ = sink.send(&line).await;
                            }
                        }
                        prev_listings = listings;
                        prev_fee = Some(fee);
                    }
                    Err(e) => {
                        tracing::error!(error=%e, "binance metadata fetch");
                    }
                }
            }
        }
    }
}

async fn fetch() -> Result<(HashMap<String, Listing>, FeeSchedule), IngestorError> {
    let client = http_client::builder()
        .build()
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;

    let exchange_info: serde_json::Value = client
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

    // poll system status endpoint for completeness; ignore errors/response
    let _ = client
        .get("https://api.binance.us/sapi/v1/system/status")
        .send()
        .await;

    let ts = Utc::now().timestamp_millis();
    let mut listings = HashMap::new();
    if let Some(arr) = exchange_info.get("symbols").and_then(|v| v.as_array()) {
        for sym in arr {
            if sym.get("status").and_then(|s| s.as_str()) != Some("TRADING") {
                continue;
            }
            let raw = sym.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            let base = sym.get("baseAsset").and_then(|s| s.as_str()).unwrap_or("");
            let quote = sym.get("quoteAsset").and_then(|s| s.as_str()).unwrap_or("");
            let lot_size = sym
                .get("filters")
                .and_then(|f| f.as_array())
                .and_then(|fa| {
                    fa.iter().find_map(|flt| {
                        if flt.get("filterType").and_then(|t| t.as_str()) == Some("LOT_SIZE") {
                            flt.get("stepSize").and_then(|s| s.as_str())
                        } else {
                            None
                        }
                    })
                })
                .map(|s| s.to_string());
            let canon =
                CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
            let listing = Listing {
                agent: "binance".into(),
                r#type: "listing".into(),
                symbol: canon,
                base: base.to_string(),
                quote: quote.to_string(),
                lot_size,
                timestamp: ts,
            };
            listings.insert(raw.to_string(), listing);
        }
    }

    let fee = FeeSchedule {
        agent: "binance".into(),
        r#type: "fee_schedule".into(),
        symbol: None,
        tiers: vec![FeeTier {
            volume: 0.0,
            maker: 0.001,
            taker: 0.001,
        }],
        timestamp: ts,
    };

    Ok((listings, fee))
}
