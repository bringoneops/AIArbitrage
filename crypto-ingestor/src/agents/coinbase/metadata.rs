use std::collections::HashMap;

use canonicalizer::{CanonicalService, FeeSchedule, FeeTier, Listing};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::{error::IngestorError, http_client, sink::DynSink};

/// Poll Coinbase REST endpoints for listing and fee metadata and emit canonical events.
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
                        tracing::error!(error=%e, "coinbase metadata fetch");
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
            exchange: "coinbase",
            symbol: None,
        })?;

    let products: serde_json::Value = client
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

    let fee_resp = client
        .get("https://api.exchange.coinbase.com/fees")
        .send()
        .await;
    let fee_value = match fee_resp {
        Ok(resp) => resp.json::<serde_json::Value>().await.unwrap_or_default(),
        Err(_) => serde_json::Value::Null,
    };

    let ts = Utc::now().timestamp_millis();
    let mut listings = HashMap::new();
    if let Some(arr) = products.as_array() {
        for prod in arr {
            if prod.get("quote_currency").and_then(|q| q.as_str()) != Some("USD") {
                continue;
            }
            let id = prod.get("id").and_then(|i| i.as_str()).unwrap_or("");
            let base = prod
                .get("base_currency")
                .and_then(|s| s.as_str())
                .unwrap_or("");
            let quote = prod
                .get("quote_currency")
                .and_then(|s| s.as_str())
                .unwrap_or("");
            let lot_size = prod
                .get("base_increment")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            let canon =
                CanonicalService::canonical_pair("coinbase", id).unwrap_or_else(|| id.to_string());
            let listing = Listing {
                agent: "coinbase".into(),
                r#type: "listing".into(),
                symbol: canon,
                base: base.to_string(),
                quote: quote.to_string(),
                lot_size,
                timestamp: ts,
            };
            listings.insert(id.to_string(), listing);
        }
    }

    let maker = fee_value
        .get("maker_fee_rate")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let taker = fee_value
        .get("taker_fee_rate")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let fee = FeeSchedule {
        agent: "coinbase".into(),
        r#type: "fee_schedule".into(),
        symbol: None,
        tiers: vec![FeeTier {
            volume: 0.0,
            maker,
            taker,
        }],
        timestamp: ts,
    };

    Ok((listings, fee))
}
