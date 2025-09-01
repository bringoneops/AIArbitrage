use ingestor::rest::{stream_fee_schedule, stream_listings};
use ingestor::sink::OutputSink;
use canonicalizer::{FeeSchedule, FeeTier, Listing};
use async_trait::async_trait;
use tokio::sync::Mutex;
use std::sync::Arc;

#[derive(Default)]
struct VecSink(Mutex<Vec<String>>);

#[async_trait]
impl OutputSink for VecSink {
    async fn send(&self, line: &str) -> Result<(), ingestor::error::IngestorError> {
        let mut guard = self.0.lock().await;
        guard.push(line.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn listing_events_are_streamed() {
    let sink = Arc::new(VecSink::default());
    let listings = vec![Listing {
        agent: "binance".into(),
        r#type: "listing".into(),
        symbol: "BTC-USDT".into(),
        base: "BTC".into(),
        quote: "USDT".into(),
        lot_size: Some("0.001".into()),
        timestamp: 0,
    }];
    let sink_dyn: Arc<dyn OutputSink> = sink.clone();
    stream_listings(&listings, &sink_dyn).await.unwrap();
    let guard = sink.0.lock().await;
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0], serde_json::to_string(&listings[0]).unwrap());
}

#[tokio::test]
async fn fee_schedule_is_streamed() {
    let sink = Arc::new(VecSink::default());
    let schedule = FeeSchedule {
        agent: "coinbase".into(),
        r#type: "fee_schedule".into(),
        symbol: None,
        tiers: vec![FeeTier { volume: 0.0, maker: 0.1, taker: 0.2 }],
        timestamp: 1,
    };
    let sink_dyn: Arc<dyn OutputSink> = sink.clone();
    stream_fee_schedule(&schedule, &sink_dyn).await.unwrap();
    let guard = sink.0.lock().await;
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0], serde_json::to_string(&schedule).unwrap());
}

