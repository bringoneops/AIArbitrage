use analytics::{spawn, Trade};
use canonicalizer::{Candle, Ticker};
use serde_json::Value;
use tokio::io::{self, AsyncBufReadExt};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder().with_target(false).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let threshold = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);

    let (trade_tx, candle_tx, ticker_tx, mut rx) = spawn(threshold);

    // Task to read canonicalized events from STDIN and forward to analytics
    tokio::spawn(async move {
        let stdin = io::BufReader::new(io::stdin());
        let mut lines = stdin.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(v) => {
                    match v.get("type").and_then(|t| t.as_str()) {
                        Some("trade") => {
                            if let Ok(trade) = serde_json::from_value::<Trade>(v.clone()) {
                                let _ = trade_tx.send(trade).await;
                            }
                        }
                        Some("candle") => {
                            if let Ok(candle) = serde_json::from_value::<Candle>(v.clone()) {
                                let _ = candle_tx.send(candle).await;
                            }
                        }
                        Some("ticker") => {
                            if let Ok(ticker) = serde_json::from_value::<Ticker>(v.clone()) {
                                let _ = ticker_tx.send(ticker).await;
                            }
                        }
                        _ => {}
                    }
                }
                Err(_) => {
                    // ignore malformed input
                }
            }
        }
    });

    // Print spread events as JSON lines
    while let Ok(event) = rx.recv().await {
        println!("{}", serde_json::to_string(&event)?);
    }

    Ok(())
}
