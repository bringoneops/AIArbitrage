use analytics::{spawn, Trade};
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

    let (tx, mut rx) = spawn(threshold);

    // Task to read canonicalized trades from STDIN and forward to analytics
    tokio::spawn(async move {
        let stdin = io::BufReader::new(io::stdin());
        let mut lines = stdin.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(v) => {
                    if let Ok(trade) = serde_json::from_value::<Trade>(v) {
                        let _ = tx.send(trade).await;
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
