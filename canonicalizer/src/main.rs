use serde_json::Value;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};

use canonicalizer::CanonicalService;

#[tokio::main]
async fn main() -> io::Result<()> {
    CanonicalService::init().await;

    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = io::stdout();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(mut v) => {
                if let (Some(exchange), Some(pair)) = (
                    v.get("agent").and_then(|a| a.as_str()),
                    v.get("s").and_then(|s| s.as_str()),
                ) {
                    if let Some(canon) = CanonicalService::canonical_pair(exchange, pair) {
                        v["s"] = Value::String(canon);
                    }
                }
                let out = serde_json::to_string(&v).unwrap_or(line);
                stdout.write_all(out.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
            }
            Err(_) => {
                stdout.write_all(line.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
            }
        }
    }

    Ok(())
}
