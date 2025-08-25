use serde::Deserialize;
use serde_json::Value;
use tabwriter::TabWriter;
use tokio::io::{self as aio, AsyncBufReadExt, AsyncWriteExt};
use std::io::{self, Write};

use canonicalizer::CanonicalService;

#[derive(Deserialize)]
struct Record {
    agent: Option<String>,
    s: Option<String>,
    p: Option<Value>,
    q: Option<Value>,
}

#[tokio::main]
async fn main() -> aio::Result<()> {
    CanonicalService::init().await;

    let json_output = std::env::args().any(|a| a == "--json");

    let stdin = aio::BufReader::new(aio::stdin());
    let mut lines = stdin.lines();

    if json_output {
        let mut stdout = aio::stdout();

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
        stdout.flush().await?;
    } else {
        let stdout = io::stdout();
        let mut tw = TabWriter::new(stdout);

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
                    if let Ok(rec) = serde_json::from_value::<Record>(v) {
                        let agent = rec.agent.unwrap_or_default();
                        let s = rec.s.unwrap_or_default();
                        let p = rec.p.map(|p| p.to_string()).unwrap_or_default();
                        let q = rec.q.map(|q| q.to_string()).unwrap_or_default();
                        writeln!(tw, "{}\t{}\t{}\t{}", agent, s, p, q)?;
                    } else {
                        writeln!(tw, "{}", line)?;
                    }
                }
                Err(_) => {
                    writeln!(tw, "{}", line)?;
                }
            }
        }
        tw.flush()?;
    }

    Ok(())
}
