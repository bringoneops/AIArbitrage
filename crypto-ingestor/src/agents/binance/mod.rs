use futures_util::{StreamExt, SinkExt}; // <-- add SinkExt
use tokio_tungstenite::tungstenite::Message;

use crate::agent::Agent;

pub struct BinanceAgent {
    symbols: Vec<String>,
    max_reconnect_delay_secs: u64,
}

impl BinanceAgent {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols, max_reconnect_delay_secs: 30 }
    }

    fn build_ws_url(&self) -> String {
        // Combined stream: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
        let streams = self.symbols
            .iter()
            .map(|s| format!("{}@trade", s))
            .collect::<Vec<_>>()
            .join("/");
        format!("wss://stream.binance.com:9443/stream?streams={}", streams)
    }
}

#[async_trait::async_trait]
impl Agent for BinanceAgent {
    fn name(&self) -> &'static str { "binance" }

    async fn run(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt: u32 = 0;

        loop {
            if *shutdown.borrow() { break; }

            let url = self.build_ws_url();
            tracing::info!(%url, "connecting");

            match tokio_tungstenite::connect_async(&url).await {
                Ok((mut ws, _resp)) => {
                    tracing::info!("connected");
                    attempt = 0;

                    loop {
                        tokio::select! {
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() {
                                    tracing::info!("shutdown signal - closing connection");
                                    let _ = ws.close(None).await;
                                    return Ok(());
                                }
                            }
                            msg = ws.next() => {
                                match msg {
                                    Some(Ok(Message::Text(txt))) => {
                                        // Combined stream payload: {"stream":"btcusdt@trade","data":{...}}
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                            let d = v.get("data").unwrap_or(&serde_json::Value::Null);
                                            let sym = d.get("s").and_then(|s| s.as_str()).unwrap_or("?");
                                            let trade_id = d.get("t").and_then(|t| t.as_i64()).unwrap_or_default();
                                            let px = d.get("p").and_then(|p| p.as_str()).unwrap_or("?");
                                            let qty = d.get("q").and_then(|q| q.as_str()).unwrap_or("?");
                                            let ts = d.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
                                            println!(r#"{{"agent":"binance","type":"trade","s":"{}","t":{},"p":"{}","q":"{}","ts":{}}}"#, sym, trade_id, px, qty, ts);
                                        } else {
                                            tracing::warn!("non-json text msg");
                                        }
                                    }
                                    Some(Ok(Message::Binary(_))) => { /* ignore */ }
                                    Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; } // <-- works now
                                    Some(Ok(Message::Pong(_))) => { /* ignore */ }
                                    Some(Ok(Message::Close(frame))) => { tracing::warn!(?frame, "server closed connection"); break; }
                                    Some(Err(e)) => { tracing::error!(error=%e, "ws error"); break; }
                                    None => { tracing::warn!("stream ended"); break; }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error=%e, "connect failed");
                }
            }

            // Exponential backoff: 1,2,4,8,16, then cap at 30s
            attempt = attempt.saturating_add(1);
            let exp: u32 = attempt.saturating_sub(1).min(4); // 0..=4
            let delay = (1u64 << exp).min(self.max_reconnect_delay_secs);
            let sleep = std::time::Duration::from_secs(delay);

            tracing::info!(?sleep, "reconnecting");
            tokio::select! {
                _ = tokio::time::sleep(sleep) => {},
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("shutdown during backoff");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
