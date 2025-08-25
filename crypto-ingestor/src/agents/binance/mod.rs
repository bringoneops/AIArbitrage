use futures_util::{SinkExt, StreamExt}; // <-- add SinkExt
use tokio_tungstenite::tungstenite::Message;

use crate::agent::Agent;

/// Fetch all tradable symbols from Binance US REST API.
pub async fn fetch_all_symbols(
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let resp: serde_json::Value = reqwest::Client::new()
        .get("https://api.binance.us/api/v3/exchangeInfo")
        .send()
        .await?
        .json()
        .await?;

    let symbols = resp
        .get("symbols")
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s
            .get("status")
            .and_then(|st| st.as_str())
            == Some("TRADING"))
        .filter_map(|s| {
            s.get("symbol")
                .and_then(|sym| sym.as_str())
                .map(|sym| sym.to_lowercase())
        })
        .collect();

    Ok(symbols)
}

pub struct BinanceAgent {
    symbols: Vec<String>,
    max_reconnect_delay_secs: u64,
}

impl BinanceAgent {
    pub async fn new(
        symbols: Option<Vec<String>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let symbols = match symbols {
            Some(v) => v,
            None => fetch_all_symbols().await?,
        };

        Ok(Self {
            symbols,
            max_reconnect_delay_secs: 30,
        })
    }

    fn build_ws_url(&self) -> String {
        // Base websocket endpoint; subscriptions are sent after connecting.
        "wss://stream.binance.us:9443/ws".to_string()
    }
}

#[async_trait::async_trait]
impl Agent for BinanceAgent {
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn run(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt: u32 = 0;

        loop {
            if *shutdown.borrow() {
                break;
            }

            let url = self.build_ws_url();
            tracing::info!(%url, "connecting");

            match tokio_tungstenite::connect_async(&url).await {
                Ok((mut ws, _resp)) => {
                    tracing::info!("connected");
                    attempt = 0;

                    let params = self
                        .symbols
                        .iter()
                        .map(|s| format!("{}@trade", s))
                        .collect::<Vec<_>>();
                    let sub_msg = serde_json::json!({
                        "method": "SUBSCRIBE",
                        "params": params,
                        "id": 1,
                    });
                    if let Err(e) = ws.send(Message::Text(sub_msg.to_string())).await {
                        tracing::error!(error=%e, "failed to send subscription");
                        continue;
                    }

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
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                            if v.get("id").and_then(|id| id.as_i64()) == Some(1) {
                                                if let Some(err) = v.get("error") {
                                                    tracing::error!(?err, "subscription error");
                                                    break;
                                                } else {
                                                    tracing::info!("subscription acknowledged");
                                                }
                                                continue;
                                            }

                                            let sym = v.get("s").and_then(|s| s.as_str()).unwrap_or("?");
                                            let trade_id = v.get("t").and_then(|t| t.as_i64()).unwrap_or_default();
                                            let px = v.get("p").and_then(|p| p.as_str()).unwrap_or("?");
                                            let qty = v.get("q").and_then(|q| q.as_str()).unwrap_or("?");
                                            let ts = v.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
                                            println!(r#"{{"agent":"binance","type":"trade","s":"{}","t":{},"p":"{}","q":"{}","ts":{}}}"#, sym, trade_id, px, qty, ts);
                                        } else {
                                            tracing::warn!("non-json text msg");
                                        }
                                    }
                                    Some(Ok(Message::Binary(_))) => { /* ignore */ }
                                    Some(Ok(Message::Frame(_))) => { /* ignore */ }
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
