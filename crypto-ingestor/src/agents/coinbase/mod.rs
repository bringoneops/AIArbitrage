use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::agent::Agent;

const WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

/// Fetch all tradable USD product IDs from Coinbase.
pub async fn fetch_all_symbols() -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let products: serde_json::Value = reqwest::Client::new()
        .get("https://api.exchange.coinbase.com/products")
        .send()
        .await?
        .json()
        .await?;

    let arr = products.as_array().ok_or("unexpected response")?;
    let mut symbols = Vec::new();
    for prod in arr {
        if prod
            .get("quote_currency")
            .and_then(|q| q.as_str())
            == Some("USD")
        {
            if let Some(id) = prod.get("id").and_then(|i| i.as_str()) {
                symbols.push(id.to_string());
            }
        }
    }
    Ok(symbols)
}

pub struct CoinbaseAgent {
    symbols: Vec<String>,
    max_reconnect_delay_secs: u64,
}

impl CoinbaseAgent {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            max_reconnect_delay_secs: 30,
        }
    }
}

#[async_trait::async_trait]
impl Agent for CoinbaseAgent {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    async fn run(
        &mut self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        connection_task(self.symbols.clone(), shutdown, self.max_reconnect_delay_secs).await;
        Ok(())
    }
}

async fn connection_task(
    symbols: Vec<String>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    max_reconnect_delay_secs: u64,
) {
    let mut attempt: u32 = 0;

    loop {
        if *shutdown.borrow() {
            break;
        }

        tracing::info!(url = WS_URL, "connecting");
        match connect_async(WS_URL).await {
            Ok((mut ws, _)) => {
                tracing::info!("connected");
                attempt = 0;

                if let Err(e) = send_subscribe(&mut ws, &symbols).await {
                    tracing::error!(error=%e, "failed to send subscription");
                    continue;
                }

                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                tracing::info!("shutdown signal - closing connection");
                                let _ = ws.close(None).await;
                                return;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                        let typ = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                                        if typ == "match" || typ == "ticker" {
                                            let sym = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                            let price = v.get("price").and_then(|p| p.as_str()).unwrap_or("?");
                                            let size = v.get("size").and_then(|q| q.as_str()).unwrap_or("?");
                                            let time = v.get("time").and_then(|t| t.as_str()).unwrap_or("?");
                                            println!(r#"{{"agent":"coinbase","type":"trade","s":"{}","p":"{}","q":"{}","ts":"{}"}}"#, sym, price, size, time);
                                        }
                                    } else {
                                        tracing::warn!("non-json text msg");
                                    }
                                }
                                Some(Ok(Message::Binary(_))) => { }
                                Some(Ok(Message::Frame(_))) => { }
                                Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Pong(_))) => { }
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

        attempt = attempt.saturating_add(1);
        let exp: u32 = attempt.saturating_sub(1).min(4);
        let delay = (1u64 << exp).min(max_reconnect_delay_secs);
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
}

async fn send_subscribe(
    ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    symbols: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let msg = serde_json::json!({
        "type": "subscribe",
        "product_ids": symbols,
        "channels": ["matches"],
    });
    ws.send(Message::Text(msg.to_string())).await
}
