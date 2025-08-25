use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::agent::Agent;

const MAX_STREAMS_PER_CONN: usize = 1024; // per Binance docs
const WS_URL: &str = "wss://stream.binance.us:9443/ws";

/// Fetch all tradable symbols from Binance US REST API.
pub async fn fetch_all_symbols() -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
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
        .filter(|s| s.get("status").and_then(|st| st.as_str()) == Some("TRADING"))
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
        let mut handles = Vec::new();
        let mut symbol_txs = Vec::new();

        let chunks = self
            .symbols
            .chunks(MAX_STREAMS_PER_CONN)
            .map(|c| c.to_vec())
            .collect::<Vec<_>>();

        for chunk in chunks {
            let (tx, rx) = tokio::sync::watch::channel(chunk);
            symbol_txs.push(tx);
            let shutdown_rx = shutdown.clone();
            let max_delay = self.max_reconnect_delay_secs;
            handles.push(tokio::spawn(async move {
                connection_task(rx, shutdown_rx, max_delay).await;
            }));
        }

        let mut refresh = tokio::time::interval(std::time::Duration::from_secs(60 * 60));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
                _ = refresh.tick() => {
                    match fetch_all_symbols().await {
                        Ok(new_symbols) => {
                            if new_symbols != self.symbols {
                                self.symbols = new_symbols;
                                let new_chunks = self.symbols
                                    .chunks(MAX_STREAMS_PER_CONN)
                                    .map(|c| c.to_vec())
                                    .collect::<Vec<_>>();

                                if new_chunks.len() == symbol_txs.len() {
                                    for (tx, chunk) in symbol_txs.iter().zip(new_chunks.iter()) {
                                        let _ = tx.send(chunk.clone());
                                    }
                                } else {
                                    drop(symbol_txs);
                                    for h in handles.drain(..) { let _ = h.await; }
                                    let mut new_txs = Vec::new();
                                    let mut new_handles = Vec::new();
                                    for chunk in new_chunks {
                                        let (tx, rx) = tokio::sync::watch::channel(chunk);
                                        new_txs.push(tx);
                                        let shutdown_rx = shutdown.clone();
                                        let max_delay = self.max_reconnect_delay_secs;
                                        new_handles.push(tokio::spawn(async move {
                                            connection_task(rx, shutdown_rx, max_delay).await;
                                        }));
                                    }
                                    symbol_txs = new_txs;
                                    handles = new_handles;
                                }
                            }
                        }
                        Err(e) => tracing::error!(error=%e, "failed to refresh symbols"),
                    }
                }
            }
        }

        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }
}

async fn connection_task(
    mut symbols_rx: tokio::sync::watch::Receiver<Vec<String>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    max_reconnect_delay_secs: u64,
) {
    let mut attempt: u32 = 0;

    loop {
        if *shutdown.borrow() {
            break;
        }

        tracing::info!(url = WS_URL, "connecting");
        let mut current_symbols = symbols_rx.borrow().clone();
        match connect_async(WS_URL).await {
            Ok((mut ws, _)) => {
                tracing::info!("connected");
                attempt = 0;

                if let Err(e) = send_subscribe(&mut ws, &current_symbols).await {
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
                        changed = symbols_rx.changed() => {
                            if changed.is_ok() {
                                let new_syms = symbols_rx.borrow().clone();
                                if new_syms != current_symbols {
                                    let _ = send_unsubscribe(&mut ws, &current_symbols).await;
                                    if let Err(e) = send_subscribe(&mut ws, &new_syms).await {
                                        tracing::error!(error=%e, "failed to update subscription");
                                        break;
                                    }
                                    current_symbols = new_syms;
                                }
                            } else {
                                break;
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
    let params = symbols
        .iter()
        .map(|s| format!("{}@trade", s))
        .collect::<Vec<_>>();
    let sub_msg = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": params,
        "id": 1,
    });
    ws.send(Message::Text(sub_msg.to_string())).await
}

async fn send_unsubscribe(
    ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    symbols: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    if symbols.is_empty() {
        return Ok(());
    }
    let params = symbols
        .iter()
        .map(|s| format!("{}@trade", s))
        .collect::<Vec<_>>();
    let msg = serde_json::json!({
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": 1,
    });
    ws.send(Message::Text(msg.to_string())).await
}
