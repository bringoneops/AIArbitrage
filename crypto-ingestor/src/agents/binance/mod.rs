use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::{agent::Agent, config::Settings, http_client};
use canonicalizer::CanonicalService;

const MAX_STREAMS_PER_CONN: usize = 1024; // per Binance docs

/// Fetch all tradable symbols from Binance US REST API.
pub async fn fetch_all_symbols() -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = http_client::builder().build()?;
    let resp: serde_json::Value = client
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
        .filter(|s| {
            s.get("quoteAsset")
                .and_then(|q| q.as_str())
                .map(|q| q.eq_ignore_ascii_case("USD") || q.eq_ignore_ascii_case("USDT"))
                .unwrap_or(false)
        })
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
    ws_url: String,
    max_reconnect_delay_secs: u64,
    refresh_interval_mins: u64,
}

impl BinanceAgent {
    pub async fn new(
        symbols: Option<Vec<String>>,
        cfg: &Settings,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let symbols = match symbols {
            Some(v) => v,
            None => fetch_all_symbols().await?,
        };

        Ok(Self {
            symbols,
            ws_url: cfg.binance_ws_url.clone(),
            max_reconnect_delay_secs: cfg.binance_max_reconnect_delay_secs,
            refresh_interval_mins: cfg.binance_refresh_interval_mins,
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
        out_tx: mpsc::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut handles = Vec::new();
        let mut symbol_txs = Vec::new();

        let chunks = self
            .symbols
            .chunks(MAX_STREAMS_PER_CONN)
            .map(|c| c.to_vec())
            .collect::<Vec<_>>();

        for chunk in chunks {
            let (sym_tx, rx) = tokio::sync::watch::channel(chunk);
            symbol_txs.push(sym_tx);
            let shutdown_rx = shutdown.clone();
            let max_delay = self.max_reconnect_delay_secs;
            let ws_url = self.ws_url.clone();
            let tx_clone = out_tx.clone();
            handles.push(tokio::spawn(async move {
                connection_task(rx, shutdown_rx, tx_clone, ws_url, max_delay).await;
            }));
        }

        let mut refresh = tokio::time::interval(std::time::Duration::from_secs(
            60 * self.refresh_interval_mins,
        ));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
                _ = refresh.tick() => {
                    match fetch_all_symbols().await {
                        Ok(new_symbols) => {
                            let new_set: HashSet<_> = new_symbols.iter().cloned().collect();
                            let old_set: HashSet<_> = self.symbols.iter().cloned().collect();
                            let added: Vec<_> = new_set.difference(&old_set).cloned().collect();
                            let removed: Vec<_> = old_set.difference(&new_set).cloned().collect();

                            if added.is_empty() && removed.is_empty() {
                                tracing::info!("symbol refresh: no changes");
                            } else {
                                tracing::info!(?added, ?removed, total=new_symbols.len(), "symbol refresh");
                                self.symbols = new_symbols;

                                let new_chunks = self
                                    .symbols
                                    .chunks(MAX_STREAMS_PER_CONN)
                                    .map(|c| c.to_vec())
                                    .collect::<Vec<_>>();

                                if new_chunks.len() == symbol_txs.len() {
                                    for (tx, chunk) in symbol_txs.iter().zip(new_chunks.iter()) {
                                        let _ = tx.send(chunk.clone());
                                    }
                                } else if new_chunks.len() > symbol_txs.len() {
                                    for (tx, chunk) in symbol_txs.iter().zip(new_chunks.iter()) {
                                        let _ = tx.send(chunk.clone());
                                    }
                                    for chunk in new_chunks.iter().skip(symbol_txs.len()) {
                                        let (sym_tx, rx) = tokio::sync::watch::channel(chunk.clone());
                                        symbol_txs.push(sym_tx);
                                        let shutdown_rx = shutdown.clone();
                                        let tx_conn = out_tx.clone();
                                        let max_delay = self.max_reconnect_delay_secs;
                                        let ws_url = self.ws_url.clone();
                                        handles.push(tokio::spawn(async move {
                                            connection_task(rx, shutdown_rx, tx_conn, ws_url, max_delay).await;
                                        }));
                                    }
                                } else {
                                    for (tx, chunk) in symbol_txs.iter().zip(new_chunks.iter()) {
                                        let _ = tx.send(chunk.clone());
                                    }
                                    let extra_txs = symbol_txs.split_off(new_chunks.len());
                                    let extra_handles = handles.split_off(new_chunks.len());
                                    for tx in &extra_txs {
                                        let _ = tx.send(Vec::new());
                                    }
                                    drop(extra_txs);
                                    for h in extra_handles {
                                        let _ = h.await;
                                    }
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
    tx: mpsc::Sender<String>,
    ws_url: String,
    max_reconnect_delay_secs: u64,
) {
    let mut attempt: u32 = 0;

    loop {
        if *shutdown.borrow() {
            break;
        }

        tracing::info!(url = %ws_url, "connecting");
        let mut current_symbols = symbols_rx.borrow().clone();
        match connect_async(&ws_url).await {
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
                                    let new_set: HashSet<_> = new_syms.iter().cloned().collect();
                                    let old_set: HashSet<_> = current_symbols.iter().cloned().collect();
                                    let to_sub: Vec<_> = new_set.difference(&old_set).cloned().collect();
                                    let to_unsub: Vec<_> = old_set.difference(&new_set).cloned().collect();

                                    if !to_unsub.is_empty() {
                                        let _ = send_unsubscribe(&mut ws, &to_unsub).await;
                                    }
                                    if !to_sub.is_empty() {
                                        if let Err(e) = send_subscribe(&mut ws, &to_sub).await {
                                            tracing::error!(error=%e, "failed to update subscription");
                                            break;
                                        }
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

                                        let raw = v.get("s").and_then(|s| s.as_str()).unwrap_or("?");
                                        let sym = CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
                                        // Missing or non-positive trade IDs are represented as JSON null.
                                        let trade_id = v
                                            .get("t")
                                            .and_then(|t| t.as_i64())
                                            .filter(|id| *id > 0);
                                        let px = v
                                            .get("p")
                                            .and_then(|p| p.as_str())
                                            .and_then(|p| p.parse::<Decimal>().ok())
                                            .map(|p| format!("{:.28}", p.round_dp(28)))
                                            .unwrap_or_else(|| "?".to_string());
                                        let qty = v
                                            .get("q")
                                            .and_then(|q| q.as_str())
                                            .and_then(|q| q.parse::<Decimal>().ok())
                                            .map(|q| format!("{:.28}", q.round_dp(28)))
                                            .unwrap_or_else(|| "?".to_string());
                                        let ts = v.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
                                        let line = serde_json::json!({
                                            "agent": "binance",
                                            "type": "trade",
                                            "s": sym,
                                            "t": trade_id,
                                            "p": px,
                                            "q": qty,
                                            "ts": ts
                                        }).to_string();
                                        if tx.send(line).await.is_err() {
                                            break;
                                        }
                                    } else {
                                        tracing::warn!("non-json text msg");
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Close(frame))) => { tracing::warn!(?frame, "server closed connection"); break; }
                                Some(Ok(_)) => { }
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
