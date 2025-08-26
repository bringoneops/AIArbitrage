use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::{
    agent::Agent,
    config::Settings,
    error::IngestorError,
    http_client,
    metrics::{ACTIVE_CONNECTIONS, LAST_TRADE_TIMESTAMP, MESSAGES_INGESTED},
    parse::parse_decimal_str,
};

use super::{shared_symbols, AgentFactory};
use canonicalizer::CanonicalService;

const MAX_STREAMS_PER_CONN: usize = 1024; // per Binance docs

/// Fetch all tradable symbols from Binance US REST API.
pub async fn fetch_all_symbols() -> Result<Vec<String>, IngestorError> {
    let client = http_client::builder()
        .build()
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;
    let resp: serde_json::Value = client
        .get("https://api.binance.us/api/v3/exchangeInfo")
        .send()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?
        .json()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;

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
    kline_intervals: Vec<String>,
}

impl BinanceAgent {
    pub async fn new(symbols: Option<Vec<String>>, cfg: &Settings) -> Result<Self, IngestorError> {
        let symbols = match symbols {
            Some(v) => v,
            None => fetch_all_symbols().await?,
        };

        Ok(Self {
            symbols,
            ws_url: cfg.binance_ws_url.clone(),
            max_reconnect_delay_secs: cfg.binance_max_reconnect_delay_secs,
            refresh_interval_mins: cfg.binance_refresh_interval_mins,
            kline_intervals: cfg.kline_intervals.clone(),
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
    ) -> Result<(), IngestorError> {
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
            let intervals = self.kline_intervals.clone();
            let tx_clone = out_tx.clone();
            handles.push(tokio::spawn(async move {
                connection_task(rx, shutdown_rx, tx_clone, ws_url, max_delay, intervals).await;
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
                                        let intervals = self.kline_intervals.clone();
                                        handles.push(tokio::spawn(async move {
                                            connection_task(rx, shutdown_rx, tx_conn, ws_url, max_delay, intervals).await;
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

pub struct BinanceFactory;

#[async_trait::async_trait]
impl AgentFactory for BinanceFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let symbols = if spec.is_empty() || spec.eq_ignore_ascii_case("all") {
            match shared_symbols().await {
                Ok((b, _)) => Some(b),
                Err(e) => {
                    tracing::error!(error=%e, "failed to fetch shared symbols");
                    return None;
                }
            }
        } else {
            Some(
                spec.split(',')
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>(),
            )
        };

        match BinanceAgent::new(symbols, cfg).await {
            Ok(agent) => Some(Box::new(agent)),
            Err(e) => {
                tracing::error!(error=%e, "failed to create binance agent");
                None
            }
        }
    }
}

async fn connection_task(
    mut symbols_rx: tokio::sync::watch::Receiver<Vec<String>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    tx: mpsc::Sender<String>,
    ws_url: String,
    max_reconnect_delay_secs: u64,
    intervals: Vec<String>,
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
                ACTIVE_CONNECTIONS.with_label_values(&["binance"]).inc();

                if let Err(e) = send_subscribe(&mut ws, &current_symbols, &intervals).await {
                    tracing::error!(error=%e, "failed to send subscription");
                    ACTIVE_CONNECTIONS.with_label_values(&["binance"]).dec();
                    continue;
                }

                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                tracing::info!("shutdown signal - closing connection");
                                let _ = ws.close(None).await;
                                ACTIVE_CONNECTIONS.with_label_values(&["binance"]).dec();
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
                                        let _ = send_unsubscribe(&mut ws, &to_unsub, &intervals).await;
                                    }
                                    if !to_sub.is_empty() {
                                        if let Err(e) = send_subscribe(&mut ws, &to_sub, &intervals).await {
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
                                        if let Some(event) = v.get("e").and_then(|e| e.as_str()) {
                                            match event {
                                                "trade" => {
                                                    let trade_id = v
                                                        .get("t")
                                                        .and_then(|t| t.as_i64())
                                                        .filter(|id| *id > 0);
                                                    let px = v
                                                        .get("p")
                                                        .and_then(|p| p.as_str())
                                                        .and_then(parse_decimal_str)
                                                        .unwrap_or_else(|| "?".to_string());
                                                    let qty = v
                                                        .get("q")
                                                        .and_then(|q| q.as_str())
                                                        .and_then(parse_decimal_str)
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
                                                    if tx.send(line).await.is_ok() {
                                                        MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                        LAST_TRADE_TIMESTAMP
                                                            .with_label_values(&["binance"])
                                                            .set(ts);
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                "kline" => {
                                                    if let Some(k) = v.get("k") {
                                                        if k.get("x").and_then(|b| b.as_bool()).unwrap_or(false) {
                                                            let interval = k.get("i").and_then(|i| i.as_str()).unwrap_or("");
                                                            let open = k.get("o").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                            let high = k.get("h").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                            let low = k.get("l").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                            let close = k.get("c").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                            let vol = k.get("v").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                            let ts = k.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
                                                            let line = serde_json::json!({
                                                                "agent": "binance",
                                                                "type": "candle",
                                                                "s": sym,
                                                                "i": interval,
                                                                "o": open,
                                                                "h": high,
                                                                "l": low,
                                                                "c": close,
                                                                "v": vol,
                                                                "ts": ts
                                                            }).to_string();
                                                            if tx.send(line).await.is_ok() {
                                                                MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                            } else {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                "24hrTicker" => {
                                                    let price = v
                                                        .get("c")
                                                        .and_then(|p| p.as_str())
                                                        .and_then(parse_decimal_str)
                                                        .unwrap_or_else(|| "?".to_string());
                                                    let vol = v
                                                        .get("v")
                                                        .and_then(|p| p.as_str())
                                                        .and_then(parse_decimal_str)
                                                        .unwrap_or_else(|| "?".to_string());
                                                    let ts = v.get("E").and_then(|x| x.as_i64()).unwrap_or_default();
                                                    let line = serde_json::json!({
                                                        "agent": "binance",
                                                        "type": "ticker",
                                                        "s": sym,
                                                        "p": price,
                                                        "v": vol,
                                                        "ts": ts
                                                    }).to_string();
                                                    if tx.send(line).await.is_ok() {
                                                        MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                _ => {}
                                            }
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
                ACTIVE_CONNECTIONS.with_label_values(&["binance"]).dec();
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
    intervals: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let mut params = Vec::new();
    for s in symbols {
        params.push(format!("{}@trade", s));
        params.push(format!("{}@ticker", s));
        for i in intervals {
            params.push(format!("{}@kline_{}", s, i));
        }
    }
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
    intervals: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    if symbols.is_empty() {
        return Ok(());
    }
    let mut params = Vec::new();
    for s in symbols {
        params.push(format!("{}@trade", s));
        params.push(format!("{}@ticker", s));
        for i in intervals {
            params.push(format!("{}@kline_{}", s, i));
        }
    }
    let msg = serde_json::json!({
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": 1,
    });
    ws.send(Message::Text(msg.to_string())).await
}
