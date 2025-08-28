use futures_util::{SinkExt, StreamExt};
pub mod ohlcv;
pub mod options;
pub mod funding_history;
pub mod open_interest_history;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::clock;
use crate::{
    agent::Agent,
    config::Settings,
    error::IngestorError,
    http_client,
    metrics::{
        ACTIVE_CONNECTIONS, BACKOFF_SECS, BACKPRESSURE, LAST_FUNDING_TIMESTAMP,
        LAST_LIQUIDATION_TIMESTAMP, LAST_MARK_PRICE_TIMESTAMP, LAST_OPEN_INTEREST_TIMESTAMP,
        LAST_TERM_TIMESTAMP, LAST_TRADE_TIMESTAMP, MESSAGES_INGESTED, RECONNECTS, STREAM_DROPS,
        STREAM_LATENCY_MS, STREAM_SEQ_GAPS, STREAM_THROUGHPUT, VALIDATION_ERRORS,
    },
    parse::parse_decimal_str,
};

use super::{shared_symbols, AgentFactory};
use canonicalizer::CanonicalService;

const MAX_STREAMS_PER_CONN: usize = 1024; // per Binance docs
const STREAMS_PER_SYMBOL: usize = 3; // trade, depth diff, book ticker

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
        // backfill historical funding and open interest before starting streams
        funding_history::backfill(&self.symbols, out_tx.clone()).await;
        open_interest_history::backfill(&self.symbols, out_tx.clone()).await;

        let mut handles = Vec::new();
        let mut symbol_txs = Vec::new();

        let per_conn = (MAX_STREAMS_PER_CONN / STREAMS_PER_SYMBOL).max(1);
        let chunks = self
            .symbols
            .chunks(per_conn)
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
        // additional aggregated streams not tied to symbol subsets
        let shutdown_clone = shutdown.clone();
        let tx_clone = out_tx.clone();
        handles.push(tokio::spawn(async move {
            mark_price_task(shutdown_clone, tx_clone).await;
        }));

        let shutdown_clone = shutdown.clone();
        let tx_clone = out_tx.clone();
        handles.push(tokio::spawn(async move {
            funding_rate_task(shutdown_clone, tx_clone).await;
        }));

        let shutdown_clone = shutdown.clone();
        let tx_clone = out_tx.clone();
        handles.push(tokio::spawn(async move {
            open_interest_task(shutdown_clone, tx_clone).await;
        }));

        let shutdown_clone = shutdown.clone();
        let tx_clone = out_tx.clone();
        handles.push(tokio::spawn(async move {
            liquidation_task(shutdown_clone, tx_clone).await;
        }));

        let symbols_clone = self.symbols.clone();
        let shutdown_clone = shutdown.clone();
        let tx_clone = out_tx.clone();
        handles.push(tokio::spawn(async move {
            term_structure_task(symbols_clone, shutdown_clone, tx_clone).await;
        }));
        for sym in self.symbols.clone() {
            let tx_clone = out_tx.clone();
            handles.push(tokio::spawn(async move {
                snapshot_task(sym, tx_clone).await;
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
                                if !added.is_empty() {
                                    funding_history::backfill(&added, out_tx.clone()).await;
                                    open_interest_history::backfill(&added, out_tx.clone()).await;
                                }
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
) {
    let mut attempt: u32 = 0;
    let mut last_trade_ids: HashMap<String, i64> = HashMap::new();

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

                if let Err(e) = send_subscribe(&mut ws, &current_symbols).await {
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
                                                VALIDATION_ERRORS.with_label_values(&["binance"]).inc();
                                                tracing::error!(?err, "subscription error");
                                                break;
                                            } else {
                                                tracing::info!("subscription acknowledged");
                                            }
                                            continue;
                                        }

                                        let ev = v.get("e").and_then(|e| e.as_str()).unwrap_or("");
                                        let raw = v.get("s").and_then(|s| s.as_str()).unwrap_or("?");
                                        let sym = CanonicalService::canonical_pair("binance", raw)
                                            .unwrap_or_else(|| raw.to_string());
                                        match ev {
                                            "trade" => {
                                                let trade_id = v
                                                    .get("t")
                                                    .and_then(|t| t.as_i64())
                                                    .filter(|id| *id > 0);
                                                if let Some(id) = trade_id {
                                                    if let Some(last) = last_trade_ids.get_mut(&sym) {
                                                        if id > *last + 1 {
                                                            STREAM_SEQ_GAPS
                                                                .with_label_values(&["binance", &sym])
                                                                .inc_by((id - *last - 1) as u64);
                                                        }
                                                        *last = id;
                                                    } else {
                                                        last_trade_ids.insert(sym.clone(), id);
                                                    }
                                                }
                                                let px = match v
                                                    .get("p")
                                                    .and_then(|p| p.as_str())
                                                    .and_then(parse_decimal_str)
                                                {
                                                    Some(p) => p,
                                                    None => {
                                                        VALIDATION_ERRORS
                                                            .with_label_values(&["binance"])
                                                            .inc();
                                                        "?".to_string()
                                                    }
                                                };
                                                let qty = match v
                                                    .get("q")
                                                    .and_then(|q| q.as_str())
                                                    .and_then(parse_decimal_str)
                                                {
                                                    Some(q) => q,
                                                    None => {
                                                        VALIDATION_ERRORS
                                                            .with_label_values(&["binance"])
                                                            .inc();
                                                        "?".to_string()
                                                    }
                                                };
                                                let ts = v.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
                                                let now = chrono::Utc::now().timestamp_millis();
                                                STREAM_LATENCY_MS
                                                    .with_label_values(&["binance", &sym])
                                                    .set(now - ts);
                                                let skew = clock::current_skew_ms();
                                                let line = serde_json::json!({
                                                    "agent": "binance",
                                                    "type": "trade",
                                                    "s": sym,
                                                    "t": trade_id,
                                                    "p": px,
                                                    "q": qty,
                                                    "ts": ts,
                                                    "skew": skew
                                                })
                                                .to_string();
                                                let backlog = tx.max_capacity() - tx.capacity();
                                                BACKPRESSURE
                                                    .with_label_values(&["binance", &sym])
                                                    .set(backlog as i64);
                                                match tx.send(line).await {
                                                    Ok(()) => {
                                                        MESSAGES_INGESTED
                                                            .with_label_values(&["binance"])
                                                            .inc();
                                                        STREAM_THROUGHPUT
                                                            .with_label_values(&["binance", &sym])
                                                            .inc();
                                                        LAST_TRADE_TIMESTAMP
                                                            .with_label_values(&["binance"])
                                                            .set(ts);
                                                    }
                                                    Err(_) => {
                                                        STREAM_DROPS
                                                            .with_label_values(&["binance", &sym])
                                                            .inc();
                                                        break;
                                                    }
                                                }
                                            }
                                            "depthUpdate" => {
                                                let bids = v
                                                    .get("b")
                                                    .and_then(|b| b.as_array())
                                                    .cloned()
                                                    .unwrap_or_default()
                                                    .into_iter()
                                                    .filter_map(|lvl| {
                                                        let p = lvl.get(0)?.as_str()?.to_string();
                                                        let q = lvl.get(1)?.as_str()?.to_string();
                                                        Some([p, q])
                                                    })
                                                    .collect::<Vec<[String;2]>>();
                                                let asks = v
                                                    .get("a")
                                                    .and_then(|a| a.as_array())
                                                    .cloned()
                                                    .unwrap_or_default()
                                                    .into_iter()
                                                    .filter_map(|lvl| {
                                                        let p = lvl.get(0)?.as_str()?.to_string();
                                                        let q = lvl.get(1)?.as_str()?.to_string();
                                                        Some([p, q])
                                                    })
                                                    .collect::<Vec<[String;2]>>();
                                                let ts = v.get("E").and_then(|x| x.as_i64()).unwrap_or_default();
                                                let line = serde_json::json!({
                                                    "agent": "binance",
                                                    "type": "l2_diff",
                                                    "s": sym,
                                                    "bids": bids,
                                                    "asks": asks,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                } else { break; }
                                            }
                                            "bookTicker" => {
                                                let bid_px = v
                                                    .get("b")
                                                    .and_then(|p| p.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let bid_qty = v
                                                    .get("B")
                                                    .and_then(|q| q.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let ask_px = v
                                                    .get("a")
                                                    .and_then(|p| p.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let ask_qty = v
                                                    .get("A")
                                                    .and_then(|q| q.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let ts = v.get("E").and_then(|x| x.as_i64()).unwrap_or_default();
                                                let line = serde_json::json!({
                                                    "agent": "binance",
                                                    "type": "book_ticker",
                                                    "s": sym,
                                                    "bp": bid_px,
                                                    "bq": bid_qty,
                                                    "ap": ask_px,
                                                    "aq": ask_qty,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                } else { break; }
                                            }
                                            _ => {}
                                        }
                                    } else {
                                        VALIDATION_ERRORS.with_label_values(&["binance"]).inc();
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
        RECONNECTS.with_label_values(&["binance"]).inc();
        BACKOFF_SECS.with_label_values(&["binance"]).inc_by(delay);
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

async fn snapshot_task(symbol: String, tx: mpsc::Sender<String>) {
    let client = match http_client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error=%e, "binance snapshot http client");
            return;
        }
    };
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        let url = format!(
            "https://api.binance.us/api/v3/depth?symbol={}&limit=1000",
            symbol.to_uppercase()
        );
        match client.get(&url).send().await {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(v) => {
                    let bids = v
                        .get("bids")
                        .and_then(|b| b.as_array())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|lvl| {
                            let p = lvl.get(0)?.as_str()?.to_string();
                            let q = lvl.get(1)?.as_str()?.to_string();
                            Some([p, q])
                        })
                        .collect::<Vec<[String; 2]>>();
                    let asks = v
                        .get("asks")
                        .and_then(|b| b.as_array())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|lvl| {
                            let p = lvl.get(0)?.as_str()?.to_string();
                            let q = lvl.get(1)?.as_str()?.to_string();
                            Some([p, q])
                        })
                        .collect::<Vec<[String; 2]>>();
                    let sym = CanonicalService::canonical_pair("binance", &symbol)
                        .unwrap_or_else(|| symbol.clone());
                    let ts = chrono::Utc::now().timestamp_millis();
                    let line = serde_json::json!({
                        "agent": "binance",
                        "type": "snapshot",
                        "s": sym,
                        "bids": bids,
                        "asks": asks,
                        "ts": ts
                    })
                    .to_string();
                    let _ = tx.send(line).await;
                }
                Err(e) => {
                    tracing::error!(error=%e, symbol=%symbol, "snapshot parse failed");
                }
            },
            Err(e) => {
                tracing::error!(error=%e, symbol=%symbol, "snapshot failed");
            }
        }
        interval.tick().await;
    }
}

async fn send_subscribe(
    ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    symbols: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let params = symbols
        .iter()
        .flat_map(|s| {
            [
                format!("{}@trade", s),
                format!("{}@depth@100ms", s),
                format!("{}@bookTicker", s),
            ]
        })
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
        .flat_map(|s| {
            [
                format!("{}@trade", s),
                format!("{}@depth@100ms", s),
                format!("{}@bookTicker", s),
            ]
        })
        .collect::<Vec<_>>();
    let msg = serde_json::json!({
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": 1,
    });
    ws.send(Message::Text(msg.to_string())).await
}

async fn mark_price_task(shutdown: tokio::sync::watch::Receiver<bool>, tx: mpsc::Sender<String>) {
    let url = "wss://fstream.binance.com/stream?streams=!markPrice@arr";
    aggregated_ws_loop(url, "mark_price", shutdown, tx, |item| {
        let raw = item.get("s").and_then(|s| s.as_str()).unwrap_or("?");
        let sym =
            CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
        let price = item
            .get("p")
            .and_then(|p| p.as_str())
            .and_then(parse_decimal_str)
            .unwrap_or_else(|| "?".to_string());
        let ts = item.get("E").and_then(|x| x.as_i64()).unwrap_or_default();
        let line = serde_json::json!({
            "agent": "binance",
            "type": "mark_price",
            "s": sym,
            "p": price,
            "ts": ts
        })
        .to_string();
        (line, ts)
    })
    .await;
}

async fn funding_rate_task(shutdown: tokio::sync::watch::Receiver<bool>, tx: mpsc::Sender<String>) {
    let url = "wss://fstream.binance.com/stream?streams=!fundingRate@arr";
    aggregated_ws_loop(url, "funding", shutdown, tx, |item| {
        let raw = item.get("s").and_then(|s| s.as_str()).unwrap_or("?");
        let sym =
            CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
        let rate = item
            .get("r")
            .and_then(|p| p.as_str())
            .and_then(parse_decimal_str)
            .unwrap_or_else(|| "?".to_string());
        let ts = item.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
        let line = serde_json::json!({
            "agent": "binance",
            "type": "funding",
            "s": sym,
            "r": rate,
            "ts": ts
        })
        .to_string();
        (line, ts)
    })
    .await;
}

async fn open_interest_task(
    shutdown: tokio::sync::watch::Receiver<bool>,
    tx: mpsc::Sender<String>,
) {
    let url = "wss://fstream.binance.com/stream?streams=!openInterest@arr";
    aggregated_ws_loop(url, "open_interest", shutdown, tx, |item| {
        let raw = item.get("s").and_then(|s| s.as_str()).unwrap_or("?");
        let sym =
            CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
        let oi = item
            .get("oi")
            .and_then(|p| p.as_str())
            .and_then(parse_decimal_str)
            .unwrap_or_else(|| "?".to_string());
        let ts = item.get("T").and_then(|x| x.as_i64()).unwrap_or_default();
        let line = serde_json::json!({
            "agent": "binance",
            "type": "open_interest",
            "s": sym,
            "oi": oi,
            "ts": ts
        })
        .to_string();
        (line, ts)
    })
    .await;
}

async fn liquidation_task(shutdown: tokio::sync::watch::Receiver<bool>, tx: mpsc::Sender<String>) {
    let url = "wss://fstream.binance.com/stream?streams=!forceOrder@arr";
    aggregated_ws_loop(url, "liquidation", shutdown, tx, |item| {
        let raw = item.get("s").and_then(|s| s.as_str()).unwrap_or("?");
        let sym =
            CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
        let o = item.get("o").and_then(|o| o.as_object());
        let price = o
            .and_then(|m| m.get("p"))
            .and_then(|p| p.as_str())
            .and_then(parse_decimal_str)
            .unwrap_or_else(|| "?".to_string());
        let qty = o
            .and_then(|m| m.get("q"))
            .and_then(|p| p.as_str())
            .and_then(parse_decimal_str)
            .unwrap_or_else(|| "?".to_string());
        let side = o
            .and_then(|m| m.get("S"))
            .and_then(|s| s.as_str())
            .unwrap_or("?")
            .to_string();
        let ts = item.get("E").and_then(|x| x.as_i64()).unwrap_or_default();
        let line = serde_json::json!({
            "agent": "binance",
            "type": "liquidation",
            "s": sym,
            "p": price,
            "q": qty,
            "side": side,
            "ts": ts
        })
        .to_string();
        (line, ts)
    })
    .await;
}

async fn term_structure_task(
    symbols: Vec<String>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    tx: mpsc::Sender<String>,
) {
    let client = match http_client::builder().build() {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = interval.tick() => {
                for sym in &symbols {
                    let url = format!("https://fapi.binance.com/futures/data/basis?symbol={}&period=5m&limit=1", sym.to_uppercase());
                    if let Ok(resp) = client.get(&url).send().await {
                        if let Ok(resp) = resp.json::<serde_json::Value>().await {
                            if let Some(arr) = resp.as_array().and_then(|a| a.first()) {
                                let raw = arr.get("symbol").and_then(|s| s.as_str()).unwrap_or("?");
                                let canon = CanonicalService::canonical_pair("binance", raw).unwrap_or_else(|| raw.to_string());
                                let basis = arr
                                    .get("basis")
                                    .and_then(|b| b.as_str())
                                    .and_then(parse_decimal_str)
                                    .unwrap_or_else(|| "?".to_string());
                                let ts = arr.get("timestamp").and_then(|t| t.as_i64()).unwrap_or_default();
                                let line = serde_json::json!({
                                    "agent": "binance",
                                    "type": "term",
                                    "s": canon,
                                    "b": basis,
                                    "ts": ts
                                }).to_string();
                                let _ = tx.send(line).await;
                                MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                LAST_TERM_TIMESTAMP.with_label_values(&["binance"]).set(ts);
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn aggregated_ws_loop<F>(
    url: &str,
    metric: &str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    tx: mpsc::Sender<String>,
    mut build: F,
) where
    F: FnMut(&serde_json::Value) -> (String, i64),
{
    let mut attempt: u32 = 0;
    loop {
        if *shutdown.borrow() {
            break;
        }
        tracing::info!(%url, "connecting");
        match connect_async(url).await {
            Ok((mut ws, _)) => {
                ACTIVE_CONNECTIONS.with_label_values(&["binance"]).inc();
                attempt = 0;
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                let _ = ws.close(None).await;
                                ACTIVE_CONNECTIONS.with_label_values(&["binance"]).dec();
                                return;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                        if let Some(arr) = v.get("data").and_then(|d| d.as_array()) {
                                            for item in arr {
                                                let (line, ts) = build(item);
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["binance"]).inc();
                                                    match metric {
                                                        "mark_price" => LAST_MARK_PRICE_TIMESTAMP.with_label_values(&["binance"]).set(ts),
                                                        "funding" => LAST_FUNDING_TIMESTAMP.with_label_values(&["binance"]).set(ts),
                                                        "open_interest" => LAST_OPEN_INTEREST_TIMESTAMP.with_label_values(&["binance"]).set(ts),
                                                        "liquidation" => LAST_LIQUIDATION_TIMESTAMP.with_label_values(&["binance"]).set(ts),
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Close(_))) => { break; }
                                Some(Ok(_)) => {}
                                Some(Err(e)) => { tracing::error!(error=%e, "ws error"); break; }
                                None => { break; }
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
        let delay = (1u64 << exp).min(16);
        let sleep = std::time::Duration::from_secs(delay);
        tracing::info!(?sleep, "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(sleep) => {},
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}
