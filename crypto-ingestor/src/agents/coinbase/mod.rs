use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use super::{shared_symbols, AgentFactory};
use crate::clock;
use crate::{
    agent::Agent,
    config::Settings,
    error::IngestorError,
    http_client,
    metrics::{
        ACTIVE_CONNECTIONS, BACKOFF_SECS, BACKPRESSURE, LAST_TRADE_TIMESTAMP, MESSAGES_INGESTED,
        RECONNECTS, STREAM_DROPS, STREAM_LATENCY_MS, STREAM_SEQ_GAPS, STREAM_THROUGHPUT,
        VALIDATION_ERRORS,
    },
    parse::parse_decimal_str,
};
use canonicalizer::CanonicalService;

/// Fetch all tradable USD product IDs from Coinbase.
pub async fn fetch_all_symbols() -> Result<Vec<String>, IngestorError> {
    let client = http_client::builder()
        .build()
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?;
    let products: serde_json::Value = client
        .get("https://api.exchange.coinbase.com/products")
        .send()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?
        .json()
        .await
        .map_err(|e| IngestorError::Http {
            source: e,
            exchange: "coinbase",
            symbol: None,
        })?;

    let arr = products
        .as_array()
        .ok_or_else(|| IngestorError::Other("coinbase unexpected response".into()))?;
    let mut symbols = Vec::new();
    for prod in arr {
        if prod.get("quote_currency").and_then(|q| q.as_str()) == Some("USD") {
            if let Some(id) = prod.get("id").and_then(|i| i.as_str()) {
                symbols.push(id.to_string());
            }
        }
    }
    Ok(symbols)
}

pub struct CoinbaseAgent {
    symbols: Vec<String>,
    ws_url: String,
    max_reconnect_delay_secs: u64,
    refresh_interval_mins: u64,
}

impl CoinbaseAgent {
    pub fn new(symbols: Vec<String>, cfg: &Settings) -> Self {
        Self {
            symbols,
            ws_url: cfg.coinbase_ws_url.clone(),
            max_reconnect_delay_secs: cfg.coinbase_max_reconnect_delay_secs,
            refresh_interval_mins: cfg.coinbase_refresh_interval_mins,
        }
    }
}

#[async_trait::async_trait]
impl Agent for CoinbaseAgent {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    fn event_types(&self) -> Vec<crate::agent::EventType> {
        use crate::agent::EventType::*;
        vec![Trade, L2Diff, Snapshot, BookTicker]
    }

    async fn run(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        tx: mpsc::Sender<String>,
    ) -> Result<(), IngestorError> {
        let mut handle = None;
        let mut sym_tx = None;
        let mut snap_handles = Vec::new();

        if !self.symbols.is_empty() {
            let (s_tx, rx) = tokio::sync::watch::channel(self.symbols.clone());
            sym_tx = Some(s_tx);
            let shutdown_rx = shutdown.clone();
            let tx_clone = tx.clone();
            let ws_url = self.ws_url.clone();
            let max_delay = self.max_reconnect_delay_secs;
            handle = Some(tokio::spawn(async move {
                connection_task(rx, shutdown_rx, tx_clone, ws_url, max_delay).await;
            }));
            for sym in self.symbols.clone() {
                let tx_snap = tx.clone();
                snap_handles.push(tokio::spawn(async move {
                    snapshot_task(sym, tx_snap).await;
                }));
            }
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

                                if self.symbols.is_empty() {
                                    if let Some(tx_sym) = sym_tx.take() {
                                        let _ = tx_sym.send(Vec::new());
                                    }
                                    if let Some(h) = handle.take() {
                                        let _ = h.await;
                                    }
                                } else if let Some(tx_sym) = &sym_tx {
                                    let _ = tx_sym.send(self.symbols.clone());
                                } else {
                                    let (s_tx, rx) = tokio::sync::watch::channel(self.symbols.clone());
                                    sym_tx = Some(s_tx);
                                    let shutdown_rx = shutdown.clone();
                                    let tx_clone = tx.clone();
                                    let ws_url = self.ws_url.clone();
                                    let max_delay = self.max_reconnect_delay_secs;
                                    handle = Some(tokio::spawn(async move {
                                        connection_task(rx, shutdown_rx, tx_clone, ws_url, max_delay).await;
                                    }));
                                }
                            }
                        }
                        Err(e) => tracing::error!(error=%e, "failed to refresh symbols"),
                    }
                }
            }
        }

        if let Some(tx_sym) = sym_tx {
            drop(tx_sym);
        }
        if let Some(h) = handle {
            let _ = h.await;
        }
        for h in snap_handles {
            let _ = h.await;
        }

        Ok(())
    }
}

pub struct CoinbaseFactory;

#[async_trait::async_trait]
impl AgentFactory for CoinbaseFactory {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
        let symbols = if spec.is_empty() {
            vec!["BTC-USD".to_string()]
        } else if spec.eq_ignore_ascii_case("all") {
            match shared_symbols().await {
                Ok((_, c)) => c,
                Err(e) => {
                    tracing::error!(error=%e, "failed to fetch shared symbols");
                    return None;
                }
            }
        } else {
            spec.split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        };
        Some(Box::new(CoinbaseAgent::new(symbols, cfg)))
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
                ACTIVE_CONNECTIONS.with_label_values(&["coinbase"]).inc();

                if let Err(e) = send_subscribe(&mut ws, &current_symbols).await {
                    tracing::error!(error=%e, "failed to send subscription");
                    ACTIVE_CONNECTIONS.with_label_values(&["coinbase"]).dec();
                    continue;
                }

                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                tracing::info!("shutdown signal - closing connection");
                                let _ = ws.close(None).await;
                                ACTIVE_CONNECTIONS.with_label_values(&["coinbase"]).dec();
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
                                    if current_symbols.is_empty() {
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(txt))) => {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                        let typ = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                                        match typ {
                                            "match" => {
                                                let raw = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                                let sym = CanonicalService::canonical_pair("coinbase", raw).unwrap_or_else(|| raw.to_string());
                                                // Missing or non-positive trade IDs are represented as JSON null.
                                                let trade_id = v
                                                    .get("trade_id")
                                                    .and_then(|id| id.as_i64())
                                                    .filter(|id| *id > 0);
                                                let price = v
                                                    .get("price")
                                                    .and_then(|p| p.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let size = v
                                                    .get("size")
                                                    .and_then(|q| q.as_str())
                                                    .and_then(parse_decimal_str)
                                                    .unwrap_or_else(|| "?".to_string());
                                                let ts = v
                                                    .get("time")
                                                    .and_then(|t| t.as_str())
                                                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                                                    .map(|dt| dt.timestamp_millis())
                                                    .unwrap_or_default();
                                                let line = serde_json::json!({
                                                    "agent": "coinbase",
                                                    "type": "trade",
                                                    "s": sym,
                                                    "t": trade_id,
                                                    "p": price,
                                                    "q": size,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["coinbase"]).inc();
                                                    LAST_TRADE_TIMESTAMP
                                                        .with_label_values(&["coinbase"])
                                                        .set(ts);
                                                } else {
                                                    let raw = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                                    let sym = CanonicalService::canonical_pair("coinbase", raw)
                                                        .unwrap_or_else(|| raw.to_string());
                                                    // Missing or non-positive trade IDs are represented as JSON null.
                                                    let trade_id = v
                                                        .get("trade_id")
                                                        .and_then(|id| id.as_i64())
                                                        .filter(|id| *id > 0);
                                                    if let Some(id) = trade_id {
                                                        if let Some(last) = last_trade_ids.get_mut(&sym) {
                                                            if id > *last + 1 {
                                                                STREAM_SEQ_GAPS
                                                                    .with_label_values(&["coinbase", &sym])
                                                                    .inc_by((id - *last - 1) as u64);
                                                            }
                                                            *last = id;
                                                        } else {
                                                            last_trade_ids.insert(sym.clone(), id);
                                                        }
                                                    }
                                                    let price = match v
                                                        .get("price")
                                                        .and_then(|p| p.as_str())
                                                        .and_then(parse_decimal_str)
                                                    {
                                                        Some(p) => p,
                                                        None => {
                                                            VALIDATION_ERRORS
                                                                .with_label_values(&["coinbase"])
                                                                .inc();
                                                            "?".to_string()
                                                        }
                                                    };
                                                    let size = match v
                                                        .get("size")
                                                        .and_then(|q| q.as_str())
                                                        .and_then(parse_decimal_str)
                                                    {
                                                        Some(q) => q,
                                                        None => {
                                                            VALIDATION_ERRORS
                                                                .with_label_values(&["coinbase"])
                                                                .inc();
                                                            "?".to_string()
                                                        }
                                                    };
                                                    let ts = v
                                                        .get("time")
                                                        .and_then(|t| t.as_str())
                                                        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                                                        .map(|dt| dt.timestamp_millis())
                                                        .unwrap_or_default();
                                                    let now = chrono::Utc::now().timestamp_millis();
                                                    STREAM_LATENCY_MS
                                                        .with_label_values(&["coinbase", &sym])
                                                        .set(now - ts);
                                                    let skew = clock::current_skew_ms();
                                                    let line = serde_json::json!({
                                                        "agent": "coinbase",
                                                        "type": "trade",
                                                        "s": sym,
                                                        "t": trade_id,
                                                        "p": price,
                                                        "q": size,
                                                        "ts": ts,
                                                        "skew": skew
                                                    })
                                                    .to_string();
                                                    let backlog = tx.max_capacity() - tx.capacity();
                                                    BACKPRESSURE
                                                        .with_label_values(&["coinbase", &sym])
                                                        .set(backlog as i64);
                                                    match tx.send(line).await {
                                                        Ok(()) => {
                                                            MESSAGES_INGESTED
                                                                .with_label_values(&["coinbase"])
                                                                .inc();
                                                            STREAM_THROUGHPUT
                                                                .with_label_values(&["coinbase", &sym])
                                                                .inc();
                                                            LAST_TRADE_TIMESTAMP
                                                                .with_label_values(&["coinbase"])
                                                                .set(ts);
                                                        }
                                                        Err(_) => {
                                                            STREAM_DROPS
                                                                .with_label_values(&["coinbase", &sym])
                                                                .inc();
                                                            break;
                                                        }
                                                    }
                                                }
                                            },
                                            "l2update" => {
                                                let raw = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                                let sym = CanonicalService::canonical_pair("coinbase", raw).unwrap_or_else(|| raw.to_string());
                                                let mut bids = Vec::new();
                                                let mut asks = Vec::new();
                                                if let Some(changes) = v.get("changes").and_then(|c| c.as_array()) {
                                                    for c in changes {
                                                        if let (Some(side), Some(p), Some(sz)) = (
                                                            c.get(0).and_then(|s| s.as_str()),
                                                            c.get(1).and_then(|p| p.as_str()),
                                                            c.get(2).and_then(|q| q.as_str()),
                                                        ) {
                                                            let price = parse_decimal_str(p);
                                                            let qty = parse_decimal_str(sz);
                                                            if let (Some(price), Some(qty)) = (price, qty) {
                                                                if side == "buy" {
                                                                    bids.push([price, qty]);
                                                                } else {
                                                                    asks.push([price, qty]);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                let ts = v
                                                    .get("time")
                                                    .and_then(|t| t.as_str())
                                                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                                                    .map(|dt| dt.timestamp_millis())
                                                    .unwrap_or_default();
                                                let line = serde_json::json!({
                                                    "agent": "coinbase",
                                                    "type": "l2_diff",
                                                    "s": sym,
                                                    "bids": bids,
                                                    "asks": asks,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["coinbase"]).inc();
                                                } else { break; }
                                            }
                                            "snapshot" => {
                                                let raw = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                                let sym = CanonicalService::canonical_pair("coinbase", raw).unwrap_or_else(|| raw.to_string());
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
                                                    .collect::<Vec<[String;2]>>();
                                                let asks = v
                                                    .get("asks")
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
                                                let ts = chrono::Utc::now().timestamp_millis();
                                                let line = serde_json::json!({
                                                    "agent": "coinbase",
                                                    "type": "snapshot",
                                                    "s": sym,
                                                    "bids": bids,
                                                    "asks": asks,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["coinbase"]).inc();
                                                } else { break; }
                                            }
                                            "ticker" => {
                                                let raw = v.get("product_id").and_then(|s| s.as_str()).unwrap_or("?");
                                                let sym = CanonicalService::canonical_pair("coinbase", raw).unwrap_or_else(|| raw.to_string());
                                                let bid_px = v.get("best_bid").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                let bid_qty = v.get("best_bid_size").and_then(|q| q.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                let ask_px = v.get("best_ask").and_then(|p| p.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                let ask_qty = v.get("best_ask_size").and_then(|q| q.as_str()).and_then(parse_decimal_str).unwrap_or_else(|| "?".to_string());
                                                let ts = v
                                                    .get("time")
                                                    .and_then(|t| t.as_str())
                                                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                                                    .map(|dt| dt.timestamp_millis())
                                                    .unwrap_or_default();
                                                let line = serde_json::json!({
                                                    "agent": "coinbase",
                                                    "type": "book_ticker",
                                                    "s": sym,
                                                    "bp": bid_px,
                                                    "bq": bid_qty,
                                                    "ap": ask_px,
                                                    "aq": ask_qty,
                                                    "ts": ts
                                                }).to_string();
                                                if tx.send(line).await.is_ok() {
                                                    MESSAGES_INGESTED.with_label_values(&["coinbase"]).inc();
                                                } else { break; }
                                            }
                                            _ => {}
                                        }
                                    } else {
                                        VALIDATION_ERRORS.with_label_values(&["coinbase"]).inc();
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
                ACTIVE_CONNECTIONS.with_label_values(&["coinbase"]).dec();
            }
            Err(e) => {
                tracing::error!(error=%e, "connect failed");
            }
        }

        if symbols_rx.borrow().is_empty() {
            break;
        }

        attempt = attempt.saturating_add(1);
        let exp: u32 = attempt.saturating_sub(1).min(4);
        let delay = (1u64 << exp).min(max_reconnect_delay_secs);
        let sleep = std::time::Duration::from_secs(delay);

        tracing::info!(?sleep, "reconnecting");
        RECONNECTS.with_label_values(&["coinbase"]).inc();
        BACKOFF_SECS.with_label_values(&["coinbase"]).inc_by(delay);
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
        "channels": ["matches", "level2", "ticker"],
    });
    ws.send(Message::Text(msg.to_string())).await
}

async fn send_unsubscribe(
    ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    symbols: &[String],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    if symbols.is_empty() {
        return Ok(());
    }
    let msg = serde_json::json!({
        "type": "unsubscribe",
        "product_ids": symbols,
        "channels": ["matches", "level2", "ticker"],
    });
    ws.send(Message::Text(msg.to_string())).await
}

async fn snapshot_task(symbol: String, tx: mpsc::Sender<String>) {
    let client = match http_client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error=%e, "coinbase snapshot http client");
            return;
        }
    };
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        let url = format!(
            "https://api.exchange.coinbase.com/products/{}/book?level=2",
            symbol
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
                        .and_then(|a| a.as_array())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|lvl| {
                            let p = lvl.get(0)?.as_str()?.to_string();
                            let q = lvl.get(1)?.as_str()?.to_string();
                            Some([p, q])
                        })
                        .collect::<Vec<[String; 2]>>();
                    let sym = CanonicalService::canonical_pair("coinbase", &symbol)
                        .unwrap_or_else(|| symbol.clone());
                    let ts = chrono::Utc::now().timestamp_millis();
                    let line = serde_json::json!({
                        "agent": "coinbase",
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
