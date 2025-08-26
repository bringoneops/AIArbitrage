use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use super::{shared_symbols, AgentFactory};
use crate::{
    agent::Agent,
    config::Settings,
    error::IngestorError,
    http_client,
    telemetry::{ERRORS, RECONNECTS, TRADES_RECEIVED},
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

fn parse_decimal_str(s: &str) -> Option<String> {
    s.parse::<Decimal>()
        .ok()
        .map(|d| d.round_dp(28).normalize().to_string())
}

pub struct CoinbaseAgent {
    symbols: Vec<String>,
    ws_url: String,
    max_reconnect_delay_secs: u64,
}

impl CoinbaseAgent {
    pub fn new(symbols: Vec<String>, cfg: &Settings) -> Self {
        Self {
            symbols,
            ws_url: cfg.coinbase_ws_url.clone(),
            max_reconnect_delay_secs: cfg.coinbase_max_reconnect_delay_secs,
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
        tx: mpsc::Sender<String>,
    ) -> Result<(), IngestorError> {
        connection_task(
            self.symbols.clone(),
            shutdown,
            tx,
            self.ws_url.clone(),
            self.max_reconnect_delay_secs,
        )
        .await;
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
    symbols: Vec<String>,
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
        match connect_async(&ws_url).await {
            Ok((mut ws, _)) => {
                tracing::info!("connected");
                attempt = 0;

                if let Err(e) = send_subscribe(&mut ws, &symbols).await {
                    tracing::error!(error=%e, "failed to send subscription");
                    ERRORS.with_label_values(&["coinbase"]).inc();
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
                                        if typ == "match" {
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
                                                TRADES_RECEIVED.with_label_values(&["coinbase"]).inc();
                                            } else {
                                                break;
                                            }
                                        }
                                    } else {
                                        tracing::warn!("non-json text msg");
                                    }
                                }
                                Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                                Some(Ok(Message::Close(frame))) => { tracing::warn!(?frame, "server closed connection"); break; }
                                Some(Ok(_)) => { }
                                Some(Err(e)) => { tracing::error!(error=%e, "ws error"); ERRORS.with_label_values(&["coinbase"]).inc(); break; }
                                None => { tracing::warn!("stream ended"); break; }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(error=%e, "connect failed");
                ERRORS.with_label_values(&["coinbase"]).inc();
            }
        }

        attempt = attempt.saturating_add(1);
        let exp: u32 = attempt.saturating_sub(1).min(4);
        let delay = (1u64 << exp).min(max_reconnect_delay_secs);
        let sleep = std::time::Duration::from_secs(delay);

        RECONNECTS.with_label_values(&["coinbase"]).inc();
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
