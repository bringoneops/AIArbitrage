use std::path::PathBuf;

use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::connect_async;

use crate::{
    agent::Agent,
    config::Settings,
    error::IngestorError,
    http_client,
};

use canonicalizer::{CanonicalService, Fill, Order, Position};

/// Binance account stream handler.
pub struct BinanceAccount {
    api_key: String,
    ws_url: String,
    offset_file: PathBuf,
}

impl BinanceAccount {
    /// Create a new account agent if API credentials are configured.
    pub fn new(cfg: &Settings) -> Option<Self> {
        let api_key = cfg.binance_api_key.clone()?;
        let _api_secret = cfg.binance_api_secret.clone()?;
        let ws_url = cfg.binance_ws_url.clone();
        Some(Self {
            api_key,
            ws_url,
            offset_file: PathBuf::from("binance_account.offset"),
        })
    }

    async fn read_offset(&self) -> i64 {
        match tokio::fs::read_to_string(&self.offset_file).await {
            Ok(s) => s.trim().parse().unwrap_or(0),
            Err(_) => 0,
        }
    }

    async fn write_offset(&self, v: i64) {
        if let Ok(s) = serde_json::to_string(&v) {
            let _ = tokio::fs::write(&self.offset_file, s).await;
        }
    }

    async fn listen_key(&self) -> Result<String, IngestorError> {
        let client = http_client::builder().build().map_err(|e| IngestorError::Http {
            source: e,
            exchange: "binance",
            symbol: None,
        })?;
        let v: serde_json::Value = client
            .post("https://api.binance.us/api/v3/userDataStream")
            .header("X-MBX-APIKEY", &self.api_key)
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
        v.get("listenKey")
            .and_then(|k| k.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| IngestorError::Other("missing listenKey".into()))
    }
}

#[async_trait::async_trait]
impl Agent for BinanceAccount {
    fn name(&self) -> &'static str {
        "binance_account"
    }

    async fn run(
        &mut self,
        shutdown: watch::Receiver<bool>,
        out_tx: mpsc::Sender<String>,
    ) -> Result<(), IngestorError> {
        let mut backoff = 1u64;
        let mut last = self.read_offset().await;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let listen_key = match self.listen_key().await {
                Ok(k) => k,
                Err(e) => {
                    tracing::error!(error=?e, "listen key error");
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                    continue;
                }
            };
            backoff = 1;
            let url = format!("{}/{}", self.ws_url.trim_end_matches('/'), listen_key);
            let (ws, _) = match connect_async(&url).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error=?e, "ws connect error");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };
            let (_, mut read) = ws.split();
            while let Some(msg) = read.next().await {
                if *shutdown.borrow() {
                    break;
                }
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error=?e, "ws read error");
                        break;
                    }
                };
                if !msg.is_text() { continue; }
                let data = msg.into_text().unwrap();
                let v: serde_json::Value = match serde_json::from_str(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error=?e, "json parse");
                        continue;
                    }
                };
                let ts = v.get("E").and_then(|e| e.as_i64()).unwrap_or(0);
                if ts <= last { continue; }
                match v.get("e").and_then(|e| e.as_str()) {
                    Some("executionReport") => {
                        if let (Some(sym), Some(id)) = (v.get("s"), v.get("i")) {
                            let symbol = sym.as_str().unwrap_or("");
                            let canon = CanonicalService::canonical_pair("binance", symbol)
                                .unwrap_or_else(|| symbol.to_string());
                            let order = Order {
                                agent: "binance".into(),
                                symbol: canon.clone(),
                                order_id: id.to_string(),
                                side: v.get("S").and_then(|x| x.as_str()).unwrap_or("").into(),
                                status: v.get("X").and_then(|x| x.as_str()).unwrap_or("").into(),
                                price: v.get("p").and_then(|x| x.as_str()).unwrap_or("").into(),
                                quantity: v.get("q").and_then(|x| x.as_str()).unwrap_or("").into(),
                                timestamp: ts,
                            };
                            if let Ok(js) = serde_json::to_string(&order) {
                                let _ = out_tx.send(js).await;
                            }
                            if let Some(fill_qty) = v.get("l").and_then(|x| x.as_str()) {
                                if fill_qty != "0" {
                                    let fill = Fill {
                                        agent: "binance".into(),
                                        symbol: canon,
                                        order_id: id.to_string(),
                                        trade_id: v.get("t").map(|x| x.to_string()).unwrap_or_default(),
                                        price: v.get("L").and_then(|x| x.as_str()).unwrap_or("").into(),
                                        quantity: fill_qty.into(),
                                        timestamp: ts,
                                    };
                                    if let Ok(js) = serde_json::to_string(&fill) {
                                        let _ = out_tx.send(js).await;
                                    }
                                }
                            }
                        }
                    }
                    Some("outboundAccountPosition") => {
                        if let Some(arr) = v.get("B").and_then(|b| b.as_array()) {
                            for bal in arr {
                                if let (Some(a), Some(f), Some(l)) = (
                                    bal.get("a"),
                                    bal.get("f"),
                                    bal.get("l"),
                                ) {
                                    let pos = Position {
                                        agent: "binance".into(),
                                    symbol: a.as_str().unwrap_or("").to_uppercase(),
                                    free: f.as_str().unwrap_or("").into(),
                                    locked: l.as_str().unwrap_or("").into(),
                                    timestamp: ts,
                                    };
                                    if let Ok(js) = serde_json::to_string(&pos) {
                                        let _ = out_tx.send(js).await;
                                    }
                                }
                            }
                        }
                    }
                    Some("balanceUpdate") => {
                        if let (Some(a), Some(d)) = (v.get("a"), v.get("d")) {
                            let pos = Position {
                                agent: "binance".into(),
                                symbol: a.as_str().unwrap_or("").to_uppercase(),
                                free: d.as_str().unwrap_or("").into(),
                                locked: "0".into(),
                                timestamp: ts,
                            };
                            if let Ok(js) = serde_json::to_string(&pos) {
                                let _ = out_tx.send(js).await;
                            }
                        }
                    }
                    _ => {}
                }
                last = ts;
                self.write_offset(last).await;
            }
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(60);
        }
        Ok(())
    }
}
