use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::connect_async;

use crate::{agent::Agent, config::Settings, error::IngestorError, http_client};

/// Coinbase account stream handler. This is a minimal implementation that
/// authenticates and listens for user-channel messages.
pub struct CoinbaseAccount {
    api_key: String,
    api_secret: String,
    ws_url: String,
}

impl CoinbaseAccount {
    pub fn new(cfg: &Settings) -> Option<Self> {
        let api_key = cfg.coinbase_api_key.clone()?;
        let api_secret = cfg.coinbase_api_secret.clone()?;
        let ws_url = cfg.coinbase_ws_url.clone();
        Some(Self {
            api_key,
            api_secret,
            ws_url,
        })
    }

    fn sign(&self, ts: &str) -> String {
        http_client::hmac_sha256(&self.api_secret, ts)
    }
}

#[async_trait::async_trait]
impl Agent for CoinbaseAccount {
    fn name(&self) -> &'static str {
        "coinbase_account"
    }

    async fn run(
        &mut self,
        shutdown: watch::Receiver<bool>,
        _out_tx: mpsc::Sender<String>,
    ) -> Result<(), IngestorError> {
        let (ws, _) = connect_async(&self.ws_url)
            .await
            .map_err(|e| IngestorError::Other(format!("ws connect: {}", e)))?;
        let (mut write, mut read) = ws.split();
        let ts = chrono::Utc::now().timestamp().to_string();
        let sig = self.sign(&ts);
        let msg = serde_json::json!({
            "type": "subscribe",
            "channels": ["user"],
            "api_key": self.api_key,
            "timestamp": ts,
            "signature": sig,
        });
        let _ = write
            .send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string()))
            .await;
        while let Some(_m) = read.next().await {
            if *shutdown.borrow() {
                break;
            }
        }
        Ok(())
    }
}
