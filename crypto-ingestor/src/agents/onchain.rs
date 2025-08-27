use std::collections::HashMap;

use ethers::prelude::*;
use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::{mpsc::Sender, watch};

use std::sync::Arc;

use crate::{agent::Agent, config::Settings, error::IngestorError, labels::load_labels, token_state::TokenState};

pub struct OnchainAgent {
    provider: Arc<Provider<Ws>>,
    pending: HashMap<H256, Transaction>,
    labels: HashMap<Address, String>,
    token_state: TokenState,
}

impl OnchainAgent {
    pub async fn new(ws_url: &str, label_file: Option<&str>) -> Result<Self, IngestorError> {
        let provider = Provider::<Ws>::connect(ws_url)
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;
        let provider = Arc::new(provider);
        let labels = match label_file {
            Some(path) => load_labels(path)?,
            None => HashMap::new(),
        };
        Ok(Self {
            provider,
            pending: HashMap::new(),
            labels,
            token_state: TokenState::new(),
        })
    }
}

#[async_trait::async_trait]
impl Agent for OnchainAgent {
    fn name(&self) -> &'static str {
        "onchain"
    }

    fn event_types(&self) -> Vec<crate::agent::EventType> {
        Vec::new()
    }

    async fn run(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
        tx: Sender<String>,
    ) -> Result<(), IngestorError> {
        let mut stream = self
            .provider
            .subscribe_pending_txs()
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;

        loop {
            tokio::select! {
                _ = shutdown.changed() => { break; }
                Some(hash) = stream.next() => {
                    if let Some(txn) = self
                        .provider
                        .get_transaction(hash)
                        .await
                        .map_err(|e| IngestorError::Other(e.to_string()))? {
                        self.pending.insert(hash, txn.clone());
                        let evt = json!({
                            "type": "PendingTransaction",
                            "hash": format!("{:?}", hash),
                        });
                        tx.send(evt.to_string()).await.map_err(|e| IngestorError::Other(e.to_string()))?;
                        let from = txn.from;
                        if let Some(label) = self.labels.get(&from) {
                            let evt = json!({
                                "type": "AddressLabel",
                                "address": format!("{:?}", from),
                                "label": label,
                            });
                            tx.send(evt.to_string()).await.map_err(|e| IngestorError::Other(e.to_string()))?;
                        }
                        if let Some(addr) = txn.to {
                            if let Some(label) = self.labels.get(&addr) {
                                let evt = json!({
                                    "type": "AddressLabel",
                                    "address": format!("{:?}", addr),
                                    "label": label,
                                });
                                tx.send(evt.to_string()).await.map_err(|e| IngestorError::Other(e.to_string()))?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct OnchainFactory;

#[async_trait::async_trait]
impl super::AgentFactory for OnchainFactory {
    async fn create(&self, spec: &str, _cfg: &Settings) -> Option<Box<dyn Agent>> {
        // spec: ws_url[,label_file]
        let mut parts = spec.split(',');
        let ws_url = parts.next().unwrap_or("ws://localhost:8546");
        let label_file = parts.next();
        match OnchainAgent::new(ws_url, label_file).await {
            Ok(agent) => Some(Box::new(agent)),
            Err(e) => {
                tracing::error!("failed to create onchain agent: {}", e);
                None
            }
        }
    }
}
