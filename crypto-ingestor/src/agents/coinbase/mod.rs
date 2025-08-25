use crate::agent::Agent;

pub struct CoinbaseAgent {
    symbols: Vec<String>,
    interval_secs: u64,
}

impl CoinbaseAgent {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            interval_secs: 5,
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
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(self.interval_secs));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
                _ = interval.tick() => {
                    for sym in &self.symbols {
                        let url = format!("https://api.coinbase.com/v2/prices/{sym}/spot");
                        match client.get(&url).send().await {
                            Ok(resp) => {
                                match resp.json::<serde_json::Value>().await {
                                    Ok(val) => {
                                        let price = val.get("data")
                                            .and_then(|d| d.get("amount"))
                                            .and_then(|a| a.as_str())
                                            .unwrap_or("?");
                                        tracing::info!(%sym, %price, "coinbase spot");
                                    }
                                    Err(e) => tracing::error!(%sym, error=%e, "parse error"),
                                }
                            }
                            Err(e) => tracing::error!(%sym, error=%e, "request error"),
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
