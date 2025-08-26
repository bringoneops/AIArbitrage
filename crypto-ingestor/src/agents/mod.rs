pub mod binance;
pub mod coinbase;

use crate::{agent::Agent, config::Settings, error::IngestorError};
use canonicalizer::CanonicalService;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;

#[async_trait::async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self, spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>>;
}

pub static AGENT_FACTORIES: Lazy<Mutex<HashMap<&'static str, Box<dyn AgentFactory>>>> =
    Lazy::new(|| {
        let mut m: HashMap<&'static str, Box<dyn AgentFactory>> = HashMap::new();
        m.insert("binance", Box::new(binance::BinanceFactory));
        m.insert("binance_options", Box::new(binance::options::BinanceOptionsFactory));
        m.insert("coinbase", Box::new(coinbase::CoinbaseFactory));
        Mutex::new(m)
    });

async fn shared_symbols() -> Result<(Vec<String>, Vec<String>), IngestorError> {
    // Ensure that the canonicalizer has loaded the quote asset list before we
    // attempt any symbol comparisons.
    CanonicalService::init().await;

    let (binance_all, coinbase_all) =
        tokio::try_join!(binance::fetch_all_symbols(), coinbase::fetch_all_symbols())?;

    let mut bmap: HashMap<String, String> = HashMap::new();
    for s in binance_all.into_iter() {
        if let Some(c) = CanonicalService::canonical_pair("binance", &s) {
            bmap.insert(c, s);
        }
    }

    let mut cmap: HashMap<String, String> = HashMap::new();
    for s in coinbase_all.into_iter() {
        if let Some(c) = CanonicalService::canonical_pair("coinbase", &s) {
            cmap.insert(c, s);
        }
    }

    let mut b_syms = Vec::new();
    let mut c_syms = Vec::new();
    for (canon, b) in bmap.iter() {
        if let Some(c) = cmap.get(canon) {
            b_syms.push(b.clone());
            c_syms.push(c.clone());
        }
    }

    Ok((b_syms, c_syms))
}

/// Factory: "<agent>:<comma-separated-args>"
/// e.g., "binance:btcusdt,ethusdt" or "binance:all"
pub async fn make_agent(spec: &str, cfg: &Settings) -> Option<Box<dyn Agent>> {
    let (name, args) = match spec.split_once(':') {
        Some((n, a)) => (n.trim().to_lowercase(), a.trim().to_string()),
        None => (spec.trim().to_lowercase(), String::new()),
    };

    if let Some(factory) = AGENT_FACTORIES.lock().unwrap().get(name.as_str()) {
        factory.create(&args, cfg).await
    } else {
        None
    }
}

pub fn available_agents() -> Vec<&'static str> {
    AGENT_FACTORIES.lock().unwrap().keys().copied().collect()
}
