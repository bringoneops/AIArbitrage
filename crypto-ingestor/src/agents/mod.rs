pub mod binance;
pub mod coinbase;

use crate::{agent::Agent, config::Settings, error::IngestorError};
use canonicalizer::CanonicalService;
use std::collections::HashMap;

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

    match name.as_str() {
        "binance" => {
            let symbols = if args.is_empty() || args.eq_ignore_ascii_case("all") {
                match shared_symbols().await {
                    Ok((b, _)) => Some(b),
                    Err(e) => {
                        tracing::error!(error=%e, "failed to fetch shared symbols");
                        return None;
                    }
                }
            } else {
                Some(
                    args.split(',')
                        .map(|s| s.trim().to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>(),
                )
            };

            match binance::BinanceAgent::new(symbols, cfg).await {
                Ok(agent) => Some(Box::new(agent)),
                Err(e) => {
                    tracing::error!(error=%e, "failed to create binance agent");
                    None
                }
            }
        }
        "coinbase" => {
            let symbols = if args.is_empty() {
                vec!["BTC-USD".to_string()]
            } else if args.eq_ignore_ascii_case("all") {
                match shared_symbols().await {
                    Ok((_, c)) => c,
                    Err(e) => {
                        tracing::error!(error=%e, "failed to fetch shared symbols");
                        return None;
                    }
                }
            } else {
                args.split(',')
                    .map(|s| s.trim().to_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            };
            Some(Box::new(coinbase::CoinbaseAgent::new(symbols, cfg)))
        }
        _ => None,
    }
}

pub fn available_agents() -> &'static [&'static str] {
    &[
        "binance:<csv symbols|all>  (e.g. binance:btcusdt,ethusdt)",
        "coinbase:<csv pairs|all>   (e.g. coinbase:BTC-USD,ETH-USD)",
    ]
}
