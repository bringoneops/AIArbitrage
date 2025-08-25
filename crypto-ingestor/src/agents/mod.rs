pub mod binance;

use crate::agent::Agent;

/// Factory: "<agent>:<comma-separated-args>"
/// e.g., "binance:btcusdt,ethusdt" or "binance:all"
pub async fn make_agent(spec: &str) -> Option<Box<dyn Agent>> {
    let (name, args) = match spec.split_once(':') {
        Some((n, a)) => (n.trim().to_lowercase(), a.trim().to_string()),
        None => (spec.trim().to_lowercase(), String::new()),
    };

    match name.as_str() {
        "binance" => {
            let symbols = if args.is_empty() || args.eq_ignore_ascii_case("all") {
                None
            } else {
                Some(
                    args.split(',')
                        .map(|s| s.trim().to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>(),
                )
            };

            match binance::BinanceAgent::new(symbols).await {
                Ok(agent) => Some(Box::new(agent)),
                Err(e) => {
                    tracing::error!(error=%e, "failed to create binance agent");
                    None
                }
            }
        }
        _ => None,
    }
}

pub fn available_agents() -> &'static [&'static str] {
    &["binance:<csv symbols|all>  (e.g. binance:btcusdt,ethusdt)"]
}
