pub mod binance;

use crate::agent::Agent;

/// Factory: "<agent>:<comma-separated-args>"
/// e.g., "binance:btcusdt,ethusdt"
pub fn make_agent(spec: &str) -> Option<Box<dyn Agent>> {
    let (name, args) = match spec.split_once(':') {
        Some((n, a)) => (n.trim().to_lowercase(), a.trim().to_string()),
        None => (spec.trim().to_lowercase(), String::new()),
    };

    match name.as_str() {
        "binance" => {
            let symbols: Vec<String> = if args.is_empty() {
                vec!["btcusdt".into()]
            } else {
                args.split(',')
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            };
            Some(Box::new(binance::BinanceAgent::new(symbols)))
        }
        _ => None,
    }
}

pub fn available_agents() -> &'static [&'static str] {
    &["binance:<csv symbols>  (e.g. binance:btcusdt,ethusdt)"]
}
