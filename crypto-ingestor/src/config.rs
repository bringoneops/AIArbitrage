use clap::Parser;
use serde::Deserialize;

/// Command line arguments
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Optional path to a configuration file
    #[arg(short, long)]
    pub config: Option<String>,

    /// Agent specifications (e.g. binance:btcusdt)
    pub specs: Vec<String>,
}

/// Application configuration loaded from file and environment
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub binance_ws_url: String,
    pub binance_refresh_interval_mins: u64,
    pub binance_max_reconnect_delay_secs: u64,
    pub coinbase_ws_url: String,
    pub coinbase_refresh_interval_mins: u64,
    pub coinbase_max_reconnect_delay_secs: u64,
}

impl Settings {
    pub fn load(cli: &Cli) -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .set_default("binance_ws_url", "wss://stream.binance.us:9443/ws")?
            .set_default("binance_refresh_interval_mins", 60)?
            .set_default("binance_max_reconnect_delay_secs", 30)?
            .set_default("coinbase_ws_url", "wss://ws-feed.exchange.coinbase.com")?
            .set_default("coinbase_refresh_interval_mins", 60)?
            .set_default("coinbase_max_reconnect_delay_secs", 30)?
            .add_source(config::Environment::with_prefix("INGESTOR").separator("_"));
        if let Some(path) = &cli.config {
            builder = builder.add_source(config::File::with_name(path));
        }
        let cfg = builder.build()?;
        cfg.try_deserialize()
    }
}
