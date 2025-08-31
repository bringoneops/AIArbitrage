use clap::Parser;
use serde::Deserialize;

/// Default refresh interval for the Coinbase websocket connection.
pub const DEFAULT_COINBASE_REFRESH_INTERVAL_MINS: u64 = 60;

/// Command line arguments
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Optional path to a configuration file
    #[arg(short, long)]
    pub config: Option<String>,

    /// Enable trade feeds
    #[arg(long)]
    pub trades: bool,

    /// Agent specifications (e.g. binance:btcusdt)
    pub specs: Vec<String>,
}

/// Application configuration loaded from file and environment
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub binance_ws_url: String,
    pub binance_refresh_interval_mins: u64,
    pub binance_max_reconnect_delay_secs: u64,
    #[serde(default)]
    pub binance_futures_rest_url: Option<String>,
    #[serde(default)]
    pub binance_futures_ws_url: Option<String>,
    #[serde(default)]
    pub binance_options_rest_url: String,
    #[serde(default)]
    pub binance_options_symbols: Vec<String>,
    #[serde(default = "default_binance_options_poll_interval_secs")]
    pub binance_options_poll_interval_secs: u64,
    #[serde(default)]
    pub binance_ohlcv_intervals: Vec<u64>,
    #[serde(default = "default_binance_ohlcv_poll_interval_secs")]
    pub binance_ohlcv_poll_interval_secs: u64,
    pub coinbase_ws_url: String,
    pub coinbase_refresh_interval_mins: u64,
    pub coinbase_max_reconnect_delay_secs: u64,
    #[serde(default)]
    pub coinbase_ohlcv_intervals: Vec<u64>,
    #[serde(default = "default_coinbase_ohlcv_poll_interval_secs")]
    pub coinbase_ohlcv_poll_interval_secs: u64,
    #[serde(default)]
    pub binance_api_key: Option<String>,
    #[serde(default)]
    pub binance_api_secret: Option<String>,
    #[serde(default)]
    pub coinbase_api_key: Option<String>,
    #[serde(default)]
    pub coinbase_api_secret: Option<String>,
    #[serde(default)]
    pub trades: bool,
}

fn default_binance_options_poll_interval_secs() -> u64 {
    60
}

fn default_binance_ohlcv_poll_interval_secs() -> u64 {
    60
}

fn default_coinbase_ohlcv_poll_interval_secs() -> u64 {
    60
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            binance_ws_url: String::new(),
            binance_refresh_interval_mins: 60,
            binance_max_reconnect_delay_secs: 30,
            binance_futures_rest_url: None,
            binance_futures_ws_url: None,
            binance_options_rest_url: String::new(),
            binance_options_symbols: Vec::new(),
            binance_options_poll_interval_secs: 60,
            binance_ohlcv_intervals: Vec::new(),
            binance_ohlcv_poll_interval_secs: 60,
            coinbase_ws_url: String::new(),
            coinbase_refresh_interval_mins: DEFAULT_COINBASE_REFRESH_INTERVAL_MINS,
            coinbase_max_reconnect_delay_secs: 30,
            coinbase_ohlcv_intervals: Vec::new(),
            coinbase_ohlcv_poll_interval_secs: 60,
            binance_api_key: None,
            binance_api_secret: None,
            coinbase_api_key: None,
            coinbase_api_secret: None,
            trades: false,
        }
    }
}

impl Settings {
    pub fn load(cli: &Cli) -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .set_default("binance_ws_url", "wss://stream.binance.us:9443/ws")?
            .set_default("binance_refresh_interval_mins", 60)?
            .set_default("binance_max_reconnect_delay_secs", 30)?
            .set_default("binance_futures_rest_url", "https://fapi.binance.com")?
            .set_default("binance_futures_ws_url", "wss://fstream.binance.com")?
            .set_default(
                "binance_options_rest_url",
                "https://eapi.binance.us/eapi/v1",
            )?
            .set_default("binance_options_poll_interval_secs", 60)?
            .set_default("binance_ohlcv_poll_interval_secs", 60)?
            .set_default("binance_ohlcv_intervals", vec![60])?
            .set_default("coinbase_ws_url", "wss://ws-feed.exchange.coinbase.com")?
            .set_default(
                "coinbase_refresh_interval_mins",
                DEFAULT_COINBASE_REFRESH_INTERVAL_MINS,
            )?
            .set_default("coinbase_max_reconnect_delay_secs", 30)?
            .set_default("coinbase_ohlcv_poll_interval_secs", 60)?
            .set_default("coinbase_ohlcv_intervals", vec![60])?
            .set_default("trades", false)?
            .add_source(config::Environment::with_prefix("INGESTOR").separator("_"));
        if let Some(path) = &cli.config {
            builder = builder.add_source(config::File::with_name(path));
        }
        let cfg = builder.build()?;
        let mut settings: Settings = cfg.try_deserialize()?;
        // populate API keys from environment if not set in config
        settings.binance_api_key = settings
            .binance_api_key
            .or_else(|| std::env::var("BINANCE_API_KEY").ok());
        settings.binance_api_secret = settings
            .binance_api_secret
            .or_else(|| std::env::var("BINANCE_API_SECRET").ok());
        settings.coinbase_api_key = settings
            .coinbase_api_key
            .or_else(|| std::env::var("COINBASE_API_KEY").ok());
        settings.coinbase_api_secret = settings
            .coinbase_api_secret
            .or_else(|| std::env::var("COINBASE_API_SECRET").ok());
        settings.trades = settings.trades || cli.trades;
        settings.binance_futures_rest_url =
            settings.binance_futures_rest_url.filter(|s| !s.is_empty());
        settings.binance_futures_ws_url = settings.binance_futures_ws_url.filter(|s| !s.is_empty());
        Ok(settings)
    }
}
