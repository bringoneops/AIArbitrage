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

    /// Output sink type (stdout, kafka, file)
    #[arg(long, default_value = "stdout")]
    pub sink: String,

    /// Kafka broker list
    #[arg(long)]
    pub kafka_brokers: Option<String>,

    /// Kafka topic
    #[arg(long)]
    pub kafka_topic: Option<String>,

    /// Output file path
    #[arg(long)]
    pub file_path: Option<String>,

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
    pub binance_options_rest_url: String,
    #[serde(default)]
    pub binance_options_symbols: Vec<String>,
    #[serde(default)]
    pub binance_options_expiries: Vec<String>,
    #[serde(default = "default_binance_options_poll_interval_secs")]
    pub binance_options_poll_interval_secs: u64,
    pub coinbase_ws_url: String,
    pub coinbase_refresh_interval_mins: u64,
    pub coinbase_max_reconnect_delay_secs: u64,
    #[serde(default = "default_sink")]
    pub sink: String,
    #[serde(default)]
    pub kafka_brokers: Option<String>,
    #[serde(default)]
    pub kafka_topic: Option<String>,
    #[serde(default)]
    pub file_path: Option<String>,
}

fn default_sink() -> String {
    "stdout".into()
}

fn default_binance_options_poll_interval_secs() -> u64 {
    60
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            binance_ws_url: String::new(),
            binance_refresh_interval_mins: 60,
            binance_max_reconnect_delay_secs: 30,
            binance_options_rest_url: String::new(),
            binance_options_symbols: Vec::new(),
            binance_options_expiries: Vec::new(),
            binance_options_poll_interval_secs: 60,
            coinbase_ws_url: String::new(),
            coinbase_refresh_interval_mins: DEFAULT_COINBASE_REFRESH_INTERVAL_MINS,
            coinbase_max_reconnect_delay_secs: 30,
            sink: default_sink(),
            kafka_brokers: None,
            kafka_topic: None,
            file_path: None,
        }
    }
}

impl Settings {
    pub fn load(cli: &Cli) -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .set_default("binance_ws_url", "wss://stream.binance.us:9443/ws")?
            .set_default("binance_refresh_interval_mins", 60)?
            .set_default("binance_max_reconnect_delay_secs", 30)?
            .set_default("binance_options_rest_url", "https://eapi.binance.com/eapi/v1")?
            .set_default("binance_options_poll_interval_secs", 60)?
            .set_default("coinbase_ws_url", "wss://ws-feed.exchange.coinbase.com")?
            .set_default(
                "coinbase_refresh_interval_mins",
                DEFAULT_COINBASE_REFRESH_INTERVAL_MINS,
            )?
            .set_default("coinbase_max_reconnect_delay_secs", 30)?
            .set_default("sink", "stdout")?
            .add_source(config::Environment::with_prefix("INGESTOR").separator("_"));
        if let Some(path) = &cli.config {
            builder = builder.add_source(config::File::with_name(path));
        }
        let cfg = builder.build()?;
        let mut settings: Settings = cfg.try_deserialize()?;
        settings.sink = cli.sink.clone();
        if let Some(b) = &cli.kafka_brokers {
            settings.kafka_brokers = Some(b.clone());
        }
        if let Some(t) = &cli.kafka_topic {
            settings.kafka_topic = Some(t.clone());
        }
        if let Some(p) = &cli.file_path {
            settings.file_path = Some(p.clone());
        }
        Ok(settings)
    }
}
