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

    /// Enable trade feeds
    #[arg(long)]
    pub trades: bool,

    /// Enable level 2 diff order book feeds
    #[arg(long)]
    pub l2_diffs: bool,

    /// Enable level 2 snapshot order book feeds
    #[arg(long)]
    pub l2_snapshots: bool,

    /// Enable book ticker updates
    #[arg(long)]
    pub book_ticker: bool,

    /// Enable rolling 24h ticker updates
    #[arg(long)]
    pub ticker_24h: bool,

    /// Enable OHLCV candle data
    #[arg(long)]
    pub ohlcv: bool,

    /// Enable index price feeds
    #[arg(long)]
    pub index_price: bool,

    /// Enable mark price feeds
    #[arg(long)]
    pub mark_price: bool,

    /// Enable funding rates
    #[arg(long)]
    pub funding_rates: bool,

    /// Enable open interest data
    #[arg(long)]
    pub open_interest: bool,

    /// Enable onchain transfer feeds
    #[arg(long)]
    pub onchain_transfers: bool,

    /// Enable onchain balance feeds
    #[arg(long)]
    pub onchain_balances: bool,

    /// Enable top DEX pool price feeds
    #[arg(long)]
    pub top_dex_pools: bool,

    /// Enable news headline feeds
    #[arg(long)]
    pub news_headlines: bool,

    /// Enable telemetry events
    #[arg(long)]
    pub telemetry: bool,

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
    pub deribit_options_rest_url: String,
    #[serde(default)]
    pub deribit_options_symbols: Vec<String>,
    #[serde(default = "default_deribit_options_poll_interval_secs")]
    pub deribit_options_poll_interval_secs: u64,
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
    #[serde(default = "default_sink")]
    pub sink: String,
    #[serde(default)]
    pub kafka_brokers: Option<String>,
    #[serde(default)]
    pub kafka_topic: Option<String>,
    #[serde(default)]
    pub file_path: Option<String>,

    #[serde(default)]
    pub trades: bool,
    #[serde(default)]
    pub l2_diffs: bool,
    #[serde(default)]
    pub l2_snapshots: bool,
    #[serde(default)]
    pub book_ticker: bool,
    #[serde(default)]
    pub ticker_24h: bool,
    #[serde(default)]
    pub ohlcv: bool,
    #[serde(default)]
    pub index_price: bool,
    #[serde(default)]
    pub mark_price: bool,
    #[serde(default)]
    pub funding_rates: bool,
    #[serde(default)]
    pub open_interest: bool,
    #[serde(default)]
    pub onchain_transfers: bool,
    #[serde(default)]
    pub onchain_balances: bool,
    #[serde(default)]
    pub top_dex_pools: bool,
    #[serde(default)]
    pub news_headlines: bool,
    #[serde(default)]
    pub telemetry: bool,
}

fn default_sink() -> String {
    "stdout".into()
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

fn default_deribit_options_poll_interval_secs() -> u64 {
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
            deribit_options_rest_url: String::new(),
            deribit_options_symbols: Vec::new(),
            deribit_options_poll_interval_secs: 60,
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
            sink: default_sink(),
            kafka_brokers: None,
            kafka_topic: None,
            file_path: None,
            trades: false,
            l2_diffs: false,
            l2_snapshots: false,
            book_ticker: false,
            ticker_24h: false,
            ohlcv: false,
            index_price: false,
            mark_price: false,
            funding_rates: false,
            open_interest: false,
            onchain_transfers: false,
            onchain_balances: false,
            top_dex_pools: false,
            news_headlines: false,
            telemetry: false,
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
            .set_default("sink", "stdout")?
            .set_default("trades", false)?
            .set_default("l2_diffs", false)?
            .set_default("l2_snapshots", false)?
            .set_default("book_ticker", false)?
            .set_default("ticker_24h", false)?
            .set_default("ohlcv", false)?
            .set_default("index_price", false)?
            .set_default("mark_price", false)?
            .set_default("funding_rates", false)?
            .set_default("open_interest", false)?
            .set_default("onchain_transfers", false)?
            .set_default("onchain_balances", false)?
            .set_default("top_dex_pools", false)?
            .set_default("news_headlines", false)?
            .set_default("telemetry", false)?
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
        settings.l2_diffs = settings.l2_diffs || cli.l2_diffs;
        settings.l2_snapshots = settings.l2_snapshots || cli.l2_snapshots;
        settings.book_ticker = settings.book_ticker || cli.book_ticker;
        settings.ticker_24h = settings.ticker_24h || cli.ticker_24h;
        settings.ohlcv = settings.ohlcv || cli.ohlcv;
        settings.index_price = settings.index_price || cli.index_price;
        settings.mark_price = settings.mark_price || cli.mark_price;
        settings.funding_rates = settings.funding_rates || cli.funding_rates;
        settings.open_interest = settings.open_interest || cli.open_interest;
        settings.onchain_transfers = settings.onchain_transfers || cli.onchain_transfers;
        settings.onchain_balances = settings.onchain_balances || cli.onchain_balances;
        settings.top_dex_pools = settings.top_dex_pools || cli.top_dex_pools;
        settings.news_headlines = settings.news_headlines || cli.news_headlines;
        settings.telemetry = settings.telemetry || cli.telemetry;
        settings.binance_futures_rest_url =
            settings.binance_futures_rest_url.filter(|s| !s.is_empty());
        settings.binance_futures_ws_url = settings.binance_futures_ws_url.filter(|s| !s.is_empty());
        Ok(settings)
    }
}
