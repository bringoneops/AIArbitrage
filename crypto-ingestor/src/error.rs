use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestorError {
    #[error("HTTP request failed for {exchange} {symbol:?}: {source}")]
    Http {
        #[source]
        source: reqwest::Error,
        exchange: &'static str,
        symbol: Option<String>,
    },
    #[error(transparent)]
    Config(#[from] ::config::ConfigError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}
