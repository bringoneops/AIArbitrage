use reqwest::ClientBuilder;

/// Build a `reqwest::ClientBuilder` configured for this crate.
///
/// Certificate verification is enabled by default. To allow invalid
/// certificates (useful for development with self-signed certs), set the
/// `BINANCE_ACCEPT_INVALID_CERTS` environment variable to a truthy value
/// (`1`, `true`, `yes`). Disabling certificate verification is strongly
/// discouraged for production use.
pub fn builder() -> ClientBuilder {
    let mut builder = reqwest::Client::builder();
    let allow_invalid = std::env::var("BINANCE_ACCEPT_INVALID_CERTS")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if allow_invalid {
        builder = builder.danger_accept_invalid_certs(true);
    }
    builder
}
