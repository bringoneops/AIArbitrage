use reqwest::ClientBuilder;

/// Build a `reqwest::ClientBuilder` configured for the current runtime.
///
/// Certificate verification is enabled by default.  To opt out (for example,
/// when working with self-signed certificates in development), set the
/// environment variable `INGESTOR_ACCEPT_INVALID_CERTS` to a truthy value
/// (`1`, `true`, `yes`).  Disabling certificate verification is strongly
/// discouraged for production use.
pub fn builder() -> ClientBuilder {
    let mut builder = reqwest::Client::builder();
    let allow_invalid = std::env::var("INGESTOR_ACCEPT_INVALID_CERTS")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if allow_invalid {
        builder = builder.danger_accept_invalid_certs(true);
    }
    builder
}
