use reqwest::ClientBuilder;

/// Build a `reqwest::ClientBuilder` configured for this crate.
///
/// The builder uses reqwest's default TLS settings which enforce certificate
/// verification.
pub fn builder() -> ClientBuilder {
    reqwest::Client::builder()
}
