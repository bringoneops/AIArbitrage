use reqwest::ClientBuilder;

/// Build a `reqwest::ClientBuilder` configured for this crate.
///
/// Certificate verification is disabled to allow operation against hosts with
/// self-signed or otherwise untrusted certificates. **This should not be used
/// in production.**
pub fn builder() -> ClientBuilder {
    reqwest::Client::builder().danger_accept_invalid_certs(true)
}
