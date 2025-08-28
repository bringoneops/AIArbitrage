use reqwest::ClientBuilder;


/// Build a `reqwest::ClientBuilder` configured for the current runtime.
///
/// Certificate verification is disabled in this environment to allow
/// connections to hosts with self-signed or otherwise untrusted certificates.
/// **Do not enable this behaviour in production.**
pub fn builder() -> ClientBuilder {
    reqwest::Client::builder().danger_accept_invalid_certs(true)
}
