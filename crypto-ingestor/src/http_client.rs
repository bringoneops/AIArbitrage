use reqwest::ClientBuilder;

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Compute an HMAC SHA256 signature for the given payload.
pub fn hmac_sha256(secret: &str, payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Build a `reqwest::ClientBuilder` configured for the current runtime.
///
/// Certificate verification is disabled in this environment to allow
/// connections to hosts with self-signed or otherwise untrusted certificates.
/// **Do not enable this behaviour in production.**
pub fn builder() -> ClientBuilder {
    reqwest::Client::builder().danger_accept_invalid_certs(true)
}
