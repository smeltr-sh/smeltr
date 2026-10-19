//! The clients the leader opens to services it alone talks to: the OpenID provider and the model endpoint. Each
//! verifies the far end through AWS-LC against the public roots, and reads what comes back to a bound.

use std::time::Duration;

use crate::io::discovery::check_url;

/// How long reaching the far end may take, apart from its answer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A client for the service at `url`, called `what` in what it says: HTTPS only unless `url` is plain HTTP on this
/// host. `timeout` bounds each request, its answer included.
pub fn client(
    url: &str,
    what: &str,
    timeout: Duration,
    redirect: reqwest::redirect::Policy,
) -> Result<reqwest::Client, String> {
    let scheme = check_url(url, false)?;
    let roots = crate::io::trust::store(None).map_err(|e| e.to_string())?;
    let tls = rustls::ClientConfig::builder_with_provider(crate::crypto::in_use())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("{what}'s TLS: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .https_only(scheme == "https")
        .timeout(timeout)
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(redirect)
        .build()
        .map_err(|e| format!("could not make a client for {what}: {e}"))
}

/// The body, read a chunk at a time so a service that sends more than `limit` is cut off at it, whatever it says
/// its length is.
pub async fn read(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("could not read the answer: {e}"))?
    {
        if body.len() + chunk.len() > limit {
            return Err(format!("the answer is longer than {} KiB", limit / 1024));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
