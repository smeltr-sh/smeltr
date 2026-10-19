//! Reading an OpenID provider's discovery document, `/.well-known/openid-configuration`: where its endpoints are and
//! how its token endpoint takes a client's secret. The leader reads one to sign people in, a source to sign in as an
//! app.
//!
//! What the provider sends is untrusted input: the document is read to a bound, must name the issuer it was asked
//! for exactly, and every endpoint it names must pass the issuer's URL rule over the issuer's scheme.

use std::time::Duration;

use crate::io::client_credentials::TokenAuth;

/// The most a discovery document may hold.
pub const MAX_DISCOVERY: usize = 64 * 1024;

/// Whether `url` may be an issuer: `https://`, or `http://` to this host alone (a local Keycloak, a test). It names a
/// host and no user, query or fragment, since the issuer is compared character for character with what the provider
/// says it is.
pub fn check_issuer(url: &str) -> Result<(), String> {
    check_url(url, false)?;
    Ok(())
}

/// `url`'s scheme, once it passes the issuer's rule; an endpoint may also carry a query (RFC 6749 3.1).
pub fn check_url(url: &str, query_allowed: bool) -> Result<&'static str, String> {
    if url.is_empty() || !url.chars().all(|c| c.is_ascii_graphic()) || url.contains('\\') {
        return Err(format!(
            "{url:?} is not a URL: it is empty or holds spaces or backslashes"
        ));
    }
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("{url:?} is not a URL: {e}"))?;
    // A URL parser forgives `https:///host`; the provider's own comparison would not.
    let authority = url
        .split_once("://")
        .map_or("", |(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""));
    if authority.is_empty() || parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!("{url:?} names no host"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "{url:?} names a user: the provider's address names only the provider"
        ));
    }
    if parsed.query().is_some() && !query_allowed {
        return Err(format!("{url:?} has a query; an issuer has none"));
    }
    if parsed.fragment().is_some() {
        return Err(format!("{url:?} has a fragment"));
    }
    match parsed.scheme() {
        "https" => Ok("https"),
        "http" if !crate::config_file::plain_off_this_host(url) => Ok("http"),
        "http" => Err(format!(
            "{url:?} is plain HTTP, which only this host may be reached over. Use https://"
        )),
        _ => Err(format!("{url:?} is not an https:// URL")),
    }
}

/// What a provider's discovery document says. Every field is the provider's word, checked only by [`read`] and
/// [`Document::endpoint`].
#[derive(serde::Deserialize)]
pub struct Discovery {
    pub issuer: Option<String>,
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
    pub jwks_uri: Option<String>,
    #[serde(default)]
    pub response_types_supported: Vec<String>,
    pub code_challenge_methods_supported: Option<Vec<String>>,
    pub token_endpoint_auth_methods_supported: Option<Vec<String>>,
    pub id_token_signing_alg_values_supported: Option<Vec<String>>,
}

/// A discovery document that names the issuer it was read for.
pub struct Document {
    pub found: Discovery,
    url: String,
    scheme: &'static str,
}

impl Document {
    /// `value`, the endpoint the document names as `field`, once it passes the issuer's URL rule over the issuer's
    /// scheme.
    pub fn endpoint(&self, field: &str, value: Option<String>) -> Result<String, String> {
        let url = &self.url;
        let value = value.ok_or_else(|| format!("{url} names no {field}"))?;
        let ok = check_url(&value, true).map_err(|e| format!("its {field}: {e}"))?;
        if ok != self.scheme {
            return Err(format!(
                "its {field} {value:?} is not {}:// like the issuer",
                self.scheme
            ));
        }
        Ok(value)
    }
}

/// Where `issuer`'s discovery document is.
pub fn address(issuer: &str) -> String {
    // OpenID Connect Discovery 4: a trailing slash is removed before the well-known path is appended.
    format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'))
}

/// `body`, read from [`address`] of `issuer`. Refused unless it names `issuer` exactly.
pub fn read(issuer: &str, body: &[u8]) -> Result<Document, String> {
    let scheme = check_url(issuer, false)?;
    let url = address(issuer);
    let found: Discovery =
        serde_json::from_slice(body).map_err(|e| format!("{url} is not a discovery document: {e}"))?;
    let named = found.issuer.clone().unwrap_or_default();
    if named != issuer {
        return Err(format!(
            "{url} names the issuer {:?}, not {issuer:?}; set the issuer exactly as the provider writes it",
            said(named.as_bytes())
        ));
    }
    Ok(Document { found, url, scheme })
}

/// How `issuer`'s token endpoint takes a client secret, from the methods its document lists.
pub fn secret_auth(issuer: &str, methods: Option<&[String]>) -> Result<TokenAuth, String> {
    // OAuth 2.0 Authorization Server Metadata 2: absent means client_secret_basic.
    match methods {
        None => Ok(TokenAuth::Basic),
        Some(m) if m.iter().any(|m| m == "client_secret_basic") => Ok(TokenAuth::Basic),
        Some(m) if m.iter().any(|m| m == "client_secret_post") => Ok(TokenAuth::Post),
        Some(_) => Err(format!(
            "{issuer}'s token endpoint takes neither client_secret_basic nor client_secret_post"
        )),
    }
}

/// What an app signing in at `issuer` needs from its document: the token endpoint, and the client authentication
/// methods it lists, when it lists them. A blocking GET over `agent`, bounded by `timeout`.
pub fn token_endpoint(
    agent: &ureq::Agent,
    timeout: Duration,
    issuer: &str,
) -> Result<(String, Option<Vec<String>>), String> {
    check_url(issuer, false)?;
    let url = address(issuer);
    let mut response = agent
        .get(&url)
        .config()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        // The document says where the issuer is; one found elsewhere is not its word.
        .max_redirects(0)
        .build()
        .header("Accept", "application/json")
        .call()
        .map_err(|e| format!("could not reach {url}: {e}"))?;
    let status = response.status().as_u16();
    let body = response
        .body_mut()
        .with_config()
        .limit(MAX_DISCOVERY as u64)
        .read_to_vec()
        .map_err(|e| format!("could not read {url}: {e}"))?;
    if !(200..300).contains(&status) {
        return Err(format!("{url} answered {status}: {}", said(&body)));
    }
    let mut document = read(issuer, &body)?;
    let methods = document.found.token_endpoint_auth_methods_supported.take();
    let token = document.found.token_endpoint.take();
    Ok((document.endpoint("token_endpoint", token)?, methods))
}

/// At most 200 bytes of what a provider sent, as one line of text.
pub fn said(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .trim()
        .to_string()
}
