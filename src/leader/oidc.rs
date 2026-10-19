//! Signing a person in through their organization's OpenID provider: the authorization code flow with PKCE, and the
//! ID token it ends in checked against the provider's published keys.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant B as browser
//!   participant L as leader
//!   participant P as provider
//!   L->>P: GET /.well-known/openid-configuration, then the JWKS it names
//!   B->>L: sign in
//!   L-->>B: redirect to the authorization endpoint: state, nonce, PKCE challenge
//!   B->>P: the person signs in there
//!   P-->>B: redirect back: code, state
//!   B->>L: code, state
//!   L->>P: token endpoint: code, PKCE verifier, client secret
//!   P-->>L: ID token, refresh token
//!   L->>L: signature by a JWKS key, issuer, audience, times, nonce
//!   loop while the person is signed in
//!     L->>P: token endpoint: refresh token, client secret
//!     P-->>L: a fresh ID token and refresh token, or invalid_grant
//!   end
//! ```
//!
//! Everything the provider sends is untrusted input: each answer is read to a bound, and a token is believed only once
//! its signature checks out. Every signature is checked by AWS-LC, the same module TLS runs through.

use std::time::Duration;

use aws_lc_rs::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey, VerificationAlgorithm};
use base64::Engine as _;

use super::outbound::read;
pub use crate::io::client_credentials::TokenAuth;
use crate::io::client_credentials::basic;
pub use crate::io::discovery::check_issuer;
use crate::io::discovery::{self, MAX_DISCOVERY, check_url, said};
use crate::io::http::percent_encode;

/// How long any one request to the provider may take.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The most a key set may hold.
const MAX_JWKS: usize = 256 * 1024;

/// The most a token endpoint's answer may hold, and so the longest ID token.
const MAX_TOKEN_ANSWER: usize = 64 * 1024;

/// How far the provider's clock may be from ours.
const LEEWAY_SECS: i64 = 60;

/// The longest subject OpenID Connect allows.
const MAX_SUBJECT: usize = 255;

/// The scopes asked for: who the person is, their name and their address.
const SCOPE: &str = "openid profile email";

/// base64url, as JWS writes it. Padding is tolerated on reading, since what is signed is the text as sent.
const B64URL: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::URL_SAFE,
    base64::engine::GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

/// A client for talking to `issuer`'s provider: TLS through AWS-LC, trusting the public roots, HTTPS only unless the
/// issuer is plain HTTP on this host. A redirect is followed only within the origin it came from, so a form carrying
/// the client secret never moves to another host.
pub fn client(issuer: &str) -> Result<reqwest::Client, String> {
    let redirect = reqwest::redirect::Policy::custom(|attempt| {
        let previous = attempt.previous().last();
        if attempt.previous().len() > 5 {
            attempt.error("the provider redirected more than five times")
        } else if previous.is_some_and(|p| p.origin() == attempt.url().origin()) {
            attempt.follow()
        } else {
            attempt.stop()
        }
    });
    super::outbound::client(issuer, "the provider", TIMEOUT, redirect)
}

/// What discovery found out about a provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provider {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub jwks_uri: String,
    pub token_auth: TokenAuth,
}

/// Reads `issuer`'s discovery document. Refused unless it names the same issuer exactly, every endpoint passes the
/// issuer's URL rule over the issuer's scheme, and the provider can do what sign-in needs: the code flow, S256 PKCE
/// when it lists its methods, a client secret, and a signature this module checks.
pub async fn discover(client: &reqwest::Client, issuer: &str) -> Result<Provider, String> {
    check_url(issuer, false)?;
    let body = get(client, &discovery::address(issuer), MAX_DISCOVERY).await?;
    let document = discovery::read(issuer, &body)?;
    let found = &document.found;
    let authorization_endpoint = document.endpoint("authorization_endpoint", found.authorization_endpoint.clone())?;
    let token_endpoint = document.endpoint("token_endpoint", found.token_endpoint.clone())?;
    let jwks_uri = document.endpoint("jwks_uri", found.jwks_uri.clone())?;
    if !found.response_types_supported.iter().any(|t| t == "code") {
        return Err(format!("{issuer} does not offer the authorization code flow"));
    }
    if found
        .code_challenge_methods_supported
        .as_ref()
        .is_some_and(|m| !m.iter().any(|m| m == "S256"))
    {
        return Err(format!("{issuer} does not offer S256 PKCE"));
    }
    if found
        .id_token_signing_alg_values_supported
        .as_ref()
        .is_some_and(|algs| !algs.iter().any(|a| Alg::named(a).is_some()))
    {
        return Err(format!(
            "{issuer} signs ID tokens with none of RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384"
        ));
    }
    let token_auth = discovery::secret_auth(issuer, found.token_endpoint_auth_methods_supported.as_deref())?;
    Ok(Provider {
        issuer: issuer.to_string(),
        authorization_endpoint,
        token_endpoint,
        jwks_uri,
        token_auth,
    })
}

/// The keys a provider signs ID tokens with.
#[derive(Clone, Debug, Default)]
pub struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Clone, Debug)]
struct Jwk {
    kid: Option<String>,
    /// The algorithm the key is pinned to, when the provider pins one.
    alg: Option<String>,
    key: PublicKey,
}

#[derive(Clone, Debug)]
enum PublicKey {
    /// Modulus and exponent, big-endian with no leading zeros, as AWS-LC takes them.
    Rsa { n: Vec<u8>, e: Vec<u8> },
    /// The uncompressed point: 0x04, then x and y.
    Ec { curve: Curve, point: Vec<u8> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Curve {
    P256,
    P384,
}

/// Fetches the key set at `uri`.
pub async fn fetch_jwks(client: &reqwest::Client, uri: &str) -> Result<Jwks, String> {
    Jwks::parse(&get(client, uri, MAX_JWKS).await?).map_err(|e| format!("{uri}: {e}"))
}

impl Jwks {
    /// A key set as JSON. RSA and EC P-256 and P-384 keys meant for signatures are kept; any other key is skipped, so
    /// a set that also holds encryption keys still works. A set with no key kept is refused.
    pub fn parse(json: &[u8]) -> Result<Jwks, String> {
        let set: serde_json::Value =
            serde_json::from_slice(json).map_err(|e| format!("the key set is not JSON: {e}"))?;
        let listed = set
            .get("keys")
            .and_then(|k| k.as_array())
            .ok_or("the key set has no keys array")?;
        let keys: Vec<Jwk> = listed.iter().filter_map(Jwk::parse).collect();
        if keys.is_empty() {
            return Err("the key set holds no RSA or EC P-256/P-384 signing key".to_string());
        }
        Ok(Jwks { keys })
    }
}

impl Jwk {
    fn parse(key: &serde_json::Value) -> Option<Jwk> {
        let text = |field: &str| key.get(field).and_then(|v| v.as_str());
        let bytes = |field: &str| text(field).and_then(|v| B64URL.decode(v).ok());
        if text("use").is_some_and(|u| u != "sig") {
            return None;
        }
        let public = match text("kty")? {
            "RSA" => {
                // RFC 7518 says no leading zeros; some providers write one anyway, and AWS-LC takes none.
                let strip = |v: Vec<u8>| v.iter().position(|b| *b != 0).map(|i| v[i..].to_vec());
                PublicKey::Rsa {
                    n: strip(bytes("n")?)?,
                    e: strip(bytes("e")?)?,
                }
            }
            "EC" => {
                let (curve, len) = match text("crv")? {
                    "P-256" => (Curve::P256, 32),
                    "P-384" => (Curve::P384, 48),
                    _ => return None,
                };
                let (x, y) = (bytes("x")?, bytes("y")?);
                if x.len() != len || y.len() != len {
                    return None;
                }
                let mut point = Vec::with_capacity(1 + 2 * len);
                point.push(0x04);
                point.extend_from_slice(&x);
                point.extend_from_slice(&y);
                PublicKey::Ec { curve, point }
            }
            _ => return None,
        };
        Some(Jwk {
            kid: text("kid").map(str::to_string),
            alg: text("alg").map(str::to_string),
            key: public,
        })
    }
}

/// A PKCE pair (RFC 7636): the verifier is kept, the challenge is sent.
#[derive(Clone, Debug)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

/// A fresh PKCE pair, S256.
pub fn pkce() -> Pkce {
    let verifier = random_token();
    let challenge = s256(&verifier);
    Pkce { verifier, challenge }
}

fn s256(verifier: &str) -> String {
    B64URL.encode(aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA256,
        verifier.as_bytes(),
    ))
}

/// 256 random bits, base64url: a state or a nonce.
pub fn random_token() -> String {
    B64URL.encode(crate::leader::auth::random_bytes(32))
}

/// Where the browser is sent to sign in.
pub fn authorize_url(
    provider: &Provider,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
    challenge: &str,
) -> String {
    let query = form(&[
        ("response_type", "code"),
        ("client_id", client_id),
        ("redirect_uri", redirect_uri),
        ("scope", SCOPE),
        ("state", state),
        ("nonce", nonce),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
    ]);
    // An endpoint's own query is kept (RFC 6749 3.1).
    let joiner = if provider.authorization_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    format!("{}{joiner}{query}", provider.authorization_endpoint)
}

/// What the token endpoint gave for a code. The ID token is still unchecked: `verify_id_token` is what makes it
/// believed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tokens {
    pub id_token: String,
    /// Absent when the provider gives none, and then the sign-in cannot be refreshed.
    pub refresh_token: Option<String>,
}

/// Trades the code the browser brought back for tokens.
pub async fn exchange(
    client: &reqwest::Client,
    provider: &Provider,
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
    code: &str,
    verifier: &str,
) -> Result<Tokens, String> {
    let fields = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", verifier),
    ];
    let answer = post_token(client, provider, client_id, client_secret, &fields, "the code")
        .await
        .map_err(|failed| failed.why)?;
    match token_field(&answer, "id_token") {
        None => Err(format!("{} answered with no id_token", provider.token_endpoint)),
        Some(id_token) => Ok(Tokens {
            id_token,
            refresh_token: token_field(&answer, "refresh_token"),
        }),
    }
}

/// What the token endpoint gave for a refresh token. Either may be absent; a refresh token given replaces the one
/// sent, which the provider may no longer take.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refreshed {
    /// Still unchecked, as `Tokens::id_token` is; check it with `Nonce::AbsentOr`.
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
}

/// Why a refresh gave no tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshFailure {
    /// The provider says the grant is gone (`invalid_grant`): the session ended there, the person was disabled, or the
    /// token expired. The sign-in ends.
    Refused(String),
    /// Anything else: the provider could not be reached, failed, or refused this client. Nothing is known of the
    /// person.
    Unavailable(String),
}

impl std::fmt::Display for RefreshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshFailure::Refused(why) | RefreshFailure::Unavailable(why) => f.write_str(why),
        }
    }
}

/// Trades a refresh token for fresh tokens (RFC 6749 6).
pub async fn refresh(
    client: &reqwest::Client,
    provider: &Provider,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<Refreshed, RefreshFailure> {
    let fields = [("grant_type", "refresh_token"), ("refresh_token", refresh_token)];
    let answer = post_token(client, provider, client_id, client_secret, &fields, "the refresh token")
        .await
        .map_err(|failed| match failed.error.as_deref() {
            Some("invalid_grant") => RefreshFailure::Refused(failed.why),
            _ => RefreshFailure::Unavailable(failed.why),
        })?;
    Ok(Refreshed {
        id_token: token_field(&answer, "id_token"),
        refresh_token: token_field(&answer, "refresh_token"),
    })
}

/// Why the token endpoint gave no answer to read tokens from.
struct TokenFailure {
    /// The OAuth error code (RFC 6749 5.2), when the provider answered one.
    error: Option<String>,
    why: String,
}

/// POSTs `fields` to the token endpoint, the client authenticated as the provider wants, and gives the JSON object of
/// a success. `what` names what was sent, for the message when it is refused.
async fn post_token(
    client: &reqwest::Client,
    provider: &Provider,
    client_id: &str,
    client_secret: &str,
    fields: &[(&str, &str)],
    what: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, TokenFailure> {
    let url = &provider.token_endpoint;
    let failed = |why: String| TokenFailure { error: None, why };
    let mut fields = fields.to_vec();
    let mut request = client
        .post(url)
        .header("Accept", "application/json")
        .header("Content-Type", "application/x-www-form-urlencoded");
    match provider.token_auth {
        TokenAuth::Basic => request = request.header("Authorization", basic(client_id, client_secret)),
        TokenAuth::Post => fields.extend([("client_id", client_id), ("client_secret", client_secret)]),
    }
    let response = request
        .body(form(&fields))
        .send()
        .await
        .map_err(|e| failed(format!("could not reach {url}: {e}")))?;
    let status = response.status();
    let body = read(response, MAX_TOKEN_ANSWER)
        .await
        .map_err(|e| failed(format!("{url}: {e}")))?;
    let answer = match serde_json::from_slice(&body) {
        Ok(serde_json::Value::Object(answer)) => Some(answer),
        _ => None,
    };
    let text = |field: &str| {
        answer
            .as_ref()
            .and_then(|a| a.get(field))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
    };
    if !status.is_success() {
        let error = text("error");
        let why = match error {
            "" => said(&body),
            error => format!(
                "{}: {}",
                said(error.as_bytes()),
                said(text("error_description").as_bytes())
            ),
        };
        return Err(TokenFailure {
            error: Some(error.to_string()).filter(|e| !e.is_empty()),
            why: format!("{url} refused {what} ({}): {why}", status.as_u16()),
        });
    }
    answer.ok_or_else(|| failed(format!("{url} answered with no JSON object: {}", said(&body))))
}

/// A token the answer gives, absent when it gives none or an empty one.
fn token_field(answer: &serde_json::Map<String, serde_json::Value>, field: &str) -> Option<String> {
    answer
        .get(field)
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// What an ID token must say to be believed.
#[derive(Clone, Copy, Debug)]
pub struct Expect<'a> {
    pub issuer: &'a str,
    pub client_id: &'a str,
    pub nonce: Nonce<'a>,
    pub now_secs: i64,
}

/// What an ID token's nonce must be, given the nonce sent with the sign-in.
#[derive(Clone, Copy, Debug)]
pub enum Nonce<'a> {
    /// A token the sign-in ended in carries that nonce.
    Is(&'a str),
    /// A refreshed token carries none (OpenID Connect Core 12.2), or repeats the sign-in's, as Keycloak does.
    AbsentOr(&'a str),
}

/// Who a checked ID token says signed in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claims {
    /// The provider's id for the person, unique and never reassigned within the issuer.
    pub subject: String,
    pub name: Option<String>,
    pub preferred_username: Option<String>,
    /// Only an address the provider says it verified.
    pub email: Option<String>,
    pub groups: Vec<String>,
}

impl Claims {
    /// The best the token gives to show a person by: their name, else username, else address, else their id.
    pub fn display_name(&self) -> &str {
        [&self.name, &self.preferred_username, &self.email]
            .into_iter()
            .flatten()
            .find(|v| !v.trim().is_empty())
            .unwrap_or(&self.subject)
    }
}

/// Why an ID token was not believed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Signed by a key the key set held does not have. The provider may have rotated its keys: fetch the set again,
    /// once, and check again.
    UnknownKey,
    Invalid(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::UnknownKey => f.write_str("the ID token is signed by a key the provider does not publish"),
            Refusal::Invalid(why) => write!(f, "the ID token is not valid: {why}"),
        }
    }
}

/// The signature algorithms an ID token may be signed with. `none` and the HMAC family are absent: HMAC would make
/// the client secret a signing key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Alg {
    Rs256,
    Rs384,
    Rs512,
    Ps256,
    Ps384,
    Ps512,
    Es256,
    Es384,
}

impl Alg {
    fn named(name: &str) -> Option<Alg> {
        Some(match name {
            "RS256" => Alg::Rs256,
            "RS384" => Alg::Rs384,
            "RS512" => Alg::Rs512,
            "PS256" => Alg::Ps256,
            "PS384" => Alg::Ps384,
            "PS512" => Alg::Ps512,
            "ES256" => Alg::Es256,
            "ES384" => Alg::Es384,
            _ => return None,
        })
    }

    fn fits(self, key: &PublicKey) -> bool {
        match (self, key) {
            (Alg::Es256, PublicKey::Ec { curve, .. }) => *curve == Curve::P256,
            (Alg::Es384, PublicKey::Ec { curve, .. }) => *curve == Curve::P384,
            (Alg::Es256 | Alg::Es384, PublicKey::Rsa { .. }) => false,
            (_, PublicKey::Rsa { .. }) => true,
            (_, PublicKey::Ec { .. }) => false,
        }
    }

    /// Checks `signature` over `message` with `key`, which `fits` this algorithm.
    fn verify(self, key: &PublicKey, message: &[u8], signature: &[u8]) -> bool {
        match key {
            PublicKey::Rsa { n, e } => {
                let params = match self {
                    Alg::Rs256 => &signature::RSA_PKCS1_2048_8192_SHA256,
                    Alg::Rs384 => &signature::RSA_PKCS1_2048_8192_SHA384,
                    Alg::Rs512 => &signature::RSA_PKCS1_2048_8192_SHA512,
                    Alg::Ps256 => &signature::RSA_PSS_2048_8192_SHA256,
                    Alg::Ps384 => &signature::RSA_PSS_2048_8192_SHA384,
                    Alg::Ps512 => &signature::RSA_PSS_2048_8192_SHA512,
                    Alg::Es256 | Alg::Es384 => return false,
                };
                RsaPublicKeyComponents { n, e }
                    .verify(params, message, signature)
                    .is_ok()
            }
            PublicKey::Ec { point, .. } => {
                // JWS writes an ECDSA signature as r and s side by side, not DER: the FIXED form.
                let algorithm: &'static dyn VerificationAlgorithm = match self {
                    Alg::Es256 => &signature::ECDSA_P256_SHA256_FIXED,
                    Alg::Es384 => &signature::ECDSA_P384_SHA384_FIXED,
                    _ => return false,
                };
                UnparsedPublicKey::new(algorithm, point)
                    .verify(message, signature)
                    .is_ok()
            }
        }
    }
}

/// Checks an ID token as OpenID Connect Core 3.1.3.7 says: signed by a key in `jwks` with an allowed algorithm, then
/// issuer, audience, authorized party, times and nonce as `expect` says. `groups_claim` names the claim that lists the
/// person's groups, a list of strings or one string; an empty name reads none. Keys a token's header points to by URL
/// (the jku, x5u and jwk header parameters) are never followed: only the provider's published set is trusted.
pub fn verify_id_token(token: &str, jwks: &Jwks, expect: &Expect<'_>, groups_claim: &str) -> Result<Claims, Refusal> {
    let invalid = |why: &str| Refusal::Invalid(why.to_string());
    if token.len() > MAX_TOKEN_ANSWER {
        return Err(invalid("it is longer than any token this leader reads"));
    }
    let mut parts = token.split('.');
    let (Some(raw_header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid("it is not three dot-separated parts"));
    };
    let header = object(raw_header).ok_or_else(|| invalid("its header is not a base64url JSON object"))?;
    if header.contains_key("crit") {
        return Err(invalid(
            "its header names critical extensions, which this leader does not understand",
        ));
    }
    let alg_name = header.get("alg").and_then(|v| v.as_str()).unwrap_or_default();
    let alg = Alg::named(alg_name).ok_or_else(|| {
        Refusal::Invalid(format!(
            "it is signed with {:?}, which is not allowed",
            said(alg_name.as_bytes())
        ))
    })?;
    let key = pick_key(jwks, &header, alg)?;
    let signature = B64URL
        .decode(signature)
        .map_err(|_| invalid("its signature is not base64url"))?;
    // What is signed is the header and payload exactly as sent.
    let signed = &token[..raw_header.len() + 1 + payload.len()];
    if !alg.verify(key, signed.as_bytes(), &signature) {
        return Err(invalid("its signature does not verify"));
    }

    let claims = object(payload).ok_or_else(|| invalid("its payload is not a base64url JSON object"))?;
    let text = |field: &str| claims.get(field).and_then(|v| v.as_str());
    if text("iss") != Some(expect.issuer) {
        return Err(invalid("it was issued by another issuer"));
    }
    let audience: Vec<&str> = match claims.get("aud") {
        Some(serde_json::Value::String(one)) => vec![one.as_str()],
        Some(serde_json::Value::Array(many)) => many.iter().filter_map(|v| v.as_str()).collect(),
        _ => return Err(invalid("it names no audience")),
    };
    if !audience.contains(&expect.client_id) {
        return Err(invalid("it was issued for another client"));
    }
    match text("azp") {
        Some(azp) if azp != expect.client_id => return Err(invalid("it was issued to another authorized party")),
        None if audience.len() > 1 => {
            return Err(invalid("it names several audiences and no authorized party"));
        }
        _ => {}
    }
    let time = |field: &str| claims.get(field).and_then(seconds);
    let now = expect.now_secs;
    let exp = time("exp").ok_or_else(|| invalid("it has no expiry"))?;
    if now >= exp.saturating_add(LEEWAY_SECS) {
        return Err(invalid("it has expired"));
    }
    let iat = time("iat").ok_or_else(|| invalid("it says nothing of when it was issued"))?;
    if iat > now.saturating_add(LEEWAY_SECS) {
        return Err(invalid("it was issued in the future; check this host's clock"));
    }
    if claims.contains_key("nbf") {
        let nbf = time("nbf").ok_or_else(|| invalid("its not-before is not a time"))?;
        if nbf > now.saturating_add(LEEWAY_SECS) {
            return Err(invalid("it is not valid yet; check this host's clock"));
        }
    }
    let nonce_holds = match expect.nonce {
        Nonce::Is(nonce) => text("nonce") == Some(nonce),
        Nonce::AbsentOr(nonce) => !claims.contains_key("nonce") || text("nonce") == Some(nonce),
    };
    if !nonce_holds {
        return Err(invalid("its nonce is not this sign-in's"));
    }
    let subject = text("sub").unwrap_or_default();
    if subject.is_empty() || subject.len() > MAX_SUBJECT {
        return Err(invalid("its subject is empty or longer than 255 characters"));
    }

    let groups = match (groups_claim, claims.get(groups_claim)) {
        ("", _) => Vec::new(),
        (_, Some(serde_json::Value::Array(many))) => {
            many.iter().filter_map(|v| v.as_str()).map(str::to_string).collect()
        }
        (_, Some(serde_json::Value::String(one))) => vec![one.clone()],
        _ => Vec::new(),
    };
    Ok(Claims {
        subject: subject.to_string(),
        name: text("name").map(str::to_string),
        preferred_username: text("preferred_username").map(str::to_string),
        // An address the provider has not verified is one the person typed, so it names nobody.
        email: text("email")
            .filter(|_| claims.get("email_verified").and_then(|v| v.as_bool()) == Some(true))
            .map(str::to_string),
        groups,
    })
}

/// The key the token's header names by `kid`, or with no `kid` the only key that fits `alg`. A token whose key is not
/// in the set is `UnknownKey`, so the caller can fetch the set again.
fn pick_key<'a>(
    jwks: &'a Jwks,
    header: &serde_json::Map<String, serde_json::Value>,
    alg: Alg,
) -> Result<&'a PublicKey, Refusal> {
    let pinned_elsewhere = |k: &Jwk| k.alg.as_deref().is_some_and(|a| Alg::named(a) != Some(alg));
    match header.get("kid") {
        Some(kid) => {
            let kid = kid
                .as_str()
                .ok_or_else(|| Refusal::Invalid("its key id is not a string".to_string()))?;
            let named: Vec<&Jwk> = jwks.keys.iter().filter(|k| k.kid.as_deref() == Some(kid)).collect();
            if named.is_empty() {
                return Err(Refusal::UnknownKey);
            }
            named
                .into_iter()
                .find(|k| alg.fits(&k.key) && !pinned_elsewhere(k))
                .map(|k| &k.key)
                .ok_or_else(|| Refusal::Invalid("its key is not one for its algorithm".to_string()))
        }
        None => {
            let mut fitting = jwks.keys.iter().filter(|k| alg.fits(&k.key) && !pinned_elsewhere(k));
            match (fitting.next(), fitting.next()) {
                (Some(only), None) => Ok(&only.key),
                (None, _) => Err(Refusal::UnknownKey),
                (Some(_), Some(_)) => Err(Refusal::Invalid(
                    "it names no key id and the provider has several keys".to_string(),
                )),
            }
        }
    }
}

/// A base64url JSON object, as a JWT's header and payload are.
fn object(part: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    match serde_json::from_slice(&B64URL.decode(part).ok()?).ok()? {
        serde_json::Value::Object(map) => Some(map),
        _ => None,
    }
}

/// A NumericDate: seconds since the epoch, which may have a fraction.
fn seconds(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_f64().filter(|f| f.is_finite()).map(|f| f.floor() as i64))
}

/// GETs `url`, refusing any answer but a success and any body longer than `limit`.
async fn get(client: &reqwest::Client, url: &str, limit: usize) -> Result<Vec<u8>, String> {
    let response = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("could not reach {url}: {e}"))?;
    let status = response.status();
    let body = read(response, limit).await.map_err(|e| format!("{url}: {e}"))?;
    if !status.is_success() {
        // The body goes to the log only: a failed save answers whoever made it, and the URL may name a host they
        // could not otherwise read.
        tracing::warn!(target: "leader.oidc_provider", url = %url, status = status.as_u16(), body = %said(&body), "an OIDC provider answered with an error");
        return Err(format!("{url} answered {}", status.as_u16()));
    }
    Ok(body)
}

/// `fields` as `application/x-www-form-urlencoded`.
fn form(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(name, value)| format!("{}={}", percent_encode(name), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use aws_lc_rs::rand::SystemRandom;
    use aws_lc_rs::rsa::KeySize;
    use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair, RSA_PKCS1_SHA256, RsaKeyPair};
    use serde_json::json;

    use super::*;

    const ISSUER: &str = "https://idp.example.com/realms/acme";
    const CLIENT: &str = "smeltr";
    const NONCE: &str = "n-0S6_WzA2Mj";
    const NOW: i64 = 1_800_000_000;

    fn expect() -> Expect<'static> {
        Expect {
            issuer: ISSUER,
            client_id: CLIENT,
            nonce: Nonce::Is(NONCE),
            now_secs: NOW,
        }
    }

    fn ec_key() -> &'static EcdsaKeyPair {
        static KEY: OnceLock<EcdsaKeyPair> = OnceLock::new();
        KEY.get_or_init(|| EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING).unwrap())
    }

    fn rsa_key() -> &'static RsaKeyPair {
        static KEY: OnceLock<RsaKeyPair> = OnceLock::new();
        KEY.get_or_init(|| RsaKeyPair::generate(KeySize::Rsa2048).unwrap())
    }

    fn ec_jwk(kid: &str) -> serde_json::Value {
        let point = ec_key().public_key().as_ref();
        json!({"kty": "EC", "crv": "P-256", "kid": kid, "use": "sig",
               "x": B64URL.encode(&point[1..33]), "y": B64URL.encode(&point[33..65])})
    }

    fn rsa_jwk(kid: &str) -> serde_json::Value {
        let public = rsa_key().public_key();
        json!({"kty": "RSA", "kid": kid, "alg": "RS256",
               "n": B64URL.encode(public.modulus().big_endian_without_leading_zero()),
               "e": B64URL.encode(public.exponent().big_endian_without_leading_zero())})
    }

    fn jwks() -> Jwks {
        let set = json!({"keys": [
            ec_jwk("ec-1"),
            rsa_jwk("rsa-1"),
            {"kty": "RSA", "use": "enc", "kid": "enc-1", "n": "AQAB", "e": "AQAB"},
            {"kty": "OKP", "crv": "Ed25519", "kid": "ed-1", "x": "AAAA"},
        ]});
        Jwks::parse(set.to_string().as_bytes()).unwrap()
    }

    fn claims() -> serde_json::Value {
        json!({
            "iss": ISSUER, "aud": CLIENT, "sub": "248289761001", "nonce": NONCE,
            "iat": NOW - 10, "exp": NOW + 300,
            "name": "Jane Doe", "email": "jane@example.com", "email_verified": true,
            "groups": ["admins", "ops", 7],
        })
    }

    fn part(value: &serde_json::Value) -> String {
        B64URL.encode(value.to_string())
    }

    /// A token with `header` and `claims`, signed by the test key `header.alg` names.
    fn token(header: serde_json::Value, claims: &serde_json::Value) -> String {
        let signed = format!("{}.{}", part(&header), part(claims));
        let signature = match header["alg"].as_str().unwrap() {
            "ES256" => ec_key()
                .sign(&SystemRandom::new(), signed.as_bytes())
                .unwrap()
                .as_ref()
                .to_vec(),
            "RS256" => {
                let mut signature = vec![0; rsa_key().public_modulus_len()];
                rsa_key()
                    .sign(
                        &RSA_PKCS1_SHA256,
                        &SystemRandom::new(),
                        signed.as_bytes(),
                        &mut signature,
                    )
                    .unwrap();
                signature
            }
            _ => b"not a signature".to_vec(),
        };
        format!("{signed}.{}", B64URL.encode(signature))
    }

    fn es256(claims: &serde_json::Value) -> String {
        token(json!({"alg": "ES256", "kid": "ec-1", "typ": "JWT"}), claims)
    }

    fn refusal(claims: &serde_json::Value) -> String {
        match verify_id_token(&es256(claims), &jwks(), &expect(), "groups") {
            Err(Refusal::Invalid(why)) => why,
            other => panic!("not refused as invalid: {other:?}"),
        }
    }

    #[test]
    fn a_good_es256_token_is_believed() {
        let claims = verify_id_token(&es256(&claims()), &jwks(), &expect(), "groups").unwrap();
        assert_eq!(claims.subject, "248289761001");
        assert_eq!(claims.display_name(), "Jane Doe");
        assert_eq!(claims.email.as_deref(), Some("jane@example.com"));
        assert_eq!(claims.groups, ["admins", "ops"]);
    }

    #[test]
    fn a_good_rs256_token_is_believed() {
        let token = token(json!({"alg": "RS256", "kid": "rsa-1"}), &claims());
        assert_eq!(
            verify_id_token(&token, &jwks(), &expect(), "groups").unwrap().subject,
            "248289761001"
        );
    }

    #[test]
    fn with_no_kid_the_only_fitting_key_is_used() {
        let token = token(json!({"alg": "RS256"}), &claims());
        assert!(verify_id_token(&token, &jwks(), &expect(), "groups").is_ok());
    }

    #[test]
    fn a_wrong_signature_is_refused() {
        let good = es256(&claims());
        let mut other = claims();
        other["sub"] = "someone-else".into();
        let forged = es256(&other);
        // The first token's signature on the second's claims.
        let (signed, _) = forged.rsplit_once('.').unwrap();
        let (_, signature) = good.rsplit_once('.').unwrap();
        let spliced = format!("{signed}.{signature}");
        assert_eq!(
            verify_id_token(&spliced, &jwks(), &expect(), "groups"),
            Err(Refusal::Invalid("its signature does not verify".to_string()))
        );
        // An RS256 header on a key that is EC.
        let token = token(json!({"alg": "RS256", "kid": "ec-1"}), &claims());
        assert!(matches!(
            verify_id_token(&token, &jwks(), &expect(), "groups"),
            Err(Refusal::Invalid(_))
        ));
    }

    #[test]
    fn none_and_hmac_are_refused() {
        for alg in ["none", "HS256", "EdDSA"] {
            let token = token(json!({"alg": alg, "kid": "ec-1"}), &claims());
            let refused = verify_id_token(&token, &jwks(), &expect(), "groups").unwrap_err();
            assert!(refused.to_string().contains("not allowed"), "{alg}: {refused}");
        }
        let unsigned = format!("{}.{}.", part(&json!({"alg": "none"})), part(&claims()));
        assert!(verify_id_token(&unsigned, &jwks(), &expect(), "groups").is_err());
    }

    #[test]
    fn a_critical_header_is_refused() {
        let token = token(
            json!({"alg": "ES256", "kid": "ec-1", "crit": ["b64"], "b64": false}),
            &claims(),
        );
        assert!(matches!(
            verify_id_token(&token, &jwks(), &expect(), "groups"),
            Err(Refusal::Invalid(_))
        ));
    }

    #[test]
    fn an_unknown_kid_asks_for_the_keys_again() {
        let token = token(json!({"alg": "ES256", "kid": "rotated"}), &claims());
        assert_eq!(
            verify_id_token(&token, &jwks(), &expect(), "groups"),
            Err(Refusal::UnknownKey)
        );
    }

    #[test]
    fn issuer_audience_and_party_must_be_ours() {
        let mut c = claims();
        c["iss"] = "https://idp.example.com/realms/other".into();
        assert!(refusal(&c).contains("another issuer"));

        let mut c = claims();
        c["aud"] = "another-app".into();
        assert!(refusal(&c).contains("another client"));

        let mut c = claims();
        c["aud"] = json!(["another-app", CLIENT]);
        assert!(refusal(&c).contains("no authorized party"));
        c["azp"] = "another-app".into();
        assert!(refusal(&c).contains("another authorized party"));
        c["azp"] = CLIENT.into();
        assert!(verify_id_token(&es256(&c), &jwks(), &expect(), "groups").is_ok());
    }

    #[test]
    fn times_are_checked_with_a_minute_of_leeway() {
        let mut c = claims();
        c["exp"] = (NOW - 61).into();
        assert!(refusal(&c).contains("expired"));
        c["exp"] = (NOW - 30).into();
        assert!(verify_id_token(&es256(&c), &jwks(), &expect(), "groups").is_ok());

        let mut c = claims();
        c["iat"] = (NOW + 120).into();
        assert!(refusal(&c).contains("future"));

        let mut c = claims();
        c["nbf"] = (NOW + 120).into();
        assert!(refusal(&c).contains("not valid yet"));
        c["nbf"] = (NOW - 5).into();
        assert!(verify_id_token(&es256(&c), &jwks(), &expect(), "groups").is_ok());

        let mut c = claims();
        c.as_object_mut().unwrap().remove("exp");
        assert!(refusal(&c).contains("no expiry"));
    }

    #[test]
    fn a_refreshed_token_carries_no_nonce_or_the_sign_ins() {
        let refreshed = Expect {
            nonce: Nonce::AbsentOr(NONCE),
            ..expect()
        };
        let check = |c: &serde_json::Value| verify_id_token(&es256(c), &jwks(), &refreshed, "groups");
        let mut c = claims();
        assert!(check(&c).is_ok());
        c.as_object_mut().unwrap().remove("nonce");
        assert!(check(&c).is_ok());
        c["nonce"] = "another".into();
        assert_eq!(
            check(&c),
            Err(Refusal::Invalid("its nonce is not this sign-in's".to_string()))
        );
    }

    #[test]
    fn nonce_and_subject_are_checked() {
        let mut c = claims();
        c["nonce"] = "another".into();
        assert!(refusal(&c).contains("nonce"));
        c.as_object_mut().unwrap().remove("nonce");
        assert!(refusal(&c).contains("nonce"));

        let mut c = claims();
        c["sub"] = "".into();
        assert!(refusal(&c).contains("subject"));
        c["sub"] = "x".repeat(256).into();
        assert!(refusal(&c).contains("subject"));
    }

    #[test]
    fn groups_are_a_list_a_string_or_none() {
        let groups = |value: Option<serde_json::Value>, claim: &str| {
            let mut c = claims();
            match value {
                Some(v) => c["roles"] = v,
                None => {
                    c.as_object_mut().unwrap().remove("roles");
                }
            }
            verify_id_token(&es256(&c), &jwks(), &expect(), claim).unwrap().groups
        };
        assert_eq!(groups(Some(json!(["a", 1, "b", null])), "roles"), ["a", "b"]);
        assert_eq!(groups(Some(json!("only")), "roles"), ["only"]);
        assert!(groups(None, "roles").is_empty());
        assert!(groups(Some(json!({"a": 1})), "roles").is_empty());
        assert!(groups(Some(json!(["a"])), "").is_empty());
    }

    #[test]
    fn display_name_falls_back_in_order() {
        let mut c = Claims {
            subject: "sub-1".into(),
            name: Some(" ".into()),
            preferred_username: Some("jdoe".into()),
            email: Some("j@example.com".into()),
            groups: vec![],
        };
        assert_eq!(c.display_name(), "jdoe");
        c.preferred_username = None;
        assert_eq!(c.display_name(), "j@example.com");
        c.email = None;
        assert_eq!(c.display_name(), "sub-1");
    }

    #[test]
    fn a_malformed_token_is_refused() {
        for bad in ["", "a.b", "a.b.c.d", "!!.!!.!!", &"a".repeat(MAX_TOKEN_ANSWER + 1)] {
            assert!(matches!(
                verify_id_token(bad, &jwks(), &expect(), "groups"),
                Err(Refusal::Invalid(_))
            ));
        }
    }

    #[test]
    fn key_sets_keep_signing_keys_alone() {
        assert_eq!(jwks().keys.len(), 2);
        let padded = json!({"keys": [{"kty": "RSA", "n": "AACr", "e": "AQAB"}]});
        match &Jwks::parse(padded.to_string().as_bytes()).unwrap().keys[0].key {
            PublicKey::Rsa { n, .. } => assert_eq!(n, &[0xab]),
            other => panic!("{other:?}"),
        }
        assert!(Jwks::parse(br#"{"keys": [{"kty": "oct", "k": "c2VjcmV0"}]}"#).is_err());
        assert!(Jwks::parse(br#"{"keys": [{"kty": "EC", "crv": "P-256", "x": "AA", "y": "AA"}]}"#).is_err());
        assert!(Jwks::parse(b"[]").is_err());
    }

    #[test]
    fn issuers_are_https_or_loopback_http() {
        for good in [
            "https://idp.example.com",
            "https://idp.example.com/",
            "https://idp.example.com/realms/acme",
            "http://localhost:8080/realms/acme",
            "http://127.0.0.1:9000",
            "http://[::1]:9000",
        ] {
            assert_eq!(check_issuer(good), Ok(()), "{good}");
        }
        for bad in [
            "",
            "idp.example.com",
            "http://idp.example.com",
            "ftp://idp.example.com",
            "https://user:pw@idp.example.com",
            "https://idp.example.com?x=1",
            "https://idp.example.com#top",
            "https://idp.example.com/a b",
            "https://idp.example.com\\@evil.com",
            "https:///realms",
        ] {
            assert!(check_issuer(bad).is_err(), "{bad}");
        }
        assert!(check_url("https://idp.example.com/auth?tenant=1", true).is_ok());
    }

    fn provider(token_auth: TokenAuth) -> Provider {
        Provider {
            issuer: ISSUER.into(),
            authorization_endpoint: format!("{ISSUER}/auth"),
            token_endpoint: format!("{ISSUER}/token"),
            jwks_uri: format!("{ISSUER}/certs"),
            token_auth,
        }
    }

    #[test]
    fn the_authorize_url_is_encoded() {
        let url = authorize_url(
            &provider(TokenAuth::Basic),
            "my app",
            "https://smeltr.example.com/oidc/callback?x=1&y=2",
            "st/ate+",
            "n0nce",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        );
        assert_eq!(
            url,
            "https://idp.example.com/realms/acme/auth?response_type=code&client_id=my%20app\
             &redirect_uri=https%3A%2F%2Fsmeltr.example.com%2Foidc%2Fcallback%3Fx%3D1%26y%3D2\
             &scope=openid%20profile%20email&state=st%2Fate%2B&nonce=n0nce\
             &code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&code_challenge_method=S256"
        );
        let mut with_query = provider(TokenAuth::Basic);
        with_query.authorization_endpoint = format!("{ISSUER}/auth?tenant=1");
        assert!(authorize_url(&with_query, "a", "b", "c", "d", "e").starts_with(&format!("{ISSUER}/auth?tenant=1&")));
    }

    #[test]
    fn pkce_is_s256_of_the_verifier() {
        // RFC 7636 appendix B.
        assert_eq!(
            s256("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        let pair = pkce();
        assert_eq!(pair.verifier.len(), 43);
        assert_eq!(pair.challenge, s256(&pair.verifier));
        assert_ne!(random_token(), random_token());
    }

    #[test]
    fn basic_auth_form_encodes_id_and_secret() {
        // RFC 6749 2.3.1: "my client" and "p@ss:w+rd" become "my%20client:p%40ss%3Aw%2Brd".
        assert_eq!(
            basic("my client", "p@ss:w+rd"),
            "Basic bXklMjBjbGllbnQ6cCU0MHNzJTNBdyUyQnJk"
        );
    }

    /// An HTTP server on loopback answering `app`, and its base URL.
    async fn serve(app: impl FnOnce(String) -> axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = app(base.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    fn document(base: &str) -> serde_json::Value {
        json!({
            "issuer": base,
            "authorization_endpoint": format!("{base}/auth"),
            "token_endpoint": format!("{base}/token"),
            "jwks_uri": format!("{base}/certs"),
            "response_types_supported": ["code", "id_token"],
            "code_challenge_methods_supported": ["plain", "S256"],
            "token_endpoint_auth_methods_supported": ["client_secret_post", "private_key_jwt"],
            "id_token_signing_alg_values_supported": ["RS256"],
        })
    }

    async fn discovered(edit: fn(&mut serde_json::Value)) -> Result<Provider, String> {
        let base = serve(|base| {
            let mut doc = document(&base);
            edit(&mut doc);
            axum::Router::new()
                .route(
                    "/.well-known/openid-configuration",
                    axum::routing::get(move || async move { axum::Json(doc) }),
                )
                .route(
                    "/certs",
                    axum::routing::get(|| async { axum::Json(json!({"keys": [ec_jwk("ec-1")]})) }),
                )
        })
        .await;
        let client = client(&base).unwrap();
        let found = discover(&client, &format!("{base}/")).await;
        // The same document under the issuer written without its trailing slash.
        let provider = discover(&client, &base).await?;
        assert!(found.unwrap_err().contains("names the issuer"));
        assert_eq!(fetch_jwks(&client, &provider.jwks_uri).await?.keys.len(), 1);
        Ok(provider)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn discovery_reads_the_provider() {
        let provider = discovered(|_| {}).await.unwrap();
        assert_eq!(provider.token_auth, TokenAuth::Post);
        assert!(provider.token_endpoint.ends_with("/token"));

        assert_eq!(
            discovered(|d| {
                d.as_object_mut()
                    .unwrap()
                    .remove("token_endpoint_auth_methods_supported");
            })
            .await
            .unwrap()
            .token_auth,
            TokenAuth::Basic
        );
        assert!(
            discovered(|d| d["code_challenge_methods_supported"] = json!(["plain"]))
                .await
                .is_err()
        );
        assert!(
            discovered(|d| d["response_types_supported"] = json!(["id_token"]))
                .await
                .is_err()
        );
        assert!(
            discovered(|d| d["token_endpoint"] = "http://idp.example.com/token".into())
                .await
                .is_err()
        );
        assert!(
            discovered(|d| d["id_token_signing_alg_values_supported"] = json!(["HS256"]))
                .await
                .is_err()
        );
        assert!(
            discovered(|d| d["token_endpoint_auth_methods_supported"] = json!(["private_key_jwt"]))
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_long_answer_is_cut_off() {
        let base = serve(|_| {
            axum::Router::new().route(
                "/.well-known/openid-configuration",
                axum::routing::get(|| async { "x".repeat(MAX_DISCOVERY + 1) }),
            )
        })
        .await;
        let refused = discover(&client(&base).unwrap(), &base).await.unwrap_err();
        assert!(refused.contains("longer than 64 KiB"), "{refused}");
    }

    /// A token endpoint that answers with an "ID token" telling what it was sent, and a refresh token. What it is
    /// sent can ask for a failure instead: `code=bad`, or a refresh token `gone`, `failing` or `stranger`.
    async fn echo_token_endpoint() -> String {
        serve(|_| {
            axum::Router::new().route(
                "/token",
                axum::routing::post(|headers: axum::http::HeaderMap, body: String| async move {
                    use axum::http::StatusCode;
                    use axum::response::IntoResponse;
                    let auth = headers
                        .get("authorization")
                        .map(|v| v.to_str().unwrap().to_string())
                        .unwrap_or_default();
                    let error = |status, error| {
                        (
                            status,
                            axum::Json(json!({"error": error, "error_description": "code\nused"})),
                        )
                    };
                    if body.contains("code=bad") || body.contains("refresh_token=gone") {
                        return error(StatusCode::BAD_REQUEST, "invalid_grant").into_response();
                    }
                    if body.contains("refresh_token=stranger") {
                        return error(StatusCode::UNAUTHORIZED, "invalid_client").into_response();
                    }
                    if body.contains("refresh_token=failing") {
                        return (StatusCode::BAD_GATEWAY, "upstream down").into_response();
                    }
                    if body.contains("refresh_token=quiet") {
                        return axum::Json(json!({"access_token": "a", "token_type": "Bearer"})).into_response();
                    }
                    axum::Json(json!({"id_token": format!("{auth}|{body}"), "refresh_token": "r-next"})).into_response()
                }),
            )
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_code_is_exchanged_with_the_secret_where_the_provider_wants_it() {
        let base = echo_token_endpoint().await;
        let client = client(&base).unwrap();
        let mut provider = provider(TokenAuth::Basic);
        provider.token_endpoint = format!("{base}/token");
        let redirect = "https://smeltr.example.com/cb";

        let sent = exchange(
            &client,
            &provider,
            "my client",
            "p@ss:w+rd",
            redirect,
            "c0de",
            "v3rifier",
        )
        .await
        .unwrap();
        assert_eq!(
            sent.id_token,
            "Basic bXklMjBjbGllbnQ6cCU0MHNzJTNBdyUyQnJk|grant_type=authorization_code&code=c0de\
             &redirect_uri=https%3A%2F%2Fsmeltr.example.com%2Fcb&code_verifier=v3rifier"
        );
        assert_eq!(sent.refresh_token.as_deref(), Some("r-next"));

        provider.token_auth = TokenAuth::Post;
        let sent = exchange(&client, &provider, "id", "s&cret", redirect, "c0de", "v")
            .await
            .unwrap()
            .id_token;
        assert!(sent.starts_with('|'));
        assert!(sent.ends_with("&client_id=id&client_secret=s%26cret"));

        let refused = exchange(&client, &provider, "id", "s", redirect, "bad", "v")
            .await
            .unwrap_err();
        assert!(refused.contains("(400): invalid_grant: code used"), "{refused}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refresh_sends_the_grant_with_the_secret_where_the_provider_wants_it() {
        let base = echo_token_endpoint().await;
        let client = client(&base).unwrap();
        let mut provider = provider(TokenAuth::Basic);
        provider.token_endpoint = format!("{base}/token");

        let fresh = refresh(&client, &provider, "my client", "p@ss:w+rd", "r/1")
            .await
            .unwrap();
        assert_eq!(
            fresh.id_token.as_deref(),
            Some("Basic bXklMjBjbGllbnQ6cCU0MHNzJTNBdyUyQnJk|grant_type=refresh_token&refresh_token=r%2F1")
        );
        assert_eq!(fresh.refresh_token.as_deref(), Some("r-next"));

        provider.token_auth = TokenAuth::Post;
        let fresh = refresh(&client, &provider, "id", "s&cret", "r1").await.unwrap();
        assert_eq!(
            fresh.id_token.as_deref(),
            Some("|grant_type=refresh_token&refresh_token=r1&client_id=id&client_secret=s%26cret")
        );

        let quiet = refresh(&client, &provider, "id", "s", "quiet").await.unwrap();
        assert_eq!(
            quiet,
            Refreshed {
                id_token: None,
                refresh_token: None
            }
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn only_invalid_grant_refuses_a_refresh() {
        let base = echo_token_endpoint().await;
        let client = client(&base).unwrap();
        let mut provider = provider(TokenAuth::Basic);
        provider.token_endpoint = format!("{base}/token");
        let failure = |token: &'static str| {
            let (client, provider) = (client.clone(), provider.clone());
            async move { refresh(&client, &provider, "id", "s", token).await.unwrap_err() }
        };

        match failure("gone").await {
            RefreshFailure::Refused(why) => assert!(why.contains("(400): invalid_grant"), "{why}"),
            other => panic!("not refused: {other:?}"),
        }
        for token in ["failing", "stranger"] {
            assert!(
                matches!(failure(token).await, RefreshFailure::Unavailable(_)),
                "{token}"
            );
        }
        provider.token_endpoint = "http://127.0.0.1:1/token".into();
        assert!(matches!(
            refresh(&client, &provider, "id", "s", "r1").await,
            Err(RefreshFailure::Unavailable(_))
        ));
    }
}
