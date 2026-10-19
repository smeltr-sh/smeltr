//! Signing in as an app with OAuth 2.0 client credentials and a client secret, for a token sent on every call until
//! shortly before it expires.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant C as client
//!   participant T as token endpoint
//!   participant S as service
//!   C->>T: client_credentials, client id and secret
//!   T-->>C: token, when it expires
//!   C->>S: Authorization: Bearer token
//!   S-->>C: refused: the token is forgotten, and the next call signs in again
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::Value;

use crate::io::IoError;
use crate::io::http::percent_encode;

/// How long before it expires a token is replaced, so one never expires on the way.
pub(crate) const RENEW_BEFORE: Duration = Duration::from_secs(60);

/// The most a token endpoint's answer may hold.
const MAX_TOKEN_ANSWER: u64 = 64 * 1024;

/// How the client secret is sent to the token endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenAuth {
    /// `client_secret_basic`: in the Authorization header.
    Basic,
    /// `client_secret_post`: in the form.
    Post,
}

/// `client_secret_basic`: id and secret are each form-encoded before they are joined (RFC 6749 2.3.1), so a colon in
/// either cannot move where one ends.
pub fn basic(client_id: &str, client_secret: &str) -> String {
    let pair = format!("{}:{}", percent_encode(client_id), percent_encode(client_secret));
    format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(pair))
}

/// An app's client credentials, and the token they were last traded for. Every sign-in is blocking, on the caller's
/// agent, and bounded by one timeout.
pub struct ClientCredentials {
    token_url: String,
    client_id: String,
    client_secret: String,
    /// Where the assertion that proves the client in place of its secret is read from, when one does.
    assertion_file: Option<PathBuf>,
    scope: Option<String>,
    auth: TokenAuth,
    agent: ureq::Agent,
    timeout: Duration,
    token: Option<Token>,
}

/// A token a token endpoint gave, signed in for by a secret here or by a key (`io::oauth`).
pub(crate) struct Token {
    pub value: String,
    /// `None` when the endpoint named no lifetime: kept until a call is refused.
    pub renew_at: Option<Instant>,
}

impl Token {
    /// Whether it is about to expire, so the next call signs in first.
    pub fn due(&self) -> bool {
        self.renew_at.is_some_and(|at| Instant::now() >= at)
    }
}

/// The longest one sign-in may take. Fixed rather than taken from what asked for the token: a token outlives the
/// request that asked for it, and a Kafka client's sign-in runs on the thread polling it, which reads nothing while
/// it waits.
pub const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(10);

/// The `Authorization` value a sign-in last sent, and whether it was new then. A new token refused is a refusal to
/// show; one used before may have been revoked or expired early, and is worth replacing.
pub(crate) struct LastSent {
    bearer: Option<String>,
    fresh: bool,
}

impl Default for LastSent {
    fn default() -> LastSent {
        LastSent {
            bearer: None,
            fresh: true,
        }
    }
}

impl LastSent {
    /// Takes `bearer` as sent: new, unless it is the one last sent.
    pub fn sent(&mut self, bearer: &str) {
        if self.bearer.as_deref() != Some(bearer) {
            self.fresh = true;
            self.bearer = Some(bearer.to_string());
        }
    }

    /// The service let the last sent through.
    pub fn used(&mut self) {
        self.fresh = false;
    }

    /// Whether a refusal of the last sent is worth signing in again for.
    pub fn worth_replacing(&self) -> bool {
        !self.fresh
    }
}

/// A token endpoint's answer: its status, its body, and the body's fields when it is JSON.
pub(crate) fn read_answer(
    url: &str,
    response: &mut ureq::http::Response<ureq::Body>,
) -> Result<(u16, Vec<u8>, Value), IoError> {
    let status = response.status().as_u16();
    let body = response
        .body_mut()
        .with_config()
        .limit(MAX_TOKEN_ANSWER)
        .read_to_vec()
        .map_err(|e| IoError::transient(format!("could not read {url}'s answer: {e}")))?;
    let answer = serde_json::from_slice(&body).unwrap_or_default();
    Ok((status, body, answer))
}

/// Why a token endpoint refused: its `error` and `error_description`, else the start of what it said.
pub(crate) fn why_refused(body: &[u8], answer: &Value) -> String {
    let text = |field: &str| answer.get(field).and_then(|v| v.as_str()).unwrap_or_default();
    match text("error") {
        "" => String::from_utf8_lossy(&body[..body.len().min(200)]).trim().to_string(),
        error => format!("{error}: {}", text("error_description")),
    }
}

/// The token a token endpoint at `url` granted in `answer`, renewed shortly before the lifetime it names.
pub(crate) fn granted(url: &str, answer: &Value) -> Result<Token, IoError> {
    let value = answer.get("access_token").and_then(|v| v.as_str()).unwrap_or_default();
    if value.is_empty() {
        return Err(IoError::transient(format!(
            "{url} answered the sign-in with no access_token"
        )));
    }
    // Azure's metadata service writes the lifetime as a string.
    let renew_at = answer
        .get("expires_in")
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.trim().parse().ok()))
        .and_then(|life| Instant::now().checked_add(Duration::from_secs(life).saturating_sub(RENEW_BEFORE)));
    Ok(Token {
        value: value.to_string(),
        renew_at,
    })
}

impl ClientCredentials {
    /// Signs in at `token_url`, sending the secret in the form. `scope` is sent only when given. `timeout` bounds each
    /// sign-in. Nothing is asked until the first [`ClientCredentials::bearer`].
    pub fn new(
        token_url: String,
        client_id: String,
        client_secret: String,
        scope: Option<String>,
        agent: ureq::Agent,
        timeout: Duration,
    ) -> ClientCredentials {
        ClientCredentials {
            token_url,
            client_id,
            client_secret,
            assertion_file: None,
            scope,
            auth: TokenAuth::Post,
            agent,
            timeout,
            token: None,
        }
    }

    /// Sends the secret as `auth` says.
    pub fn sending_secret(mut self, auth: TokenAuth) -> ClientCredentials {
        self.auth = auth;
        self
    }

    /// Proves the client with the assertion in `path` in place of its secret (RFC 7523 2.2): a workload's token from
    /// its platform. The file is read at every sign-in, because the platform replaces it before it expires.
    pub fn proven_by_assertion_file(mut self, path: PathBuf) -> ClientCredentials {
        self.assertion_file = Some(path);
        self
    }

    /// Signs in at `token_url` from now on. The token held is kept until it is due for renewal or refused.
    pub fn sign_in_at(&mut self, token_url: String) {
        self.token_url = token_url;
    }

    /// The `Authorization` header's value, `Bearer {token}`, signing in first when no token is held or the one held is
    /// about to expire. An error never holds the secret.
    pub fn bearer(&mut self) -> Result<String, IoError> {
        Ok(format!("Bearer {}", self.token()?.value))
    }

    /// The token held, signing in first when none is or the one held is about to expire.
    pub(crate) fn token(&mut self) -> Result<&Token, IoError> {
        if self.token.as_ref().is_none_or(Token::due) {
            self.token = Some(self.sign_in()?);
        }
        Ok(self.token.as_ref().expect("signed in above"))
    }

    /// Drops the token held, for one a call was refused with: the next [`ClientCredentials::bearer`] signs in again.
    pub fn forget(&mut self) {
        self.token = None;
    }

    /// Trades the client's credentials for a token.
    fn sign_in(&self) -> Result<Token, IoError> {
        let url = &self.token_url;
        let assertion = match &self.assertion_file {
            Some(path) => Some(read_assertion(path)?),
            None => None,
        };
        let mut form = vec![("grant_type", "client_credentials")];
        if let Some(assertion) = &assertion {
            form.extend([
                ("client_id", self.client_id.as_str()),
                ("client_assertion_type", crate::io::oauth::JWT_BEARER),
                ("client_assertion", assertion.as_str()),
            ]);
        } else if self.auth == TokenAuth::Post {
            form.extend([
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
            ]);
        }
        if let Some(scope) = &self.scope {
            form.push(("scope", scope.as_str()));
        }
        let mut request = self
            .agent
            .post(url)
            .config()
            .timeout_global(Some(self.timeout))
            .http_status_as_error(false)
            .max_redirects(0)
            .build()
            .header("Accept", "application/json");
        if assertion.is_none() && self.auth == TokenAuth::Basic {
            request = request.header("Authorization", basic(&self.client_id, &self.client_secret));
        }
        let mut response = request
            .send_form(form)
            .map_err(|e| IoError::transient(format!("signing in at {url} failed: {e}")))?;
        let (status, body, answer) = read_answer(url, &mut response)?;
        if !(200..300).contains(&status) {
            return Err(IoError::transient(format!(
                "{url} refused the sign-in ({status}): {}",
                why_refused(&body, &answer)
            )));
        }
        granted(url, &answer)
    }
}

/// The assertion in `path`. An error names the file, never what it holds.
fn read_assertion(path: &std::path::Path) -> Result<String, IoError> {
    let assertion = std::fs::read_to_string(path)
        .map_err(|e| IoError::transient(format!("could not read the assertion in {}: {e}", path.display())))?;
    let assertion = assertion.trim();
    if assertion.is_empty() {
        return Err(IoError::transient(format!("{} holds no assertion", path.display())));
    }
    Ok(assertion.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;

    /// A token endpoint on 127.0.0.1 that answers each connection with the next of `answers`, in order, and keeps the
    /// form each was asked with.
    pub(crate) fn serve(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
        serve_keeping("/token", answers, |asked| asked.body)
    }

    /// As [`serve`], each form kept after its `Authorization` and a `|`.
    fn serve_seeing(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
        serve_keeping("/token", answers, |asked| {
            format!("{}|{}", asked.header("authorization").unwrap_or_default(), asked.body)
        })
    }

    /// One request a test server read.
    pub(crate) struct Asked {
        /// The request line's target, such as `/token?a=b`.
        pub target: String,
        /// Each header, its name lowercased.
        pub headers: Vec<(String, String)>,
        pub body: String,
    }

    impl Asked {
        pub fn header(&self, name: &str) -> Option<&str> {
            self.headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
        }
    }

    /// A server on 127.0.0.1 at `path` that answers each connection with the next of `answers`, in order, as JSON,
    /// and keeps what `keep` takes of each request.
    pub(crate) fn serve_keeping<T: Send + 'static>(
        path: &str,
        answers: Vec<(u16, String)>,
        keep: fn(Asked) -> T,
    ) -> (String, Arc<Mutex<Vec<T>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}{path}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in answers {
                let (conn, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(conn);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let mut headers = Vec::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    let Some((name, value)) = line.trim_end().split_once(':') else {
                        break;
                    };
                    headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
                }
                let length =
                    (headers.iter().find(|(n, _)| n == "content-length")).map_or(0, |(_, v)| v.parse().unwrap());
                let mut form = vec![0; length];
                reader.read_exact(&mut form).unwrap();
                let asked = Asked {
                    target: request.split(' ').nth(1).unwrap_or_default().to_string(),
                    headers,
                    body: String::from_utf8(form).unwrap(),
                };
                log.lock().unwrap().push(keep(asked));
                let mut conn = reader.into_inner();
                write!(
                    conn,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (url, seen)
    }

    fn credentials(url: &str, scope: Option<&str>) -> ClientCredentials {
        // The environment's proxy would carry a loopback call away.
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        ClientCredentials::new(
            url.to_string(),
            "app".into(),
            "s3cr&t".into(),
            scope.map(str::to_string),
            agent,
            Duration::from_secs(5),
        )
    }

    fn token(value: &str, expires_in: u64) -> (u16, String) {
        (
            200,
            json!({"access_token": value, "token_type": "bearer", "expires_in": expires_in}).to_string(),
        )
    }

    #[test]
    fn a_token_is_kept_until_forgotten() {
        let (url, seen) = serve(vec![token("one", 3600), token("two", 3600)]);
        let mut credentials = credentials(&url, Some("PRINCIPAL_ROLE:ALL"));
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        credentials.forget();
        assert_eq!(credentials.bearer().unwrap(), "Bearer two");
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[0],
            "grant_type=client_credentials&client_id=app&client_secret=s3cr%26t&scope=PRINCIPAL_ROLE%3AALL"
        );
    }

    #[test]
    fn a_token_about_to_expire_is_renewed_before_the_call() {
        let (url, seen) = serve(vec![token("one", RENEW_BEFORE.as_secs() - 1), token("two", 3600)]);
        let mut credentials = credentials(&url, None);
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        assert_eq!(credentials.bearer().unwrap(), "Bearer two");
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_secret_never_shows_in_a_refused_sign_in() {
        let (url, _) = serve(vec![(
            401,
            json!({"error": "invalid_client", "error_description": "bad"}).to_string(),
        )]);
        let error = credentials(&url, None).bearer().unwrap_err().to_string();
        assert!(error.contains("invalid_client: bad"), "{error}");
        assert!(!error.contains("s3cr"), "{error}");
    }

    #[test]
    fn a_basic_secret_goes_in_the_header_and_not_the_form() {
        let (url, seen) = serve_seeing(vec![token("one", 3600)]);
        let mut credentials = credentials(&url, Some("all-apis")).sending_secret(TokenAuth::Basic);
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        assert_eq!(
            seen.lock().unwrap()[0],
            format!(
                "{}|grant_type=client_credentials&scope=all-apis",
                basic("app", "s3cr&t")
            )
        );
    }

    #[test]
    fn a_token_is_kept_when_sign_in_moves() {
        let (url, seen) = serve(vec![token("one", 3600)]);
        let mut credentials = credentials(&url, None);
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        credentials.sign_in_at("http://127.0.0.1:1/token".into());
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn an_assertion_file_proves_the_client_and_is_read_at_every_sign_in() {
        let (url, seen) = serve(vec![token("one", 3600), token("two", 3600)]);
        let file = std::env::temp_dir().join(format!("smeltr-assertion-{}", std::process::id()));
        std::fs::write(&file, "first.jwt\n").unwrap();
        let mut credentials = credentials(&url, Some("https://ns/.default"))
            .sending_secret(TokenAuth::Basic)
            .proven_by_assertion_file(file.clone());
        assert_eq!(credentials.bearer().unwrap(), "Bearer one");
        std::fs::write(&file, "second.jwt").unwrap();
        credentials.forget();
        assert_eq!(credentials.bearer().unwrap(), "Bearer two");
        std::fs::remove_file(&file).unwrap();
        credentials.forget();
        let err = credentials.bearer().unwrap_err().to_string();
        assert!(err.contains(&file.display().to_string()), "{err}");
        let seen = seen.lock().unwrap();
        let form = |jwt: &str| {
            format!(
                "grant_type=client_credentials&client_id=app\
                 &client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer\
                 &client_assertion={jwt}&scope=https%3A%2F%2Fns%2F.default"
            )
        };
        assert_eq!(*seen, [form("first.jwt"), form("second.jwt")]);
    }
}
