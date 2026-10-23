//! A certificate the leader orders for itself from a certificate authority, over ACME (RFC 8555), and renews.
//!
//! ```mermaid
//! flowchart LR
//!   S[settings] --> O[order: a key and a request]
//!   O --> A[the authority, over ACME]
//!   A -->|validates| P[a proof on the leader's port]
//!   A --> C[the chain]
//!   C --> R[the root that signed it]
//!   R --> T[tls: served at once, or rotated to]
//!   T --> N[state: when to renew]
//! ```
//!
//! The authority proves the leader holds a name by connecting to it: on the port the leader serves, asking for
//! `acme-tls/1` (`leader::resolver`), or on the HTTP port, asking for the challenge's token. The certificate
//! arrives the way an uploaded one does ([`tls::install`] under the root already in force, a rotation
//! ([`tls::begin_supplied_rotation`]) under another), so a worker is given the root that signed it and nothing
//! about serving it is new. What this module keeps is under `tls/acme/`: the account, and what the last order
//! left and when the next is due.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body as _;
use instant_acme::{
    Account, AuthorizationStatus, BodyWrapper, BytesResponse, ChallengeType, ExternalAccountKey, HttpClient,
    Identifier, NewAccount, NewOrder, OrderStatus, RetryPolicy,
};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use super::private_file;
use super::resolver::Resolver;
use super::store::{Store, StoreError};
use super::tls::{self, Described};

pub const ENABLED_KEY: &str = "acme.enabled";
/// The authority's directory URL.
pub const DIRECTORY_KEY: &str = "acme.directory";
/// An email address the authority may write to about the certificate.
pub const CONTACT_KEY: &str = "acme.contact";
/// The names the certificate carries, comma-separated.
pub const NAMES_KEY: &str = "acme.names";
/// How the authority is to prove the leader holds a name: `tls-alpn-01` or `http-01`.
pub const CHALLENGE_KEY: &str = "acme.challenge";
/// Roots this leader trusts beyond the platform's, PEM: the authority's own, where it is not publicly trusted.
pub const AUTHORITY_KEY: &str = "acme.authority";
/// External account binding, for an authority that asks for one: the key id, and the HMAC key, sealed.
pub const EAB_KEY_ID_KEY: &str = "acme.eab_key_id";
pub const EAB_HMAC_KEY: &str = "acme.eab_hmac";

/// Under `tls/`.
pub const DIR: &str = "acme";
const ACCOUNT: &str = "account.json";
const STATE: &str = "state.json";

/// How long an order may take, the authority's validation included. Let's Encrypt validates within seconds; a
/// minute is for an authority that queues.
const ORDER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// How a certificate authority proves the leader holds a name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Challenge {
    #[serde(rename = "tls-alpn-01")]
    TlsAlpn01,
    #[serde(rename = "http-01")]
    Http01,
}

impl Challenge {
    pub fn as_str(self) -> &'static str {
        match self {
            Challenge::TlsAlpn01 => "tls-alpn-01",
            Challenge::Http01 => "http-01",
        }
    }

    pub fn parse(text: &str) -> Option<Challenge> {
        match text {
            "tls-alpn-01" => Some(Challenge::TlsAlpn01),
            "http-01" => Some(Challenge::Http01),
            _ => None,
        }
    }
}

/// The authority this leader orders from, and what it orders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub enabled: bool,
    pub directory: String,
    pub contact: String,
    pub names: Vec<String>,
    pub challenge: Challenge,
    pub authority: String,
    pub eab_key_id: String,
    pub eab_hmac: Option<String>,
}

impl Settings {
    pub fn read(store: &dyn Store) -> Result<Settings, StoreError> {
        Ok(Settings {
            enabled: store.setting(ENABLED_KEY)?.as_deref() == Some("true"),
            directory: store.setting(DIRECTORY_KEY)?.unwrap_or_default(),
            contact: store.setting(CONTACT_KEY)?.unwrap_or_default(),
            names: split_names(&store.setting(NAMES_KEY)?.unwrap_or_default()),
            challenge: store
                .setting(CHALLENGE_KEY)?
                .and_then(|c| Challenge::parse(&c))
                .unwrap_or(Challenge::TlsAlpn01),
            authority: store.setting(AUTHORITY_KEY)?.unwrap_or_default(),
            eab_key_id: store.setting(EAB_KEY_ID_KEY)?.unwrap_or_default(),
            eab_hmac: store.opened_setting(EAB_HMAC_KEY)?.filter(|s| !s.is_empty()),
        })
    }
}

/// Whether an order can be placed with these: an authority to ask, and names to ask for.
pub fn orderable(directory: &str, names: &[String]) -> Result<(), String> {
    if directory.is_empty() || names.is_empty() {
        return Err("ordering needs the authority's directory URL and at least one name".to_string());
    }
    Ok(())
}

/// `names` as [`NAMES_KEY`] holds them: trimmed, empties dropped.
pub fn split_names(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// What the last order left, and when the next is due. Under `tls/acme/state.json`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    /// The authority the certificate came from, so a changed directory orders afresh.
    #[serde(default)]
    pub directory: String,
    /// The certificate the last order installed. What says a served certificate is this module's.
    #[serde(default)]
    pub fingerprint: Option<String>,
    /// The one before it, while an order under a new root waits on the fleet: served until the cutover, and
    /// still this module's.
    #[serde(default)]
    pub previous_fingerprint: Option<String>,
    #[serde(default)]
    pub not_after_millis: Option<i64>,
    /// Two thirds of the way through the certificate's life, which is when a renewal is ordered.
    #[serde(default)]
    pub renew_after_millis: Option<i64>,
    #[serde(default)]
    pub last_attempt_millis: Option<i64>,
    /// Why the last attempt failed. Cleared by one that succeeds.
    #[serde(default)]
    pub last_error: Option<String>,
}

pub fn dir_of(tls_dir: &Path) -> PathBuf {
    tls_dir.join(DIR)
}

/// What is under `tls/acme/state.json`, or nothing yet.
pub fn state(tls_dir: &Path) -> Result<State, String> {
    let path = dir_of(tls_dir).join(STATE);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

pub fn write_state(tls_dir: &Path, state: &State) -> Result<(), String> {
    let dir = dir_of(tls_dir);
    private_file::create_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = dir.join(STATE);
    let json = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
    tls::write_durably(&path, &json).map_err(|e| format!("{}: {e}", path.display()))
}

/// Whether a renewal is due: nothing issued yet, or the certificate two thirds through its life.
pub fn due(state: &State, now_ms: i64) -> bool {
    match (state.fingerprint.as_ref(), state.renew_after_millis) {
        (None, _) => true,
        (Some(_), Some(after)) => now_ms >= after,
        (Some(_), None) => true,
    }
}

/// Whether the renewal task orders now: only to renew the certificate this module issued, while it is the one
/// served. A first order is the administrator's, and so is coming back to ACME after another source: the task never
/// changes what is served on its own.
pub fn renews(state: &State, served: Source, now_ms: i64) -> bool {
    served == Source::Acme && due(state, now_ms)
}

/// Where a served certificate came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// This leader's own authority signed it.
    Own,
    /// An operator uploaded it.
    Upload,
    /// An order from a certificate authority, through this module.
    Acme,
}

/// Where what is served came from. An uploaded certificate and an ordered one are the same thing to `tls`; what
/// tells them apart is whether the one in force is the one the last order installed.
pub fn source(tls_dir: &Path) -> Result<Source, String> {
    let (materials, served) = tls::in_force(tls_dir)?;
    if !materials.supplied {
        return Ok(Source::Own);
    }
    supplied_by(tls_dir, &served, "the certificate")
}

/// Where what `rotation` puts in force comes from.
pub fn incoming(tls_dir: &Path, rotation: &tls::Rotation) -> Result<Source, String> {
    if rotation.installs != tls::Installs::Supplied {
        return Ok(Source::Own);
    }
    match tls::incoming_certificate(tls_dir)? {
        Some(pem) => supplied_by(tls_dir, &pem, "the incoming certificate"),
        None => Ok(Source::Upload),
    }
}

/// Which supplier a supplied certificate came from: this module, if `pem` is what an order installed, else an
/// operator.
fn supplied_by(tls_dir: &Path, pem: &[u8], what: &str) -> Result<Source, String> {
    let fingerprint = tls::describe(pem, what)?.fingerprint;
    let state = state(tls_dir)?;
    Ok(match (state.fingerprint, state.previous_fingerprint) {
        (Some(issued), _) | (_, Some(issued)) if issued == fingerprint => Source::Acme,
        _ => Source::Upload,
    })
}

/// Where a proof goes while the authority checks it.
#[async_trait]
pub trait Prover: Send + Sync {
    /// Answer a validator asking for `name` over `acme-tls/1` with a certificate carrying `digest`.
    async fn tls_alpn(&self, name: &str, digest: &[u8]) -> Result<(), String>;
    /// Answer `GET /.well-known/acme-challenge/{token}` with `key_authorization`.
    async fn http(&self, token: &str, key_authorization: &str) -> Result<(), String>;
    /// The order is over, however it ended.
    async fn done(&self);
}

/// Who signs the request: the authority over ACME, or a stand-in under test.
#[async_trait]
pub trait Issuer: Send + Sync {
    /// The chain for `csr_der`, PEM, leaf first, once the authority has validated every name through `prover`.
    async fn issue(&self, names: &[String], csr_der: &[u8], prover: &dyn Prover) -> Result<String, String>;
}

/// What an order produced, before anything serves it.
pub struct Issued {
    pub chain: Vec<u8>,
    pub key: Vec<u8>,
    /// The one root that signed `chain`, PEM: what a worker is given.
    pub ca: Vec<u8>,
    pub certificate: Described,
    /// Whether serving it changes the root workers verify by, which is what decides between serving it at once
    /// and rotating to it.
    pub rotates: bool,
}

/// Orders a certificate for `settings.names` from `issuer` and checks it the way an upload is checked: against
/// `names`, the names this leader is reached at, and `required`, the ones it cannot drop. Nothing is written.
pub async fn order(
    tls_dir: &Path,
    settings: &Settings,
    issuer: &dyn Issuer,
    prover: &dyn Prover,
    names: &[String],
    required: &[String],
) -> Result<Issued, String> {
    if settings.names.is_empty() {
        return Err("name at least one name for the certificate to carry".to_string());
    }
    // Asked before the authority is, so an order that cannot be served is not spent: an authority counts them.
    // A required name is the one the worker inside this process reaches the leader by. Neither challenge can prove
    // a wildcard, so the names carry it as written or not at all. A renewal of the
    // certificate in force changes no root and is served whatever rotation is open; anything else would begin
    // one, and only one is open at a time.
    if let Some(name) = required
        .iter()
        .find(|r| !settings.names.iter().any(|n| n.eq_ignore_ascii_case(r)))
    {
        return Err(format!(
            "this leader is reached at {name:?} by the worker inside this process, which cannot be told to use \
             another name, and the names do not include it. Add it to the names, or set worker.leader.url to one \
             of them."
        ));
    }
    if tls::rotation(tls_dir)?.is_some() && source(tls_dir)? != Source::Acme {
        tls::rotation_may_begin(tls_dir, tls::Installs::Supplied).map_err(|e| e.message().to_string())?;
    }
    let mut params = rcgen::CertificateParams::new(settings.names.clone())
        .map_err(|e| format!("the names are not ones a certificate can carry: {e}"))?;
    // Named for its first name, as a public authority names what it issues; the page shows the subject.
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, settings.names[0].clone());
    let key = rcgen::KeyPair::generate().map_err(|e| format!("a key for the certificate: {e}"))?;
    let csr = params
        .serialize_request(&key)
        .map_err(|e| format!("the certificate request: {e}"))?;

    let issued = tokio::time::timeout(ORDER_TIMEOUT, issuer.issue(&settings.names, csr.der(), prover)).await;
    prover.done().await;
    let chain = match issued {
        Ok(Ok(chain)) => chain.into_bytes(),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(format!(
                "the authority did not finish within {}s",
                ORDER_TIMEOUT.as_secs()
            ));
        }
    };
    let key = key.serialize_pem().into_bytes();

    let (chain, ca) = root_of(&chain, &settings.authority)?;
    let ca = ca.into_bytes();
    tls::check_supplied(&chain, &key, &ca, names, required)?;
    let rotates = tls::supplying_changes_root(tls_dir, &chain, &ca)?;
    // Under another root, it would begin a rotation while one is open: refused here, before a caller records one
    // beginning. The order is spent, since only the issued certificate says which root it is under.
    if rotates {
        tls::rotation_may_begin(tls_dir, tls::Installs::Supplied).map_err(|e| e.message().to_string())?;
    }
    let certificate = tls::describe(&chain, "the certificate")?;
    Ok(Issued {
        chain,
        key,
        ca,
        certificate,
        rotates,
    })
}

/// `chain` as it is served, and the root that signed it, PEM: as [`tls::split_root`] finds it, from `authority`
/// or else the platform's roots. A worker is given this one certificate, as it would be given an uploaded one.
fn root_of(chain: &[u8], authority: &str) -> Result<(Vec<u8>, String), String> {
    tls::split_root(chain, authority.as_bytes())?.ok_or_else(|| {
        "the authority issued a certificate under a root neither the platform nor the authority setting \
         trusts. Add the root to the authority setting."
            .to_string()
    })
}

/// Serves `issued`: at once under the root in force, else as the rotation an upload under another root begins.
/// Records what it installed and when to renew.
pub fn take(
    tls_dir: &Path,
    settings: &Settings,
    issued: &Issued,
    names: &[String],
    required: &[String],
    now_ms: i64,
) -> Result<(), String> {
    if issued.rotates {
        tls::rotation_may_begin(tls_dir, tls::Installs::Supplied).map_err(|e| e.message().to_string())?;
        tls::begin_supplied_rotation(tls_dir, &issued.chain, &issued.key, &issued.ca, names, required, now_ms)?;
    } else {
        tls::install(tls_dir, &issued.chain, &issued.key, &issued.ca, names, required)?;
    }
    let life = issued.certificate.not_after_millis - issued.certificate.not_before_millis;
    let was = state(tls_dir)?;
    write_state(
        tls_dir,
        &State {
            directory: settings.directory.clone(),
            fingerprint: Some(issued.certificate.fingerprint.clone()),
            previous_fingerprint: issued.rotates.then_some(was.fingerprint).flatten(),
            not_after_millis: Some(issued.certificate.not_after_millis),
            renew_after_millis: Some(issued.certificate.not_before_millis + life / 3 * 2),
            last_attempt_millis: Some(now_ms),
            last_error: None,
        },
    )
}

/// Forgets the order an abandoned switch would have served: nothing will serve it. What is served stays this
/// module's if it was, with the dates it carries.
pub fn abandoned(tls_dir: &Path) -> Result<(), String> {
    let was = state(tls_dir)?;
    let (_, served) = tls::in_force(tls_dir)?;
    let served = tls::describe(&served, "the certificate")?;
    let kept = was.previous_fingerprint.filter(|p| *p == served.fingerprint);
    let life = served.not_after_millis - served.not_before_millis;
    let dates = kept.is_some();
    write_state(
        tls_dir,
        &State {
            fingerprint: kept,
            previous_fingerprint: None,
            not_after_millis: dates.then_some(served.not_after_millis),
            renew_after_millis: dates.then_some(served.not_before_millis + life / 3 * 2),
            ..was
        },
    )
}

/// Records that an attempt at `now_ms` failed with `error`, keeping what the last success left.
pub fn failed(tls_dir: &Path, now_ms: i64, error: &str) -> Result<(), String> {
    let mut state = state(tls_dir)?;
    state.last_attempt_millis = Some(now_ms);
    state.last_error = Some(error.to_string());
    write_state(tls_dir, &state)
}

// -- the authority, over ACME -------------------------------------------------------------------------------------

/// A certificate authority reached at its directory URL, with the account this leader holds there.
pub struct Directory {
    settings: Settings,
    tls_dir: PathBuf,
}

impl Directory {
    pub fn new(settings: Settings, tls_dir: &Path) -> Directory {
        Directory {
            settings,
            tls_dir: tls_dir.to_path_buf(),
        }
    }

    /// A client for the authority: TLS through this process's provider, trusting the platform's roots and the
    /// authority setting's, HTTPS only.
    fn client(&self) -> Result<Box<dyn HttpClient>, String> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(crate::io::trust::public().iter().cloned());
        for cert in CertificateDer::pem_slice_iter(self.settings.authority.as_bytes()) {
            let cert = cert.map_err(|e| format!("the authority setting is not a certificate: {e}"))?;
            roots
                .add(cert)
                .map_err(|e| format!("the authority setting is not a certificate authority: {e}"))?;
        }
        let tls = rustls::ClientConfig::builder_with_provider(crate::crypto::in_use())
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("the provider's TLS: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| format!("could not make a client for the authority: {e}"))?;
        Ok(Box::new(Http(client)))
    }

    /// The account this leader holds at the directory, created on the first order and kept under
    /// `tls/acme/account.json`. A changed directory is another authority and another account.
    async fn account(&self) -> Result<Account, String> {
        let path = dir_of(&self.tls_dir).join(ACCOUNT);
        let directory = self.settings.directory.clone();
        if let Ok(bytes) = std::fs::read(&path)
            && let Ok(saved) = serde_json::from_slice::<SavedAccount>(&bytes)
            && saved.directory == directory
        {
            return Account::builder_with_http(self.client()?)
                .from_credentials(saved.credentials)
                .await
                .map_err(|e| format!("the account at {directory}: {e}"));
        }
        let contact = (!self.settings.contact.is_empty()).then(|| format!("mailto:{}", self.settings.contact));
        let contacts: Vec<&str> = contact.iter().map(String::as_str).collect();
        let eab = match (&self.settings.eab_key_id, &self.settings.eab_hmac) {
            (id, Some(hmac)) if !id.is_empty() => {
                use base64::Engine as _;
                let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(hmac.trim_end_matches('='))
                    .map_err(|e| format!("the external account HMAC key is not base64url: {e}"))?;
                Some(ExternalAccountKey::new(id.clone(), &key))
            }
            _ => None,
        };
        let (account, credentials) = Account::builder_with_http(self.client()?)
            .create(
                &NewAccount {
                    contact: &contacts,
                    terms_of_service_agreed: true,
                    only_return_existing: false,
                },
                directory.clone(),
                eab.as_ref(),
            )
            .await
            .map_err(|e| format!("creating an account at {directory}: {e}"))?;
        let dir = dir_of(&self.tls_dir);
        private_file::create_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let json = serde_json::to_vec(&SavedAccount { directory, credentials }).map_err(|e| e.to_string())?;
        private_file::replace(&path, &json).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(account)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SavedAccount {
    directory: String,
    credentials: instant_acme::AccountCredentials,
}

#[async_trait]
impl Issuer for Directory {
    async fn issue(&self, names: &[String], csr_der: &[u8], prover: &dyn Prover) -> Result<String, String> {
        let account = self.account().await?;
        let identifiers: Vec<Identifier> = names
            .iter()
            .map(|name| match name.parse() {
                Ok(ip) => Identifier::Ip(ip),
                Err(_) => Identifier::Dns(name.clone()),
            })
            .collect();
        let mut order = account
            .new_order(&NewOrder::new(&identifiers))
            .await
            .map_err(|e| format!("the order was refused: {e}"))?;

        let kind = match self.settings.challenge {
            Challenge::TlsAlpn01 => ChallengeType::TlsAlpn01,
            Challenge::Http01 => ChallengeType::Http01,
        };
        let mut authorizations = order.authorizations();
        while let Some(authorization) = authorizations.next().await {
            let mut authorization = authorization.map_err(|e| format!("an authorization: {e}"))?;
            if authorization.status == AuthorizationStatus::Valid {
                continue;
            }
            let name = authorization.identifier().to_string();
            let mut challenge = authorization.challenge(kind.clone()).ok_or_else(|| {
                format!(
                    "the authority offers no {} challenge for {name}",
                    self.settings.challenge.as_str()
                )
            })?;
            let key_authorization = challenge.key_authorization();
            match self.settings.challenge {
                Challenge::TlsAlpn01 => prover.tls_alpn(&name, key_authorization.digest().as_ref()).await?,
                Challenge::Http01 => prover.http(&challenge.token, key_authorization.as_str()).await?,
            }
            challenge
                .set_ready()
                .await
                .map_err(|e| format!("the challenge for {name}: {e}"))?;
        }

        let retries = RetryPolicy::new().timeout(ORDER_TIMEOUT);
        match order.poll_ready(&retries).await {
            Ok(OrderStatus::Ready) => {}
            Ok(status) => {
                return Err(match order.state().error.as_ref() {
                    Some(problem) => format!("the authority did not validate the names: {problem}"),
                    None => format!("the order is {status:?}, not ready"),
                });
            }
            Err(e) => return Err(format!("waiting for the authority to validate: {e}")),
        }
        order
            .finalize_csr(csr_der)
            .await
            .map_err(|e| format!("the certificate request was refused: {e}"))?;
        order
            .poll_certificate(&retries)
            .await
            .map_err(|e| format!("waiting for the certificate: {e}"))
    }
}

/// instant-acme's HTTP over this process's reqwest, so what it trusts is what every other client here trusts.
struct Http(reqwest::Client);

impl HttpClient for Http {
    fn request(
        &self,
        request: http::Request<BodyWrapper<Bytes>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BytesResponse, instant_acme::Error>> + Send>> {
        let client = self.0.clone();
        Box::pin(async move {
            let other = |e: Box<dyn std::error::Error + Send + Sync>| instant_acme::Error::Other(e);
            let (parts, mut body) = request.into_parts();
            // One frame, and `Infallible` to read it: the wrapper is a `Bytes` that polls out whole.
            let mut collected = Vec::new();
            while let Some(frame) = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await {
                let Ok(frame) = frame;
                if let Ok(data) = frame.into_data() {
                    collected.extend_from_slice(&data);
                }
            }
            let request = client
                .request(parts.method, parts.uri.to_string())
                .headers(parts.headers)
                .body(collected)
                .build()
                .map_err(|e| other(Box::new(e)))?;
            let response = client.execute(request).await.map_err(|e| other(Box::new(e)))?;
            Ok(BytesResponse::from(http::Response::<reqwest::Body>::from(response)))
        })
    }
}

// -- where a proof goes --------------------------------------------------------------------------------------------

/// The proofs this leader can give: on its own port through the resolver, and on an HTTP port it listens on only
/// while an order is open.
pub struct Proving {
    resolver: Arc<Resolver>,
    /// Where the HTTP-01 listener binds: the leader's bind address, on the port the authority connects to, which
    /// is 80 unless `SMELTR_ACME_HTTP_PORT` says otherwise.
    http_at: std::net::SocketAddr,
    tokens: Arc<Mutex<std::collections::HashMap<String, String>>>,
    listener: tokio::sync::Mutex<Option<Listening>>,
}

struct Listening {
    stop: tokio::sync::oneshot::Sender<()>,
    stopped: tokio::task::JoinHandle<()>,
}

impl Proving {
    pub fn new(resolver: Arc<Resolver>, http_at: std::net::SocketAddr) -> Proving {
        Proving {
            resolver,
            http_at,
            tokens: Default::default(),
            listener: tokio::sync::Mutex::new(None),
        }
    }

    /// The HTTP-01 listener, started if it is not running.
    async fn listen(&self) -> Result<(), String> {
        let mut held = self.listener.lock().await;
        if held.is_some() {
            return Ok(());
        }
        let at = self.http_at;
        let listener = tokio::net::TcpListener::bind(at).await.map_err(|e| {
            format!(
                "could not listen on {at} for the authority's HTTP-01 challenge: {e}. The authority connects to \
                 port 80, which this process has to be allowed to bind, or to whatever forwards it here."
            )
        })?;
        let tokens = self.tokens.clone();
        let app = axum::Router::new()
            .route(
                "/.well-known/acme-challenge/:token",
                axum::routing::get(
                    |axum::extract::State(tokens): axum::extract::State<
                        Arc<Mutex<std::collections::HashMap<String, String>>>,
                    >,
                     axum::extract::Path(token): axum::extract::Path<String>| async move {
                        let found = tokens.lock().unwrap_or_else(|e| e.into_inner()).get(&token).cloned();
                        match found {
                            Some(key_authorization) => (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                                key_authorization,
                            ),
                            None => (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                "no such challenge".to_string(),
                            ),
                        }
                    },
                ),
            )
            .with_state(tokens);
        let (stop, told) = tokio::sync::oneshot::channel::<()>();
        let stopped = tokio::spawn(async move {
            let served = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = told.await;
            });
            if let Err(e) = served.await {
                tracing::warn!(target: "leader.acme_http", error = %e, "the HTTP-01 listener stopped");
            }
        });
        *held = Some(Listening { stop, stopped });
        Ok(())
    }
}

#[async_trait]
impl Prover for Proving {
    async fn tls_alpn(&self, name: &str, digest: &[u8]) -> Result<(), String> {
        self.resolver.prove(name, digest)
    }

    async fn http(&self, token: &str, key_authorization: &str) -> Result<(), String> {
        self.listen().await?;
        self.tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(token.to_string(), key_authorization.to_string());
        Ok(())
    }

    async fn done(&self) {
        self.resolver.done();
        self.tokens.lock().unwrap_or_else(|e| e.into_inner()).clear();
        if let Some(listening) = self.listener.lock().await.take() {
            let _ = listening.stop.send(());
            // Until the port is free again, so a renewal that follows at once can bind it. A validator still mid-
            // request holds it a moment; one that never let go is not waited for.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), listening.stopped).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuzz::{Coverage, Rng, run};
    use std::path::PathBuf;
    use std::time::Duration;

    const A_YEAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);
    const NOW: i64 = 1_800_000_000_000;

    /// The clock, where a date on a certificate issued now is compared with it.
    fn now() -> i64 {
        crate::leader::warnings::now_millis()
    }

    fn names() -> Vec<String> {
        vec!["localhost".to_string(), "127.0.0.1".to_string()]
    }

    /// A `tls/` with this leader's own authority in force, and a socket that takes every replacement at once.
    fn scratch(tag: &str) -> PathBuf {
        static FOLLOWING: std::sync::Once = std::sync::Once::new();
        FOLLOWING.call_once(|| tls::on_reload(|dir| tls::socket_took(dir, tls::disk_generation(dir))));
        let dir = std::env::temp_dir().join(format!("smeltr-acme-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir = dir.join("tls");
        tls::ensure(&dir, names(), A_YEAR, &[]).expect("the leader's own authority");
        dir
    }

    /// A certificate authority of somebody else's, signing what it is asked to.
    struct Authority {
        params: rcgen::CertificateParams,
        key: rcgen::KeyPair,
        pem: String,
    }

    impl Authority {
        fn new(name: &str) -> Authority {
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params.distinguished_name = rcgen::DistinguishedName::new();
            params.distinguished_name.push(rcgen::DnType::CommonName, name);
            params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign, rcgen::KeyUsagePurpose::CrlSign];
            params.not_before = time::OffsetDateTime::now_utc() - Duration::from_secs(60 * 60);
            params.not_after = params.not_before + A_YEAR * 2;
            let key = rcgen::KeyPair::generate().unwrap();
            let pem = params.self_signed(&key).unwrap().pem();
            Authority { params, key, pem }
        }

        /// The chain for `csr_der`: the leaf alone, this authority being its root.
        fn sign(&self, csr_der: &[u8]) -> String {
            let request = rcgen::CertificateSigningRequestParams::from_der(&csr_der.to_vec().into()).expect("a CSR");
            let mut params = request.params;
            params.not_before = time::OffsetDateTime::now_utc() - Duration::from_secs(60 * 60);
            params.not_after = params.not_before + Duration::from_secs(90 * 24 * 60 * 60);
            let issuer = rcgen::Issuer::from_params(&self.params, &self.key);
            params.signed_by(&request.public_key, &issuer).unwrap().pem()
        }
    }

    /// Where a stand-in authority fails, when it does.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Fails {
        Never,
        /// Refuses the challenge: the proof was wrong, or it could not connect.
        Validating,
        /// Validates, then refuses the request.
        Finalizing,
    }

    /// A stand-in for the authority: asks the prover for every name, the way the real one does, and checks what the
    /// prover gave.
    struct FakeIssuer {
        authority: Authority,
        challenge: Challenge,
        fails: Fails,
        /// The HTTP-01 listener's port, where the fake issuer fetches the token like a validator would.
        http_port: u16,
    }

    #[async_trait]
    impl Issuer for FakeIssuer {
        async fn issue(&self, names: &[String], csr_der: &[u8], prover: &dyn Prover) -> Result<String, String> {
            for name in names {
                match self.challenge {
                    Challenge::TlsAlpn01 => prover.tls_alpn(name, &[9u8; 32]).await?,
                    Challenge::Http01 => {
                        let token = format!("token-{name}");
                        prover.http(&token, &format!("{token}.thumb")).await?;
                        // What a validator does: fetch it.
                        let url = format!("http://127.0.0.1:{}/.well-known/acme-challenge/{token}", self.http_port);
                        let body = reqwest::get(&url)
                            .await
                            .map_err(|e| format!("fetching {url}: {e}"))?
                            .text()
                            .await
                            .map_err(|e| e.to_string())?;
                        if body != format!("{token}.thumb") {
                            return Err(format!("the listener answered {body:?}"));
                        }
                    }
                }
            }
            match self.fails {
                Fails::Validating => return Err("the authority could not validate the names".to_string()),
                Fails::Finalizing => return Err("the authority refused the request".to_string()),
                Fails::Never => {}
            }
            Ok(self.authority.sign(csr_der))
        }
    }

    fn settings(names: Vec<String>, challenge: Challenge, authority: String) -> Settings {
        Settings {
            enabled: true,
            directory: "https://acme.test/dir".to_string(),
            contact: String::new(),
            names,
            challenge,
            authority,
            eab_key_id: String::new(),
            eab_hmac: None,
        }
    }

    /// Proofs answered by the leader whose `tls/` is `dir`, HTTP-01 on `http_port`.
    fn proving(dir: &Path, http_port: u16) -> Proving {
        let materials = tls::current(dir).unwrap();
        let resolver = Resolver::load(&materials.server_cert_path, &materials.server_key_path).unwrap();
        Proving::new(resolver, ([127, 0, 0, 1], http_port).into())
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Every way an order can go, from every state a leader can be in when it is placed.
    #[test]
    fn acme_fuzz() {
        let mut coverage = Coverage::new([
            "from-own",
            "from-upload",
            "from-acme",
            "rotation-open-refused",
            "rotation-open-renewal",
            "tls-alpn-01",
            "http-01",
            "installed",
            "rotated",
            "root-in-authority",
            "root-among-decoys",
            "root-missing",
            "name-not-served",
            "fails-validating",
            "fails-finalizing",
            "renewal-due",
            "renewal-not-due",
        ]);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        run(0..48, |seed| {
            let mut rng = Rng::seeded(seed);
            let tag = format!("fuzz-{seed}");
            let dir = scratch(&tag);
            let http_port = free_port();
            let proving = proving(&dir, http_port);
            let authority = Authority::new(&format!("acme fuzz {seed}"));

            // Where the leader starts from.
            let start = rng.below(3);
            let previous = Authority::new("previous");
            match start {
                0 => coverage.reach("from-own"),
                1 => {
                    coverage.reach("from-upload");
                    let key = rcgen::KeyPair::generate().unwrap();
                    let csr = rcgen::CertificateParams::new(names())
                        .unwrap()
                        .serialize_request(&key)
                        .unwrap();
                    let chain = previous.sign(csr.der());
                    tls::install(
                        &dir,
                        chain.as_bytes(),
                        key.serialize_pem().as_bytes(),
                        previous.pem.as_bytes(),
                        &names(),
                        &[],
                    )
                    .unwrap();
                }
                _ => {
                    coverage.reach("from-acme");
                    // A certificate from this same authority, as the last order left it.
                    let issuer = FakeIssuer {
                        authority: Authority::new("same"),
                        challenge: Challenge::TlsAlpn01,
                        fails: Fails::Never,
                        http_port,
                    };
                    let earlier = settings(names(), Challenge::TlsAlpn01, issuer.authority.pem.clone());
                    let issued = runtime
                        .block_on(order(&dir, &earlier, &issuer, &proving, &names(), &[]))
                        .unwrap();
                    take(&dir, &earlier, &issued, &names(), &[], now()).unwrap();
                    // Rotated to, since the leader's own root was in force: cut over so it is served.
                    tls::cut_over(&dir, names(), A_YEAR).unwrap();
                    assert_eq!(source(&dir).unwrap(), Source::Acme);
                    // The authority under test is that same one for a renewal, or another for a switch.
                    if rng.chance(2) {
                        return check_renewal(&dir, &earlier, issuer, &proving, &runtime, &mut rng, &mut coverage);
                    }
                }
            }
            let rotation_open = rng.chance(4);
            if rotation_open {
                open_rotation(&dir);
            }

            let challenge = if rng.chance(2) {
                Challenge::TlsAlpn01
            } else {
                Challenge::Http01
            };
            coverage.reach(challenge.as_str());
            let fails = *rng.pick(&[Fails::Never, Fails::Never, Fails::Validating, Fails::Finalizing]);
            let (root, root_case) = match rng.below(4) {
                0 | 1 => (authority.pem.clone(), "root-in-authority"),
                2 => (
                    format!("{}{}", Authority::new("decoy").pem, authority.pem),
                    "root-among-decoys",
                ),
                _ => (Authority::new("decoy").pem, "root-missing"),
            };
            let (ordered, name_case) = if rng.chance(5) {
                (vec!["elsewhere.test".to_string()], Some("name-not-served"))
            } else {
                (names(), None)
            };
            let settings = settings(ordered, challenge, root);
            let issuer = FakeIssuer {
                authority,
                challenge,
                fails,
                http_port,
            };

            let before = tls::in_force(&dir).unwrap().1;
            let source_before = source(&dir)?;
            let outcome = runtime.block_on(order(&dir, &settings, &issuer, &proving, &names(), &[]));
            // Whatever happened, the order is over: nothing is proving anything and the listener is gone.
            assert!(
                std::net::TcpStream::connect(("127.0.0.1", http_port)).is_err(),
                "the HTTP-01 listener stopped"
            );
            if rotation_open && source(&dir).unwrap() != Source::Acme {
                coverage.reach("rotation-open-refused");
                let e = outcome.err().ok_or("an order over an open rotation was taken")?;
                if !e.contains("rotat") {
                    return Err(format!("refused for the wrong reason: {e}"));
                }
                return Ok(());
            }
            match fails {
                Fails::Validating => {
                    coverage.reach("fails-validating");
                    return expect_err(outcome, "validate");
                }
                Fails::Finalizing => {
                    coverage.reach("fails-finalizing");
                    return expect_err(outcome, "refused");
                }
                Fails::Never => {}
            }
            if root_case == "root-missing" {
                coverage.reach(root_case);
                return expect_err(outcome, "neither the platform nor the authority");
            }
            if let Some(case) = name_case {
                coverage.reach(case);
                return expect_err(outcome, "no name this leader is reached at");
            }
            if rotation_open {
                // ACME in force, so the order was placed; under another root it would open a second rotation.
                return expect_err(outcome, "already rotating");
            }
            coverage.reach(root_case);
            let issued = outcome?;
            if !issued.rotates {
                return Err("a certificate under another root serves at once".to_string());
            }
            take(&dir, &settings, &issued, &names(), &[], now())?;
            coverage.reach("rotated");
            // Nothing served yet: the rotation is open and what is served has not moved.
            if tls::in_force(&dir).unwrap().1 != before {
                return Err("what is served moved before the fleet held the root".to_string());
            }
            if source(&dir)? != source_before {
                return Err(format!("what is served now reads as {:?}", source(&dir)?));
            }
            let state = state(&dir)?;
            if state.fingerprint.as_deref() != Some(&issued.certificate.fingerprint) {
                return Err("the state does not name what was ordered".to_string());
            }
            if due(&state, now()) {
                return Err("a renewal is due the moment a certificate is ordered".to_string());
            }
            tls::cut_over(&dir, names(), A_YEAR)?;
            if source(&dir)? != Source::Acme {
                return Err(format!("served, the certificate reads as {:?}", source(&dir)?));
            }
            Ok(())
        });
        coverage.assert_complete();
    }

    /// A renewal: the same authority again, which changes no root and serves at once, whatever rotation is open.
    fn check_renewal(
        dir: &Path,
        settings: &Settings,
        issuer: FakeIssuer,
        proving: &Proving,
        runtime: &tokio::runtime::Runtime,
        rng: &mut Rng,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        let earlier = state(dir)?;
        let rotation_open = rng.chance(2);
        if rotation_open {
            coverage.reach("rotation-open-renewal");
            open_rotation(dir);
        }
        let when = if rng.chance(2) {
            coverage.reach("renewal-due");
            earlier.renew_after_millis.unwrap() + 1
        } else {
            coverage.reach("renewal-not-due");
            earlier.renew_after_millis.unwrap() - 1
        };
        if !renews(&earlier, source(dir)?, when) {
            // Not due: the task would not order. Nothing else to check.
            return Ok(());
        }
        let issued = runtime.block_on(order(dir, settings, &issuer, proving, &names(), &[]))?;
        if issued.rotates {
            return Err("a renewal under the root in force rotates".to_string());
        }
        take(dir, settings, &issued, &names(), &[], when)?;
        coverage.reach("installed");
        if source(dir)? != Source::Acme {
            return Err("the renewal is not what is served".to_string());
        }
        if state(dir)?.fingerprint == earlier.fingerprint {
            return Err("the state still names the certificate renewed away from".to_string());
        }
        if rotation_open && tls::rotation(dir)?.is_none() {
            return Err("a renewal ended the rotation".to_string());
        }
        Ok(())
    }

    /// Opens the rotation the state allows: of the leader's own authority while it is in force, else to a
    /// certificate from another authority.
    fn open_rotation(dir: &Path) {
        if !tls::current(dir).unwrap().supplied {
            tls::begin_rotation(dir, A_YEAR, now() + 7 * 24 * 60 * 60 * 1000, now()).unwrap();
            return;
        }
        let switching = Authority::new("switching");
        let key = rcgen::KeyPair::generate().unwrap();
        let csr = rcgen::CertificateParams::new(names())
            .unwrap()
            .serialize_request(&key)
            .unwrap();
        let chain = switching.sign(csr.der());
        tls::begin_supplied_rotation(
            dir,
            chain.as_bytes(),
            key.serialize_pem().as_bytes(),
            switching.pem.as_bytes(),
            &names(),
            &[],
            now(),
        )
        .unwrap();
    }

    fn expect_err(outcome: Result<Issued, String>, saying: &str) -> Result<(), String> {
        match outcome {
            Ok(_) => Err(format!("an order that should have failed ({saying}) succeeded")),
            Err(e) if e.contains(saying) => Ok(()),
            Err(e) => Err(format!("failed for another reason than {saying:?}: {e}")),
        }
    }

    #[test]
    fn a_failed_attempt_is_recorded_beside_what_the_last_success_left() {
        let dir = scratch("failed");
        write_state(
            &dir,
            &State {
                directory: "https://acme.test/dir".to_string(),
                fingerprint: Some("abc".to_string()),
                previous_fingerprint: None,
                not_after_millis: Some(NOW + 1),
                renew_after_millis: Some(NOW),
                last_attempt_millis: Some(NOW - 1),
                last_error: None,
            },
        )
        .unwrap();
        failed(&dir, NOW, "the authority is down").unwrap();
        let state = state(&dir).unwrap();
        assert_eq!(state.fingerprint.as_deref(), Some("abc"));
        assert_eq!(state.last_attempt_millis, Some(NOW));
        assert_eq!(state.last_error.as_deref(), Some("the authority is down"));
        assert!(due(&state, NOW));
        assert_eq!(source(&dir).unwrap(), Source::Own);
    }

    /// The task renews what it issued, while that is served, and nothing else: not a first order that failed, and
    /// not after another source took over.
    #[test]
    fn the_task_renews_only_what_it_issued_while_it_is_served() {
        let issued = State {
            fingerprint: Some("abc".to_string()),
            renew_after_millis: Some(NOW),
            ..State::default()
        };
        assert!(renews(&issued, Source::Acme, NOW));
        assert!(!renews(&issued, Source::Acme, NOW - 1));
        assert!(!renews(&issued, Source::Own, NOW));
        assert!(!renews(&issued, Source::Upload, NOW));
        let never = State {
            last_error: Some("the authority is down".to_string()),
            ..State::default()
        };
        assert!(due(&never, NOW));
        assert!(!renews(&never, Source::Own, NOW));
        assert!(!renews(&never, Source::Upload, NOW));
    }

    /// An abandoned switch forgets the order it would have served. A first order leaves nothing; a renewal under a
    /// new root leaves the certificate still served as this module's, due when that one is.
    #[test]
    fn an_abandoned_switch_forgets_its_order() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let order_and_abandon = |dir: &Path, issuer: &FakeIssuer, proving: &Proving| {
            let ordered = settings(names(), Challenge::TlsAlpn01, issuer.authority.pem.clone());
            let issued = runtime
                .block_on(order(dir, &ordered, issuer, proving, &names(), &[]))
                .unwrap();
            assert!(issued.rotates);
            take(dir, &ordered, &issued, &names(), &[], now()).unwrap();
            let incoming = tls::rotation(dir).unwrap().expect("a switch").incoming;
            tls::abandon_rotation(dir, &incoming).unwrap();
            abandoned(dir).unwrap();
            state(dir).unwrap()
        };
        let issuer = |name: &str, http_port| FakeIssuer {
            authority: Authority::new(name),
            challenge: Challenge::TlsAlpn01,
            fails: Fails::Never,
            http_port,
        };

        let dir = scratch("abandon-first");
        let port = free_port();
        let first = order_and_abandon(&dir, &issuer("first", port), &proving(&dir, port));
        assert_eq!(first.fingerprint, None);
        assert_eq!(first.renew_after_millis, None);
        assert_eq!(source(&dir).unwrap(), Source::Own);

        let dir = scratch("abandon-renewal");
        let port = free_port();
        let proving = proving(&dir, port);
        let earlier = issuer("earlier", port);
        let ordered = settings(names(), Challenge::TlsAlpn01, earlier.authority.pem.clone());
        let issued = runtime
            .block_on(order(&dir, &ordered, &earlier, &proving, &names(), &[]))
            .unwrap();
        take(&dir, &ordered, &issued, &names(), &[], now()).unwrap();
        tls::cut_over(&dir, names(), A_YEAR).unwrap();
        let served = issued.certificate;
        let renewal = order_and_abandon(&dir, &issuer("later", port), &proving);
        assert_eq!(renewal.fingerprint, Some(served.fingerprint));
        assert_eq!(renewal.previous_fingerprint, None);
        assert_eq!(renewal.not_after_millis, Some(served.not_after_millis));
        assert!(renewal.renew_after_millis.is_some());
        assert_eq!(source(&dir).unwrap(), Source::Acme);
    }

    /// A renewal under a new root while a switch is open is refused by the order itself, so the caller never records
    /// a rotation beginning, and what the last order left stands.
    #[test]
    fn a_renewal_under_a_new_root_during_a_switch_is_refused_before_it_is_recorded() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let dir = scratch("new-root-during-switch");
        let port = free_port();
        let proving = proving(&dir, port);
        let issuer = |name: &str| FakeIssuer {
            authority: Authority::new(name),
            challenge: Challenge::TlsAlpn01,
            fails: Fails::Never,
            http_port: port,
        };
        let earlier = issuer("earlier");
        let first = settings(names(), Challenge::TlsAlpn01, earlier.authority.pem.clone());
        let issued = runtime
            .block_on(order(&dir, &first, &earlier, &proving, &names(), &[]))
            .unwrap();
        take(&dir, &first, &issued, &names(), &[], now()).unwrap();
        tls::cut_over(&dir, names(), A_YEAR).unwrap();
        open_rotation(&dir);
        let before = state(&dir).unwrap();

        let later = issuer("later");
        let renewal = settings(names(), Challenge::TlsAlpn01, later.authority.pem.clone());
        let refused = runtime
            .block_on(order(&dir, &renewal, &later, &proving, &names(), &[]))
            .err()
            .expect("a second rotation");
        assert!(refused.contains("already rotating"), "{refused}");
        assert_eq!(state(&dir).unwrap(), before);
        assert_eq!(source(&dir).unwrap(), Source::Acme);
    }

    /// An authority that must not be asked.
    struct Unasked(std::sync::atomic::AtomicBool);

    #[async_trait]
    impl Issuer for Unasked {
        async fn issue(&self, _: &[String], _: &[u8], _: &dyn Prover) -> Result<String, String> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            Err("the authority was asked".to_string())
        }
    }

    #[test]
    fn names_the_worker_inside_cannot_reach_are_refused_before_the_authority_is_asked() {
        let dir = scratch("unasked");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let proving = proving(&dir, free_port());
        let issuer = Unasked(false.into());
        let public = settings(
            vec!["leader.example.test".to_string()],
            Challenge::TlsAlpn01,
            String::new(),
        );
        let outcome = runtime.block_on(order(
            &dir,
            &public,
            &issuer,
            &proving,
            &names(),
            &["127.0.0.1".to_string()],
        ));
        let refusal = outcome.err().expect("refused");
        assert!(refusal.contains("\"127.0.0.1\""), "{refusal}");
        assert!(
            !issuer.0.load(std::sync::atomic::Ordering::SeqCst),
            "the authority was asked"
        );
    }

    #[test]
    fn names_are_split_on_commas_and_trimmed() {
        assert_eq!(split_names(" a.test, ,b.test,"), vec!["a.test", "b.test"]);
        assert!(split_names("").is_empty());
    }
}
