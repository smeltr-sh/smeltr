//! Signing in through the organization's OpenID provider, and the settings that name it.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant B as browser
//!   participant L as leader
//!   participant I as provider
//!   B->>L: GET /api/auth/oidc/start
//!   L-->>B: 302 to the provider, and a cookie binding this browser to the flow
//!   B->>I: signs in
//!   I-->>B: 302 to /api/auth/oidc/callback with a code and the flow's state
//!   B->>L: the callback, carrying the cookie
//!   L->>I: the code and the PKCE verifier, for an ID token
//!   L-->>B: a session cookie, and 302 to the GUI
//!   loop while the session lives
//!     L->>I: the refresh token, for what the provider says of the person now
//!   end
//! ```
//!
//! An OIDC sign-in ends in the same session a password does, so the gate, the deadlines and revocation treat the
//! two alike. Local sign-in never depends on anything here: an administrator holding a password can always get in
//! to fix a provider that is down or misconfigured.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use smeltr::leader::envelope::{ApiError, Envelope};
use smeltr::leader::oidc::{self, Jwks, Provider, RefreshFailure, Refusal};
use smeltr::leader::sessions::{Due, Grant};
use smeltr::leader::store::{Actor, OidcSignIn, Store, StoreError};
use smeltr::leader::{accounts, blocking};

use crate::gate::{Auth, refuse};

pub const ENABLED_KEY: &str = "oidc.enabled";
pub const ISSUER_KEY: &str = "oidc.issuer";
pub const CLIENT_ID_KEY: &str = "oidc.client_id";
pub const SECRET_KEY: &str = "oidc.client_secret";
pub const GROUPS_CLAIM_KEY: &str = "oidc.groups_claim";
pub const REQUIRED_GROUP_KEY: &str = "oidc.required_group";

/// The claim groups are read from where nobody has said. What Okta, Keycloak and Entra ID send by that name.
pub const DEFAULT_GROUPS_CLAIM: &str = "groups";

/// Where a sign-in starts. Reached at an alias origin, it first sends the browser to the configured one.
pub const START_PATH: &str = "/api/auth/oidc/start";

/// Where the provider sends a person back. Registered with the provider under the leader's configured origin.
pub const CALLBACK_PATH: &str = "/api/auth/oidc/callback";

/// How long a person has at the provider before the flow they started is forgotten.
const FLOW_LIFE: Duration = Duration::from_secs(10 * 60);

/// The most flows held at once. Starting one costs nothing, so the number is bounded rather than the callers; a
/// flood evicts the oldest, which ends sign-ins in progress while it lasts and never touches local sign-in.
const MOST_FLOWS: usize = 10_000;

/// How long a provider's discovery document and keys are trusted before they are read again. A key the provider
/// rotated in sooner is fetched the first time a token names it.
const PROVIDER_LIFE: Duration = Duration::from_secs(60 * 60);

/// How long a provider that could not be read is not asked again. Every sign-in would otherwise wait out its
/// timeouts in turn.
const FAILURE_LIFE: Duration = Duration::from_secs(10);

/// The provider this leader is configured for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub enabled: bool,
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub groups_claim: String,
    /// The group a person must hold to sign in. Required: a provider anyone can sign up to would otherwise let
    /// every one of its accounts in here.
    pub required_group: String,
}

impl Settings {
    pub fn read(store: &dyn Store) -> Result<Settings, StoreError> {
        Ok(Settings {
            enabled: store.setting(ENABLED_KEY)?.as_deref() == Some("true"),
            issuer: store.setting(ISSUER_KEY)?.unwrap_or_default(),
            client_id: store.setting(CLIENT_ID_KEY)?.unwrap_or_default(),
            client_secret: store.opened_setting(SECRET_KEY)?.filter(|s| !s.is_empty()),
            groups_claim: store
                .setting(GROUPS_CLAIM_KEY)?
                .filter(|claim| !claim.is_empty())
                .unwrap_or_else(|| DEFAULT_GROUPS_CLAIM.to_string()),
            required_group: store.setting(REQUIRED_GROUP_KEY)?.unwrap_or_default(),
        })
    }
}

/// A sign-in under way: sent to the provider, not back yet.
struct Flow {
    /// SHA-256 of the cookie that started it. Only that browser can finish it, so a link carrying somebody else's
    /// code and state cannot sign a visitor in as them.
    binding: String,
    nonce: String,
    verifier: String,
    started: Instant,
}

/// What discovery found, and the keys it named.
#[derive(Clone)]
struct Known {
    issuer: String,
    client: reqwest::Client,
    provider: Provider,
    jwks: Jwks,
    read_at: Instant,
}

/// The last read of a provider that failed.
struct Failed {
    issuer: String,
    error: String,
    at: Instant,
}

#[derive(Default)]
struct Cache {
    /// Moved by every settings change, so a read that began before one does not store what it found after it.
    epoch: u64,
    known: Option<Known>,
    failed: Option<Failed>,
}

/// The flows under way and the provider they go to. Lost on restart, like sessions: a sign-in in progress then
/// starts again.
///
/// No lock is held while the provider is read, so a provider that is slow or down holds up only the sign-ins that
/// need it, never a settings change that would fix it.
#[derive(Default)]
pub struct Oidc {
    flows: Mutex<HashMap<String, Flow>>,
    cache: Mutex<Cache>,
}

impl Oidc {
    /// Forget the provider, so the next sign-in reads it again. What a settings change does.
    pub fn forget(&self) {
        let mut cache = self.cache();
        cache.epoch += 1;
        cache.known = None;
        cache.failed = None;
    }

    fn cache(&self) -> MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn hold(&self, state: String, flow: Flow) {
        let mut flows = self.flows.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        flows.retain(|_, f| now.duration_since(f.started) < FLOW_LIFE);
        if flows.len() >= MOST_FLOWS
            && let Some(oldest) = flows.iter().min_by_key(|(_, f)| f.started).map(|(k, _)| k.clone())
        {
            flows.remove(&oldest);
        }
        flows.insert(state, flow);
    }

    /// The flow `state` names, once: a callback replayed finds nothing.
    fn take(&self, state: &str) -> Option<Flow> {
        let flow = self.flows.lock().unwrap_or_else(|e| e.into_inner()).remove(state)?;
        (flow.started.elapsed() < FLOW_LIFE).then_some(flow)
    }

    /// The provider for `issuer`, read now if it is not known or is stale. A stale one is kept while it cannot be
    /// read again, so a provider whose discovery is briefly down still signs people in with the keys it had.
    async fn provider(&self, issuer: &str) -> Result<Known, String> {
        let (epoch, stale) = {
            let cache = self.cache();
            let held = cache.known.as_ref().filter(|k| k.issuer == issuer);
            if let Some(k) = held.filter(|k| k.read_at.elapsed() < PROVIDER_LIFE) {
                return Ok(k.clone());
            }
            if let Some(failed) = cache
                .failed
                .as_ref()
                .filter(|f| f.issuer == issuer && f.at.elapsed() < FAILURE_LIFE)
            {
                return held.cloned().ok_or_else(|| failed.error.clone());
            }
            (cache.epoch, held.cloned())
        };
        let read = read_provider(issuer).await;
        let mut cache = self.cache();
        let current = cache.epoch == epoch;
        match read {
            Ok(known) => {
                if current {
                    cache.known = Some(known.clone());
                    cache.failed = None;
                }
                Ok(known)
            }
            Err(error) => {
                if current {
                    cache.failed = Some(Failed {
                        issuer: issuer.to_string(),
                        error: error.clone(),
                        at: Instant::now(),
                    });
                }
                match stale {
                    Some(known) => {
                        tracing::warn!(target: "leader.oidc_provider", issuer = %issuer, error = %error, "the OIDC provider could not be read again; its last keys are kept");
                        Ok(known)
                    }
                    None => Err(error),
                }
            }
        }
    }

    /// `token` checked against `known`'s keys, which are read again once if it names one they lack.
    async fn verify(
        &self,
        known: &Known,
        token: &str,
        expect: &oidc::Expect<'_>,
        groups_claim: &str,
    ) -> Result<oidc::Claims, Unverified> {
        match oidc::verify_id_token(token, &known.jwks, expect, groups_claim) {
            Ok(claims) => return Ok(claims),
            Err(Refusal::UnknownKey) => {}
            Err(Refusal::Invalid(why)) => return Err(Unverified::Refused(why)),
        }
        let jwks = oidc::fetch_jwks(&known.client, &known.provider.jwks_uri)
            .await
            .map_err(Unverified::KeysUnavailable)?;
        let checked = oidc::verify_id_token(token, &jwks, expect, groups_claim);
        if let Some(held) = self.cache().known.as_mut().filter(|k| k.issuer == known.issuer) {
            held.jwks = jwks;
        }
        checked.map_err(|refusal| {
            Unverified::Refused(match refusal {
                Refusal::UnknownKey => "the ID token was signed with a key the provider does not publish".to_string(),
                Refusal::Invalid(why) => why,
            })
        })
    }
}

/// Why a token was not believed.
enum Unverified {
    /// The token is not one to believe.
    Refused(String),
    /// It names a key the provider's set could not be read for, so nothing is known of the token.
    KeysUnavailable(String),
}

impl std::fmt::Display for Unverified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unverified::Refused(why) | Unverified::KeysUnavailable(why) => f.write_str(why),
        }
    }
}

/// Discovery for `issuer`, and the keys it names.
async fn read_provider(issuer: &str) -> Result<Known, String> {
    let client = oidc::client(issuer)?;
    let provider = oidc::discover(&client, issuer).await?;
    let jwks = oidc::fetch_jwks(&client, &provider.jwks_uri).await?;
    Ok(Known {
        issuer: issuer.to_string(),
        client,
        provider,
        jwks,
        read_at: Instant::now(),
    })
}

/// The cookie that binds a browser to the flow `state` names. One per flow, so a second sign-in started in another
/// tab leaves the first one's in place. `__Host-` where the session cookie is, for the same reason; `Lax`, because
/// the provider's redirect back is a top-level navigation from another site.
fn binding_cookie_name(auth: &Auth, state: &str) -> String {
    let tag: String = state.chars().take(16).collect();
    match auth.origin.secure {
        true => format!("__Host-smeltr_oidc_{tag}"),
        false => format!("smeltr_oidc_{tag}"),
    }
}

fn binding_cookie(auth: &Auth, state: &str, value: &str, max_age: u64) -> String {
    format!(
        "{}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{}",
        binding_cookie_name(auth, state),
        if auth.origin.secure { "; Secure" } else { "" }
    )
}

/// What the provider is told to send a person back to: the configured origin, the one registered with the provider.
/// A sign-in started at an alias was moved there first, so the browser holds the binding cookie the callback reads.
pub fn redirect_uri(auth: &Auth) -> String {
    format!("{}{CALLBACK_PATH}", auth.origin.value)
}

/// Whether this request reached the leader at one of its alias origins rather than the configured one. Judged by
/// the `Host` header against the aliases only, since a proxy may rewrite it to a name this leader does not know.
fn at_alias(auth: &Auth, headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let authority = |origin: &str| -> String {
        origin
            .split_once("://")
            .map_or(origin, |(_, rest)| rest)
            .to_ascii_lowercase()
    };
    let host = host.to_ascii_lowercase();
    host != authority(&auth.origin.value) && auth.origin.aliases.iter().any(|alias| authority(alias) == host)
}

/// Why a sign-in ended back at the GUI rather than in a session. The GUI says it in its own words; the detail is
/// in the leader's log, since it can describe the provider's configuration to whoever is asking.
#[derive(Clone, Copy)]
enum Ended {
    /// Not configured, or turned off.
    Unavailable,
    /// The provider could not be reached or answered with something unusable.
    Provider,
    /// The provider or the leader refused this person.
    Refused,
    /// The flow was unknown, stale, or started in another browser.
    Expired,
}

impl Ended {
    fn code(self) -> &'static str {
        match self {
            Ended::Unavailable => "unavailable",
            Ended::Provider => "provider",
            Ended::Refused => "refused",
            Ended::Expired => "expired",
        }
    }
}

/// Back to the GUI, saying why. Clears the binding cookie of the flow `state` names, when there is one.
fn back_to_gui(auth: &Auth, ended: Ended, state: Option<&str>) -> Response {
    let mut response = see_other(&format!("/?oidc={}", ended.code()));
    if let Some(state) = state {
        append_cookie(&mut response, &binding_cookie(auth, state, "", 0));
    }
    response
}

fn see_other(location: &str) -> Response {
    let mut response = StatusCode::SEE_OTHER.into_response();
    let headers = response.headers_mut();
    if let Ok(location) = HeaderValue::from_str(location) {
        headers.insert(header::LOCATION, location);
    }
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn append_cookie(response: &mut Response, cookie: &str) {
    if let Ok(cookie) = HeaderValue::from_str(cookie) {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
}

/// The settings, read where a sign-in may start: enabled and complete. `None` otherwise.
async fn usable(auth: &Auth) -> Result<Option<Settings>, StoreError> {
    let store = auth.store.clone();
    blocking(move || {
        let settings = Settings::read(store.as_ref())?;
        let complete = settings.enabled
            && !settings.issuer.is_empty()
            && !settings.client_id.is_empty()
            && settings.client_secret.is_some()
            && !settings.required_group.is_empty();
        Ok(complete.then_some(settings))
    })
    .await
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OfferedDto {
    /// Whether the sign-in screen offers OIDC sign-in.
    offered: bool,
}

/// `GET /api/auth/oidc`: whether a sign-in through the provider can start. Public, since the sign-in screen asks
/// before anyone is signed in. Says nothing about which provider.
pub async fn offered(State(auth): State<Auth>) -> Response {
    match usable(&auth).await {
        Ok(settings) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(OfferedDto {
                offered: settings.is_some(),
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "the OIDC settings could not be read");
            refuse(StatusCode::SERVICE_UNAVAILABLE, "the account store is unavailable")
        }
    }
}

/// `GET /api/auth/oidc/start`: send this browser to the provider.
///
/// Public, and a `GET`, because it is a link the sign-in screen follows. It changes nothing but the flows held.
pub async fn start(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    if at_alias(&auth, &headers) {
        return see_other(&format!("{}{START_PATH}", auth.origin.value));
    }
    let settings = match usable(&auth).await {
        Ok(Some(settings)) => settings,
        Ok(None) => return back_to_gui(&auth, Ended::Unavailable, None),
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "the OIDC settings could not be read");
            return back_to_gui(&auth, Ended::Unavailable, None);
        }
    };
    let known = match auth.oidc.provider(&settings.issuer).await {
        Ok(known) => known,
        Err(e) => {
            tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %e, "the OIDC provider could not be read");
            return back_to_gui(&auth, Ended::Provider, None);
        }
    };
    let state = oidc::random_token();
    let nonce = oidc::random_token();
    let binding = oidc::random_token();
    let pkce = oidc::pkce();
    let url = oidc::authorize_url(
        &known.provider,
        &settings.client_id,
        &redirect_uri(&auth),
        &state,
        &nonce,
        &pkce.challenge,
    );
    let mut response = see_other(&url);
    append_cookie(
        &mut response,
        &binding_cookie(&auth, &state, &binding, FLOW_LIFE.as_secs()),
    );
    auth.oidc.hold(
        state,
        Flow {
            binding: smeltr::leader::auth::hash_secret(&binding),
            nonce,
            verifier: pkce.verifier,
            started: Instant::now(),
        },
    );
    response
}

#[derive(serde::Deserialize)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// `GET /api/auth/oidc/callback`: the provider sending a person back.
///
/// Public, and a `GET` that issues a session, because that is how the provider returns. The flow it names must have
/// been started by this browser, so nobody can finish a sign-in somebody else began.
pub async fn callback(State(auth): State<Auth>, headers: HeaderMap, Query(query): Query<CallbackQuery>) -> Response {
    let Some(state) = query.state else {
        return back_to_gui(&auth, Ended::Expired, None);
    };
    let state = state.as_str();
    let Some(flow) = auth.oidc.take(state) else {
        return back_to_gui(&auth, Ended::Expired, Some(state));
    };
    let presented = crate::gate::presented_token(&headers, &binding_cookie_name(&auth, state));
    if presented.map(|b| smeltr::leader::auth::hash_secret(&b)).as_deref() != Some(flow.binding.as_str()) {
        return back_to_gui(&auth, Ended::Expired, Some(state));
    }
    // Recorded only now, for a flow this browser started, so an unauthenticated caller cannot fill the audit log.
    let refused = |target: &str| {
        accounts::record(
            auth.store.as_ref(),
            &Actor::anonymous(),
            "login",
            &bounded(target),
            "refused",
        );
        back_to_gui(&auth, Ended::Refused, Some(state))
    };
    if let Some(error) = query.error {
        tracing::warn!(target: "leader.sign_in_refused", provider_error = %bounded(&error), "the OIDC provider refused a sign-in");
        return refused("oidc");
    }
    let Some(code) = query.code else {
        return back_to_gui(&auth, Ended::Expired, Some(state));
    };

    // Read again: turned off while this person was at the provider ends the sign-in here.
    let settings = match usable(&auth).await {
        Ok(Some(settings)) => settings,
        Ok(None) => return back_to_gui(&auth, Ended::Unavailable, Some(state)),
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "the OIDC settings could not be read");
            return back_to_gui(&auth, Ended::Unavailable, Some(state));
        }
    };
    let Some(secret) = settings.client_secret.as_deref() else {
        return back_to_gui(&auth, Ended::Unavailable, Some(state));
    };
    let known = match auth.oidc.provider(&settings.issuer).await {
        Ok(known) => known,
        Err(e) => {
            tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %e, "the OIDC provider could not be read");
            return back_to_gui(&auth, Ended::Provider, Some(state));
        }
    };
    let tokens = match oidc::exchange(
        &known.client,
        &known.provider,
        &settings.client_id,
        secret,
        &redirect_uri(&auth),
        &code,
        &flow.verifier,
    )
    .await
    {
        Ok(tokens) => tokens,
        Err(e) => {
            tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %e, "an OIDC code could not be exchanged");
            return back_to_gui(&auth, Ended::Provider, Some(state));
        }
    };
    let (token, refresh_token) = (tokens.id_token, tokens.refresh_token);
    let expect = oidc::Expect {
        issuer: &settings.issuer,
        client_id: &settings.client_id,
        nonce: oidc::Nonce::Is(&flow.nonce),
        now_secs: smeltr::leader::warnings::now_millis() / 1000,
    };
    let claims = match auth.oidc.verify(&known, &token, &expect, &settings.groups_claim).await {
        Ok(claims) => claims,
        Err(e) => {
            tracing::warn!(target: "leader.sign_in_refused", error = %e, "an OIDC ID token was refused");
            return refused(&settings.issuer);
        }
    };
    if !admitted(&settings, &claims.groups) {
        tracing::warn!(target: "leader.sign_in_refused", subject = %bounded(&claims.subject), "an OIDC sign-in lacks the required group");
        return refused(&claims.subject);
    }

    let store = auth.store.clone();
    let issuer = settings.issuer.clone();
    let name = fit_display_name(claims.display_name(), &claims.subject);
    let subject = claims.subject.clone();
    let groups = claims.groups.clone();
    let login = fit_login(&claims);
    let signed_in = blocking(move || {
        let new_id = accounts::generate_user_id();
        let created = accounts::event_for(&Actor::system(), "account-create", &new_id, "ok");
        store.oidc_sign_in(
            OidcSignIn {
                issuer: &issuer,
                subject: &subject,
                display_name: &name,
                groups: &groups,
                login: login.as_deref(),
                new_id: &new_id,
                at: smeltr::leader::warnings::now_millis(),
            },
            &created,
        )
    })
    .await;
    let user = match signed_in {
        Ok(user) => user,
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "an OIDC sign-in could not be recorded");
            return back_to_gui(&auth, Ended::Unavailable, Some(state));
        }
    };
    // Let in whatever its roles give, nothing included: the required group decided who may sign in, and what
    // they may do is the gate's to decide on every request.
    if !user.enabled {
        return refused(&user.id);
    }
    let Some(issued) = auth.sessions.issue(&user.id, user.generation, false, Instant::now()) else {
        tracing::warn!(target: "leader.sign_in_refused", "too many sessions are open for an OIDC sign-in");
        return back_to_gui(&auth, Ended::Unavailable, Some(state));
    };
    if let Some(refresh_token) = refresh_token {
        let grant = Grant {
            refresh_token,
            nonce: flow.nonce.clone(),
            issuer: settings.issuer.clone(),
        };
        auth.sessions.give_grant(&issued.token, grant, Instant::now());
    }
    accounts::record(
        auth.store.as_ref(),
        &Actor::person(&user.id, &user.display_name),
        "login",
        &user.id,
        "ok",
    );
    let mut response = see_other("/");
    append_cookie(&mut response, &binding_cookie(&auth, state, "", 0));
    append_cookie(&mut response, &auth.origin.set_cookie(&issued.token));
    response
}

/// Refresh every OIDC session whose grant is `every` old, so what the provider says of each person now reaches the
/// gate. A provider that refuses a grant, or whose answer no longer admits the person, ends that session. A provider
/// that cannot be reached leaves every session as it is, to be tried again one interval later.
pub async fn refresh_due(auth: &Auth, every: Duration) {
    let due = auth.sessions.due_grants(Instant::now(), every);
    if due.is_empty() {
        return;
    }
    let retry_all = |due: Vec<Due>| {
        for d in due {
            auth.sessions.retry_later(&d.key, Instant::now());
        }
    };
    let settings = match usable(auth).await {
        Ok(Some(settings)) => settings,
        Ok(None) => return retry_all(due),
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "the OIDC settings could not be read");
            return retry_all(due);
        }
    };
    let Some(secret) = settings.client_secret.clone() else {
        return retry_all(due);
    };
    let known = match auth.oidc.provider(&settings.issuer).await {
        Ok(known) => known,
        Err(e) => {
            tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %e, "the OIDC provider could not be read");
            return retry_all(due);
        }
    };
    let mut due = due.into_iter();
    while let Some(d) = due.next() {
        // A grant from a provider no longer configured is one this leader cannot spend.
        if d.grant.issuer != settings.issuer {
            auth.sessions.drop_grant(&d.key);
            continue;
        }
        let refreshed = oidc::refresh(
            &known.client,
            &known.provider,
            &settings.client_id,
            &secret,
            &d.grant.refresh_token,
        )
        .await;
        match refreshed {
            Ok(refreshed) => refreshed_one(auth, &settings, &known, d, refreshed).await,
            Err(RefreshFailure::Refused(why)) => end_session(auth, &d, &why),
            Err(RefreshFailure::Unavailable(why)) => {
                // Every grant goes to the same provider, so the rest would wait out the same failure in turn.
                tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %why, "OIDC sessions could not be refreshed");
                auth.sessions.retry_later(&d.key, Instant::now());
                return retry_all(due.collect());
            }
        }
    }
}

/// Believe what a refresh answered for `d`'s session, or end it.
async fn refreshed_one(auth: &Auth, settings: &Settings, known: &Known, d: Due, refreshed: oidc::Refreshed) {
    let Some(token) = refreshed.id_token else {
        // Nothing new said of the person; the grant still stands.
        auth.sessions.renewed(&d.key, refreshed.refresh_token, Instant::now());
        return;
    };
    let expect = oidc::Expect {
        issuer: &settings.issuer,
        client_id: &settings.client_id,
        nonce: oidc::Nonce::AbsentOr(&d.grant.nonce),
        now_secs: smeltr::leader::warnings::now_millis() / 1000,
    };
    let claims = match auth.oidc.verify(known, &token, &expect, &settings.groups_claim).await {
        Ok(claims) => claims,
        Err(Unverified::Refused(why)) => {
            return end_session(auth, &d, &format!("its refreshed ID token was refused: {why}"));
        }
        Err(Unverified::KeysUnavailable(why)) => {
            tracing::warn!(target: "leader.oidc_provider", issuer = %settings.issuer, error = %why, "the OIDC provider's keys could not be read");
            // Renewed rather than retried, so a refresh token the provider rotated is the one spent next time.
            return auth.sessions.renewed(&d.key, refreshed.refresh_token, Instant::now());
        }
    };
    if !admitted(settings, &claims.groups) {
        return end_session(auth, &d, "it no longer holds the required group");
    }
    let store = auth.store.clone();
    let user_id = d.user_id.clone();
    let login = fit_login(&claims);
    let recorded = blocking(move || {
        let Some(identity) = store.oidc_identity_of(&user_id)? else {
            return Ok(false);
        };
        if identity.subject != claims.subject {
            return Ok(false);
        }
        store.oidc_refreshed(&user_id, &claims.groups, login.as_deref())?;
        Ok::<_, StoreError>(true)
    })
    .await;
    match recorded {
        Ok(true) => auth.sessions.renewed(&d.key, refreshed.refresh_token, Instant::now()),
        Ok(false) => end_session(auth, &d, "its refreshed ID token names somebody else"),
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "a refreshed OIDC sign-in could not be recorded");
            // Renewed rather than retried, so a refresh token the provider rotated is the one spent next time.
            auth.sessions.renewed(&d.key, refreshed.refresh_token, Instant::now());
        }
    }
}

/// End `d`'s session because of what its provider said, and say so in the audit.
fn end_session(auth: &Auth, d: &Due, why: &str) {
    if auth.sessions.end(&d.key) {
        tracing::info!(target: "leader.oidc_session_ended", user = %d.user_id, reason = %bounded(why), "an OIDC session ended at its provider");
        accounts::record(auth.store.as_ref(), &Actor::system(), "logout", &d.user_id, "ok");
    }
}

/// Whether `groups` lets a person in under `settings`. An empty required group admits nobody.
fn admitted(settings: &Settings, groups: &[String]) -> bool {
    !settings.required_group.is_empty() && groups.contains(&settings.required_group)
}

/// A name the provider sent, made one a git author line can hold: what git cannot hold becomes a space, as
/// `git.rs` does, so words stay apart. Never refused: the provider decides what people are called, and an
/// administrator can rename one afterwards.
fn fit_display_name(name: &str, subject: &str) -> String {
    let fit = |s: &str| -> String {
        s.chars()
            .map(|c| if unprintable(c) || c == '<' || c == '>' { ' ' } else { c })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(128)
            .collect::<String>()
            .trim_end()
            .to_string()
    };
    let name = fit(name);
    if !name.is_empty() {
        return name;
    }
    match fit(subject) {
        s if s.is_empty() => "OIDC user".to_string(),
        s => s,
    }
}

/// What the provider calls the person, for the roster to tell OIDC accounts apart: their email, else their
/// preferred username, on one line and bounded, since the provider writes it.
fn fit_login(claims: &oidc::Claims) -> Option<String> {
    [&claims.email, &claims.preferred_username]
        .into_iter()
        .flatten()
        .map(|s| {
            s.chars()
                .map(|c| if unprintable(c) { ' ' } else { c })
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .chars()
                .take(256)
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .find(|s| !s.is_empty())
}

/// A character that shows nothing of its own: a control, or one that only steers how the text around it is laid
/// out. A right-to-left override in a provider's name would make the roster show something other than it holds.
fn unprintable(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{061C}' | '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2060}'..='\u{2069}' | '\u{FEFF}')
}

/// Text a caller chose, short enough for a log line or an audit row.
fn bounded(text: &str) -> String {
    const MOST: usize = 64;
    if text.len() <= MOST {
        return text.to_string();
    }
    let cut = (0..=MOST).rev().find(|i| text.is_char_boundary(*i)).unwrap_or(0);
    format!("{}...", &text[..cut])
}

// -- settings -----------------------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsDto {
    enabled: bool,
    issuer: String,
    client_id: String,
    /// Whether a client secret is stored. The secret itself is never sent back.
    client_secret_set: bool,
    groups_claim: String,
    required_group: String,
    /// What to register with the provider as the sign-in redirect.
    redirect_uri: String,
}

#[derive(Default, serde::Serialize)]
pub struct OidcSettings {
    settings: Option<SettingsDto>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutSettings {
    enabled: bool,
    issuer: String,
    client_id: String,
    /// `None` keeps the stored secret while the issuer and client id stay the same; empty removes it.
    client_secret: Option<String>,
    #[serde(default)]
    groups_claim: String,
    #[serde(default)]
    required_group: String,
}

async fn answer(auth: &Auth, error: Option<String>) -> Json<Envelope<OidcSettings>> {
    let store = auth.store.clone();
    let read = blocking(move || Settings::read(store.as_ref()))
        .await
        .map_err(|e| e.to_string());
    Json(match read {
        Ok(settings) => Envelope::with(
            OidcSettings {
                settings: Some(SettingsDto {
                    enabled: settings.enabled,
                    issuer: settings.issuer,
                    client_id: settings.client_id,
                    client_secret_set: settings.client_secret.is_some(),
                    groups_claim: settings.groups_claim,
                    required_group: settings.required_group,
                    redirect_uri: redirect_uri(auth),
                }),
            },
            error.into_iter().map(ApiError::new).collect(),
        ),
        Err(e) => Envelope::failed_one(e),
    })
}

/// `GET /api/settings/oidc`.
pub async fn get_settings(State(auth): State<Auth>) -> Json<Envelope<OidcSettings>> {
    answer(&auth, None).await
}

/// The most a client id, claim name or group name may be. Far beyond any provider's, and short enough to show.
const MOST_TEXT: usize = 256;

/// `PUT /api/settings/oidc`. Recorded with who set it and what changed, in the transaction that sets it; the client
/// secret is sealed before it is stored.
///
/// Turning sign-in on reads the provider and its keys first, so a setting that cannot work is refused here rather
/// than at the next person's sign-in.
pub async fn put_settings(
    State(auth): State<Auth>,
    axum::Extension(actor): axum::Extension<Actor>,
    Json(request): Json<PutSettings>,
) -> Response {
    write_settings(&auth, actor, request).await.into_response()
}

async fn write_settings(auth: &Auth, actor: Actor, request: PutSettings) -> Json<Envelope<OidcSettings>> {
    let issuer = request.issuer.trim().to_string();
    let client_id = request.client_id.trim().to_string();
    let groups_claim = request.groups_claim.trim().to_string();
    let required_group = request.required_group.trim().to_string();
    if !issuer.is_empty()
        && let Err(e) = oidc::check_issuer(&issuer)
    {
        return answer(auth, Some(e)).await;
    }
    if [&client_id, &groups_claim, &required_group]
        .iter()
        .any(|text| text.len() > MOST_TEXT)
    {
        return answer(
            auth,
            Some(format!("a client id, claim or group is at most {MOST_TEXT} bytes")),
        )
        .await;
    }
    if request.client_secret.as_ref().is_some_and(|s| s.len() > 4096) {
        return answer(auth, Some("a client secret is at most 4096 bytes".to_string())).await;
    }

    let one_at_a_time = auth.settings_writes.lock().await;
    let store = auth.store.clone();
    let was = match blocking(move || Settings::read(store.as_ref())).await {
        Ok(was) => was,
        Err(e) => return answer(auth, Some(e.to_string())).await,
    };
    // A secret is for the client it was issued to: another issuer or client id forgets it unless a new one is given.
    let secret = match request.client_secret {
        None if issuer != was.issuer || client_id != was.client_id => Some(String::new()),
        secret => secret,
    };
    let will_hold_secret = match &secret {
        Some(s) => !s.is_empty(),
        None => was.client_secret.is_some(),
    };
    if request.enabled {
        if issuer.is_empty() || client_id.is_empty() || !will_hold_secret || required_group.is_empty() {
            return answer(
                auth,
                Some("OIDC needs an issuer, a client id, a client secret and a required group".to_string()),
            )
            .await;
        }
        if let Err(e) = read_provider(&issuer).await {
            return answer(auth, Some(format!("the provider at {issuer} could not be read: {e}"))).await;
        }
    }

    let claim_now = match groups_claim.as_str() {
        "" => DEFAULT_GROUPS_CLAIM,
        claim => claim,
    };
    let secret_moved = match secret.as_deref() {
        None => false,
        Some("") => was.client_secret.is_some(),
        Some(new) => was.client_secret.as_deref() != Some(new),
    };
    let changed: Vec<&str> = [
        (request.enabled && !was.enabled, "turned on"),
        (!request.enabled && was.enabled, "turned off"),
        (issuer != was.issuer, "issuer"),
        (client_id != was.client_id, "client id"),
        (secret_moved, "client secret"),
        (claim_now != was.groups_claim, "groups claim"),
        (required_group != was.required_group, "required group"),
    ]
    .into_iter()
    .filter_map(|(moved, name)| moved.then_some(name))
    .collect();
    let what = match changed.is_empty() {
        true => "nothing".to_string(),
        false => changed.join(", "),
    };
    let event = accounts::event_for(&actor, "oidc-change", &what, "ok");
    let store = auth.store.clone();
    let written = blocking(move || {
        let mut entries = vec![
            (ENABLED_KEY, request.enabled.to_string()),
            (ISSUER_KEY, issuer),
            (CLIENT_ID_KEY, client_id),
            (GROUPS_CLAIM_KEY, groups_claim),
            (REQUIRED_GROUP_KEY, required_group),
        ];
        if let Some(secret) = secret {
            let stored = if secret.is_empty() {
                secret
            } else {
                store.sealed_setting(SECRET_KEY, &secret)?
            };
            entries.push((SECRET_KEY, stored));
        }
        store.put_settings(&entries, Some(&event))
    })
    .await;
    drop(one_at_a_time);
    let error = written.err().map(|e| e.to_string());
    auth.oidc.forget();
    answer(auth, error).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(required_group: &str) -> Settings {
        Settings {
            enabled: true,
            issuer: "https://id.example.com".to_string(),
            client_id: "smeltr".to_string(),
            client_secret: Some("s".to_string()),
            groups_claim: DEFAULT_GROUPS_CLAIM.to_string(),
            required_group: required_group.to_string(),
        }
    }

    #[test]
    fn no_required_group_admits_nobody() {
        assert!(!admitted(&settings(""), &[]));
        assert!(!admitted(&settings(""), &[String::new()]));
    }

    #[test]
    fn a_required_group_must_be_held_exactly() {
        let held = vec!["smeltr-admins".to_string()];
        assert!(admitted(&settings("smeltr-admins"), &held));
        assert!(!admitted(&settings("smeltr-admin"), &held));
        assert!(!admitted(&settings("smeltr-admins"), &[]));
    }

    /// A newline would end a commit's author line, and angle brackets would end its name. Both become a space, so
    /// the words either side stay apart.
    #[test]
    fn a_provider_name_is_made_one_git_can_hold() {
        assert_eq!(fit_display_name("Ada\nLovelace <x>", "s"), "Ada Lovelace x");
        assert_eq!(fit_display_name(" Ada \t\r\n Lovelace ", "s"), "Ada Lovelace");
        assert_eq!(fit_display_name("  ", "00u1abc"), "00u1abc");
        assert_eq!(fit_display_name("", "\n"), "OIDC user");
        assert_eq!(fit_display_name(&"é".repeat(200), "s").chars().count(), 128);
    }

    fn claims(email: Option<&str>, preferred_username: Option<&str>) -> oidc::Claims {
        oidc::Claims {
            subject: "sub".to_string(),
            name: None,
            preferred_username: preferred_username.map(str::to_string),
            email: email.map(str::to_string),
            groups: Vec::new(),
        }
    }

    /// The email wins over the username, and one that is blank once made printable is as good as none.
    #[test]
    fn a_provider_login_is_the_email_else_the_username() {
        assert_eq!(fit_login(&claims(Some("ada@x"), Some("ada"))).as_deref(), Some("ada@x"));
        assert_eq!(fit_login(&claims(Some(" \n\t"), Some("ada"))).as_deref(), Some("ada"));
        assert_eq!(fit_login(&claims(None, Some(" a\r\nda "))).as_deref(), Some("a da"));
        assert_eq!(fit_login(&claims(None, Some("\u{7}"))), None);
        assert_eq!(fit_login(&claims(None, None)), None);
        assert_eq!(
            fit_login(&claims(Some("ada\u{202E}moc.x@"), None)).as_deref(),
            Some("ada moc.x@")
        );
        assert_eq!(
            fit_login(&claims(Some(&"é".repeat(300)), None))
                .unwrap()
                .chars()
                .count(),
            256
        );
    }
}
