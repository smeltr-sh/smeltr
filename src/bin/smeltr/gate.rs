//! The operator gate: who is asking, decided by the server on every request.
//!
//! A cookie names a session; a session names an account; the account decides. The account is read from the store
//! on every request rather than trusted from the session, which is what makes disabling somebody, resetting their
//! password or revoking their sessions take effect on the next request instead of at the next sweep.
//!
//! Worker routes never come through here. A worker authenticates with its own token (`leader::require_worker_auth`)
//! and a human cookie is worth nothing on that route family, nor a worker token on this one.
//!
//! What the person may do is decided here as well, from the roles their account holds now and what the route
//! needs (`policy`), so a change to a role reaches everyone holding it on their next request.
//!
//! The browser protections are here too, because they are the same decision: a cookie is attached by the browser
//! whoever caused the request, so an unsafe request must also prove it came from the GUI -- an exact `Origin` and
//! a header carrying the session's own CSRF value, which a cross-site caller can read neither of.

use std::sync::Arc;
use std::time::Instant;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use axum::extract::State;
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

use smeltr::leader::access::{Access, AgentPermissions, ByAgent, Level};
use smeltr::leader::blocking;
use smeltr::leader::sessions::{Refused, Sessions};
use smeltr::leader::store::{Actor, AuthSource, Store, StoreError, UserRecord};
use smeltr::leader::throttle::LoginThrottle;

use crate::policy;

/// The header an unsafe request echoes its session's CSRF value in.
pub const CSRF_HEADER: &str = "x-smeltr-csrf";

/// How many password hashes run at once. Each takes [`smeltr::leader::accounts`]'s memory for as long as it runs,
/// so this number times that memory is what a login flood can occupy.
///
/// Two: enough that one person's login is not queued behind another's, few enough that the leader still answers
/// heartbeats while both run. The blocking pool is not the bound -- it is far larger, and a queue of waiting hash
/// jobs would each be holding memory.
pub const CONCURRENT_HASHES: usize = 2;

/// Everything the gate and the auth routes share.
#[derive(Clone)]
pub struct Auth {
    pub store: Arc<dyn Store>,
    pub sessions: Arc<Sessions>,
    pub throttle: Arc<LoginThrottle>,
    pub hashes: Arc<tokio::sync::Semaphore>,
    /// Held across writing a leader setting and installing it in memory, so the two cannot be applied in opposite
    /// orders by two requests that raced. The store would then disagree with what is being enforced, and only a
    /// restart would reveal which had won.
    pub settings_writes: Arc<tokio::sync::Mutex<()>>,
    pub origin: Arc<Origin>,
    /// Sign-ins through the OpenID provider that are under way, and what is known about the provider.
    pub oidc: Arc<crate::oidc_api::Oidc>,
    /// The turns people's agents are running, and the router their calls go through.
    pub chats: Arc<crate::chat_api::Chats>,
}

impl Auth {
    pub fn new(store: Arc<dyn Store>, origin: Origin, lifetimes: smeltr::leader::sessions::Lifetimes) -> Auth {
        let sessions = Sessions::default();
        sessions.set_lifetimes(lifetimes);
        Auth {
            store,
            sessions: Arc::new(sessions),
            throttle: Arc::new(LoginThrottle::default()),
            hashes: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_HASHES)),
            settings_writes: Arc::new(tokio::sync::Mutex::new(())),
            origin: Arc::new(origin),
            oidc: Arc::new(crate::oidc_api::Oidc::default()),
            chats: Arc::new(crate::chat_api::Chats::default()),
        }
    }
}

/// The origin a browser reaches this leader at, and what follows from it.
#[derive(Debug)]
pub struct Origin {
    /// What an `Origin` header must equal, absent a match in `aliases`. Scheme, host and port, no trailing slash.
    pub value: String,
    /// Other origins this leader is also reachable at and so also accepts, alongside `value`: the rest of a
    /// comma-separated `SMELTR_PUBLIC_ORIGIN`, and the loopback names of a leader whose bind reaches loopback,
    /// when its origin is derived or it serves its own TLS. `resolve_origin` in `main` decides them.
    pub aliases: Vec<String>,
    /// Whether the cookie is marked `Secure`, and therefore whether it takes the `__Host-` prefix. Follows the
    /// origin rather than the socket: a proxy terminating TLS serves this process plain HTTP and the browser
    /// still speaks `https`.
    pub secure: bool,
    /// Whether the socket's peer is a proxy terminating TLS rather than the browser: the origin is `https` and
    /// this process serves plain HTTP. Every browser then arrives from the proxy's address.
    pub proxied: bool,
}

impl Origin {
    /// Whether an `Origin` header naming `sent` is this leader, under `value` or one of `aliases`.
    pub fn accepts(&self, sent: &str) -> bool {
        sent == self.value || self.aliases.iter().any(|alias| alias == sent)
    }

    /// The session cookie's name.
    ///
    /// `__Host-` is a promise the browser enforces: no `Domain`, `Path=/`, and `Secure`. A sibling host under the
    /// same registrable domain then cannot set a cookie this leader would read. It requires `Secure`, so a
    /// leader reached over plain HTTP uses the bare name -- the prefix would be ignored and the cookie dropped.
    pub fn cookie_name(&self) -> &'static str {
        match self.secure {
            true => "__Host-smeltr_session",
            false => "smeltr_session",
        }
    }

    /// `Set-Cookie` for a session that has just been issued.
    ///
    /// SameSite is Lax rather than Strict. Strict withholds the cookie on a link followed from anywhere else, so
    /// an operator clicking a link to a warning would arrive signed out. Lax withholds it from every unsafe
    /// cross-site request, which is what matters, and the CSRF header covers the rest.
    pub fn set_cookie(&self, token: &str) -> String {
        format!(
            "{}={token}; Path=/; HttpOnly; SameSite=Lax{}",
            self.cookie_name(),
            if self.secure { "; Secure" } else { "" }
        )
    }

    /// `Set-Cookie` that ends the session cookie. The same attributes, because a browser matches on them: a
    /// clearing cookie that differs leaves the original in place.
    pub fn clear_cookie(&self) -> String {
        format!(
            "{}=; Path=/; HttpOnly; SameSite=Lax{}; Max-Age=0",
            self.cookie_name(),
            if self.secure { "; Secure" } else { "" }
        )
    }
}

/// What a session whose account must change its password may reach. Everything else answers `403` until it has.
///
/// Signing out is on the list: an operator who cannot finish must still be able to leave.
const RESTRICTED: &[&str] = &["/api/auth/session", "/api/auth/password", "/api/auth/logout"];

/// A refusal, as the JSON body `{"ok":false,"error":message}`, so no route under the gate answers a browser's `fetch`
/// with HTML it will try to parse. `error` reads back as exactly `message`, whatever characters it holds: the GUI
/// shows it as the reason.
pub fn refuse(status: StatusCode, message: &str) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            // A refusal is about this request and this moment; a cache holding one would sign somebody out later.
            (header::CACHE_CONTROL, "no-store"),
        ],
        format!("{{\"ok\":false,\"error\":{}}}", json_string(message)),
    )
        .into_response()
}

/// `message` as a JSON string. Hand-rolled because the alternative is a `serde_json::json!` allocation on a path
/// that runs for every refusal, and the escaping a refusal needs is this short.
fn json_string(message: &str) -> String {
    let mut out = String::with_capacity(message.len() + 2);
    out.push('"');
    for c in message.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The session token this request carries, if any.
///
/// Every `Cookie` header line, not just the first: HTTP/2 lets a client send more than one, and Firefox does --
/// each cookie crumb its own header, rather than the single semicolon-joined line HTTP/1.1 always carried. A
/// second cookie for this origin (a stray one from another tool, an old value under a name this leader no longer
/// uses) then lands in a header line of its own, and `HeaderMap::get` would see only whichever line arrived
/// first -- silently missing the session on every request until the browser's cookie jar for this origin holds
/// exactly one again.
pub fn presented_token(headers: &axum::http::HeaderMap, cookie_name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|cookies| cookies.split(';'))
        .find_map(|c| {
            let (name, value) = c.trim().split_once('=')?;
            (name == cookie_name).then(|| value.to_string())
        })
        .filter(|token| !token.is_empty())
}

/// Whether a method changes anything. `GET` and `HEAD` do not, and there are no state-changing `GET`s here, so
/// this is also the list of methods that skip the CSRF check.
fn is_safe(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// The gate. Every operator route is behind it; the public auth routes are not.
pub async fn require_principal(
    State(auth): State<Auth>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let Some(token) = presented_token(req.headers(), auth.origin.cookie_name()) else {
        return refuse(StatusCode::UNAUTHORIZED, "sign in to use this");
    };

    let session = match auth.sessions.resolve(&token, Instant::now()) {
        Ok(session) => session,
        Err(Refused::Expired) => {
            return (
                [(header::SET_COOKIE, auth.origin.clear_cookie())],
                refuse(StatusCode::UNAUTHORIZED, "your session expired -- sign in again"),
            )
                .into_response();
        }
        Err(Refused::Unknown) => {
            return (
                [(header::SET_COOKIE, auth.origin.clear_cookie())],
                refuse(StatusCode::UNAUTHORIZED, "sign in to use this"),
            )
                .into_response();
        }
    };

    if !is_safe(&method)
        && let Err(response) = check_browser_protections(&auth, req.headers(), &session.csrf)
    {
        return *response;
    }

    // The account as it is now, not as it was when the session was issued. A store that cannot answer makes this
    // request fail, never anonymous: `503` is the one honest answer when identity is unavailable.
    let store = auth.store.clone();
    let user_id = session.user_id.clone();
    let by_agent = req.extensions().get::<ByAgent>().is_some();
    let looked_up = blocking(move || {
        let Some(user) = store.get_user(&user_id)? else {
            return Ok(None);
        };
        let mut access = access_of(store.as_ref(), &user)?;
        // A person's agent uses what they let it, never more.
        if by_agent {
            access = access.for_agent(store.agent_permissions_of(&user.id)?);
        }
        Ok::<_, StoreError>(Some((user, access)))
    })
    .await;
    let user = match looked_up {
        Ok(user) => user,
        Err(e) => {
            tracing::error!(target: "leader.identity_unavailable", error = %e, "the account store could not be read");
            return refuse(StatusCode::SERVICE_UNAVAILABLE, "the account store is unavailable");
        }
    };

    let Some((user, access)) = user.filter(|(u, _)| admitted(u) && u.generation == session.generation) else {
        // Disabled, demoted, or the generation moved under it -- a password change, a reset or a revocation. The
        // session is worth nothing from here on, so it goes rather than being refused one request at a time.
        auth.sessions.remove(&token);
        return (
            [(header::SET_COOKIE, auth.origin.clear_cookie())],
            refuse(
                StatusCode::UNAUTHORIZED,
                "your session is no longer valid -- sign in again",
            ),
        )
            .into_response();
    };

    // Before any authorization an ordinary route would do: an account that has not chosen its own password has
    // no business loading the fleet, and this is what keeps the GUI from asking for it.
    if session.must_change && !RESTRICTED.contains(&path.as_str()) {
        return refuse(
            StatusCode::FORBIDDEN,
            "choose a new password before doing anything else",
        );
    }

    let need = policy::need(&method, &path);
    if !need.met_by(&access) {
        return refuse(StatusCode::FORBIDDEN, &need.refusal());
    }

    // An `Actor`, which is what an audit row and a git author both take: who is asking is one fact, and a second
    // type for it would be a second fact to keep in step. Constructed here and nowhere else, so no field of a
    // request body reaches it.
    // The agent acts as its person and says so wherever the name is shown, in history and on a change alike.
    let actor = if by_agent {
        Actor::persons_agent(&user.id, &user.display_name)
    } else {
        Actor::person(&user.id, &user.display_name)
    };
    req.extensions_mut().insert(actor);
    // Beside the actor, for a handler whose area is in the body or the branch and so could not be judged above.
    req.extensions_mut().insert(access);
    next.run(req).await
}

/// What every answer carries for the browser, GUI and API alike: a served body is never sniffed into a script, no
/// other site frames the GUI against a signed-in administrator, and where a person came from stays inside this
/// origin. Defence beside the session and CSRF checks above, which already refuse a cross-site request.
pub async fn harden(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static("frame-ancestors 'none'"),
    );
    headers.insert(header::REFERRER_POLICY, header::HeaderValue::from_static("same-origin"));
    response
}

/// Whether `user` may hold a session at all. A local account must still be an administrator, which it always is;
/// an OIDC account is let in whatever it holds, and what it may then do is the policy's to decide.
pub fn admitted(user: &UserRecord) -> bool {
    user.enabled && (user.auth_source != AuthSource::Local || user.administrator)
}

/// What `user` holds now: its groups as of its last sign-in, against the roles as they are.
pub fn access_of(store: &dyn Store, user: &UserRecord) -> Result<Access, StoreError> {
    let groups = match user.auth_source {
        // Holds everything whatever the roles say, so neither is read.
        AuthSource::Local => return Ok(Access::of(AuthSource::Local, &[], &[])),
        AuthSource::Oidc => store
            .oidc_identity_of(&user.id)?
            .map(|identity| identity.groups)
            .unwrap_or_default(),
    };
    Ok(Access::of(user.auth_source, &groups, &store.list_roles()?))
}

/// `access`, as a session and the roster report it: `areas` is empty for an administrator, who holds everything.
#[derive(serde::Serialize)]
pub struct AccessDto {
    pub administrator: bool,
    pub areas: std::collections::BTreeMap<String, Level>,
}

impl From<&Access> for AccessDto {
    fn from(access: &Access) -> AccessDto {
        match access {
            Access::Administrator => AccessDto {
                administrator: true,
                areas: Default::default(),
            },
            Access::Areas(areas) => AccessDto {
                administrator: false,
                areas: areas.clone(),
            },
            // The areas its person names, each as the agent permissions leave it. An administrator's agent names
            // only the areas its permissions do, since an administrator names none.
            Access::Agent { held, permissions } => {
                let named: Vec<&String> = match (held.as_ref(), permissions) {
                    (Access::Areas(areas), _) => areas.keys().collect(),
                    (_, AgentPermissions::Upto { areas, .. }) => areas.keys().collect(),
                    (_, AgentPermissions::Everything) => vec![],
                };
                AccessDto {
                    administrator: false,
                    areas: named
                        .into_iter()
                        .filter_map(|area| Some((area.clone(), access.level(area)?)))
                        .collect(),
                }
            }
        }
    }
}

/// The two checks that stop a request the browser attached a cookie to but the GUI did not make.
///
/// Only an origin this leader recognizes as itself is accepted, and the session's own CSRF value must be echoed
/// in a header. A cross-site page can cause a request but can read neither, so it cannot satisfy the second even
/// if a browser let it set the first.
pub fn check_browser_protections(
    auth: &Auth,
    headers: &axum::http::HeaderMap,
    csrf: &str,
) -> Result<(), Box<Response>> {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    match origin {
        // Absent rather than wrong: some clients omit it, and accepting that would be accepting every one of
        // them. A browser sends it on every unsafe request, which is the only kind that reaches here.
        None => {
            return Err(Box::new(refuse(
                StatusCode::FORBIDDEN,
                "this request carried no Origin",
            )));
        }
        Some(sent) if !auth.origin.accepts(sent) => {
            return Err(Box::new(refuse(
                StatusCode::FORBIDDEN,
                "this request came from another origin",
            )));
        }
        Some(_) => {}
    }

    let presented = headers.get(CSRF_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("");
    // An unequal length is refused at once, so timing tells whether the presented value is as long as the token,
    // whose length is fixed, and nothing of the token's bytes.
    if verify_slices_are_equal(presented.as_bytes(), csrf.as_bytes()).is_err() {
        return Err(Box::new(refuse(
            StatusCode::FORBIDDEN,
            "this request carried no valid CSRF token",
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secure() -> Origin {
        Origin {
            value: "https://leader.example.com".into(),
            aliases: Vec::new(),
            secure: true,
            proxied: false,
        }
    }

    fn plain() -> Origin {
        Origin {
            value: "http://127.0.0.1:4000".into(),
            aliases: vec!["http://localhost:4000".into()],
            secure: false,
            proxied: false,
        }
    }

    /// `__Host-` is a promise the browser enforces, and it requires `Secure`. Used without it the cookie is
    /// dropped entirely, so a leader reached over plain HTTP takes the bare name instead.
    #[test]
    fn the_host_prefix_is_used_exactly_when_the_cookie_is_secure() {
        assert_eq!(secure().cookie_name(), "__Host-smeltr_session");
        assert_eq!(plain().cookie_name(), "smeltr_session");
    }

    #[test]
    fn a_secure_cookie_is_host_only_and_path_wide() {
        let cookie = secure().set_cookie("t0ken");
        assert!(cookie.starts_with("__Host-smeltr_session=t0ken"));
        for attribute in ["Path=/", "HttpOnly", "SameSite=Lax", "Secure"] {
            assert!(cookie.contains(attribute), "{cookie}");
        }
        assert!(!cookie.contains("Domain"), "a __Host- cookie names no domain: {cookie}");
    }

    /// A browser matches a clearing cookie on its attributes. One that differs leaves the original in place, so
    /// signing out would not.
    #[test]
    fn clearing_matches_the_cookie_it_clears() {
        for origin in [secure(), plain()] {
            let set = origin.set_cookie("t0ken");
            let clear = origin.clear_cookie();
            assert!(clear.contains("Max-Age=0"), "{clear}");
            for attribute in ["Path=/", "HttpOnly", "SameSite=Lax"] {
                assert_eq!(
                    set.contains(attribute),
                    clear.contains(attribute),
                    "{attribute} differs:\n{set}\n{clear}"
                );
            }
            assert_eq!(set.contains("; Secure"), clear.contains("; Secure"));
        }
    }

    #[test]
    fn the_token_is_read_from_the_cookie_this_leader_sets() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::COOKIE, "__Host-smeltr_session=abc123".parse().unwrap());
        assert_eq!(
            presented_token(&headers, "__Host-smeltr_session").as_deref(),
            Some("abc123")
        );
        assert_eq!(presented_token(&headers, "smeltr_session"), None);
    }

    /// The name has to match exactly. A cookie whose name merely ends with the one being looked for is a
    /// different cookie, and on a shared host it may be somebody else's.
    #[test]
    fn a_cookie_with_a_similar_name_is_not_the_session() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "other_smeltr_session=theirs; smeltr_session_extra=no".parse().unwrap(),
        );
        assert_eq!(presented_token(&headers, "smeltr_session"), None);
    }

    #[test]
    fn the_session_cookie_is_found_among_others() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "theme=dark; smeltr_session=abc123; other=1".parse().unwrap(),
        );
        assert_eq!(presented_token(&headers, "smeltr_session").as_deref(), Some("abc123"));
    }

    /// HTTP/2 lets a client send `Cookie` as more than one header line, and Firefox does -- one crumb per line
    /// rather than the single semicolon-joined line HTTP/1.1 always carried. `HeaderMap::get` sees only the
    /// first line; a session cookie sent on a later one must still be found.
    #[test]
    fn the_session_cookie_is_found_across_several_cookie_header_lines() {
        let mut headers = axum::http::HeaderMap::new();
        headers.append(header::COOKIE, "SID=unrelated-value".parse().unwrap());
        headers.append(header::COOKIE, "smeltr_session=abc123".parse().unwrap());
        assert_eq!(presented_token(&headers, "smeltr_session").as_deref(), Some("abc123"));
    }

    #[test]
    fn an_empty_cookie_is_no_token() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::COOKIE, "smeltr_session=".parse().unwrap());
        assert_eq!(presented_token(&headers, "smeltr_session"), None);
    }

    #[test]
    fn only_methods_that_change_nothing_are_safe() {
        assert!(is_safe(&Method::GET));
        assert!(is_safe(&Method::HEAD));
        for changing in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
            assert!(!is_safe(&changing), "{changing}");
        }
    }

    #[test]
    fn a_refusal_carries_json_and_is_never_cached() {
        let response = refuse(StatusCode::UNAUTHORIZED, "sign in to use this");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }

    /// The GUI parses every refusal and shows its `error`. Any message must reach it whole, whatever it holds.
    #[tokio::test]
    async fn a_refusal_reads_back_as_the_message_it_was_given() {
        for message in [
            "a \"quoted\" \\ thing",
            "two\nlines\r\nand a\ttab",
            "control \u{0} and \u{1} and \u{1f}",
            "naïve \u{1d11e}",
        ] {
            let response = refuse(StatusCode::BAD_REQUEST, message);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).expect("a refusal is JSON");
            assert_eq!(body, serde_json::json!({ "ok": false, "error": message }));
        }
    }

    /// The gate over real routes and a real store, with roles: what an OIDC account holds is what it may do.
    mod access {
        use super::*;

        use std::collections::BTreeMap;
        use std::path::Path;

        use axum::Router;
        use axum::routing::{get, put};
        use smeltr::leader::access::Role;
        use smeltr::leader::accounts;
        use smeltr::leader::sqlite_store::SqliteStore;
        use smeltr::leader::store::OidcSignIn;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const ORIGIN: &str = "http://127.0.0.1:4000";

        fn store() -> Arc<dyn Store> {
            let key = std::env::temp_dir().join(format!(
                "smeltr-gate-access-{}-{}.key",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            Arc::new(SqliteStore::open(Path::new(":memory:"), &key).unwrap())
        }

        /// An OIDC account whose provider last said it holds `groups`.
        fn sign_in(store: &dyn Store, subject: &str, groups: &[&str]) -> UserRecord {
            let groups: Vec<String> = groups.iter().map(|g| g.to_string()).collect();
            let new_id = accounts::generate_user_id();
            let created = accounts::event_for(&Actor::system(), "account-create", &new_id, "ok");
            store
                .oidc_sign_in(
                    OidcSignIn {
                        issuer: "https://idp.example.com",
                        subject,
                        display_name: subject,
                        groups: &groups,
                        login: None,
                        new_id: &new_id,
                        at: 0,
                    },
                    &created,
                )
                .unwrap()
        }

        /// What a handler behind the gate sees: that the gate handed it the person's access.
        async fn reached(access: Option<axum::Extension<Access>>) -> &'static str {
            match access {
                Some(_) => "reached",
                None => "reached without access",
            }
        }

        /// The gate, over stand-ins mounted where the real config routes are: what is under test is who gets to
        /// them, which the gate decides before any handler runs.
        async fn serve(auth: Auth) -> std::net::SocketAddr {
            serve_routes(router(auth)).await
        }

        /// As [`serve`], with every request marked as the leader's agent marks the ones it sends for a person.
        async fn serve_by_agent(auth: Auth) -> std::net::SocketAddr {
            serve_routes(router(auth).layer(axum::Extension(ByAgent))).await
        }

        fn router(auth: Auth) -> Router {
            Router::new()
                .route("/api/areas/:area", get(reached))
                .route("/api/areas/:area/sources", put(reached))
                .route("/api/users", get(reached))
                .route_layer(axum::middleware::from_fn_with_state(auth, require_principal))
        }

        async fn serve_routes(app: Router) -> std::net::SocketAddr {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let at = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            at
        }

        /// One request, carrying `user`'s session and everything an unsafe request must; the status and the body.
        async fn ask(
            at: std::net::SocketAddr,
            auth: &Auth,
            user: &UserRecord,
            method: &str,
            target: &str,
        ) -> (u16, String) {
            let issued = auth
                .sessions
                .issue(&user.id, user.generation, false, Instant::now())
                .unwrap();
            let request = format!(
                "{method} {target} HTTP/1.1\r\nHost: {at}\r\nCookie: {}={}\r\nOrigin: {ORIGIN}\r\n{CSRF_HEADER}: {}\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n",
                auth.origin.cookie_name(),
                issued.token,
                issued.session.csrf,
            );
            let mut stream = tokio::net::TcpStream::connect(at).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut answer = String::new();
            stream.read_to_string(&mut answer).await.unwrap();
            let status = answer.split(' ').nth(1).unwrap().parse().unwrap();
            let body = answer
                .split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default();
            (status, body)
        }

        fn auth(store: Arc<dyn Store>) -> Auth {
            let origin = Origin {
                value: ORIGIN.into(),
                aliases: Vec::new(),
                secure: false,
                proxied: false,
            };
            Auth::new(store, origin, Default::default())
        }

        fn payments_viewer() -> Role {
            Role {
                name: "smeltr-payments".into(),
                administrator: false,
                areas: BTreeMap::from([("payments".to_string(), Level::Viewer)]),
            }
        }

        #[tokio::test]
        async fn a_viewer_reads_their_area_and_nothing_more() {
            let store = store();
            let event = accounts::event_for(&Actor::system(), "role-put", "smeltr-payments", "ok");
            store.put_role(&payments_viewer(), &event).unwrap();
            let viewer = sign_in(store.as_ref(), "vera", &["smeltr-payments", "everyone"]);
            let auth = auth(store);
            let at = serve(auth.clone()).await;

            let (status, body) = ask(at, &auth, &viewer, "GET", "/api/areas/payments").await;
            assert_eq!((status, body.as_str()), (200, "reached"));

            let (status, body) = ask(at, &auth, &viewer, "PUT", "/api/areas/payments/sources").await;
            assert_eq!(status, 403, "{body}");
            assert!(body.contains("this needs editor in payments"), "{body}");

            let (status, body) = ask(at, &auth, &viewer, "GET", "/api/areas/search").await;
            assert_eq!(status, 403, "{body}");
            assert!(body.contains("this needs viewer in search"), "{body}");

            let (status, _) = ask(at, &auth, &viewer, "GET", "/api/users").await;
            assert_eq!(status, 403);
        }

        /// An account whose groups no role names holds nothing, whatever session it still has.
        #[tokio::test]
        async fn an_oidc_account_with_no_role_is_refused_config() {
            let store = store();
            let event = accounts::event_for(&Actor::system(), "role-put", "smeltr-payments", "ok");
            store.put_role(&payments_viewer(), &event).unwrap();
            let nobody = sign_in(store.as_ref(), "nora", &["everyone"]);
            let auth = auth(store);
            let at = serve(auth.clone()).await;

            for target in ["/api/areas/payments", "/api/areas/shared"] {
                let (status, body) = ask(at, &auth, &nobody, "GET", target).await;
                assert_eq!(status, 403, "{target}: {body}");
            }
        }

        /// A role changed is in force on the very next request: access is read from the store every time.
        #[tokio::test]
        async fn a_role_marked_administrator_reaches_everything_from_the_next_request() {
            let store = store();
            let person = sign_in(store.as_ref(), "ada", &["smeltr-admins"]);
            let auth = auth(store.clone());
            let at = serve(auth.clone()).await;

            assert_eq!(ask(at, &auth, &person, "GET", "/api/users").await.0, 403);
            let admins = Role {
                name: "smeltr-admins".into(),
                administrator: true,
                areas: BTreeMap::new(),
            };
            let event = accounts::event_for(&Actor::system(), "role-put", "smeltr-admins", "ok");
            store.put_role(&admins, &event).unwrap();
            assert_eq!(
                ask(at, &auth, &person, "GET", "/api/users").await,
                (200, "reached".to_string())
            );
            assert_eq!(ask(at, &auth, &person, "PUT", "/api/areas/search/sources").await.0, 200);
        }

        fn payments_deployer() -> Role {
            Role {
                name: "smeltr-payments-deploy".into(),
                administrator: false,
                areas: BTreeMap::from([("payments".to_string(), Level::Deployer)]),
            }
        }

        /// A person's agent reads what they read until they give it more, never more than they hold, and takes a
        /// change to its agent permissions from the next request.
        #[tokio::test]
        async fn a_persons_agent_holds_no_more_than_its_permissions() {
            let store = store();
            let event = accounts::event_for(&Actor::system(), "role-put", "r", "ok");
            store.put_role(&payments_deployer(), &event).unwrap();
            let person = sign_in(store.as_ref(), "dee", &["smeltr-payments-deploy"]);
            let auth = auth(store.clone());
            let direct = serve(auth.clone()).await;
            let agent = serve_by_agent(auth.clone()).await;

            assert_eq!(
                ask(direct, &auth, &person, "PUT", "/api/areas/payments/sources")
                    .await
                    .0,
                200
            );
            assert_eq!(ask(agent, &auth, &person, "GET", "/api/areas/payments").await.0, 200);
            let (status, body) = ask(agent, &auth, &person, "PUT", "/api/areas/payments/sources").await;
            assert_eq!(status, 403, "{body}");
            assert!(body.contains("this needs editor in payments"), "{body}");

            let permissions = AgentPermissions::Upto {
                default: Level::Viewer,
                areas: BTreeMap::from([
                    ("payments".to_string(), Level::Editor),
                    ("search".to_string(), Level::Deployer),
                ]),
            };
            let event = accounts::event_for(&Actor::system(), "agent-permissions-put", &person.id, "ok");
            store
                .put_agent_permissions(&person.id, &permissions, &event)
                .unwrap()
                .unwrap();
            assert_eq!(
                ask(agent, &auth, &person, "PUT", "/api/areas/payments/sources").await.0,
                200
            );
            assert_eq!(
                ask(agent, &auth, &person, "GET", "/api/areas/search").await.0,
                403,
                "agent permissions never reach past their person"
            );
        }

        /// An administrator's agent is no administrator until they give it everything.
        #[tokio::test]
        async fn an_administrators_agent_is_an_administrator_only_when_given_everything() {
            let store = store();
            let admins = Role {
                name: "smeltr-admins".into(),
                administrator: true,
                areas: BTreeMap::new(),
            };
            let event = accounts::event_for(&Actor::system(), "role-put", "smeltr-admins", "ok");
            store.put_role(&admins, &event).unwrap();
            let person = sign_in(store.as_ref(), "ada", &["smeltr-admins"]);
            let auth = auth(store.clone());
            let agent = serve_by_agent(auth.clone()).await;

            assert_eq!(ask(agent, &auth, &person, "GET", "/api/users").await.0, 403);
            assert_eq!(ask(agent, &auth, &person, "GET", "/api/areas/search").await.0, 200);
            let event = accounts::event_for(&Actor::system(), "agent-permissions-put", &person.id, "ok");
            store
                .put_agent_permissions(&person.id, &AgentPermissions::Everything, &event)
                .unwrap()
                .unwrap();
            assert_eq!(ask(agent, &auth, &person, "GET", "/api/users").await.0, 200);
        }
    }
}
