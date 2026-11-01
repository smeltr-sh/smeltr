//! Signing in, signing out, and what the GUI asks to find out whether it is signed in at all.
//!
//! These are the only routes outside the gate. `GET /api/auth/session` answers anonymously and
//! `POST /api/auth/login` has no session to check, so both are public; the rest carry a session and are gated by
//! hand here rather than by the middleware, because a session that must change its password may still use them.
//!
//! Every response is `no-store`. A session DTO in a shared cache is a session handed to whoever gets the cached
//! copy.

use std::time::Instant;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

use smeltr::leader::accounts::{self, Authenticated, PasswordChange};
use smeltr::leader::blocking;
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::sessions::Refused;
use smeltr::leader::store::Actor;

use crate::gate::{
    AccessDto, Auth, access_of, admitted, check_browser_protections, presented_token, refuse, store_unavailable,
    store_unavailable_because,
};

/// What the GUI needs to decide which screen to be on.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDto {
    pub authenticated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// `local` or `oidc`. Only a local account has a password to change.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_source: Option<&'static str>,
    /// What the provider last called an OIDC account, which the GUI shows where a local one shows its username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_login: Option<String>,
    /// Whether this account holds everything, by holding the administrator role. The same as
    /// `access.administrator`.
    pub administrator: bool,
    /// What this account may do, per area, as the gate decides it now.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access: Option<AccessDto>,
    /// This account holds a password it did not choose. The GUI shows the change-password screen and loads
    /// nothing else; the gate refuses everything else regardless.
    pub must_change_password: bool,
    /// Whether AI agents are turned on and their endpoint set, so the agent's screens are offered. Says only that.
    pub agents: bool,
    /// Echoed in [`CSRF_HEADER`] on every unsafe request. Kept in the GUI's memory and never in storage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csrf: Option<String>,
}

impl SessionDto {
    fn anonymous() -> SessionDto {
        SessionDto {
            authenticated: false,
            user_id: None,
            display_name: None,
            username: None,
            auth_source: None,
            provider_login: None,
            administrator: false,
            access: None,
            must_change_password: false,
            agents: false,
            csrf: None,
        }
    }
}

/// A refusal that says how long to wait, in the header a client acts on and in the sentence a person reads.
///
/// `Retry-After` is what makes a countdown honest: without it a client can only guess, and a guess that is short
/// spends the next attempt on nothing.
fn too_many(wait: std::time::Duration, what: &str) -> Response {
    let seconds = wait.as_secs().max(1);
    let message = format!("{what} -- try again in {seconds} seconds");
    (
        [(header::RETRY_AFTER, seconds.to_string())],
        Refusal::new(StatusCode::TOO_MANY_REQUESTS, message),
    )
        .into_response()
}

fn json(dto: SessionDto) -> Response {
    Envelope::ok(dto).into_response()
}

/// `GET /api/auth/session`: who this browser is, or that it is nobody.
///
/// Public, and the one route that answers anonymously rather than refusing. It must tell "signed out" apart from
/// "the store cannot answer": reporting an unavailable store as anonymous would send an operator to a login form
/// that cannot work, over and over.
pub async fn session(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    let Some(token) = presented_token(&headers, auth.origin.cookie_name()) else {
        return json(SessionDto::anonymous());
    };
    let Ok(session) = auth.sessions.resolve(&token, Instant::now()) else {
        return (
            [(header::SET_COOKIE, auth.origin.clear_cookie())],
            json(SessionDto::anonymous()),
        )
            .into_response();
    };

    let store = auth.store.clone();
    let user_id = session.user_id.clone();
    // One blocking hop for both: the account decides whether this session still stands, and the username is what
    // the GUI shows and what a password change has to be told.
    let looked_up = blocking(move || {
        let user = store.get_user(&user_id)?;
        let agents = smeltr::leader::model::on(store.as_ref())?;
        let (username, provider_login, access) = match &user {
            Some(user) => (
                store.username_of(&user.id)?,
                crate::accounts_api::provider_login_of(store.as_ref(), user)?,
                Some(access_of(store.as_ref(), user)?),
            ),
            None => (None, None, None),
        };
        Ok::<_, smeltr::leader::store::StoreError>((user, username, provider_login, access, agents))
    })
    .await;

    let (user, username, provider_login, access, agents) = match looked_up {
        Ok(pair) => pair,
        Err(e) => return store_unavailable_because("the account store could not be read", &e),
    };

    let (Some(user), Some(access)) = (
        user.filter(|u| admitted(u) && u.generation == session.generation),
        access,
    ) else {
        auth.sessions.remove(&token);
        return (
            [(header::SET_COOKIE, auth.origin.clear_cookie())],
            json(SessionDto::anonymous()),
        )
            .into_response();
    };

    json(SessionDto {
        authenticated: true,
        user_id: Some(user.id),
        display_name: Some(user.display_name),
        username,
        auth_source: Some(user.auth_source.as_str()),
        provider_login,
        administrator: access.is_administrator(),
        access: Some(AccessDto::from(&access)),
        must_change_password: session.must_change,
        agents,
        csrf: Some(session.csrf),
    })
}

#[derive(serde::Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// `POST /api/auth/login`.
///
/// Public, because there is no session yet to check a CSRF token against. The `Origin` stands in its place: a
/// cross-site page can cause this request but cannot set that header, so it cannot sign a visitor in as somebody
/// else. A JSON content type is required for the same reason -- a form post can be made cross-site and carries
/// none.
pub async fn login(
    State(auth): State<Auth>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    if !origin.is_some_and(|sent| auth.origin.accepts(sent)) {
        return refuse(StatusCode::FORBIDDEN, "this request came from another origin");
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/json") {
        return refuse(StatusCode::FORBIDDEN, "this request is not JSON");
    }
    let Ok(request) = serde_json::from_slice::<LoginRequest>(&body) else {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "expected a username and a password");
    };

    // Bounded before a permit is taken, so an oversized password costs a length check rather than a hash.
    if request.password.len() > accounts::MAX_PASSWORD_BYTES {
        return refuse(StatusCode::UNAUTHORIZED, GENERIC_REFUSAL);
    }

    // Only the socket's own peer. A forwarded header is whatever the client wrote unless a trusted proxy replaced
    // it, and trusting one unconditionally would let an attacker spend somebody else's bucket -- or, worse, a
    // fresh one per guess. Behind a proxy the peer is the proxy, so there the address bucket is not counted.
    let address = peer.ip().to_string();
    let per_address = (!auth.origin.proxied).then_some(address.as_str());
    let canonical = accounts::canonical_username(&request.username).unwrap_or_else(|| request.username.clone());
    if let Err(wait) = auth.throttle.allow(&canonical, per_address, Instant::now()) {
        return too_many(wait, "too many sign-in attempts");
    }

    // The permit bounds how many hashes run at once; the blocking pool is far larger, so without it a flood would
    // queue jobs that are each holding Argon2's memory.
    let Ok(_permit) = auth.hashes.acquire().await else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "the leader is shutting down");
    };
    let store = auth.store.clone();
    let username = request.username.clone();
    let password = request.password.clone();
    let outcome = blocking(move || accounts::authenticate(store.as_ref(), &username, &password)).await;

    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(e) => return store_unavailable_because("a sign-in could not be verified", &e),
    };

    let Authenticated::Ok {
        user,
        must_change,
        rehash,
    } = outcome
    else {
        // The username, never the password -- and a bounded form of it. An unknown name is whatever was sent, so
        // recording it whole would let an unauthenticated caller write a body-sized row per attempt and fill the
        // leader's disk through the throttle.
        let subject = bounded_subject(&canonical);
        tracing::warn!(target: "leader.sign_in_refused", subject = %subject, source = %address, "a sign-in was refused");
        accounts::record(auth.store.as_ref(), &Actor::anonymous(), "login", &subject, "refused");
        return refuse(StatusCode::UNAUTHORIZED, GENERIC_REFUSAL);
    };

    // The generation the password was verified against. The session is bound to it, so a reset landing from
    // here on refuses the session rather than being inherited by it.
    let mut verified = user.generation;
    if let Some(hash) = rehash {
        // `must_change` is carried over rather than cleared: rewriting a hash at a higher work factor is not the
        // account choosing a password. Clearing it would hand an unrestricted session to the next sign-in with
        // the same temporary one.
        //
        // Conditional on what was verified, so a reset landing while this hashed is not overwritten by a
        // rewrite of the password it replaced. Best effort otherwise: the sign-in succeeded, and failing it
        // because the hash could not be rewritten would lock somebody out over an improvement.
        let store = auth.store.clone();
        let user_id = user.id.clone();
        let expect = verified;
        let written =
            blocking(move || store.set_local_password(&user_id, &hash, must_change, Some(expect), None)).await;
        // Rewriting the password moves the generation, and the write says where to. A write that did not land
        // leaves this where it was, and the read below refuses the sign-in against whatever did.
        if let Ok(Ok(now)) = written {
            verified = now;
        }
    }

    let store = auth.store.clone();
    let user_id = user.id.clone();
    let looked_up = blocking(move || {
        let Some(user) = store.get_user(&user_id)? else {
            return Ok(None);
        };
        let access = access_of(store.as_ref(), &user)?;
        Ok(Some((user, access, smeltr::leader::model::on(store.as_ref())?)))
    })
    .await;
    let (current, access, agents) = match looked_up {
        Ok::<_, smeltr::leader::store::StoreError>(Some(found)) => found,
        _ => return store_unavailable(),
    };
    // A reset, revocation or disable that landed since the hash was checked has moved the account past what
    // was verified. Refused like any other sign-in that no longer matches: the next attempt is judged afresh.
    if current.generation != verified {
        let subject = bounded_subject(&canonical);
        tracing::warn!(target: "leader.sign_in_refused", subject = %subject, source = %address, "a sign-in was refused");
        accounts::record(auth.store.as_ref(), &Actor::anonymous(), "login", &subject, "refused");
        return refuse(StatusCode::UNAUTHORIZED, GENERIC_REFUSAL);
    }

    let Some(issued) = auth
        .sessions
        .issue(&current.id, current.generation, must_change, Instant::now())
    else {
        return refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many sessions are open on this leader",
        );
    };

    accounts::record(
        auth.store.as_ref(),
        &Actor::account(&current.id, &current.display_name),
        "login",
        &current.id,
        "ok",
    );
    (
        [(header::SET_COOKIE, auth.origin.set_cookie(&issued.token))],
        Envelope::ok(SessionDto {
            authenticated: true,
            user_id: Some(current.id),
            display_name: Some(current.display_name),
            username: Some(accounts::canonical_username(&request.username).unwrap_or(canonical)),
            auth_source: Some(current.auth_source.as_str()),
            provider_login: None,
            administrator: access.is_administrator(),
            access: Some(AccessDto::from(&access)),
            must_change_password: must_change,
            agents,
            csrf: Some(issued.session.csrf),
        }),
    )
        .into_response()
}

/// A refused sign-in's subject, short enough that a flood of them cannot fill a disk.
///
/// A name that is already a valid username is recorded as it is. Anything else is truncated at a character
/// boundary and marked, so a reader can tell "this account exists and the password was wrong" from "somebody is
/// sending rubbish" without the row carrying the rubbish.
fn bounded_subject(canonical: &str) -> String {
    const MOST: usize = 64;
    if canonical.len() <= MOST {
        return canonical.to_string();
    }
    let cut = (0..=MOST).rev().find(|i| canonical.is_char_boundary(*i)).unwrap_or(0);
    format!("{}...", &canonical[..cut])
}

/// One answer for an unknown username, a disabled account and a wrong password. Which of the three it was is
/// exactly what an attacker is asking.
const GENERIC_REFUSAL: &str = "that username and password do not match an account that can sign in";

/// `DELETE /api/auth/session`: end this session.
///
/// Gated by hand rather than by the middleware, because an account that must change its password must still be
/// able to leave.
pub async fn logout(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    let Some(token) = presented_token(&headers, auth.origin.cookie_name()) else {
        // Already signed out. Clearing the cookie and answering the same way means a stale tab's sign-out is not
        // an error an operator has to read.
        return cleared(&auth, StatusCode::OK, "signed out");
    };
    let Ok(session) = auth.sessions.resolve(&token, Instant::now()) else {
        return cleared(&auth, StatusCode::OK, "signed out");
    };
    if let Err(response) = check_browser_protections(&auth, &headers, &session.csrf) {
        return *response;
    }
    auth.sessions.remove(&token);
    accounts::record(
        auth.store.as_ref(),
        &Actor::account(&session.user_id, ""),
        "logout",
        &session.user_id,
        "ok",
    );
    cleared(&auth, StatusCode::OK, "signed out")
}

/// An answer that also clears the session cookie. The body follows the status: a success carries `message`, a
/// refusal carries `error`, so a client reads one shape per outcome whether or not a cookie went with it.
fn cleared(auth: &Auth, status: StatusCode, message: &str) -> Response {
    let answer = match status.is_success() {
        true => Envelope::with(serde_json::json!({ "message": message }), status, vec![]),
        false => Envelope::refused(Refusal::new(status, message)),
    };
    ([(header::SET_COOKIE, auth.origin.clear_cookie())], answer).into_response()
}

/// `POST /api/auth/session/activity`: the operator did something, so the idle clock starts again.
///
/// Sent after an interaction and never on a timer, which is the difference between a session that ends when
/// somebody stops working and one that never ends because a tab is open. The manager bounds how often it counts.
pub async fn activity(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    let Some(token) = presented_token(&headers, auth.origin.cookie_name()) else {
        return refuse(StatusCode::UNAUTHORIZED, "sign in to use this");
    };
    let session = match auth.sessions.resolve(&token, Instant::now()) {
        Ok(session) => session,
        Err(Refused::Expired) => return cleared(&auth, StatusCode::UNAUTHORIZED, "your session expired"),
        // A cookie this leader never issued, or one that outlived a restart: cleared so the browser stops
        // presenting it, as the session route does.
        Err(Refused::Unknown) => return cleared(&auth, StatusCode::UNAUTHORIZED, "sign in to use this"),
    };
    if let Err(response) = check_browser_protections(&auth, &headers, &session.csrf) {
        return *response;
    }
    auth.sessions.touch(&token, Instant::now());
    Envelope::done().into_response()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PasswordRequest {
    pub username: String,
    /// Required of a settled session. A session that must change its password was issued by presenting the
    /// one it holds, so it is not asked again.
    #[serde(default)]
    pub current_password: Option<String>,
    pub new_password: String,
}

/// `PUT /api/auth/password`: choose a new password.
///
/// Reachable by a session that must change its password, which is the whole point of it. It ends every session
/// the account holds, including this one, so the operator signs in again with what they just chose -- and so a
/// password somebody else knows stops working everywhere at once.
///
/// A settled session proves the current password first: a session is not a license to lock its owner out.
pub async fn password(State(auth): State<Auth>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    let Some(token) = presented_token(&headers, auth.origin.cookie_name()) else {
        return refuse(StatusCode::UNAUTHORIZED, "sign in to use this");
    };
    let Ok(session) = auth.sessions.resolve(&token, Instant::now()) else {
        return cleared(&auth, StatusCode::UNAUTHORIZED, "your session expired");
    };
    if let Err(response) = check_browser_protections(&auth, &headers, &session.csrf) {
        return *response;
    }
    let Ok(request) = serde_json::from_slice::<PasswordRequest>(&body) else {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "expected a new password");
    };
    if request.new_password.len() > accounts::MAX_PASSWORD_BYTES {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &accounts::PasswordRefused::TooLong.to_string(),
        );
    }

    let changed = if session.must_change {
        let Ok(_permit) = auth.hashes.acquire().await else {
            return refuse(StatusCode::SERVICE_UNAVAILABLE, "the leader is shutting down");
        };
        // Judged again once the permit is held: this path verifies nothing, so a session that queued many of
        // these behind the permit would otherwise spend a hash on each after its first one had ended it.
        if auth.sessions.resolve(&token, Instant::now()).is_err() {
            return cleared(&auth, StatusCode::UNAUTHORIZED, "your session ended -- sign in again");
        }
        let store = auth.store.clone();
        let user_id = session.user_id.clone();
        let generation = session.generation;
        blocking(move || accounts::choose_first_password(store.as_ref(), &user_id, generation, &request.new_password))
            .await
    } else {
        let Some(current) = request.current_password else {
            return refuse(StatusCode::UNPROCESSABLE_ENTITY, "expected your current password");
        };
        if current.len() > accounts::MAX_PASSWORD_BYTES {
            return refuse(StatusCode::UNAUTHORIZED, "that is not your current password");
        }
        // This path verifies a current password, so it is a guessing surface like any other. Without a bucket,
        // anyone holding a session could work through that account's password here at the full rate the hash
        // permits -- around the login throttle that exists to stop exactly this.
        if let Err(wait) = auth.throttle.allow_password_check(&session.user_id, Instant::now()) {
            return too_many(wait, "too many attempts -- your password has not changed");
        }

        let Ok(_permit) = auth.hashes.acquire().await else {
            return refuse(StatusCode::SERVICE_UNAVAILABLE, "the leader is shutting down");
        };
        let store = auth.store.clone();
        let user_id = session.user_id.clone();
        blocking(move || {
            accounts::change_own_password(
                store.as_ref(),
                &user_id,
                &request.username,
                &current,
                &request.new_password,
            )
        })
        .await
    };

    match changed {
        Ok(PasswordChange::Changed) => {
            // Every one of them, this session included. The generation already refuses them; this frees what the
            // process was holding.
            auth.sessions.remove_all_for(&session.user_id);
            cleared(&auth, StatusCode::OK, "password changed -- sign in again")
        }
        // Nothing was written, and the session that asked is worth nothing either: what authorized it has moved.
        Ok(PasswordChange::Superseded) => cleared(
            &auth,
            StatusCode::UNAUTHORIZED,
            "your account changed under you -- sign in again",
        ),
        Ok(PasswordChange::WrongPassword) => refuse(StatusCode::UNAUTHORIZED, "that is not your current password"),
        Ok(PasswordChange::Refused(refused)) => refuse(StatusCode::UNPROCESSABLE_ENTITY, &refused.to_string()),
        Err(e) => store_unavailable_because("a password change did not land", &e),
    }
}
