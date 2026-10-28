//! Managing accounts: the roster, creating one, disabling one, and putting somebody back in when they are locked
//! out.
//!
//! Behind the gate, so every handler has an [`Actor`] the server built. Who is acting is taken from that and
//! never from the body: a caller naming themselves in JSON is naming nothing.
//!
//! Every account created here is a local one, and so an administrator. An OIDC account holds what its roles give
//! (`roles_api`), which the roster reports beside it.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

use std::time::Duration;

use smeltr::leader::access::{Access, Role};
use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{AccountError, Actor, AuthSource, NewLocalUser, Store, StoreError, UserRecord};
use smeltr::leader::{accounts, blocking, sessions};

use crate::gate::{AccessDto, Auth, refuse, store_unavailable};

/// An account as the roster shows it. No verifier, no password state beyond whether one must be changed.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserDto {
    pub id: String,
    pub username: Option<String>,
    pub display_name: String,
    pub enabled: bool,
    pub administrator: bool,
    pub auth_source: String,
    /// What the provider last called an OIDC account, since its display name may not tell two people apart.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_login: Option<String>,
    /// The roles this account holds: those of its provider groups, as of its last sign-in, that a role is named
    /// for. Empty for a local account, which holds everything without one.
    pub roles: Vec<String>,
    /// Every group the provider named at this account's last sign-in or refresh, a role's or not, exactly as it
    /// named them: what a new role must be called to reach this person. Empty for a local account.
    pub groups: Vec<String>,
    pub access: AccessDto,
    pub created_at: i64,
    pub updated_at: i64,
}

impl UserDto {
    /// `user` as the roster shows it, given every role there is.
    fn of(store: &dyn Store, user: UserRecord, roles: &[Role]) -> Result<UserDto, StoreError> {
        let username = store.username_of(&user.id)?;
        let identity = match user.auth_source {
            AuthSource::Oidc => store.oidc_identity_of(&user.id)?,
            AuthSource::Local => None,
        };
        let groups = identity.as_ref().map(|i| i.groups.clone()).unwrap_or_default();
        let held = roles
            .iter()
            .filter(|role| groups.contains(&role.name))
            .map(|role| role.name.clone())
            .collect();
        let access = Access::of(user.auth_source, &groups, roles);
        Ok(UserDto {
            id: user.id,
            username,
            display_name: user.display_name,
            enabled: user.enabled,
            administrator: access.is_administrator(),
            auth_source: user.auth_source.as_str().to_string(),
            provider_login: identity.and_then(|identity| identity.login),
            roles: held,
            groups,
            access: AccessDto::from(&access),
            created_at: user.created_at,
            updated_at: user.updated_at,
        })
    }
}

/// What the provider last called `user`, if an OpenID provider signs it in.
pub fn provider_login_of(store: &dyn Store, user: &UserRecord) -> Result<Option<String>, StoreError> {
    if user.auth_source != AuthSource::Oidc {
        return Ok(None);
    }
    Ok(store.oidc_identity_of(&user.id)?.and_then(|identity| identity.login))
}

pub fn no_store<T: serde::Serialize>(body: T) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Envelope::ok(body)).into_response()
}

/// Turn a refused account operation into the status that describes it.
fn account_error(e: AccountError) -> Response {
    let status = match e {
        AccountError::UsernameTaken | AccountError::LastAdministrator => StatusCode::CONFLICT,
        AccountError::NoSuchUser => StatusCode::NOT_FOUND,
        AccountError::WrongAuthSource | AccountError::Superseded => StatusCode::CONFLICT,
    };
    refuse(status, &e.to_string())
}

fn unavailable(e: StoreError) -> Response {
    tracing::error!(target: "leader.identity_unavailable", error = %e, "an account operation could not be completed");
    store_unavailable()
}

/// A store call on the pool, however it fails answered as unavailable.
pub async fn store_call<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, Box<Response>> {
    blocking(f).await.map_err(|e| Box::new(unavailable(e)))
}

/// `GET /api/users`.
pub async fn list_users(State(auth): State<Auth>) -> Response {
    let store = auth.store.clone();
    let listed = store_call(move || {
        let roles = store.list_roles()?;
        store
            .list_users()?
            .into_iter()
            .map(|user| UserDto::of(store.as_ref(), user, &roles))
            .collect::<Result<Vec<_>, _>>()
    })
    .await;
    match listed {
        Ok(users) => no_store(serde_json::json!({ "users": users })),
        Err(response) => *response,
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRequest {
    pub username: String,
    pub display_name: String,
}

/// What a created or reset account is handed over as: a password to pass on once, and never again.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryPasswordDto {
    pub user_id: String,
    pub username: String,
    pub temporary_password: String,
}

/// `POST /api/users`: create a local administrator with a temporary password.
pub async fn create_user(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Json(request): Json<CreateRequest>,
) -> Response {
    let Some(username) = accounts::canonical_username(&request.username) else {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "a username starts with a letter and holds letters, digits, dots, dashes and underscores",
        );
    };
    let display_name = match display_name(&request.display_name) {
        Ok(name) => name,
        Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, why),
    };

    let password = accounts::generate_password();
    let store = auth.store.clone();
    let id = accounts::generate_user_id();
    // Written with the account, in one transaction: an administrator who exists with no record of who created
    // them is the one case an audit is for.
    let event = accounts::event_for(&actor, "account-create", &id, "ok");
    // The permit bounds how many hashes run at once, here as on every sign-in: a script creating many accounts
    // would otherwise fill the blocking pool with jobs each holding Argon2's memory.
    let Ok(_permit) = auth.hashes.acquire().await else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "the leader is shutting down");
    };
    let (created_id, created_username, outcome) = (id.clone(), username.clone(), {
        let password = password.clone();
        store_call(move || {
            let hash = accounts::hash_password(&password)?;
            store.create_local_user(
                NewLocalUser {
                    id: &id,
                    username: &username,
                    display_name: &display_name,
                    password_hash: &hash,
                    must_change: true,
                },
                &event,
            )
        })
        .await
    });
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(response) => return *response,
    };
    if let Err(e) = outcome {
        accounts::record(
            auth.store.as_ref(),
            &actor,
            "account-create",
            &created_username,
            "refused",
        );
        return account_error(e);
    }
    (
        StatusCode::CREATED,
        no_store(TemporaryPasswordDto {
            user_id: created_id,
            username: created_username,
            temporary_password: password,
        }),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchRequest {
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
}

/// `PATCH /api/users/:id`.
///
/// One field at a time. A rename and an enable are separate store writes, so a request carrying both could land
/// the first and be refused on the second -- a caller told `409` while the display name had already changed. A
/// refusal before anything is written is the honest answer, and no screen asks for both at once.
pub async fn patch_user(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PatchRequest>,
) -> Response {
    match (&request.display_name, request.enabled) {
        (None, None) => {
            return refuse(
                StatusCode::UNPROCESSABLE_ENTITY,
                "name a display name or an enabled state",
            );
        }
        (Some(_), Some(_)) => {
            return refuse(
                StatusCode::UNPROCESSABLE_ENTITY,
                "change a display name or an enabled state, not both in one request",
            );
        }
        _ => {}
    }

    if let Some(name) = &request.display_name {
        let name = match display_name(name) {
            Ok(name) => name,
            Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, why),
        };
        let store = auth.store.clone();
        let target = id.clone();
        match store_call(move || store.set_display_name(&target, &name)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return account_error(e),
            Err(response) => return *response,
        }
        accounts::record(auth.store.as_ref(), &actor, "account-rename", &id, "ok");
    }

    if let Some(enabled) = request.enabled {
        let action = if enabled { "account-enable" } else { "account-disable" };
        let event = accounts::event_for(&actor, action, &id, "ok");
        let store = auth.store.clone();
        let target = id.clone();
        match store_call(move || store.set_user_enabled(&target, enabled, &event)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                accounts::record(auth.store.as_ref(), &actor, action, &id, "refused");
                return account_error(e);
            }
            Err(response) => return *response,
        }
        // Disabling moved the generation, so no session issued before it is admitted again -- not even after the
        // account is re-enabled. This frees what the process is still holding for them.
        if !enabled {
            auth.sessions.remove_all_for(&id);
        }
    }

    current_user(&auth, &id).await
}

/// The account as it stands, for a caller that just changed it. With its username, since a client replacing its
/// roster entry with this would otherwise lose the login name.
async fn current_user(auth: &Auth, id: &str) -> Response {
    let store = auth.store.clone();
    let target = id.to_string();
    let looked_up = store_call(move || match store.get_user(&target)? {
        Some(user) => UserDto::of(store.as_ref(), user, &store.list_roles()?).map(Some),
        None => Ok(None),
    })
    .await;
    match looked_up {
        Ok(Some(user)) => no_store(user),
        Ok(None) => account_error(AccountError::NoSuchUser),
        Err(response) => *response,
    }
}

/// `POST /api/users/:id/reset-password`: give an account a temporary password and end every session it holds.
pub async fn reset_password(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    let password = accounts::generate_password();
    let store = auth.store.clone();
    let target = id.clone();
    let chosen = password.clone();
    let event = accounts::event_for(&actor, "password-reset", &id, "ok");
    let Ok(_permit) = auth.hashes.acquire().await else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "the leader is shutting down");
    };
    let outcome = store_call(move || {
        let hash = accounts::hash_password(&chosen)?;
        store
            .set_local_password(&target, &hash, true, None, Some(&event))
            .map(|r| r.map(|_| ()))
    })
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return account_error(e),
        Err(response) => return *response,
    }
    auth.sessions.remove_all_for(&id);

    let store = auth.store.clone();
    let target = id.clone();
    let username = match store_call(move || store.username_of(&target)).await {
        Ok(username) => username.unwrap_or_default(),
        Err(response) => return *response,
    };
    no_store(TemporaryPasswordDto {
        user_id: id,
        username,
        temporary_password: password,
    })
}

/// `POST /api/users/:id/revoke-sessions`: end every session an account holds, without touching its password.
pub async fn revoke_sessions(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
) -> Response {
    let event = accounts::event_for(&actor, "revoke-sessions", &id, "ok");
    let store = auth.store.clone();
    let target = id.clone();
    match store_call(move || store.revoke_sessions(&target, &event)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return account_error(e),
        Err(response) => return *response,
    }
    let ended = auth.sessions.remove_all_for(&id);
    Envelope::ok(serde_json::json!({ "ended": ended })).into_response()
}

/// `GET /api/security-events`: what has happened to accounts, newest first.
pub async fn list_security_events(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    let _ = headers;
    const LIMIT: usize = 500;
    let store = auth.store.clone();
    match store_call(move || store.list_security_events(LIMIT)).await {
        Ok(events) => {
            let events: Vec<_> = events
                .into_iter()
                .map(|e| {
                    serde_json::json!({
                        "id": e.id,
                        "at": e.at,
                        "actor": e.actor,
                        "action": e.action,
                        "target": e.target,
                        "outcome": e.outcome,
                    })
                })
                .collect();
            no_store(serde_json::json!({ "events": events }))
        }
        Err(response) => *response,
    }
}

/// A display name, checked. `Err` carries the sentence to answer with.
///
/// Unicode is fine -- it is what a person is called. What delimits a git author line is not: a name reaches
/// history through one, and a name git cannot hold is a name history would report as something else.
pub fn display_name(raw: &str) -> Result<String, &'static str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().count() > 128 {
        return Err("a display name is between 1 and 128 characters");
    }
    if trimmed.chars().any(|c| c.is_control()) {
        return Err("a display name holds no control characters");
    }
    if trimmed.contains(['<', '>']) {
        return Err("a display name holds no angle brackets");
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_display_name_takes_any_script() {
        assert_eq!(display_name("  Élodie Bernard  ").unwrap(), "Élodie Bernard");
        assert_eq!(display_name("大野 智").unwrap(), "大野 智");
    }

    /// A newline in a display name would end the author line of a commit and start whatever came next.
    #[test]
    fn a_display_name_holds_no_control_characters() {
        for bad in ["a\nb", "a\rb", "a\tb", "a\u{0}b"] {
            assert!(display_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_display_name_is_bounded() {
        assert!(display_name("").is_err());
        assert!(display_name("   ").is_err());
        assert!(display_name(&"a".repeat(128)).is_ok());
        assert!(display_name(&"a".repeat(129)).is_err());
    }

    /// The roster names the roles an OIDC account holds -- the provider groups a role exists for -- and what
    /// they give it.
    #[test]
    fn an_oidc_account_is_listed_with_its_roles_and_access() {
        use smeltr::leader::access::Level;
        use smeltr::leader::store::OidcSignIn;
        let key = std::env::temp_dir().join(format!("smeltr-users-api-roles-{}.key", std::process::id()));
        let store = smeltr::leader::sqlite_store::SqliteStore::open(std::path::Path::new(":memory:"), &key).unwrap();
        let groups = vec!["smeltr-payments".to_string(), "everyone".to_string()];
        let created = accounts::event_for(&Actor::system(), "account-create", "u_oidc", "ok");
        let user = store
            .oidc_sign_in(
                OidcSignIn {
                    issuer: "https://idp.example.com",
                    subject: "vera",
                    display_name: "Vera",
                    groups: &groups,
                    login: Some("vera@example.com"),
                    new_id: "u_oidc",
                    at: 0,
                },
                &created,
            )
            .unwrap();
        let roles = [
            Role {
                name: "smeltr-payments".into(),
                administrator: false,
                areas: [("payments".to_string(), Level::Editor)].into(),
            },
            Role {
                name: "smeltr-admins".into(),
                administrator: true,
                areas: Default::default(),
            },
        ];
        let dto = UserDto::of(&store, user, &roles).unwrap();
        assert_eq!(dto.roles, vec!["smeltr-payments".to_string()]);
        assert!(!dto.administrator);
        assert!(!dto.access.administrator);
        assert_eq!(dto.access.areas.get("payments"), Some(&Level::Editor));
        assert_eq!(dto.provider_login.as_deref(), Some("vera@example.com"));
    }

    /// A duplicate username and a last-administrator refusal are both conflicts; a missing account is not.
    #[test]
    fn an_account_refusal_carries_the_status_that_describes_it() {
        assert_eq!(
            account_error(AccountError::UsernameTaken).status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            account_error(AccountError::LastAdministrator).status(),
            StatusCode::CONFLICT
        );
        assert_eq!(account_error(AccountError::NoSuchUser).status(), StatusCode::NOT_FOUND);
        assert_eq!(
            account_error(AccountError::WrongAuthSource).status(),
            StatusCode::CONFLICT
        );
    }
}

// -- system settings --------------------------------------------------------

/// The Sessions tab of Admin -> System. Seconds on the wire, because a unit in the value is a second thing to
/// parse; the form converts to the minutes and hours a person thinks in.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSettingsDto {
    pub idle_seconds: u64,
    pub absolute_seconds: u64,
    /// What the form may offer, so the bounds live in one place rather than being restated in the GUI and drifting.
    pub min_seconds: u64,
    pub max_seconds: u64,
}

impl From<sessions::Lifetimes> for SessionSettingsDto {
    fn from(l: sessions::Lifetimes) -> SessionSettingsDto {
        SessionSettingsDto {
            idle_seconds: l.idle.as_secs(),
            absolute_seconds: l.absolute.as_secs(),
            min_seconds: sessions::MIN_LIFETIME.as_secs(),
            max_seconds: sessions::MAX_LIFETIME.as_secs(),
        }
    }
}

/// `GET /api/settings/sessions`: the deadlines in force.
pub async fn get_session_settings(State(auth): State<Auth>) -> Response {
    no_store(SessionSettingsDto::from(auth.sessions.lifetimes()))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutSessionSettings {
    pub idle_seconds: u64,
    pub absolute_seconds: u64,
}

/// `PUT /api/settings/sessions`.
pub async fn put_session_settings(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Json(request): Json<PutSessionSettings>,
) -> Response {
    let lifetimes = match sessions::Lifetimes::new(
        Duration::from_secs(request.idle_seconds),
        Duration::from_secs(request.absolute_seconds),
    ) {
        Ok(lifetimes) => lifetimes,
        Err(e) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };

    // One writer at a time, across the durable write and the in-memory install together. SQLite serializes the
    // two commits on its own, but the tasks awaiting them can resume in the other order -- so the leader would
    // enforce one pair while the store, and the next restart, held the other.
    let _ordered = auth.settings_writes.lock().await;

    // Durable first, carrying the audit row in the same transaction: a leader that enforced a deadline it would
    // forget on restart is the outcome worth avoiding, and a change to how long a session lives must not land
    // without the row saying who made it.
    let event = accounts::event_for(
        &actor,
        "session-timeouts",
        "leader",
        &format!(
            "idle={}s absolute={}s",
            lifetimes.idle.as_secs(),
            lifetimes.absolute.as_secs()
        ),
    );
    let store = auth.store.clone();
    if let Err(e) = blocking(move || lifetimes.write(store.as_ref(), Some(&event))).await {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    auth.sessions.set_lifetimes(lifetimes);
    no_store(SessionSettingsDto::from(lifetimes))
}
