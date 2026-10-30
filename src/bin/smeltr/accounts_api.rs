//! Managing accounts: the roster, creating one, disabling one, and putting somebody back in when they are locked
//! out.
//!
//! Behind the gate, so every handler has an [`Actor`] the server built. Who is acting is taken from that and
//! never from the body: a caller naming themselves in JSON is naming nothing.
//!
//! An account signs in with a password, through an OpenID provider, or with a key (`key_accounts_api`). A local
//! account is an administrator. An OIDC account holds what its roles give (`roles_api`), which the roster reports
//! beside it. A key account holds the roles it is given here, or everything when it is made an administrator.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use std::time::Duration;

use smeltr::leader::access::{Access, Role};
use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{
    AccessChange, AccountError, Actor, AuthSource, GivenAccess, NewLocalUser, Store, StoreError, UserRecord,
};
use smeltr::leader::{accounts, blocking, sessions};

use crate::gate::{AccessDto, Auth, refuse, store_unavailable};
use crate::key_accounts_api::{self, KeyDto};

/// An account as the roster shows it. No verifier, no password state beyond whether one must be changed, and
/// never a key or its hash.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountDto {
    pub id: String,
    pub username: Option<String>,
    pub display_name: String,
    pub enabled: bool,
    /// The account's own flag, which an administrator gives a local or key account. What it holds, from this or
    /// its roles, is `access`.
    pub administrator: bool,
    pub auth_source: String,
    /// What the provider last called an OIDC account, since its display name may not tell two people apart.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_login: Option<String>,
    /// The roles this account holds: an OIDC account's are those of its provider groups, as of its last sign-in,
    /// that a role is named for; a local or key account's are those an administrator gave it.
    pub roles: Vec<String>,
    /// Every group the provider named at this account's last sign-in or refresh, a role's or not, exactly as it
    /// named them: what a new role must be called to reach this person. Empty for any other account.
    pub groups: Vec<String>,
    pub access: AccessDto,
    /// A key account's keys in force. Absent for any other account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<KeyDto>>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl AccountDto {
    /// `user` as the roster shows it, given every role there is.
    pub fn of(store: &dyn Store, user: UserRecord, roles: &[Role]) -> Result<AccountDto, StoreError> {
        let username = store.username_of(&user.id)?;
        let identity = match user.auth_source {
            AuthSource::Oidc => store.oidc_identity_of(&user.id)?,
            AuthSource::Local | AuthSource::Key => None,
        };
        let groups = identity.as_ref().map(|i| i.groups.clone()).unwrap_or_default();
        let (named, held) = match user.auth_source {
            AuthSource::Oidc => {
                let held = roles
                    .iter()
                    .filter(|role| groups.contains(&role.name))
                    .map(|role| role.name.clone())
                    .collect();
                (groups.clone(), held)
            }
            AuthSource::Local | AuthSource::Key => {
                let given = store.given_roles(&user.id)?;
                (given.clone(), given)
            }
        };
        let keys = match user.auth_source {
            AuthSource::Key => Some(store.api_keys_of(&user.id)?.into_iter().map(KeyDto::from).collect()),
            AuthSource::Local | AuthSource::Oidc => None,
        };
        let access = Access::of(user.administrator, &named, roles);
        Ok(AccountDto {
            id: user.id,
            username,
            display_name: user.display_name,
            enabled: user.enabled,
            administrator: user.administrator,
            auth_source: user.auth_source.as_str().to_string(),
            provider_login: identity.and_then(|identity| identity.login),
            roles: held,
            groups,
            access: AccessDto::from(&access),
            keys,
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

/// Turn a refused account operation into the status that describes it.
pub fn account_error(e: AccountError) -> Response {
    let status = match e {
        AccountError::UsernameTaken
        | AccountError::LastAdministrator
        | AccountError::HeldByProvider
        | AccountError::Disabled => StatusCode::CONFLICT,
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

/// `GET /api/accounts`: every account, however it signs in, oldest first.
pub async fn list_accounts(State(auth): State<Auth>) -> Response {
    let store = auth.store.clone();
    let listed = store_call(move || {
        let roles = store.list_roles()?;
        store
            .list_users()?
            .into_iter()
            .map(|user| AccountDto::of(store.as_ref(), user, &roles))
            .collect::<Result<Vec<_>, _>>()
    })
    .await;
    match listed {
        Ok(accounts) => Envelope::ok(serde_json::json!({ "accounts": accounts })).into_response(),
        Err(response) => *response,
    }
}

/// A new account, by how it signs in. An OIDC account is made at its first sign-in, never here.
#[derive(serde::Deserialize)]
#[serde(tag = "authSource", rename_all = "lowercase")]
pub enum CreateRequest {
    #[serde(rename_all = "camelCase")]
    Local {
        username: String,
        display_name: String,
        #[serde(default)]
        roles: Vec<String>,
        #[serde(default)]
        administrator: bool,
    },
    #[serde(rename_all = "camelCase")]
    Key {
        display_name: String,
        #[serde(default)]
        roles: Vec<String>,
        #[serde(default)]
        administrator: bool,
    },
}

/// `POST /api/accounts`: a local account, answered with its temporary password, or an enabled key account holding
/// no key yet, answered as the roster shows it. Each holds the roles it is given, or everything as an
/// administrator.
pub async fn create_account(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Json(request): Json<CreateRequest>,
) -> Response {
    match request {
        CreateRequest::Local {
            username,
            display_name,
            roles,
            administrator,
        } => create_user(&auth, &actor, &username, &display_name, roles, administrator).await,
        CreateRequest::Key {
            display_name,
            roles,
            administrator,
        } => key_accounts_api::create_key_account(&auth, &actor, &display_name, roles, administrator).await,
    }
}

/// What a created or reset account is handed over as: a password to pass on once, and never again.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryPasswordDto {
    pub user_id: String,
    pub username: String,
    pub temporary_password: String,
}

/// A local account with a temporary password.
async fn create_user(
    auth: &Auth,
    actor: &Actor,
    username: &str,
    display_name: &str,
    roles: Vec<String>,
    administrator: bool,
) -> Response {
    let Some(username) = accounts::canonical_username(username) else {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "a username starts with a letter and holds letters, digits, dots, dashes and underscores",
        );
    };
    let display_name = match self::display_name(display_name) {
        Ok(name) => name,
        Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, why),
    };
    let roles = match known_roles(auth, roles).await {
        Ok(roles) => roles,
        Err(response) => return *response,
    };

    let password = accounts::generate_password();
    let store = auth.store.clone();
    let id = accounts::generate_user_id();
    // Written with the account, in one transaction: an administrator who exists with no record of who created
    // them is the one case an audit is for.
    let event = accounts::event_for(actor, "account-create", &id, "ok");
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
                GivenAccess {
                    roles: &roles,
                    administrator,
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
            actor,
            "account-create",
            &created_username,
            "refused",
        );
        return account_error(e);
    }
    (
        StatusCode::CREATED,
        Envelope::ok(TemporaryPasswordDto {
            user_id: created_id,
            username: created_username,
            temporary_password: password,
        })
        .into_response(),
    )
        .into_response()
}

/// `roles`, each named once, if every one is a role that exists.
pub async fn known_roles(auth: &Auth, mut roles: Vec<String>) -> Result<Vec<String>, Box<Response>> {
    roles.sort();
    roles.dedup();
    let store = auth.store.clone();
    let existing = store_call(move || store.list_roles()).await?;
    match roles
        .iter()
        .find(|name| !existing.iter().any(|role| &role.name == *name))
    {
        Some(unknown) => Err(Box::new(refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("there is no role named {unknown:?}"),
        ))),
        None => Ok(roles),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PatchRequest {
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
    pub roles: Option<Vec<String>>,
    pub administrator: Option<bool>,
}

/// `PATCH /api/accounts/:id`.
///
/// One field at a time. Each is its own store write, so a request carrying two could land the first and be
/// refused on the second -- a caller told `409` while the display name had already changed. A refusal before
/// anything is written is the honest answer, and no screen asks for two at once.
///
/// Roles and administrator are given to a local or key account. An OIDC account's roles are its provider's.
pub async fn patch_account(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PatchRequest>,
) -> Response {
    let named = [
        request.display_name.is_some(),
        request.enabled.is_some(),
        request.roles.is_some(),
        request.administrator.is_some(),
    ];
    if named.iter().filter(|&&n| n).count() != 1 {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "change one of display name, enabled, roles or administrator in each request",
        );
    }

    let change = match (request.roles, request.administrator) {
        (Some(roles), _) => match known_roles(&auth, roles).await {
            Ok(roles) => Some(AccessChange::Roles(roles)),
            Err(response) => return *response,
        },
        (None, Some(administrator)) => Some(AccessChange::Administrator(administrator)),
        (None, None) => None,
    };
    if let Some(change) = change {
        let event = accounts::event_for(&actor, "account-access", &id, "ok");
        let store = auth.store.clone();
        let target = id.clone();
        match store_call(move || store.change_access(&target, &change, &event)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                accounts::record(auth.store.as_ref(), &actor, "account-access", &id, "refused");
                return account_error(e);
            }
            Err(response) => return *response,
        }
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

    current_account(&auth, &id).await
}

/// The account as it stands, for a caller that just changed it. With its username, since a client replacing its
/// roster entry with this would otherwise lose the login name.
pub async fn current_account(auth: &Auth, id: &str) -> Response {
    let store = auth.store.clone();
    let target = id.to_string();
    let looked_up = store_call(move || {
        let roles = store.list_roles()?;
        match store.get_user(&target)? {
            Some(user) => AccountDto::of(store.as_ref(), user, &roles).map(Some),
            None => Ok(None),
        }
    })
    .await;
    match looked_up {
        Ok(Some(account)) => Envelope::ok(account).into_response(),
        Ok(None) => account_error(AccountError::NoSuchUser),
        Err(response) => *response,
    }
}

/// `POST /api/accounts/:id/reset-password`: give an account a temporary password and end every session it holds.
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
    Envelope::ok(TemporaryPasswordDto {
        user_id: id,
        username,
        temporary_password: password,
    })
    .into_response()
}

/// `POST /api/accounts/:id/revoke-sessions`: end every session an account holds, without touching its password.
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
            Envelope::ok(serde_json::json!({ "events": events })).into_response()
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
        let dto = AccountDto::of(&store, user, &roles).unwrap();
        assert_eq!(dto.roles, vec!["smeltr-payments".to_string()]);
        assert!(!dto.administrator);
        assert!(!dto.access.administrator);
        assert_eq!(dto.access.areas.get("payments"), Some(&Level::Editor));
        assert_eq!(dto.provider_login.as_deref(), Some("vera@example.com"));
    }

    #[test]
    fn a_new_account_names_how_it_signs_in() {
        let local: CreateRequest =
            serde_json::from_str(r#"{"authSource":"local","username":"ada","displayName":"Ada"}"#).unwrap();
        assert!(matches!(local, CreateRequest::Local { .. }));
        let key: CreateRequest = serde_json::from_str(r#"{"authSource":"key","displayName":"exporter"}"#).unwrap();
        assert!(matches!(
            key,
            CreateRequest::Key {
                administrator: false,
                ..
            }
        ));
        assert!(serde_json::from_str::<CreateRequest>(r#"{"authSource":"oidc","displayName":"Vera"}"#).is_err());
        assert!(serde_json::from_str::<CreateRequest>(r#"{"username":"ada","displayName":"Ada"}"#).is_err());
    }

    #[test]
    fn a_change_names_what_it_changes() {
        assert!(serde_json::from_str::<PatchRequest>(r#"{"enabled":false}"#).is_ok());
        assert!(serde_json::from_str::<PatchRequest>(r#"{"admin":true}"#).is_err());
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
    Envelope::ok(SessionSettingsDto::from(auth.sessions.lifetimes())).into_response()
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
    Envelope::ok(SessionSettingsDto::from(lifetimes)).into_response()
}
