//! The accounts fuzzer. An administrator, signed in however an administrator can be, makes local and key accounts,
//! and a provider signs OIDC accounts in. Accounts are renamed, given roles, the administrator role among them,
//! disabled and enabled, have their password reset and their sessions ended, and key accounts are given keys that
//! expire or not, and have them revoked; roles are given other groups; each account sets what its agent may use.
//! Between those, accounts call the leader with a session or a key, as themselves or through their agent.
//!
//! However it goes, the leader answers as the account model says (`docs/architecture/security.md`, Identity and
//! access), which [`Model`] holds apart from the leader's own code: an account reaches what it holds and nothing
//! more, however it signs in; its agent holds no more than it, narrowed by its agent permissions; a change is made or
//! refused for the reason the model gives, and recorded under whoever made it; and the roster shows every account
//! as the model holds it.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::routing::{delete, get, patch, post, put};
use serde_json::{Value, json};
use smeltr::leader::access::{ADMINISTRATOR, AgentPermissions, ByAgent, Level, Role};
use smeltr::leader::sqlite_store::SqliteStore;
use smeltr::leader::store::{Actor, NewApiKey, NewLocalUser, OidcSignIn, Store};
use smeltr::leader::{accounts, auth as tokens};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::fuzz::{self, Coverage, Rng};
use crate::gate::{self, Auth, CSRF_HEADER, Origin};
use crate::{accounts_api, key_accounts_api, roles_api};

const ORIGIN: &str = "http://127.0.0.1:4000";
const DAY_MILLIS: i64 = 86_400_000;

/// Areas a request names: `shared`, and two groups.
const AREAS: [&str; 3] = ["shared", "payments", "search"];

/// A role: its name, the groups it starts given by, and what it gives by area.
type RoleDef = (&'static str, &'static [&'static str], &'static [(&'static str, Level)]);

/// The roles there are, the built-in one among them.
const ROLES: [RoleDef; 3] = [
    ("viewers", &["g-viewers", "g-staff"], &[("payments", Level::Viewer)]),
    (
        "editors",
        &["g-editors"],
        &[("payments", Level::Editor), ("search", Level::Viewer)],
    ),
    (ADMINISTRATOR, &["g-admins"], &[]),
];

/// What a provider may say a person belongs to, and a role's groups may name: groups roles start with, one no role
/// starts with, and a role's own name, which gives nothing unless a role lists it.
const GROUPS: [&str; 6] = ["g-viewers", "g-staff", "g-editors", "g-admins", "everyone", "viewers"];

/// What an administrator may give: each role, and a name no role has.
const GIVEN: [&str; 4] = ["viewers", "editors", ADMINISTRATOR, "ghost"];

/// Display names, which may repeat.
const NAMES: [&str; 3] = ["Ada", "Bo", "nightly export"];

const OPTIONS: &[&str] = &[
    "actor-local",
    "actor-oidc",
    "actor-key",
    "create-local",
    "create-key-account",
    "create-unknown-role-refused",
    "oidc-first-sign-in",
    "oidc-sign-in-again",
    "rename",
    "roles-given",
    "roles-unknown-refused",
    "roles-oidc-refused",
    "roles-last-administrator-refused",
    "administrator-given",
    "administrator-taken",
    "administrator-oidc-refused",
    "administrator-last-refused",
    "role-groups-changed",
    "role-name-is-no-group",
    "administrator-areas-refused",
    "administrator-delete-refused",
    "disable",
    "disable-last-administrator-refused",
    "enable",
    "reset-password",
    "reset-password-not-local-refused",
    "revoke-sessions",
    "key-never-expires",
    "key-expires",
    "key-zero-days-refused",
    "key-not-key-account-refused",
    "key-disabled-account-refused",
    "key-already-expired",
    "key-revoked",
    "key-revoked-again-refused",
    "agent-administrator",
    "agent-administrator-gives-nothing",
    "agent-upto",
    "probe-local",
    "probe-oidc",
    "probe-key",
    "probe-key-account-session-refused",
    "probe-ended-session-refused",
    "probe-disabled-refused",
    "probe-revoked-key-refused",
    "probe-expired-key-refused",
    "probe-unknown-key-refused",
    "probe-by-agent",
    "probe-allowed",
    "probe-forbidden",
    "roster",
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Local,
    Oidc,
    Key,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Local => "local",
            Kind::Oidc => "oidc",
            Kind::Key => "key",
        }
    }
}

struct Key {
    id: String,
    secret: String,
    created_at: i64,
    expires_at: Option<i64>,
    revoked: bool,
}

impl Key {
    fn expired(&self) -> bool {
        self.expires_at.is_some_and(|at| at <= now())
    }
}

struct Session {
    token: String,
    csrf: String,
    /// Not ended since it was issued, as far as the model knows.
    live: bool,
}

struct Account {
    id: String,
    kind: Kind,
    display_name: String,
    username: Option<String>,
    subject: Option<String>,
    enabled: bool,
    /// What an administrator gave a local or key account.
    roles: Vec<String>,
    /// What the provider last said an OIDC account belongs to.
    groups: Vec<String>,
    keys: Vec<Key>,
    agent: AgentPermissions,
    /// The one session the fuzzer holds for it, so issuing another never pushes it out.
    session: Option<Session>,
}

/// What an account holds: everything, or a level in some areas.
enum Held {
    Everything,
    Areas(BTreeMap<&'static str, Level>),
}

/// The account model, apart from the leader's code.
struct Model {
    accounts: Vec<Account>,
    /// Each role's groups as they stand, by its place in [`ROLES`].
    groups: [Vec<String>; 3],
    made: usize,
}

impl Model {
    fn new() -> Model {
        Model {
            accounts: Vec::new(),
            groups: ROLES.map(|(_, groups, _)| groups.iter().map(|g| g.to_string()).collect()),
            made: 0,
        }
    }

    /// The roles `account` holds: those its groups give an OIDC account, those given any other.
    fn roles_of(&self, account: &Account) -> Vec<&'static str> {
        ROLES
            .iter()
            .zip(&self.groups)
            .filter(|((name, ..), groups)| match account.kind {
                Kind::Oidc => groups.iter().any(|g| account.groups.contains(g)),
                Kind::Local | Kind::Key => account.roles.iter().any(|r| r == name),
            })
            .map(|((name, ..), _)| *name)
            .collect()
    }

    fn held(&self, account: &Account) -> Held {
        let named = self.roles_of(account);
        if named.contains(&ADMINISTRATOR) {
            return Held::Everything;
        }
        let mut areas = BTreeMap::new();
        for (name, _, gives) in ROLES {
            if !named.contains(&name) {
                continue;
            }
            for (area, level) in gives {
                let at = areas.entry(*area).or_insert(*level);
                *at = (*at).max(*level);
            }
        }
        Held::Areas(areas)
    }

    /// The level `account`, or its agent, holds in `area`.
    fn level(&self, account: &Account, by_agent: bool, area: &str) -> Option<Level> {
        let level = match self.held(account) {
            Held::Everything => Some(Level::Deployer),
            Held::Areas(areas) => match areas.get(area) {
                // Anyone holding an area reads shared, since a group is read merged with it.
                None if area == "shared" && !areas.is_empty() => Some(Level::Viewer),
                named => named.copied(),
            },
        }?;
        if by_agent && !self.administers(account, true) {
            let agent = &account.agent;
            return Some(level.min(*agent.areas.get(area).unwrap_or(&agent.default)));
        }
        Some(level)
    }

    /// Whether `account`, or its agent, is an administrator: an agent only when its permissions name the role too.
    fn administers(&self, account: &Account, by_agent: bool) -> bool {
        matches!(self.held(account), Held::Everything) && (!by_agent || account.agent.administrator)
    }

    /// The enabled local administrators but `id`, who keep a way in.
    fn other_local_administrators(&self, id: &str) -> usize {
        self.accounts
            .iter()
            .filter(|a| a.id != id && a.kind == Kind::Local && a.enabled && a.roles.iter().any(|r| r == ADMINISTRATOR))
            .count()
    }

    /// Every session of `id` ends.
    fn end_sessions(&mut self, id: &str) {
        if let Some(session) = self.account_mut(id).session.as_mut() {
            session.live = false;
        }
    }

    fn account_mut(&mut self, id: &str) -> &mut Account {
        self.accounts.iter_mut().find(|a| a.id == id).unwrap()
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// How a request says who sends it.
enum Credential {
    Session { token: String, csrf: String },
    Key(String),
}

/// The leader under test: the account routes, and stand-ins at the config routes a request's access is judged
/// on, behind the gate; once as the caller, and once with every request marked as an agent's.
struct Leader {
    auth: Auth,
    store: Arc<dyn Store>,
    at: std::net::SocketAddr,
    by_agent: std::net::SocketAddr,
}

async fn reached() -> &'static str {
    "reached"
}

fn routes(auth: Auth) -> Router {
    Router::new()
        .route("/api/areas/:area", get(reached))
        .route("/api/areas/:area/sources", put(reached))
        .route(
            "/api/accounts",
            get(accounts_api::list_accounts).post(accounts_api::create_account),
        )
        .route("/api/accounts/:id", patch(accounts_api::patch_account))
        .route("/api/accounts/:id/reset-password", post(accounts_api::reset_password))
        .route("/api/accounts/:id/revoke-sessions", post(accounts_api::revoke_sessions))
        .route("/api/accounts/:id/keys", post(key_accounts_api::create_key))
        .route("/api/accounts/:id/keys/:key", delete(key_accounts_api::revoke_key))
        .route(
            "/api/roles/:name",
            put(roles_api::put_role).delete(roles_api::delete_role),
        )
        .with_state(auth.clone())
        .route_layer(axum::middleware::from_fn_with_state(auth, gate::require_principal))
}

async fn serve(app: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    at
}

impl Leader {
    async fn start() -> Leader {
        let key = std::env::temp_dir().join(format!(
            "smeltr-accounts-fuzz-{}-{}.key",
            std::process::id(),
            tokens::generate_secret()
        ));
        let store: Arc<dyn Store> = Arc::new(SqliteStore::open(Path::new(":memory:"), &key).unwrap());
        let _ = std::fs::remove_file(&key);
        let origin = Origin {
            value: ORIGIN.into(),
            aliases: Vec::new(),
            secure: false,
            proxied: false,
        };
        let auth = Auth::new(store.clone(), origin, Default::default());
        let at = serve(routes(auth.clone())).await;
        let by_agent = serve(routes(auth.clone()).layer(axum::Extension(ByAgent))).await;
        Leader {
            auth,
            store,
            at,
            by_agent,
        }
    }

    /// One request; the status and the body as JSON, or null.
    async fn send(
        &self,
        by_agent: bool,
        credential: &Credential,
        method: &str,
        target: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let at = if by_agent { self.by_agent } else { self.at };
        let body = body.map(Value::to_string).unwrap_or_default();
        let who = match credential {
            Credential::Session { token, csrf } => format!(
                "Cookie: {}={token}\r\nOrigin: {ORIGIN}\r\n{CSRF_HEADER}: {csrf}\r\n",
                self.auth.origin.cookie_name()
            ),
            Credential::Key(key) => format!("Authorization: Bearer {key}\r\n"),
        };
        let request = format!(
            "{method} {target} HTTP/1.1\r\nHost: {at}\r\n{who}Content-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut stream = tokio::net::TcpStream::connect(at).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        let status = answer.split(' ').nth(1).unwrap().parse().unwrap();
        let body = answer
            .split_once("\r\n\r\n")
            .and_then(|(_, b)| serde_json::from_str(b).ok())
            .unwrap_or(Value::Null);
        (status, body)
    }

    /// The newest audit row: who did what to whom.
    fn last_event(&self) -> (String, String, String) {
        let event = self.store.list_security_events(1).unwrap().remove(0);
        (event.actor, event.action, event.target)
    }
}

/// `account`'s credential: its live session, or a new one; or, for a key account, the key `key` picks.
fn session_of(leader: &Leader, account: &mut Account) -> Credential {
    if !account.session.as_ref().is_some_and(|s| s.live) {
        let user = leader.store.get_user(&account.id).unwrap().unwrap();
        let issued = leader
            .auth
            .sessions
            .issue(&user.id, user.generation, false, Instant::now())
            .unwrap();
        account.session = Some(Session {
            token: issued.token,
            csrf: issued.session.csrf,
            live: true,
        });
    }
    let session = account.session.as_ref().unwrap();
    Credential::Session {
        token: session.token.clone(),
        csrf: session.csrf.clone(),
    }
}

/// An administrator who can act now, however it signs in, and its credential.
fn actor(leader: &Leader, model: &mut Model, rng: &mut Rng, coverage: &mut Coverage) -> (String, Credential) {
    let able: Vec<usize> = (0..model.accounts.len())
        .filter(|&i| {
            let a = &model.accounts[i];
            a.enabled
                && model.administers(a, false)
                && (a.kind != Kind::Key || a.keys.iter().any(|k| !k.revoked && !k.expired()))
        })
        .collect();
    let i = *rng.pick(&able);
    let account = &mut model.accounts[i];
    coverage.reach(format!("actor-{}", account.kind.as_str()));
    let credential = match account.kind {
        Kind::Key => {
            let live: Vec<&Key> = account.keys.iter().filter(|k| !k.revoked && !k.expired()).collect();
            Credential::Key(rng.pick(&live).secret.clone())
        }
        Kind::Local | Kind::Oidc => session_of(leader, account),
    };
    (account.id.clone(), credential)
}

/// Some of what an administrator may give, unknown names among them.
fn some_roles(rng: &mut Rng) -> Vec<String> {
    GIVEN.iter().filter(|_| rng.chance(3)).map(|r| r.to_string()).collect()
}

fn known(roles: &[String]) -> bool {
    roles.iter().all(|r| ROLES.iter().any(|(name, ..)| name == r))
}

/// Sorted and each once, as the leader keeps given roles.
fn kept(mut roles: Vec<String>) -> Vec<String> {
    roles.sort();
    roles.dedup();
    roles
}

fn expect(what: &str, got: u16, want: u16, body: &Value) -> Result<(), String> {
    if got == want {
        Ok(())
    } else {
        Err(format!("{what}: answered {got}, the model says {want}: {body}"))
    }
}

/// The audit's newest row is `action` on `target`, by `actor`.
fn audited(leader: &Leader, actor: &str, action: &str, target: &str) -> Result<(), String> {
    let got = leader.last_event();
    let want = (actor.to_string(), action.to_string(), target.to_string());
    if got == want {
        Ok(())
    } else {
        Err(format!("the audit holds {got:?}, the model says {want:?}"))
    }
}

async fn create_local(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let (by, credential) = actor(leader, model, rng, cov);
    let username = format!("person{}", model.made);
    model.made += 1;
    let display_name = rng.pick(&NAMES).to_string();
    let roles = some_roles(rng);
    let body = json!({
        "authSource": "local",
        "username": username,
        "displayName": display_name,
        "roles": roles,
    });
    let (status, answer) = leader
        .send(false, &credential, "POST", "/api/accounts", Some(&body))
        .await;
    if !known(&roles) {
        cov.reach("create-unknown-role-refused");
        return expect("a local account given a role there is not", status, 422, &answer);
    }
    expect("a new local account", status, 201, &answer)?;
    cov.reach("create-local");
    let id = answer["userId"].as_str().ok_or("no userId")?.to_string();
    audited(leader, &by, "account-create", &id)?;
    model.accounts.push(Account {
        id,
        kind: Kind::Local,
        display_name,
        username: Some(username),
        subject: None,
        enabled: true,
        roles: kept(roles),
        groups: Vec::new(),
        keys: Vec::new(),
        agent: AgentPermissions::default(),
        session: None,
    });
    Ok(())
}

async fn create_key_account(
    leader: &Leader,
    model: &mut Model,
    rng: &mut Rng,
    cov: &mut Coverage,
) -> Result<(), String> {
    let (by, credential) = actor(leader, model, rng, cov);
    let display_name = rng.pick(&NAMES).to_string();
    let roles = some_roles(rng);
    let body = json!({
        "authSource": "key",
        "displayName": display_name,
        "roles": roles,
    });
    let (status, answer) = leader
        .send(false, &credential, "POST", "/api/accounts", Some(&body))
        .await;
    if !known(&roles) {
        cov.reach("create-unknown-role-refused");
        return expect("a key account given a role there is not", status, 422, &answer);
    }
    expect("a new key account", status, 201, &answer)?;
    cov.reach("create-key-account");
    let id = answer["id"].as_str().ok_or("no id")?.to_string();
    audited(leader, &by, "account-create", &id)?;
    model.accounts.push(Account {
        id,
        kind: Kind::Key,
        display_name,
        username: None,
        subject: None,
        enabled: true,
        roles: kept(roles),
        groups: Vec::new(),
        keys: Vec::new(),
        agent: AgentPermissions::default(),
        session: None,
    });
    Ok(())
}

/// The provider signs someone in: a new identity makes its account, and a known one says its groups again.
fn oidc_sign_in(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let signed_in: Vec<String> = model.accounts.iter().filter_map(|a| a.subject.clone()).collect();
    let subject = if !signed_in.is_empty() && rng.chance(2) {
        rng.pick(&signed_in).clone()
    } else {
        model.made += 1;
        format!("subject-{}", model.made)
    };
    let groups: Vec<String> = GROUPS.iter().filter(|_| rng.chance(3)).map(|g| g.to_string()).collect();
    let display_name = rng.pick(&NAMES).to_string();
    let new_id = accounts::generate_user_id();
    let login = format!("{subject}@example.com");
    let created = accounts::event_for(&Actor::system(), "account-create", &new_id, "ok");
    let user = leader
        .store
        .oidc_sign_in(
            OidcSignIn {
                issuer: "https://idp.example.com",
                subject: &subject,
                display_name: &display_name,
                groups: &groups,
                login: Some(&login),
                new_id: &new_id,
                at: now(),
            },
            &created,
        )
        .map_err(|e| e.to_string())?;
    match model
        .accounts
        .iter_mut()
        .find(|a| a.subject.as_deref() == Some(&subject))
    {
        Some(account) => {
            cov.reach("oidc-sign-in-again");
            if user.id != account.id {
                return Err(format!("{subject} signed in again as a second account"));
            }
            account.groups = groups.clone();
        }
        None => {
            cov.reach("oidc-first-sign-in");
            if user.id != new_id {
                return Err(format!("{subject}'s first sign-in made {user:?}"));
            }
            model.accounts.push(Account {
                id: new_id,
                kind: Kind::Oidc,
                display_name,
                username: None,
                subject: Some(subject.clone()),
                enabled: true,
                roles: Vec::new(),
                groups: groups.clone(),
                keys: Vec::new(),
                agent: AgentPermissions::default(),
                session: None,
            });
        }
    }
    let account = model
        .accounts
        .iter()
        .find(|a| a.subject.as_deref() == Some(&subject))
        .unwrap();
    if groups.iter().any(|g| g == "viewers") && !model.roles_of(account).contains(&"viewers") {
        cov.reach("role-name-is-no-group");
    }
    Ok(())
}

/// One change to any account, by an administrator: what it is, and what the model says of it.
async fn change(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let (by, credential) = actor(leader, model, rng, cov);
    let i = rng.below(model.accounts.len());
    let (id, kind) = (model.accounts[i].id.clone(), model.accounts[i].kind);
    let target = format!("/api/accounts/{id}");
    match rng.below(4) {
        0 => {
            let name = rng.pick(&NAMES).to_string();
            let body = json!({ "displayName": name });
            let (status, answer) = leader.send(false, &credential, "PATCH", &target, Some(&body)).await;
            expect("a rename", status, 200, &answer)?;
            cov.reach("rename");
            audited(leader, &by, "account-rename", &id)?;
            model.accounts[i].display_name = name;
        }
        1 => {
            let roles = some_roles(rng);
            let body = json!({ "roles": roles });
            let (status, answer) = leader.send(false, &credential, "PATCH", &target, Some(&body)).await;
            if !known(&roles) {
                cov.reach("roles-unknown-refused");
                return expect("roles that are not all roles", status, 422, &answer);
            }
            if kind == Kind::Oidc {
                cov.reach("roles-oidc-refused");
                return expect("roles given an OIDC account", status, 409, &answer);
            }
            if !roles.iter().any(|r| r == ADMINISTRATOR) && model.other_local_administrators(&id) == 0 {
                cov.reach("roles-last-administrator-refused");
                return expect(
                    "roles without administrator for the last local administrator",
                    status,
                    409,
                    &answer,
                );
            }
            expect("roles given", status, 200, &answer)?;
            cov.reach("roles-given");
            audited(leader, &by, "account-access", &id)?;
            model.accounts[i].roles = kept(roles);
        }
        2 => {
            // The administrator role given or taken, the account's other roles as they are.
            let administrator = rng.chance(2);
            let mut roles: Vec<String> = model.accounts[i]
                .roles
                .iter()
                .filter(|r| *r != ADMINISTRATOR)
                .cloned()
                .collect();
            if administrator {
                roles.push(ADMINISTRATOR.to_string());
            }
            let body = json!({ "roles": roles });
            let (status, answer) = leader.send(false, &credential, "PATCH", &target, Some(&body)).await;
            if kind == Kind::Oidc {
                cov.reach("administrator-oidc-refused");
                return expect("the administrator role given an OIDC account", status, 409, &answer);
            }
            if !administrator && model.other_local_administrators(&id) == 0 {
                cov.reach("administrator-last-refused");
                return expect(
                    "the administrator role taken from the last local administrator",
                    status,
                    409,
                    &answer,
                );
            }
            expect("the administrator role", status, 200, &answer)?;
            cov.reach(if administrator {
                "administrator-given"
            } else {
                "administrator-taken"
            });
            audited(leader, &by, "account-access", &id)?;
            model.accounts[i].roles = kept(roles);
        }
        _ => {
            let enabled = rng.chance(2);
            let body = json!({ "enabled": enabled });
            let (status, answer) = leader.send(false, &credential, "PATCH", &target, Some(&body)).await;
            if !enabled && model.other_local_administrators(&id) == 0 {
                cov.reach("disable-last-administrator-refused");
                return expect("the last local administrator disabled", status, 409, &answer);
            }
            expect("an account enabled or disabled", status, 200, &answer)?;
            let action = if enabled { "account-enable" } else { "account-disable" };
            cov.reach(if enabled { "enable" } else { "disable" });
            audited(leader, &by, action, &id)?;
            model.accounts[i].enabled = enabled;
            if !enabled {
                for key in &mut model.accounts[i].keys {
                    key.revoked = true;
                }
                model.end_sessions(&id);
            }
        }
    }
    Ok(())
}

/// A password reset, or every session ended, for any account.
async fn sign_out(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let (by, credential) = actor(leader, model, rng, cov);
    let i = rng.below(model.accounts.len());
    let (id, kind) = (model.accounts[i].id.clone(), model.accounts[i].kind);
    if rng.chance(2) {
        let target = format!("/api/accounts/{id}/reset-password");
        let (status, answer) = leader.send(false, &credential, "POST", &target, None).await;
        if kind != Kind::Local {
            cov.reach("reset-password-not-local-refused");
            return expect("a password reset for an account with no password", status, 409, &answer);
        }
        expect("a password reset", status, 200, &answer)?;
        cov.reach("reset-password");
        audited(leader, &by, "password-reset", &id)?;
    } else {
        let target = format!("/api/accounts/{id}/revoke-sessions");
        let (status, answer) = leader.send(false, &credential, "POST", &target, None).await;
        expect("sessions revoked", status, 200, &answer)?;
        cov.reach("revoke-sessions");
        audited(leader, &by, "revoke-sessions", &id)?;
    }
    model.end_sessions(&id);
    Ok(())
}

/// A key asked of an administrator for any account, or one made already expired, or one revoked.
async fn key(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let keyed: Vec<usize> = (0..model.accounts.len())
        .filter(|&i| !model.accounts[i].keys.is_empty())
        .collect();
    if !keyed.is_empty() && rng.chance(3) {
        let (by, credential) = actor(leader, model, rng, cov);
        let i = *rng.pick(&keyed);
        let k = rng.below(model.accounts[i].keys.len());
        let (id, key_id) = (model.accounts[i].id.clone(), model.accounts[i].keys[k].id.clone());
        let target = format!("/api/accounts/{id}/keys/{key_id}");
        let (status, answer) = leader.send(false, &credential, "DELETE", &target, None).await;
        if model.accounts[i].keys[k].revoked {
            cov.reach("key-revoked-again-refused");
            return expect("a key revoked twice", status, 404, &answer);
        }
        expect("a key revoked", status, 200, &answer)?;
        cov.reach("key-revoked");
        audited(leader, &by, "key-revoke", &key_id)?;
        model.accounts[i].keys[k].revoked = true;
        return Ok(());
    }

    let i = rng.below(model.accounts.len());
    let id = model.accounts[i].id.clone();
    if model.accounts[i].kind == Kind::Key && model.accounts[i].enabled && rng.chance(4) {
        // Made already past its expiry, through the store: the API makes none that expires in under a day.
        let secret = tokens::generate_secret();
        let key_id = format!("key_expired_{}", model.made);
        model.made += 1;
        let created_at = now() - 2 * DAY_MILLIS;
        let event = accounts::event_for(&Actor::system(), "key-create", &key_id, "ok");
        leader
            .store
            .create_api_key(
                &id,
                NewApiKey {
                    id: &key_id,
                    label: "expired",
                    token_hash: &tokens::hash_secret(&secret),
                    created_at,
                    expires_at: Some(created_at + DAY_MILLIS),
                },
                &event,
            )
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        cov.reach("key-already-expired");
        model.accounts[i].keys.push(Key {
            id: key_id,
            secret,
            created_at,
            expires_at: Some(created_at + DAY_MILLIS),
            revoked: false,
        });
        return Ok(());
    }

    let (by, credential) = actor(leader, model, rng, cov);
    let days: Option<u32> = *rng.pick(&[None, Some(0), Some(1), Some(30)]);
    let body = json!({ "label": "fuzz", "expiresInDays": days });
    let target = format!("/api/accounts/{id}/keys");
    let (status, answer) = leader.send(false, &credential, "POST", &target, Some(&body)).await;
    let account = &mut model.accounts[i];
    if days == Some(0) {
        cov.reach("key-zero-days-refused");
        return expect("a key that expires in no days", status, 422, &answer);
    }
    if account.kind != Kind::Key {
        cov.reach("key-not-key-account-refused");
        return expect("a key for an account that is not a key account", status, 404, &answer);
    }
    if !account.enabled {
        cov.reach("key-disabled-account-refused");
        return expect("a key for a disabled account", status, 409, &answer);
    }
    expect("a new key", status, 201, &answer)?;
    let key_id = answer["id"].as_str().ok_or("no key id")?.to_string();
    let created_at = answer["createdAt"].as_i64().ok_or("no createdAt")?;
    let expires_at = answer["expiresAt"].as_i64();
    let want = days.map(|d| created_at + i64::from(d) * DAY_MILLIS);
    if expires_at != want {
        return Err(format!(
            "a key for {days:?} days expires at {expires_at:?}, not {want:?}"
        ));
    }
    cov.reach(if days.is_some() {
        "key-expires"
    } else {
        "key-never-expires"
    });
    audited(leader, &by, "key-create", &key_id)?;
    account.keys.push(Key {
        id: key_id,
        secret: answer["key"].as_str().ok_or("no key")?.to_string(),
        created_at,
        expires_at,
        revoked: false,
    });
    Ok(())
}

/// An account sets what its agent may use.
fn agent_permissions(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let i = rng.below(model.accounts.len());
    let levels = [Level::Viewer, Level::Editor, Level::Deployer];
    let administrator = rng.chance(3);
    cov.reach(match (administrator, model.administers(&model.accounts[i], false)) {
        (true, true) => "agent-administrator",
        (true, false) => "agent-administrator-gives-nothing",
        (false, _) => "agent-upto",
    });
    let mut areas = BTreeMap::new();
    for area in AREAS {
        if rng.chance(2) {
            areas.insert(area.to_string(), *rng.pick(&levels));
        }
    }
    let permissions = AgentPermissions {
        administrator,
        default: *rng.pick(&levels),
        areas,
    };
    let account = &mut model.accounts[i];
    let event = accounts::event_for(
        &Actor::account(&account.id, ""),
        "agent-permissions-put",
        &account.id,
        "ok",
    );
    leader
        .store
        .put_agent_permissions(&account.id, &permissions, &event)
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    account.agent = permissions;
    Ok(())
}

/// Any account calls, with a session or a key, as itself or through its agent, and is answered as what it holds
/// says.
async fn probe(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let i = rng.below(model.accounts.len());
    let by_agent = rng.chance(3);
    let area = *rng.pick(&AREAS);
    // What it asks, what that needs (none is an administrator), and what it is answered once let in.
    let (method, target, need, answered) = match rng.below(4) {
        0 => ("GET", format!("/api/areas/{area}"), Some(Level::Viewer), 200),
        1 => ("PUT", format!("/api/areas/{area}/sources"), Some(Level::Editor), 200),
        2 => ("GET", "/api/accounts".to_string(), None, 200),
        // Only an administrator makes a key: anyone else is refused before the account is looked for.
        _ => ("POST", "/api/accounts/u_nobody/keys".to_string(), None, 404),
    };
    let account = &model.accounts[i];
    let kind = account.kind;

    // Who the request comes from, and whether the gate lets it in at all.
    let (credential, refused) = if kind == Kind::Key && rng.chance(4) {
        // A session for an account that signs in with a key, which no sign-in issues.
        cov.reach("probe-key-account-session-refused");
        (session_of(leader, &mut model.accounts[i]), Some(401))
    } else if kind == Kind::Key {
        cov.reach("probe-key");
        let account = &model.accounts[i];
        match rng.index(account.keys.len()).filter(|_| !rng.chance(5)) {
            None => {
                cov.reach("probe-unknown-key-refused");
                (Credential::Key(tokens::generate_secret()), Some(401))
            }
            Some(k) => {
                let key = &account.keys[k];
                let refused = if key.revoked {
                    cov.reach("probe-revoked-key-refused");
                    Some(403)
                } else if key.expired() {
                    cov.reach("probe-expired-key-refused");
                    Some(403)
                } else {
                    None
                };
                (Credential::Key(key.secret.clone()), refused)
            }
        }
    } else {
        cov.reach(format!("probe-{}", kind.as_str()));
        let ended = account.session.as_ref().is_some_and(|s| !s.live);
        let refused = if ended && rng.chance(2) {
            // The session it held before it ended.
            cov.reach("probe-ended-session-refused");
            let s = account.session.as_ref().unwrap();
            let credential = Credential::Session {
                token: s.token.clone(),
                csrf: s.csrf.clone(),
            };
            let (status, answer) = leader
                .send(by_agent, &credential, method, &target, Some(&json!({})))
                .await;
            return expect(
                &format!("{method} {target} with an ended session"),
                status,
                401,
                &answer,
            );
        } else if !account.enabled {
            cov.reach("probe-disabled-refused");
            Some(401)
        } else {
            None
        };
        (session_of(leader, &mut model.accounts[i]), refused)
    };

    let account = &model.accounts[i];
    let want = refused.unwrap_or_else(|| {
        let allowed = match need {
            Some(need) => model.level(account, by_agent, area).is_some_and(|held| held >= need),
            None => model.administers(account, by_agent),
        };
        cov.reach(if allowed { "probe-allowed" } else { "probe-forbidden" });
        if allowed { answered } else { 403 }
    });
    if by_agent {
        cov.reach("probe-by-agent");
    }
    let (status, answer) = leader
        .send(by_agent, &credential, method, &target, Some(&json!({})))
        .await;
    let who = format!("{} {} {}", kind.as_str(), account.display_name, account.id);
    let as_agent = if by_agent { "'s agent" } else { "" };
    expect(&format!("{method} {target} by {who}{as_agent}"), status, want, &answer)?;
    if status == 401
        && let Some(session) = model.accounts[i].session.as_mut()
    {
        // The gate forgets a session it refuses.
        session.live = false;
    }
    Ok(())
}

/// A role given other groups, which change what OIDC accounts holding them hold; or the administrator role asked
/// to give a level in an area, or to go, which it never does.
async fn role(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let (by, credential) = actor(leader, model, rng, cov);
    let r = rng.below(ROLES.len());
    let (name, _, gives) = ROLES[r];
    let target = format!("/api/roles/{name}");
    if name == ADMINISTRATOR && rng.chance(3) {
        let (status, answer) = leader.send(false, &credential, "DELETE", &target, None).await;
        cov.reach("administrator-delete-refused");
        return expect("the administrator role deleted", status, 409, &answer);
    }
    let groups: Vec<String> = GROUPS.iter().filter(|_| rng.chance(3)).map(|g| g.to_string()).collect();
    let mut areas: BTreeMap<&str, &str> = gives.iter().map(|(a, l)| (*a, l.as_str())).collect();
    let asks_area = name == ADMINISTRATOR && rng.chance(3);
    if asks_area {
        areas.insert("payments", "viewer");
    }
    let body = json!({ "groups": groups, "areas": areas });
    let (status, answer) = leader.send(false, &credential, "PUT", &target, Some(&body)).await;
    if asks_area {
        cov.reach("administrator-areas-refused");
        return expect("the administrator role given a level in an area", status, 422, &answer);
    }
    expect("a role given other groups", status, 200, &answer)?;
    cov.reach("role-groups-changed");
    audited(leader, &by, "role-put", name)?;
    model.groups[r] = groups;
    Ok(())
}

/// The roster, read by an administrator, shows every account as the model holds it.
async fn roster(leader: &Leader, model: &mut Model, rng: &mut Rng, cov: &mut Coverage) -> Result<(), String> {
    let (_, credential) = actor(leader, model, rng, cov);
    let (status, answer) = leader.send(false, &credential, "GET", "/api/accounts", None).await;
    expect("the roster", status, 200, &answer)?;
    cov.reach("roster");
    let listed = answer["accounts"].as_array().ok_or("no accounts")?;
    if listed.len() != model.accounts.len() {
        return Err(format!(
            "the roster lists {}, the model {}",
            listed.len(),
            model.accounts.len()
        ));
    }
    for account in &model.accounts {
        let shown = listed
            .iter()
            .find(|a| a["id"] == account.id.as_str())
            .ok_or_else(|| format!("{} is not on the roster", account.id))?;
        let mut roles: Vec<&str> = match account.kind {
            Kind::Oidc => model.roles_of(account),
            Kind::Local | Kind::Key => account.roles.iter().map(String::as_str).collect(),
        };
        roles.sort();
        let mut shown_roles: Vec<&str> = shown["roles"]
            .as_array()
            .ok_or("no roles")?
            .iter()
            .filter_map(Value::as_str)
            .collect();
        shown_roles.sort();
        let groups: Vec<&str> = account.groups.iter().map(String::as_str).collect();
        // Those not revoked, expired or not, oldest first.
        let keys: Option<Vec<(String, Option<i64>)>> = (account.kind == Kind::Key).then(|| {
            let mut kept: Vec<&Key> = account.keys.iter().filter(|k| !k.revoked).collect();
            kept.sort_by_key(|k| k.created_at);
            kept.iter().map(|k| (k.id.clone(), k.expires_at)).collect()
        });
        let shown_keys: Option<Vec<(String, Option<i64>)>> = shown["keys"].as_array().map(|keys| {
            keys.iter()
                .map(|k| (k["id"].as_str().unwrap_or("").to_string(), k["expiresAt"].as_i64()))
                .collect()
        });
        let want = json!({
            "authSource": account.kind.as_str(),
            "displayName": account.display_name,
            "username": account.username,
            "enabled": account.enabled,
            "administrator": model.administers(account, false),
            "roles": roles,
            "groups": groups,
            "keys": keys,
        });
        let got = json!({
            "authSource": shown["authSource"],
            "displayName": shown["displayName"],
            "username": shown["username"],
            "enabled": shown["enabled"],
            "administrator": shown["access"]["administrator"],
            "roles": shown_roles,
            "groups": shown["groups"],
            "keys": shown_keys,
        });
        if got != want {
            return Err(format!("the roster shows {got}, the model {want}"));
        }
    }
    Ok(())
}

async fn journey(seed: u64, cov: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let leader = Leader::start().await;
    for (name, groups, gives) in ROLES {
        let role = Role {
            name: name.to_string(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            areas: gives.iter().map(|(a, l)| (a.to_string(), *l)).collect(),
        };
        let event = accounts::event_for(&Actor::system(), "role-put", name, "ok");
        leader.store.put_role(&role, &event).map_err(|e| e.to_string())?;
    }
    // The administrator the leader starts with.
    let root = accounts::generate_user_id();
    let event = accounts::event_for(&Actor::system(), "bootstrap", &root, "ok");
    leader
        .store
        .create_local_user(
            NewLocalUser {
                id: &root,
                username: "root",
                display_name: "Ada",
                password_hash: "never-checked",
                must_change: false,
            },
            &[ADMINISTRATOR.to_string()],
            &event,
        )
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    let mut model = Model::new();
    model.accounts.push(Account {
        id: root,
        kind: Kind::Local,
        display_name: "Ada".into(),
        username: Some("root".into()),
        subject: None,
        enabled: true,
        roles: vec![ADMINISTRATOR.to_string()],
        groups: Vec::new(),
        keys: Vec::new(),
        agent: AgentPermissions::default(),
        session: None,
    });

    let mut done = Vec::new();
    for _ in 0..60 {
        let locals = model.accounts.iter().filter(|a| a.kind == Kind::Local).count();
        let (name, outcome) = match rng.below(10) {
            // Each local account and reset hashes a password, which is slow by design: a few per seed.
            0 if locals < 4 => ("create-local", create_local(&leader, &mut model, &mut rng, cov).await),
            1 => (
                "create-key-account",
                create_key_account(&leader, &mut model, &mut rng, cov).await,
            ),
            2 => ("oidc-sign-in", oidc_sign_in(&leader, &mut model, &mut rng, cov)),
            3 | 4 => ("change", change(&leader, &mut model, &mut rng, cov).await),
            5 if rng.chance(2) => ("sign-out", sign_out(&leader, &mut model, &mut rng, cov).await),
            6 => ("key", key(&leader, &mut model, &mut rng, cov).await),
            7 => (
                "agent-permissions",
                agent_permissions(&leader, &mut model, &mut rng, cov),
            ),
            8 if rng.chance(2) => ("roster", roster(&leader, &mut model, &mut rng, cov).await),
            8 => ("role", role(&leader, &mut model, &mut rng, cov).await),
            _ => ("probe", probe(&leader, &mut model, &mut rng, cov).await),
        };
        done.push(name);
        outcome.map_err(|why| format!("{why}\nafter {}", done.join(", ")))?;
    }
    Ok(())
}

#[test]
fn accounts_fuzz() {
    let mut coverage = Coverage::new(OPTIONS.iter().copied());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    fuzz::run(0..48, |seed| runtime.block_on(journey(seed, &mut coverage)));
    coverage.assert_complete();
}
