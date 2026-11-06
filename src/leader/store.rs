//! Durable leader state: accounts, worker credentials, enrollment codes, the secret catalog, every group's color, and
//! who holds each single-worker source with what it has delivered. A worker group's identity and configuration are
//! not here at all -- it exists exactly when `main` holds a file under `groups/<id>/`, and that repository is what
//! holds its name and configuration. Its color is the one thing about it kept here instead. Live heartbeat state is `registry`'s, in memory.
//!
//! Password verifiers live here too, so this file is the reason `leader.db` needs the filesystem protection the
//! secrets encryption key already has: a copy of it can be attacked offline.

use super::envelope::ErrorKind;
use crate::heartbeat::SourceKey;

pub use super::sqlite_store::Store;

/// A worker's Bearer credential, as its hash. The raw token exists only in the response to the `POST /api/worker/enroll` call
/// that issued it.
pub struct WorkerCredentialRecord {
    pub worker_id: String,
    pub token_hash: String,
    pub revoked: bool,
    /// Whether it has ever authenticated a request. Until it has, a retry of the enrollment that issued it may
    /// replace it: the worker may never have received it.
    pub used: bool,
    /// The kind of the enrollment code that brought this worker in.
    pub kind: WorkerKind,
    /// When an enrollment code brought it in, in epoch milliseconds. `None` for a worker enrolled before enrollment
    /// codes recorded it.
    pub enrolled_at: Option<i64>,
}

/// What a worker being gone means to the leader. The enrollment code that brings a worker in decides it, and the worker
/// keeps it for its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    /// Kept until revoked. It comes back from a restart as itself, from its own state directory, so while it is
    /// gone it is missed.
    Permanent,
    /// Its identity lives and dies with the pod or process holding it, so once it is gone the leader forgets it.
    Ephemeral,
}

impl WorkerKind {
    /// The spelling used in the database and the API.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerKind::Permanent => "permanent",
            WorkerKind::Ephemeral => "ephemeral",
        }
    }

    pub fn parse(s: &str) -> Option<WorkerKind> {
        match s {
            "permanent" => Some(WorkerKind::Permanent),
            "ephemeral" => Some(WorkerKind::Ephemeral),
            _ => None,
        }
    }
}

/// Where a secret's value lives. Declared once, when the secret is created, and never changed: moving a value
/// between hosts and the leader is creating a different secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Custody {
    /// The worker's own environment, read at deploy time as it always has been. The leader knows the name and
    /// nothing else, which is how an operator declines to hand it a credential.
    WorkerLocal,
    /// The leader, sealed under its key (`secrets`).
    LeaderStored,
}

impl Custody {
    /// The spelling used in the database, the API and a bundle.
    pub fn as_str(self) -> &'static str {
        match self {
            Custody::WorkerLocal => "worker-local",
            Custody::LeaderStored => "leader-stored",
        }
    }

    pub fn parse(s: &str) -> Option<Custody> {
        match s {
            "worker-local" => Some(Custody::WorkerLocal),
            "leader-stored" => Some(Custody::LeaderStored),
            _ => None,
        }
    }
}

/// What a certificate this leader issued is issued for, and what each renewal issues it for again.
#[derive(Clone, Debug, PartialEq)]
pub struct Issued {
    /// The DNS names and IP addresses it covers.
    pub names: Vec<String>,
    /// How long each version is valid, in days. Never past the leader's authority.
    pub validity_days: i64,
}

impl Issued {
    pub fn validity(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.validity_days.max(1) as u64 * 24 * 60 * 60)
    }
}

/// How the leader keeps a certificate up to date on its own: an entry with one is never replaced by an upload.
#[derive(Clone, Debug, PartialEq)]
pub enum Kept {
    /// Issued under the leader's own authority, and issued again the same way.
    Issued(Issued),
    /// Ordered over ACME, under the `acme.*` settings, and ordered again under them.
    Acme,
}

/// What the leader knows about a secret. Never its value.
pub struct SecretRecord {
    pub name: String,
    pub description: String,
    /// Whether this is a certificate's private key, with the certificate's public chain kept beside each version.
    /// Listed on the Certificates page rather than with the secrets.
    pub certificate: bool,
    /// Every version still held, oldest first. Non-empty exactly when custody is [`Custody::LeaderStored`]: the
    /// leader either holds values and has these facts about them, or holds nothing and has none.
    ///
    /// More than one because a bundle names the version it was built for, and a rollback names an earlier one. How
    /// many are kept is the leader's setting (`startup_config::leader_secret_versions`); what a deployed bundle
    /// names is kept whatever that number says.
    pub versions: Vec<StoredValue>,
}

/// What the leader knows about one stored value.
pub struct StoredValue {
    /// The number a bundle quotes to run this value, and nothing else runs in its place.
    pub version: i64,
    /// When it was stored, in epoch milliseconds.
    pub set_at: i64,
    /// Whether the leader's current key opens it. `false` is a credential the fleet cannot obtain -- the same fact
    /// as an unset environment variable, and it takes the same path.
    pub usable: bool,
}

impl SecretRecord {
    pub fn custody(&self) -> Custody {
        match self.versions.is_empty() {
            false => Custody::LeaderStored,
            true => Custody::WorkerLocal,
        }
    }

    /// The newest version, which is what a reference written today quotes. `None` for worker-local custody.
    pub fn current(&self) -> Option<&StoredValue> {
        self.versions.last()
    }
}

/// Where the secrets encryption key stands against the values sealed under it (`docs/architecture/security.md`,
/// Secrets boundary).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyStanding {
    /// Where the leader reads the key from.
    pub path: std::path::PathBuf,
    pub key: KeyState,
    /// Every value whose newest copy the key cannot open, by name: what an admin enters again.
    pub unopenable: Vec<Unopenable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyState {
    /// No key and nothing sealed: a leader that has stored nothing yet.
    Absent,
    /// The key opens every value sealed since it was written. Values sealed before it, under a key it replaced, are
    /// [`KeyStanding::unopenable`] until entered again.
    Held { key_id: String },
    /// No key, and values sealed under one.
    Missing,
    /// A key that did not seal the stored values, nor replaced the key that did.
    Mismatched { key_id: String },
}

/// One value the key cannot open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unopenable {
    /// A leader-stored secret, by name.
    Secret(String),
    /// A certificate's private key, by the certificate's name.
    Certificate(String),
    /// A sealed leader setting, by its key.
    Setting(String),
}

impl KeyStanding {
    /// Whether the key is gone or wrong. While it is, nothing is sealed: a value sealed now would be sealed under a
    /// key the restored one cannot open, or written beside values the leader can no longer read.
    pub fn broken(&self) -> bool {
        matches!(self.key, KeyState::Missing | KeyState::Mismatched { .. })
    }

    /// What is wrong, in the words the Issues page and a refused write both use. `None` while the key is not broken.
    pub fn problem(&self) -> Option<String> {
        let what = match self.key {
            KeyState::Missing => "is missing",
            KeyState::Mismatched { .. } => "does not match the stored secrets",
            KeyState::Absent | KeyState::Held { .. } => return None,
        };
        let n = self.unopenable.len();
        Some(format!(
            "the secrets encryption key at {} {what}, so the {n} stored secret{} cannot be opened and no new ones can \
             be stored. Restore it and restart the leader, or regenerate it under Admin, \
             Secrets, Encryption key",
            self.path.display(),
            if n == 1 { "" } else { "s" },
        ))
    }
}

// -- accounts ---------------------------------------------------------------

/// Where an account's credentials come from, fixed when the account is created.
///
/// One account is never two identities: an OIDC account matching a local account's name or email is a different
/// account, and no operation converts one into the other. The database enforces it -- `local_credentials` and
/// `oidc_identities` each carry this column and a foreign key over `(user_id, auth_source)`, so a password can only
/// ever hang off a local account and an identity only off an OIDC one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthSource {
    /// A username and password held by this leader.
    Local,
    /// An issuer and a subject an OpenID provider vouches for. The leader holds no credential for it.
    Oidc,
    /// A key the leader keeps the hash of, sent on every request ([`Store::api_key_by_hash`]). It never signs in.
    Key,
}

impl AuthSource {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthSource::Local => "local",
            AuthSource::Oidc => "oidc",
            AuthSource::Key => "key",
        }
    }

    pub fn parse(s: &str) -> Option<AuthSource> {
        match s {
            "local" => Some(AuthSource::Local),
            "oidc" => Some(AuthSource::Oidc),
            "key" => Some(AuthSource::Key),
            _ => None,
        }
    }
}

/// An account, however it signs in. Never its credentials: a password hash reaches only [`Store::local_credential_by_username`], so
/// nothing that logs, serializes or `Debug`s a user can carry one.
#[derive(Clone, Debug)]
pub struct UserRecord {
    /// Opaque and immutable. What a preference, a grant and a git author address are keyed by, so renaming a
    /// person changes nothing that refers to them.
    pub id: String,
    pub auth_source: AuthSource,
    pub display_name: String,
    pub enabled: bool,
    /// Bumped by a password change, a reset and an explicit revocation. A session quoting an older one is refused,
    /// which is what makes those three take effect on the next request rather than at the next sweep.
    pub generation: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A local account's login name and password verifier.
///
/// `password_hash` is a PHC string: it carries the algorithm and the parameters it was written with, so raising
/// the work factor leaves every stored hash verifiable and rehashing is a decision taken per login.
pub struct LocalCredentialRecord {
    pub user_id: String,
    pub username: String,
    pub password_hash: String,
    /// The account's generation as of this hash. Read with it rather than after it, so a verifier and the
    /// generation a session would quote for it cannot come from either side of a reset.
    pub generation: i64,
    /// The account holds a password it did not choose -- from bootstrap, a reset, or an administrator creating it.
    /// Its session may do nothing but change that password.
    pub must_change: bool,
}

/// Something that happened to an account, kept for the operator who has to answer what changed and who did it.
#[derive(Clone, Debug)]
pub struct SecurityEvent {
    pub id: String,
    pub at: i64,
    /// [`Actor::id`]: a user id, or a reserved one for what no person asked for.
    pub actor: String,
    /// What was attempted, in a fixed vocabulary: `login`, `logout`, `password-change`, `account-create`,
    /// `account-disable`, `account-enable`, `account-delete`, `password-reset`, `revoke-sessions`, `bootstrap`,
    /// `recover`, `role-put`, `role-delete`, `account-rename`, `account-access`, `key-create`, `key-revoke`.
    pub action: String,
    /// What it was done to -- a user id, usually. Empty when the action names no object.
    pub target: String,
    pub outcome: String,
    /// The request this row answers for, which writing it marks as recorded.
    pub recorded: Recorded,
}

/// Whether what one request did has been recorded: an audit row written, or a commit signed, for its [`Actor`].
/// The gate arms it for each request it admits and reads it once the handler has answered, so a change that
/// records nothing is caught where every request passes rather than in each handler. Every clone of the request's
/// actor shares it, onto whatever thread the work runs. An actor no request made carries none.
#[derive(Clone, Debug, Default)]
pub struct Recorded(Option<std::sync::Arc<std::sync::atomic::AtomicBool>>);

impl Recorded {
    /// One request's, not yet recorded.
    pub fn armed() -> Recorded {
        Recorded(Some(Default::default()))
    }

    pub fn mark(&self) {
        if let Some(flag) = &self.0 {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn is_marked(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
    }
}

/// Who caused something. Constructed by the server and never from a request body, so a caller naming themselves
/// in JSON is naming nothing.
///
/// Carries a display name as well as an id because a git commit needs one, and the name a commit records is the
/// one that was current when it was written. Renaming somebody later does not rewrite history, and should not:
/// what the commit says is who wrote it, as they were called then.
#[derive(Clone, Debug)]
pub struct Actor {
    /// A user id, or one of the reserved ids below. What an audit row is keyed by.
    pub id: String,
    pub display_name: String,
    /// The request this actor made, when a request made it.
    pub recorded: Recorded,
}

/// Who someone is, not which request they made.
impl PartialEq for Actor {
    fn eq(&self, other: &Actor) -> bool {
        (&self.id, &self.display_name) == (&other.id, &other.display_name)
    }
}

impl Eq for Actor {}

impl Actor {
    /// The leader acting on its own behalf: seeding a repository, reconciling, expiring a session.
    pub const SYSTEM_ID: &'static str = "system";
    /// Whoever has the leader's host and its data directory. Bootstrap and recovery run as this rather than
    /// fabricating a person who did not ask for them.
    pub const HOST_ID: &'static str = "host";
    /// A request that carried no session. Only a refused sign-in is recorded this way; nothing anonymous reaches
    /// a mutation.
    pub const ANONYMOUS_ID: &'static str = "anonymous";

    pub fn system() -> Actor {
        Actor::reserved(Actor::SYSTEM_ID, "smeltr-leader")
    }

    pub fn host() -> Actor {
        Actor::reserved(Actor::HOST_ID, "smeltr-host")
    }

    pub fn anonymous() -> Actor {
        Actor::reserved(Actor::ANONYMOUS_ID, "anonymous")
    }

    fn reserved(id: &str, display_name: &str) -> Actor {
        Actor {
            id: id.to_string(),
            display_name: display_name.to_string(),
            recorded: Recorded::default(),
        }
    }

    /// An account, as the gate resolved it.
    pub fn account(id: &str, display_name: &str) -> Actor {
        Actor {
            id: id.to_string(),
            display_name: display_name.to_string(),
            recorded: Recorded::default(),
        }
    }

    /// An account's agent acting for it: its account, named as its.
    pub fn agent_of(id: &str, display_name: &str) -> Actor {
        Actor {
            id: id.to_string(),
            display_name: format!("{display_name} (agent)"),
            recorded: Recorded::default(),
        }
    }

    /// Whether this is an account rather than the leader, its host or nobody.
    pub fn is_account(&self) -> bool {
        ![Actor::SYSTEM_ID, Actor::HOST_ID, Actor::ANONYMOUS_ID].contains(&self.id.as_str())
    }
}

/// Why a durable account operation was refused. Distinct from [`StoreError`], which is the store failing rather
/// than answering.
#[derive(Debug, PartialEq, Eq)]
pub enum AccountError {
    /// The username is taken -- by an enabled account, or by a disabled one whose name is never released.
    UsernameTaken,
    /// No such account.
    NoSuchUser,
    /// The operation would leave no enabled administrator able to log in.
    LastAdministrator,
    /// An OIDC account holds what its provider's groups give, and is given nothing here.
    HeldByProvider,
    /// The account is disabled, and takes no key until it is enabled.
    Disabled,
    /// A local-credential operation named an account that is not local.
    WrongAuthSource,
    /// The account moved on since the caller read it: disabled, or its generation bumped by a reset or a
    /// revocation.
    Superseded,
    /// Only a disabled account is deleted.
    Enabled,
    /// The account recovery restores, the way back in from the host.
    Recovery,
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountError::UsernameTaken => write!(f, "that username is already taken"),
            AccountError::NoSuchUser => write!(f, "no such account"),
            AccountError::LastAdministrator => write!(f, "this is the last administrator who signs in with a password"),
            AccountError::HeldByProvider => write!(
                f,
                "roles are not given here: an OIDC account holds the roles its provider's groups give"
            ),
            AccountError::Disabled => write!(f, "that account is disabled"),
            AccountError::WrongAuthSource => write!(f, "that account does not sign in with a password"),
            AccountError::Superseded => write!(f, "that account changed since it was read"),
            AccountError::Enabled => write!(f, "disable the account before deleting it"),
            AccountError::Recovery => write!(
                f,
                "the admin account is never deleted, because recovery from the host restores it"
            ),
        }
    }
}

impl std::error::Error for AccountError {}

/// A new local account and its first password, written as one.
pub struct NewLocalUser<'a> {
    pub id: &'a str,
    pub username: &'a str,
    pub display_name: &'a str,
    pub password_hash: &'a str,
    pub must_change: bool,
}

/// What an OpenID provider said about the person signing in.
pub struct OidcSignIn<'a> {
    pub issuer: &'a str,
    pub subject: &'a str,
    /// Used only when the account is created; afterwards the name is the leader's, which an administrator may change.
    pub display_name: &'a str,
    pub groups: &'a [String],
    /// What the provider calls the person, kept as of each sign-in so an administrator can tell accounts apart.
    pub login: Option<&'a str>,
    /// The id to give the account if this identity has none yet.
    pub new_id: &'a str,
    pub at: i64,
}

/// The identity an OIDC account signs in as, and what its provider last said about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OidcIdentityRecord {
    pub issuer: String,
    pub subject: String,
    /// The groups claim as of the last sign-in or refresh, which the roles of the same names give access by.
    pub groups: Vec<String>,
    /// What the provider called the person at the last sign-in or refresh: their email, else their preferred username.
    pub login: Option<String>,
    pub last_sign_in: i64,
}

/// One key an account holds, as its roster shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyRecord {
    pub id: String,
    pub label: String,
    pub created_at: i64,
    /// When it stops letting anyone in. `None` for a key that never expires.
    pub expires_at: Option<i64>,
    /// When it last let a request in, to the minute. `None` for a key never used.
    pub last_used_at: Option<i64>,
}

/// A key presented to the gate, as its row says: whose it is, and whether it still lets anyone in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresentedKey {
    pub id: String,
    pub user_id: String,
    pub revoked: bool,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
}

/// A new key account, which holds no key until one is made for it.
pub struct NewKeyAccount<'a> {
    pub id: &'a str,
    pub display_name: &'a str,
}

/// A new key. The key itself is never here: the leader answers it once and keeps the hash.
pub struct NewApiKey<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub token_hash: &'a str,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

/// A conversation between a person and their agent. Its turns are kept apart, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationRecord {
    pub id: String,
    /// Whose it is: only they read or continue it.
    pub user_id: String,
    pub title: String,
    /// Milliseconds since the epoch.
    pub created_at: i64,
    /// When a turn was last added. A conversation untouched past the setting's days is removed.
    pub updated_at: i64,
}

/// A store call that did not do what it was asked. `kind` says whether the store failed or refused, which is what
/// a route answers it at (`envelope::Refusal`).
#[derive(Debug)]
pub struct StoreError {
    pub kind: ErrorKind,
    pub message: String,
}

impl StoreError {
    pub fn failed(message: impl Into<String>) -> Self {
        StoreError::of(ErrorKind::Failed, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        StoreError::of(ErrorKind::Conflict, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        StoreError::of(ErrorKind::NotFound, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        StoreError::of(ErrorKind::Invalid, message)
    }

    pub fn of(kind: ErrorKind, message: impl Into<String>) -> Self {
        StoreError {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for StoreError {}

impl From<String> for StoreError {
    fn from(message: String) -> Self {
        StoreError::failed(message)
    }
}

impl From<StoreError> for String {
    fn from(e: StoreError) -> String {
        e.message
    }
}

/// What a save does to a credential the leader holds sealed in a setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretSave {
    Keep,
    Forget,
    Replace(String),
}

impl SecretSave {
    /// As a page sends it: absent keeps the stored one, empty forgets it. A credential belongs to what it was saved
    /// with, a URL, host or service, so `moved`, that it changed, forgets it unless a new one is sent: it is never
    /// sent anywhere else.
    pub fn sent(secret: Option<String>, moved: bool) -> Self {
        match secret {
            Some(s) if s.is_empty() => SecretSave::Forget,
            Some(s) => SecretSave::Replace(s),
            None if moved => SecretSave::Forget,
            None => SecretSave::Keep,
        }
    }

    /// The credential held after the save, given the one held now.
    pub fn held<'a>(&'a self, now: Option<&'a str>) -> Option<&'a str> {
        match self {
            SecretSave::Keep => now,
            SecretSave::Forget => None,
            SecretSave::Replace(s) => Some(s),
        }
    }

    /// The entry for [`Store::put_settings`] that makes the save to `key`, sealed; none to keep it.
    pub fn entry<K: AsRef<str>>(self, store: &Store, key: K) -> Result<Option<(K, String)>, StoreError> {
        Ok(match self {
            SecretSave::Keep => None,
            SecretSave::Forget => Some((key, String::new())),
            SecretSave::Replace(s) => {
                let sealed = store.sealed_setting(key.as_ref(), &s)?;
                Some((key, sealed))
            }
        })
    }
}

/// An enrollment code being written down. The secret itself is never here -- the leader shows it once and keeps the
/// hash.
pub struct NewEnrollmentCode {
    pub id: String,
    pub code_hash: String,
    /// The authority a worker taking this enrollment code will pin to, as a SHA-256 fingerprint. Empty for a leader its
    /// workers verify with the platform's own roots.
    pub fingerprint: String,
    /// The group the operator minted it for. It pre-fills the command and binds nothing: a worker names its own
    /// group, which is the only thing that decides what it runs.
    pub group_id: String,
    /// [`Actor::id`] of whoever minted it.
    pub minted_by: String,
    pub minted_at: i64,
    /// `None` for an enrollment code with no expiry of its own.
    pub expires_at: Option<i64>,
    /// `None` for an enrollment code with no cap on how many workers it brings in.
    pub max_uses: Option<i64>,
    /// What each worker it brings in is.
    pub kind: WorkerKind,
}

/// An enrollment code as the roster shows it. Never carries the hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentCodeRecord {
    pub id: String,
    pub fingerprint: String,
    pub group_id: String,
    pub minted_by: String,
    pub minted_at: i64,
    pub expires_at: Option<i64>,
    pub max_uses: Option<i64>,
    pub kind: WorkerKind,
    pub uses: i64,
    /// The workers this enrollment code brought in, oldest first. When an enrollment code leaks, this is the list to
    /// revoke.
    pub used_by: Vec<String>,
    /// When an operator withdrew it, where one did.
    pub withdrawn_at: Option<i64>,
}

/// An enrollment code that can still bring a worker in, carrying only what deciding about one needs.
///
/// Not an [`EnrollmentCodeRecord`]: that listing also answers with every enrollment code ever spent, as provenance, and
/// reads each one's workers back in a query of its own. A rotation is watched on a poll a couple of seconds apart, and
/// that is a scan it should not repeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsableEnrollmentCode {
    pub id: String,
    pub group_id: String,
    /// The root a worker taking it pins. Empty where the leader that minted it had no authority of its own.
    pub fingerprint: String,
}

/// What became of a worker presenting an enrollment code.
///
/// The refusals are told apart so a worker can say which it was. That tells an unauthenticated caller
/// whether a secret it already holds is known here, which is worth far less than an operator being told to mint
/// another rather than hunting a leader that is answering correctly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Spend {
    Spent {
        code_id: String,
        /// The root it was minted against, as the leader wrote it down. What a caller re-reading the authority
        /// after the fact measures against.
        fingerprint: String,
    },
    /// No enrollment code has that secret: never minted, or minted by a different leader.
    Unknown,
    Expired {
        expired_at: i64,
    },
    /// Withdrawn by an operator. Kept apart from [`Spend::Expired`]: one is a date passing and the other is
    /// somebody deciding, and only the second says the enrollment code may have leaked.
    Withdrawn,
    /// Every use is already spent. Carries what it brought in, because the operator's next question is which.
    Exhausted {
        used_by: Vec<String>,
    },
    /// `worker_id` has enrolled before, revoked or not, and is not retrying an enrollment whose answer it never
    /// received. The enrollment code is left untouched: nothing was taken, so nothing is owed back.
    AlreadyEnrolled,
    /// The pasted enrollment code names an authority other than the one it was minted against. Its secret is intact, so
    /// this is that value edited rather than an enrollment code from somewhere else.
    Altered,
    /// The root this enrollment code was minted against is not the one the leader signs under now. A worker taking it
    /// would pin a certificate no handshake presents and never reach its leader again.
    WrongAuthority,
}

/// Who holds a single-worker source in one group, and where it resumes. Its ledger is read on its own
/// ([`Store::ledger_page`]), since it can be far larger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceHold {
    pub holder: String,
    pub cursor: Option<String>,
}

/// One hold as the map reports it: who holds a source at one place in one group, and how many items its ledger
/// keeps there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldSource {
    pub group: String,
    pub key: SourceKey,
    pub holder: String,
    pub ledger_entries: u64,
}

/// A warning an alert channel was told of (`leader::alerts`).
#[derive(Debug, Clone, PartialEq)]
pub struct AlertTold {
    pub channel: String,
    /// The warning's key, which names it across looks.
    pub warning: String,
    pub told_at: i64,
    /// The warning as told, as JSON, so that its clearing can say what cleared.
    pub body: String,
}
