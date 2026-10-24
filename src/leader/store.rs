//! Durable leader state: people, worker credentials, enrollment codes, the secret catalog, every group's color, and
//! who holds each single-worker source with what it has delivered. A worker group's identity and configuration are
//! not here at all -- it exists exactly when `main` holds a file under `groups/<id>/`, and that repository is what
//! holds its name and configuration. Its color is the one thing about it kept here instead. Live heartbeat state is `registry`'s, in memory.
//!
//! Password verifiers live here too, so this file is the reason `leader.db` needs the filesystem protection the
//! secrets encryption key already has: a copy of it can be attacked offline.

use crate::heartbeat::{LedgerEntry, SourceKey};
use crate::io::Progress;

/// A worker's Bearer credential, as its hash. The raw token exists only in the response to the `POST /api/enroll` call
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

/// What the leader knows about a secret. Never its value.
pub struct SecretRecord {
    pub name: String,
    pub description: String,
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
             be stored. Restore it and restart the leader, or regenerate it under Secrets, Encryption key",
            self.path.display(),
            if n == 1 { "" } else { "s" },
        ))
    }
}

// -- people -----------------------------------------------------------------

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
}

impl AuthSource {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthSource::Local => "local",
            AuthSource::Oidc => "oidc",
        }
    }

    pub fn parse(s: &str) -> Option<AuthSource> {
        match s {
            "local" => Some(AuthSource::Local),
            "oidc" => Some(AuthSource::Oidc),
            _ => None,
        }
    }
}

/// A person. Never their credentials: a password hash reaches only [`Store::local_credential_by_username`], so
/// nothing that logs, serializes or `Debug`s a user can carry one.
#[derive(Clone, Debug)]
pub struct UserRecord {
    /// Opaque and immutable. What a preference, a grant and a git author address are keyed by, so renaming a
    /// person changes nothing that refers to them.
    pub id: String,
    pub auth_source: AuthSource,
    pub display_name: String,
    pub enabled: bool,
    /// A local account always is. An OIDC account never is by this flag: what it holds comes from its roles
    /// (`access::Access::of`).
    pub administrator: bool,
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
    /// `account-disable`, `account-enable`, `password-reset`, `revoke-sessions`, `bootstrap`, `recover`, `role-put`,
    /// `role-delete`.
    pub action: String,
    /// What it was done to -- a user id, usually. Empty when the action names no object.
    pub target: String,
    pub outcome: String,
}

/// Who caused something. Constructed by the server and never from a request body, so a caller naming themselves
/// in JSON is naming nothing.
///
/// Carries a display name as well as an id because a git commit needs one, and the name a commit records is the
/// one that was current when it was written. Renaming somebody later does not rewrite history, and should not:
/// what the commit says is who wrote it, as they were called then.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actor {
    /// A user id, or one of the reserved ids below. What an audit row is keyed by.
    pub id: String,
    pub display_name: String,
}

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
        }
    }

    /// An account, as the gate resolved it.
    pub fn person(id: &str, display_name: &str) -> Actor {
        Actor {
            id: id.to_string(),
            display_name: display_name.to_string(),
        }
    }

    /// A person's agent acting for them: their account, named as theirs.
    pub fn persons_agent(id: &str, display_name: &str) -> Actor {
        Actor {
            id: id.to_string(),
            display_name: format!("{display_name} (agent)"),
        }
    }

    /// Whether this is a person rather than the leader or its host. What decides if an action needs a session.
    pub fn is_person(&self) -> bool {
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
    /// A local-credential operation named an account that is not local.
    WrongAuthSource,
    /// The account moved on since the caller read it: disabled, or its generation bumped by a reset or a
    /// revocation.
    Superseded,
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountError::UsernameTaken => write!(f, "that username is already taken"),
            AccountError::NoSuchUser => write!(f, "no such user"),
            AccountError::LastAdministrator => write!(f, "this is the last local administrator account"),
            AccountError::WrongAuthSource => write!(f, "that account does not sign in with a password"),
            AccountError::Superseded => write!(f, "that account changed since it was read"),
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

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<String> for StoreError {
    fn from(message: String) -> Self {
        StoreError(message)
    }
}

impl From<StoreError> for String {
    fn from(e: StoreError) -> String {
        e.0
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

/// Blocking; every implementation is a local file. HTTP handlers reach it through `blocking`.
pub trait Store: Send + Sync {
    /// `code_hash` is already hashed; the store never sees a raw secret.
    fn create_enrollment_code(&self, code: &NewEnrollmentCode) -> Result<(), StoreError>;
    /// An enrollment code and the row saying who minted it, in one transaction. Like an account mutation and its audit
    /// row: an enrollment code that exists with nobody recorded as having asked for it is the worse of the two
    /// failures.
    fn create_enrollment_code_recorded(
        &self,
        code: &NewEnrollmentCode,
        event: &SecurityEvent,
    ) -> Result<(), StoreError>;
    /// Every enrollment code still usable, and every enrollment code that brought a worker in, newest first. The second
    /// is provenance: which workers arrived on an enrollment code outlives the enrollment code itself.
    fn list_enrollment_codes(&self, now_ms: i64) -> Result<Vec<EnrollmentCodeRecord>, StoreError>;
    /// `false` if no enrollment code has that id. A spent enrollment code is withdrawn like any other: what it already
    /// brought in stays enrolled, and what it would have brought in no longer can. The row stays, marked withdrawn:
    /// which workers it brought in is what an operator acts on next, and deleting it leaves `enrollment_code_uses` rows
    /// belonging to nothing.
    fn withdraw_enrollment_code(&self, id: &str, now_ms: i64) -> Result<bool, StoreError>;
    /// [`Store::withdraw_enrollment_code`] and the row saying who withdrew it, in one transaction.
    fn withdraw_enrollment_code_recorded(&self, id: &str, event: &SecurityEvent) -> Result<bool, StoreError>;
    /// Every enrollment code that can still bring a worker in, newest first.
    fn list_usable_enrollment_codes(&self, now_ms: i64) -> Result<Vec<UsableEnrollmentCode>, StoreError>;
    /// Withdraws every enrollment code that can still bring a worker in and names a root outside `may_have_named`, each
    /// with the row saying so, in one transaction. Answers with the ids withdrawn.
    ///
    /// An enrollment code that already brought its workers in is left alone: it enrolls nobody else whatever it names,
    /// and withdrawing it would rewrite the provenance an operator reads it for.
    ///
    /// `event` is asked for a row per id, because which enrollment codes these are is not known until the transaction
    /// has read them -- and an enrollment code withdrawn with nobody recorded as having withdrawn it is the hole
    /// [`Store::withdraw_enrollment_code_recorded`] exists to avoid.
    fn withdraw_enrollment_codes_not_naming(
        &self,
        may_have_named: &[&str],
        now_ms: i64,
        event: &dyn Fn(&str) -> SecurityEvent,
    ) -> Result<Vec<String>, StoreError>;
    /// Removes the credential this `token_hash` was issued for, where it is still the one `worker_id` holds.
    ///
    /// Removed rather than revoked: the token never reached the worker, so there is no identity to keep a record of,
    /// and a revoked row would occupy the worker id for good -- [`Store::spend_enrollment_code`] refuses one that has
    /// ever enrolled. The hash is in the predicate so this can only take back the row it was called about.
    fn remove_worker_credential(&self, worker_id: &str, token_hash: &str) -> Result<bool, StoreError>;
    /// Gives a spend of `code_id` back, as one transaction: the credential `token_hash` bought, the row in
    /// `enrollment_code_uses` recording who took it, and the use itself.
    ///
    /// For a spend this leader cannot vouch for right now rather than one it will never vouch for -- the worker is told
    /// to retry the very same enrollment code, and a use left spent while nothing was issued for it would make that
    /// retry fail on an enrollment code with none left, or collide with the row this spend already wrote on one that
    /// has.
    fn unspend_enrollment_code(&self, code_id: &str, worker_id: &str, token_hash: &str) -> Result<(), StoreError>;
    /// Two questions about the authority, and both are asked here so neither can be answered against a state that has
    /// moved. `presented` is the root the pasted enrollment code names, which must be the one it was minted against:
    /// that value rides in the clear beside the secret and only the secret is hashed, so a holder can edit one and not
    /// the other, and an enrollment code edited to name no root leaves its worker trusting every platform root in place
    /// of the one it was given. `may_have_named` are the roots an enrollment code minted against them can still be
    /// spent for, which is about this leader having moved rather than the enrollment code having been edited.
    ///
    /// Spends one use against `worker_id` and stores its credential, or says why it could not.
    ///
    /// One transaction, because "this enrollment code brought in this worker" and "this worker holds this credential"
    /// are one fact. Spending first and storing after would burn a use on a worker that never got a credential.
    ///
    /// A worker this same enrollment code already brought in, holding a credential nothing has used, is retrying an
    /// enrollment whose answer it never received. It gets `token_hash` in place of that credential, and no second
    /// use is spent: it is the same worker.
    fn spend_enrollment_code(
        &self,
        code_hash: &str,
        worker_id: &str,
        token_hash: &str,
        now_ms: i64,
        presented: &str,
        may_have_named: &[&str],
    ) -> Result<Spend, StoreError>;

    /// Errors if `worker_id` has ever enrolled, revoked or not. An enrollment code must not take over an existing
    /// worker's identity or resurrect a revoked one.
    fn store_worker_credential(&self, worker_id: &str, token_hash: &str) -> Result<(), StoreError>;
    fn get_worker_credential_by_hash(&self, token_hash: &str) -> Result<Option<WorkerCredentialRecord>, StoreError>;
    /// Records that this credential has authenticated a request, which ends the window in which a retried
    /// enrollment may replace it.
    fn mark_credential_used(&self, token_hash: &str) -> Result<(), StoreError>;
    /// `false` if `worker_id` has never enrolled.
    fn revoke_worker_credential(&self, worker_id: &str) -> Result<bool, StoreError>;
    /// Every worker that has ever enrolled, revoked or not, ordered by `worker_id`, less those forgotten or removed. The only
    /// record of a worker that enrolled and never heartbeated.
    fn list_worker_credentials(&self) -> Result<Vec<WorkerCredentialRecord>, StoreError>;
    /// Forgets an ephemeral worker that is gone: its credential, and what its enrollment code says it brought in, in
    /// one transaction. `false` where there was nothing to forget -- no such worker, a permanent one, or one revoked,
    /// which is kept so it stays out.
    fn forget_worker(&self, worker_id: &str) -> Result<bool, StoreError>;
    /// Removes a worker for good: revoked, and left out of [`Store::list_worker_credentials`] from now on. Its row
    /// stays, so its id is never enrolled again. `false` where no such worker has enrolled, or it is already removed.
    fn remove_worker(&self, worker_id: &str, now_ms: i64) -> Result<bool, StoreError>;

    // -- the secret catalog ------------------------------------------------
    //
    // No method here returns a value. Setting one, replacing it and asking whether a candidate matches is the whole
    // surface: reading one back needs a subject to permit it, and per-human identity is not built.

    /// Every secret the leader knows, ordered by name.
    fn list_secrets(&self) -> Result<Vec<SecretRecord>, StoreError>;

    /// Declares `name`. `value` is required for [`Custody::LeaderStored`] and refused for [`Custody::WorkerLocal`],
    /// whose value is never the leader's to hold. Errors if the name is taken or unusable as a name.
    fn create_secret(
        &self,
        name: &str,
        custody: Custody,
        description: &str,
        value: Option<&str>,
    ) -> Result<(), StoreError>;

    /// Seals `value` as a new version of `name` and returns its number -- what a bundle quotes to run it. Errors
    /// for an unknown name and for worker-local custody.
    ///
    /// Adds; it replaces nothing. Which older versions stop being kept is a policy decision the caller makes and
    /// applies with [`Store::delete_secret_version`], because it depends on what the fleet is running and the
    /// store cannot see that.
    fn add_secret_version(&self, name: &str, value: &str) -> Result<i64, StoreError>;

    /// `false` if `name` was not there. Takes every version with it.
    fn delete_secret(&self, name: &str) -> Result<bool, StoreError>;

    /// Forgets one version. `false` if that version was not held.
    fn delete_secret_version(&self, name: &str, version: i64) -> Result<bool, StoreError>;

    /// Whether `candidate` is the value stored as `version` -- the only question the leader answers about one, and
    /// what tells an admin a credential still needs rotating without showing it to them. `false` for an unknown
    /// name, an unheld version, and a row the leader's key cannot open.
    fn verify_secret(&self, name: &str, version: i64, candidate: &str) -> Result<bool, StoreError>;

    /// Whether the value at `name`/`version` is the private key `certificate` was issued for.
    ///
    /// A yes-or-no about a value, like `verify_secret`, and for the same reason: an outbound identity's certificate
    /// is committed and its key is not, so nothing but the leader can tell whether a bundle names two that belong
    /// together -- and a bundle that names two that do not deploys into a handshake nobody can explain.
    ///
    /// `false` for a value this leader cannot open, so an unopenable row is refused rather than waved through.
    fn certificate_matches(&self, name: &str, version: i64, certificate: &str) -> Result<bool, StoreError>;

    /// The values among `entitled` that a worker does not already hold, as name, version and value, for delivery on
    /// the heartbeat. `entitled` is the exact `(name, version)` pairs the worker's group's bundle quotes.
    ///
    /// The one method here that returns a value, and it cannot be asked for one: it answers "what does this group
    /// lack", never "what is the value of X". A pair that is worker-local, unknown, no longer held, or sealed under
    /// a key the leader no longer has is passed over, so it reaches the worker as a credential it does not have --
    /// the same fact as an unset variable, refusing the deploy and leaving the running one alone.
    fn secrets_for_delivery(
        &self,
        entitled: &[(String, i64)],
        held: &[(String, i64)],
    ) -> Result<Vec<(String, i64, String)>, StoreError>;

    /// Where the secrets encryption key stands against what is sealed under it.
    fn key_standing(&self) -> Result<KeyStanding, StoreError>;

    /// Writes a new secrets encryption key in place of one that is missing or does not match, and records the keys
    /// it replaces so the values they sealed read as values to enter again rather than as a wrong key. A
    /// mismatched key file is moved aside, not overwritten: it may yet be the right one for something. Refused
    /// while the key is not broken.
    fn regenerate_key(&self, event: &SecurityEvent) -> Result<KeyStanding, StoreError>;

    /// Every hue chosen so far, keyed by whatever id it was set for -- a real group's, or `"shared"`'s. What a new
    /// group's color is picked distinct from.
    fn group_hues(&self) -> Result<std::collections::HashMap<String, u16>, StoreError>;

    /// Chooses `group_id`'s color, in degrees, replacing whatever was there. Never refused: color rides on no
    /// group's existence, so setting one for an id that is `"shared"`, or one that never becomes a real group, is
    /// equally fine.
    fn set_group_hue(&self, group_id: &str, hue: u16) -> Result<(), StoreError>;

    // -- single-worker sources ------------------------------------------------
    //
    // Keyed by group as well as by source: every group runs its own bundle, so two groups reading one place are two
    // readers, each with its own progress.

    /// Which worker holds `key` in `group`, and the cursor it resumes from. `None` where no worker ever has.
    fn source_hold(&self, group: &str, key: &SourceKey) -> Result<Option<SourceHold>, StoreError>;

    /// Gives `key` in `group` to `holder`. Its progress stays, which is what the next holder resumes from.
    fn set_source_holder(&self, group: &str, key: &SourceKey, holder: &str) -> Result<(), StoreError>;

    /// Records what `key`'s holder has delivered, in one transaction: a cursor replaces the one kept, an added entry
    /// replaces its item's, and a removed item leaves the ledger. A key nobody holds keeps no cursor.
    fn apply_source_progress(&self, group: &str, key: &SourceKey, progress: &Progress) -> Result<(), StoreError>;

    /// Up to `limit` of `key`'s ledger entries in item order, starting after the item `after`. Read a page at a
    /// time, so a ledger of millions of items never holds the store for long.
    fn ledger_page(
        &self,
        group: &str,
        key: &SourceKey,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LedgerEntry>, StoreError>;

    /// Every hold, in every group and at every place, with its ledger's size. One read for the whole fleet, so a
    /// page polling it costs one query however many sources there are. The size is kept as the ledger changes, so
    /// reading it counts nothing.
    fn held_sources(&self) -> Result<Vec<HeldSource>, StoreError>;

    /// Forgets `key` in `group`: its hold, its cursor and its whole ledger, in one transaction. For a source no bundle
    /// reads any more; one that comes back starts afresh.
    fn forget_source(&self, group: &str, key: &SourceKey) -> Result<(), StoreError>;

    // -- people ------------------------------------------------------------
    //
    // A password hash is written and compared, never listed: only `local_credential_by_username` returns one, so
    // the roster the GUI and the audit read cannot carry a verifier by accident.

    /// Whether any account exists at all. What bootstrap asks before creating the first administrator; the answer
    /// is only ever a hint, since [`Store::bootstrap_administrator`] re-checks it inside its own transaction.
    fn any_user_exists(&self) -> Result<bool, StoreError>;

    /// Create the first administrator. `Ok(false)` when an account already existed, in which case nothing was
    /// written: the check and the insert are one transaction, so exactly one of two concurrent bootstraps wins.
    fn bootstrap_administrator(&self, user: NewLocalUser<'_>) -> Result<bool, StoreError>;

    /// Create an enabled local administrator alongside the accounts already there.
    ///
    /// `event` lands in the same transaction: an account that exists with no record of who created it is the one
    /// case an audit is for, so the two are one write or neither is.
    fn create_local_user(
        &self,
        user: NewLocalUser<'_>,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError>;

    fn list_users(&self) -> Result<Vec<UserRecord>, StoreError>;

    fn get_user(&self, id: &str) -> Result<Option<UserRecord>, StoreError>;

    /// The account to verify a login against, by canonical username. The one method that returns a verifier.
    fn local_credential_by_username(&self, username: &str) -> Result<Option<LocalCredentialRecord>, StoreError>;

    /// What a local account signs in as. Its own method rather than a field of [`UserRecord`], so reading a
    /// person's login name never reaches the table holding their password.
    fn username_of(&self, user_id: &str) -> Result<Option<String>, StoreError>;

    /// Replace a local account's password, set its must-change flag, and bump its generation -- one
    /// transaction, so no session survives a password it no longer matches. Answers the generation the account
    /// now stands at, which is what a session issued for this password must quote.
    ///
    /// `expect` is the generation the caller's authority was decided against. The write lands only while the
    /// account is still enabled and still there, and `Superseded` says it is not. `None` writes whatever it
    /// finds, which is what an administrator acting now does.
    ///
    /// `event` is written in the same transaction where there is one to write. A rehash at a higher work factor
    /// is not an event -- nothing about the account changed -- so that caller passes `None`.
    fn set_local_password(
        &self,
        user_id: &str,
        password_hash: &str,
        must_change: bool,
        expect: Option<i64>,
        event: Option<&SecurityEvent>,
    ) -> Result<Result<i64, AccountError>, StoreError>;

    /// Bump an account's generation, which refuses every session already issued for it.
    fn revoke_sessions(&self, user_id: &str, event: &SecurityEvent) -> Result<Result<(), AccountError>, StoreError>;

    /// Enable or disable an account. Disabling is refused when it would leave no enabled administrator holding a
    /// password -- checked inside the transaction, so two concurrent disables cannot remove the last one between
    /// them -- and moves the generation, so no session issued before it survives being re-enabled.
    fn set_user_enabled(
        &self,
        user_id: &str,
        enabled: bool,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError>;

    fn set_display_name(&self, user_id: &str, display_name: &str) -> Result<Result<(), AccountError>, StoreError>;

    /// The account the identity `(issuer, subject)` signs in as, creating it on its first sign-in, with `created`
    /// written in the same transaction. Every sign-in records its groups and time.
    ///
    /// The account comes back as stored, disabled or not: whether it may have a session is the caller's to judge,
    /// as it is for a local login.
    fn oidc_sign_in(&self, who: OidcSignIn<'_>, created: &SecurityEvent) -> Result<UserRecord, StoreError>;

    /// The identity an OIDC account signs in as. `None` for a local account or no account.
    fn oidc_identity_of(&self, user_id: &str) -> Result<Option<OidcIdentityRecord>, StoreError>;

    /// Record what a token refresh found the provider saying about the OIDC account `user_id`: its groups and login.
    /// A refresh is not a sign-in, so the time of the last sign-in stays. An account with no OIDC identity is left
    /// as it is.
    fn oidc_refreshed(&self, user_id: &str, groups: &[String], login: Option<&str>) -> Result<(), StoreError>;

    /// Re-enable a local account and give it a fresh must-change password, bumping its generation: what recovery
    /// from the host does, as one transaction.
    fn recover_local_user(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<Result<String, AccountError>, StoreError>;

    fn record_security_event(&self, event: &SecurityEvent) -> Result<(), StoreError>;

    /// Newest first, capped at `limit`.
    fn list_security_events(&self, limit: usize) -> Result<Vec<SecurityEvent>, StoreError>;

    /// What `user_id` lets their own agent use: the default until they have said.
    fn agent_permissions_of(&self, user_id: &str) -> Result<super::access::AgentPermissions, StoreError>;

    /// Replace what `user_id` lets their own agent use, with `event` written in the same transaction.
    fn put_agent_permissions(
        &self,
        user_id: &str,
        permissions: &super::access::AgentPermissions,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError>;

    /// Keep a new conversation, with no turns yet.
    fn create_conversation(&self, conversation: &ConversationRecord) -> Result<(), StoreError>;

    /// `user_id`'s conversations, the one added to last first.
    fn conversations_of(&self, user_id: &str) -> Result<Vec<ConversationRecord>, StoreError>;

    /// The conversation `id`, whoever's it is.
    fn conversation(&self, id: &str) -> Result<Option<ConversationRecord>, StoreError>;

    /// The turns of `conversation_id`, in the order said, as [`Store::append_turn`] was given them.
    fn turns_of(&self, conversation_id: &str) -> Result<Vec<String>, StoreError>;

    /// Add `turn` after the last of `conversation_id`'s, and mark it touched `at`. False when there is no such
    /// conversation, deleted while its turn ran.
    fn append_turn(&self, conversation_id: &str, turn: &str, at: i64) -> Result<bool, StoreError>;

    /// Remove a conversation and its turns. False when there was none.
    fn delete_conversation(&self, id: &str) -> Result<bool, StoreError>;

    /// Remove every conversation not added to since `before`, in milliseconds since the epoch. How many went.
    fn delete_conversations_untouched_since(&self, before: i64) -> Result<usize, StoreError>;

    /// Every role, by name.
    fn list_roles(&self) -> Result<Vec<super::access::Role>, StoreError>;

    /// Create `role`, or replace the one of that name whole, with `event` written in the same transaction.
    fn put_role(&self, role: &super::access::Role, event: &SecurityEvent) -> Result<(), StoreError>;

    /// Remove the role called `name`, with `event` written in the same transaction. False when there was none.
    fn delete_role(&self, name: &str, event: &SecurityEvent) -> Result<bool, StoreError>;

    /// What an operator has configured, or `None` where they have not. Absent means the default applies, which is
    /// a different thing from a value they chose that happens to equal it.
    fn setting(&self, key: &str) -> Result<Option<String>, StoreError>;

    /// Write several settings as one transaction. A tab saves together or not at all: half of a pair that
    /// constrains itself is worse than neither half.
    fn put_settings(&self, entries: &[(&str, String)], event: Option<&SecurityEvent>) -> Result<(), StoreError>;

    /// `value` sealed under the leader's key and bound to the setting `key`, as the text [`Store::put_settings`]
    /// stores for it. For a setting that is a credential.
    fn sealed_setting(&self, key: &str, value: &str) -> Result<String, StoreError>;

    /// The setting `key`, written by [`Store::sealed_setting`], opened. `None` when unset, or sealed by a key this
    /// leader does not hold.
    fn opened_setting(&self, key: &str) -> Result<Option<String>, StoreError>;
}
