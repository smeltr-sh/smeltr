//! `Store` on one SQLite file, one connection behind a `Mutex`. Every write is an operator action, never a
//! per-heartbeat one, but a single-worker source's hold and progress: those are what its holder's heartbeat says.
//!
//! The secrets encryption key is not in this file and not in the leader's data directory (`secrets`). It is read per
//! operation rather than held: operator actions are rare, and a key that is not resident is not in a core dump.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{Connection, ErrorCode, OptionalExtension};

use super::access::{AgentPermissions, Level, Role};
use super::secrets::{self, EncryptionKey, Sealed};
use super::store::{
    AccountError, AuthSource, ConversationRecord, Custody, EnrollmentCodeRecord, HeldSource, Issued, KeyStanding,
    KeyState, LocalCredentialRecord, NewEnrollmentCode, NewLocalUser, OidcIdentityRecord, OidcSignIn, SecretRecord,
    SecurityEvent, SourceHold, Spend, Store, StoreError, StoredValue, Unopenable, UsableEnrollmentCode, UserRecord,
    WorkerCredentialRecord, WorkerKind,
};
use crate::heartbeat::{LedgerEntry, SourceKey};
use crate::io::Progress;

/// The setting that lists the keys a regeneration replaced, comma-separated.
const RETIRED_KEY_IDS: &str = "secrets.retired_key_ids";

pub struct SqliteStore {
    conn: Mutex<Connection>,
    secret_key: PathBuf,
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> StoreError {
        StoreError(e.to_string())
    }
}

impl From<secrets::KeyError> for StoreError {
    fn from(e: secrets::KeyError) -> StoreError {
        StoreError(e.0)
    }
}

/// Keep the database readable by its owner and nobody else.
///
/// It holds password verifiers, so a copy of it can be attacked offline. On a host with the usual `022` umask SQLite would create it mode `0644`, and an upgraded install keeps
/// whatever mode it already had -- so this runs on every open rather than only at creation, and tightens what it
/// finds instead of trusting it.
///
/// The sidecars matter as much as the file: `-wal` holds pages not yet checkpointed into it, which is to say
/// recently written rows. The directory goes with them, since a mode on a file nobody can reach is not what keeps
/// it private, and one anybody can list gives away every secret's name.
///
/// Unix only. The Windows half below is a no-op, and says why.
#[cfg(unix)]
fn make_private(path: &Path) -> Result<(), StoreError> {
    // A database with no file behind it, which the tests use. There is nothing to protect and no parent to fix.
    if path.as_os_str() == ":memory:" {
        return Ok(());
    }
    let failed = |what: &Path, e: std::io::Error| StoreError(format!("{}: {e}", what.display()));

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        set_mode_if_looser(parent, 0o700).map_err(|e| failed(parent, e))?;
    }
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let sidecar = std::path::PathBuf::from(name);
        if sidecar.exists() {
            set_mode_if_looser(&sidecar, 0o600).map_err(|e| failed(&sidecar, e))?;
        }
    }
    Ok(())
}

/// Narrow `path` to `wanted` when anyone but its owner can reach it. A mode already at least as tight is left
/// alone, so an operator who chose something stricter keeps it.
#[cfg(unix)]
fn set_mode_if_looser(path: &Path, wanted: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = std::fs::metadata(path)?.permissions();
    if perms.mode() & 0o077 != 0 {
        perms.set_mode(wanted);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// **On Windows the database is not protected by anything here.**
///
/// Its access is whatever the data directory's ACL grants, inherited from wherever that directory was created.
/// Under a user profile that is usually the owner alone; under a path like `C:\smeltr` it is commonly
/// `Users:(RX)`, which is every local account able to read the password verifiers.
///
/// `std::fs::set_permissions` cannot close this: on Windows it toggles the read-only flag and says nothing about
/// who may read. Doing it properly means writing a DACL that grants the owner and the SYSTEM account only, which needs the
/// Win32 API rather than `std::fs`. Until that exists this is a no-op that returns `Ok`, and saying so is the
/// point -- the secrets encryption key has the same hole (`secrets.rs`), and neither should read as though it were
/// handled.
#[cfg(not(unix))]
fn make_private(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

// -- schema -----------------------------------------------------------------

/// Every step this file has taken, oldest first (`steps`). Append; never edit one that has shipped.
pub(crate) const STEPS: &[&str] = &[
    // 1 -- the schema of the first release.
    "CREATE TABLE enrollment_codes (
         id           TEXT PRIMARY KEY,
         code_hash    TEXT NOT NULL UNIQUE,
         -- SHA-256 of the one certificate a worker taking this enrollment code will trust. Empty where the leader
         -- is verified by the platform's own roots, which is what a proxy holding a public certificate needs.
         fingerprint  TEXT NOT NULL DEFAULT '',
         group_id     TEXT NOT NULL DEFAULT '',
         minted_by    TEXT NOT NULL,
         minted_at    INTEGER NOT NULL,
         -- NULL is no expiry and no cap: an ephemeral enrollment code has neither, since an autoscaler spends it
         -- for as long as it runs.
         expires_at   INTEGER,
         max_uses     INTEGER,
         uses         INTEGER NOT NULL DEFAULT 0,
         -- A withdrawn enrollment code is marked rather than removed: which workers a leaked one brought in is the
         -- one thing an operator needs after withdrawing it.
         withdrawn_at INTEGER,
         kind         TEXT NOT NULL DEFAULT 'permanent' CHECK (kind IN ('permanent', 'ephemeral'))
     );
     -- Which workers each enrollment code brought in. One row per use, because an enrollment code minted for a
     -- fleet brings in many, and the list is what an operator revokes against when an enrollment code leaks.
     CREATE TABLE enrollment_code_uses (
         code_id   TEXT NOT NULL,
         worker_id TEXT NOT NULL,
         used_at   INTEGER NOT NULL,
         PRIMARY KEY (code_id, worker_id)
     );
     CREATE TABLE worker_credentials (
         worker_id  TEXT PRIMARY KEY,
         token_hash TEXT NOT NULL UNIQUE,
         revoked    INTEGER NOT NULL DEFAULT 0,
         -- Whether the credential has ever authenticated a request. One nothing has used may be replaced by a
         -- retry of the enrollment that issued it: the answer carrying it can be lost after the leader wrote it
         -- down. A credential written without saying counts as used, so none is replaced on a guess.
         used       INTEGER NOT NULL DEFAULT 1,
         -- The kind of the enrollment code that brought the worker in, for its life.
         kind       TEXT NOT NULL DEFAULT 'permanent' CHECK (kind IN ('permanent', 'ephemeral'))
     );
     -- `next_version` counts up and never down, so a number is never reused after its version is forgotten: a
     -- bundle still quoting it reads as a credential this leader cannot produce, never as a different value
     -- wearing the same number.
     CREATE TABLE secrets (
         name         TEXT PRIMARY KEY,
         custody      TEXT NOT NULL,
         description  TEXT NOT NULL DEFAULT '',
         next_version INTEGER NOT NULL DEFAULT 1
     );
     -- One row per stored value. A worker-local secret has none here at all, which is what `custody` says in one
     -- word; a leader-stored one has at least the version it was created with.
     CREATE TABLE secret_versions (
         name        TEXT NOT NULL,
         version     INTEGER NOT NULL,
         set_at      INTEGER NOT NULL,
         key_id      TEXT NOT NULL,
         nonce       BLOB NOT NULL,
         ciphertext  BLOB NOT NULL,
         PRIMARY KEY (name, version)
     );
     CREATE TABLE group_colors (
         group_id TEXT PRIMARY KEY,
         hue      INTEGER NOT NULL
     );
     CREATE TABLE users (
         id            TEXT PRIMARY KEY,
         auth_source   TEXT NOT NULL,
         display_name  TEXT NOT NULL,
         enabled       INTEGER NOT NULL DEFAULT 1,
         administrator INTEGER NOT NULL DEFAULT 1,
         generation    INTEGER NOT NULL DEFAULT 1,
         created_at    INTEGER NOT NULL,
         updated_at    INTEGER NOT NULL,
         -- What the composite key below references: an account's source cannot change while a credential
         -- hangs off it.
         UNIQUE (id, auth_source)
     );
     -- A password belongs to a local account and to no other kind. The foreign key carries `auth_source` so that
     -- is a property of the schema rather than of the code above it: no future OIDC account can acquire a
     -- password, and no password fallback can be added without a step that says so out loud.
     CREATE TABLE local_credentials (
         user_id       TEXT PRIMARY KEY,
         auth_source   TEXT NOT NULL CHECK (auth_source = 'local'),
         username      TEXT NOT NULL UNIQUE,
         password_hash TEXT NOT NULL,
         must_change   INTEGER NOT NULL DEFAULT 0,
         FOREIGN KEY (user_id, auth_source) REFERENCES users (id, auth_source)
     );
     -- Append-only. A disabled account keeps its rows, which is why an account is disabled rather than deleted
     -- and why a username is never released.
     CREATE TABLE security_events (
         id      TEXT PRIMARY KEY,
         at      INTEGER NOT NULL,
         actor   TEXT NOT NULL,
         action  TEXT NOT NULL,
         target  TEXT NOT NULL DEFAULT '',
         outcome TEXT NOT NULL
     );
     CREATE INDEX security_events_at ON security_events (at DESC);
     -- What an operator has configured about this leader itself. One table rather than a column per setting: the
     -- next setting is a row, and a setting nobody has touched is an absent row rather than a null to tell apart
     -- from a deliberate zero.
     CREATE TABLE settings (
         key   TEXT PRIMARY KEY,
         value TEXT NOT NULL
     );",
    // 2 -- a worker removed for good keeps its row, so its id is never enrolled again.
    "ALTER TABLE worker_credentials ADD COLUMN removed_at INTEGER;",
    // 3 -- who holds each single-worker source, and what it has delivered (`holds`).
    "CREATE TABLE source_holds (
         grp    TEXT NOT NULL,
         source TEXT NOT NULL,
         place  TEXT NOT NULL,
         holder TEXT NOT NULL,
         -- NULL until the source names a position. One that keeps a ledger never does.
         cursor TEXT,
         PRIMARY KEY (grp, source, place)
     );
     -- One row per item a source still holds and has read. An item leaves when its holder says the source no
     -- longer has it, so the ledger is as large as the source and no larger.
     CREATE TABLE source_ledger (
         grp    TEXT NOT NULL,
         source TEXT NOT NULL,
         place  TEXT NOT NULL,
         item   TEXT NOT NULL,
         entry  TEXT NOT NULL,
         PRIMARY KEY (grp, source, place, item)
     );",
    // 4 -- how many items each hold's ledger keeps, so the map reads a size without counting a ledger. Triggers keep
    // it, so every write to the ledger counts, whatever build makes it.
    "ALTER TABLE source_holds ADD COLUMN ledger_entries INTEGER NOT NULL DEFAULT 0;
     UPDATE source_holds SET ledger_entries = (
         SELECT COUNT(*) FROM source_ledger l
          WHERE l.grp = source_holds.grp AND l.source = source_holds.source AND l.place = source_holds.place);
     CREATE TRIGGER source_ledger_added AFTER INSERT ON source_ledger BEGIN
         UPDATE source_holds SET ledger_entries = ledger_entries + 1
          WHERE grp = NEW.grp AND source = NEW.source AND place = NEW.place;
     END;
     CREATE TRIGGER source_ledger_removed AFTER DELETE ON source_ledger BEGIN
         UPDATE source_holds SET ledger_entries = ledger_entries - 1
          WHERE grp = OLD.grp AND source = OLD.source AND place = OLD.place;
     END;",
    // 5 -- accounts that sign in through an OpenID provider. Breaking: a build without this step reads an 'oidc'
    // users row as local (`read_user`), so it must never open a file that may hold one.
    "-- One OIDC identity is one account. The foreign key carries `auth_source`, as `local_credentials` does, so an
     -- identity hangs only off an OIDC account and that account never gains a password.
     CREATE TABLE oidc_identities (
         user_id      TEXT PRIMARY KEY,
         auth_source  TEXT NOT NULL CHECK (auth_source = 'oidc'),
         issuer       TEXT NOT NULL,
         subject      TEXT NOT NULL,
         -- The groups claim as a JSON array of strings, as of the last sign-in.
         groups       TEXT NOT NULL DEFAULT '[]',
         last_sign_in INTEGER NOT NULL,
         UNIQUE (issuer, subject),
         FOREIGN KEY (user_id, auth_source) REFERENCES users (id, auth_source)
     );",
    // 6 -- what the provider calls an OIDC account, for the roster. Additive: nothing older reads it.
    "-- The provider's verified email, else its preferred username, as of the last sign-in.
     ALTER TABLE oidc_identities ADD COLUMN login TEXT;",
    // 7 -- roles: what each provider group holds, per area (`access`). Breaking: from here an OIDC account's access
    // comes from its roles and never from `users.administrator`, which it clears, so a build that still reads the
    // flag must not open the file.
    "CREATE TABLE roles (
         name          TEXT PRIMARY KEY,
         administrator INTEGER NOT NULL DEFAULT 0 CHECK (administrator IN (0, 1))
     );
     -- An area a role does not name is one it gives nothing in.
     CREATE TABLE role_areas (
         role  TEXT NOT NULL REFERENCES roles (name) ON DELETE CASCADE,
         area  TEXT NOT NULL,
         level TEXT NOT NULL CHECK (level IN ('viewer', 'editor', 'deployer')),
         PRIMARY KEY (role, area)
     );
     UPDATE users SET administrator = 0 WHERE auth_source = 'oidc';",
    // 8 -- autonomous agents, and what each person lets their own agent use (`access::AgentPermissions`). Breaking: a build
    // without this step reads an 'agent' account as local, and a local account is an administrator.
    "CREATE TABLE agent_accounts (
         user_id     TEXT PRIMARY KEY,
         auth_source TEXT NOT NULL CHECK (auth_source = 'agent'),
         -- The roles an administrator gave it, by name, as a JSON array: what an OIDC account's groups are to it.
         roles       TEXT NOT NULL DEFAULT '[]',
         FOREIGN KEY (user_id, auth_source) REFERENCES users (id, auth_source)
     );
     -- What each person lets their own agent use (`access::AgentPermissions`, as JSON). A person with no row gives the default.
     CREATE TABLE agent_permissions (
         user_id TEXT PRIMARY KEY REFERENCES users (id),
         permissions TEXT NOT NULL
     );",
    // 9 -- what each person and their agent said to each other (`leader::agent`). Removed once untouched for the
    // setting's number of days. No foreign keys, which would make the step breaking: deleting a conversation deletes
    // its turns in the same transaction.
    "CREATE TABLE agent_conversations (
         id         TEXT PRIMARY KEY,
         user_id    TEXT NOT NULL,
         title      TEXT NOT NULL,
         created_at INTEGER NOT NULL,
         updated_at INTEGER NOT NULL
     );
     CREATE INDEX agent_conversations_by_user ON agent_conversations (user_id, updated_at);
     -- Each turn as `leader::model::Turn` writes it in JSON, in the order said.
     CREATE TABLE agent_turns (
         conversation_id TEXT NOT NULL,
         seq             INTEGER NOT NULL,
         turn            TEXT NOT NULL,
         PRIMARY KEY (conversation_id, seq)
     );",
    // 10 -- autonomous agents are gone: only a person's own agent remains. Breaking: a build with them reads
    // `agent_accounts`, which this drops.
    "DROP TABLE agent_accounts;
     DELETE FROM agent_permissions WHERE user_id IN (SELECT id FROM users WHERE auth_source = 'agent');
     DELETE FROM users WHERE auth_source = 'agent';",
    // 11 -- the public chain of a certificate, beside each version of the secret that holds its private key. A
    // secret with chains is a certificate. Additive: an older build lists it as the secret it also is, and a chain
    // whose version an older build deleted is read as nothing, since every read joins it to its version.
    "CREATE TABLE certificate_chains (
         name    TEXT NOT NULL,
         version INTEGER NOT NULL,
         chain   TEXT NOT NULL,
         PRIMARY KEY (name, version)
     );",
    // 12 -- the names a certificate this leader issues is issued for, as a JSON array, and how many days each version
    // is valid for, which are what each renewal issues it with again. A certificate with a row here is the leader's
    // to renew. Additive: an older build lists it as the certificate it also is, and never renews it.
    "CREATE TABLE issued_certificates (
         name          TEXT PRIMARY KEY,
         names         TEXT NOT NULL,
         validity_days INTEGER NOT NULL
     );",
];

fn conversation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationRecord> {
    Ok(ConversationRecord {
        id: row.get(0)?,
        user_id: row.get(1)?,
        title: row.get(2)?,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

/// How thoroughly [`damage`] reads a file.
pub(super) enum Check {
    /// Every page and row, but not whether each index holds what its table does: for a file that can be had again.
    Quick,
    /// Indexes against their tables as well, at the cost of a sort per index: for a file with no other copy.
    Integrity,
}

/// What `check` finds wrong with the file `conn` has open, its first few findings joined; `None` when it finds
/// nothing. A file SQLite cannot read as a database at all is damage too, not an error.
pub(super) fn damage(conn: &Connection, check: Check) -> Result<Option<String>, rusqlite::Error> {
    let pragma = match check {
        Check::Quick => "PRAGMA quick_check(5)",
        Check::Integrity => "PRAGMA integrity_check(5)",
    };
    let found = conn.prepare(pragma).and_then(|mut stmt| {
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()
    });
    match found {
        Ok(lines) if lines == ["ok"] => Ok(None),
        Ok(lines) => Ok(Some(lines.join("; "))),
        Err(e)
            if matches!(
                e.sqlite_error_code(),
                Some(ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt)
            ) =>
        {
            Ok(Some(e.to_string()))
        }
        Err(e) => Err(e),
    }
}

/// The check a start gives `leader.db`. A start that will run steps gets the full one: the steps rewrite the file,
/// so damage found later could not be told from a step's doing, and the copy that start keeps is the last in the
/// old format. Any other start gets the quick one, which reads every page.
fn check_for(conn: &Connection) -> Check {
    let applied: Result<i64, _> = conn.pragma_query_value(None, "user_version", |row| row.get(0));
    match applied {
        Ok(applied) if applied >= STEPS.len() as i64 => Check::Quick,
        _ => Check::Integrity,
    }
}

/// Where each start keeps the copy of a sound `leader.db`, for a later start that finds it damaged.
pub fn previous_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".previous");
    PathBuf::from(name)
}

/// A consistent copy of the file `conn` has open, at [`previous_path`]. Owner-only from the moment it exists, since
/// it holds what `leader.db` holds, and renamed into place so a start stopped part way leaves the last copy whole.
fn keep_previous(conn: &Connection, path: &Path) -> Result<(), String> {
    if path.as_os_str() == ":memory:" {
        return Ok(());
    }
    let previous = previous_path(path);
    let mut partial = previous.clone().into_os_string();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    let _ = std::fs::remove_file(&partial);
    // `VACUUM INTO` writes into an empty file, so the mode is set before any byte lands.
    drop(super::private_file::create(&partial).map_err(|e| format!("{}: {e}", partial.display()))?);
    let into = partial
        .to_str()
        .ok_or_else(|| format!("{} is not UTF-8", partial.display()))?;
    conn.execute("VACUUM INTO ?1", [into]).map_err(|e| {
        let _ = std::fs::remove_file(&partial);
        e.to_string()
    })?;
    std::fs::rename(&partial, &previous).map_err(|e| format!("{}: {e}", previous.display()))
}

/// What a start that finds `path` damaged says: the copy the last sound start kept, when there is one, and how old.
fn refusal(path: &Path, found: &str) -> String {
    let previous = previous_path(path);
    let age = std::fs::metadata(&previous)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|at| at.elapsed().ok());
    let copy = match age {
        Some(age) => format!(
            "{}, the copy the last start that found it sound kept {} ago, or a backup of your own",
            previous.display(),
            spoken_age(age)
        ),
        None => "a backup of your own".to_string(),
    };
    format!(
        "{} is damaged ({found}). Put {copy} in its place, remove its -wal and -shm files, and start the leader \
         again. What changed since that copy was made is lost, and sources resume from the positions it held.",
        path.display()
    )
}

fn spoken_age(age: std::time::Duration) -> String {
    let hours = age.as_secs() / 3600;
    match hours {
        0 => format!("{} minutes", age.as_secs() / 60),
        1..48 => format!("{hours} hours"),
        _ => format!("{} days", hours / 24),
    }
}

impl SqliteStore {
    /// `secret_key` is where the key that seals stored values lives, written the first time one is stored.
    /// A damaged file is refused: nothing else holds what it holds.
    pub fn open(path: &Path, secret_key: &Path) -> Result<SqliteStore, StoreError> {
        let mut conn = Connection::open(path)?;
        // First, so the leader never acts on what a damaged file says.
        if let Some(found) = damage(&conn, check_for(&conn))? {
            return Err(StoreError(refusal(path, &found)));
        }
        // Before the steps, so it is the file as this start found it, in the format the last build left.
        if let Err(e) = keep_previous(&conn, path) {
            tracing::warn!(
                target: "leader.store_not_copied",
                path = %path.display(),
                error = %e,
                "the copy of leader.db kept for a damaged start was not made"
            );
        }
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        // After WAL is on, so the sidecar files it creates are covered too.
        make_private(path)?;
        super::steps::upgrade_sqlite(&mut conn, "leader.db", STEPS).map_err(StoreError)?;
        Ok(SqliteStore {
            conn: Mutex::new(conn),
            secret_key: secret_key.to_path_buf(),
        })
    }

    /// The key, writing one if there is none and nothing is sealed yet. Refused while the key is broken: a value
    /// sealed then is one the restored key could not open.
    fn key_for_sealing(&self) -> Result<EncryptionKey, StoreError> {
        if let Some(problem) = self.key_standing()?.problem() {
            return Err(StoreError(problem));
        }
        Ok(secrets::load_or_create(&self.secret_key)?)
    }

    /// The key ids every sealed value carries, and which value each newest copy is, keyed the way
    /// [`Unopenable`] names it.
    fn sealed_by(&self) -> Result<Vec<(Unopenable, String)>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut newest = conn.prepare(
            "SELECT v.name, v.key_id,
                    EXISTS (SELECT 1 FROM certificate_chains c WHERE c.name = v.name AND c.version = v.version)
               FROM secret_versions v
              WHERE v.version = (SELECT MAX(version) FROM secret_versions WHERE name = v.name)
              ORDER BY v.name",
        )?;
        let mut out = newest
            .query_map((), |row| {
                let name = row.get(0)?;
                let value = if row.get(2)? {
                    Unopenable::Certificate(name)
                } else {
                    Unopenable::Secret(name)
                };
                Ok((value, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        // A sealed setting is told apart by its shape: nothing else a setting holds is a key id, a nonce of the
        // one length a sealed value carries, and hex, separated by colons.
        let mut settings = conn.prepare("SELECT key, value FROM settings ORDER BY key")?;
        for row in settings.query_map((), |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))? {
            let (key, value) = row?;
            if let Some(sealed) = Sealed::from_text(&value) {
                out.push((Unopenable::Setting(key), sealed.key_id().to_string()));
            }
        }
        Ok(out)
    }

    /// Every key id any stored version carries, older versions included.
    fn every_key_id(&self) -> Result<std::collections::BTreeSet<String>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT DISTINCT key_id FROM secret_versions")?;
        Ok(stmt
            .query_map((), |row| row.get::<_, String>(0))?
            .collect::<Result<_, _>>()?)
    }

    /// The keys a regeneration replaced, whose values read as values to enter again.
    fn retired_key_ids(&self) -> Result<std::collections::BTreeSet<String>, StoreError> {
        Ok(self
            .setting(RETIRED_KEY_IDS)?
            .map(|ids| ids.split(',').filter(|id| !id.is_empty()).map(str::to_string).collect())
            .unwrap_or_default())
    }

    /// The key if there is one. Absent means every stored value is unusable, which is a fact to report rather than
    /// an error, and never a reason to write a key.
    fn key_if_any(&self) -> Result<Option<EncryptionKey>, StoreError> {
        Ok(secrets::load(&self.secret_key)?)
    }

    /// The sealed value stored as `(name, version)`, or `None` when that version is not held.
    fn sealed(conn: &Connection, name: &str, version: i64) -> Result<Option<Sealed>, StoreError> {
        let mut stmt =
            conn.prepare("SELECT key_id, nonce, ciphertext FROM secret_versions WHERE name = ?1 AND version = ?2")?;
        let row = stmt
            .query_row((name, version), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .optional()?;
        let Some((key_id, nonce, ciphertext)) = row else {
            return Ok(None);
        };
        Sealed::from_parts(key_id, &nonce, ciphertext).map(Some).ok_or_else(|| {
            StoreError(format!(
                "secret {name:?} v{version}: its stored nonce is the wrong length"
            ))
        })
    }

    fn insert_version(
        conn: &Connection,
        name: &str,
        version: i64,
        sealed: &Sealed,
        chain: Option<&str>,
    ) -> Result<(), StoreError> {
        conn.execute(
            "INSERT INTO secret_versions (name, version, set_at, key_id, nonce, ciphertext)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                name,
                version,
                super::warnings::now_millis(),
                sealed.key_id(),
                sealed.nonce(),
                sealed.ciphertext(),
            ],
        )?;
        if let Some(chain) = chain {
            conn.execute(
                "INSERT INTO certificate_chains (name, version, chain) VALUES (?1, ?2, ?3)",
                (name, version, chain),
            )?;
        }
        Ok(())
    }

    /// Declares `name` with its first value where it is leader-stored, and that value's certificate chain where it
    /// is a certificate's private key.
    fn create(
        &self,
        name: &str,
        custody: Custody,
        description: &str,
        value: Option<&str>,
        chain: Option<&str>,
        issued_for: Option<&Issued>,
    ) -> Result<(), StoreError> {
        if !crate::config_file::is_safe_secret_name(name) {
            return Err(StoreError(crate::config_file::unsafe_secret_name(name)));
        }
        // Sealed before the row is written, so a key that cannot be created leaves no half-made secret behind.
        let sealed = match (custody, value) {
            (Custody::LeaderStored, Some(value)) => Some(self.key_for_sealing()?.seal(name, value.as_bytes())),
            (Custody::LeaderStored, None) => {
                return Err(StoreError(format!(
                    "secret {name:?} is leader-stored, so it needs a value"
                )));
            }
            (Custody::WorkerLocal, None) => None,
            (Custody::WorkerLocal, Some(_)) => {
                return Err(StoreError(format!(
                    "secret {name:?} is worker-local, so its value is never the leader's to hold"
                )));
            }
        };

        let mut conn = self.conn.lock().unwrap();
        // One transaction: a secret that exists with no version would be a leader-stored credential the fleet can
        // never obtain, and nothing would say why.
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO secrets (name, custody, description, next_version) VALUES (?1, ?2, ?3, 1)",
            (name, custody.as_str(), description),
        )?;
        if inserted == 0 {
            return Err(StoreError(format!("secret {name:?} already exists")));
        }
        if let Some(sealed) = &sealed {
            let version = Self::take_version(&tx, name)?.expect("the row this transaction just inserted");
            Self::insert_version(&tx, name, version, sealed, chain)?;
        }
        if let Some(issued) = issued_for {
            let names = serde_json::to_string(&issued.names).map_err(|e| StoreError(e.to_string()))?;
            tx.execute(
                "INSERT INTO issued_certificates (name, names, validity_days) VALUES (?1, ?2, ?3)",
                (name, names, issued.validity_days),
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Seals `value` as a new version of `name`, with its certificate chain where it is a certificate's private key.
    fn add_version(&self, name: &str, value: &str, chain: Option<&str>) -> Result<i64, StoreError> {
        let sealed = self.key_for_sealing()?.seal(name, value.as_bytes());
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let version = Self::take_version(&tx, name)?
            .ok_or_else(|| StoreError(format!("no leader-stored secret named {name:?}")))?;
        Self::insert_version(&tx, name, version, &sealed, chain)?;
        tx.commit()?;
        Ok(version)
    }

    /// Takes the next version number for `name` and advances the counter, in one statement so two rotations at
    /// once cannot both take it. `None` when there is no such leader-stored secret.
    fn take_version(conn: &Connection, name: &str) -> Result<Option<i64>, StoreError> {
        let mut stmt = conn.prepare(
            "UPDATE secrets SET next_version = next_version + 1
              WHERE name = ?1 AND custody = 'leader-stored'
          RETURNING next_version - 1",
        )?;
        Ok(stmt.query_row((name,), |row| row.get(0)).optional()?)
    }
}

/// Marks an enrollment code withdrawn, once. `withdrawn_at IS NULL` in the predicate is what makes a second withdrawal
/// answer "already gone" rather than moving the date.
const WITHDRAW_CODE: &str = "UPDATE enrollment_codes SET withdrawn_at = ?2 WHERE id = ?1 AND withdrawn_at IS NULL";

/// What makes an enrollment code one that can still bring a worker in. Spliced into the two queries that ask it, so the
/// listing a warning is derived from and the withdrawal a cutover performs cannot come to mean different things.
/// Named rather than positional, because their other bindings differ.
const USABLE_CODE: &str = "withdrawn_at IS NULL AND (expires_at IS NULL OR expires_at > :now) \
                           AND (max_uses IS NULL OR uses < max_uses)";

/// The order both of those answer in: an operator reads the one they just minted first.
const NEWEST_FIRST: &str = "ORDER BY minted_at DESC, id";

fn insert_enrollment_code(conn: &Connection, code: &NewEnrollmentCode) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO enrollment_codes
                (id, code_hash, fingerprint, group_id, minted_by, minted_at, expires_at, max_uses, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            &code.id,
            &code.code_hash,
            &code.fingerprint,
            &code.group_id,
            &code.minted_by,
            code.minted_at,
            code.expires_at,
            code.max_uses,
            code.kind.as_str(),
        ),
    )?;
    Ok(())
}

/// What spending an enrollment code reads of the one its secret names.
struct Presented {
    id: String,
    expires_at: Option<i64>,
    withdrawn_at: Option<i64>,
    fingerprint: String,
    kind: WorkerKind,
}

/// The worker kind in column `at`. The schema's own check admits nothing else.
fn kind_at(row: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<WorkerKind> {
    let kind: String = row.get(at)?;
    WorkerKind::parse(&kind).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            at,
            rusqlite::types::Type::Text,
            format!("{kind:?} is not a worker kind").into(),
        )
    })
}

/// The columns [`credential_row`] reads, in its order, for a query over `worker_credentials`. When a worker was
/// brought in is its enrollment code's to say.
const CREDENTIAL_COLUMNS: &str = "worker_id, token_hash, revoked, used, kind,
    (SELECT MIN(used_at) FROM enrollment_code_uses
      WHERE enrollment_code_uses.worker_id = worker_credentials.worker_id)";

/// A `worker_credentials` row selected as [`CREDENTIAL_COLUMNS`].
fn credential_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerCredentialRecord> {
    Ok(WorkerCredentialRecord {
        worker_id: row.get(0)?,
        token_hash: row.get(1)?,
        revoked: row.get::<_, i64>(2)? != 0,
        used: row.get::<_, i64>(3)? != 0,
        kind: kind_at(row, 4)?,
        enrolled_at: row.get(5)?,
    })
}

/// The workers an enrollment code brought in, oldest first.
fn used_by(conn: &Connection, code_id: &str) -> Result<Vec<String>, StoreError> {
    let mut statement =
        conn.prepare("SELECT worker_id FROM enrollment_code_uses WHERE code_id = ?1 ORDER BY used_at, worker_id")?;
    Ok(statement
        .query_map((code_id,), |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?)
}

impl Store for SqliteStore {
    fn create_enrollment_code(&self, code: &NewEnrollmentCode) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        insert_enrollment_code(&conn, code)
    }

    fn create_enrollment_code_recorded(
        &self,
        code: &NewEnrollmentCode,
        event: &SecurityEvent,
    ) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        insert_enrollment_code(&tx, code)?;
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(())
    }

    fn list_enrollment_codes(&self, now_ms: i64) -> Result<Vec<EnrollmentCodeRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        // Still usable, or it brought somebody in. The second half is the provenance: which workers arrived on an
        // enrollment code is the question that outlives the enrollment code, and one that expired unused answers
        // nothing.
        let mut statement = conn.prepare(&format!(
            "SELECT id, fingerprint, group_id, minted_by, minted_at, expires_at, max_uses, uses, withdrawn_at, kind
               FROM enrollment_codes
              WHERE ({USABLE_CODE}) OR uses > 0
              {NEWEST_FIRST}"
        ))?;
        let rows: Vec<EnrollmentCodeRecord> = statement
            .query_map(rusqlite::named_params! { ":now": now_ms }, |row| {
                Ok(EnrollmentCodeRecord {
                    id: row.get(0)?,
                    fingerprint: row.get(1)?,
                    group_id: row.get(2)?,
                    minted_by: row.get(3)?,
                    minted_at: row.get(4)?,
                    expires_at: row.get(5)?,
                    max_uses: row.get(6)?,
                    uses: row.get(7)?,
                    withdrawn_at: row.get(8)?,
                    kind: kind_at(row, 9)?,
                    used_by: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;

        let mut used =
            conn.prepare("SELECT worker_id FROM enrollment_code_uses WHERE code_id = ?1 ORDER BY used_at, worker_id")?;
        rows.into_iter()
            .map(|code| {
                let used_by = used
                    .query_map((&code.id,), |row| row.get(0))?
                    .collect::<Result<Vec<String>, _>>()?;
                Ok(EnrollmentCodeRecord { used_by, ..code })
            })
            .collect()
    }

    fn withdraw_enrollment_code(&self, id: &str, now_ms: i64) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(WITHDRAW_CODE, (id, now_ms))? > 0)
    }

    fn withdraw_enrollment_code_recorded(&self, id: &str, event: &SecurityEvent) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let withdrawn = tx.execute(WITHDRAW_CODE, (id, event.at))? > 0;
        if withdrawn {
            insert_event(&tx, event)?;
        }
        tx.commit()?;
        Ok(withdrawn)
    }

    fn list_usable_enrollment_codes(&self, now_ms: i64) -> Result<Vec<UsableEnrollmentCode>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut usable = conn.prepare(&format!(
            "SELECT id, group_id, fingerprint FROM enrollment_codes WHERE {USABLE_CODE} {NEWEST_FIRST}"
        ))?;
        usable
            .query_map(rusqlite::named_params! { ":now": now_ms }, |row| {
                Ok(UsableEnrollmentCode {
                    id: row.get(0)?,
                    group_id: row.get(1)?,
                    fingerprint: row.get(2)?,
                })
            })?
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn remove_worker_credential(&self, worker_id: &str, token_hash: &str) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute(
            "DELETE FROM worker_credentials WHERE worker_id = ?1 AND token_hash = ?2",
            (worker_id, token_hash),
        )? > 0)
    }

    fn unspend_enrollment_code(&self, code_id: &str, worker_id: &str, token_hash: &str) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM worker_credentials WHERE worker_id = ?1 AND token_hash = ?2",
            (worker_id, token_hash),
        )?;
        tx.execute(
            "DELETE FROM enrollment_code_uses WHERE code_id = ?1 AND worker_id = ?2",
            (code_id, worker_id),
        )?;
        tx.execute(
            "UPDATE enrollment_codes SET uses = uses - 1 WHERE id = ?1 AND uses > 0",
            (code_id,),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn withdraw_enrollment_codes_not_naming(
        &self,
        may_have_named: &[&str],
        now_ms: i64,
        event: &dyn Fn(&str) -> SecurityEvent,
    ) -> Result<Vec<String>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        // Read inside the transaction the withdrawal happens in, so an enrollment code spent between the two is not
        // withdrawn out from under the worker it just let in.
        let ids: Vec<String> = {
            let mut usable = tx.prepare(&format!(
                "SELECT id, fingerprint FROM enrollment_codes WHERE {USABLE_CODE} {NEWEST_FIRST}"
            ))?;
            // Filtered here rather than in the predicate: the roots an enrollment code may have been minted against are
            // a list this leader builds, and one of them is the empty fingerprint the worker inside this process is
            // minted an enrollment code against -- which a `<>` comparison selects and a cutover would then withdraw
            // out from under it, stopping this leader with it.
            usable
                .query_map(rusqlite::named_params! { ":now": now_ms }, |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .filter_map(|row| match row {
                    Ok((id, fingerprint)) => (!may_have_named.contains(&fingerprint.as_str())).then_some(Ok(id)),
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<_, _>>()?
        };
        for id in &ids {
            tx.execute(WITHDRAW_CODE, (id, now_ms))?;
            insert_event(&tx, &event(id))?;
        }
        tx.commit()?;
        Ok(ids)
    }

    fn spend_enrollment_code(
        &self,
        code_hash: &str,
        worker_id: &str,
        token_hash: &str,
        now_ms: i64,
        presented: &str,
        may_have_named: &[&str],
    ) -> Result<Spend, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let found = tx
            .query_row(
                "SELECT id, expires_at, withdrawn_at, fingerprint, kind FROM enrollment_codes WHERE code_hash = ?1",
                (code_hash,),
                |row| {
                    Ok(Presented {
                        id: row.get(0)?,
                        expires_at: row.get(1)?,
                        withdrawn_at: row.get(2)?,
                        fingerprint: row.get(3)?,
                        kind: kind_at(row, 4)?,
                    })
                },
            )
            .optional()?;
        let Some(Presented {
            id,
            expires_at,
            withdrawn_at,
            fingerprint,
            kind,
        }) = found
        else {
            return Ok(Spend::Unknown);
        };
        // Before the expiry: a withdrawn enrollment code is withdrawn whatever its dates say, and the two are different
        // things to be told.
        if withdrawn_at.is_some() {
            return Ok(Spend::Withdrawn);
        }
        if let Some(expires_at) = expires_at
            && now_ms >= expires_at
        {
            return Ok(Spend::Expired { expired_at: expires_at });
        }
        // The row's fingerprint, not the one pasted with the secret. Inside this transaction, so no cutover lands
        // between reading it and spending the use.
        //
        // What was pasted against what this leader wrote down. Asked before the rest, because an edited enrollment code
        // is about the request rather than about anything here having changed.
        if presented != fingerprint {
            return Ok(Spend::Altered);
        }
        if !may_have_named.contains(&fingerprint.as_str()) {
            return Ok(Spend::WrongAuthority);
        }

        // A retry: this enrollment code already brought this worker in, and the credential that bought has
        // authenticated nothing. The answer carrying it was lost or never kept, so the worker holds nothing. It is the
        // same worker, so no second use is spent. After the checks above, because a retry is still a spend of this
        // enrollment code, and a withdrawn one brings nobody in.
        let retried = tx.execute(
            "UPDATE worker_credentials SET token_hash = ?2
              WHERE worker_id = ?1 AND revoked = 0 AND used = 0
                AND EXISTS (SELECT 1 FROM enrollment_code_uses WHERE code_id = ?3 AND worker_id = ?1)",
            (worker_id, token_hash, &id),
        )?;
        if retried > 0 {
            tx.commit()?;
            return Ok(Spend::Spent {
                code_id: id,
                fingerprint,
            });
        }

        // `uses < max_uses` in the predicate is what makes two workers presenting the last use atomic: the second
        // matches zero rows and reads the count back as spent out.
        let taken = tx.execute(
            "UPDATE enrollment_codes SET uses = uses + 1 WHERE id = ?1 AND (max_uses IS NULL OR uses < max_uses)",
            (&id,),
        )?;
        if taken == 0 {
            let used_by = used_by(&tx, &id)?;
            return Ok(Spend::Exhausted { used_by });
        }

        // The credential in the same transaction. A worker id that has ever enrolled is refused here, and the
        // rollback gives the use back rather than spending it on a worker that got nothing.
        let stored = tx.execute(
            "INSERT OR IGNORE INTO worker_credentials (worker_id, token_hash, revoked, used, kind)
             VALUES (?1, ?2, 0, 0, ?3)",
            (worker_id, token_hash, kind.as_str()),
        )?;
        if stored == 0 {
            return Ok(Spend::AlreadyEnrolled);
        }
        tx.execute(
            "INSERT INTO enrollment_code_uses (code_id, worker_id, used_at) VALUES (?1, ?2, ?3)",
            (&id, worker_id, now_ms),
        )?;
        tx.commit()?;
        Ok(Spend::Spent {
            code_id: id,
            fingerprint,
        })
    }

    fn store_worker_credential(&self, worker_id: &str, token_hash: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO worker_credentials (worker_id, token_hash, revoked) VALUES (?1, ?2, 0)",
            (worker_id, token_hash),
        )?;
        if inserted == 0 {
            return Err(StoreError(format!(
                "worker {worker_id:?} is already enrolled; a worker that lost its credential \
                 needs a new worker id"
            )));
        }
        Ok(())
    }

    fn get_worker_credential_by_hash(&self, token_hash: &str) -> Result<Option<WorkerCredentialRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM worker_credentials WHERE token_hash = ?1"
        ))?;
        stmt.query_row((token_hash,), credential_row)
            .optional()
            .map_err(StoreError::from)
    }

    fn mark_credential_used(&self, token_hash: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE worker_credentials SET used = 1 WHERE token_hash = ?1 AND used = 0",
            (token_hash,),
        )?;
        Ok(())
    }

    fn forget_worker(&self, worker_id: &str) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let forgotten = tx.execute(
            "DELETE FROM worker_credentials WHERE worker_id = ?1 AND kind = 'ephemeral' AND revoked = 0",
            (worker_id,),
        )? > 0;
        // The enrollment code's count stays: it says how many it brought in. Its list says who is still there.
        if forgotten {
            tx.execute("DELETE FROM enrollment_code_uses WHERE worker_id = ?1", (worker_id,))?;
        }
        tx.commit()?;
        Ok(forgotten)
    }

    fn remove_worker(&self, worker_id: &str, now_ms: i64) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        let updated = conn.execute(
            "UPDATE worker_credentials SET revoked = 1, removed_at = ?2 WHERE worker_id = ?1 AND removed_at IS NULL",
            (worker_id, now_ms),
        )?;
        Ok(updated > 0)
    }

    fn revoke_worker_credential(&self, worker_id: &str) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        let updated = conn.execute(
            "UPDATE worker_credentials SET revoked = 1 WHERE worker_id = ?1",
            (worker_id,),
        )?;
        Ok(updated > 0)
    }

    fn list_secrets(&self) -> Result<Vec<SecretRecord>, StoreError> {
        let key = self.key_if_any()?;
        let conn = self.conn.lock().unwrap();

        let mut names = conn.prepare(
            "SELECT name, description,
                    EXISTS (SELECT 1 FROM certificate_chains JOIN secret_versions USING (name, version)
                             WHERE name = s.name)
               FROM secrets s ORDER BY name",
        )?;
        let secrets = names
            .query_map((), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut held =
            conn.prepare("SELECT name, version, set_at, key_id FROM secret_versions ORDER BY name, version")?;
        let mut by_name: std::collections::HashMap<String, Vec<StoredValue>> = std::collections::HashMap::new();
        for row in held.query_map((), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })? {
            let (name, version, set_at, key_id) = row?;
            by_name.entry(name).or_default().push(StoredValue {
                version,
                set_at,
                // Compared rather than opened: which key sealed a row is enough to say whether this leader can
                // produce it, and listing the catalog has no business decrypting anything.
                usable: key.as_ref().is_some_and(|k| k.key_id() == key_id),
            });
        }

        Ok(secrets
            .into_iter()
            .map(|(name, description, certificate)| SecretRecord {
                versions: by_name.remove(&name).unwrap_or_default(),
                name,
                description,
                certificate,
            })
            .collect())
    }

    fn create_secret(
        &self,
        name: &str,
        custody: Custody,
        description: &str,
        value: Option<&str>,
    ) -> Result<(), StoreError> {
        self.create(name, custody, description, value, None, None)
    }

    fn create_certificate(&self, name: &str, description: &str, chain: &str, key: &str) -> Result<(), StoreError> {
        self.create(name, Custody::LeaderStored, description, Some(key), Some(chain), None)
    }

    fn create_issued_certificate(
        &self,
        name: &str,
        description: &str,
        issued: &Issued,
        chain: &str,
        key: &str,
    ) -> Result<(), StoreError> {
        self.create(
            name,
            Custody::LeaderStored,
            description,
            Some(key),
            Some(chain),
            Some(issued),
        )
    }

    fn issued_certificates(&self) -> Result<std::collections::BTreeMap<String, Issued>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut statement = conn.prepare("SELECT name, names, validity_days FROM issued_certificates")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut out = std::collections::BTreeMap::new();
        for row in rows {
            let (name, names, validity_days) = row?;
            let names = serde_json::from_str(&names).map_err(|e| StoreError(format!("{name:?}'s names: {e}")))?;
            out.insert(name, Issued { names, validity_days });
        }
        Ok(out)
    }

    fn add_secret_version(&self, name: &str, value: &str) -> Result<i64, StoreError> {
        self.add_version(name, value, None)
    }

    fn add_certificate_version(&self, name: &str, chain: &str, key: &str) -> Result<i64, StoreError> {
        self.add_version(name, key, Some(chain))
    }

    fn certificate_chain(&self, name: &str, version: i64) -> Result<Option<String>, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT chain FROM certificate_chains JOIN secret_versions USING (name, version)
                  WHERE name = ?1 AND version = ?2",
                (name, version),
                |row| row.get(0),
            )
            .optional()?)
    }

    fn delete_secret(&self, name: &str) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM certificate_chains WHERE name = ?1", (name,))?;
        tx.execute("DELETE FROM issued_certificates WHERE name = ?1", (name,))?;
        tx.execute("DELETE FROM secret_versions WHERE name = ?1", (name,))?;
        let gone = tx.execute("DELETE FROM secrets WHERE name = ?1", (name,))? > 0;
        tx.commit()?;
        Ok(gone)
    }

    fn delete_secret_version(&self, name: &str, version: i64) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM certificate_chains WHERE name = ?1 AND version = ?2",
            (name, version),
        )?;
        let gone = tx.execute(
            "DELETE FROM secret_versions WHERE name = ?1 AND version = ?2",
            (name, version),
        )? > 0;
        tx.commit()?;
        Ok(gone)
    }

    fn secrets_for_delivery(
        &self,
        entitled: &[(String, i64)],
        held: &[(String, i64)],
    ) -> Result<Vec<crate::heartbeat::SecretDelivery>, StoreError> {
        let Some(key) = self.key_if_any()? else {
            return Ok(Vec::new());
        };
        let conn = self.conn.lock().unwrap();
        let mut out = Vec::new();
        for (name, version) in entitled {
            if held.iter().any(|(n, v)| n == name && v == version) {
                continue;
            }
            let Some(sealed) = Self::sealed(&conn, name, *version)? else {
                continue;
            };
            // An unopenable row is passed over rather than reported: the worker's own refusal names it, and this
            // answer carries values, not logs.
            let Some(Ok(value)) = key.open(name, &sealed).map(String::from_utf8) else {
                continue;
            };
            let chain = conn
                .query_row(
                    "SELECT chain FROM certificate_chains WHERE name = ?1 AND version = ?2",
                    (name, version),
                    |row| row.get(0),
                )
                .optional()?;
            out.push(crate::heartbeat::SecretDelivery {
                name: name.clone(),
                version: *version,
                value,
                chain,
            });
        }
        Ok(out)
    }

    fn verify_secret(&self, name: &str, version: i64, candidate: &str) -> Result<bool, StoreError> {
        let Some(key) = self.key_if_any()? else {
            return Ok(false);
        };
        let conn = self.conn.lock().unwrap();
        let Some(sealed) = Self::sealed(&conn, name, version)? else {
            return Ok(false);
        };
        Ok(key.verify(name, &sealed, candidate))
    }

    fn list_worker_credentials(&self) -> Result<Vec<WorkerCredentialRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM worker_credentials WHERE removed_at IS NULL ORDER BY worker_id"
        ))?;
        let rows = stmt.query_map((), credential_row)?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn key_standing(&self) -> Result<KeyStanding, StoreError> {
        let key = self.key_if_any()?;
        // Only the newest copy of each value counts. An older version under a key since replaced is history: a leader
        // that lost its key before regeneration existed, and entered every secret again, is not broken.
        let sealed = self.sealed_by()?;
        let ids: std::collections::BTreeSet<&String> = sealed.iter().map(|(_, id)| id).collect();
        let retired = self.retired_key_ids()?;
        let held = key.as_ref().map(|k| k.key_id().to_string());
        let foreign = ids.iter().any(|&id| Some(id) != held.as_ref() && !retired.contains(id));
        let state = match held {
            None if ids.is_empty() => KeyState::Absent,
            None => KeyState::Missing,
            Some(key_id) if foreign => KeyState::Mismatched { key_id },
            Some(key_id) => KeyState::Held { key_id },
        };
        let opens = |id: &String| matches!(&state, KeyState::Held { key_id } if key_id == id);
        Ok(KeyStanding {
            path: self.secret_key.clone(),
            unopenable: sealed
                .into_iter()
                .filter(|(_, id)| !opens(id))
                .map(|(what, _)| what)
                .collect(),
            key: state,
        })
    }

    fn regenerate_key(&self, event: &SecurityEvent) -> Result<KeyStanding, StoreError> {
        let standing = self.key_standing()?;
        if !standing.broken() {
            return Err(StoreError(
                "the secrets encryption key opens what is stored under it: there is nothing to regenerate".to_string(),
            ));
        }
        if let KeyState::Mismatched { .. } = standing.key {
            let now = super::warnings::now_millis() / 1000;
            let aside = PathBuf::from(format!("{}.replaced-{now}", self.secret_key.display()));
            std::fs::rename(&self.secret_key, &aside)
                .map_err(|e| StoreError(format!("{}: {e}", self.secret_key.display())))?;
            tracing::warn!(
                target: "secrets.key_replaced",
                path = %aside.display(),
                "moved aside a secrets encryption key that did not match the stored secrets"
            );
        }
        let key = secrets::load_or_create(&self.secret_key)?;
        let mut retired = self.retired_key_ids()?;
        retired.extend(self.every_key_id()?);
        retired.extend(self.sealed_by()?.into_iter().map(|(_, id)| id));
        retired.remove(key.key_id());
        let retired = retired.into_iter().collect::<Vec<_>>().join(",");
        self.put_settings(&[(RETIRED_KEY_IDS, retired)], Some(event))?;
        self.key_standing()
    }

    fn group_hues(&self) -> Result<std::collections::HashMap<String, u16>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT group_id, hue FROM group_colors")?;
        let rows = stmt
            .query_map((), |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u16)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows.into_iter().collect())
    }

    fn set_group_hue(&self, group_id: &str, hue: u16) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO group_colors (group_id, hue) VALUES (?1, ?2)
             ON CONFLICT(group_id) DO UPDATE SET hue = excluded.hue",
            (group_id, hue),
        )?;
        Ok(())
    }

    // -- single-worker sources -------------------------------------------------

    fn source_hold(&self, group: &str, key: &SourceKey) -> Result<Option<SourceHold>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let hold = conn
            .query_row(
                "SELECT holder, cursor FROM source_holds WHERE grp = ?1 AND source = ?2 AND place = ?3",
                (group, &key.source, &key.place),
                |row| {
                    Ok(SourceHold {
                        holder: row.get(0)?,
                        cursor: row.get(1)?,
                    })
                },
            )
            .optional()?;
        Ok(hold)
    }

    fn set_source_holder(&self, group: &str, key: &SourceKey, holder: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO source_holds (grp, source, place, holder) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(grp, source, place) DO UPDATE SET holder = excluded.holder",
            (group, &key.source, &key.place, holder),
        )?;
        Ok(())
    }

    fn apply_source_progress(&self, group: &str, key: &SourceKey, progress: &Progress) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if let Some(cursor) = &progress.cursor {
            tx.execute(
                "UPDATE source_holds SET cursor = ?4 WHERE grp = ?1 AND source = ?2 AND place = ?3",
                (group, &key.source, &key.place, cursor),
            )?;
        }
        {
            // Removals first (`Progress`): an item gone and back in one change is held.
            let mut remove =
                tx.prepare("DELETE FROM source_ledger WHERE grp = ?1 AND source = ?2 AND place = ?3 AND item = ?4")?;
            for item in &progress.removed {
                remove.execute((group, &key.source, &key.place, item))?;
            }
            let mut add = tx.prepare(
                "INSERT INTO source_ledger (grp, source, place, item, entry) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(grp, source, place, item) DO UPDATE SET entry = excluded.entry",
            )?;
            for added in &progress.added {
                add.execute((group, &key.source, &key.place, &added.item, &added.entry))?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn ledger_page(
        &self,
        group: &str,
        key: &SourceKey,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LedgerEntry>, StoreError> {
        let conn = self.conn.lock().unwrap();
        // `?4 IS NULL` rather than two statements: the first page is the one with nothing before it.
        let mut stmt = conn.prepare(
            "SELECT item, entry FROM source_ledger
              WHERE grp = ?1 AND source = ?2 AND place = ?3 AND (?4 IS NULL OR item > ?4)
              ORDER BY item LIMIT ?5",
        )?;
        let rows = stmt
            .query_map((group, &key.source, &key.place, after, limit as i64), |row| {
                Ok(LedgerEntry {
                    item: row.get(0)?,
                    entry: row.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn held_sources(&self) -> Result<Vec<HeldSource>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT grp, source, place, holder, ledger_entries FROM source_holds ORDER BY grp, source, place",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(HeldSource {
                    group: row.get(0)?,
                    key: SourceKey {
                        source: row.get(1)?,
                        place: row.get(2)?,
                    },
                    holder: row.get(3)?,
                    ledger_entries: row.get::<_, i64>(4)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn forget_source(&self, group: &str, key: &SourceKey) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        // The hold first, so the trigger that counts each removed item finds no hold row left to update.
        tx.execute(
            "DELETE FROM source_holds WHERE grp = ?1 AND source = ?2 AND place = ?3",
            (group, &key.source, &key.place),
        )?;
        tx.execute(
            "DELETE FROM source_ledger WHERE grp = ?1 AND source = ?2 AND place = ?3",
            (group, &key.source, &key.place),
        )?;
        tx.commit()?;
        Ok(())
    }

    // -- people ------------------------------------------------------------

    fn any_user_exists(&self) -> Result<bool, StoreError> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row("SELECT EXISTS (SELECT 1 FROM users)", [], |row| row.get(0))?;
        Ok(n != 0)
    }

    fn bootstrap_administrator(&self, user: NewLocalUser<'_>) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let occupied: i64 = tx.query_row("SELECT EXISTS (SELECT 1 FROM users)", [], |row| row.get(0))?;
        if occupied != 0 {
            return Ok(false);
        }
        insert_local_user(&tx, &user)?;
        tx.commit()?;
        Ok(true)
    }

    fn create_local_user(
        &self,
        user: NewLocalUser<'_>,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if username_taken(&tx, user.username)? {
            return Ok(Err(AccountError::UsernameTaken));
        }
        insert_local_user(&tx, &user)?;
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(Ok(()))
    }

    fn list_users(&self) -> Result<Vec<UserRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, auth_source, display_name, enabled, administrator, generation, created_at, updated_at
               FROM users ORDER BY created_at, id",
        )?;
        let rows = stmt.query_map([], read_user)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn get_user(&self, id: &str) -> Result<Option<UserRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id, auth_source, display_name, enabled, administrator, generation, created_at, updated_at
                   FROM users WHERE id = ?1",
                (id,),
                read_user,
            )
            .optional()?)
    }

    fn local_credential_by_username(&self, username: &str) -> Result<Option<LocalCredentialRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                // One statement, so the hash and the generation are the same moment of this account. Read apart,
                // a reset landing between them verifies the old password against the new generation.
                "SELECT c.user_id, c.username, c.password_hash, c.must_change, u.generation
                   FROM local_credentials c JOIN users u ON u.id = c.user_id
                  WHERE c.username = ?1",
                (username,),
                |row| {
                    Ok(LocalCredentialRecord {
                        user_id: row.get(0)?,
                        username: row.get(1)?,
                        password_hash: row.get(2)?,
                        must_change: row.get::<_, i64>(3)? != 0,
                        generation: row.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    fn username_of(&self, user_id: &str) -> Result<Option<String>, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT username FROM local_credentials WHERE user_id = ?1",
                (user_id,),
                |row| row.get(0),
            )
            .optional()?)
    }

    fn set_local_password(
        &self,
        user_id: &str,
        password_hash: &str,
        must_change: bool,
        expect: Option<i64>,
        event: Option<&SecurityEvent>,
    ) -> Result<Result<i64, AccountError>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        match local_user_exists(&tx, user_id)? {
            Presence::Absent => return Ok(Err(AccountError::NoSuchUser)),
            Presence::NotLocal => return Ok(Err(AccountError::WrongAuthSource)),
            Presence::Local => {}
        }
        // Judged here rather than by the caller: a check outside this transaction is a check with a gap after
        // it, and the gap is where the reset the caller is about to overwrite lands.
        if let Some(expect) = expect {
            let stands: bool = tx.query_row(
                "SELECT enabled != 0 AND generation = ?2 FROM users WHERE id = ?1",
                rusqlite::params![user_id, expect],
                |row| row.get(0),
            )?;
            if !stands {
                return Ok(Err(AccountError::Superseded));
            }
        }
        tx.execute(
            "UPDATE local_credentials SET password_hash = ?2, must_change = ?3 WHERE user_id = ?1",
            rusqlite::params![user_id, password_hash, must_change as i64],
        )?;
        bump_generation(&tx, user_id)?;
        if let Some(event) = event {
            insert_event(&tx, event)?;
        }
        let now: i64 = tx.query_row("SELECT generation FROM users WHERE id = ?1", (user_id,), |row| {
            row.get(0)
        })?;
        tx.commit()?;
        Ok(Ok(now))
    }

    fn revoke_sessions(&self, user_id: &str, event: &SecurityEvent) -> Result<Result<(), AccountError>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if bump_generation(&tx, user_id)? == 0 {
            return Ok(Err(AccountError::NoSuchUser));
        }
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(Ok(()))
    }

    fn set_user_enabled(
        &self,
        user_id: &str,
        enabled: bool,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if !user_exists(&tx, user_id)? {
            return Ok(Err(AccountError::NoSuchUser));
        }
        // Counted inside the transaction and excluding this account, so two disables running at once serialize
        // and the second sees the first's effect rather than the roster they both started from.
        if !enabled && others_who_can_sign_in(&tx, user_id)? == 0 {
            return Ok(Err(AccountError::LastAdministrator));
        }
        tx.execute(
            "UPDATE users SET enabled = ?2, updated_at = ?3 WHERE id = ?1",
            rusqlite::params![user_id, enabled as i64, super::warnings::now_millis()],
        )?;
        // Disabling moves the generation in the same transaction. Ending the in-memory sessions is a separate
        // step, so a login that passed verification just before the disable could issue one afterward: refused
        // while the account is disabled, and valid again the moment it is re-enabled. Moving the generation is
        // what makes re-enabling revive nothing.
        if !enabled {
            bump_generation(&tx, user_id)?;
        }
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(Ok(()))
    }

    fn set_display_name(&self, user_id: &str, display_name: &str) -> Result<Result<(), AccountError>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE users SET display_name = ?2, updated_at = ?3 WHERE id = ?1",
            rusqlite::params![user_id, display_name, super::warnings::now_millis()],
        )?;
        match n {
            0 => Ok(Err(AccountError::NoSuchUser)),
            _ => Ok(Ok(())),
        }
    }

    fn oidc_sign_in(&self, who: OidcSignIn<'_>, created: &SecurityEvent) -> Result<UserRecord, StoreError> {
        let groups = serde_json::to_string(who.groups).map_err(|e| StoreError(format!("encoding groups: {e}")))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        // The lookup and the insert are one transaction, so two first sign-ins racing for one identity make one
        // account between them.
        let known: Option<String> = tx
            .query_row(
                "SELECT user_id FROM oidc_identities WHERE issuer = ?1 AND subject = ?2",
                (who.issuer, who.subject),
                |row| row.get(0),
            )
            .optional()?;
        let user_id = match known {
            Some(user_id) => {
                tx.execute(
                    "UPDATE oidc_identities SET groups = ?2, login = ?3, last_sign_in = ?4 WHERE user_id = ?1",
                    rusqlite::params![&user_id, groups, who.login, who.at],
                )?;
                user_id
            }
            None => {
                tx.execute(
                    "INSERT INTO users (id, auth_source, display_name, enabled, administrator, generation, created_at,
                                        updated_at)
                     VALUES (?1, 'oidc', ?2, 1, 0, 1, ?3, ?3)",
                    rusqlite::params![who.new_id, who.display_name, who.at],
                )?;
                tx.execute(
                    "INSERT INTO oidc_identities (user_id, auth_source, issuer, subject, groups, login, last_sign_in)
                     VALUES (?1, 'oidc', ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![who.new_id, who.issuer, who.subject, groups, who.login, who.at],
                )?;
                insert_event(&tx, created)?;
                who.new_id.to_string()
            }
        };
        let user = tx.query_row(
            "SELECT id, auth_source, display_name, enabled, administrator, generation, created_at, updated_at
               FROM users WHERE id = ?1",
            (&user_id,),
            read_user,
        )?;
        tx.commit()?;
        Ok(user)
    }

    fn oidc_identity_of(&self, user_id: &str) -> Result<Option<OidcIdentityRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(String, String, String, Option<String>, i64)> = conn
            .query_row(
                "SELECT issuer, subject, groups, login, last_sign_in FROM oidc_identities WHERE user_id = ?1",
                (user_id,),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()?;
        let Some((issuer, subject, groups, login, last_sign_in)) = row else {
            return Ok(None);
        };
        let groups =
            serde_json::from_str(&groups).map_err(|e| StoreError(format!("reading groups of {user_id}: {e}")))?;
        Ok(Some(OidcIdentityRecord {
            issuer,
            subject,
            groups,
            login,
            last_sign_in,
        }))
    }

    fn oidc_refreshed(&self, user_id: &str, groups: &[String], login: Option<&str>) -> Result<(), StoreError> {
        let groups = serde_json::to_string(groups).map_err(|e| StoreError(format!("encoding groups: {e}")))?;
        self.conn.lock().unwrap().execute(
            "UPDATE oidc_identities SET groups = ?2, login = ?3 WHERE user_id = ?1",
            rusqlite::params![user_id, groups, login],
        )?;
        Ok(())
    }

    fn recover_local_user(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<Result<String, AccountError>, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let user_id: Option<String> = tx
            .query_row(
                "SELECT user_id FROM local_credentials WHERE username = ?1",
                (username,),
                |row| row.get(0),
            )
            .optional()?;
        let Some(user_id) = user_id else {
            return Ok(Err(AccountError::NoSuchUser));
        };
        tx.execute(
            "UPDATE local_credentials SET password_hash = ?2, must_change = 1 WHERE user_id = ?1",
            (&user_id, password_hash),
        )?;
        // Enabled and an administrator both: recovery exists for the case where neither is true any more, and an
        // account it cannot sign in with is not recovered.
        tx.execute(
            "UPDATE users SET enabled = 1, administrator = 1, updated_at = ?2 WHERE id = ?1",
            rusqlite::params![&user_id, super::warnings::now_millis()],
        )?;
        bump_generation(&tx, &user_id)?;
        tx.commit()?;
        Ok(Ok(user_id))
    }

    fn record_security_event(&self, event: &SecurityEvent) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        insert_event(&conn, event)
    }

    fn list_security_events(&self, limit: usize) -> Result<Vec<SecurityEvent>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, at, actor, action, target, outcome FROM security_events
              ORDER BY at DESC, rowid DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map((limit as i64,), |row| {
            Ok(SecurityEvent {
                id: row.get(0)?,
                at: row.get(1)?,
                actor: row.get(2)?,
                action: row.get(3)?,
                target: row.get(4)?,
                outcome: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn agent_permissions_of(&self, user_id: &str) -> Result<AgentPermissions, StoreError> {
        let conn = self.conn.lock().unwrap();
        let permissions: Option<String> = conn
            .query_row(
                "SELECT permissions FROM agent_permissions WHERE user_id = ?1",
                (user_id,),
                |row| row.get(0),
            )
            .optional()?;
        match permissions {
            None => Ok(AgentPermissions::default()),
            Some(permissions) => serde_json::from_str(&permissions)
                .map_err(|e| StoreError(format!("reading the agent permissions of {user_id}: {e}"))),
        }
    }

    fn put_agent_permissions(
        &self,
        user_id: &str,
        permissions: &AgentPermissions,
        event: &SecurityEvent,
    ) -> Result<Result<(), AccountError>, StoreError> {
        let encoded =
            serde_json::to_string(permissions).map_err(|e| StoreError(format!("encoding agent permissions: {e}")))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if !user_exists(&tx, user_id)? {
            return Ok(Err(AccountError::NoSuchUser));
        }
        tx.execute(
            "INSERT INTO agent_permissions (user_id, permissions) VALUES (?1, ?2)
             ON CONFLICT(user_id) DO UPDATE SET permissions = excluded.permissions",
            (user_id, &encoded),
        )?;
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(Ok(()))
    }

    fn create_conversation(&self, conversation: &ConversationRecord) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agent_conversations (id, user_id, title, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                conversation.id,
                conversation.user_id,
                conversation.title,
                conversation.created_at,
                conversation.updated_at
            ],
        )?;
        Ok(())
    }

    fn conversations_of(&self, user_id: &str) -> Result<Vec<ConversationRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, user_id, title, created_at, updated_at FROM agent_conversations
             WHERE user_id = ?1 ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map((user_id,), conversation_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn conversation(&self, id: &str) -> Result<Option<ConversationRecord>, StoreError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id, user_id, title, created_at, updated_at FROM agent_conversations WHERE id = ?1",
                (id,),
                conversation_row,
            )
            .optional()?)
    }

    fn turns_of(&self, conversation_id: &str) -> Result<Vec<String>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT turn FROM agent_turns WHERE conversation_id = ?1 ORDER BY seq")?;
        let rows = stmt.query_map((conversation_id,), |row| row.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn append_turn(&self, conversation_id: &str, turn: &str, at: i64) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE agent_conversations SET updated_at = ?2 WHERE id = ?1",
            rusqlite::params![conversation_id, at],
        )?;
        if n == 0 {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO agent_turns (conversation_id, seq, turn)
             VALUES (?1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM agent_turns WHERE conversation_id = ?1), ?2)",
            (conversation_id, turn),
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn delete_conversation(&self, id: &str) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM agent_turns WHERE conversation_id = ?1", (id,))?;
        let n = tx.execute("DELETE FROM agent_conversations WHERE id = ?1", (id,))?;
        tx.commit()?;
        Ok(n > 0)
    }

    fn delete_conversations_untouched_since(&self, before: i64) -> Result<usize, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM agent_turns WHERE conversation_id IN
             (SELECT id FROM agent_conversations WHERE updated_at < ?1)",
            (before,),
        )?;
        let n = tx.execute("DELETE FROM agent_conversations WHERE updated_at < ?1", (before,))?;
        tx.commit()?;
        Ok(n)
    }

    fn list_roles(&self) -> Result<Vec<Role>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut roles: Vec<Role> = conn
            .prepare("SELECT name, administrator FROM roles ORDER BY name")?
            .query_map([], |row| {
                Ok(Role {
                    name: row.get(0)?,
                    administrator: row.get::<_, i64>(1)? != 0,
                    areas: std::collections::BTreeMap::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut stmt = conn.prepare("SELECT role, area, level FROM role_areas ORDER BY role, area")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get(1)?, row.get::<_, String>(2)?))
        })?;
        for row in rows {
            let (name, area, level): (String, String, String) = row?;
            let level = Level::parse(&level)
                .ok_or_else(|| StoreError(format!("role {name} holds an unknown level {level:?} in {area}")))?;
            // Both lists are by name and the foreign key holds every area's role in the first.
            let at = roles
                .binary_search_by(|role| role.name.as_str().cmp(&name))
                .map_err(|_| StoreError(format!("an area of {name}, which is no role")))?;
            roles[at].areas.insert(area, level);
        }
        Ok(roles)
    }

    fn put_role(&self, role: &Role, event: &SecurityEvent) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO roles (name, administrator) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET administrator = excluded.administrator",
            rusqlite::params![role.name, role.administrator as i64],
        )?;
        // Replaced whole, so an area left out of the new role is one it no longer gives anything in.
        tx.execute("DELETE FROM role_areas WHERE role = ?1", (&role.name,))?;
        for (area, level) in &role.areas {
            tx.execute(
                "INSERT INTO role_areas (role, area, level) VALUES (?1, ?2, ?3)",
                (&role.name, area, level.as_str()),
            )?;
        }
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(())
    }

    fn delete_role(&self, name: &str, event: &SecurityEvent) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        if tx.execute("DELETE FROM roles WHERE name = ?1", (name,))? == 0 {
            return Ok(false);
        }
        insert_event(&tx, event)?;
        tx.commit()?;
        Ok(true)
    }

    fn setting(&self, key: &str) -> Result<Option<String>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?1")?;
        let mut rows = stmt.query_map([key], |row| row.get::<_, String>(0))?;
        rows.next().transpose().map_err(StoreError::from)
    }

    fn put_settings(&self, entries: &[(&str, String)], event: Option<&SecurityEvent>) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        for (key, value) in entries {
            tx.execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![key, value],
            )?;
        }
        // In the same transaction, so a change to how long a session lives cannot land without the row saying who
        // made it. A separate best-effort insert afterward is exactly the gap an audit exists to close.
        if let Some(event) = event {
            insert_event(&tx, event)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn sealed_setting(&self, key: &str, value: &str) -> Result<String, StoreError> {
        Ok(self.key_for_sealing()?.seal(key, value.as_bytes()).to_text())
    }

    fn opened_setting(&self, key: &str) -> Result<Option<String>, StoreError> {
        let (Some(text), Some(sealing)) = (self.setting(key)?, self.key_if_any()?) else {
            return Ok(None);
        };
        let opened = Sealed::from_text(&text).and_then(|sealed| sealing.open(key, &sealed));
        Ok(opened.and_then(|value| String::from_utf8(value).ok()))
    }
}

/// One audit row. Takes any connection, so a caller inside a transaction writes it with the mutation it
/// describes rather than after it.
fn insert_event(conn: &Connection, event: &SecurityEvent) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO security_events (id, at, actor, action, target, outcome)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            event.id,
            event.at,
            event.actor,
            event.action,
            event.target,
            event.outcome
        ],
    )?;
    Ok(())
}

// -- account helpers --------------------------------------------------------

/// Whether an account exists and whether it is local, which are different refusals: an OIDC account is present and
/// simply has no password to operate on.
enum Presence {
    Absent,
    NotLocal,
    Local,
}

fn read_user(row: &rusqlite::Row<'_>) -> rusqlite::Result<UserRecord> {
    let source: String = row.get(1)?;
    Ok(UserRecord {
        id: row.get(0)?,
        // A row whose source this build does not know is read as local rather than refused: a build that adds a
        // source adds a breaking step with it (`steps`), so this build never opens a file holding one.
        auth_source: AuthSource::parse(&source).unwrap_or(AuthSource::Local),
        display_name: row.get(2)?,
        enabled: row.get::<_, i64>(3)? != 0,
        administrator: row.get::<_, i64>(4)? != 0,
        generation: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn insert_local_user(tx: &rusqlite::Transaction<'_>, user: &NewLocalUser<'_>) -> Result<(), StoreError> {
    let now = super::warnings::now_millis();
    tx.execute(
        "INSERT INTO users (id, auth_source, display_name, enabled, administrator, generation, created_at, updated_at)
         VALUES (?1, 'local', ?2, 1, 1, 1, ?3, ?3)",
        rusqlite::params![user.id, user.display_name, now],
    )?;
    tx.execute(
        "INSERT INTO local_credentials (user_id, auth_source, username, password_hash, must_change)
         VALUES (?1, 'local', ?2, ?3, ?4)",
        rusqlite::params![user.id, user.username, user.password_hash, user.must_change as i64],
    )?;
    Ok(())
}

/// Including a disabled account's: a name is never released, so a new person cannot inherit what an old one's
/// audit rows point at.
fn username_taken(tx: &rusqlite::Transaction<'_>, username: &str) -> Result<bool, StoreError> {
    let n: i64 = tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM local_credentials WHERE username = ?1)",
        (username,),
        |row| row.get(0),
    )?;
    Ok(n != 0)
}

fn user_exists(tx: &rusqlite::Transaction<'_>, user_id: &str) -> Result<bool, StoreError> {
    let n: i64 = tx.query_row("SELECT EXISTS (SELECT 1 FROM users WHERE id = ?1)", (user_id,), |row| {
        row.get(0)
    })?;
    Ok(n != 0)
}

fn local_user_exists(tx: &rusqlite::Transaction<'_>, user_id: &str) -> Result<Presence, StoreError> {
    let source: Option<String> = tx
        .query_row("SELECT auth_source FROM users WHERE id = ?1", (user_id,), |row| {
            row.get(0)
        })
        .optional()?;
    Ok(match source.as_deref() {
        None => Presence::Absent,
        Some("local") => Presence::Local,
        Some(_) => Presence::NotLocal,
    })
}

/// How many accounts other than `user_id` could still sign in and administer: enabled, an administrator, and
/// holding a password. Anything short of all three is an account that cannot let an operator back in. An OIDC
/// administrator does not count: it signs in only while its provider answers, and a local one is the way in when
/// that provider is down or misconfigured.
fn others_who_can_sign_in(tx: &rusqlite::Transaction<'_>, user_id: &str) -> Result<i64, StoreError> {
    Ok(tx.query_row(
        "SELECT COUNT(*) FROM users u
           JOIN local_credentials c ON c.user_id = u.id
          WHERE u.id <> ?1 AND u.enabled = 1 AND u.administrator = 1",
        (user_id,),
        |row| row.get(0),
    )?)
}

/// Refuses every session already issued for the account. Returns rows affected, so a caller can tell a missing
/// account from a revoked one.
fn bump_generation(conn: &Connection, user_id: &str) -> Result<usize, StoreError> {
    Ok(conn.execute(
        "UPDATE users SET generation = generation + 1, updated_at = ?2 WHERE id = ?1",
        rusqlite::params![user_id, super::warnings::now_millis()],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::leader::sessions::Lifetimes;

    /// Its own key file per store: the key is a file, and two tests sharing one would not be independent.
    fn temp_key() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("smeltr-store-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("sealing.key")
    }

    fn store_with_key(key: &Path) -> SqliteStore {
        SqliteStore::open(Path::new(":memory:"), key).unwrap()
    }

    fn store() -> SqliteStore {
        store_with_key(&temp_key())
    }

    const NOW: i64 = 1_700_000_000_000;
    const AN_HOUR: i64 = 60 * 60 * 1000;

    fn a_code(id: &str, hash: &str, max_uses: i64, expires_at: i64) -> NewEnrollmentCode {
        NewEnrollmentCode {
            id: id.to_string(),
            code_hash: hash.to_string(),
            fingerprint: "fp".to_string(),
            group_id: "default".to_string(),
            minted_by: "u_1".to_string(),
            minted_at: NOW,
            expires_at: Some(expires_at),
            max_uses: Some(max_uses),
            kind: WorkerKind::Permanent,
        }
    }

    #[test]
    fn an_enrollment_code_for_one_worker_is_spent_by_the_first_and_refused_to_the_second() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();

        assert!(matches!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap(),
            Spend::Spent { .. }
        ));
        match s.spend_enrollment_code("h1", "w2", "t2", NOW, "fp", &["fp"]).unwrap() {
            Spend::Exhausted { used_by } => assert_eq!(used_by, ["w1"]),
            other => panic!("{other:?}"),
        }
        assert!(s.get_worker_credential_by_hash("t2").unwrap().is_none());
    }

    #[test]
    fn an_enrollment_code_minted_for_a_fleet_brings_in_that_many_and_stops() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 3, NOW + AN_HOUR)).unwrap();
        for worker in ["w1", "w2", "w3"] {
            assert!(matches!(
                s.spend_enrollment_code("h1", worker, &format!("t-{worker}"), NOW, "fp", &["fp"])
                    .unwrap(),
                Spend::Spent { .. }
            ));
        }
        match s.spend_enrollment_code("h1", "w4", "t4", NOW, "fp", &["fp"]).unwrap() {
            Spend::Exhausted { used_by } => assert_eq!(used_by, ["w1", "w2", "w3"]),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_enrollment_code_past_its_expiry_is_refused_and_says_so() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW)).unwrap();
        match s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap() {
            Spend::Expired { expired_at } => assert_eq!(expired_at, NOW),
            other => panic!("{other:?}"),
        }
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_none());
    }

    /// An enrollment code carries its authority in the clear beside the secret, and only the secret is hashed: whoever
    /// holds one can edit the first and not the second. So the root an enrollment code names is read off the row this
    /// leader wrote, which is the copy nobody else has had their hands on.
    #[test]
    fn an_enrollment_code_is_held_to_the_authority_it_was_minted_against_not_the_one_presented() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();

        // "fp" is what `a_code` wrote down; this is a leader that has since moved to another root.
        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["the root signed under now"])
                .unwrap(),
            Spend::WrongAuthority
        );
        assert_eq!(
            s.list_enrollment_codes(NOW).unwrap()[0].uses,
            0,
            "the use was spent on a worker that got nothing"
        );
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_none());

        assert!(matches!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap(),
            Spend::Spent { .. }
        ));
    }

    /// A credential taken back before its token ever reached the worker leaves no identity to keep a record of, and a
    /// revoked row would hold that worker id for good -- an enrollment code cannot be spent by one that has enrolled
    /// before. The hash is in the predicate, so this can only take back the row it was called about.
    #[test]
    fn a_credential_taken_back_frees_the_worker_id_it_was_issued_for() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        s.create_enrollment_code(&a_code("j2", "h2", 1, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();

        assert!(
            !s.remove_worker_credential("w1", "another token entirely").unwrap(),
            "a credential this call did not issue was taken back"
        );
        assert!(s.remove_worker_credential("w1", "t1").unwrap());
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_none());

        assert!(
            matches!(
                s.spend_enrollment_code("h2", "w1", "t2", NOW, "fp", &["fp"]).unwrap(),
                Spend::Spent { .. }
            ),
            "the worker id is still occupied, so the remedy this leader names cannot be taken"
        );
    }

    /// An enrollment code a leader tells the worker to send again has to still have a use in it, and the same worker id
    /// has to be free to take it -- both are things spending it just changed, and `unspend_enrollment_code` is what
    /// puts them back. A single-use enrollment code with the credential merely deleted would tell a worker to retry an
    /// enrollment code with none left; a multi-use one would collide with the row this spend already wrote for the same
    /// worker.
    #[test]
    fn unspending_an_enrollment_code_lets_the_same_worker_spend_it_again() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        let Spend::Spent { code_id, .. } = s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap()
        else {
            panic!("expected the enrollment code to spend")
        };

        s.unspend_enrollment_code(&code_id, "w1", "t1").unwrap();

        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_none());
        assert_eq!(
            s.list_enrollment_codes(NOW).unwrap()[0].uses,
            0,
            "the use came back with the credential"
        );
        assert!(
            matches!(
                s.spend_enrollment_code("h1", "w1", "t2", NOW, "fp", &["fp"]).unwrap(),
                Spend::Spent { .. }
            ),
            "the same enrollment code, the same worker id, spent as if the first attempt had never happened"
        );
    }

    /// The authority rides the pasted enrollment code in the clear beside the secret, and only the secret is hashed.
    /// Edited to name no root, it would spend cleanly wherever naming none is also acceptable -- and leave its worker
    /// trusting every platform root in place of the one it was given.
    #[test]
    fn an_enrollment_code_edited_to_name_a_different_authority_is_refused() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();

        // What a leader serving an operator's publicly trusted certificate accepts: that root, or none.
        let may = ["fp", ""];
        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "", &may).unwrap(),
            Spend::Altered,
            "an enrollment code minted against a root was spent naming none, and its worker would trust every \
             platform root"
        );
        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "another root entirely", &may)
                .unwrap(),
            Spend::Altered
        );
        assert!(
            matches!(
                s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &may).unwrap(),
                Spend::Spent { .. }
            ),
            "the enrollment code as it was minted"
        );
    }

    /// Naming no root is an authority like any other, and held to like one: it is what an enrollment code minted on a
    /// leader presenting none of its own was written down as, and it reaches nothing else.
    #[test]
    fn an_enrollment_code_that_pins_no_root_is_held_to_naming_none() {
        let s = store();
        s.create_enrollment_code(&NewEnrollmentCode {
            fingerprint: String::new(),
            ..a_code("j1", "h1", 1, NOW + AN_HOUR)
        })
        .unwrap();

        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "", &["a root this leader signs under"])
                .unwrap(),
            Spend::WrongAuthority,
            "the platform's roots do not reach a certificate this leader signed itself"
        );
        assert!(matches!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "", &[""]).unwrap(),
            Spend::Spent { .. }
        ));
    }

    #[test]
    fn a_secret_this_leader_never_minted_is_unknown() {
        let s = store();
        assert_eq!(
            s.spend_enrollment_code("nope", "w1", "t1", NOW, "fp", &["fp"]).unwrap(),
            Spend::Unknown
        );
    }

    /// The use is given back rather than spent on a worker that got no credential.
    #[test]
    fn a_worker_id_that_has_enrolled_before_costs_the_enrollment_code_nothing() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        s.store_worker_credential("w1", "already").unwrap();

        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap(),
            Spend::AlreadyEnrolled
        );
        assert_eq!(
            s.list_enrollment_codes(NOW).unwrap()[0].uses,
            0,
            "the use was not spent"
        );
        assert!(matches!(
            s.spend_enrollment_code("h1", "w2", "t2", NOW, "fp", &["fp"]).unwrap(),
            Spend::Spent { .. }
        ));
    }

    /// A worker whose enrollment answer was lost asks again, with the same enrollment code and the same id. The
    /// credential it was issued has authenticated nothing, so it never arrived: the retry replaces it, and spends no
    /// second use.
    #[test]
    fn a_retried_enrollment_replaces_a_credential_nothing_has_used() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        assert!(matches!(
            s.spend_enrollment_code("h1", "w1", "lost", NOW, "fp", &["fp"]).unwrap(),
            Spend::Spent { .. }
        ));

        assert!(
            matches!(
                s.spend_enrollment_code("h1", "w1", "kept", NOW, "fp", &["fp"]).unwrap(),
                Spend::Spent { .. }
            ),
            "the enrollment code's one use went to this worker, so its retry needs none"
        );
        assert!(
            s.get_worker_credential_by_hash("lost").unwrap().is_none(),
            "the credential in the lost answer still authenticates"
        );
        let kept = s
            .get_worker_credential_by_hash("kept")
            .unwrap()
            .expect("the retry's credential");
        assert_eq!(kept.worker_id, "w1");
        assert!(!kept.used);
        let code = &s.list_enrollment_codes(NOW).unwrap()[0];
        assert_eq!((code.uses, code.used_by.as_slice()), (1, ["w1".to_string()].as_slice()));
    }

    /// Once a credential has authenticated anything, the worker holds it. A retry carries only an enrollment code and a
    /// worker id, and a stranger can hold both.
    #[test]
    fn a_credential_in_use_is_not_replaced_by_a_retry() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 2, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();
        s.mark_credential_used("t1").unwrap();

        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t2", NOW, "fp", &["fp"]).unwrap(),
            Spend::AlreadyEnrolled
        );
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_some_and(|c| c.used));
        assert!(s.get_worker_credential_by_hash("t2").unwrap().is_none());
    }

    /// A retry is of the enrollment that issued the credential. Another enrollment code presented for the same id is a
    /// second enrollment of a worker that has one, and a revoked worker is brought back by neither.
    #[test]
    fn only_the_enrollment_code_that_enrolled_a_worker_retries_it_and_never_once_revoked() {
        let s = store();
        // A use to spare, so what refuses the revoked worker is its being enrolled and not the count.
        s.create_enrollment_code(&a_code("j1", "h1", 2, NOW + AN_HOUR)).unwrap();
        s.create_enrollment_code(&a_code("j2", "h2", 1, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();

        assert_eq!(
            s.spend_enrollment_code("h2", "w1", "t2", NOW, "fp", &["fp"]).unwrap(),
            Spend::AlreadyEnrolled
        );

        s.revoke_worker_credential("w1").unwrap();
        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t3", NOW, "fp", &["fp"]).unwrap(),
            Spend::AlreadyEnrolled
        );
        assert!(
            s.get_worker_credential_by_hash("t1")
                .unwrap()
                .is_some_and(|c| c.revoked)
        );
    }

    /// An ephemeral enrollment code has no cap and no expiry: an autoscaler spends it for as long as it runs, and it
    /// stops only when an operator withdraws it.
    #[test]
    fn an_ephemeral_enrollment_code_brings_workers_in_until_it_is_withdrawn() {
        let s = store();
        s.create_enrollment_code(&NewEnrollmentCode {
            expires_at: None,
            max_uses: None,
            kind: WorkerKind::Ephemeral,
            ..a_code("j1", "h1", 1, NOW)
        })
        .unwrap();

        // Past any cap a permanent enrollment code has ever had, and long past any date.
        let later = NOW + 365 * 24 * AN_HOUR;
        for n in 0..60 {
            let worker = format!("pod-{n}");
            assert!(
                matches!(
                    s.spend_enrollment_code("h1", &worker, &format!("t-{n}"), later, "fp", &["fp"])
                        .unwrap(),
                    Spend::Spent { .. }
                ),
                "{worker}"
            );
        }
        let code = &s.list_enrollment_codes(later).unwrap()[0];
        assert_eq!((code.uses, code.max_uses, code.expires_at), (60, None, None));
        assert_eq!(code.kind, WorkerKind::Ephemeral);
        assert_eq!(s.list_usable_enrollment_codes(later).unwrap().len(), 1);

        s.withdraw_enrollment_code("j1", later).unwrap();
        assert_eq!(
            s.spend_enrollment_code("h1", "pod-60", "t-60", later, "fp", &["fp"])
                .unwrap(),
            Spend::Withdrawn
        );
    }

    /// Forgetting takes an ephemeral worker's credential and the enrollment code's record of bringing it in, and
    /// nothing else: the enrollment code's count stays, and a permanent or a revoked worker is not forgotten at all.
    #[test]
    fn forgetting_takes_an_ephemeral_worker_and_nothing_else() {
        let s = store();
        s.create_enrollment_code(&NewEnrollmentCode {
            kind: WorkerKind::Ephemeral,
            ..a_code("fleet", "hf", 3, NOW + AN_HOUR)
        })
        .unwrap();
        s.create_enrollment_code(&a_code("vm", "hv", 1, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("hf", "pod-1", "t1", NOW, "fp", &["fp"])
            .unwrap();
        s.spend_enrollment_code("hf", "pod-2", "t2", NOW + 5, "fp", &["fp"])
            .unwrap();
        s.spend_enrollment_code("hv", "vm-1", "tv", NOW, "fp", &["fp"]).unwrap();
        s.revoke_worker_credential("pod-2").unwrap();
        assert_eq!(
            s.get_worker_credential_by_hash("t2").unwrap().unwrap().enrolled_at,
            Some(NOW + 5),
            "a worker's enrollment is when its enrollment code brought it in"
        );

        assert!(s.forget_worker("pod-1").unwrap());
        assert!(!s.forget_worker("pod-1").unwrap(), "already forgotten");
        assert!(!s.forget_worker("pod-2").unwrap(), "revoked, so kept to stay out");
        assert!(
            !s.forget_worker("vm-1").unwrap(),
            "permanent, so missed rather than forgotten"
        );

        let left: Vec<String> = s
            .list_worker_credentials()
            .unwrap()
            .into_iter()
            .map(|c| c.worker_id)
            .collect();
        assert_eq!(left, ["pod-2", "vm-1"]);
        let fleet = s
            .list_enrollment_codes(NOW)
            .unwrap()
            .into_iter()
            .find(|j| j.id == "fleet")
            .unwrap();
        assert_eq!((fleet.uses, fleet.used_by), (2, vec!["pod-2".to_string()]));
    }

    /// A worker is what the enrollment code that brought it in says it is, for its life.
    #[test]
    fn a_worker_keeps_the_kind_of_the_enrollment_code_that_brought_it_in() {
        let s = store();
        s.create_enrollment_code(&a_code("permanent", "hp", 1, NOW + AN_HOUR))
            .unwrap();
        s.create_enrollment_code(&NewEnrollmentCode {
            kind: WorkerKind::Ephemeral,
            ..a_code("ephemeral", "he", 1, NOW + AN_HOUR)
        })
        .unwrap();
        s.spend_enrollment_code("hp", "vm", "tp", NOW, "fp", &["fp"]).unwrap();
        s.spend_enrollment_code("he", "pod", "te", NOW, "fp", &["fp"]).unwrap();

        let kinds: Vec<(String, WorkerKind)> = s
            .list_worker_credentials()
            .unwrap()
            .into_iter()
            .map(|c| (c.worker_id, c.kind))
            .collect();
        assert_eq!(
            kinds,
            [
                ("pod".to_string(), WorkerKind::Ephemeral),
                ("vm".to_string(), WorkerKind::Permanent)
            ]
        );
        assert_eq!(
            s.get_worker_credential_by_hash("te").unwrap().unwrap().kind,
            WorkerKind::Ephemeral
        );
    }

    /// A retry is still a spend of its enrollment code, and a withdrawn enrollment code brings nobody in -- not even
    /// the worker it brought in before.
    #[test]
    fn a_withdrawn_enrollment_code_retries_nothing() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();
        s.withdraw_enrollment_code("j1", NOW).unwrap();

        assert_eq!(
            s.spend_enrollment_code("h1", "w1", "t2", NOW, "fp", &["fp"]).unwrap(),
            Spend::Withdrawn
        );
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_some());
        assert!(s.get_worker_credential_by_hash("t2").unwrap().is_none());
    }

    /// A cutover withdraws exactly the enrollment codes that could still have brought a worker in under the root it
    /// retired.
    ///
    /// `list_usable_enrollment_codes` is what a warning about these same enrollment codes is derived from, and the two
    /// share one predicate. This reads the withdrawal back through that listing, so a change to either is a change to
    /// both.
    #[test]
    fn a_cutover_withdraws_every_usable_enrollment_code_off_the_authority_and_no_other() {
        let s = store();
        let promoted = "incoming";
        let named = |id: &str, fingerprint: &str, max_uses: i64, expires_at: i64| NewEnrollmentCode {
            fingerprint: fingerprint.to_string(),
            ..a_code(id, id, max_uses, expires_at)
        };
        s.create_enrollment_code(&named("outgoing-live", "outgoing", 1, NOW + AN_HOUR))
            .unwrap();
        s.create_enrollment_code(&named("outgoing-spent", "outgoing", 1, NOW + AN_HOUR))
            .unwrap();
        s.create_enrollment_code(&named("outgoing-expired", "outgoing", 1, NOW))
            .unwrap();
        s.create_enrollment_code(&named("outgoing-withdrawn", "outgoing", 1, NOW + AN_HOUR))
            .unwrap();
        s.create_enrollment_code(&named("incoming-live", promoted, 1, NOW + AN_HOUR))
            .unwrap();
        assert!(
            matches!(
                s.spend_enrollment_code("outgoing-spent", "w1", "t1", NOW, "outgoing", &["outgoing"])
                    .unwrap(),
                Spend::Spent { .. }
            ),
            "the enrollment code this test needs spent was refused, so it is still usable and proves nothing"
        );
        s.withdraw_enrollment_code("outgoing-withdrawn", NOW).unwrap();

        // What this leader mints the worker inside its own process. A cutover taking it would refuse that
        // worker's enrollment, and that refusal stops this leader rather than just the thread.
        s.create_enrollment_code(&NewEnrollmentCode {
            fingerprint: String::new(),
            ..a_code("rootless", "rootless", 1, NOW + AN_HOUR)
        })
        .unwrap();

        let withdrawn = s
            .withdraw_enrollment_codes_not_naming(&[promoted, ""], NOW, &|id| SecurityEvent {
                id: format!("e-{id}"),
                at: NOW,
                actor: "system".to_string(),
                action: "enrollment-code-withdrawn".to_string(),
                target: id.to_string(),
                outcome: "ok".to_string(),
            })
            .unwrap();
        assert_eq!(
            withdrawn,
            ["outgoing-live"],
            "an enrollment code this leader would still accept was withdrawn"
        );

        // The same answer read the other way: nothing still able to bring a worker in names anything but the root
        // now signed under.
        let mut left: Vec<String> = s
            .list_usable_enrollment_codes(NOW)
            .unwrap()
            .into_iter()
            .map(|code| code.id)
            .collect();
        left.sort();
        assert_eq!(left, ["incoming-live", "rootless"]);
        let events = s.list_security_events(10).unwrap();
        assert_eq!(
            events.iter().filter(|e| e.target == "outgoing-live").count(),
            1,
            "an enrollment code was withdrawn with nobody recorded as having withdrawn it"
        );
    }

    #[test]
    fn an_outstanding_enrollment_code_reads_back_as_it_was_minted() {
        let s = store();
        s.create_enrollment_code(&a_code("live", "h1", 1, NOW + AN_HOUR))
            .unwrap();

        let listed = s.list_enrollment_codes(NOW).unwrap();
        assert_eq!(listed.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), ["live"]);
        assert_eq!(listed[0].minted_by, "u_1");
        assert_eq!(listed[0].max_uses, Some(1));
        assert_eq!(listed[0].withdrawn_at, None);
    }

    #[test]
    fn an_enrollment_code_says_which_workers_it_brought_in() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 5, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();
        s.spend_enrollment_code("h1", "w2", "t2", NOW + 1, "fp", &["fp"])
            .unwrap();

        let outstanding = s.list_enrollment_codes(NOW).unwrap();
        assert_eq!(outstanding[0].used_by, ["w1", "w2"]);
        assert_eq!(outstanding[0].uses, 2);
    }

    /// An enrollment code that is gone with nobody recorded as having withdrawn it is the same hole in the audit as one
    /// that exists with nobody recorded as having minted it.
    #[test]
    fn withdrawing_an_enrollment_code_and_recording_it_are_one_transaction() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        let event = SecurityEvent {
            id: "e1".to_string(),
            at: NOW,
            actor: "u_1".to_string(),
            action: "enrollment-code-withdrawn".to_string(),
            target: "j1".to_string(),
            outcome: "ok".to_string(),
        };

        assert!(s.withdraw_enrollment_code_recorded("j1", &event).unwrap());
        assert_eq!(s.list_security_events(10).unwrap().len(), 1);

        // Nothing to withdraw writes nothing: an event for an enrollment code that was already gone would be a record
        // of an act that did not happen.
        assert!(!s.withdraw_enrollment_code_recorded("j1", &event).unwrap());
        assert_eq!(s.list_security_events(10).unwrap().len(), 1);
    }

    /// What it already brought in stays enrolled: withdrawing an enrollment code is not un-enrolling a worker. And
    /// which workers it brought in is exactly what an operator acts on next, so the record stays with them.
    #[test]
    fn withdrawing_an_enrollment_code_stops_it_bringing_in_anybody_else() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 5, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();

        assert!(s.withdraw_enrollment_code("j1", NOW).unwrap());
        assert!(!s.withdraw_enrollment_code("j1", NOW).unwrap(), "already withdrawn");
        assert_eq!(
            s.spend_enrollment_code("h1", "w2", "t2", NOW, "fp", &["fp"]).unwrap(),
            Spend::Withdrawn
        );
        assert!(s.get_worker_credential_by_hash("t1").unwrap().is_some());

        let listed = s.list_enrollment_codes(NOW).unwrap();
        assert_eq!(
            listed.len(),
            1,
            "a withdrawn enrollment code took its own provenance with it"
        );
        assert_eq!(listed[0].used_by, vec!["w1".to_string()]);
        assert_eq!(listed[0].withdrawn_at, Some(NOW));
    }

    /// An enrollment code spent to its limit is when `used_by` matters most: it is the whole of who arrived on that
    /// secret.
    #[test]
    fn an_enrollment_code_spent_out_still_says_who_it_brought_in() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        s.spend_enrollment_code("h1", "w1", "t1", NOW, "fp", &["fp"]).unwrap();

        let listed = s.list_enrollment_codes(NOW).unwrap();
        assert_eq!(
            listed.len(),
            1,
            "a spent-out enrollment code took its own provenance with it"
        );
        assert_eq!(listed[0].used_by, vec!["w1".to_string()]);
        assert_eq!(Some(listed[0].uses), listed[0].max_uses);
    }

    /// One that brought nobody in says nothing, and goes when it stops being usable.
    #[test]
    fn an_enrollment_code_that_expired_unused_is_not_kept() {
        let s = store();
        s.create_enrollment_code(&a_code("j1", "h1", 1, NOW + AN_HOUR)).unwrap();
        assert_eq!(s.list_enrollment_codes(NOW).unwrap().len(), 1, "outstanding");
        assert!(s.list_enrollment_codes(NOW + 2 * AN_HOUR).unwrap().is_empty());
    }

    #[test]
    fn store_and_look_up_worker_credential_by_hash() {
        let s = store();
        s.store_worker_credential("worker-1", "tokenhash1").unwrap();
        let cred = s.get_worker_credential_by_hash("tokenhash1").unwrap().unwrap();
        assert_eq!(cred.worker_id, "worker-1");
        assert!(!cred.revoked);
    }

    #[test]
    fn enrolling_an_existing_worker_id_is_refused_and_keeps_the_old_credential() {
        let s = store();
        s.store_worker_credential("worker-1", "tokenhash1").unwrap();
        assert!(s.store_worker_credential("worker-1", "tokenhash2").is_err());
        assert!(s.get_worker_credential_by_hash("tokenhash1").unwrap().is_some());
        assert!(s.get_worker_credential_by_hash("tokenhash2").unwrap().is_none());
    }

    #[test]
    fn a_revoked_worker_id_stays_revoked_after_an_enrollment_attempt() {
        let s = store();
        s.store_worker_credential("worker-1", "tokenhash1").unwrap();
        s.revoke_worker_credential("worker-1").unwrap();
        assert!(s.store_worker_credential("worker-1", "tokenhash2").is_err());
        let cred = s.get_worker_credential_by_hash("tokenhash1").unwrap().unwrap();
        assert!(cred.revoked);
        assert!(s.get_worker_credential_by_hash("tokenhash2").unwrap().is_none());
    }

    #[test]
    fn revoke_worker_credential_marks_it_revoked() {
        let s = store();
        s.store_worker_credential("worker-1", "tokenhash1").unwrap();
        assert!(s.revoke_worker_credential("worker-1").unwrap());
        let cred = s.get_worker_credential_by_hash("tokenhash1").unwrap().unwrap();
        assert!(cred.revoked);
    }

    #[test]
    fn revoke_worker_credential_on_an_unknown_worker_returns_false_not_an_error() {
        let s = store();
        assert!(!s.revoke_worker_credential("nope").unwrap());
    }

    #[test]
    fn list_worker_credentials_includes_a_revoked_worker() {
        let s = store();
        s.store_worker_credential("worker-1", "hash1").unwrap();
        s.revoke_worker_credential("worker-1").unwrap();
        let creds = s.list_worker_credentials().unwrap();
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].worker_id, "worker-1");
        assert!(creds[0].revoked);
    }

    #[test]
    fn list_worker_credentials_is_ordered_by_worker_id() {
        let s = store();
        for id in ["worker-b", "worker-a", "worker-c"] {
            s.store_worker_credential(id, &format!("hash-{id}")).unwrap();
        }
        let ids: Vec<String> = s
            .list_worker_credentials()
            .unwrap()
            .into_iter()
            .map(|c| c.worker_id)
            .collect();
        assert_eq!(ids, vec!["worker-a", "worker-b", "worker-c"]);
    }

    // -- the secret catalog ------------------------------------------------

    fn leader_stored(s: &SqliteStore, name: &str, value: &str) {
        s.create_secret(name, Custody::LeaderStored, "", Some(value)).unwrap();
    }

    fn record(s: &SqliteStore, name: &str) -> SecretRecord {
        s.list_secrets()
            .unwrap()
            .into_iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("no secret named {name:?}"))
    }

    fn versions(s: &SqliteStore, name: &str) -> Vec<i64> {
        record(s, name).versions.iter().map(|v| v.version).collect()
    }

    /// The point of worker-local custody: declaring the name costs the leader nothing to lose, so it does not even
    /// write a key.
    #[test]
    fn a_worker_local_secret_is_a_name_and_nothing_else() {
        let key = temp_key();
        let s = store_with_key(&key);
        s.create_secret("KAFKA_WEST_SASL", Custody::WorkerLocal, "the west broker", None)
            .unwrap();

        let r = record(&s, "KAFKA_WEST_SASL");
        assert_eq!(r.custody(), Custody::WorkerLocal);
        assert_eq!(r.description, "the west broker");
        assert!(r.versions.is_empty());
        assert!(r.current().is_none());
        assert!(!key.exists(), "no value was stored, so no key was written");
    }

    #[test]
    fn a_stored_secret_starts_at_one_version() {
        let s = store();
        let before = super::super::warnings::now_millis();
        leader_stored(&s, "kafka-west-sasl", "hunter2");

        let r = record(&s, "kafka-west-sasl");
        assert_eq!(r.custody(), Custody::LeaderStored);
        let current = r.current().expect("a stored value");
        assert_eq!(current.version, 1);
        assert!(current.usable);
        assert!(current.set_at >= before);
    }

    /// An issued certificate keeps the names it is issued for until it is deleted; an uploaded one has none.
    #[test]
    fn an_issued_certificate_keeps_its_names_until_deleted() {
        let s = store();
        let issued = Issued {
            names: vec!["a.example.com".to_string(), "10.0.0.7".to_string()],
            validity_days: 90,
        };
        s.create_issued_certificate("issued", "", &issued, "chain", "key")
            .unwrap();
        s.create_certificate("uploaded", "", "chain", "key").unwrap();
        assert_eq!(
            s.issued_certificates().unwrap(),
            [("issued".to_string(), issued)].into_iter().collect()
        );
        assert!(record(&s, "issued").certificate);

        s.delete_secret("issued").unwrap();
        assert!(s.issued_certificates().unwrap().is_empty());
    }

    /// A certificate is a stored secret the Secrets page leaves out, and its value is read only as a certificate.
    #[test]
    fn a_certificate_is_a_secret_with_a_chain_beside_each_version() {
        let s = store();
        s.create_certificate("leader", "served", "chain 1", "key 1").unwrap();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        assert_eq!(s.add_certificate_version("leader", "chain 2", "key 2").unwrap(), 2);

        let r = record(&s, "leader");
        assert!(r.certificate);
        assert_eq!(r.custody(), Custody::LeaderStored);
        assert!(!record(&s, "kafka-west-sasl").certificate);
        assert_eq!(s.certificate_chain("leader", 2).unwrap().as_deref(), Some("chain 2"));
        assert_eq!(s.certificate_chain("kafka-west-sasl", 1).unwrap(), None);
        let delivered = s.secrets_for_delivery(&[("leader".to_string(), 2)], &[]).unwrap();
        assert_eq!(
            values(delivered.clone()),
            [("leader".to_string(), 2, "key 2".to_string())]
        );
        assert_eq!(
            delivered[0].chain.as_deref(),
            Some("chain 2"),
            "the chain travels with its key"
        );
        let plain = s
            .secrets_for_delivery(&[("kafka-west-sasl".to_string(), 1)], &[])
            .unwrap();
        assert_eq!(plain[0].chain, None);

        s.delete_secret_version("leader", 1).unwrap();
        assert_eq!(
            s.certificate_chain("leader", 1).unwrap(),
            None,
            "a chain goes with its version"
        );
        s.delete_secret("leader").unwrap();
        assert_eq!(s.certificate_chain("leader", 2).unwrap(), None);
    }

    #[test]
    fn custody_and_the_value_must_agree_at_creation() {
        let s = store();
        assert!(s.create_secret("no-value", Custody::LeaderStored, "", None).is_err());
        assert!(
            s.create_secret("not-mine", Custody::WorkerLocal, "", Some("hunter2"))
                .is_err()
        );
        assert!(s.list_secrets().unwrap().is_empty(), "neither was written");
    }

    #[test]
    fn a_second_secret_of_the_same_name_is_refused() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        assert!(
            s.create_secret("kafka-west-sasl", Custody::WorkerLocal, "", None)
                .is_err()
        );
        assert!(s.verify_secret("kafka-west-sasl", 1, "hunter2").unwrap(), "unchanged");
    }

    #[test]
    fn a_name_that_could_not_be_written_into_a_bundle_is_refused() {
        let s = store();
        for bad in ["", "has space", "quote\"", "../escape", "wide\u{00e9}"] {
            assert!(
                s.create_secret(bad, Custody::WorkerLocal, "", None).is_err(),
                "{bad:?} should be refused"
            );
        }
    }

    #[test]
    fn verify_answers_for_one_version_and_for_nothing_else() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        assert!(s.verify_secret("kafka-west-sasl", 1, "hunter2").unwrap());
        assert!(!s.verify_secret("kafka-west-sasl", 1, "hunter3").unwrap());
        assert!(
            !s.verify_secret("kafka-west-sasl", 2, "hunter2").unwrap(),
            "no such version"
        );
        assert!(!s.verify_secret("never-declared", 1, "hunter2").unwrap());

        s.create_secret("KAFKA_EAST_SASL", Custody::WorkerLocal, "", None)
            .unwrap();
        assert!(
            !s.verify_secret("KAFKA_EAST_SASL", 1, "hunter2").unwrap(),
            "the leader holds no value to compare against"
        );
    }

    /// Adding, not replacing: the version a bundle quotes has to keep answering after a rotation, or rolling one
    /// back would restore a reference to nothing.
    #[test]
    fn a_new_value_is_a_new_version_and_the_old_one_still_answers() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");

        assert_eq!(s.add_secret_version("kafka-west-sasl", "hunter3").unwrap(), 2);
        assert_eq!(s.add_secret_version("kafka-west-sasl", "hunter4").unwrap(), 3);
        assert_eq!(versions(&s, "kafka-west-sasl"), vec![1, 2, 3]);
        assert_eq!(record(&s, "kafka-west-sasl").current().unwrap().version, 3);

        assert!(s.verify_secret("kafka-west-sasl", 1, "hunter2").unwrap());
        assert!(s.verify_secret("kafka-west-sasl", 2, "hunter3").unwrap());
        assert!(s.verify_secret("kafka-west-sasl", 3, "hunter4").unwrap());
    }

    /// A number is never reused, so a bundle quoting a forgotten version reads as a credential this leader cannot
    /// produce -- never as a different value wearing the same number.
    #[test]
    fn a_forgotten_version_number_is_not_handed_out_again() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        assert_eq!(s.add_secret_version("kafka-west-sasl", "hunter3").unwrap(), 2);

        assert!(s.delete_secret_version("kafka-west-sasl", 2).unwrap());
        assert!(!s.delete_secret_version("kafka-west-sasl", 2).unwrap(), "gone already");
        assert_eq!(versions(&s, "kafka-west-sasl"), vec![1]);

        assert_eq!(
            s.add_secret_version("kafka-west-sasl", "hunter4").unwrap(),
            3,
            "counts past what was forgotten"
        );
        assert!(!s.verify_secret("kafka-west-sasl", 2, "hunter3").unwrap());
        assert!(!s.verify_secret("kafka-west-sasl", 2, "hunter4").unwrap());
    }

    #[test]
    fn only_a_leader_stored_secret_has_a_value_to_add() {
        let s = store();
        s.create_secret("KAFKA_EAST_SASL", Custody::WorkerLocal, "", None)
            .unwrap();
        assert!(s.add_secret_version("KAFKA_EAST_SASL", "hunter2").is_err());
        assert!(s.add_secret_version("never-declared", "hunter2").is_err());

        let r = record(&s, "KAFKA_EAST_SASL");
        assert_eq!(r.custody(), Custody::WorkerLocal);
        assert!(r.versions.is_empty());
        assert!(!s.verify_secret("KAFKA_EAST_SASL", 1, "hunter2").unwrap());
    }

    #[test]
    fn deleting_says_whether_there_was_one_and_takes_every_version() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        s.add_secret_version("kafka-west-sasl", "hunter3").unwrap();

        assert!(s.delete_secret("kafka-west-sasl").unwrap());
        assert!(!s.delete_secret("kafka-west-sasl").unwrap());
        assert!(s.list_secrets().unwrap().is_empty());
        assert!(!s.verify_secret("kafka-west-sasl", 1, "hunter2").unwrap());
        assert!(!s.verify_secret("kafka-west-sasl", 2, "hunter3").unwrap());
    }

    /// The restore-without-the-key case. It reads as a credential the fleet cannot obtain, which is the same fact
    /// as an unset environment variable and takes the same path -- not as a corrupt database.
    #[test]
    fn a_value_whose_key_is_gone_is_unusable_rather_than_an_error() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        std::fs::remove_file(&key).unwrap();

        let r = record(&s, "kafka-west-sasl");
        let current = r.current().expect("the row is still there");
        assert!(!current.usable);
        assert_eq!(current.version, 1, "and still says which value it was");
        assert!(!s.verify_secret("kafka-west-sasl", 1, "hunter2").unwrap());
    }

    fn regenerated() -> SecurityEvent {
        crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "key-regenerate", "", "ok")
    }

    fn secret(name: &str) -> Unopenable {
        Unopenable::Secret(name.to_string())
    }

    #[test]
    fn a_leader_that_has_sealed_nothing_has_no_key_and_nothing_wrong() {
        let s = store();
        let standing = s.key_standing().unwrap();
        assert_eq!(standing.key, KeyState::Absent);
        assert!(!standing.broken());
        assert!(standing.unopenable.is_empty());
    }

    /// The case issue 21 is about: values sealed, key gone. Every value is named, and nothing more is sealed until
    /// the key is back or regenerated -- a value sealed now would be one the restored key could not open.
    #[test]
    fn a_missing_key_with_values_sealed_is_broken_and_refuses_to_seal() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        let token = s.sealed_setting("git.remote.token", "s3cret").unwrap();
        s.put_settings(&[("git.remote.token", token)], None).unwrap();
        std::fs::remove_file(&key).unwrap();

        let standing = s.key_standing().unwrap();
        assert_eq!(standing.key, KeyState::Missing);
        assert_eq!(
            standing.unopenable,
            vec![
                secret("kafka-west-sasl"),
                Unopenable::Setting("git.remote.token".to_string())
            ]
        );
        let problem = standing.problem().expect("a broken key is a problem");
        assert!(
            problem.contains("is missing, so the 2 stored secrets cannot be opened"),
            "{problem}"
        );

        let refused = s.add_secret_version("kafka-west-sasl", "hunter3").unwrap_err();
        assert_eq!(refused.0, problem);
        assert!(
            s.create_secret("es-auth", Custody::LeaderStored, "", Some("x"))
                .is_err()
        );
        assert!(s.sealed_setting("agents.api_key", "sk-1").is_err());
        assert!(!key.exists(), "a refused write does not write a key either");
        s.create_secret("pg-replica", Custody::WorkerLocal, "", None)
            .expect("a worker-local secret seals nothing");
    }

    #[test]
    fn a_key_that_did_not_seal_the_values_is_mismatched() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        std::fs::remove_file(&key).unwrap();
        let other = secrets::load_or_create(&key).unwrap();

        let standing = s.key_standing().unwrap();
        assert_eq!(
            standing.key,
            KeyState::Mismatched {
                key_id: other.key_id().to_string()
            }
        );
        assert!(
            standing
                .problem()
                .unwrap()
                .contains("does not match the stored secrets")
        );
        assert!(s.add_secret_version("kafka-west-sasl", "hunter3").is_err());
    }

    /// Regenerating keeps the old values and names each one to enter again; entering one clears it.
    #[test]
    fn a_regenerated_key_lists_what_to_enter_again_until_it_is() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        leader_stored(&s, "es-auth", "changeme");
        s.create_certificate("ingest-tls", "", "chain", "key").unwrap();
        std::fs::remove_file(&key).unwrap();

        let standing = s.regenerate_key(&regenerated()).unwrap();
        assert!(matches!(standing.key, KeyState::Held { .. }), "{:?}", standing.key);
        assert_eq!(
            standing.unopenable,
            vec![
                secret("es-auth"),
                Unopenable::Certificate("ingest-tls".to_string()),
                secret("kafka-west-sasl")
            ]
        );
        assert!(key.exists());
        assert!(
            !record(&s, "es-auth").current().unwrap().usable,
            "the old value stays, unusable"
        );

        s.add_secret_version("es-auth", "changeme2")
            .expect("sealing works again");
        s.add_certificate_version("ingest-tls", "chain2", "key2").unwrap();
        assert_eq!(s.key_standing().unwrap().unopenable, vec![secret("kafka-west-sasl")]);
        assert_eq!(
            s.list_security_events(10)
                .unwrap()
                .iter()
                .filter(|e| e.action == "key-regenerate")
                .count(),
            1,
            "the regeneration is audited"
        );
    }

    #[test]
    fn an_older_version_under_a_lost_key_does_not_break_the_key() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        s.add_secret_version("kafka-west-sasl", "hunter3").unwrap();
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE secret_versions SET key_id = '0000000000000000' WHERE version = 1",
                (),
            )
            .unwrap();

        let standing = s.key_standing().unwrap();
        assert!(matches!(standing.key, KeyState::Held { .. }), "{:?}", standing.key);
        assert!(standing.unopenable.is_empty());
    }

    /// A key file that is there but wrong may yet be the right key for something, so it is kept beside the new one.
    #[test]
    fn regenerating_over_a_mismatched_key_moves_it_aside() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        std::fs::remove_file(&key).unwrap();
        let wrong = std::fs::read_to_string({
            secrets::load_or_create(&key).unwrap();
            &key
        })
        .unwrap();

        s.regenerate_key(&regenerated()).unwrap();
        let aside: Vec<_> = std::fs::read_dir(key.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().contains(".replaced-"))
            .collect();
        assert_eq!(aside.len(), 1);
        assert_eq!(std::fs::read_to_string(&aside[0]).unwrap(), wrong);
        assert_ne!(std::fs::read_to_string(&key).unwrap(), wrong);
    }

    #[test]
    fn a_working_key_is_not_regenerated() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        assert!(s.regenerate_key(&regenerated()).is_err());
        assert!(s.regenerate_key(&regenerated()).is_err(), "nor is an absent one");
    }

    /// The name is sealed in with the value, so lifting one row's ciphertext into another buys nothing -- which is
    /// what stops a database write from redirecting a connection to a credential its admin can read.
    #[test]
    fn a_ciphertext_moved_to_another_row_does_not_open_there() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        leader_stored(&s, "kafka-east-sasl", "swordfish");

        {
            let conn = s.conn.lock().unwrap();
            conn.execute(
                "UPDATE secret_versions SET key_id = w.key_id, nonce = w.nonce, ciphertext = w.ciphertext
                   FROM (SELECT key_id, nonce, ciphertext FROM secret_versions WHERE name = 'kafka-west-sasl') AS w
                  WHERE name = 'kafka-east-sasl'",
                (),
            )
            .unwrap();
        }

        assert!(!s.verify_secret("kafka-east-sasl", 1, "hunter2").unwrap());
        assert!(!s.verify_secret("kafka-east-sasl", 1, "swordfish").unwrap());
    }

    /// Each delivery's name, version and value.
    fn values(delivered: Vec<crate::heartbeat::SecretDelivery>) -> Vec<(String, i64, String)> {
        delivered.into_iter().map(|d| (d.name, d.version, d.value)).collect()
    }

    /// Only the pairs the group's bundle quotes, and only what the worker lacks. The version is matched exactly:
    /// a worker asking about one it holds is told nothing, whatever else has been stored since.
    #[test]
    fn delivery_carries_the_versions_a_group_quotes_and_a_worker_lacks() {
        let s = store();
        leader_stored(&s, "kafka-west-sasl", "hunter2");
        leader_stored(&s, "es-auth", "ApiKey abc");
        leader_stored(&s, "not-ours", "swordfish");
        s.add_secret_version("es-auth", "ApiKey def").unwrap();

        let entitled = [("kafka-west-sasl".to_string(), 1), ("es-auth".to_string(), 1)];
        let mut out = values(s.secrets_for_delivery(&entitled, &[]).unwrap());
        out.sort();
        assert_eq!(
            out,
            vec![
                ("es-auth".to_string(), 1, "ApiKey abc".to_string()),
                ("kafka-west-sasl".to_string(), 1, "hunter2".to_string()),
            ],
            "the quoted version, not the newest, and nothing the group does not name"
        );

        let held = [("kafka-west-sasl".to_string(), 1), ("es-auth".to_string(), 1)];
        assert!(
            s.secrets_for_delivery(&entitled, &held).unwrap().is_empty(),
            "a worker already holding what its bundle quotes is sent nothing"
        );

        let bumped = [("es-auth".to_string(), 2)];
        assert_eq!(
            values(s.secrets_for_delivery(&bumped, &held).unwrap()),
            vec![("es-auth".to_string(), 2, "ApiKey def".to_string())],
            "and the bundle moving is what asks for the new one"
        );
    }

    /// A worker holding a version its bundle no longer quotes is not a reason to withhold the one it does.
    #[test]
    fn holding_another_version_does_not_satisfy_the_one_asked_for() {
        let s = store();
        leader_stored(&s, "es-auth", "ApiKey abc");
        s.add_secret_version("es-auth", "ApiKey def").unwrap();

        assert_eq!(
            values(
                s.secrets_for_delivery(&[("es-auth".to_string(), 2)], &[("es-auth".to_string(), 1)])
                    .unwrap()
            ),
            vec![("es-auth".to_string(), 2, "ApiKey def".to_string())]
        );
        assert_eq!(
            values(
                s.secrets_for_delivery(&[("es-auth".to_string(), 1)], &[("es-auth".to_string(), 2)])
                    .unwrap()
            ),
            vec![("es-auth".to_string(), 1, "ApiKey abc".to_string())],
            "a rollback asks for the older one and gets it"
        );
    }

    #[test]
    fn a_version_that_is_not_held_is_not_delivered() {
        let s = store();
        s.create_secret("KAFKA_EAST_SASL", Custody::WorkerLocal, "", None)
            .unwrap();
        leader_stored(&s, "es-auth", "ApiKey abc");
        assert!(
            s.secrets_for_delivery(
                &[
                    ("KAFKA_EAST_SASL".to_string(), 1),
                    ("never-declared".to_string(), 1),
                    ("es-auth".to_string(), 9),
                ],
                &[]
            )
            .unwrap()
            .is_empty()
        );
    }

    /// The restore-without-the-key case reaches the worker as a credential it does not have, refusing the deploy --
    /// not as a delivery of something that will not open.
    #[test]
    fn a_value_whose_key_is_gone_is_not_delivered() {
        let key = temp_key();
        let s = store_with_key(&key);
        leader_stored(&s, "es-auth", "ApiKey abc");
        std::fs::remove_file(&key).unwrap();
        assert!(
            s.secrets_for_delivery(&[("es-auth".to_string(), 1)], &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn secrets_list_by_name() {
        let s = store();
        for name in ["zulu", "alpha", "mike"] {
            s.create_secret(name, Custody::WorkerLocal, "", None).unwrap();
        }
        let names: Vec<_> = s.list_secrets().unwrap().into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["alpha", "mike", "zulu"]);
    }

    // -- group hues -----------------------------------------------------------

    #[test]
    fn setting_a_hue_twice_replaces_rather_than_duplicates() {
        let s = store();
        s.set_group_hue("west", 120).unwrap();
        s.set_group_hue("west", 200).unwrap();
        let hues = s.group_hues().unwrap();
        assert_eq!(hues.len(), 1);
        assert_eq!(hues.get("west"), Some(&200));
    }

    #[test]
    fn two_groups_each_keep_their_own_hue() {
        let s = store();
        s.set_group_hue("west", 120).unwrap();
        s.set_group_hue("east", 300).unwrap();
        let hues = s.group_hues().unwrap();
        assert_eq!(hues.get("west"), Some(&120));
        assert_eq!(hues.get("east"), Some(&300));
    }

    // -- people ------------------------------------------------------------

    fn new_user<'a>(id: &'a str, username: &'a str) -> NewLocalUser<'a> {
        NewLocalUser {
            id,
            username,
            display_name: "A Person",
            password_hash: "$argon2id$v=19$m=64,t=1,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYQ",
            must_change: false,
        }
    }

    /// The condition bootstrap tests for, and what it stops testing as soon as it has run once.
    #[test]
    fn a_fresh_store_holds_nobody() {
        let s = store();
        assert!(!s.any_user_exists().unwrap());
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        assert!(s.any_user_exists().unwrap());
    }

    /// Two processes bootstrapping one file: the check and the insert are one transaction, so the second finds an
    /// account and writes nothing rather than adding a second administrator nobody was told about.
    #[test]
    fn only_the_first_bootstrap_creates_anybody() {
        let s = store();
        assert!(s.bootstrap_administrator(new_user("u_1", "admin")).unwrap());
        assert!(!s.bootstrap_administrator(new_user("u_2", "admin2")).unwrap());
        assert_eq!(s.list_users().unwrap().len(), 1);
    }

    #[test]
    fn a_username_is_taken_once() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        assert_eq!(
            s.create_local_user(
                new_user("u_2", "admin"),
                &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", "", "ok")
            )
            .unwrap(),
            Err(AccountError::UsernameTaken)
        );
    }

    /// A disabled account keeps its name. Releasing it would let a new person inherit what the old one's audit
    /// rows point at.
    #[test]
    fn a_disabled_accounts_username_is_not_released() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        s.create_local_user(
            new_user("u_2", "bob"),
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", "", "ok"),
        )
        .unwrap()
        .unwrap();
        s.set_user_enabled(
            "u_2",
            false,
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-disable", "", "ok"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            s.create_local_user(
                new_user("u_3", "bob"),
                &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", "", "ok")
            )
            .unwrap(),
            Err(AccountError::UsernameTaken)
        );
    }

    /// The store is told the canonical form, so it compares exactly; `accounts::canonical_username` is what folds
    /// case, once, on the way in.
    #[test]
    fn the_store_compares_usernames_as_given() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        assert!(s.local_credential_by_username("admin").unwrap().is_some());
        assert!(s.local_credential_by_username("ADMIN").unwrap().is_none());
    }

    /// A setting nobody has touched is an absent row, so the default applies and the leader does not have to tell
    /// a stored zero from an unset one.
    #[test]
    fn an_unset_setting_reads_as_the_default() {
        let store = store();
        assert_eq!(store.setting(Lifetimes::IDLE_KEY).unwrap(), None);
        assert_eq!(Lifetimes::read(&store).unwrap(), Lifetimes::default());
    }

    #[test]
    fn session_timeouts_survive_a_write_and_a_read() {
        let store = store();
        let chosen = Lifetimes::new(Duration::from_secs(15 * 60), Duration::from_secs(8 * 60 * 60)).unwrap();
        chosen.write(&store, None).unwrap();
        assert_eq!(Lifetimes::read(&store).unwrap(), chosen);

        // Writing again replaces rather than duplicating: the key is the primary key.
        let again = Lifetimes::new(Duration::from_secs(5 * 60), Duration::from_secs(60 * 60)).unwrap();
        again.write(&store, None).unwrap();
        assert_eq!(Lifetimes::read(&store).unwrap(), again);
    }

    /// A row an older build wrote, or a hand edit, must not start a leader enforcing deadlines nobody configured.
    /// A change to how long a session lives must not land without the row saying who made it. Before this the
    /// audit was a separate best-effort insert afterward, which is exactly the gap an audit exists to close.
    #[test]
    fn a_settings_write_carries_its_audit_row() {
        let store = store();
        let event = crate::leader::accounts::event_for(
            &crate::leader::store::Actor::system(),
            "session-timeouts",
            "leader",
            "idle=900s absolute=28800s",
        );
        let chosen = Lifetimes::new(Duration::from_secs(900), Duration::from_secs(28800)).unwrap();
        chosen.write(&store, Some(&event)).unwrap();

        assert_eq!(Lifetimes::read(&store).unwrap(), chosen);
        let recorded = store.list_security_events(10).unwrap();
        assert!(
            recorded
                .iter()
                .any(|e| e.action == "session-timeouts" && e.outcome.contains("900")),
            "the settings landed without an audit row: {recorded:?}"
        );
    }

    #[test]
    fn a_sealed_setting_is_stored_sealed_and_opens_only_under_its_own_name() {
        let store = store();
        let sealed = store.sealed_setting("git.remote.token", "s3cret").unwrap();
        assert!(!sealed.contains("s3cret"));
        store
            .put_settings(&[("git.remote.token", sealed.clone()), ("elsewhere", sealed)], None)
            .unwrap();
        assert_eq!(
            store.opened_setting("git.remote.token").unwrap().as_deref(),
            Some("s3cret")
        );
        assert_eq!(store.opened_setting("elsewhere").unwrap(), None, "bound to its name");
        assert_eq!(store.opened_setting("unset").unwrap(), None);
    }

    #[test]
    fn a_stored_pair_that_no_longer_validates_is_reported() {
        let store = store();
        store
            .put_settings(
                &[
                    (Lifetimes::IDLE_KEY, "86400".to_string()),
                    (Lifetimes::ABSOLUTE_KEY, "3600".to_string()),
                ],
                None,
            )
            .unwrap();
        let e = Lifetimes::read(&store).unwrap_err();
        assert!(e.contains("could never be reached"), "{e}");

        store
            .put_settings(&[(Lifetimes::IDLE_KEY, "half an hour".to_string())], None)
            .unwrap();
        let e = Lifetimes::read(&store).unwrap_err();
        assert!(e.contains("not a number of seconds"), "{e}");
    }

    /// Two accounts, both disabled in turn: the second refusal is what proves the count is taken inside the
    /// transaction rather than from the roster the caller read.
    #[test]
    fn disabling_stops_at_the_last_one_who_can_sign_in() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        s.create_local_user(
            new_user("u_2", "bob"),
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", "", "ok"),
        )
        .unwrap()
        .unwrap();
        s.set_user_enabled(
            "u_1",
            false,
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-disable", "", "ok"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            s.set_user_enabled(
                "u_2",
                false,
                &crate::leader::accounts::event_for(
                    &crate::leader::store::Actor::system(),
                    "account-disable",
                    "",
                    "ok"
                )
            )
            .unwrap(),
            Err(AccountError::LastAdministrator)
        );
    }

    /// A password is chosen against the account state it was authorized against. One that moved meanwhile keeps
    /// the credential it moved to: an administrator's reset is not overwritten by what was already hashing.
    #[test]
    fn a_chosen_password_written_against_a_stale_generation_lands_nowhere() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        let stale = s.get_user("u_1").unwrap().unwrap().generation;
        let held = s.local_credential_by_username("admin").unwrap().unwrap().password_hash;
        let event =
            crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "password-reset", "u_1", "ok");
        s.set_local_password(
            "u_1",
            "$argon2id$v=19$m=64,t=1,p=1$cmVzZXQ$cmVzZXRoYXNo",
            true,
            None,
            Some(&event),
        )
        .unwrap()
        .unwrap();
        let after_reset = s.local_credential_by_username("admin").unwrap().unwrap();

        assert_eq!(
            s.set_local_password(
                "u_1",
                "$argon2id$v=19$m=64,t=1,p=1$Y2hvc2Vu$Y2hvc2VuaGFzaA",
                false,
                Some(stale),
                Some(&event)
            )
            .unwrap(),
            Err(AccountError::Superseded)
        );
        let now = s.local_credential_by_username("admin").unwrap().unwrap();
        assert_eq!(
            now.password_hash, after_reset.password_hash,
            "the reset was overwritten"
        );
        assert_ne!(
            now.password_hash, held,
            "the reset never landed, so this proves nothing"
        );
        assert!(now.must_change, "the reset's must-change flag was cleared");
    }

    /// Every path that invalidates a session moves the same number, so one comparison at request time covers a
    /// password change, a reset and an explicit revocation alike.
    #[test]
    fn a_password_change_and_a_revocation_both_move_the_generation() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        let first = s.get_user("u_1").unwrap().unwrap().generation;
        let reported = s
            .set_local_password("u_1", "$argon2id$v=19$m=64,t=1,p=1$c2FsdA$aGFzaA", false, None, None)
            .unwrap()
            .unwrap();
        let second = s.get_user("u_1").unwrap().unwrap().generation;
        assert!(second > first);
        assert_eq!(
            reported, second,
            "the write reported a generation the account is not at"
        );
        s.revoke_sessions(
            "u_1",
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "revoke-sessions", "", "ok"),
        )
        .unwrap()
        .unwrap();
        assert!(s.get_user("u_1").unwrap().unwrap().generation > second);
    }

    #[test]
    fn an_operation_on_a_missing_account_says_so() {
        let s = store();
        assert_eq!(
            s.revoke_sessions(
                "nobody",
                &crate::leader::accounts::event_for(
                    &crate::leader::store::Actor::system(),
                    "revoke-sessions",
                    "",
                    "ok"
                )
            )
            .unwrap(),
            Err(AccountError::NoSuchUser)
        );
        assert_eq!(
            s.set_display_name("nobody", "x").unwrap(),
            Err(AccountError::NoSuchUser)
        );
        assert_eq!(
            s.set_user_enabled(
                "nobody",
                true,
                &crate::leader::accounts::event_for(
                    &crate::leader::store::Actor::system(),
                    "account-disable",
                    "",
                    "ok"
                )
            )
            .unwrap(),
            Err(AccountError::NoSuchUser)
        );
        assert_eq!(
            s.set_local_password("nobody", "x", false, None, None).unwrap(),
            Err(AccountError::NoSuchUser)
        );
        assert_eq!(
            s.recover_local_user("nobody", "x").unwrap(),
            Err(AccountError::NoSuchUser)
        );
    }

    /// What recovery is for: an account disabled, with a password nobody has, is signed back in to.
    #[test]
    fn recovery_reenables_the_account_and_moves_its_generation() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        s.create_local_user(
            new_user("u_2", "bob"),
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", "", "ok"),
        )
        .unwrap()
        .unwrap();
        s.set_user_enabled(
            "u_1",
            false,
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-disable", "", "ok"),
        )
        .unwrap()
        .unwrap();
        let before = s.get_user("u_1").unwrap().unwrap().generation;

        let id = s
            .recover_local_user("admin", "$argon2id$v=19$m=64,t=1,p=1$c2FsdA$bmV3")
            .unwrap()
            .unwrap();
        assert_eq!(id, "u_1");
        let after = s.get_user("u_1").unwrap().unwrap();
        assert!(after.enabled);
        assert!(after.administrator);
        assert!(after.generation > before);
        let credential = s.local_credential_by_username("admin").unwrap().unwrap();
        assert!(credential.must_change, "a password nobody chose has to be changed");
    }

    /// The roster is what the GUI and the audit read. A verifier reaching it would be a password hash in every
    /// response that lists people.
    #[test]
    fn the_roster_carries_no_password_hash() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        let listed = s.list_users().unwrap();
        let rendered = format!("{listed:?}");
        assert!(!rendered.contains("argon2"), "{rendered}");
    }

    /// A password belongs to a local account by the schema, not by the code above it: the composite foreign key is
    /// what an OIDC account runs into.
    #[test]
    fn a_credential_cannot_hang_off_a_non_local_account() {
        let s = store();
        let conn = s.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (id, auth_source, display_name, enabled, administrator, generation, created_at, updated_at)
             VALUES ('u_oidc', 'oidc', 'Someone', 1, 1, 1, 0, 0)",
            [],
        )
        .unwrap();
        let refused = conn.execute(
            "INSERT INTO local_credentials (user_id, auth_source, username, password_hash, must_change)
             VALUES ('u_oidc', 'local', 'someone', 'x', 0)",
            [],
        );
        assert!(refused.is_err(), "a local credential was written for an oidc account");
    }

    fn sign_in<'a>(
        issuer: &'a str,
        subject: &'a str,
        groups: &'a [String],
        new_id: &'a str,
        at: i64,
    ) -> OidcSignIn<'a> {
        OidcSignIn {
            issuer,
            subject,
            display_name: "Someone",
            groups,
            login: None,
            new_id,
            at,
        }
    }

    fn created(target: &str) -> SecurityEvent {
        crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-create", target, "ok")
    }

    /// The first sign-in makes an enabled account, holding nothing by its own flag, and says who made it; the next
    /// finds the same account and records what the provider says now.
    #[test]
    fn an_oidc_identity_is_one_account_across_sign_ins() {
        let s = store();
        let first = s
            .oidc_sign_in(
                OidcSignIn {
                    login: Some("ada@example.com"),
                    ..sign_in("https://idp", "sub-1", &["ops".into()], "u_a", NOW)
                },
                &created("u_a"),
            )
            .unwrap();
        assert_eq!(
            s.oidc_identity_of("u_a").unwrap().unwrap().login.as_deref(),
            Some("ada@example.com")
        );
        assert_eq!(first.id, "u_a");
        assert_eq!(first.auth_source, AuthSource::Oidc);
        assert!(
            first.enabled && !first.administrator,
            "an OIDC account's access comes from its roles"
        );
        assert_eq!(s.list_security_events(10).unwrap().len(), 1);

        let groups = vec!["ops".to_string(), "dev".to_string()];
        let again = s
            .oidc_sign_in(
                OidcSignIn {
                    login: Some("ada"),
                    ..sign_in("https://idp", "sub-1", &groups, "u_b", NOW + 1)
                },
                &created("u_b"),
            )
            .unwrap();
        assert_eq!(again.id, "u_a", "a second sign-in made a second account");
        assert!(s.get_user("u_b").unwrap().is_none());
        assert_eq!(
            s.list_security_events(10).unwrap().len(),
            1,
            "a returning sign-in recorded a creation"
        );
        let identity = s.oidc_identity_of("u_a").unwrap().unwrap();
        assert_eq!(identity.groups, groups);
        assert_eq!(identity.login.as_deref(), Some("ada"));
        assert_eq!(identity.last_sign_in, NOW + 1);
        assert_eq!(
            (identity.issuer.as_str(), identity.subject.as_str()),
            ("https://idp", "sub-1")
        );
    }

    /// A refresh records what the provider says now without counting as a sign-in.
    #[test]
    fn a_refresh_records_groups_and_login_but_not_a_sign_in() {
        let s = store();
        s.oidc_sign_in(
            sign_in("https://idp", "sub", &["ops".into()], "u_a", NOW),
            &created("u_a"),
        )
        .unwrap();
        let groups = vec!["dev".to_string()];
        s.oidc_refreshed("u_a", &groups, Some("ada@example.com")).unwrap();
        let identity = s.oidc_identity_of("u_a").unwrap().unwrap();
        assert_eq!(identity.groups, groups);
        assert_eq!(identity.login.as_deref(), Some("ada@example.com"));
        assert_eq!(identity.last_sign_in, NOW, "a refresh moved the last sign-in");

        s.oidc_refreshed("u_none", &groups, None).unwrap();
        assert!(s.oidc_identity_of("u_none").unwrap().is_none());
    }

    /// A subject is unique only within its issuer, so the same one from another provider is another person.
    #[test]
    fn the_same_subject_under_another_issuer_is_another_account() {
        let s = store();
        let a = s
            .oidc_sign_in(sign_in("https://one", "sub", &[], "u_a", NOW), &created("u_a"))
            .unwrap();
        let b = s
            .oidc_sign_in(sign_in("https://two", "sub", &[], "u_b", NOW), &created("u_b"))
            .unwrap();
        assert_ne!(a.id, b.id);
    }

    /// The store answers who the identity is; refusing a disabled account is the caller's, as for a local login.
    #[test]
    fn a_disabled_oidc_account_signs_in_as_itself_disabled() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        s.oidc_sign_in(sign_in("https://idp", "sub", &[], "u_a", NOW), &created("u_a"))
            .unwrap();
        s.set_user_enabled(
            "u_a",
            false,
            &crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), "account-disable", "u_a", "ok"),
        )
        .unwrap()
        .unwrap();
        let back = s
            .oidc_sign_in(sign_in("https://idp", "sub", &[], "u_b", NOW + 1), &created("u_b"))
            .unwrap();
        assert_eq!(back.id, "u_a");
        assert!(!back.enabled);
    }

    /// Local administrators are the way in when the provider is not: an OIDC administrator does not stand in for
    /// the last of them, even one whose flag says it is one.
    #[test]
    fn an_oidc_administrator_does_not_count_as_one_who_can_sign_in() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        s.oidc_sign_in(sign_in("https://idp", "sub", &[], "u_a", NOW), &created("u_a"))
            .unwrap();
        s.conn
            .lock()
            .unwrap()
            .execute("UPDATE users SET administrator = 1 WHERE id = 'u_a'", [])
            .unwrap();
        assert_eq!(
            s.set_user_enabled(
                "u_1",
                false,
                &crate::leader::accounts::event_for(
                    &crate::leader::store::Actor::system(),
                    "account-disable",
                    "u_1",
                    "ok"
                )
            )
            .unwrap(),
            Err(AccountError::LastAdministrator)
        );
    }

    #[test]
    fn an_oidc_account_has_no_password_to_set() {
        let s = store();
        s.oidc_sign_in(sign_in("https://idp", "sub", &[], "u_a", NOW), &created("u_a"))
            .unwrap();
        assert_eq!(
            s.set_local_password("u_a", "x", false, None, None).unwrap(),
            Err(AccountError::WrongAuthSource)
        );
        assert!(s.username_of("u_a").unwrap().is_none());
    }

    #[test]
    fn a_local_account_has_no_oidc_identity() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        assert!(s.oidc_identity_of("u_1").unwrap().is_none());
    }

    /// The mirror of the password rule: an identity cannot hang off a local account either.
    #[test]
    fn an_identity_cannot_hang_off_a_local_account() {
        let s = store();
        s.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        let conn = s.conn.lock().unwrap();
        let refused = conn.execute(
            "INSERT INTO oidc_identities (user_id, auth_source, issuer, subject, last_sign_in)
             VALUES ('u_1', 'oidc', 'https://idp', 'sub', 0)",
            [],
        );
        assert!(refused.is_err(), "an oidc identity was written for a local account");
    }

    #[test]
    fn security_events_come_back_newest_first() {
        let s = store();
        for (i, action) in ["bootstrap", "login", "logout"].iter().enumerate() {
            s.record_security_event(&SecurityEvent {
                id: format!("e{i}"),
                at: i as i64,
                actor: "host".into(),
                action: (*action).into(),
                target: "u_1".into(),
                outcome: "ok".into(),
            })
            .unwrap();
        }
        let listed = s.list_security_events(2).unwrap();
        assert_eq!(
            listed.iter().map(|e| e.action.as_str()).collect::<Vec<_>>(),
            vec!["logout", "login"]
        );
    }

    // -- schema ------------------------------------------------------------

    /// A file that has had every step applied is at the version this build serves, and opening it again applies
    /// nothing.
    #[test]
    fn opening_twice_leaves_the_schema_where_it_was() {
        let dir = std::env::temp_dir().join(format!("smeltr-migrate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("twice.db");
        let _ = std::fs::remove_file(&db);
        let key = temp_key();

        let first = SqliteStore::open(&db, &key).unwrap();
        first.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        drop(first);

        let again = SqliteStore::open(&db, &key).unwrap();
        assert!(again.any_user_exists().unwrap(), "the account survived reopening");
        let version: i64 = again
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version as usize, STEPS.len());
    }

    /// A damaged file is refused at open, naming the file and where to get it back from.
    #[test]
    fn a_damaged_database_is_refused() {
        let dir = std::env::temp_dir().join(format!("smeltr-damaged-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("leader.db");
        let _ = std::fs::remove_file(&db);
        let key = temp_key();
        let store = SqliteStore::open(&db, &key).unwrap();
        store.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        drop(store);
        // A second sound start, whose copy holds the account.
        drop(SqliteStore::open(&db, &key).unwrap());

        // Garbage over page 2, a table's root: the header still reads, so it is the integrity check that finds it.
        let mut bytes = std::fs::read(&db).unwrap();
        let page = usize::from(u16::from_be_bytes([bytes[16], bytes[17]]));
        bytes[page..2 * page].fill(0xA5);
        std::fs::write(&db, &bytes).unwrap();

        let refused = SqliteStore::open(&db, &key).err().expect("a damaged file opened").0;
        assert!(refused.contains(&db.display().to_string()), "{refused}");
        assert!(
            refused.contains("damaged") && refused.contains("a backup of your own"),
            "{refused}"
        );

        // The copy the sound start kept is named, and it is the store as that start found it.
        let previous = previous_path(&db);
        assert!(refused.contains(&previous.display().to_string()), "{refused}");
        std::fs::rename(&previous, &db).unwrap();
        for sidecar in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{sidecar}", db.display()));
        }
        let restored = SqliteStore::open(&db, &key).unwrap();
        assert!(restored.get_user("u_1").unwrap().is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An index whose key no longer matches its table: every page still reads, so only the full check finds it.
    fn flip_an_index_key(db: &Path) {
        let conn = Connection::open(db).unwrap();
        let root: i64 = conn
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE name = 'sqlite_autoindex_users_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        let mut bytes = std::fs::read(db).unwrap();
        let page = match u16::from_be_bytes([bytes[16], bytes[17]]) {
            1 => 65536,
            size => usize::from(size),
        };
        let (from, to) = ((root as usize - 1) * page, root as usize * page);
        let at = bytes[from..to]
            .windows(3)
            .position(|w| w == b"u_1")
            .expect("the index page holds the key");
        bytes[from + at + 2] = b'9';
        std::fs::write(db, &bytes).unwrap();
    }

    /// An ordinary start reads every page and no more; a start that will run steps checks each index against its
    /// table as well, since the steps are about to rewrite the file.
    #[test]
    fn a_start_that_will_upgrade_checks_the_indexes_and_an_ordinary_one_does_not() {
        let dir = std::env::temp_dir().join(format!("smeltr-index-damage-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("leader.db");
        let _ = std::fs::remove_file(&db);
        let key = temp_key();
        let store = SqliteStore::open(&db, &key).unwrap();
        store.bootstrap_administrator(new_user("u_1", "admin")).unwrap();
        drop(store);

        flip_an_index_key(&db);
        drop(SqliteStore::open(&db, &key).expect("an ordinary start takes the quick check"));

        // One step behind: this start would run it.
        let conn = Connection::open(&db).unwrap();
        conn.pragma_update(None, "user_version", (STEPS.len() - 1) as i64)
            .unwrap();
        drop(conn);
        let refused = SqliteStore::open(&db, &key)
            .err()
            .expect("a start about to upgrade opened")
            .0;
        assert!(
            refused.contains("damaged") && refused.contains("sqlite_autoindex_users_1"),
            "{refused}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A file a newer build broke for this one is refused (`steps`).
    #[test]
    fn a_database_from_a_newer_build_is_refused() {
        let dir = std::env::temp_dir().join(format!("smeltr-migrate-newer-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("newer.db");
        let _ = std::fs::remove_file(&db);
        let key = temp_key();
        drop(SqliteStore::open(&db, &key).unwrap());

        let conn = Connection::open(&db).unwrap();
        conn.pragma_update(None, "user_version", (STEPS.len() + 1) as i64)
            .unwrap();
        conn.execute("UPDATE min_reader SET steps = ?1", [(STEPS.len() + 1) as i64])
            .unwrap();
        drop(conn);

        match SqliteStore::open(&db, &key) {
            Err(e) => assert!(e.0.contains("newer build"), "{}", e.0),
            Ok(_) => panic!("a database from a newer build was opened"),
        }
    }

    // -- roles ---------------------------------------------------------------

    fn role(name: &str, administrator: bool, areas: &[(&str, Level)]) -> Role {
        Role {
            name: name.into(),
            administrator,
            areas: areas.iter().map(|(a, l)| (a.to_string(), *l)).collect(),
        }
    }

    fn role_event(action: &str, target: &str) -> SecurityEvent {
        crate::leader::accounts::event_for(&crate::leader::store::Actor::system(), action, target, "ok")
    }

    #[test]
    fn roles_read_back_by_name_with_their_areas() {
        let s = store();
        let payments = role(
            "smeltr-payments",
            false,
            &[("payments", Level::Deployer), ("shared", Level::Viewer)],
        );
        let admins = role("smeltr-admins", true, &[]);
        s.put_role(&payments, &role_event("role-put", &payments.name)).unwrap();
        s.put_role(&admins, &role_event("role-put", &admins.name)).unwrap();
        assert_eq!(s.list_roles().unwrap(), vec![admins, payments]);
    }

    #[test]
    fn putting_a_role_again_replaces_its_areas_whole() {
        let s = store();
        s.put_role(
            &role("r", true, &[("payments", Level::Deployer), ("search", Level::Editor)]),
            &role_event("role-put", "r"),
        )
        .unwrap();
        let replaced = role("r", false, &[("search", Level::Viewer)]);
        s.put_role(&replaced, &role_event("role-put", "r")).unwrap();
        assert_eq!(s.list_roles().unwrap(), vec![replaced]);
    }

    #[test]
    fn deleting_a_role_takes_its_areas_and_says_whether_there_was_one() {
        let s = store();
        s.put_role(
            &role("r", false, &[("payments", Level::Editor)]),
            &role_event("role-put", "r"),
        )
        .unwrap();
        assert!(s.delete_role("r", &role_event("role-delete", "r")).unwrap());
        assert!(s.list_roles().unwrap().is_empty());
        let areas: i64 = s
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM role_areas", [], |row| row.get(0))
            .unwrap();
        assert_eq!(areas, 0, "the role's areas outlived it");

        assert!(!s.delete_role("r", &role_event("role-delete", "r")).unwrap());
        assert_eq!(
            s.list_security_events(10).unwrap().len(),
            2,
            "deleting nothing recorded an event"
        );
    }

    #[test]
    fn a_role_change_carries_its_audit_row() {
        let s = store();
        s.put_role(&role("r", false, &[]), &role_event("role-put", "r"))
            .unwrap();
        s.delete_role("r", &role_event("role-delete", "r")).unwrap();
        let events = s.list_security_events(10).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|e| (e.action.as_str(), e.target.as_str()))
                .collect::<Vec<_>>(),
            vec![("role-delete", "r"), ("role-put", "r")]
        );
    }

    #[test]
    fn a_stored_level_this_build_does_not_know_is_an_error() {
        let s = store();
        s.put_role(&role("r", false, &[]), &role_event("role-put", "r"))
            .unwrap();
        s.conn
            .lock()
            .unwrap()
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 INSERT INTO role_areas (role, area, level) VALUES ('r', 'payments', 'owner');
                 PRAGMA ignore_check_constraints = OFF;",
            )
            .unwrap();
        assert!(s.list_roles().is_err());
    }

    /// An OIDC account made before roles held everything by its flag. From the roles step on only its roles say what
    /// it holds, so the step clears the flag, leaves local accounts as they were, and shuts out builds that read it.
    #[test]
    fn the_roles_step_takes_the_flag_from_oidc_accounts_and_is_breaking() {
        assert!(!super::super::steps::is_additive(STEPS[6]));
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..6]).unwrap();
        conn.execute_batch(
            "INSERT INTO users (id, auth_source, display_name, administrator, created_at, updated_at)
             VALUES ('u_local', 'local', 'admin', 1, 0, 0), ('u_oidc', 'oidc', 'someone', 1, 0, 0);",
        )
        .unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..7]).unwrap();
        let flags: Vec<(String, i64)> = conn
            .prepare("SELECT id, administrator FROM users ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(flags, [("u_local".to_string(), 1), ("u_oidc".to_string(), 0)]);
        let min_reader: i64 = conn
            .query_row("SELECT steps FROM min_reader", [], |row| row.get(0))
            .unwrap();
        assert_eq!(min_reader, 7);
    }

    // -- agents -----------------------------------------------------------------

    /// A build without the agent step would read an autonomous agent as local, and so as an administrator.
    #[test]
    fn the_agent_step_is_breaking() {
        assert!(!super::super::steps::is_additive(STEPS[7]));
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..8]).unwrap();
        let min_reader: i64 = conn
            .query_row("SELECT steps FROM min_reader", [], |row| row.get(0))
            .unwrap();
        assert_eq!(min_reader, 8);
    }

    /// A build without conversations never reads them, so rolling back past the step loses only them.
    #[test]
    fn the_conversation_step_is_additive() {
        assert!(super::super::steps::is_additive(STEPS[8]));
    }

    /// Autonomous agents leave no account behind, and a build that still reads them cannot open the file.
    #[test]
    fn the_step_that_removes_autonomous_agents_deletes_their_accounts_and_is_breaking() {
        assert!(!super::super::steps::is_additive(STEPS[9]));
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..9]).unwrap();
        conn.execute_batch(
            "INSERT INTO users (id, auth_source, display_name, administrator, created_at, updated_at)
             VALUES ('u_local', 'local', 'admin', 1, 0, 0), ('u_agent', 'agent', 'Night shift', 0, 0, 0);
             INSERT INTO agent_accounts (user_id, auth_source, roles) VALUES ('u_agent', 'agent', '[]');
             INSERT INTO agent_permissions (user_id, permissions)
             VALUES ('u_local', '{}'), ('u_agent', '{}');",
        )
        .unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", STEPS).unwrap();
        let ids = |sql: &str| -> Vec<String> {
            conn.prepare(sql)
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(ids("SELECT id FROM users"), ["u_local"]);
        assert_eq!(ids("SELECT user_id FROM agent_permissions"), ["u_local"]);
        assert!(conn.prepare("SELECT 1 FROM agent_accounts").is_err());
        let min_reader: i64 = conn
            .query_row("SELECT steps FROM min_reader", [], |row| row.get(0))
            .unwrap();
        assert_eq!(min_reader, 10);
    }

    #[test]
    fn a_conversation_keeps_its_turns_in_order_until_it_ages_out() {
        let s = store();
        let at = |id: &str, updated_at: i64| ConversationRecord {
            id: id.to_string(),
            user_id: "u_admin".to_string(),
            title: id.to_string(),
            created_at: 1,
            updated_at,
        };
        s.create_conversation(&at("c_old", 10)).unwrap();
        s.create_conversation(&at("c_new", 20)).unwrap();
        assert!(s.append_turn("c_old", "first", 30).unwrap());
        assert!(s.append_turn("c_old", "second", 40).unwrap());
        assert!(!s.append_turn("c_gone", "x", 40).unwrap());
        assert_eq!(s.turns_of("c_old").unwrap(), ["first", "second"]);
        let listed: Vec<String> = s
            .conversations_of("u_admin")
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(listed, ["c_old", "c_new"], "the one added to last comes first");

        assert_eq!(s.delete_conversations_untouched_since(25).unwrap(), 1);
        assert!(s.conversation("c_new").unwrap().is_none());
        assert!(s.delete_conversation("c_old").unwrap());
        assert!(s.turns_of("c_old").unwrap().is_empty());
    }

    #[test]
    fn a_persons_agent_permissions_are_the_default_until_they_set_them() {
        let s = store();
        s.bootstrap_administrator(NewLocalUser {
            id: "u_admin",
            username: "admin",
            display_name: "admin",
            password_hash: "x",
            must_change: false,
        })
        .unwrap();
        assert_eq!(s.agent_permissions_of("u_admin").unwrap(), AgentPermissions::default());
        s.put_agent_permissions(
            "u_admin",
            &AgentPermissions::Everything,
            &role_event("agent-permissions-put", "u_admin"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(s.agent_permissions_of("u_admin").unwrap(), AgentPermissions::Everything);
        assert_eq!(
            s.put_agent_permissions(
                "u_none",
                &AgentPermissions::Everything,
                &role_event("agent-permissions-put", "u_none")
            )
            .unwrap(),
            Err(AccountError::NoSuchUser)
        );
    }

    // -- single-worker sources -------------------------------------------------

    /// Older builds can still open a file this step has run on, so rolling back past it costs nothing.
    #[test]
    fn the_single_worker_step_is_additive() {
        assert!(super::super::steps::is_additive(STEPS[2]));
    }

    /// The provider's name for an account is only shown, so a build without it loses nothing it relies on.
    #[test]
    fn the_oidc_login_step_is_additive() {
        assert!(super::super::steps::is_additive(STEPS[5]));
    }

    /// A build without the OIDC step would read an OIDC account as local, so the step shuts such builds out.
    #[test]
    fn the_oidc_step_is_breaking() {
        assert!(!super::super::steps::is_additive(STEPS[4]));
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..4]).unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..5]).unwrap();
        let min_reader: i64 = conn
            .query_row("SELECT steps FROM min_reader", [], |row| row.get(0))
            .unwrap();
        assert_eq!(min_reader, 5);
    }

    /// A file whose ledgers were written before their sizes were kept takes each size from what it holds, and keeps
    /// it from there.
    #[test]
    fn the_ledger_size_step_counts_the_ledgers_already_written() {
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", &STEPS[..3]).unwrap();
        conn.execute_batch(
            "INSERT INTO source_holds (grp, source, place, holder) VALUES ('g', 'logs', 'p', 'w1'), ('g', 'empty', 'p', 'w1');
             INSERT INTO source_ledger (grp, source, place, item, entry) VALUES ('g', 'logs', 'p', 'a', 'v'), ('g', 'logs', 'p', 'b', 'v');",
        )
        .unwrap();
        super::super::steps::upgrade_sqlite(&mut conn, "leader.db", STEPS).unwrap();
        conn.execute("DELETE FROM source_ledger WHERE item = 'a'", []).unwrap();
        conn.execute(
            "INSERT INTO source_ledger (grp, source, place, item, entry) VALUES ('g', 'empty', 'p', 'c', 'v')",
            [],
        )
        .unwrap();
        let sizes: Vec<(String, i64)> = conn
            .prepare("SELECT source, ledger_entries FROM source_holds ORDER BY source")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(sizes, [("empty".to_string(), 1), ("logs".to_string(), 1)]);
    }

    /// A fresh file takes the step, and a hold with its cursor and ledger reads back the same after reopening.
    #[test]
    fn a_hold_and_its_progress_survive_reopening() {
        let dir = std::env::temp_dir().join(format!("smeltr-holds-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("holds.db");
        let _ = std::fs::remove_file(&db);
        let key = temp_key();
        let source = SourceKey {
            source: "logs".to_string(),
            place: "bucket/prefix".to_string(),
        };
        let entry = |item: &str, entry: &str| LedgerEntry {
            item: item.to_string(),
            entry: entry.to_string(),
        };

        let first = SqliteStore::open(&db, &key).unwrap();
        assert_eq!(first.source_hold("g", &source).unwrap(), None);
        first.set_source_holder("g", &source, "w1").unwrap();
        first
            .apply_source_progress(
                "g",
                &source,
                &Progress {
                    cursor: Some("c1".to_string()),
                    added: vec![entry("a", "v1"), entry("b", "v1"), entry("c", "v1")],
                    removed: Vec::new(),
                },
            )
            .unwrap();
        first
            .apply_source_progress(
                "g",
                &source,
                &Progress {
                    cursor: None,
                    added: vec![entry("a", "v2")],
                    removed: vec!["b".to_string(), "never-there".to_string()],
                },
            )
            .unwrap();
        // A new holder keeps what the old one delivered.
        first.set_source_holder("g", &source, "w2").unwrap();
        drop(first);

        let again = SqliteStore::open(&db, &key).unwrap();
        assert_eq!(
            again.source_hold("g", &source).unwrap(),
            Some(SourceHold {
                holder: "w2".to_string(),
                cursor: Some("c1".to_string()),
            })
        );
        assert_eq!(again.ledger_page("g", &source, None, 10).unwrap().len(), 2);
        assert_eq!(again.ledger_page("g", &source, None, 1).unwrap(), [entry("a", "v2")]);
        assert_eq!(
            again.ledger_page("g", &source, Some("a"), 10).unwrap(),
            [entry("c", "v1")]
        );
        // Another group, another place: nothing of this one's.
        assert_eq!(again.source_hold("h", &source).unwrap(), None);
        let moved = SourceKey {
            place: "bucket/other".to_string(),
            ..source.clone()
        };
        assert!(again.ledger_page("g", &moved, None, 1).unwrap().is_empty());
    }

    /// Forgetting a source drops its hold and its ledger, and nothing of the same source in another group or at
    /// another place. Taken again, it starts afresh.
    #[test]
    fn a_forgotten_source_starts_afresh() {
        let store = store();
        let here = SourceKey {
            source: "s".to_string(),
            place: "bucket/a".to_string(),
        };
        let there = SourceKey {
            place: "bucket/b".to_string(),
            ..here.clone()
        };
        let progress = Progress {
            cursor: Some("listed".to_string()),
            added: vec![LedgerEntry {
                item: "x".to_string(),
                entry: "v".to_string(),
            }],
            removed: Vec::new(),
        };
        for (group, key) in [("g", &here), ("g", &there), ("h", &here)] {
            store.set_source_holder(group, key, "w1").unwrap();
            store.apply_source_progress(group, key, &progress).unwrap();
        }

        store.forget_source("g", &here).unwrap();

        assert_eq!(store.source_hold("g", &here).unwrap(), None);
        assert!(store.ledger_page("g", &here, None, 10).unwrap().is_empty());
        let left: Vec<_> = store
            .held_sources()
            .unwrap()
            .into_iter()
            .map(|h| (h.group, h.key.place, h.ledger_entries))
            .collect();
        assert_eq!(
            left,
            [
                ("g".to_string(), "bucket/b".to_string(), 1),
                ("h".to_string(), "bucket/a".to_string(), 1)
            ]
        );

        store.set_source_holder("g", &here, "w2").unwrap();
        assert_eq!(
            store.source_hold("g", &here).unwrap(),
            Some(SourceHold {
                holder: "w2".to_string(),
                cursor: None,
            })
        );
        assert_eq!(store.held_sources().unwrap()[0].ledger_entries, 0);
    }

    /// Every hold reads back with its own ledger's size: a hold with no ledger counts none, and a ledger left at
    /// another place or in another group is not counted toward this one.
    #[test]
    fn held_sources_counts_each_holds_own_ledger() {
        let store = store();
        let key = |source: &str, place: &str| SourceKey {
            source: source.to_string(),
            place: place.to_string(),
        };
        let items = |names: &[&str]| Progress {
            cursor: None,
            added: names
                .iter()
                .map(|item| LedgerEntry {
                    item: item.to_string(),
                    entry: "v1".to_string(),
                })
                .collect(),
            removed: Vec::new(),
        };
        let (logs, moved, cursor) = (key("logs", "s3://b/new"), key("logs", "s3://b/old"), key("tables", "t"));
        store.set_source_holder("g", &logs, "w1").unwrap();
        store.apply_source_progress("g", &logs, &items(&["a", "b"])).unwrap();
        store.set_source_holder("g", &moved, "w0").unwrap();
        store
            .apply_source_progress("g", &moved, &items(&["a", "b", "c"]))
            .unwrap();
        store.set_source_holder("g", &cursor, "w2").unwrap();
        store.set_source_holder("h", &logs, "w3").unwrap();
        store.apply_source_progress("h", &logs, &items(&["z"])).unwrap();

        let held = |group: &str, key: &SourceKey, holder: &str, ledger_entries: u64| HeldSource {
            group: group.to_string(),
            key: key.clone(),
            holder: holder.to_string(),
            ledger_entries,
        };
        assert_eq!(
            store.held_sources().unwrap(),
            vec![
                held("g", &logs, "w1", 2),
                held("g", &moved, "w0", 3),
                held("g", &cursor, "w2", 0),
                held("h", &logs, "w3", 1),
            ]
        );

        // An item read again keeps one entry; an item gone leaves the count.
        store.apply_source_progress("g", &logs, &items(&["a"])).unwrap();
        let gone = Progress {
            cursor: None,
            added: Vec::new(),
            removed: vec!["b".to_string(), "never-held".to_string()],
        };
        store.apply_source_progress("g", &logs, &gone).unwrap();
        assert_eq!(store.held_sources().unwrap()[0], held("g", &logs, "w1", 1));
    }

    // -- keeping the file to its owner -------------------------------------

    /// The database holds password verifiers, so a copy of it is offline-attackable. On a host with the usual
    /// umask SQLite would create it mode `0644`.
    #[cfg(unix)]
    #[test]
    fn the_database_and_its_sidecars_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("smeltr-private-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("leader.db");
        let _ = std::fs::remove_file(&db);
        let store = SqliteStore::open(&db, &temp_key()).unwrap();
        // A write, so the WAL exists to be checked.
        store
            .create_enrollment_code(&a_code("j1", "deadbeef", 1, i64::MAX))
            .unwrap();
        make_private(&db).unwrap();

        for suffix in ["", "-wal"] {
            let mut name = db.as_os_str().to_os_string();
            name.push(suffix);
            let path = std::path::PathBuf::from(name);
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "{} is mode {:03o}", path.display(), mode & 0o777);
        }
        let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o077, 0, "the directory is mode {:03o}", dir_mode & 0o777);
    }

    /// An upgraded install keeps whatever mode it already had, so opening one has to narrow it rather than trust
    /// it.
    #[cfg(unix)]
    #[test]
    fn opening_an_existing_permissive_database_tightens_it() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("smeltr-private-upgrade-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("leader.db");
        let _ = std::fs::remove_file(&db);
        drop(SqliteStore::open(&db, &temp_key()).unwrap());

        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o644)).unwrap();
        drop(SqliteStore::open(&db, &temp_key()).unwrap());

        let mode = std::fs::metadata(&db).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "still mode {:03o} after reopening", mode & 0o777);
    }

    /// A mode stricter than this asks for is the operator's choice and is left alone.
    #[cfg(unix)]
    #[test]
    fn a_stricter_mode_is_left_as_it_was() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("smeltr-private-strict-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("leader.db");
        let _ = std::fs::remove_file(&db);
        drop(SqliteStore::open(&db, &temp_key()).unwrap());

        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o400)).unwrap();
        make_private(&db).unwrap();
        let mode = std::fs::metadata(&db).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o400, "a read-only database was widened");
    }
}
