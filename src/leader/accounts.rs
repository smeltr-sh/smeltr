//! Local accounts: what a username and a password are, how a password is verified, and the operations that create
//! and recover an administrator. [`super::store`] holds them; this decides what is allowed to be written.
//!
//! Two rules shape everything here. A password is verified with a memory-hard function, never the SHA-256 that
//! hashes a worker token -- a token is 256 random bits and a password is not. And an account's authentication
//! source is fixed at creation: these operations name local accounts only, so a future OIDC identity cannot
//! acquire a password by any path but a step that says so (`steps`).

use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};

use super::auth;
use super::store::{AccountError, Actor, LocalCredentialRecord, NewLocalUser, SecurityEvent, Store, StoreError};

/// The account bootstrap creates and recovery restores. Fixed, so recovery always has a target and never has to be
/// told which account to reach for.
pub const BOOTSTRAP_USERNAME: &str = "admin";

/// The shortest password accepted. Length is the only composition rule: OWASP's guidance is that a minimum length
/// with no character-class requirement produces stronger passwords than the reverse, because the rules are what
/// push people toward `Password1!`.
pub const MIN_PASSWORD_LEN: usize = 15;

/// The longest, in bytes rather than characters, because the cost being bounded is the hash's input. Long enough
/// for any passphrase and any password manager's output; refused rather than truncated, so a password is never
/// silently a prefix of itself.
pub const MAX_PASSWORD_BYTES: usize = 1024;

/// Argon2id parameters. At or above the OWASP floor of 19 MiB with one lane, with the memory raised because a
/// leader is a server rather than a phone.
///
/// They are recorded in every hash written, so changing them here leaves existing passwords verifiable and
/// [`Verified::needs_rehash`] reports the ones worth rewriting at their next successful login.
const MEMORY_KIB: u32 = 64 * 1024;
const ITERATIONS: u32 = 3;
const LANES: u32 = 1;

/// Why a password was refused before it was ever hashed.
#[derive(Debug, PartialEq, Eq)]
pub enum PasswordRefused {
    TooShort,
    TooLong,
}

impl std::fmt::Display for PasswordRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PasswordRefused::TooShort => write!(f, "a password must be at least {MIN_PASSWORD_LEN} characters"),
            PasswordRefused::TooLong => write!(f, "a password must be at most {MAX_PASSWORD_BYTES} bytes"),
        }
    }
}

impl std::error::Error for PasswordRefused {}

/// What a stored hash says about the password that just matched it.
pub struct Verified {
    /// The hash was written with weaker parameters than [`hash_password`] uses now. The caller holds the only
    /// plaintext there will be until the next login, so this is the moment to rewrite it.
    pub needs_rehash: bool,
}

fn argon2() -> Argon2<'static> {
    let params = Params::new(MEMORY_KIB, ITERATIONS, LANES, None).expect("the Argon2 parameters above are valid");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// Bounds a password before it costs anything to process. Callers check this before taking a hashing permit, so an
/// oversized body cannot spend one.
pub fn check_password(password: &str) -> Result<(), PasswordRefused> {
    // Characters for the floor, bytes for the ceiling: the first is about how much a person typed, the second
    // about how much work the hash is asked to do.
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(PasswordRefused::TooShort);
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(PasswordRefused::TooLong);
    }
    Ok(())
}

/// The PHC-format salt length the specification recommends, and what the crate's own salt generation uses.
const SALT_BYTES: usize = 16;

/// A PHC string carrying the salt and the parameters it was written with.
///
/// The salt comes from [`auth::random_bytes`] rather than the hashing crate's own generator, so every
/// unpredictable value in the leader has one source.
///
/// Expensive by construction -- tens of milliseconds and [`MEMORY_KIB`] of memory. Never call it on a thread that
/// serves requests or while holding the store's mutex.
pub fn hash_password(password: &str) -> Result<String, StoreError> {
    let salt = auth::random_bytes(SALT_BYTES);
    argon2()
        .hash_password_with_salt(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| StoreError::failed(format!("hashing a password: {e}")))
}

/// Whether `password` matches `stored`. `Ok(None)` is a clean mismatch; an error is a hash this build cannot read,
/// which is not the same thing and must not be reported to the caller as a wrong password.
///
/// As expensive as [`hash_password`], and under the same rule about where it may run.
pub fn verify_password(password: &str, stored: &str) -> Result<Option<Verified>, StoreError> {
    let parsed =
        PasswordHash::new(stored).map_err(|e| StoreError::failed(format!("reading a stored password hash: {e}")))?;
    match argon2().verify_password(password.as_bytes(), &parsed) {
        Ok(()) => Ok(Some(Verified {
            needs_rehash: weaker_than_current(&parsed),
        })),
        // The one error that means what the caller asked about. Everything else -- an algorithm, version or
        // parameter set this build cannot compute -- is the store being unreadable, not the password being wrong,
        // and reporting it as a mismatch would leave an operator locked out with nothing to act on.
        Err(argon2::password_hash::Error::PasswordInvalid) => Ok(None),
        Err(e) => Err(StoreError::failed(format!(
            "verifying against a stored password hash: {e}"
        ))),
    }
}

/// Whether a hash was written with parameters below what [`hash_password`] writes now. A hash whose parameters
/// cannot be read at all is left alone: rewriting one this build does not understand would be guessing.
fn weaker_than_current(parsed: &PasswordHash) -> bool {
    let Ok(params) = Params::try_from(parsed) else {
        return false;
    };
    params.m_cost() < MEMORY_KIB || params.t_cost() < ITERATIONS
}

// -- generated passwords ----------------------------------------------------

/// The alphabet a generated password is drawn from: Crockford base32 without `I`, `L`, `O` and `U`, so nothing in
/// one can be misread off a terminal or misheard down a phone.
const ALPHABET: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Four groups of five, hyphenated. Twenty characters of a 32-symbol alphabet is 100 bits -- far past what the
/// login throttle would ever let anyone work through, and still short enough to read aloud.
///
/// Hyphens are separators rather than password material, and they count toward [`MIN_PASSWORD_LEN`] like any
/// other character, which is why the generated form is comfortably longer than the floor.
pub fn generate_password() -> String {
    let raw = auth::random_bytes(20);
    let mut out = String::with_capacity(23);
    for (i, byte) in raw.iter().enumerate() {
        if i > 0 && i % 5 == 0 {
            out.push('-');
        }
        out.push(ALPHABET[usize::from(*byte) % ALPHABET.len()] as char);
    }
    out
}

// -- usernames --------------------------------------------------------------

/// The canonical form of `raw`, or `None` if it is not a username.
///
/// ASCII, lowercase, and bounded: a login name is an identifier rather than a display name, and two accounts whose
/// names differ only by case or by a Unicode form that renders identically are two accounts an operator cannot
/// tell apart. What a person is called is [`super::store::UserRecord::display_name`], which takes anything.
pub fn canonical_username(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let ok = (1..=64).contains(&trimmed.len())
        && trimmed.starts_with(|c: char| c.is_ascii_alphabetic())
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    ok.then(|| trimmed.to_ascii_lowercase())
}

/// An opaque account id. Never a username: a display name changes and a name is only unique among the names, so
/// what a preference, an audit row and a git author address are keyed by has to be neither.
pub fn generate_user_id() -> String {
    format!("u_{}", auth::generate_secret().split_at(24).0)
}

// -- operations -------------------------------------------------------------

/// What [`bootstrap_if_empty`] and [`recover_administrator`] hand back: the password an operator has to be shown
/// once, and never again.
///
/// It is deliberately not `Debug`: the whole point of the type is that its contents reach one `eprintln!` and
/// nothing else.
pub struct OneTimePassword {
    pub username: String,
    pub password: String,
}

/// Create the first administrator if no account exists at all.
///
/// `Ok(None)` means there was already an account, which is the ordinary case on every start after the first.
/// Blocking, and expensive: it hashes a password. Runs before the leader serves anything.
pub fn bootstrap_if_empty(store: &dyn Store) -> Result<Option<OneTimePassword>, StoreError> {
    if store.any_user_exists()? {
        return Ok(None);
    }
    let password = generate_password();
    let hash = hash_password(&password)?;
    let id = generate_user_id();
    let created = store.bootstrap_administrator(NewLocalUser {
        id: &id,
        username: BOOTSTRAP_USERNAME,
        display_name: "Administrator",
        password_hash: &hash,
        must_change: true,
    })?;
    // Lost a race with another process bootstrapping the same file. Its administrator is as good as this one
    // would have been, and printing a password that was never written would be worse than printing nothing.
    if !created {
        return Ok(None);
    }
    record(store, &Actor::host(), "bootstrap", &id, "ok");
    Ok(Some(OneTimePassword {
        username: BOOTSTRAP_USERNAME.to_string(),
        password,
    }))
}

/// Give [`BOOTSTRAP_USERNAME`] a fresh must-change password, re-enable it, and refuse every session it had.
///
/// The escape hatch for an operator locked out of their own leader: a forgotten password, an administrator
/// disabled by another, a revocation that left nobody. It runs from the host at startup rather than over the
/// network, so holding the data directory is the whole of its authorization.
pub fn recover_administrator(store: &dyn Store) -> Result<Result<OneTimePassword, AccountError>, StoreError> {
    let password = generate_password();
    let hash = hash_password(&password)?;
    match store.recover_local_user(BOOTSTRAP_USERNAME, &hash)? {
        Err(e) => Ok(Err(e)),
        Ok(user_id) => {
            record(store, &Actor::host(), "recover", &user_id, "ok");
            Ok(Ok(OneTimePassword {
                username: BOOTSTRAP_USERNAME.to_string(),
                password,
            }))
        }
    }
}

// -- authenticating ---------------------------------------------------------

/// A hash of a password nobody has, verified against when the username is unknown or the account cannot sign in.
///
/// Without it, a login for a name that does not exist answers in microseconds and one for a name that does takes
/// as long as Argon2 does, and the difference is a way to enumerate accounts. Written with the same parameters as
/// a real one, because it is the parameters that decide how long the comparison takes.
static DUMMY_HASH: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    hash_password(&generate_password()).expect("hashing a generated password with valid parameters")
});

/// What a login attempt came to.
pub enum Authenticated {
    /// The password matched an account that may sign in. `rehash` carries a hash written with the current work
    /// factor when the stored one was weaker -- the caller holds the only plaintext there will be until the next
    /// login, so this is the moment to write it.
    Ok {
        user: super::store::UserRecord,
        must_change: bool,
        rehash: Option<String>,
    },
    /// An unknown username, a disabled account, or a wrong password. One answer for all three: which of them it
    /// was is exactly what an attacker is asking.
    Refused,
}

/// Verify `password` against `username`.
///
/// Blocking and expensive -- it hashes. The caller runs it on a blocking thread, under a permit that bounds how
/// many run at once, and never while holding a lock.
///
/// The account is read twice on purpose. The first read finds a verifier and the generation it belongs to, in one
/// statement; the second, after the hash, decides whether this account may still sign in. A disable or a reset
/// landing during the tens of milliseconds the hash takes therefore wins, where a single read taken before the
/// hash would have issued a session against a decision that had already been made.
///
/// The generation is what makes that cover a reset as well as a disable. Every path that supersedes a password
/// moves it, so a generation that changed under the hash means the verifier just checked is no longer the account's
/// -- and the old password must not be admitted on the strength of it.
pub fn authenticate(store: &dyn Store, username: &str, password: &str) -> Result<Authenticated, StoreError> {
    let Some(canonical) = canonical_username(username) else {
        // Not a username at all, so no account can hold it. Still hashed, so the answer takes as long as a real
        // one: refusing early here would say "that name could never exist" in the timing.
        let _ = verify_password(password, &DUMMY_HASH);
        return Ok(Authenticated::Refused);
    };

    let credential = store.local_credential_by_username(&canonical)?;
    let Some(credential) = credential else {
        let _ = verify_password(password, &DUMMY_HASH);
        return Ok(Authenticated::Refused);
    };
    judge(store, credential, password)
}

/// Sign-in after its first read: verify `password` against `credential`, then read the account again and decide
/// whether it may still sign in. The hash runs here, so a disable, revoke or reset racing sign-in lands here too.
fn judge(store: &dyn Store, credential: LocalCredentialRecord, password: &str) -> Result<Authenticated, StoreError> {
    // A hash this build cannot read is not a wrong password: reporting it as one would leave an operator locked
    // out with nothing in the log to say why.
    let verified = match verify_password(password, &credential.password_hash) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                target: "leader.password_hash_unreadable",
                subject = %credential.user_id,
                error = %e,
                "a stored password hash could not be read, so its account cannot sign in"
            );
            return Ok(Authenticated::Refused);
        }
    };
    let Some(verified) = verified else {
        return Ok(Authenticated::Refused);
    };

    // The second read. Everything decided here is as of now, not as of before the hash.
    let Some(user) = store.get_user(&credential.user_id)? else {
        return Ok(Authenticated::Refused);
    };
    if !user.enabled {
        return Ok(Authenticated::Refused);
    }
    // A reset that landed while this was hashing leaves the verifier just checked superseded. Refusing is the
    // fail-closed answer: an operator whose own login raced an unrelated revocation signs in again and succeeds.
    if credential.generation != user.generation {
        return Ok(Authenticated::Refused);
    }

    let rehash = verified.needs_rehash.then(|| hash_password(password)).transpose()?;
    Ok(Authenticated::Ok {
        user,
        must_change: credential.must_change,
        rehash,
    })
}

/// What a password change did. One answer, so a caller says the right thing without inferring which of two
/// meanings a `false` carried.
#[derive(Debug, PartialEq, Eq)]
pub enum PasswordChange {
    /// Chosen. Every session the account held stops being admitted, including the one that asked.
    Changed,
    /// The current password offered does not match the account.
    WrongPassword,
    /// The account moved on while this was hashing: reset, revoked, disabled or gone. Nothing was written.
    Superseded,
    /// The new password is not one this leader accepts.
    Refused(PasswordRefused),
}

/// Replace the password of an account that is choosing its own, after checking the one it is replacing.
///
/// The write is conditional on the generation the check passed against, so a reset landing while the new
/// password hashes wins rather than being overwritten by it.
pub fn change_own_password(
    store: &dyn Store,
    user_id: &str,
    username: &str,
    current: &str,
    new: &str,
) -> Result<PasswordChange, StoreError> {
    if let Err(e) = check_password(new) {
        return Ok(PasswordChange::Refused(e));
    }
    // Only the caller's own verifier is ever checked. The session names the account; a username naming another
    // one, or none, is refused before a hash runs against it.
    let credential = match canonical_username(username) {
        Some(name) => store.local_credential_by_username(&name)?,
        None => None,
    };
    let Some(credential) = credential.filter(|c| c.user_id == user_id) else {
        return Ok(PasswordChange::WrongPassword);
    };
    let (actor, generation) = match judge(store, credential, current)? {
        Authenticated::Refused => return Ok(PasswordChange::WrongPassword),
        Authenticated::Ok { user, .. } => (Actor::person(&user.id, &user.display_name), user.generation),
    };
    let hash = hash_password(new)?;
    // Bumps the generation, so every session this account holds -- including the one asking -- stops being
    // admitted. Choosing a password signs you out of everywhere, which is what makes it a remedy for a password
    // somebody else knows. The record lands in the same transaction rather than after it.
    let event = event_for(&actor, "password-change", user_id, "ok");
    written(store.set_local_password(user_id, &hash, false, Some(generation), Some(&event))?)
}

/// How a conditional credential write reads as an outcome. A `Superseded` and an account that went while this
/// hashed are the same fact: what authorized the write no longer holds.
fn written(result: Result<i64, AccountError>) -> Result<PasswordChange, StoreError> {
    match result {
        Ok(_) => Ok(PasswordChange::Changed),
        Err(AccountError::Superseded | AccountError::NoSuchUser) => Ok(PasswordChange::Superseded),
        Err(e) => Err(StoreError::failed(e.to_string())),
    }
}

/// Replace the password of an account that holds one it did not choose. Nothing is verified: the session
/// asking was issued by presenting that password. The account must still stand at `generation`, judged in the
/// transaction that writes, so a reset or a disable landing while this hashed wins.
pub fn choose_first_password(
    store: &dyn Store,
    user_id: &str,
    generation: i64,
    new: &str,
) -> Result<PasswordChange, StoreError> {
    if let Err(e) = check_password(new) {
        return Ok(PasswordChange::Refused(e));
    }
    let Some(user) = store.get_user(user_id)? else {
        return Ok(PasswordChange::Superseded);
    };
    let actor = Actor::person(&user.id, &user.display_name);
    let hash = hash_password(new)?;
    let event = event_for(&actor, "password-change", user_id, "ok");
    written(store.set_local_password(user_id, &hash, false, Some(generation), Some(&event))?)
}

/// One audit row, for a caller to hand to the store operation it belongs to.
pub fn event_for(actor: &Actor, action: &str, target: &str, outcome: &str) -> SecurityEvent {
    SecurityEvent {
        id: auth::generate_secret().split_at(24).0.to_string(),
        at: super::warnings::now_millis(),
        actor: actor.id.clone(),
        action: action.to_string(),
        target: target.to_string(),
        outcome: outcome.to_string(),
    }
}

/// Write one audit row on its own, for something with no durable mutation to ride along with: a sign-in, a
/// sign-out, a refused attempt. A mutation records itself in the transaction that makes it.
pub fn record(store: &dyn Store, actor: &Actor, action: &str, target: &str, outcome: &str) {
    let event = event_for(actor, action, target, outcome);
    if let Err(e) = store.record_security_event(&event) {
        tracing::error!(
            target: "leader.security_event_lost",
            action, outcome, error = %e,
            "a security event could not be recorded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cheap parameters. The tests here are about what the code decides, not about how long Argon2 takes, and the
    /// real cost is what makes a suite that hashes dozens of times unusable.
    fn cheap() -> Argon2<'static> {
        Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::new(8, 1, 1, None).unwrap())
    }

    fn cheap_hash(password: &str) -> String {
        cheap()
            .hash_password_with_salt(password.as_bytes(), &auth::random_bytes(SALT_BYTES))
            .unwrap()
            .to_string()
    }

    /// Two accounts choosing one password must not produce one hash: a stolen database would otherwise say which
    /// of them to attack once.
    #[test]
    fn the_same_password_hashes_differently_every_time() {
        assert_ne!(hash_password(GOOD).unwrap(), hash_password(GOOD).unwrap());
    }

    #[test]
    fn a_hash_written_with_weaker_parameters_asks_to_be_rewritten() {
        let weak = cheap_hash("correct horse battery staple");
        assert!(weaker_than_current(&PasswordHash::new(&weak).unwrap()));
    }

    #[test]
    fn a_password_is_bounded_at_both_ends() {
        assert_eq!(
            check_password(&"a".repeat(MIN_PASSWORD_LEN - 1)),
            Err(PasswordRefused::TooShort)
        );
        assert_eq!(check_password(&"a".repeat(MIN_PASSWORD_LEN)), Ok(()));
        assert_eq!(check_password(&"a".repeat(MAX_PASSWORD_BYTES)), Ok(()));
        assert_eq!(
            check_password(&"a".repeat(MAX_PASSWORD_BYTES + 1)),
            Err(PasswordRefused::TooLong)
        );
    }

    /// The floor counts characters and the ceiling counts bytes, so a short passphrase in a non-Latin script is
    /// not refused for being long in UTF-8.
    #[test]
    fn a_multibyte_password_is_measured_the_way_it_was_typed() {
        let phrase = "\u{3053}".repeat(MIN_PASSWORD_LEN);
        assert!(phrase.len() > MIN_PASSWORD_LEN);
        assert_eq!(check_password(&phrase), Ok(()));
    }

    #[test]
    fn a_generated_password_clears_the_floor_and_never_repeats() {
        let (a, b) = (generate_password(), generate_password());
        assert_eq!(check_password(&a), Ok(()));
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c == '-' || ALPHABET.contains(&(c as u8))), "{a}");
    }

    #[test]
    fn a_username_is_canonicalized_once() {
        assert_eq!(canonical_username("  Alice  ").as_deref(), Some("alice"));
        assert_eq!(canonical_username("a.b-c_1").as_deref(), Some("a.b-c_1"));
    }

    #[test]
    fn a_name_that_is_not_an_identifier_is_refused() {
        for bad in [
            "",
            " ",
            "1alice",
            "-alice",
            ".alice",
            "al ice",
            "al/ice",
            "\u{e9}lodie",
            &"a".repeat(65),
        ] {
            assert_eq!(canonical_username(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn an_id_is_not_a_name() {
        let (a, b) = (generate_user_id(), generate_user_id());
        assert_ne!(a, b);
        assert!(a.starts_with("u_"));
    }

    // -- authenticating ----------------------------------------------------

    fn store_with_admin(password: &str) -> super::super::sqlite_store::SqliteStore {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-auth-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store =
            super::super::sqlite_store::SqliteStore::open(std::path::Path::new(":memory:"), &dir.join("k")).unwrap();
        let hash = hash_password(password).unwrap();
        store
            .bootstrap_administrator(super::super::store::NewLocalUser {
                id: "u_1",
                username: "admin",
                display_name: "Administrator",
                password_hash: &hash,
                must_change: false,
            })
            .unwrap();
        store
    }

    const GOOD: &str = "correct horse battery staple";

    #[test]
    fn the_right_password_signs_the_account_in() {
        let store = store_with_admin(GOOD);
        match authenticate(&store, "admin", GOOD).unwrap() {
            Authenticated::Ok { user, .. } => assert_eq!(user.id, "u_1"),
            Authenticated::Refused => panic!("the right password was refused"),
        }
    }

    /// The name is canonicalized once, on the way in, so the spelling an operator types does not decide whether
    /// they can sign in.
    #[test]
    fn the_username_is_matched_in_its_canonical_form() {
        let store = store_with_admin(GOOD);
        for spelling in ["admin", "ADMIN", "  Admin  "] {
            assert!(
                matches!(authenticate(&store, spelling, GOOD).unwrap(), Authenticated::Ok { .. }),
                "{spelling:?}"
            );
        }
    }

    /// An unknown name, a name that could never exist, and a wrong password are one answer. Which of the three it
    /// was is exactly what an attacker is asking.
    #[test]
    fn an_unknown_account_and_a_wrong_password_answer_the_same_way() {
        let store = store_with_admin(GOOD);
        for (name, password) in [
            ("nobody", GOOD),
            ("not a username!", GOOD),
            ("admin", "wrong wrong wrong"),
        ] {
            assert!(
                matches!(authenticate(&store, name, password).unwrap(), Authenticated::Refused),
                "{name:?}"
            );
        }
    }

    /// The race the second read exists for: a disable landing while the hash runs must win, or a session is
    /// issued against a decision that had already been made.
    #[test]
    fn an_account_disabled_during_the_hash_cannot_sign_in() {
        let store = store_with_admin(GOOD);
        store
            .create_local_user(
                super::super::store::NewLocalUser {
                    id: "u_2",
                    username: "other",
                    display_name: "Other",
                    password_hash: &hash_password(GOOD).unwrap(),
                    must_change: false,
                },
                super::super::store::GivenAccess {
                    roles: &[],
                    administrator: true,
                },
                &event_for(&Actor::system(), "account-create", "", "ok"),
            )
            .unwrap()
            .unwrap();
        // Standing in for the concurrent disable: what matters is that it lands between the verifier being read
        // and the account being judged, and the second read is what makes the order come out this way.
        store
            .set_user_enabled("u_1", false, &event_for(&Actor::system(), "account-disable", "", "ok"))
            .unwrap()
            .unwrap();
        assert!(matches!(
            authenticate(&store, "admin", GOOD).unwrap(),
            Authenticated::Refused
        ));
    }

    /// A password reset replaces the verifier, so the old password stops working at once rather than at the next
    /// sweep.
    #[test]
    fn a_reset_password_stops_the_old_one_working() {
        let store = store_with_admin(GOOD);
        let fresh = hash_password("a different passphrase entirely").unwrap();
        store
            .set_local_password("u_1", &fresh, true, None, None)
            .unwrap()
            .unwrap();
        assert!(matches!(
            authenticate(&store, "admin", GOOD).unwrap(),
            Authenticated::Refused
        ));
        match authenticate(&store, "admin", "a different passphrase entirely").unwrap() {
            Authenticated::Ok { must_change, .. } => assert!(must_change, "a password nobody chose"),
            Authenticated::Refused => panic!("the new password was refused"),
        }
    }

    /// Nothing to rewrite when the stored hash already carries the current parameters.
    #[test]
    fn a_current_hash_is_not_rehashed_on_the_way_in() {
        let store = store_with_admin(GOOD);
        match authenticate(&store, "admin", GOOD).unwrap() {
            Authenticated::Ok { rehash, .. } => assert!(rehash.is_none()),
            Authenticated::Refused => panic!("refused"),
        }
    }

    #[test]
    fn changing_a_password_needs_the_one_it_replaces() {
        let store = store_with_admin(GOOD);
        assert_eq!(
            change_own_password(&store, "u_1", "admin", "not the current one", "a brand new passphrase").unwrap(),
            PasswordChange::WrongPassword
        );
        assert_eq!(
            change_own_password(&store, "u_1", "admin", GOOD, "a brand new passphrase").unwrap(),
            PasswordChange::Changed
        );
        assert!(matches!(
            authenticate(&store, "admin", "a brand new passphrase").unwrap(),
            Authenticated::Ok { must_change: false, .. }
        ));
    }

    /// The session names the account, so a username naming another one is refused before its verifier is
    /// checked: a signed-in person is never a way to test somebody else's password, however the answer reads.
    #[test]
    fn a_password_change_checks_only_the_callers_own_verifier() {
        let store = store_with_admin(GOOD);
        store
            .create_local_user(
                super::super::store::NewLocalUser {
                    id: "u_2",
                    username: "other",
                    display_name: "Other",
                    password_hash: &hash_password("the other account's passphrase").unwrap(),
                    must_change: false,
                },
                super::super::store::GivenAccess {
                    roles: &[],
                    administrator: true,
                },
                &event_for(&Actor::system(), "account-create", "u_2", "ok"),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            change_own_password(
                &store,
                "u_1",
                "other",
                "the other account's passphrase",
                "a brand new passphrase"
            )
            .unwrap(),
            PasswordChange::WrongPassword
        );
        assert_eq!(
            change_own_password(&store, "u_1", "nobody", GOOD, "a brand new passphrase").unwrap(),
            PasswordChange::WrongPassword
        );
        assert!(matches!(
            authenticate(&store, "other", "the other account's passphrase").unwrap(),
            Authenticated::Ok { .. }
        ));
    }

    /// Bounded before anything is hashed, so an oversized body costs a length check rather than 64 MiB of work.
    #[test]
    fn a_new_password_is_bounded_before_it_is_hashed() {
        let store = store_with_admin(GOOD);
        assert_eq!(
            change_own_password(&store, "u_1", "admin", GOOD, "short").unwrap(),
            PasswordChange::Refused(PasswordRefused::TooShort)
        );
    }

    /// The verifier and the generation a session would quote for it are one moment of the account. Read apart, a
    /// reset landing between them verifies the password it replaced against the generation that replaced it.
    #[test]
    fn a_credential_carries_the_generation_it_was_read_with() {
        let store = store_with_admin(GOOD);
        let before = store.local_credential_by_username("admin").unwrap().unwrap();
        assert_eq!(
            before.generation,
            store.get_user("u_1").unwrap().unwrap().generation,
            "the hash and the account disagree about when this is"
        );

        let fresh = hash_password("a brand new passphrase").unwrap();
        store
            .set_local_password("u_1", &fresh, true, None, None)
            .unwrap()
            .unwrap();
        let after = store.local_credential_by_username("admin").unwrap().unwrap();
        assert!(
            after.generation > before.generation,
            "a reset left the hash reading as before"
        );
    }

    /// A required change hashes for a while. A reset or a disable that lands meanwhile has moved the generation,
    /// and the write judges that in its own transaction, so the stale session cannot install what it hashed.
    #[test]
    fn a_required_change_loses_to_whatever_moved_the_account_meanwhile() {
        let store = store_with_admin(GOOD);
        let before = store.get_user("u_1").unwrap().unwrap().generation;
        let event = event_for(&Actor::person("u_2", "Someone"), "session-revoke", "u_1", "ok");
        store.revoke_sessions("u_1", &event).unwrap().unwrap();
        assert_eq!(
            choose_first_password(&store, "u_1", before, "a brand new passphrase").unwrap(),
            PasswordChange::Superseded
        );
        assert!(matches!(
            authenticate(&store, "admin", GOOD).unwrap(),
            Authenticated::Ok { .. }
        ));
        let now = store.get_user("u_1").unwrap().unwrap().generation;
        assert_eq!(
            choose_first_password(&store, "u_1", now, "a brand new passphrase").unwrap(),
            PasswordChange::Changed
        );
    }

    /// Choosing a password signs you out of everywhere, which is what makes it a remedy for one somebody else
    /// knows. The generation is what every session is then judged against.
    #[test]
    fn changing_a_password_moves_the_generation() {
        let store = store_with_admin(GOOD);
        let before = store.get_user("u_1").unwrap().unwrap().generation;
        change_own_password(&store, "u_1", "admin", GOOD, "a brand new passphrase").unwrap();
        assert!(store.get_user("u_1").unwrap().unwrap().generation > before);
    }

    /// A hash this build cannot compute is the store being unreadable, not the password being wrong. Reporting it
    /// as a mismatch would leave an operator locked out with nothing in the log to act on.
    #[test]
    fn a_hash_this_build_cannot_read_is_an_error_rather_than_a_mismatch() {
        // A well-formed PHC string naming an algorithm this build does not have.
        let foreign = "$scrypt$ln=16,r=8,p=1$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYQ";
        assert!(
            verify_password("correct horse battery staple", foreign).is_err(),
            "an unreadable hash was reported as a wrong password"
        );
    }

    /// And an ordinary wrong password still is one.
    #[test]
    fn a_wrong_password_against_a_readable_hash_is_a_mismatch() {
        let hash = cheap_hash("correct horse battery staple");
        assert!(verify_password("something else entirely", &hash).unwrap().is_none());
    }

    /// The race the generation check exists for. A reset landing while the hash runs supersedes the verifier that
    /// was just checked, so the old password must not buy a session on the strength of it.
    ///
    /// `judge` is sign-in after its first read. What a test does between that read and `judge` lands where the hash
    /// runs.
    #[test]
    fn a_password_reset_during_the_hash_refuses_the_old_password() {
        let store = store_with_admin(GOOD);
        let credential = store.local_credential_by_username("admin").unwrap().unwrap();
        let fresh = cheap_hash("a different passphrase entirely");
        store
            .set_local_password("u_1", &fresh, true, None, None)
            .unwrap()
            .unwrap();
        assert!(matches!(
            judge(&store, credential, GOOD).unwrap(),
            Authenticated::Refused
        ));
    }

    /// A revoke or a disable landing while the hash runs refuses the attempt too. Fail-closed: once nothing moves
    /// under it, the same password signs in after a revoke.
    #[test]
    fn a_revoke_or_a_disable_during_the_hash_refuses_the_attempt() {
        let revoked = store_with_admin(GOOD);
        let credential = revoked.local_credential_by_username("admin").unwrap().unwrap();
        let event = event_for(&Actor::system(), "revoke-sessions", "u_1", "ok");
        revoked.revoke_sessions("u_1", &event).unwrap().unwrap();
        assert!(matches!(
            judge(&revoked, credential, GOOD).unwrap(),
            Authenticated::Refused
        ));
        assert!(
            matches!(authenticate(&revoked, "admin", GOOD).unwrap(), Authenticated::Ok { .. }),
            "a revoke before the first read refuses nothing"
        );

        let disabled = store_with_admin(GOOD);
        // The last enabled administrator cannot be disabled, so there is a second.
        let other = NewLocalUser {
            id: "u_2",
            username: "other",
            display_name: "Other",
            password_hash: &cheap_hash(GOOD),
            must_change: false,
        };
        let event = event_for(&Actor::system(), "account-create", "u_2", "ok");
        disabled
            .create_local_user(
                other,
                super::super::store::GivenAccess {
                    roles: &[],
                    administrator: true,
                },
                &event,
            )
            .unwrap()
            .unwrap();
        let credential = disabled.local_credential_by_username("admin").unwrap().unwrap();
        let event = event_for(&Actor::system(), "account-disable", "u_1", "ok");
        disabled.set_user_enabled("u_1", false, &event).unwrap().unwrap();
        assert!(matches!(
            judge(&disabled, credential, GOOD).unwrap(),
            Authenticated::Refused
        ));
    }
}
