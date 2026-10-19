//! Browser sessions: what a cookie stands for, and how long it stands for it.
//!
//! In memory and never written down. A leader restart signs everyone out, which is the cost of not having a
//! session table to keep consistent with the accounts one; accounts, passwords and history survive it.
//!
//! A session is found by a hash of its token, never by the token. What the process holds is therefore not a working
//! credential: a core dump or a `Debug` of this map hands out nothing.
//!
//! The exception is a session signed in through OIDC. It carries the provider's refresh token, so the leader can
//! refresh it every few minutes and a group change or a refused grant at the provider reaches smeltr. That token
//! is a working credential at the provider, and the process holds it in memory for as long as the session lives:
//! that is the cost. It stays on this module's own entry, is never copied into the [`Session`] a request handler
//! sees, and no `Debug` here prints it. It is never written down, so a restart drops it with the session.
//!
//! Two deadlines, both monotonic so a clock adjustment cannot extend one. Idle expiry ends a session nobody is
//! using; absolute expiry ends one somebody is, and always wins. Times for an operator to read are wall-clock and
//! live in the audit, not here.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::auth;

/// How long a session survives with nothing but automatic traffic on it, until an operator says otherwise.
pub const DEFAULT_IDLE: Duration = Duration::from_secs(30 * 60);

/// How long a session survives at all, however busy, until an operator says otherwise. Re-authenticating once a
/// working day is the point.
pub const DEFAULT_ABSOLUTE: Duration = Duration::from_secs(12 * 60 * 60);

/// The shortest either deadline may be set to.
///
/// Above [`ACTIVITY_GRANULARITY`] rather than equal to it: an idle deadline of exactly one granularity can never
/// be pushed back, because the first report that would move it arrives at the instant expiry is checked, and
/// expiry is checked first. A setting that cannot be extended by working is not an idle timeout, it is a session
/// length -- so the floor is the smallest value activity can actually hold open.
pub const MIN_LIFETIME: Duration = Duration::from_secs(120);

/// The longest a session may live. "Never expires" is not a session, so there is a ceiling rather than an option
/// to remove one.
pub const MAX_LIFETIME: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The two deadlines, as an operator has set them.
///
/// A pair rather than two settings, because they constrain each other: an idle timeout longer than the absolute
/// one can never be reached, so it is not a configuration with a surprising result but a nonsensical one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lifetimes {
    pub idle: Duration,
    pub absolute: Duration,
}

impl Default for Lifetimes {
    fn default() -> Lifetimes {
        Lifetimes {
            idle: DEFAULT_IDLE,
            absolute: DEFAULT_ABSOLUTE,
        }
    }
}

impl Lifetimes {
    /// Checked, with the sentence to answer a caller with. Both bounds and their relation are enforced here so
    /// there is one place they hold, whether the values came from an operator, an environment variable or a row
    /// written by an older build.
    pub fn new(idle: Duration, absolute: Duration) -> Result<Lifetimes, String> {
        for (what, value) in [("idle", idle), ("absolute", absolute)] {
            if value < MIN_LIFETIME {
                return Err(format!(
                    "the {what} session timeout is {}s; the shortest allowed is {}s",
                    value.as_secs(),
                    MIN_LIFETIME.as_secs()
                ));
            }
            if value > MAX_LIFETIME {
                return Err(format!(
                    "the {what} session timeout is {}s; the longest allowed is {}s -- a session that does not end \
                     is not a session",
                    value.as_secs(),
                    MAX_LIFETIME.as_secs()
                ));
            }
        }
        if idle > absolute {
            return Err(format!(
                "the idle session timeout ({}s) is longer than the absolute one ({}s), so it could never be \
                 reached",
                idle.as_secs(),
                absolute.as_secs()
            ));
        }
        Ok(Lifetimes { idle, absolute })
    }

    /// The key each deadline is stored under. Seconds, because a unit in the value would be a second thing to
    /// parse and a second thing to get wrong; the form converts.
    pub const IDLE_KEY: &'static str = "session.idle_seconds";
    pub const ABSOLUTE_KEY: &'static str = "session.absolute_seconds";

    /// What the store holds, falling back to the defaults for whatever it does not.
    ///
    /// A stored pair that no longer validates -- written by an older build, or edited by hand -- is reported
    /// rather than silently replaced: a leader running deadlines nobody configured is worse than one that says
    /// what it found.
    pub fn read(store: &dyn super::store::Store) -> Result<Lifetimes, String> {
        let seconds = |key: &str| -> Result<Option<Duration>, String> {
            match store.setting(key).map_err(|e| e.to_string())? {
                None => Ok(None),
                Some(raw) => raw
                    .parse::<u64>()
                    .map(|s| Some(Duration::from_secs(s)))
                    .map_err(|_| format!("{key} holds {raw:?}, which is not a number of seconds")),
            }
        };
        let default = Lifetimes::default();
        Lifetimes::new(
            seconds(Lifetimes::IDLE_KEY)?.unwrap_or(default.idle),
            seconds(Lifetimes::ABSOLUTE_KEY)?.unwrap_or(default.absolute),
        )
    }

    /// Both deadlines and the row saying who set them, as one transaction.
    pub fn write(
        &self,
        store: &dyn super::store::Store,
        event: Option<&super::store::SecurityEvent>,
    ) -> Result<(), String> {
        store
            .put_settings(
                &[
                    (Lifetimes::IDLE_KEY, self.idle.as_secs().to_string()),
                    (Lifetimes::ABSOLUTE_KEY, self.absolute.as_secs().to_string()),
                ],
                event,
            )
            .map_err(|e| e.to_string())
    }
}

/// The most often an activity report moves the idle deadline. The GUI sends one per interaction; this is what
/// stops a person holding a key down from spending a request per keystroke.
pub const ACTIVITY_GRANULARITY: Duration = Duration::from_secs(60);

// The floor has to stay above the granularity, and the two are declared apart, so the relationship is asserted
// where changing either one would break it rather than left to be noticed.
const _: () = assert!(MIN_LIFETIME.as_secs() > ACTIVITY_GRANULARITY.as_secs());

/// Sessions one account may hold at once. A person has a laptop, a phone and a spare tab; the sixth sign-in ends
/// the oldest, which is theirs to notice rather than an attacker's to exploit.
pub const MAX_PER_USER: usize = 5;

/// Sessions the process holds at once, across everyone. A ceiling on what an authenticated flood can allocate.
pub const MAX_TOTAL: usize = 10_000;

/// What the cookie is worth, resolved. Copied out under the lock so no caller holds it across a store call.
#[derive(Clone, Debug)]
pub struct Session {
    pub user_id: String,
    /// The account generation this session was issued against. A request is admitted only while the account still
    /// reports it, which is what makes a password change, a reset and a revocation take effect on the next
    /// request rather than at the next sweep.
    pub generation: i64,
    /// The header value an unsafe request must echo. Session-bound and held in the GUI's memory, never in a
    /// cookie: a cookie would ride along with the forgery it is meant to stop.
    pub csrf: String,
    /// The account holds a password it did not choose. Such a session may change that password, read its own
    /// status and sign out, and nothing else.
    pub must_change: bool,
}

/// What refreshes an OIDC session at its provider.
#[derive(Clone)]
pub struct Grant {
    pub refresh_token: String,
    /// The nonce the session's ID token was issued with. A refreshed ID token that carries one must carry this.
    pub nonce: String,
    /// The provider that issued the grant. A grant from a provider no longer configured is not refreshed.
    pub issuer: String,
}

// Written out so a log line or a panic message carrying a grant does not carry the credential in it.
impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("refresh_token", &"<redacted>")
            .field("nonce", &self.nonce)
            .field("issuer", &self.issuer)
            .finish()
    }
}

/// Names one session to this module without being able to sign in as it: the digest of its token, which is
/// what the map is keyed by.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey(String);

/// A grant whose refresh is due, handed out to the one caller that will make it. Until that caller answers with
/// [`Sessions::renewed`], [`Sessions::retry_later`], [`Sessions::drop_grant`] or [`Sessions::end`], the grant
/// is not handed out again.
#[derive(Debug)]
pub struct Due {
    pub key: SessionKey,
    pub user_id: String,
    pub grant: Grant,
}

/// A grant as this module keeps it.
struct Held {
    grant: Grant,
    refreshed: Instant,
    in_flight: bool,
}

/// A session as this module keeps it.
struct Entry {
    session: Session,
    issued: Instant,
    last_activity: Instant,
    grant: Option<Held>,
}

impl Entry {
    fn expired(&self, now: Instant, lifetimes: Lifetimes) -> bool {
        now.duration_since(self.issued) >= lifetimes.absolute
            || now.duration_since(self.last_activity) >= lifetimes.idle
    }
}

/// Why a token did not resolve. The caller answers `401` either way; the distinction is for the audit and for the
/// GUI, which says "signed out" rather than "sign in" when a session merely ran out.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    Unknown,
    Expired,
}

/// Every live session, by the digest of its token.
#[derive(Default)]
pub struct Sessions(Mutex<State>);

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    lifetimes: Lifetimes,
}

/// A newly issued session: the token that goes in the cookie, and what the GUI needs to use it.
pub struct Issued {
    pub token: String,
    pub session: Session,
}

impl Sessions {
    /// Issue a session for `user_id`. Takes `now` rather than reading the clock, so expiry is testable without
    /// waiting out a deadline.
    ///
    /// `None` when the process is at [`MAX_TOTAL`] after clearing what has expired: refusing a new session is a
    /// worse answer than issuing one, and a better answer than evicting somebody else's to make room.
    pub fn issue(&self, user_id: &str, generation: i64, must_change: bool, now: Instant) -> Option<Issued> {
        let token = auth::generate_secret();
        let session = Session {
            user_id: user_id.to_string(),
            generation,
            csrf: auth::generate_secret(),
            must_change,
        };
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        held.entries.retain(|_, entry| !entry.expired(now, lifetimes));

        // This account's own oldest, before the global ceiling: a person signing in on a sixth device is not the
        // reason the process is full, and taking their own oldest is what keeps them under the limit.
        let mut mine: Vec<(String, Instant)> = held
            .entries
            .iter()
            .filter(|(_, e)| e.session.user_id == user_id)
            .map(|(digest, e)| (digest.clone(), e.issued))
            .collect();
        if mine.len() >= MAX_PER_USER {
            mine.sort_by_key(|(_, issued)| *issued);
            for (digest, _) in mine.iter().take(mine.len() - MAX_PER_USER + 1) {
                held.entries.remove(digest);
            }
        }
        if held.entries.len() >= MAX_TOTAL {
            return None;
        }

        held.entries.insert(
            auth::hash_secret(&token),
            Entry {
                session: session.clone(),
                issued: now,
                last_activity: now,
                grant: None,
            },
        );
        Some(Issued { token, session })
    }

    /// What `token` stands for, if it still stands for anything.
    ///
    /// Reading only: the account behind it is checked by the caller, against the store, outside this lock. A
    /// session that resolves here is not yet a principal.
    pub fn resolve(&self, token: &str, now: Instant) -> Result<Session, Refused> {
        let digest = auth::hash_secret(token);
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        match held.entries.get(&digest) {
            None => Err(Refused::Unknown),
            Some(entry) if entry.expired(now, lifetimes) => {
                held.entries.remove(&digest);
                Err(Refused::Expired)
            }
            Some(entry) => Ok(entry.session.clone()),
        }
    }

    /// Move the idle deadline, at most once per [`ACTIVITY_GRANULARITY`]. Absolute expiry is untouched, so this
    /// can never extend a session past it.
    ///
    /// Answers whether the session is still live, so a caller need not resolve it first.
    pub fn touch(&self, token: &str, now: Instant) -> bool {
        let digest = auth::hash_secret(token);
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        let Some(entry) = held.entries.get_mut(&digest) else {
            return false;
        };
        if entry.expired(now, lifetimes) {
            held.entries.remove(&digest);
            return false;
        }
        if now.duration_since(entry.last_activity) >= ACTIVITY_GRANULARITY {
            entry.last_activity = now;
        }
        true
    }

    /// End one session. Whether there was one to end: an ordinary sign-out on an already-expired cookie is not an
    /// error to report.
    pub fn remove(&self, token: &str) -> bool {
        let digest = auth::hash_secret(token);
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .remove(&digest)
            .is_some()
    }

    /// End every session one account holds, and answer how many.
    ///
    /// Belt to the account generation's braces: the generation is what makes a revocation hold against a session
    /// this process never saw, and this is what frees the memory of the ones it did.
    pub fn remove_all_for(&self, user_id: &str) -> usize {
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let before = held.entries.len();
        held.entries.retain(|_, entry| entry.session.user_id != user_id);
        before - held.entries.len()
    }

    /// Drop what has expired. Called on a schedule so a leader nobody visits does not hold a session per sign-in
    /// since it started.
    pub fn sweep(&self, now: Instant) -> usize {
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        let before = held.entries.len();
        held.entries.retain(|_, entry| !entry.expired(now, lifetimes));
        before - held.entries.len()
    }

    pub fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).entries.len()
    }

    /// The deadlines in force.
    pub fn lifetimes(&self) -> Lifetimes {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).lifetimes
    }

    /// Replace them. Live sessions are measured against the new values on their next request, so shortening a
    /// timeout ends the sessions it should already have ended rather than waiting for the old deadline.
    pub fn set_lifetimes(&self, lifetimes: Lifetimes) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).lifetimes = lifetimes;
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attach the grant that refreshes the session `token` names. Its clock starts at `now`, so the first refresh
    /// comes one interval after sign-in. False when there is no live session to attach it to.
    pub fn give_grant(&self, token: &str, grant: Grant, now: Instant) -> bool {
        let digest = auth::hash_secret(token);
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        let Some(entry) = held.entries.get_mut(&digest) else {
            return false;
        };
        if entry.expired(now, lifetimes) {
            held.entries.remove(&digest);
            return false;
        }
        entry.grant = Some(Held {
            grant,
            refreshed: now,
            in_flight: false,
        });
        true
    }

    /// Every grant on a live session last refreshed at least `every` ago, marked in flight as it is handed out so
    /// an overlapping round does not refresh it a second time.
    pub fn due_grants(&self, now: Instant, every: Duration) -> Vec<Due> {
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let lifetimes = held.lifetimes;
        let mut due = Vec::new();
        for (digest, entry) in held.entries.iter_mut() {
            if entry.expired(now, lifetimes) {
                continue;
            }
            let Some(g) = entry.grant.as_mut() else {
                continue;
            };
            if g.in_flight || now.duration_since(g.refreshed) < every {
                continue;
            }
            g.in_flight = true;
            due.push(Due {
                key: SessionKey(digest.clone()),
                user_id: entry.session.user_id.clone(),
                grant: g.grant.clone(),
            });
        }
        due
    }

    /// The provider accepted the grant. The clock restarts at `now`; a provider that rotates refresh tokens hands
    /// back the one to use next time.
    pub fn renewed(&self, key: &SessionKey, refresh_token: Option<String>, now: Instant) {
        self.with_grant(key, |g| {
            g.in_flight = false;
            g.refreshed = now;
            if let Some(token) = refresh_token {
                g.grant.refresh_token = token;
            }
        });
    }

    /// The provider could not be reached, which says nothing about the grant. It comes due again one interval
    /// from `now`.
    pub fn retry_later(&self, key: &SessionKey, now: Instant) {
        self.with_grant(key, |g| {
            g.in_flight = false;
            g.refreshed = now;
        });
    }

    /// Stop refreshing this session and leave it signed in, for when the provider's settings changed under it.
    pub fn drop_grant(&self, key: &SessionKey) {
        if let Some(entry) = self.0.lock().unwrap_or_else(|e| e.into_inner()).entries.get_mut(&key.0) {
            entry.grant = None;
        }
    }

    /// End the session `key` names, for when the provider refused its grant. Whether there was one to end.
    pub fn end(&self, key: &SessionKey) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .remove(&key.0)
            .is_some()
    }

    fn with_grant(&self, key: &SessionKey, f: impl FnOnce(&mut Held)) {
        let mut held = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(g) = held.entries.get_mut(&key.0).and_then(|e| e.grant.as_mut()) {
            f(g);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issued(sessions: &Sessions, user: &str, now: Instant) -> String {
        sessions.issue(user, 1, false, now).expect("room for a session").token
    }

    #[test]
    fn a_session_resolves_to_the_account_it_was_issued_for() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        assert_eq!(s.resolve(&token, now).unwrap().user_id, "u_1");
    }

    /// What the process holds is not a working credential. The map is keyed by the hash the leader stores every
    /// secret under, and anyone reading it -- a core dump, a `Debug` -- finds the token nowhere in it.
    #[test]
    fn the_token_is_not_what_is_stored() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        let held = s.0.lock().unwrap();
        assert_eq!(held.entries.len(), 1);
        assert!(
            held.entries.contains_key(&auth::hash_secret(&token)),
            "the key is not the token's hash"
        );
        for (key, entry) in &held.entries {
            assert!(!key.contains(&token), "the map is keyed by the token");
            assert!(
                !format!("{:?}", entry.session).contains(&token),
                "the session carries the token"
            );
        }
    }

    #[test]
    fn two_sessions_never_share_a_token_or_a_csrf_value() {
        let s = Sessions::default();
        let now = Instant::now();
        let a = s.issue("u_1", 1, false, now).unwrap();
        let b = s.issue("u_1", 1, false, now).unwrap();
        assert_ne!(a.token, b.token);
        assert_ne!(a.session.csrf, b.session.csrf);
    }

    #[test]
    fn an_unknown_token_resolves_to_nothing() {
        let s = Sessions::default();
        assert_eq!(s.resolve("nonsense", Instant::now()).unwrap_err(), Refused::Unknown);
    }

    #[test]
    fn a_session_idle_past_its_deadline_is_gone() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        assert!(s.resolve(&token, now + DEFAULT_IDLE - Duration::from_secs(1)).is_ok());
        assert_eq!(s.resolve(&token, now + DEFAULT_IDLE).unwrap_err(), Refused::Expired);
    }

    /// Automatic traffic must not keep a session alive: the fleet view polls every two seconds, so a tab left open
    /// would otherwise never idle out.
    #[test]
    fn resolving_does_not_move_the_idle_deadline() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        for minute in 0..29 {
            assert!(s.resolve(&token, now + Duration::from_secs(minute * 60)).is_ok());
        }
        assert_eq!(s.resolve(&token, now + DEFAULT_IDLE).unwrap_err(), Refused::Expired);
    }

    #[test]
    fn activity_moves_the_idle_deadline() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        let later = now + Duration::from_secs(20 * 60);
        assert!(s.touch(&token, later));
        assert!(s.resolve(&token, later + DEFAULT_IDLE - Duration::from_secs(1)).is_ok());
    }

    /// The one that always wins. A session somebody is using all day still ends.
    #[test]
    fn absolute_expiry_beats_any_amount_of_activity() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = issued(&s, "u_1", now);
        // Busy right up to the deadline, so idle expiry never gets a chance to be the reason.
        let mut at = now;
        while at + Duration::from_secs(60) < now + DEFAULT_ABSOLUTE {
            at += Duration::from_secs(60);
            assert!(s.touch(&token, at), "still live at {:?}", at - now);
        }
        assert_eq!(s.resolve(&token, now + DEFAULT_ABSOLUTE).unwrap_err(), Refused::Expired);
    }

    #[test]
    fn signing_out_ends_one_session_and_leaves_the_others() {
        let s = Sessions::default();
        let now = Instant::now();
        let laptop = issued(&s, "u_1", now);
        let phone = issued(&s, "u_1", now);
        assert!(s.remove(&laptop));
        assert!(!s.remove(&laptop), "there was nothing left to end");
        assert!(s.resolve(&phone, now).is_ok());
    }

    #[test]
    fn revoking_ends_every_session_that_account_holds_and_nobody_elses() {
        let s = Sessions::default();
        let now = Instant::now();
        let mine = [issued(&s, "u_1", now), issued(&s, "u_1", now)];
        let theirs = issued(&s, "u_2", now);
        assert_eq!(s.remove_all_for("u_1"), 2);
        for token in mine {
            assert_eq!(s.resolve(&token, now).unwrap_err(), Refused::Unknown);
        }
        assert!(s.resolve(&theirs, now).is_ok());
    }

    /// The sixth sign-in takes this account's own oldest, so one person cannot spend the process's whole budget.
    #[test]
    fn an_account_holds_no_more_than_its_share() {
        let s = Sessions::default();
        let now = Instant::now();
        let first = issued(&s, "u_1", now);
        for i in 1..MAX_PER_USER {
            issued(&s, "u_1", now + Duration::from_secs(i as u64));
        }
        assert_eq!(s.len(), MAX_PER_USER);

        issued(&s, "u_1", now + Duration::from_secs(99));
        assert_eq!(s.len(), MAX_PER_USER, "still at the limit");
        assert_eq!(s.resolve(&first, now).unwrap_err(), Refused::Unknown, "the oldest went");
    }

    #[test]
    fn an_expired_session_is_swept_rather_than_held_forever() {
        let s = Sessions::default();
        let now = Instant::now();
        issued(&s, "u_1", now);
        assert_eq!(s.sweep(now), 0);
        assert_eq!(s.sweep(now + DEFAULT_ABSOLUTE), 1);
        assert!(s.is_empty());
    }

    /// Issuing sweeps first, so a process full of expired sessions admits a new one rather than refusing it.
    #[test]
    fn expired_sessions_do_not_keep_a_new_one_out() {
        let s = Sessions::default();
        let now = Instant::now();
        issued(&s, "u_1", now);
        assert!(s.issue("u_2", 1, false, now + DEFAULT_ABSOLUTE).is_some());
        assert_eq!(s.len(), 1, "the expired one went as the new one arrived");
    }

    const EVERY: Duration = Duration::from_secs(5 * 60);

    fn grant(refresh_token: &str) -> Grant {
        Grant {
            refresh_token: refresh_token.to_string(),
            nonce: "n_1".to_string(),
            issuer: "https://idp.example".to_string(),
        }
    }

    /// A session signed in through OIDC, with its grant given at `now`.
    fn signed_in(sessions: &Sessions, user: &str, now: Instant) -> String {
        let token = issued(sessions, user, now);
        assert!(sessions.give_grant(&token, grant("rt_1"), now));
        token
    }

    #[test]
    fn a_grant_comes_due_one_interval_after_it_was_given() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        assert!(s.due_grants(now + EVERY - Duration::from_secs(1), EVERY).is_empty());
        let due = s.due_grants(now + EVERY, EVERY);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].user_id, "u_1");
        assert_eq!(due[0].grant.refresh_token, "rt_1");
    }

    #[test]
    fn a_grant_needs_a_live_session_to_attach_to() {
        let s = Sessions::default();
        let now = Instant::now();
        assert!(!s.give_grant("nonsense", grant("rt_1"), now));
        let token = issued(&s, "u_1", now);
        assert!(!s.give_grant(&token, grant("rt_1"), now + DEFAULT_ABSOLUTE));
    }

    /// Two rounds overlapping must not spend one refresh token twice: a provider that rotates them would take
    /// the second use for a replay.
    #[test]
    fn a_grant_in_flight_is_not_handed_out_again() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        assert_eq!(s.due_grants(now + EVERY, EVERY).len(), 1);
        assert!(s.due_grants(now + EVERY * 3, EVERY).is_empty());
    }

    #[test]
    fn a_renewal_restarts_the_clock_and_rotates_the_token() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        let key = s.due_grants(now + EVERY, EVERY).remove(0).key;

        let renewed_at = now + EVERY + Duration::from_secs(10);
        s.renewed(&key, Some("rt_2".to_string()), renewed_at);
        assert!(
            s.due_grants(renewed_at + EVERY - Duration::from_secs(1), EVERY)
                .is_empty()
        );
        let due = s.due_grants(renewed_at + EVERY, EVERY);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].grant.refresh_token, "rt_2");

        // A provider that does not rotate leaves the token as it was.
        s.renewed(&key, None, renewed_at + EVERY);
        assert_eq!(
            s.due_grants(renewed_at + EVERY * 2, EVERY)[0].grant.refresh_token,
            "rt_2"
        );
    }

    #[test]
    fn an_unreachable_provider_is_tried_again_one_interval_later() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        let key = s.due_grants(now + EVERY, EVERY).remove(0).key;

        s.retry_later(&key, now + EVERY);
        assert!(s.due_grants(now + EVERY * 2 - Duration::from_secs(1), EVERY).is_empty());
        assert_eq!(s.due_grants(now + EVERY * 2, EVERY).len(), 1);
    }

    #[test]
    fn a_dropped_grant_is_never_due_and_the_session_stays() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = signed_in(&s, "u_1", now);
        let key = s.due_grants(now + EVERY, EVERY).remove(0).key;

        s.drop_grant(&key);
        assert!(s.due_grants(now + EVERY * 3, EVERY).is_empty());
        assert!(s.resolve(&token, now + EVERY * 3).is_ok());
    }

    #[test]
    fn a_refused_grant_ends_its_session_and_nobody_elses() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = signed_in(&s, "u_1", now);
        let other = issued(&s, "u_1", now);
        let key = s.due_grants(now + EVERY, EVERY).remove(0).key;

        assert!(s.end(&key));
        assert!(!s.end(&key), "there was nothing left to end");
        assert_eq!(s.resolve(&token, now + EVERY).unwrap_err(), Refused::Unknown);
        assert!(s.resolve(&other, now + EVERY).is_ok());
    }

    /// Refreshing is not activity: a session nobody uses expires on schedule, and its grant with it.
    #[test]
    fn an_expired_sessions_grant_is_never_due() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        assert!(s.due_grants(now + DEFAULT_IDLE, EVERY).is_empty());
    }

    #[test]
    fn a_grant_does_not_print_its_refresh_token() {
        let s = Sessions::default();
        let now = Instant::now();
        signed_in(&s, "u_1", now);
        let due = s.due_grants(now + EVERY, EVERY).remove(0);
        assert!(!format!("{:?}", due.grant).contains("rt_1"));
        assert!(!format!("{due:?}").contains("rt_1"));
        assert!(format!("{due:?}").contains("u_1"), "the rest is still there to read");
    }
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;

    /// An idle deadline past the absolute one can never be reached, so it is not a surprising configuration but a
    /// meaningless one.
    #[test]
    fn an_idle_deadline_cannot_outlast_the_absolute_one() {
        let e = Lifetimes::new(Duration::from_secs(7200), Duration::from_secs(3600)).unwrap_err();
        assert!(e.contains("could never be reached"), "{e}");
        assert!(Lifetimes::new(Duration::from_secs(3600), Duration::from_secs(3600)).is_ok());
    }

    #[test]
    fn a_session_that_does_not_end_is_refused() {
        let e = Lifetimes::new(MIN_LIFETIME, MAX_LIFETIME + Duration::from_secs(1)).unwrap_err();
        assert!(e.contains("longest allowed"), "{e}");
        let e = Lifetimes::new(Duration::from_secs(0), DEFAULT_ABSOLUTE).unwrap_err();
        assert!(e.contains("shortest allowed"), "{e}");
    }

    /// The deadlines are read when a request arrives, not when the session was issued, so shortening one ends the
    /// sessions it should already have ended rather than waiting out the old value.
    #[test]
    fn shortening_the_idle_deadline_expires_a_session_already_past_it() {
        let s = Sessions::default();
        let now = Instant::now();
        let token = s.issue("u_1", 1, false, now).unwrap().token;
        let twenty_minutes = now + Duration::from_secs(20 * 60);
        assert!(s.resolve(&token, twenty_minutes).is_ok(), "still inside the default");

        s.set_lifetimes(Lifetimes::new(Duration::from_secs(10 * 60), DEFAULT_ABSOLUTE).unwrap());
        assert_eq!(
            s.resolve(&token, twenty_minutes).unwrap_err(),
            Refused::Expired,
            "a session past the new deadline outlived it"
        );
    }
}

#[cfg(test)]
mod floor_tests {
    use super::*;

    /// The shortest idle timeout an operator may choose has to be one that working can hold open. At exactly one
    /// granularity it cannot be: the report that would move the deadline arrives at the instant expiry is
    /// checked, and expiry is checked first, so a session doing something every second still dies at the minute.
    #[test]
    fn a_session_at_the_shortest_idle_timeout_can_be_held_open() {
        let s = Sessions::default();
        s.set_lifetimes(Lifetimes::new(MIN_LIFETIME, DEFAULT_ABSOLUTE).unwrap());

        let start = Instant::now();
        let token = s.issue("u_1", 1, false, start).unwrap().token;

        // Somebody working: a report every granularity, which is as often as one is counted.
        let mut now = start;
        for _ in 0..5 {
            now += ACTIVITY_GRANULARITY;
            assert!(s.touch(&token, now), "the session died while it was being used");
        }
        assert!(s.resolve(&token, now).is_ok(), "activity did not hold it open");
    }
}
