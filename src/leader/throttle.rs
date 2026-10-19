//! What bounds the rate passwords may be guessed at.
//!
//! Two buckets, because the two attacks have different shapes: many passwords against one account, and one
//! password against many accounts. A per-username bucket stops the first; a per-address bucket stops the second,
//! which a per-username one never sees.
//!
//! Both are bounded in what they remember. A key is whatever the request carried, including usernames that do not
//! exist, so an attacker choosing a fresh one each time must not be able to make this grow without limit. At
//! capacity a key nobody has seen recently is refused rather than admitted: admitting it is what an attacker who
//! filled the table was paying for.
//!
//! A refused attempt still counts. Otherwise the window could be spent and then waited out at the cost of only
//! the refusals.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The window both buckets count over.
pub const WINDOW: Duration = Duration::from_secs(60);

/// Attempts per minute against one username. Enough for a person mistyping a password manager's paste; far too
/// few to work through anything.
pub const PER_USERNAME: usize = 5;

/// Attempts per minute from one address. Higher, because an office behind one NAT address is several people.
pub const PER_ADDRESS: usize = 30;

/// Current-password checks per minute by one already-signed-in account: a password change names the password it
/// replaces.
///
/// Its own bucket rather than the sign-in one, because sharing it would let anyone holding a session spend that
/// account's sign-in allowance, locking the person out of signing in at all. Still bounded, because a browser
/// somebody walked away from could otherwise guess the password at the full rate the hash permits.
pub const PER_PASSWORD_CHECK: usize = 10;

/// Keys either bucket remembers at once. Reached only under attack: it is one entry per distinct username or
/// address tried inside a minute.
pub const MAX_KEYS: usize = 4096;

/// The longest key either bucket stores, in bytes. A login keys on whatever username was sent, valid or not, so
/// this bounds what an unauthenticated caller can make the table hold.
const MAX_KEY_LEN: usize = 128;

/// `key`, no longer than [`MAX_KEY_LEN`] bytes.
///
/// Cut at a character boundary, because a key is arbitrary text: 127 ASCII bytes followed by one two-byte
/// character puts byte 128 inside it, and slicing there panics rather than answering the request.
fn bounded(key: &str) -> &str {
    if key.len() <= MAX_KEY_LEN {
        return key;
    }
    let cut = (0..=MAX_KEY_LEN).rev().find(|i| key.is_char_boundary(*i)).unwrap_or(0);
    &key[..cut]
}

/// A sliding window per key, bounded in keys and in what each key remembers.
pub struct Bucket {
    limit: usize,
    attempts: Mutex<HashMap<String, Vec<Instant>>>,
}

impl Bucket {
    pub fn new(limit: usize) -> Bucket {
        Bucket {
            limit,
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `key` may try again, counting the attempt either way.
    pub fn allow(&self, key: &str, now: Instant) -> bool {
        let key = bounded(key);
        let fresh = |t: &Instant| now.duration_since(*t) < WINDOW;
        let mut attempts = self.attempts.lock().unwrap_or_else(|e| e.into_inner());

        // A key whose attempts have all aged out goes with them: what the table holds is who is trying now.
        attempts.retain(|_, recent| recent.iter().any(fresh));

        if attempts.len() >= MAX_KEYS && !attempts.contains_key(key) {
            // Full of keys that are all still inside their window, so this is a flood of distinct keys. Refusing
            // the new one holds the line; evicting an active throttle to admit it is what the flood is for.
            return false;
        }

        let recent = attempts.entry(key.to_string()).or_default();
        recent.retain(fresh);
        // Bounded per key as well: a key at its limit gains nothing from remembering attempts past it, and this
        // is what stops one key's vector growing with the flood.
        let allowed = recent.len() < self.limit;
        if recent.len() <= self.limit {
            recent.push(now);
        }
        allowed
    }

    /// How long until `key` may try again. Zero when it may now, which is what a caller reports as a retry
    /// interval rather than as a refusal.
    pub fn retry_after(&self, key: &str, now: Instant) -> Duration {
        let key = bounded(key);
        let attempts = self.attempts.lock().unwrap_or_else(|e| e.into_inner());
        let Some(recent) = attempts.get(key) else {
            return Duration::ZERO;
        };
        let mut fresh: Vec<Instant> = recent
            .iter()
            .copied()
            .filter(|t| now.duration_since(*t) < WINDOW)
            .collect();
        if fresh.len() < self.limit {
            return Duration::ZERO;
        }
        fresh.sort();
        // The oldest attempt still counting is the one whose expiry frees a slot.
        WINDOW - now.duration_since(fresh[fresh.len() - self.limit])
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }
}

/// Every bucket password verification is bounded by.
pub struct LoginThrottle {
    pub username: Bucket,
    pub address: Bucket,
    /// Keyed by account id: a password change checking the current password, which is verification but not a
    /// sign-in.
    pub password_check: Bucket,
}

impl Default for LoginThrottle {
    fn default() -> LoginThrottle {
        LoginThrottle {
            username: Bucket::new(PER_USERNAME),
            address: Bucket::new(PER_ADDRESS),
            password_check: Bucket::new(PER_PASSWORD_CHECK),
        }
    }
}

impl LoginThrottle {
    /// Whether this attempt may proceed to a password verification, counting it against both buckets.
    ///
    /// Both are consulted, never short-circuited: an attempt refused by one still counts against the other, so
    /// spreading guesses across usernames does not buy an unlimited number of them from one address.
    ///
    /// `address` is `None` where the socket's peer is not the client: behind a proxy terminating TLS, every
    /// browser shares the proxy's address, so an address bucket there is one bucket for everyone, which a
    /// stranger fills with thirty bad guesses a minute to lock every operator out of signing in. Without it,
    /// each account is still bounded by its own name.
    pub fn allow(&self, username: &str, address: Option<&str>, now: Instant) -> Result<(), Duration> {
        let by_username = self.username.allow(username, now);
        let by_address = address.is_none_or(|address| self.address.allow(address, now));
        if by_username && by_address {
            return Ok(());
        }
        let wait = self
            .username
            .retry_after(username, now)
            .max(address.map_or(Duration::ZERO, |address| self.address.retry_after(address, now)));
        // Never zero: a caller told to retry immediately would.
        Err(wait.max(Duration::from_secs(1)))
    }

    /// Whether `user_id` may have their current password checked again.
    ///
    /// Touches neither of the sign-in buckets, so a password change cannot use up somebody's ability to sign in.
    pub fn allow_password_check(&self, user_id: &str, now: Instant) -> Result<(), Duration> {
        if self.password_check.allow(user_id, now) {
            return Ok(());
        }
        Err(self
            .password_check
            .retry_after(user_id, now)
            .max(Duration::from_secs(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_gets_its_limit_and_then_is_refused() {
        let b = Bucket::new(3);
        let now = Instant::now();
        for i in 0..3 {
            assert!(b.allow("alice", now), "attempt {i}");
        }
        assert!(!b.allow("alice", now));
    }

    /// Otherwise the window is spent, then waited out at the cost of only the refusals.
    #[test]
    fn a_refused_attempt_still_counts() {
        let b = Bucket::new(1);
        let now = Instant::now();
        assert!(b.allow("alice", now));
        assert!(!b.allow("alice", now + Duration::from_secs(30)));
        // The second attempt pushed the window along, so the first one expiring does not free a slot.
        assert!(!b.allow("alice", now + Duration::from_secs(61)));
    }

    /// Sliding, not a window that resets on the hour: the oldest attempt expiring is what frees one slot, rather
    /// than every attempt expiring together.
    ///
    /// No probe in the middle, because a refused attempt counts and would take the slot this is watching for.
    #[test]
    fn the_window_slides_rather_than_resetting() {
        let b = Bucket::new(2);
        let now = Instant::now();
        assert!(b.allow("alice", now));
        assert!(b.allow("alice", now + Duration::from_secs(30)));
        assert!(
            b.allow("alice", now + WINDOW + Duration::from_secs(1)),
            "the first attempt aged out and freed its slot"
        );
        assert!(
            !b.allow("alice", now + WINDOW + Duration::from_secs(2)),
            "the second has not"
        );
    }

    #[test]
    fn one_key_does_not_throttle_another() {
        let b = Bucket::new(1);
        let now = Instant::now();
        assert!(b.allow("alice", now));
        assert!(b.allow("bob", now));
    }

    /// Keys that filled the table give their room back once their attempts age out. Otherwise, once `MAX_KEYS`
    /// usernames had been tried over the life of the process, every new one would be refused until a restart.
    #[test]
    fn a_full_table_lets_a_new_key_in_once_its_keys_age_out() {
        let b = Bucket::new(1);
        let now = Instant::now();
        for i in 0..MAX_KEYS {
            b.allow(&format!("key-{i}"), now);
        }
        assert!(!b.allow("one-more", now), "the table is full");
        assert!(b.allow("one-more", now + WINDOW + Duration::from_secs(1)));
    }

    /// At capacity the new key is refused. Evicting an active throttle to make room is exactly what a flood of
    /// invented usernames would be paying for.
    #[test]
    fn a_flood_of_new_keys_is_refused_rather_than_evicting_an_active_throttle() {
        let b = Bucket::new(1);
        let now = Instant::now();
        for i in 0..MAX_KEYS {
            b.allow(&format!("key-{i}"), now);
        }
        assert_eq!(b.tracked(), MAX_KEYS);
        assert!(!b.allow("one-more", now));
        assert_eq!(b.tracked(), MAX_KEYS, "nothing was evicted to admit it");
    }

    /// A username arrives from an unauthenticated request and is whatever was sent. A key that could be megabytes
    /// would be a way to spend the leader's memory a request at a time.
    #[test]
    fn an_over_long_key_is_bounded_rather_than_stored_whole() {
        let b = Bucket::new(1);
        let now = Instant::now();
        b.allow(&"x".repeat(100_000), now);
        let held = b.attempts.lock().unwrap();
        assert!(held.keys().all(|k| k.len() <= MAX_KEY_LEN), "a key was stored whole");
    }

    /// The cut lands on a character boundary. A key of 127 ASCII bytes and one two-byte character puts byte 128
    /// inside that character, and slicing there would panic on a request anyone can send.
    #[test]
    fn a_key_cut_mid_character_does_not_panic() {
        let b = Bucket::new(1);
        let now = Instant::now();
        // Answering at all is the assertion: slicing mid-character panics, and a panic fails this test.
        for filler in [MAX_KEY_LEN - 1, MAX_KEY_LEN, MAX_KEY_LEN + 1] {
            let key = format!("{}\u{e9}", "x".repeat(filler));
            b.allow(&key, now);
            b.retry_after(&key, now);
        }
    }

    /// And a key that is nothing but one over-long character still cuts to something.
    #[test]
    fn a_key_of_multibyte_characters_is_bounded() {
        let key = "\u{1f600}".repeat(1000);
        assert!(bounded(&key).len() <= MAX_KEY_LEN);
        assert!(key.starts_with(bounded(&key)), "the bound cut into a character");
    }

    /// One key's attempts are bounded too, so a flood against one username cannot grow its vector without limit.
    #[test]
    fn one_keys_attempts_are_bounded() {
        let b = Bucket::new(2);
        let now = Instant::now();
        for _ in 0..1000 {
            b.allow("alice", now);
        }
        let held = b.attempts.lock().unwrap();
        assert!(held["alice"].len() <= 3, "held {} attempts", held["alice"].len());
    }

    #[test]
    fn a_throttled_caller_is_told_how_long_to_wait() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for _ in 0..PER_USERNAME {
            t.allow("alice", Some("10.0.0.1"), now).unwrap();
        }
        let wait = t.allow("alice", Some("10.0.0.1"), now).unwrap_err();
        assert!(wait > Duration::ZERO && wait <= WINDOW, "{wait:?}");
    }

    /// Guessing one password against many accounts never trips the per-username bucket, which is the whole reason
    /// the per-address one exists.
    #[test]
    fn spreading_guesses_across_usernames_still_runs_into_the_address_limit() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for i in 0..PER_ADDRESS {
            t.allow(&format!("user-{i}"), Some("10.0.0.1"), now)
                .unwrap_or_else(|_| panic!("attempt {i} should be allowed"));
        }
        assert!(t.allow("user-fresh", Some("10.0.0.1"), now).is_err());
    }

    /// Sharing the sign-in bucket would let anyone holding a session spend that account's ability to sign in.
    #[test]
    fn checking_the_current_password_does_not_spend_the_sign_in_allowance() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for i in 0..PER_PASSWORD_CHECK {
            t.allow_password_check("u_1", now)
                .unwrap_or_else(|_| panic!("check {i} should be allowed"));
        }
        assert!(
            t.allow("admin", Some("10.0.0.1"), now).is_ok(),
            "signing in was throttled"
        );
    }

    /// Bounded all the same: a session somebody walked away from does not buy unlimited guesses at the password.
    #[test]
    fn current_password_checks_are_bounded() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for _ in 0..PER_PASSWORD_CHECK {
            t.allow_password_check("u_1", now).unwrap();
        }
        assert!(t.allow_password_check("u_1", now).is_err());
        assert!(
            t.allow_password_check("u_2", now).is_ok(),
            "another account was caught in it"
        );
    }

    /// And one address filling its bucket does not lock everybody else out.
    #[test]
    fn one_address_running_out_does_not_throttle_another() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for i in 0..PER_ADDRESS {
            let _ = t.allow(&format!("user-{i}"), Some("10.0.0.1"), now);
        }
        assert!(t.allow("alice", Some("10.0.0.2"), now).is_ok());
    }

    /// Behind a proxy every attempt shares the proxy's address, so the address bucket is not counted there:
    /// one bad guess per name goes on past thirty, and the sixth against one name is still refused.
    #[test]
    fn without_an_address_only_the_username_bucket_counts() {
        let t = LoginThrottle::default();
        let now = Instant::now();
        for i in 0..PER_ADDRESS + 1 {
            assert!(t.allow(&format!("user-{i}"), None, now).is_ok(), "attempt {i}");
        }
        for _ in 0..PER_USERNAME {
            t.allow("alice", None, now).unwrap();
        }
        assert!(t.allow("alice", None, now).is_err());
    }
}
