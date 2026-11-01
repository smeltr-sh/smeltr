//! A failure a page shows, logged where it was recorded.
//!
//! What the leader stores for a page to show -- why the last ACME order failed, why the remote is not followed, why
//! a save is not on the host -- must also be in the log, where it is read later and beside everything else. The
//! page's text and the log line are one event, so one construction makes both: [`noted!`] logs the sentence with
//! its fields and the message as `error`, and hands back the message as a [`Noted`]. A `Noted` has no other way
//! to be made from a string, so a stored failure that was never logged does not compile.
//!
//! Reading one back from a store is not a new failure, so deserializing makes one silently.

use std::fmt;

/// A failure message that was logged when it was recorded. Serializes as the string it holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Noted(String);

impl Noted {
    /// The message [`noted!`] just logged. Only the macro calls this: it is public so the macro expands outside
    /// this crate's module tree, and is not a way around it.
    #[doc(hidden)]
    pub fn logged(message: String) -> Self {
        Noted(message)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Noted {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Noted {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Noted {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Noted {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for Noted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Logs a failure at warn and keeps its message as a [`Noted`], for a page to show:
///
/// ```ignore
/// let message = noted!(target: "remote.sync_failed", "the remote could not be followed", why);
/// let message = noted!(target: "remote.push_failed", "a save was not pushed", why, branch = %branch);
/// ```
///
/// The target and sentence are constant per site, as every log line's are; the message goes in the `error` field,
/// after any fields given.
#[macro_export]
macro_rules! noted {
    (target: $target:literal, $sentence:literal, $message:expr $(, $($field:tt)+)?) => {{
        let message: String = $message;
        tracing::warn!(target: $target, $($($field)+,)? error = %message, $sentence);
        $crate::logs::Noted::logged(message)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_noted_failure_is_logged_as_it_is_made() {
        let mut made = None;
        let seen = crate::logs::said(|| {
            made = Some(noted!(target: "test.noted", "something failed", "it broke".to_string(), subject = "x"));
        });
        let made = made.unwrap();
        assert_eq!(made, "it broke");
        assert_eq!(seen.len(), 1, "{seen:?}");
        let (level, site, fields) = &seen[0];
        assert_eq!((*level, site.as_str()), (tracing::Level::WARN, "test.noted"));
        let field = |k: &str| fields.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(field("subject"), Some("x"));
        assert_eq!(field("error"), Some("it broke"));
        assert_eq!(field("message"), Some("something failed"));
    }

    #[test]
    fn a_noted_failure_is_stored_and_read_back_as_its_text() {
        let made = noted!(target: "test.noted", "something failed", "it broke".to_string());
        assert_eq!(serde_json::to_string(&made).unwrap(), r#""it broke""#);
        let back: Noted = serde_json::from_str(r#""it broke""#).unwrap();
        assert_eq!(back, made);
    }
}
