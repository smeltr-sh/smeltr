//! The one value an operator pastes to bring a worker into a cluster.
//!
//! An enrollment code carries two things that have to arrive together: the secret that buys a credential, and how to
//! verify the leader it is spent against. Apart they are two things to copy and one to get wrong -- a secret pasted at
//! the wrong leader, or an authority that verifies a leader the secret does not belong to. Together, a worker either
//! has both or has neither.
//!
//! ```text
//! smeltrenroll-1-<authority>-<secret>
//!                |           |
//!                |           the leader exchanges this, once, for a credential
//!                the certificate a worker will accept, or `system` for the platform's own roots
//! ```
//!
//! The leader's address is not in here. It is not a secret, it is the part an operator most often has to correct, and a
//! leader behind a proxy or a port-forward cannot know it. It travels beside the enrollment code, where it can be read
//! and edited.

use std::fmt;
use std::str::FromStr;

const PREFIX: &str = "smeltrenroll";
const VERSION: &str = "1";
/// SHA-256 in lowercase hex.
const FINGERPRINT_CHARS: usize = 64;

/// What a worker will accept as the leader it enrolls with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// The platform's own roots. What a leader behind a proxy holding a publicly issued certificate needs, and
    /// what a leader serving plain HTTP does not use at all.
    System,
    /// One certificate, by SHA-256 over its DER. The worker keeps the certificate matching this and nothing else
    /// the leader offers beside it.
    Pinned(String),
}

/// A pasted enrollment code, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentCode {
    pub authority: Authority,
    pub secret: String,
}

impl fmt::Display for EnrollmentCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let authority = match &self.authority {
            Authority::System => "system",
            Authority::Pinned(fingerprint) => fingerprint,
        };
        write!(f, "{PREFIX}-{VERSION}-{authority}-{}", self.secret)
    }
}

impl FromStr for EnrollmentCode {
    type Err = String;

    /// Every refusal names what to do about it. An enrollment code is pasted by hand at the one moment a worker has
    /// nothing else to go on, and a worker that will not start is worth more than one that starts wrongly -- but only
    /// if it says which.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err("this enrollment code is empty. Mint one on the leader's Workers page.".to_string());
        }

        // The prefix first, and separately: a value that is not an enrollment code at all is far more often the
        // leader's address pasted into the wrong box than an enrollment code that lost its tail, and saying
        // "incomplete" to somebody holding a whole URL sends them looking for the wrong thing.
        let Some(rest) = trimmed.strip_prefix(PREFIX).and_then(|rest| rest.strip_prefix('-')) else {
            return Err(format!(
                "an enrollment code begins with {PREFIX:?} and this begins with {:?}. Check that the whole value was \
                 pasted, and that it is the enrollment code rather than the leader's address.",
                elided(trimmed)
            ));
        };

        // Three fields left, and the secret is last, so it may hold a dash of its own.
        let fields: Vec<&str> = rest.splitn(3, '-').collect();
        let [version, authority, secret] = fields.as_slice() else {
            return Err(cut_short(trimmed));
        };
        if *version != VERSION {
            return Err(format!(
                "this enrollment code is version {:?} and this worker reads version {VERSION:?}. Upgrade this worker, \
                 or mint an enrollment code on a leader of its own age.",
                elided(version)
            ));
        }

        let authority = if *authority == "system" {
            Authority::System
        } else if authority.len() == FINGERPRINT_CHARS && authority.chars().all(|c| c.is_ascii_hexdigit()) {
            Authority::Pinned(authority.to_ascii_lowercase())
        } else {
            return Err(format!(
                "the certificate authority in this enrollment code is {:?}, which is neither a SHA-256 fingerprint nor \
                 {:?}. The enrollment code was altered in transit -- mint another.",
                elided(authority),
                "system"
            ));
        };

        if secret.is_empty() {
            return Err(cut_short(trimmed));
        }

        Ok(EnrollmentCode {
            authority,
            secret: (*secret).to_string(),
        })
    }
}

/// An enrollment code that lost its tail -- a wrapped line, a truncated copy, a shell that ate the rest.
fn cut_short(value: &str) -> String {
    format!(
        "this enrollment code is incomplete: {:?}. It is one unbroken value; check nothing was lost to a line break \
         when it was copied.",
        elided(value)
    )
}

/// Enough of a value to recognize, without printing a secret into a log whole.
///
/// Every field of a malformed enrollment code goes through this, not only the one named `secret`: a paste that lost a
/// field shifts the rest along, and the secret lands wherever the missing one used to be.
fn elided(value: &str) -> String {
    const KEPT: usize = 24;
    match value.char_indices().nth(KEPT) {
        Some((at, _)) => format!("{}…", &value[..at]),
        None => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FINGERPRINT: &str = "5f2b8c1d4e6a7b9c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e";
    const SECRET: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    fn pinned() -> EnrollmentCode {
        EnrollmentCode {
            authority: Authority::Pinned(FINGERPRINT.to_string()),
            secret: SECRET.to_string(),
        }
    }

    #[test]
    fn an_enrollment_code_survives_being_written_down_and_read_back() {
        let written = pinned().to_string();
        assert_eq!(written.parse::<EnrollmentCode>().expect("reads back"), pinned());
        assert_eq!(written, format!("smeltrenroll-1-{FINGERPRINT}-{SECRET}"));
    }

    /// A paste that lost a field shifts every later one along, so the secret arrives where the version or the
    /// authority was expected. A refusal quoting the field it read has to elide whatever it holds.
    #[test]
    fn a_refusal_never_quotes_a_shifted_field_whole() {
        // The version dropped, so the fingerprint is read as the version and the secret as the authority.
        let shifted = format!("smeltrenroll-{FINGERPRINT}-{SECRET}-tail");
        let refused = shifted
            .parse::<EnrollmentCode>()
            .expect_err("an enrollment code with no version");
        assert!(!refused.contains(SECRET), "{refused}");
        assert!(!refused.contains(FINGERPRINT), "{refused}");
    }

    #[test]
    fn a_leader_verified_by_the_platform_says_so_rather_than_leaving_it_out() {
        let code = EnrollmentCode {
            authority: Authority::System,
            secret: SECRET.to_string(),
        };
        let written = code.to_string();
        assert_eq!(written, format!("smeltrenroll-1-system-{SECRET}"));
        assert_eq!(written.parse::<EnrollmentCode>().expect("reads back"), code);
    }

    /// Pasting drags whitespace along with it.
    #[test]
    fn surrounding_whitespace_is_not_part_of_the_enrollment_code() {
        let padded = format!("  {}\n", pinned());
        assert_eq!(padded.parse::<EnrollmentCode>().expect("reads back"), pinned());
    }

    #[test]
    fn a_fingerprint_reads_back_the_same_whatever_case_it_arrived_in() {
        let shouted = format!("smeltrenroll-1-{}-{SECRET}", FINGERPRINT.to_ascii_uppercase());
        assert_eq!(shouted.parse::<EnrollmentCode>().expect("reads back"), pinned());
    }

    #[test]
    fn a_secret_may_hold_the_dash_that_would_have_split_it() {
        let code: EnrollmentCode = "smeltrenroll-1-system-aaaa-bbbb".parse().expect("reads back");
        assert_eq!(code.secret, "aaaa-bbbb", "the fourth field is the whole of the rest");
    }

    #[test]
    fn every_refusal_says_what_to_do_about_it() {
        let cases = [
            ("", "Mint one"),
            ("smeltrenroll-1", "unbroken value"),
            (&format!("smeltrenroll-1-{FINGERPRINT}-"), "unbroken value"),
            (&format!("smeltrenroll-9-{FINGERPRINT}-{SECRET}"), "Upgrade this worker"),
            ("https://leader.example.com:4000", "the leader's address"),
            (&format!("smeltrenroll-1-nonsense-{SECRET}"), "mint another"),
        ];
        for (value, expected) in cases {
            let refused = value.parse::<EnrollmentCode>().expect_err(value);
            assert!(refused.contains(expected), "{value:?} said {refused:?}");
        }
    }

    /// A refusal is logged, and a log is drained to wherever the fleet delivers.
    #[test]
    fn a_refusal_does_not_print_the_whole_secret() {
        let long = format!("smeltrenroll-1-{FINGERPRINT}{SECRET}");
        let refused = long.parse::<EnrollmentCode>().expect_err("incomplete");
        assert!(!refused.contains(SECRET), "{refused}");
    }
}
