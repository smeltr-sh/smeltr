//! The build's own version: a date, not a bump, so it moves whenever the binary is rebuilt rather than
//! when someone remembers to touch `Cargo.toml`. `build.rs` composes it once, at compile time. The leader
//! reports it as its own, and each worker carries it onto every heartbeat.

/// `YYYY.M.D+<branch>.<first 12 of the sha>`, with `-dirty-<first 12 of a hash>` after it for a build with
/// uncommitted edits. See `build.rs` for how it is built.
pub const VERSION: &str = env!("SMELTR_VERSION");

/// The release this build is, `1.2.0`, when it was built from the commit tagged `v1.2.0` with nothing uncommitted;
/// `None` for every other build. What people read; [`VERSION`] is what builds compare.
pub const RELEASE: Option<&str> = match env!("SMELTR_RELEASE").as_bytes() {
    [] => None,
    _ => Some(env!("SMELTR_RELEASE")),
};

/// The commit a version was built from: what names the code, where the date only names the day it was compiled.
pub fn commit_of(version: &str) -> &str {
    version.rsplit('.').next().unwrap_or(version)
}

/// This build's commit.
pub fn commit() -> &'static str {
    commit_of(VERSION)
}

/// Whether a worker of one build takes deployments from a leader of the other. Two releases are when they share a
/// major: within one, a newer release reads what an older one wrote, and an older one refuses what it does not
/// know. Any other pair is only when it is one commit, since nothing promises what an unreleased build reads.
pub fn compatible(commit: &str, release: Option<&str>, other_commit: &str, other_release: Option<&str>) -> bool {
    match (release.and_then(line), other_release.and_then(line)) {
        (Some(major), Some(other_major)) => major == other_major,
        _ => commit == other_commit,
    }
}

/// The major a release belongs to. A pre-release (`1.3.0-rc1`) promises nothing to the rest of its major, so it
/// has none.
fn line(release: &str) -> Option<u64> {
    match release.contains('-') {
        true => None,
        false => release.split('.').next()?.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn releases_of_one_major_are_compatible_and_every_other_pair_is_one_commit() {
        let c = super::compatible;
        assert!(
            c("a", Some("1.2.0"), "b", Some("1.3.4")),
            "minor and patch releases of one major"
        );
        assert!(c("a", Some("1.3.4"), "b", Some("1.2.0")), "in either direction");
        assert!(!c("a", Some("1.3.0"), "b", Some("2.0.0")), "two majors");
        assert!(!c("a", Some("1.3.0-rc1"), "b", Some("1.3.0")), "a pre-release");
        assert!(
            c("a", Some("1.3.0-rc1"), "a", Some("1.3.0-rc1")),
            "a pre-release is its own commit"
        );
        assert!(!c("a", Some("1.2.0"), "b", None), "a release and a dev build");
        assert!(
            c("a", Some("1.2.0"), "a", None),
            "one commit, whether or not one clone holds its tag"
        );
        assert!(!c("a", None, "b", None), "two dev builds");
        assert!(c("a", None, "a", None), "one dev build");
        assert!(!c("a", Some(""), "b", Some("")), "an empty release is none");
        assert!(c("a", Some("01.2.0"), "b", Some("1.3.0")), "a major is a number");
    }

    #[test]
    fn the_commit_is_the_last_part_of_a_version() {
        assert_eq!(super::commit_of("2026.10.5+claude-x-y.1a2b3c4"), "1a2b3c4");
        assert_eq!(
            super::commit_of("2026.10.5+main.1244c9e5a0b3-dirty-3f0a9c1d2e4b"),
            "1244c9e5a0b3-dirty-3f0a9c1d2e4b"
        );
        assert_eq!(super::commit_of(""), "");
    }
}
