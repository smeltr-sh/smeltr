//! The level each area of the code is kept at, which can change while the process runs.
//!
//! An area is what a line's target names first: `kafka` for `kafka.placement_failed`. A library's area is its
//! crate, except a library that only ever works for one of smeltr's areas, whose lines are that area's: turning
//! `kafka` up turns librdkafka up with it.

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock};

use tracing::Level;

/// What lines are kept at: `default` for every area `areas` does not name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Levels {
    pub default: Level,
    pub areas: BTreeMap<String, Level>,
}

impl Levels {
    /// Every area at `default`.
    pub const fn at(default: Level) -> Levels {
        Levels {
            default,
            areas: BTreeMap::new(),
        }
    }

    /// The level a line from `target` is kept at.
    pub fn kept_at(&self, target: &str) -> Level {
        self.areas.get(area(target)).copied().unwrap_or(self.default)
    }

    /// The most any area keeps.
    pub fn most(&self) -> Level {
        self.areas.values().copied().fold(self.default, Level::max)
    }
}

/// Every area smeltr's own lines are in, for whoever chooses one to turn up. A library's crate is an area too.
pub const AREAS: &[&str] = &[
    "agent",
    "alerts",
    "api",
    "audit",
    "background",
    "bundle",
    "capture",
    "config",
    "delta",
    "deploy",
    "egress",
    "elasticsearch",
    "files",
    "git",
    "heartbeat",
    "http",
    "iceberg",
    "issues",
    "kafka",
    "leader",
    "listing",
    "logs",
    "logs_source",
    "lookup",
    "lookups",
    "map",
    "object_source",
    "plan",
    "process",
    "registry",
    "remote",
    "remote_write",
    "runtime",
    "secrets",
    "series",
    "sftp",
    "source",
    "sql",
    "startup",
    "stats",
    "store",
    "throughput",
    "tls",
    "updates",
    "warnings",
    "worker",
];

/// The libraries whose lines are one of smeltr's areas, by crate.
const SERVING: &[(&str, &str)] = &[
    ("librdkafka", "kafka"),
    ("rdkafka", "kafka"),
    ("ureq", "http"),
    ("rustls", "tls"),
    ("tokio_rustls", "tls"),
    ("gix", "git"),
];

/// The area a line from `target` belongs to.
pub fn area(target: &str) -> &str {
    let first = match target.split_once("::") {
        Some((krate, _)) => krate,
        None => target.split_once('.').map_or(target, |(area, _)| area),
    };
    SERVING
        .iter()
        .find(|(krate, _)| {
            first
                .strip_prefix(krate)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('_'))
        })
        .map_or(first, |(_, area)| area)
}

static LEVELS: RwLock<Levels> = RwLock::new(Levels::at(Level::INFO));

/// The level a line from `target` is kept at now.
pub fn kept_at(target: &str) -> Level {
    LEVELS.read().unwrap_or_else(PoisonError::into_inner).kept_at(target)
}

/// The levels kept at now.
pub fn current() -> Levels {
    LEVELS.read().unwrap_or_else(PoisonError::into_inner).clone()
}

/// Keep lines at `levels` from now on. Every call site is asked again, so one that was off costs nothing until an
/// area it belongs to is turned up, and one turned down costs nothing again at once.
pub fn set(levels: Levels) {
    let most = levels.most();
    {
        let mut held = LEVELS.write().unwrap_or_else(PoisonError::into_inner);
        if *held == levels {
            return;
        }
        *held = levels;
    }
    super::MAX_LEVEL.store(super::level_ord(most), std::sync::atomic::Ordering::Relaxed);
    tracing_log::log::set_max_level(super::as_log_filter(super::layer::facade_ceiling(most)));
    tracing::callsite::rebuild_interest_cache();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_is_in_the_area_its_target_names_first() {
        assert_eq!(area("kafka.placement_failed"), "kafka");
        assert_eq!(area("leader"), "leader");
        assert_eq!(area("hyper_util::client::legacy"), "hyper_util");
        assert_eq!(area("librdkafka"), "kafka");
        assert_eq!(area("rdkafka::client"), "kafka");
        assert_eq!(area("gix_transport::client"), "git");
        assert_eq!(area("rustls::conn"), "tls");
        assert_eq!(area("ureq::pool"), "http");
        assert_eq!(area("log"), "log");
    }

    /// A line in an area [`AREAS`] does not list could not be chosen.
    #[test]
    fn every_area_a_line_names_is_listed() {
        fn walk(dir: &std::path::Path, found: &mut std::collections::BTreeSet<String>) {
            let named = regex::Regex::new(r#"target(: | = )"([a-z_]+)\."#).unwrap();
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    for named in named.captures_iter(&text) {
                        found.insert(named[2].to_string());
                    }
                }
            }
        }
        let mut found = std::collections::BTreeSet::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut found,
        );
        // The tests' own lines.
        found.remove("test");
        let unlisted: Vec<_> = found.iter().filter(|a| !AREAS.contains(&a.as_str())).collect();
        assert!(unlisted.is_empty(), "areas not in AREAS: {unlisted:?}");
    }

    #[test]
    fn an_area_set_apart_is_kept_at_its_own_level() {
        let mut levels = Levels::at(Level::INFO);
        levels.areas.insert("kafka".into(), Level::TRACE);
        levels.areas.insert("store".into(), Level::WARN);
        assert_eq!(levels.kept_at("kafka.lag_unmeasured"), Level::TRACE);
        assert_eq!(levels.kept_at("librdkafka"), Level::TRACE);
        assert_eq!(levels.kept_at("store.statement"), Level::WARN);
        assert_eq!(levels.kept_at("http.throttled"), Level::INFO);
        assert_eq!(levels.most(), Level::TRACE);
    }
}
