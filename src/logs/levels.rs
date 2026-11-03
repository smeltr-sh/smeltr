//! The level each log line is kept at, which can change while the process runs.
//!
//! A line has a class and a type, and a service when it talks to one (`docs/architecture/observability.md`). An
//! override names a class and a type, a service, or a class. A line is kept at the first of these that names it, in
//! that order, then at the default.

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock};

use tracing::Level;

/// What lines are kept at: `default` for every line no override names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Levels {
    pub default: Level,
    /// By class, then type.
    pub types: BTreeMap<String, BTreeMap<String, Level>>,
    pub services: BTreeMap<String, Level>,
    pub classes: BTreeMap<String, Level>,
}

impl Levels {
    /// Every line at `default`.
    pub const fn at(default: Level) -> Levels {
        Levels {
            default,
            types: BTreeMap::new(),
            services: BTreeMap::new(),
            classes: BTreeMap::new(),
        }
    }

    /// The level a line of `class` and `kind` is kept at, from code that talks to `service`. A library's line said
    /// outside any span of ours has no class.
    pub fn kept_at(&self, class: Option<&str>, kind: &str, service: Option<&str>) -> Level {
        let by_type = class.and_then(|c| self.types.get(c)).and_then(|types| types.get(kind));
        let by_service = || service.and_then(|s| self.services.get(s));
        let by_class = || class.and_then(|c| self.classes.get(c));
        by_type
            .or_else(by_service)
            .or_else(by_class)
            .copied()
            .unwrap_or(self.default)
    }

    /// The most any line is kept at.
    pub fn most(&self) -> Level {
        self.types
            .values()
            .flat_map(BTreeMap::values)
            .chain(self.services.values())
            .chain(self.classes.values())
            .copied()
            .fold(self.default, Level::max)
    }
}

/// Every class a line of smeltr's is in, for whoever chooses one to turn up.
pub const CLASSES: &[&str] = &[
    "sources",
    "destinations",
    "pipelines",
    "deploy",
    "heartbeats",
    "enrollment",
    "certificates",
    "git",
    "captures",
    "status",
    "alerts",
    "api",
    "accounts",
    "secrets",
    "store",
    "logs",
    "processes",
    "updates",
];

/// Where a line is said, and the outside service the code there talks to: a module, or a library that names itself.
/// A module's own modules are in it.
const SERVICES: &[(&str, &str)] = &[
    ("smeltr::io::kafka", "kafka"),
    ("librdkafka", "kafka"),
    ("smeltr::io::elastic_receiver", "elasticReceiver"),
    ("smeltr::io::elastic", "elasticsearch"),
    ("smeltr::io::elastic_mapping", "elasticsearch"),
    ("smeltr::io::elastic_nodes", "elasticsearch"),
    ("smeltr::io::iceberg", "iceberg"),
    ("smeltr::io::delta", "delta"),
    ("smeltr::io::files", "files"),
    ("smeltr::io::sftp", "sftp"),
    ("smeltr::io::sql_source", "sql"),
    ("smeltr::io::mysql", "sql"),
    ("smeltr::io::pubsub", "gcsPubSub"),
    ("smeltr::io::storage_queue", "azureBlobQueue"),
    ("smeltr::io::syslog_receiver", "syslogReceiver"),
    ("smeltr::io::splunk", "splunkHec"),
    ("smeltr::io::okta", "oktaLogs"),
    ("smeltr::io::remote_write", "prometheus"),
    ("smeltr::leader::acme", "acme"),
    ("smeltr::leader::acme_dns", "acme"),
    ("smeltr::acme_api", "acme"),
    ("smeltr::leader::oidc", "oidc"),
    ("smeltr::oidc_api", "oidc"),
    ("smeltr::leader::remote", "git_remote"),
];

/// The service a line said at `origin` talks to: its module path, or a library's own name.
pub fn service(origin: &str) -> Option<&'static str> {
    SERVICES
        .iter()
        .find(|(at, _)| {
            origin
                .strip_prefix(at)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
        })
        .map(|(_, service)| *service)
}

/// Every service a line can name, each once.
pub fn services() -> Vec<&'static str> {
    let mut all: Vec<&str> = SERVICES.iter().map(|(_, s)| *s).collect();
    all.sort_unstable();
    all.dedup();
    all
}

static LEVELS: RwLock<Levels> = RwLock::new(Levels::at(Level::INFO));

/// The level a line of `class` and `kind` from code that talks to `service` is kept at now.
pub fn kept_at(class: Option<&str>, kind: &str, service: Option<&str>) -> Level {
    LEVELS
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .kept_at(class, kind, service)
}

/// The levels kept at now.
pub fn current() -> Levels {
    LEVELS.read().unwrap_or_else(PoisonError::into_inner).clone()
}

/// Keep lines at `levels` from now on. Every call site is asked again, so one that was off costs nothing until an
/// override that names it is turned up, and one turned down costs nothing again at once.
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
    fn a_line_talks_to_the_service_its_module_is_listed_for() {
        assert_eq!(service("smeltr::io::kafka"), Some("kafka"));
        assert_eq!(service("smeltr::io::kafka::client"), Some("kafka"));
        assert_eq!(service("librdkafka"), Some("kafka"));
        assert_eq!(service("smeltr::io::elastic_receiver"), Some("elasticReceiver"));
        assert_eq!(service("smeltr::io::elastic"), Some("elasticsearch"));
        assert_eq!(service("smeltr::leader::remote"), Some("git_remote"));
        assert_eq!(service("smeltr::leader::store"), None);
        assert_eq!(service("smeltr::io::kafkaesque"), None);
    }

    /// Each class and type a line names is checked against [`CLASSES`], and each type is snake_case, so a line in a
    /// class nobody can choose does not compile past this test. A line of ours names its type in `name:`.
    #[test]
    fn every_line_names_a_listed_class_and_a_type() {
        fn walk(dir: &std::path::Path, found: &mut Vec<String>) {
            // A line's class and type, or a span's class, which takes its name as its sentence.
            let line = regex::Regex::new(r#"(name: "([^"]*)",\s*)?target(: | = )"([^"]*)"\s*[,)]"#).unwrap();
            let span = regex::Regex::new(r"(_span!\(\s*(parent: [^,]+,\s*)?|instrument\([^)]*)$").unwrap();
            let snake = regex::Regex::new(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$").unwrap();
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    for named in line.captures_iter(&text) {
                        let class = &named[4];
                        let at = named.get(0).unwrap().start();
                        let is_span = span.is_match(&text[at.saturating_sub(200)..at]);
                        if class == "test"
                            || class.is_empty()
                            || !class.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                        {
                            continue;
                        }
                        if !CLASSES.contains(&class) {
                            found.push(format!("{}: class {class}", path.display()));
                        }
                        match named.get(2) {
                            Some(kind) if !snake.is_match(kind.as_str()) => {
                                found.push(format!("{}: type {}", path.display(), kind.as_str()))
                            }
                            None if !is_span => found.push(format!("{}: {class} has no type", path.display())),
                            _ => {}
                        }
                    }
                }
            }
        }
        let mut found = Vec::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut found,
        );
        assert!(found.is_empty(), "lines that cannot be chosen:\n{}", found.join("\n"));
    }

    #[test]
    fn a_line_is_kept_at_the_first_override_that_names_it() {
        let mut levels = Levels::at(Level::INFO);
        levels.classes.insert("sources".into(), Level::TRACE);
        levels.services.insert("kafka".into(), Level::DEBUG);
        levels
            .types
            .entry("sources".into())
            .or_default()
            .insert("assigned".into(), Level::WARN);
        assert_eq!(levels.kept_at(Some("sources"), "assigned", Some("kafka")), Level::WARN);
        assert_eq!(
            levels.kept_at(Some("sources"), "poll_failed", Some("kafka")),
            Level::DEBUG
        );
        assert_eq!(levels.kept_at(Some("sources"), "poll_failed", None), Level::TRACE);
        assert_eq!(levels.kept_at(Some("store"), "statement_ran", None), Level::INFO);
        assert_eq!(levels.kept_at(None, "ureq", None), Level::INFO);
        assert_eq!(levels.most(), Level::TRACE);
    }
}
