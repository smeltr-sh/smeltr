//! What each operator route needs a person to hold, decided from the request line alone.
//!
//! An area is the `?scope=` every config read and write names, so the gate can judge a request before any handler
//! parses it. A request that names none is let through to its handler, which refuses it: there is no area to judge,
//! and the gate and the handler must not disagree on which one it was. A route not listed here needs an administrator: a route
//! added without a decision about who may use it is closed until somebody makes one.
//!
//! Some routes cannot be judged from the request line, because the area is in the body, a branch or a capture. The
//! gate asks [`Need::Somewhere`] of those, and the handler checks the rest once it knows the area (`start_capture` and
//! the captures it opened, `review_change`, `deploy_change`, `deploy_commit`, and a test call with `ownScopes`).

use axum::http::{Method, Uri};

use smeltr::config_file::{GROUPS_DIR, SHARED_DIR};
use smeltr::leader::access::{Access, Level};

/// What a request needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Need {
    /// `level` in one area.
    Area {
        area: String,
        level: Level,
    },
    /// `level` in at least one area.
    Somewhere(Level),
    /// Any account at all: what a person sets about themselves.
    Anyone,
    Administrator,
}

impl Need {
    pub fn met_by(&self, access: &Access) -> bool {
        match self {
            Need::Area { area, level } => access.allows(area, *level),
            Need::Somewhere(level) => access.somewhere(*level),
            Need::Anyone => true,
            Need::Administrator => access.is_administrator(),
        }
    }

    /// The sentence a refusal answers with.
    pub fn refusal(&self) -> String {
        match self {
            Need::Area { area, level } => format!("this needs {} in {area}", level.as_str()),
            Need::Somewhere(level) => format!("this needs {} in at least one area", level.as_str()),
            Need::Anyone => String::new(),
            Need::Administrator => "this needs an administrator".to_string(),
        }
    }
}

/// What `method` on `path`, with `query` (the raw query string, if any), needs.
pub fn need(method: &Method, path: &str, query: Option<&str>) -> Need {
    let Some(rest) = path.strip_prefix("/api/") else {
        return Need::Administrator;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    let Some(query) = Query::parse(query) else {
        return Need::Administrator;
    };
    let get = *method == Method::GET || *method == Method::HEAD;
    let read = if get { Level::Viewer } else { Level::Editor };
    let scoped = |level: Level| query.scope(level);

    match (method.clone(), segments.as_slice()) {
        // The configuration of one area.
        (Method::GET | Method::HEAD, ["config"])
        | (Method::GET | Method::HEAD, ["config", "unreadable"])
        | (Method::GET | Method::HEAD, ["pipelines"])
        | (Method::GET | Method::HEAD, ["pipeline-callers"])
        | (Method::GET | Method::HEAD, ["connections"])
        | (Method::GET | Method::HEAD, ["lookup", "file"])
        | (Method::GET | Method::HEAD, ["lookup", "mmdb"]) => scoped(Level::Viewer),
        (Method::GET | Method::HEAD | Method::PUT | Method::DELETE, ["pipelines", _]) => scoped(read),
        (
            Method::PUT,
            ["sources" | "destinations" | "routes" | "lookup-tables" | "grok-patterns" | "schemas" | "manifest"],
        )
        | (Method::POST, ["undo"])
        | (Method::POST, ["lookup", "upload"]) => scoped(Level::Editor),
        (Method::GET | Method::HEAD | Method::PUT | Method::DELETE, ["config", "file"]) => query.file(read),

        // Tests. A review reads and runs the change's own, whose areas only the handler can find.
        (Method::GET | Method::HEAD, ["tests"]) => query.tests(Level::Viewer),
        (Method::PUT, ["tests"]) => scoped(Level::Editor),
        (Method::POST, ["tests", "run"]) => query.tests(Level::Editor),
        (Method::GET | Method::HEAD | Method::DELETE, ["tests", _]) => scoped(read),

        // Production records: samples, and what a preview or a capture shows.
        (Method::GET | Method::HEAD | Method::PUT, ["samples"])
        | (Method::GET | Method::HEAD | Method::DELETE, ["samples", _])
        | (Method::POST, ["preview"])
        | (Method::POST, ["route-preview"])
        | (Method::POST, ["captures", _, "save"]) => scoped(Level::Editor),
        (Method::POST, ["captures"])
        | (Method::GET | Method::HEAD, ["captures", _])
        | (Method::POST, ["captures", _, "stop" | "discard"])
        | (Method::POST, ["grok-captures"])
        | (Method::POST, ["onboard", "propose"])
        | (Method::POST, ["line-breaker"])
        | (Method::POST, ["format"])
        | (Method::POST, ["certificates", "describe"])
        | (Method::POST, ["sql", "statement"])
        // Names, versions and where each is used, never a value: what a secret picker offers whoever saves a form.
        | (Method::GET | Method::HEAD, ["secrets"])
        | (Method::GET | Method::HEAD, ["certificates"]) => Need::Somewhere(Level::Editor),

        // Branches and changes. Which areas a change touches is in the branch, so reviewing and deploying one is
        // checked again by its handler.
        (Method::GET | Method::HEAD, ["branches"])
        | (Method::GET | Method::HEAD, ["history"])
        | (Method::GET | Method::HEAD, ["diff"])
        | (Method::GET | Method::HEAD, ["branches", _, "diff"])
        | (Method::GET | Method::HEAD, ["branches", _, "schema-findings"])
        | (Method::GET | Method::HEAD, ["changes", _, "timeline"])
        | (Method::POST, ["changes", _, "comments"])
        | (Method::POST, ["changes", _, "threads", _]) => Need::Somewhere(Level::Viewer),
        (Method::POST, ["branches"])
        | (Method::DELETE, ["branches", _])
        | (Method::POST, ["changes"])
        | (Method::PATCH, ["changes", _])
        | (Method::POST, ["changes", _, "close" | "bring-current"]) => Need::Somewhere(Level::Editor),
        (Method::POST, ["changes", _, "reviews" | "deploy"]) | (Method::POST, ["deploy"]) => {
            Need::Somewhere(Level::Deployer)
        }

        // What the Status and Map pages read, the scope picker, and what a source or destination form asks of
        // the build.
        (Method::GET | Method::HEAD, ["cluster"])
        | (Method::GET | Method::HEAD, ["warnings"])
        | (Method::GET | Method::HEAD, ["flow"])
        | (Method::GET | Method::HEAD, ["map"])
        | (Method::GET | Method::HEAD, ["map", "measurements"])
        | (Method::GET | Method::HEAD, ["stats"])
        | (Method::GET | Method::HEAD, ["worker-groups"])
        | (Method::GET | Method::HEAD, ["system", "info"]) => Need::Somewhere(Level::Viewer),

        // The schema catalog: what is built in, and what any schema holds, never config.
        (Method::GET | Method::HEAD, ["schemas", "builtin" | "export"])
        | (Method::POST, ["schemas", "object" | "search"]) => Need::Somewhere(Level::Viewer),

        // What a person lets their own agent use.
        (Method::GET | Method::HEAD | Method::PUT, ["me", "agent-permissions"]) => Need::Anyone,
        // A person's conversations with their agent, which their handlers keep to the person.
        (Method::GET | Method::HEAD | Method::POST, ["agent", "conversations"])
        | (Method::GET | Method::HEAD, ["agent", "models"])
        | (Method::GET | Method::HEAD | Method::DELETE, ["agent", "conversations", _])
        | (Method::POST, ["agent", "conversations", _, "messages" | "stop" | "context"]) => Need::Anyone,
        // The user docs, which are public.
        (Method::GET | Method::HEAD, ["docs"]) | (Method::GET | Method::HEAD, ["docs", "page"]) => Need::Anyone,

        _ => Need::Administrator,
    }
}

/// The parts of a query string the policy reads. A key given twice is refused rather than guessed at: the gate
/// and the handler must agree on which value names the area, and the handler's decoder does not promise which
/// of two it would take.
struct Query {
    scope: Option<String>,
    path: Option<String>,
    own_scopes: Option<String>,
}

impl Query {
    fn parse(raw: Option<&str>) -> Option<Query> {
        let mut query = Query {
            scope: None,
            path: None,
            own_scopes: None,
        };
        let Some(raw) = raw.filter(|raw| !raw.is_empty()) else {
            return Some(query);
        };
        let uri: Uri = format!("/?{raw}").parse().ok()?;
        let axum::extract::Query(pairs) = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&uri).ok()?;
        for (key, value) in pairs {
            let slot = match key.as_str() {
                "scope" => &mut query.scope,
                "path" => &mut query.path,
                "ownScopes" => &mut query.own_scopes,
                _ => continue,
            };
            if slot.replace(value).is_some() {
                return None;
            }
        }
        Some(query)
    }

    /// `level` in the area `?scope=` names. A request that names none needs `level` somewhere, and its handler
    /// refuses it (`config_api::NO_SCOPE`).
    fn scope(&self, level: Level) -> Need {
        match self.scope.as_deref() {
            None | Some("") => Need::Somewhere(level),
            Some(area) => Need::Area {
                area: area.to_string(),
                level,
            },
        }
    }

    /// `level` in the area `?path=` is in: `shared/…` is shared, `groups/<id>/…` is that group, as
    /// `config_api::scope_of_file` reads it.
    fn file(&self, level: Level) -> Need {
        let Some(path) = &self.path else {
            return Need::Administrator;
        };
        match area_of_path(path) {
            Some(area) => Need::Area { area, level },
            None => Need::Administrator,
        }
    }

    /// A test call: the area `?scope=` names, or with `ownScopes=true`, the change's, which the handler checks.
    fn tests(&self, level: Level) -> Need {
        match self.own_scopes.as_deref() {
            None | Some("false") => self.scope(level),
            Some("true") => Need::Somewhere(level),
            Some(_) => Need::Administrator,
        }
    }
}

/// The area a repository path is in: `shared/…` is shared, `groups/<id>/…` is that group. `None` for a path in
/// no area.
pub fn area_of_path(path: &str) -> Option<String> {
    if let Some(rest) = path.strip_prefix(SHARED_DIR).and_then(|rest| rest.strip_prefix('/')) {
        return (!rest.is_empty()).then(|| SHARED_DIR.to_string());
    }
    let rest = path.strip_prefix(GROUPS_DIR)?.strip_prefix('/')?;
    let (group, within) = rest.split_once('/')?;
    (!group.is_empty() && !within.is_empty()).then(|| group.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn area(area: &str, level: Level) -> Need {
        Need::Area {
            area: area.into(),
            level,
        }
    }

    fn get(path: &str, query: Option<&str>) -> Need {
        need(&Method::GET, path, query)
    }

    #[test]
    fn a_route_nobody_listed_needs_an_administrator() {
        assert_eq!(get("/api/something-new", None), Need::Administrator);
        assert_eq!(
            need(&Method::POST, "/api/config", Some("scope=payments")),
            Need::Administrator
        );
        assert_eq!(get("/api/config/", None), Need::Administrator);
        assert_eq!(get("/elsewhere", None), Need::Administrator);
    }

    #[test]
    fn administration_needs_an_administrator() {
        for (method, path) in [
            (Method::GET, "/api/users"),
            (Method::POST, "/api/users"),
            (Method::PATCH, "/api/users/u_1"),
            (Method::GET, "/api/roles"),
            (Method::PUT, "/api/roles/smeltr-payments"),
            (Method::DELETE, "/api/roles/smeltr-payments"),
            (Method::GET, "/api/security-events"),
            (Method::GET, "/api/settings/sessions"),
            (Method::PUT, "/api/settings/oidc"),
            (Method::GET, "/api/settings/agents"),
            (Method::PUT, "/api/settings/agents"),
            (Method::GET, "/api/settings/certificates"),
            (Method::POST, "/api/secrets"),
            (Method::GET, "/api/secrets/s1/versions/1"),
            (Method::POST, "/api/certificates"),
            (Method::PUT, "/api/certificates/c1"),
            (Method::GET, "/api/workers"),
            (Method::POST, "/api/workers/w1/restart"),
            (Method::GET, "/api/enrollment-codes"),
            (Method::GET, "/api/logs"),
            (Method::GET, "/api/health"),
            (Method::POST, "/api/worker-groups"),
            (Method::PUT, "/api/worker-groups/payments"),
            (Method::DELETE, "/api/worker-groups/payments"),
            (Method::PUT, "/api/worker-groups/payments/hue"),
            (Method::POST, "/api/branches/b/overwrite"),
        ] {
            assert_eq!(
                need(&method, path, Some("scope=payments")),
                Need::Administrator,
                "{method} {path}"
            );
        }
    }

    #[test]
    fn a_config_read_needs_viewer_in_the_scope_it_names() {
        for path in [
            "/api/config",
            "/api/config/unreadable",
            "/api/pipelines",
            "/api/pipelines/p1",
            "/api/pipeline-callers",
            "/api/connections",
            "/api/tests",
            "/api/tests/t1",
            "/api/lookup/file",
            "/api/lookup/mmdb",
        ] {
            assert_eq!(
                get(path, Some("branch=b&scope=payments")),
                area("payments", Level::Viewer),
                "{path}"
            );
        }
    }

    /// Its handler refuses it; the gate asks only that the caller could have named some area.
    #[test]
    fn a_missing_scope_is_left_to_the_handler() {
        for query in [None, Some("branch=b"), Some("branch=b&scope=")] {
            assert_eq!(get("/api/config", query), Need::Somewhere(Level::Viewer), "{query:?}");
        }
        assert_eq!(
            need(&Method::PUT, "/api/sources", Some("branch=b")),
            Need::Somewhere(Level::Editor)
        );
    }

    #[test]
    fn a_config_write_needs_editor_in_the_scope_it_names() {
        for (method, path) in [
            (Method::PUT, "/api/sources"),
            (Method::PUT, "/api/destinations"),
            (Method::PUT, "/api/routes"),
            (Method::PUT, "/api/manifest"),
            (Method::PUT, "/api/lookup-tables"),
            (Method::PUT, "/api/grok-patterns"),
            (Method::PUT, "/api/schemas"),
            (Method::PUT, "/api/pipelines/p1"),
            (Method::DELETE, "/api/pipelines/p1"),
            (Method::PUT, "/api/tests"),
            (Method::DELETE, "/api/tests/t1"),
            (Method::POST, "/api/undo"),
            (Method::POST, "/api/lookup/upload"),
        ] {
            assert_eq!(
                need(&method, path, Some("scope=payments")),
                area("payments", Level::Editor),
                "{method} {path}"
            );
        }
    }

    #[test]
    fn records_need_editor_in_their_area() {
        for (method, path) in [
            (Method::GET, "/api/samples"),
            (Method::PUT, "/api/samples"),
            (Method::GET, "/api/samples/s1"),
            (Method::DELETE, "/api/samples/s1"),
            (Method::POST, "/api/tests/run"),
            (Method::POST, "/api/preview"),
            (Method::POST, "/api/route-preview"),
            (Method::POST, "/api/captures/c1/save"),
        ] {
            assert_eq!(
                need(&method, path, Some("scope=payments")),
                area("payments", Level::Editor),
                "{method} {path}"
            );
        }
        for (method, path) in [
            (Method::POST, "/api/captures"),
            (Method::GET, "/api/captures/c1"),
            (Method::POST, "/api/captures/c1/stop"),
            (Method::POST, "/api/captures/c1/discard"),
            (Method::POST, "/api/grok-captures"),
            (Method::POST, "/api/onboard/propose"),
            (Method::POST, "/api/line-breaker"),
            (Method::POST, "/api/format"),
            (Method::POST, "/api/certificates/describe"),
            (Method::POST, "/api/sql/statement"),
            (Method::GET, "/api/secrets"),
            (Method::GET, "/api/certificates"),
        ] {
            assert_eq!(
                need(&method, path, None),
                Need::Somewhere(Level::Editor),
                "{method} {path}"
            );
        }
    }

    /// Without the flag a missing scope would read as shared, and a reviewer holding shared would be let into
    /// every group's tests.
    #[test]
    fn a_review_s_test_call_is_judged_by_its_handler() {
        assert_eq!(
            get("/api/tests", Some("branch=b&ownScopes=true")),
            Need::Somewhere(Level::Viewer)
        );
        assert_eq!(
            need(&Method::POST, "/api/tests/run", Some("branch=b&ownScopes=true")),
            Need::Somewhere(Level::Editor)
        );
        assert_eq!(
            get("/api/tests", Some("ownScopes=false&scope=payments")),
            area("payments", Level::Viewer)
        );
        assert_eq!(get("/api/tests", Some("ownScopes=yes")), Need::Administrator);
    }

    #[test]
    fn a_file_is_in_the_area_its_path_names() {
        assert_eq!(
            get("/api/config/file", Some("path=groups%2Fpayments%2Fsources.json")),
            area("payments", Level::Viewer)
        );
        assert_eq!(
            need(&Method::PUT, "/api/config/file", Some("path=shared/routes.json")),
            area("shared", Level::Editor)
        );
        assert_eq!(
            need(&Method::DELETE, "/api/config/file", Some("path=groups/payments/x.json")),
            area("payments", Level::Editor)
        );
        for outside in [
            "other/x.json",
            "shared/",
            "groups/payments",
            "groups//x.json",
            "sharedx/y",
        ] {
            assert_eq!(
                get("/api/config/file", Some(&format!("path={outside}"))),
                Need::Administrator,
                "{outside}"
            );
        }
        assert_eq!(get("/api/config/file", None), Need::Administrator);
    }

    #[test]
    fn a_key_given_twice_is_refused_rather_than_guessed_at() {
        assert_eq!(
            get("/api/config", Some("scope=payments&scope=search")),
            Need::Administrator
        );
        assert_eq!(
            get("/api/config/file", Some("path=shared/a.json&path=groups/x/a.json")),
            Need::Administrator
        );
    }

    #[test]
    fn a_scope_is_read_decoded() {
        assert_eq!(
            get("/api/config", Some("scope=pay%6Dents")),
            area("payments", Level::Viewer)
        );
    }

    #[test]
    fn branches_and_changes() {
        for (method, path) in [
            (Method::GET, "/api/branches"),
            (Method::GET, "/api/history"),
            (Method::GET, "/api/diff"),
            (Method::GET, "/api/branches/b/diff"),
            (Method::GET, "/api/branches/b/schema-findings"),
            (Method::POST, "/api/schemas/object"),
            (Method::POST, "/api/schemas/search"),
            (Method::GET, "/api/changes/b/timeline"),
            (Method::POST, "/api/changes/b/comments"),
            (Method::POST, "/api/changes/b/threads/t1"),
        ] {
            assert_eq!(
                need(&method, path, None),
                Need::Somewhere(Level::Viewer),
                "{method} {path}"
            );
        }
        for (method, path) in [
            (Method::POST, "/api/branches"),
            (Method::DELETE, "/api/branches/b"),
            (Method::POST, "/api/changes"),
            (Method::PATCH, "/api/changes/b"),
            (Method::POST, "/api/changes/b/close"),
            (Method::POST, "/api/changes/b/bring-current"),
        ] {
            assert_eq!(
                need(&method, path, None),
                Need::Somewhere(Level::Editor),
                "{method} {path}"
            );
        }
        for (method, path) in [
            (Method::POST, "/api/changes/b/reviews"),
            (Method::POST, "/api/changes/b/deploy"),
            (Method::POST, "/api/deploy"),
        ] {
            assert_eq!(
                need(&method, path, None),
                Need::Somewhere(Level::Deployer),
                "{method} {path}"
            );
        }
    }

    #[test]
    fn a_person_s_conversations_are_anyone_s_to_have() {
        assert_eq!(get("/api/agent/conversations", None), Need::Anyone);
        assert_eq!(get("/api/agent/conversations/c_1", None), Need::Anyone);
        assert_eq!(get("/api/agent/models", None), Need::Anyone);
        for (method, path) in [
            (Method::POST, "/api/agent/conversations"),
            (Method::DELETE, "/api/agent/conversations/c_1"),
            (Method::POST, "/api/agent/conversations/c_1/messages"),
            (Method::POST, "/api/agent/conversations/c_1/stop"),
            (Method::POST, "/api/agent/conversations/c_1/context"),
        ] {
            assert_eq!(need(&method, path, None), Need::Anyone, "{method} {path}");
        }
    }

    #[test]
    fn the_docs_are_anyone_s_to_read() {
        assert_eq!(get("/api/docs", Some("q=kafka")), Need::Anyone);
        assert_eq!(get("/api/docs/page", Some("path=/docs/sources/kafka")), Need::Anyone);
        assert_eq!(need(&Method::PUT, "/api/docs/page", None), Need::Administrator);
    }

    #[test]
    fn what_status_and_map_read_needs_viewer_somewhere() {
        for path in [
            "/api/cluster",
            "/api/warnings",
            "/api/flow",
            "/api/map",
            "/api/map/measurements",
            "/api/stats",
            "/api/worker-groups",
            "/api/system/info",
        ] {
            assert_eq!(get(path, None), Need::Somewhere(Level::Viewer), "{path}");
        }
    }

    #[test]
    fn a_need_is_met_by_what_it_names() {
        use std::collections::BTreeMap;
        let access = Access::Areas(BTreeMap::from([("payments".to_string(), Level::Editor)]));
        assert!(area("payments", Level::Editor).met_by(&access));
        assert!(!area("payments", Level::Deployer).met_by(&access));
        assert!(!area("search", Level::Viewer).met_by(&access));
        assert!(area("shared", Level::Viewer).met_by(&access));
        assert!(Need::Somewhere(Level::Editor).met_by(&access));
        assert!(!Need::Administrator.met_by(&access));
        assert!(Need::Administrator.met_by(&Access::Administrator));
        assert_eq!(
            area("payments", Level::Deployer).refusal(),
            "this needs deployer in payments"
        );
    }
}
