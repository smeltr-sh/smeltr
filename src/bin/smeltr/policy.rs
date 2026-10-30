//! What each operator route needs a person to hold, decided from the request line alone.
//!
//! An area is the `:area` of `/api/areas/:area/...`, so the gate judges a request in one before any handler parses
//! it. A route not listed here needs an administrator: a route added without a decision about who may use it is
//! closed until somebody makes one.
//!
//! Some routes cannot be judged from the request line, because the area is in the body, a branch or a capture. The
//! gate asks [`Need::Somewhere`] of those, and the handler checks the rest once it knows the area (`start_capture` and
//! the captures it opened, `review_change`, `deploy_change`, `deploy_commit`, and a change's own tests).

use axum::http::Method;

use smeltr::config_file::{GROUPS_DIR, SHARED_DIR, is_safe_group_id};
use smeltr::leader::access::{Access, Level};
use smeltr::leader::envelope::Refusal;

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

/// A need the caller does not meet, as the 403 a route answers.
impl From<Need> for Refusal {
    fn from(need: Need) -> Self {
        Refusal::forbidden(need.refusal())
    }
}

/// What `method` on `path` needs.
pub fn need(method: &Method, path: &str) -> Need {
    let Some(rest) = path.strip_prefix("/api/") else {
        return Need::Administrator;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    if let ["areas", area, within @ ..] = segments.as_slice() {
        return match in_area(method, within) {
            Some(level) if is_area(area) => Need::Area {
                area: area.to_string(),
                level,
            },
            _ => Need::Administrator,
        };
    }

    match (method.clone(), segments.as_slice()) {
        // A schema read from or written as JSON Schema: what the schema it is given holds, never config.
        (Method::POST, ["preview", "from-json-schema" | "schema-export"]) => Need::Somewhere(Level::Viewer),
        // Production records a preview or a capture shows, in no one area.
        (Method::POST, ["captures"])
        | (Method::GET | Method::HEAD | Method::DELETE, ["captures", _])
        | (Method::POST, ["captures", _, "stop"])
        | (Method::POST, ["preview", _])
        // Names, versions and where each is used, never a value: what a secret picker offers whoever saves a form.
        | (Method::GET | Method::HEAD, ["secrets"])
        | (Method::GET | Method::HEAD, ["certificates"]) => Need::Somewhere(Level::Editor),

        // Branches and changes. Which areas a change touches is in the branch, so reviewing and deploying one, and
        // reading and running its own tests, is checked again by its handler.
        (Method::GET | Method::HEAD, ["branches"])
        | (Method::GET | Method::HEAD, ["main", "history" | "diff"])
        | (Method::GET | Method::HEAD, ["branches", _, "diff"])
        | (Method::GET | Method::HEAD, ["branches", _, "schema-findings"])
        | (Method::GET | Method::HEAD, ["branches", _, "tests"])
        | (Method::GET | Method::HEAD, ["branches", _, "timeline"])
        | (Method::POST, ["branches", _, "comments"])
        | (Method::POST, ["branches", _, "threads", _]) => Need::Somewhere(Level::Viewer),
        (Method::POST, ["branches"])
        | (Method::DELETE, ["branches", _])
        | (Method::PATCH, ["branches", _])
        | (Method::POST, ["branches", _, "test-runs" | "open" | "close" | "bring-current"]) => {
            Need::Somewhere(Level::Editor)
        }
        (Method::POST, ["branches", _, "reviews" | "deploy"]) | (Method::POST, ["main", "deploy"]) => {
            Need::Somewhere(Level::Deployer)
        }

        // What the Status and Map pages read, the area picker, and what a source or destination form asks of
        // the build. The streams are filtered by their handler to the groups the caller views.
        (Method::GET | Method::HEAD, ["cluster"])
        | (Method::GET | Method::HEAD, ["streams"])
        | (Method::GET | Method::HEAD, ["warnings"])
        | (Method::GET | Method::HEAD, ["flow"])
        | (Method::GET | Method::HEAD, ["map"])
        | (Method::GET | Method::HEAD, ["map", "measurements"])
        | (Method::GET | Method::HEAD, ["worker-groups"])
        | (Method::GET | Method::HEAD, ["system"]) => Need::Somewhere(Level::Viewer),

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

/// What `method` on `within`, a path inside one area, needs in that area. `None` for a path no area holds.
fn in_area(method: &Method, within: &[&str]) -> Option<Level> {
    let get = *method == Method::GET || *method == Method::HEAD;
    let read = if get { Level::Viewer } else { Level::Editor };
    Some(match (method.clone(), within) {
        // The configuration.
        (
            Method::GET | Method::HEAD,
            []
            | ["unreadable"]
            | ["pipelines"]
            | ["pipeline-callers"]
            | ["tests"]
            | ["stats"]
            | ["lookups", "file" | "mmdb"]
            | [_, _, "connections"],
        ) => Level::Viewer,
        (
            Method::GET | Method::HEAD | Method::PUT | Method::DELETE,
            ["pipelines", _] | ["tests", _] | ["files", _, ..],
        ) => read,
        (
            Method::PUT,
            ["sources" | "destinations" | "routes" | "lookups" | "grok-patterns" | "schemas" | "manifest"],
        )
        | (Method::POST, ["undo"] | ["lookups", "upload"] | ["test-runs"]) => Level::Editor,

        // Production records: samples, and what a preview of them shows.
        (Method::GET | Method::HEAD | Method::POST, ["samples"])
        | (Method::GET | Method::HEAD | Method::PUT | Method::DELETE, ["samples", _])
        | (Method::POST, ["preview", "pipeline" | "routes" | "schema-import"]) => Level::Editor,
        _ => return None,
    })
}

/// Whether `area` names one: `shared`, or a group id. Neither needs escaping in a path, so an escaped one is no
/// area, and the gate never judges a name the handler would decode to another.
fn is_area(area: &str) -> bool {
    area == SHARED_DIR || is_safe_group_id(area)
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

    fn get(path: &str) -> Need {
        need(&Method::GET, path)
    }

    #[test]
    fn an_unmet_need_is_refused_as_forbidden() {
        let refused = Refusal::from(area("payments", Level::Editor));
        assert_eq!(refused.status, axum::http::StatusCode::FORBIDDEN);
        assert_eq!(refused.message(), "this needs editor in payments");
    }

    #[test]
    fn a_route_nobody_listed_needs_an_administrator() {
        assert_eq!(get("/api/something-new"), Need::Administrator);
        assert_eq!(need(&Method::POST, "/api/areas/payments"), Need::Administrator);
        assert_eq!(get("/api/areas/payments/"), Need::Administrator);
        assert_eq!(get("/api/areas/payments/something-new"), Need::Administrator);
        assert_eq!(get("/api/areas"), Need::Administrator);
        assert_eq!(get("/elsewhere"), Need::Administrator);
    }

    #[test]
    fn administration_needs_an_administrator() {
        for (method, path) in [
            (Method::GET, "/api/accounts"),
            (Method::POST, "/api/accounts"),
            (Method::PATCH, "/api/accounts/u_1"),
            (Method::GET, "/api/roles"),
            (Method::PUT, "/api/roles/smeltr-payments"),
            (Method::DELETE, "/api/roles/smeltr-payments"),
            (Method::GET, "/api/security-events"),
            (Method::GET, "/api/settings/sessions"),
            (Method::PUT, "/api/settings/oidc"),
            (Method::GET, "/api/settings/agent"),
            (Method::PUT, "/api/settings/agent"),
            (Method::GET, "/api/settings/tls"),
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
            assert_eq!(need(&method, path), Need::Administrator, "{method} {path}");
        }
    }

    #[test]
    fn a_config_read_needs_viewer_in_its_area() {
        for path in [
            "/api/areas/payments",
            "/api/areas/payments/unreadable",
            "/api/areas/payments/pipelines",
            "/api/areas/payments/pipelines/p1",
            "/api/areas/payments/pipeline-callers",
            "/api/areas/payments/sources/s1/connections",
            "/api/areas/payments/tests",
            "/api/areas/payments/tests/t1",
            "/api/areas/payments/stats",
            "/api/areas/payments/lookups/file",
            "/api/areas/payments/lookups/mmdb",
            "/api/areas/payments/files/sources.json",
        ] {
            assert_eq!(get(path), area("payments", Level::Viewer), "{path}");
        }
    }

    #[test]
    fn a_config_write_needs_editor_in_its_area() {
        for (method, path) in [
            (Method::PUT, "/api/areas/payments/sources"),
            (Method::PUT, "/api/areas/payments/destinations"),
            (Method::PUT, "/api/areas/payments/routes"),
            (Method::PUT, "/api/areas/payments/manifest"),
            (Method::PUT, "/api/areas/payments/lookups"),
            (Method::PUT, "/api/areas/payments/grok-patterns"),
            (Method::PUT, "/api/areas/payments/pipelines/p1"),
            (Method::DELETE, "/api/areas/payments/pipelines/p1"),
            (Method::PUT, "/api/areas/payments/tests/t1"),
            (Method::DELETE, "/api/areas/payments/tests/t1"),
            (Method::POST, "/api/areas/payments/undo"),
            (Method::POST, "/api/areas/payments/lookups/upload"),
            (Method::PUT, "/api/areas/payments/files/pipelines/x.json"),
            (Method::DELETE, "/api/areas/payments/files/x.json"),
            (Method::PUT, "/api/areas/payments/schemas"),
        ] {
            assert_eq!(need(&method, path), area("payments", Level::Editor), "{method} {path}");
        }
    }

    #[test]
    fn records_need_editor_in_their_area() {
        for (method, path) in [
            (Method::GET, "/api/areas/payments/samples"),
            (Method::POST, "/api/areas/payments/samples"),
            (Method::GET, "/api/areas/payments/samples/s1"),
            (Method::PUT, "/api/areas/payments/samples/s1"),
            (Method::DELETE, "/api/areas/payments/samples/s1"),
            (Method::POST, "/api/areas/payments/test-runs"),
            (Method::POST, "/api/areas/payments/preview/pipeline"),
            (Method::POST, "/api/areas/payments/preview/routes"),
            (Method::POST, "/api/areas/payments/preview/schema-import"),
        ] {
            assert_eq!(need(&method, path), area("payments", Level::Editor), "{method} {path}");
        }
        for (method, path) in [
            (Method::POST, "/api/captures"),
            (Method::GET, "/api/captures/c1"),
            (Method::POST, "/api/captures/c1/stop"),
            (Method::DELETE, "/api/captures/c1"),
            (Method::POST, "/api/preview/grok"),
            (Method::POST, "/api/preview/onboard"),
            (Method::POST, "/api/preview/line-breaker"),
            (Method::POST, "/api/preview/format"),
            (Method::POST, "/api/preview/certificate"),
            (Method::POST, "/api/preview/sql"),
            (Method::GET, "/api/secrets"),
            (Method::GET, "/api/certificates"),
        ] {
            assert_eq!(need(&method, path), Need::Somewhere(Level::Editor), "{method} {path}");
        }
    }

    /// The areas a change touches are in its branch, so its handler judges them.
    #[test]
    fn a_change_s_own_tests_are_judged_by_their_handler() {
        assert_eq!(get("/api/branches/b/tests"), Need::Somewhere(Level::Viewer));
        assert_eq!(
            need(&Method::POST, "/api/branches/b/test-runs"),
            Need::Somewhere(Level::Editor)
        );
    }

    #[test]
    fn shared_is_an_area() {
        assert_eq!(get("/api/areas/shared/pipelines"), area("shared", Level::Viewer));
    }

    /// An area needs no escaping, so an escaped one is judged as no area rather than as the name it decodes to.
    #[test]
    fn an_escaped_or_unsafe_area_is_no_area() {
        for path in [
            "/api/areas/pay%6Dents",
            "/api/areas/..",
            "/api/areas/.hidden/pipelines",
            "/api/areas/a+b",
            "/api/areas//pipelines",
        ] {
            assert_eq!(get(path), Need::Administrator, "{path}");
        }
    }

    #[test]
    fn branches_and_changes() {
        for (method, path) in [
            (Method::GET, "/api/branches"),
            (Method::GET, "/api/main/history"),
            (Method::GET, "/api/main/diff"),
            (Method::GET, "/api/branches/b/diff"),
            (Method::GET, "/api/branches/b/schema-findings"),
            (Method::POST, "/api/preview/from-json-schema"),
            (Method::POST, "/api/preview/schema-export"),
            (Method::GET, "/api/branches/b/timeline"),
            (Method::POST, "/api/branches/b/comments"),
            (Method::POST, "/api/branches/b/threads/t1"),
        ] {
            assert_eq!(need(&method, path), Need::Somewhere(Level::Viewer), "{method} {path}");
        }
        for (method, path) in [
            (Method::POST, "/api/branches"),
            (Method::DELETE, "/api/branches/b"),
            (Method::POST, "/api/branches/b/open"),
            (Method::PATCH, "/api/branches/b"),
            (Method::POST, "/api/branches/b/close"),
            (Method::POST, "/api/branches/b/bring-current"),
        ] {
            assert_eq!(need(&method, path), Need::Somewhere(Level::Editor), "{method} {path}");
        }
        for (method, path) in [
            (Method::POST, "/api/branches/b/reviews"),
            (Method::POST, "/api/branches/b/deploy"),
            (Method::POST, "/api/main/deploy"),
        ] {
            assert_eq!(need(&method, path), Need::Somewhere(Level::Deployer), "{method} {path}");
        }
    }

    #[test]
    fn a_person_s_conversations_are_anyone_s_to_have() {
        assert_eq!(get("/api/agent/conversations"), Need::Anyone);
        assert_eq!(get("/api/agent/conversations/c_1"), Need::Anyone);
        assert_eq!(get("/api/agent/models"), Need::Anyone);
        for (method, path) in [
            (Method::POST, "/api/agent/conversations"),
            (Method::DELETE, "/api/agent/conversations/c_1"),
            (Method::POST, "/api/agent/conversations/c_1/messages"),
            (Method::POST, "/api/agent/conversations/c_1/stop"),
            (Method::POST, "/api/agent/conversations/c_1/context"),
        ] {
            assert_eq!(need(&method, path), Need::Anyone, "{method} {path}");
        }
    }

    #[test]
    fn the_docs_are_anyone_s_to_read() {
        assert_eq!(get("/api/docs"), Need::Anyone);
        assert_eq!(get("/api/docs/page"), Need::Anyone);
        assert_eq!(need(&Method::PUT, "/api/docs/page"), Need::Administrator);
    }

    #[test]
    fn what_status_and_map_read_needs_viewer_somewhere() {
        for path in [
            "/api/cluster",
            "/api/warnings",
            "/api/streams",
            "/api/flow",
            "/api/map",
            "/api/map/measurements",
            "/api/worker-groups",
            "/api/system",
        ] {
            assert_eq!(get(path), Need::Somewhere(Level::Viewer), "{path}");
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
