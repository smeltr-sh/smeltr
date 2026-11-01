//! The shape every JSON route answers in: `ok`, the errors when it is not, and the route's own fields beside them.
//! A page reads `ok` and `errors` the same way whatever it asked, and the editor puts an error on the step, row or
//! field it names.
//!
//! The status says whether the route succeeded, and `ok` repeats it: a 2xx answers `ok: true`, a 4xx or 5xx
//! `ok: false`, and no failure is answered at 200 (`docs/architecture/api.md`, The answer). On a 2xx, `errors` holds
//! warnings. An envelope is answered only as a response, never through `Json`, so its status cannot be dropped.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::config_file;
use crate::plan;

/// A route's answer: its status, what went wrong, and its own fields. The constructors tie the status to the
/// errors; a route that answers otherwise -- a listing that read most of what it could, a conflict that is its own
/// answer -- names its status with [`Envelope::with`]. `body` is flattened into the same object, so the route's own
/// fields sit beside `ok` and `errors`; a body with nothing to say is `Empty`.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<T = Empty> {
    pub status: StatusCode,
    pub errors: Vec<ApiError>,
    pub body: T,
}

/// A body with no fields of its own.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Empty;

impl<T> Envelope<T> {
    pub fn ok(body: T) -> Self {
        Envelope {
            status: StatusCode::OK,
            errors: Vec::new(),
            body,
        }
    }

    /// A body that stands at `status`, with `errors` beside it: a listing that read most of what it was asked for,
    /// a preview that names which stage failed, a conflict whose body is what conflicts.
    pub fn with(body: T, status: StatusCode, errors: Vec<ApiError>) -> Self {
        Envelope { status, errors, body }
    }

    /// Whether the route did what it was asked: what `ok` answers.
    pub fn succeeded(&self) -> bool {
        self.status.is_success()
    }

    /// The answer as its JSON object, as a response carries it.
    pub fn to_json(&self) -> serde_json::Value
    where
        T: serde::Serialize,
    {
        serde_json::to_value(Wire::of(self)).expect("an answer serializes")
    }
}

impl<T: Default> Envelope<T> {
    pub fn refused(refusal: impl Into<Refusal>) -> Self {
        let Refusal { status, errors } = refusal.into();
        Envelope::with(T::default(), status, errors)
    }
}

impl Envelope {
    pub fn done() -> Self {
        Envelope::ok(Empty)
    }
}

impl<T: Default> From<Refusal> for Envelope<T> {
    fn from(refusal: Refusal) -> Self {
        Envelope::refused(refusal)
    }
}

impl<T: Default, E: Into<Refusal>> From<Result<T, E>> for Envelope<T> {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(body) => Envelope::ok(body),
            Err(e) => Envelope::refused(e),
        }
    }
}

impl<T> std::ops::Deref for Envelope<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.body
    }
}

impl<T> std::ops::DerefMut for Envelope<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.body
    }
}

/// What an envelope is on the wire.
#[derive(serde::Serialize)]
struct Wire<'a, T> {
    ok: bool,
    errors: &'a [ApiError],
    #[serde(flatten)]
    body: &'a T,
}

impl<'a, T: serde::Serialize> Wire<'a, T> {
    fn of(envelope: &'a Envelope<T>) -> Self {
        // A body's own `ok` or `errors` would be written a second time, and a reader keeps the last.
        debug_assert!(
            serde_json::to_value(&envelope.body).map_or(true, |b| b.get("ok").is_none() && b.get("errors").is_none()),
            "an answer's body has a field of its own named ok or errors"
        );
        Wire {
            ok: envelope.succeeded(),
            errors: &envelope.errors,
            body: &envelope.body,
        }
    }
}

impl<T: serde::Serialize> IntoResponse for Envelope<T> {
    fn into_response(self) -> Response {
        let json = match serde_json::to_vec(&Wire::of(&self)) {
            Ok(json) => json,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("the answer did not serialize: {e}"),
                )
                    .into_response();
            }
        };
        let mut response = (self.status, [(header::CONTENT_TYPE, "application/json")], json).into_response();
        if !self.succeeded() {
            response.extensions_mut().insert(Reason(
                self.errors
                    .iter()
                    .map(|e| e.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        response
    }
}

/// What a refused answer said, carried on the response for the layer that logs every refusal
/// ([`log_refused`]), so it is not read back out of the body.
#[derive(Debug, Clone)]
pub struct Reason(pub String);

/// Logs a refused `/api/` call, so every refusal is in the log whether or not the handler said anything where it
/// failed: a 4xx at debug, since a caller's mistake is theirs and only worth reading when looking for it, and a 5xx
/// at error, since that one is the leader's.
///
/// A 5xx that repeats is one failure, not many: a store that is down fails every heartbeat and every poll, and a
/// line per call would fill the log it cannot write with the same sentence. So a call that fails the way one just
/// did (same method, path and status) is logged once a minute, with how many failed meanwhile under `repeats`.
/// What fails after the last line is counted and never said: a failure that stops stops being written about.
pub fn log_refused(method: &str, path: &str, status: StatusCode, error: &str) {
    log_refused_at(method, path, status, error, std::time::Instant::now());
}

/// How long a repeated 5xx is held before it is said again.
const FAILED_AGAIN: std::time::Duration = std::time::Duration::from_secs(60);

/// A failing call by its method, path and status, and what was last said of it: when, and how many have failed since.
type Failing = std::collections::HashMap<(String, String, u16), (std::time::Instant, u32)>;

static FAILED: std::sync::LazyLock<std::sync::Mutex<Failing>> = std::sync::LazyLock::new(Default::default);

/// Paths carry ids, so what failed is bounded by forgetting everything once this many are held.
const FAILED_HELD: usize = 1024;

fn log_refused_at(method: &str, path: &str, status: StatusCode, error: &str, now: std::time::Instant) {
    if status.is_server_error() {
        let mut failed = FAILED.lock().unwrap_or_else(|e| e.into_inner());
        let key = (method.to_string(), path.to_string(), status.as_u16());
        let repeats = match failed.get_mut(&key) {
            Some((said, count)) if now.duration_since(*said) < FAILED_AGAIN => {
                *count += 1;
                return;
            }
            Some((said, count)) => {
                let repeats = std::mem::take(count);
                *said = now;
                Some(repeats)
            }
            None => {
                if failed.len() >= FAILED_HELD {
                    failed.clear();
                }
                failed.insert(key, (now, 0));
                None
            }
        };
        drop(failed);
        tracing::error!(target: "api.failed", method, path, status = status.as_u16(), error, repeats, "an API call failed");
    } else if status.is_client_error() {
        tracing::debug!(target: "api.refused", method, path, status = status.as_u16(), error, "an API call was refused");
    }
}

/// A route refusing: the status and what went wrong. What a handler's `?` carries out, and what each error type
/// that a route can meet converts to, so the status is chosen once for each kind of failure.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    pub status: StatusCode,
    pub errors: Vec<ApiError>,
}

impl Refusal {
    pub fn new(status: StatusCode, error: impl Into<ApiError>) -> Self {
        Refusal::all(status, vec![error.into()])
    }

    pub fn all(status: StatusCode, errors: Vec<ApiError>) -> Self {
        debug_assert!(
            !status.is_success() && !errors.is_empty(),
            "a refusal fails and says why"
        );
        Refusal { status, errors }
    }

    /// 400: the request could not be read.
    pub fn bad_request(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::BAD_REQUEST, error)
    }

    /// 403: not allowed.
    pub fn forbidden(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::FORBIDDEN, error)
    }

    /// 404: no such thing.
    pub fn not_found(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::NOT_FOUND, error)
    }

    /// 409: the state refuses it.
    pub fn conflict(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::CONFLICT, error)
    }

    /// 422: understood, and refused by validation.
    pub fn invalid(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::UNPROCESSABLE_ENTITY, error)
    }

    /// 500: the leader's fault.
    pub fn internal(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::INTERNAL_SERVER_ERROR, error)
    }

    /// 503: try later.
    pub fn unavailable(error: impl Into<ApiError>) -> Self {
        Refusal::new(StatusCode::SERVICE_UNAVAILABLE, error)
    }

    /// What went wrong, as one line, for a caller that reports it rather than answering it.
    pub fn message(&self) -> String {
        self.errors
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        Envelope::<Empty>::refused(self).into_response()
    }
}

impl From<&config_file::ValidationError> for Refusal {
    fn from(e: &config_file::ValidationError) -> Self {
        Refusal::new(StatusCode::UNPROCESSABLE_ENTITY, ApiError::from_validation(e))
    }
}

/// One thing wrong, attributed to where it came from when that is known. `function` is the `FunctionId` the GUI's
/// step tree assigns in document order; `step` is that step's own id, stable across a `call` step's inlining where
/// `function` alone is not. `assignment_index` names the one eval assignment this came from, where the compiler or
/// the bundle's validation knows it, and `field` the setting, where validation names one.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub pipeline: Option<String>,
    pub function: Option<u32>,
    pub step: Option<String>,
    pub assignment_index: Option<u32>,
    /// The source, destination or identity this is about. Boxed, as an error rides in a `Result` on every read.
    pub item: Option<Box<config_file::ItemRef>>,
    /// The setting this is about, by its name in the file. See `config_file::ValidationError::field`.
    pub field: Option<&'static str>,
    /// The route's delivery rule, by its position, when `field` is one of its settings. See `CompileError::rule`.
    pub rule: Option<u32>,
    pub message: String,
}

impl ApiError {
    pub fn new(message: impl Into<String>) -> Self {
        ApiError {
            pipeline: None,
            function: None,
            step: None,
            assignment_index: None,
            item: None,
            field: None,
            rule: None,
            message: message.into(),
        }
    }

    /// From a bundle's own validation, keeping where it came from so the editor can put the message on the step,
    /// the row or the field that caused it.
    pub fn from_validation(e: &config_file::ValidationError) -> Self {
        ApiError {
            pipeline: e.pipeline.clone(),
            function: e.function,
            step: e.step.clone(),
            assignment_index: e.assignment_index,
            item: e.item.clone().map(Box::new),
            field: e.field,
            rule: e.rule,
            message: e.message.clone(),
        }
    }

    /// From a compiled pipeline's own `CompileError`, keeping which step it named the same way `from_validation`
    /// does, plus the assignment within it where the compiler tracked one.
    pub fn from_compile(e: &plan::config::CompileError, step: Option<String>) -> Self {
        ApiError {
            pipeline: None,
            function: e.function,
            step,
            assignment_index: e.assignment_index,
            item: None,
            field: None,
            rule: None,
            message: e.message.clone(),
        }
    }
}

impl From<String> for ApiError {
    fn from(message: String) -> Self {
        ApiError::new(message)
    }
}

impl From<&str> for ApiError {
    fn from(message: &str) -> Self {
        ApiError::new(message)
    }
}

/// What kind of failure an error is, which is the status a route answers it at. The leader's own error types carry
/// one, so the status is chosen where the failure is known and nowhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The leader could not do it: 500.
    Failed,
    /// The state refuses it: a name that exists, a ref another write moved first, a review in the wrong state: 409.
    Conflict,
    /// There is no such thing: 404.
    NotFound,
    /// What was asked for is not well formed: 422.
    Invalid,
    /// This caller may not, where another may: 403.
    Forbidden,
}

impl ErrorKind {
    pub fn status(self) -> StatusCode {
        match self {
            ErrorKind::Failed => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Conflict => StatusCode::CONFLICT,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Invalid => StatusCode::UNPROCESSABLE_ENTITY,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        }
    }
}

impl From<super::store::StoreError> for Refusal {
    fn from(e: super::store::StoreError) -> Self {
        Refusal::new(e.kind.status(), e.message)
    }
}

/// A conflict wants an operator to do something else first; an unreadable file wants nothing from them, and a client
/// watching this leader has to be able to tell the two apart.
impl From<super::tls::Refused> for Refusal {
    fn from(e: super::tls::Refused) -> Self {
        match e {
            super::tls::Refused::Conflict(message) => Refusal::conflict(message),
            super::tls::Refused::Unavailable(message) => Refusal::unavailable(message),
        }
    }
}

impl From<super::git::GitError> for Refusal {
    fn from(e: super::git::GitError) -> Self {
        Refusal::new(e.kind.status(), e.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_answer_carries_its_reason_and_a_good_one_does_not() {
        let refused = Refusal::conflict("the name exists").into_response();
        assert_eq!(
            refused.extensions().get::<Reason>().map(|s| s.0.as_str()),
            Some("the name exists")
        );
        let fine = Envelope::ok(Empty).into_response();
        assert!(fine.extensions().get::<Reason>().is_none());
    }

    #[test]
    fn a_failure_that_repeats_is_said_once_a_minute_with_its_count() {
        let t0 = std::time::Instant::now();
        let s = std::time::Duration::from_secs;
        let fail = |at: u64| {
            log_refused_at(
                "GET",
                "/api/repeat",
                StatusCode::SERVICE_UNAVAILABLE,
                "down",
                t0 + s(at),
            )
        };
        let seen = crate::logs::said(|| {
            fail(0);
            fail(1);
            fail(2);
            fail(61);
            fail(62);
        });
        let repeats: Vec<Option<String>> = seen
            .iter()
            .map(|(_, _, f)| f.iter().find(|(n, _)| n == "repeats").map(|(_, v)| v.clone()))
            .collect();
        assert_eq!(repeats, vec![None, Some("2".to_string())], "{seen:?}");
        // Another path is its own failure.
        let other = crate::logs::said(|| {
            log_refused_at("GET", "/api/other", StatusCode::SERVICE_UNAVAILABLE, "down", t0 + s(62));
        });
        assert_eq!(other.len(), 1, "{other:?}");
    }

    #[test]
    fn a_refusal_is_logged_at_debug_and_a_failure_at_error() {
        let seen = crate::logs::said(|| {
            log_refused("POST", "/api/branches/x/deploy", StatusCode::CONFLICT, "a stale base");
            log_refused(
                "GET",
                "/api/logs-once",
                StatusCode::SERVICE_UNAVAILABLE,
                "the store is unavailable",
            );
            log_refused("GET", "/api/logs-once", StatusCode::OK, "");
        });
        assert_eq!(seen.len(), 2, "{seen:?}");
        let field = |i: usize, k: &str| seen[i].2.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
        assert_eq!((seen[0].0, seen[0].1.as_str()), (tracing::Level::DEBUG, "api.refused"));
        assert_eq!(field(0, "status").as_deref(), Some("409"));
        assert_eq!(field(0, "error").as_deref(), Some("a stale base"));
        assert_eq!(field(0, "message").as_deref(), Some("an API call was refused"));
        assert_eq!((seen[1].0, seen[1].1.as_str()), (tracing::Level::ERROR, "api.failed"));
        assert_eq!(field(1, "path").as_deref(), Some("/api/logs-once"));
        assert_eq!(field(1, "message").as_deref(), Some("an API call failed"));
    }

    #[test]
    fn body_fields_sit_beside_ok_and_errors() {
        #[derive(Default, serde::Serialize)]
        struct Body {
            commit: Option<String>,
        }
        let json = Envelope::ok(Body {
            commit: Some("abc".into()),
        })
        .to_json();
        assert_eq!(json, serde_json::json!({"ok": true, "errors": [], "commit": "abc"}));
        let json = Envelope::<Body>::refused(Refusal::invalid("no")).to_json();
        assert_eq!(json["ok"], false);
        assert_eq!(json["errors"][0]["message"], "no");
        assert_eq!(json["commit"], serde_json::Value::Null);
        assert_eq!(
            Envelope::done().to_json(),
            serde_json::json!({"ok": true, "errors": []})
        );
    }

    /// `ok` repeats the status, and a failure is never answered at 200.
    #[test]
    fn ok_is_the_status() {
        assert!(Envelope::with(Empty, StatusCode::OK, vec![ApiError::new("a warning")]).to_json()["ok"] == true);
        let refused = Envelope::<Empty>::refused(Refusal::conflict("taken"));
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert_eq!(refused.to_json()["ok"], false);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "named ok or errors")]
    fn a_body_has_no_ok_of_its_own() {
        Envelope::ok(serde_json::json!({"ok": false})).to_json();
    }

    #[test]
    fn a_failure_answers_its_status() {
        let response = Refusal::not_found("gone").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(Envelope::done().into_response().status(), StatusCode::OK);
    }

    #[test]
    fn each_error_kind_answers_its_status() {
        use crate::leader::{git::GitError, store::StoreError};
        let status = |r: Refusal| r.status;
        assert_eq!(
            status(StoreError::failed("disk").into()),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(status(StoreError::conflict("taken").into()), StatusCode::CONFLICT);
        assert_eq!(status(StoreError::not_found("gone").into()), StatusCode::NOT_FOUND);
        assert_eq!(
            status(GitError::failed("bad object").into()),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(status(GitError::conflict("main moved").into()), StatusCode::CONFLICT);
        assert_eq!(status(GitError::not_found("no branch").into()), StatusCode::NOT_FOUND);
        assert_eq!(
            status(GitError::invalid("no title").into()),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status(GitError::of(ErrorKind::Forbidden, "your own").into()),
            StatusCode::FORBIDDEN
        );
        use crate::leader::tls::Refused;
        assert_eq!(
            status(Refused::Conflict("rotating".into()).into()),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(Refused::Unavailable("unreadable".into()).into()),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let invalid = config_file::ValidationError {
            field: Some("topic"),
            ..config_file::ValidationError::from("no topic".to_string())
        };
        let refused = Refusal::from(&invalid);
        assert_eq!(refused.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(refused.errors, vec![ApiError::from_validation(&invalid)]);
    }
}
