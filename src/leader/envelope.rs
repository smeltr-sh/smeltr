//! The shape every JSON route answers in: `ok`, the errors when it is not, and the route's own fields beside them.
//! A page reads `ok` and `errors` the same way whatever it asked, and the editor puts an error on the step, row or
//! field it names.

use crate::config_file;
use crate::plan;

/// `ok` says whether the route did what it was asked; `errors` says what went wrong. The constructors here tie the
/// two together, and a route that answers otherwise -- a listing that read most of what it could, a conflict that
/// is its own answer -- builds the envelope itself and says why. `body` is flattened into the same object, so the
/// route's own fields sit beside `ok` and `errors`; a body with nothing to say is `Empty`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Envelope<T = Empty> {
    pub ok: bool,
    pub errors: Vec<ApiError>,
    #[serde(flatten)]
    pub body: T,
}

/// A body with no fields of its own.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Empty;

impl<T> Envelope<T> {
    pub fn ok(body: T) -> Self {
        Envelope {
            ok: true,
            errors: Vec::new(),
            body,
        }
    }

    /// A body that stands whether or not there are errors: a listing that read most of what it was asked for, a
    /// preview that names which stage failed.
    pub fn with(body: T, errors: Vec<ApiError>) -> Self {
        Envelope {
            ok: errors.is_empty(),
            errors,
            body,
        }
    }
}

impl<T: Default> Envelope<T> {
    pub fn failed(errors: Vec<ApiError>) -> Self {
        debug_assert!(!errors.is_empty(), "a failure says what went wrong");
        Envelope {
            ok: false,
            errors,
            body: T::default(),
        }
    }

    pub fn failed_one(error: impl Into<ApiError>) -> Self {
        Self::failed(vec![error.into()])
    }
}

impl Envelope {
    pub fn done() -> Self {
        Envelope::ok(Empty)
    }
}

impl<T: Default, E: Into<ApiError>> From<Result<T, E>> for Envelope<T> {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(body) => Envelope::ok(body),
            Err(e) => Envelope::failed(vec![e.into()]),
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

impl From<super::store::StoreError> for ApiError {
    fn from(e: super::store::StoreError) -> Self {
        ApiError::new(String::from(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_fields_sit_beside_ok_and_errors() {
        #[derive(Default, serde::Serialize)]
        struct Body {
            commit: Option<String>,
        }
        let json = serde_json::to_value(Envelope::ok(Body {
            commit: Some("abc".into()),
        }))
        .unwrap();
        assert_eq!(json, serde_json::json!({"ok": true, "errors": [], "commit": "abc"}));
        let json = serde_json::to_value(Envelope::<Body>::failed_one("no")).unwrap();
        assert_eq!(json["ok"], false);
        assert_eq!(json["errors"][0]["message"], "no");
        assert_eq!(json["commit"], serde_json::Value::Null);
        assert_eq!(
            serde_json::to_value(Envelope::done()).unwrap(),
            serde_json::json!({"ok": true, "errors": []})
        );
    }

    #[test]
    fn with_is_ok_exactly_when_nothing_is_wrong() {
        assert!(Envelope::with(Empty, vec![]).ok);
        assert!(!Envelope::with(Empty, vec![ApiError::new("x")]).ok);
    }
}
