//! Elasticsearch as a protocol over the HTTP destination core: documents shipped through the Bulk API.
//!
//! The Bulk API returns `200` on partial failure, with the failures per item in the body. A retryable item (`429`,
//! `es_rejected_execution_exception`, a circuit breaker) is re-sent alone; a permanent one (a mapping conflict, a
//! malformed document) is a rejection.

use super::http::{HttpTransport, Outcome, Protocol, Response, Unwritten, push_json_string};
use super::{IoError, OwnedRecord};
use crate::format::json_scan::{Malformed, Next, Reader};
use crate::plan::topology::DocIdPolicy;

/// One document waiting to be indexed, kept until the cluster confirms it. The bulk body is rebuilt from these on every
/// attempt, because a retry must contain only the documents that failed.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingDoc {
    pub index: String,
    /// The document's `_id`, when the policy asks for one. `None` lets Elasticsearch generate one.
    pub id: Option<String>,
    pub body: Vec<u8>,
}

/// Builds the NDJSON body for `docs`: an action line and a document line each, newline-terminated.
///
/// Every `body` here holds no newline of its own -- `Elastic::admit` refuses one that does, since a line ending early
/// would pair every action after it with the wrong document.
///
/// `Dedupe` writes `create`, so a re-send of an id already present comes back as a `version_conflict_engine_exception`
/// (counted as success by `classify_response`) instead of a silent overwrite. `Overwrite` writes `index`. `Auto` writes
/// `create` too: with no id there is nothing to conflict with, and `create` is the action a data stream accepts.
pub fn bulk_body(docs: &[PendingDoc], policy: DocIdPolicy) -> Vec<u8> {
    let action: &[u8] = match policy {
        DocIdPolicy::Auto | DocIdPolicy::Dedupe => br#"{"create":{"_index":"#,
        DocIdPolicy::Overwrite => br#"{"index":{"_index":"#,
    };
    let mut out = Vec::new();
    for d in docs {
        out.extend_from_slice(action);
        push_json_string(&mut out, &d.index);
        if let Some(id) = &d.id {
            out.extend_from_slice(br#","_id":"#);
            push_json_string(&mut out, id);
        }
        out.extend_from_slice(b"}}\n");
        out.extend_from_slice(&d.body);
        out.push(b'\n');
    }
    out
}

/// Reads one bulk response body and says what to do with each document. An item that cannot be found or parsed is
/// permanent rather than assumed fine.
pub fn classify_response(body: &[u8], count: usize, policy: DocIdPolicy) -> Result<Outcome, IoError> {
    classify(body, count, policy).map(|(outcome, _)| outcome)
}

/// [`classify_response`]'s outcome, and how many documents a data stream's failure store took: each answered as
/// written, though not to the index it was sent to.
fn classify(body: &[u8], count: usize, policy: DocIdPolicy) -> Result<(Outcome, usize), IoError> {
    let Bulk { errors, items } = bulk(body).map_err(|_| IoError::transient("bulk response was not JSON"))?;
    let failure_stored = items.iter().flatten().flatten().filter(|a| a.failure_stored).count();

    if errors == Some(false) {
        return Ok((Outcome::default(), failure_stored));
    }

    let items = items.ok_or_else(|| IoError::transient("bulk response has no `items` array"))?;
    if items.len() != count {
        return Err(IoError::transient(format!(
            "bulk response answered {} of {count} documents",
            items.len()
        )));
    }

    let mut out = Outcome::default();
    for (i, item) in items.iter().enumerate() {
        let Some(answered) = item else {
            out.permanent.push((i, "unreadable bulk item".into()));
            continue;
        };
        let (status, error_type) = (answered.status, answered.error_type.as_deref());
        let reason = answered.reason.as_deref().unwrap_or("no reason given");

        match (status, error_type) {
            (200..=299, _) => {}
            // Under `Dedupe` a conflict is the at-least-once re-send arriving twice: the outcome the policy exists to
            // produce. Under any other policy nothing smeltr sent can conflict, so a 409 means something else is
            // versioning the document.
            (409, Some("version_conflict_engine_exception")) if policy == DocIdPolicy::Dedupe => {}
            (429, _) | (503, _) => out.retryable.push(i),
            (_, Some(t))
                if t == "es_rejected_execution_exception"
                    || t == "circuit_breaking_exception"
                    || t == "cluster_block_exception" =>
            {
                out.retryable.push(i)
            }
            (_, Some(t)) => out.permanent.push((i, format!("{t}: {reason}"))),
            (s, None) => out.permanent.push((i, format!("status {s}: {reason}"))),
        }
    }
    // Every retryable item is the cluster saying it is busy.
    out.throttled = !out.retryable.is_empty();
    Ok((out, failure_stored))
}

/// What a bulk answer says of one document.
#[derive(Default)]
struct Answered {
    /// 0 where the answer gives none.
    status: u64,
    error_type: Option<String>,
    reason: Option<String>,
    /// A data stream's failure store took the document.
    failure_stored: bool,
}

/// What a bulk answer says of the whole request.
struct Bulk {
    /// Its `errors`, where it is true or false.
    errors: Option<bool>,
    /// Its `items`, where it is a list: each item's answer, or `None` for one that is no object holding one.
    items: Option<Vec<Option<Answered>>>,
}

/// Reads a bulk answer.
fn bulk(body: &[u8]) -> Result<Bulk, Malformed> {
    let mut r = Reader::new(body)?;
    let top = r.value()?;
    if top != Next::Object {
        return Err(Malformed);
    }
    let (mut errors, mut items) = (None, None);
    while let Some(key) = r.key()? {
        match &*key {
            "errors" => {
                let next = r.value()?;
                errors = match next {
                    Next::Bool(b) => Some(b),
                    _ => None,
                };
                close(&mut r, &next)?;
            }
            "items" => {
                let next = r.value()?;
                items = None;
                if next != Next::Array {
                    close(&mut r, &next)?;
                    continue;
                }
                let mut answers = Vec::new();
                while r.item()? {
                    answers.push(item(&mut r)?);
                }
                items = Some(answers);
            }
            _ => r.skip()?,
        }
    }
    match r.end() {
        true => Ok(Bulk { errors, items }),
        false => Err(Malformed),
    }
}

/// One bulk item, `{"<action>": {...}}`: the answer of its first action, whatever the action is named.
fn item(r: &mut Reader) -> Result<Option<Answered>, Malformed> {
    let next = r.value()?;
    if next != Next::Object {
        close(r, &next)?;
        return Ok(None);
    }
    if r.key()?.is_none() {
        return Ok(None);
    }
    let answered = answer(r)?;
    while r.key()?.is_some() {
        r.skip()?;
    }
    Ok(Some(answered))
}

/// One action's answer. One that is no object answers nothing, as an answer of no status does.
fn answer(r: &mut Reader) -> Result<Answered, Malformed> {
    let mut answered = Answered::default();
    let next = r.value()?;
    if next != Next::Object {
        close(r, &next)?;
        return Ok(answered);
    }
    while let Some(key) = r.key()? {
        let next = r.value()?;
        match (&*key, &next) {
            ("status", Next::Number(n)) => answered.status = n.parse().unwrap_or(0),
            ("failure_store", Next::Str(s)) => answered.failure_stored = s == "used",
            ("error", Next::Object) => {
                while let Some(key) = r.key()? {
                    let next = r.value()?;
                    match (&*key, next) {
                        ("type", Next::Str(t)) => answered.error_type = Some(t.into_owned()),
                        ("reason", Next::Str(t)) => answered.reason = Some(t.into_owned()),
                        (_, next) => close(r, &next)?,
                    }
                }
                continue;
            }
            _ => {}
        }
        close(r, &next)?;
    }
    Ok(answered)
}

/// The rest of the container `next` opened, skipped.
fn close(r: &mut Reader, next: &Next) -> Result<(), Malformed> {
    match next {
        Next::Object => {
            while r.key()?.is_some() {
                r.skip()?;
            }
        }
        Next::Array => {
            while r.item()? {
                r.skip()?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// The Bulk API's request and answer. `default_index` takes a row with no `__destination.index`.
#[derive(Clone, Debug)]
pub struct Elastic {
    pub default_index: String,
    /// `Auto` by default: plain at-least-once, at no cost.
    pub doc_id: DocIdPolicy,
    /// Counts the documents a failure store took, where the destination reports on itself.
    unwritten: Option<Unwritten>,
}

impl Elastic {
    pub fn new(default_index: impl Into<String>) -> Self {
        Elastic {
            default_index: default_index.into(),
            doc_id: DocIdPolicy::default(),
            unwritten: None,
        }
    }

    pub fn with_unwritten(mut self, unwritten: Unwritten) -> Self {
        self.unwritten = Some(unwritten);
        self
    }

    pub fn with_doc_id_policy(mut self, policy: DocIdPolicy) -> Self {
        self.doc_id = policy;
        self
    }
}

impl Protocol for Elastic {
    type Item = PendingDoc;

    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    /// Only what `classify_response` reads. The full answer lists every item at length, and one past ureq's read limit
    /// would fail the request after the cluster had indexed it.
    fn path(&self) -> &str {
        "/_bulk?filter_path=errors,items.*.status,items.*.error.type,items.*.error.reason,items.*.failure_store"
    }

    /// The placement is the index name.
    fn admit(&self, row: &OwnedRecord) -> Result<PendingDoc, String> {
        // A bulk request is newline-delimited, so a document holding one would end the line early and every
        // action/document pair after it in the request would be read against the wrong partner. Refused rather than
        // escaped: the body is the document Elasticsearch indexes, and nothing may rewrite it. A document that is
        // merely not JSON needs no check here -- Elasticsearch reports that one per item, which arrives as an
        // ordinary rejection.
        if row.value.contains(&b'\n') {
            return Err(
                "record contains a newline; Elasticsearch's bulk API is newline-delimited and cannot carry one \
                        inside a document"
                    .into(),
            );
        }
        Ok(PendingDoc {
            index: row.placement.clone().unwrap_or_else(|| self.default_index.clone()),
            // Only when this destination asked for one: under `Auto` a row's `__destination.id` is ignored, since a pipeline may
            // set it for a different destination on the same route. A null `__destination.id` under a policy that wants one
            // falls back to a generated id for that row; `runtime` counts it.
            id: self.doc_id.wants_doc_id().then(|| row.doc_id.clone()).flatten(),
            body: row.value.clone(),
        })
    }

    fn size(&self, doc: &PendingDoc) -> usize {
        doc.body.len()
    }

    fn body(&self, docs: &[PendingDoc]) -> Vec<u8> {
        bulk_body(docs, self.doc_id)
    }

    /// Per-document failures live inside a 200 body. A 400 (a request the cluster could not read) or a 413 (one larger
    /// than it accepts) refuses the request whole, so it is split to find the documents at fault. Any other status
    /// fails the request, to be sent again: the cluster is busy or down, or the credential is wrong.
    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError> {
        let count = items.len();
        match response.status {
            200..300 => {
                let (outcome, failure_stored) = classify(&response.body, count, self.doc_id)?;
                if let Some(unwritten) = self.unwritten.as_ref().filter(|_| failure_stored > 0) {
                    let why = format!("{failure_stored} document(s) went to their data stream's failure store");
                    unwritten.note("document", failure_stored as u64, &why);
                }
                Ok(outcome)
            }
            400 | 413 => Ok(Outcome {
                split: Some(response.describe()),
                ..Outcome::default()
            }),
            // The cluster is busy as a whole: every document goes again after a wait, as a busy item does.
            429 | 503 => Ok(Outcome {
                retryable: (0..count).collect(),
                throttled: true,
                ..Outcome::default()
            }),
            _ => Err(IoError::transient(format!(
                "elasticsearch bulk returned {}",
                response.describe()
            ))),
        }
    }
}

/// The `Authorization` header value the destination's credential stands for: the credential as it is when it names
/// its scheme (`ApiKey <base64>`, `Basic <base64>`), and `ApiKey <credential>` when it is the bare key Elasticsearch
/// hands out.
pub fn authorization(credential: String) -> String {
    if credential.contains(' ') {
        credential
    } else {
        format!("ApiKey {credential}")
    }
}

/// A transport to the cluster at `url`, e.g. `https://es.internal:9200`. `auth` is a complete `Authorization` header
/// value (`authorization`), or `None` for a cluster with security disabled.
pub fn transport(url: &str, auth: Option<String>, config: &ureq::config::Config) -> HttpTransport {
    let headers = auth.map(|a| ("Authorization".to_string(), a)).into_iter().collect();
    // The Bulk API rejects `application/json`.
    HttpTransport::new(url, "application/x-ndjson", headers, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_key_is_sent_as_an_api_key_and_a_named_scheme_as_it_is() {
        assert_eq!(
            authorization("VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw==".into()),
            "ApiKey VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
        );
        assert_eq!(authorization("ApiKey abc".into()), "ApiKey abc");
        assert_eq!(authorization("Basic dXNlcjpwYXNz".into()), "Basic dXNlcjpwYXNz");
    }
}
