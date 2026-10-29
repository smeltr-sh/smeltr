//! The Elasticsearch destination (`io::elastic`).
//!
//! Weighted deliberately toward response *handling* rather than transport,
//! because that is where this destination can lose data silently: the Bulk
//! API answers `200 OK` even when half the documents were rejected, so a
//! destination that trusts the status line flushes clean, the runtime
//! commits the offsets, and the rejected documents are gone with nothing
//! recorded anywhere. None of that needs a live cluster to test — a canned
//! response body exercises every branch of it exactly.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use smeltr::io::elastic::{Elastic, PendingDoc, bulk_body, classify_response};
use smeltr::io::http::{HttpDestination, Outcome, Protocol, Response, Transport};
use smeltr::io::{Destination, Dispatched, IoError, OutgoingRecord, OwnedRecord, Rejection};
use smeltr::plan::topology::DocIdPolicy;
use std::borrow::Cow;

use crate::harness::destination::read_request;

fn doc(index: &str, body: &str) -> PendingDoc {
    PendingDoc {
        index: index.to_string(),
        id: None,
        body: body.as_bytes().to_vec(),
    }
}

// --- the bulk body ------------------------------------------------------

#[test]
fn a_bulk_body_is_action_and_document_lines() {
    let body = bulk_body(
        &[doc("logs", r#"{"a":1}"#), doc("audit", r#"{"b":2}"#)],
        DocIdPolicy::Auto,
    );
    assert_eq!(
        String::from_utf8(body).unwrap(),
        "{\"create\":{\"_index\":\"logs\"}}\n{\"a\":1}\n\
         {\"create\":{\"_index\":\"audit\"}}\n{\"b\":2}\n"
    );
}

/// An index name can come from a pipeline expression (`__destination.index`), so it
/// is data rather than a literal and has to be escaped like any other.
#[test]
fn an_index_name_is_escaped_not_interpolated() {
    let body = bulk_body(&[doc(r#"we"ird"#, "{}")], DocIdPolicy::Auto);
    let text = String::from_utf8(body).unwrap();
    assert!(text.starts_with(r#"{"create":{"_index":"we\"ird"}}"#), "{text}");
    // And the result is still parseable as the action line it claims to be.
    let action: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(action["create"]["_index"], r#"we"ird"#);
}

/// The policy chooses the action, and the action is the whole of what the
/// policy means: `create` refuses a re-send of an id already present so the
/// first copy is kept, `index` replaces it so the last copy is.
#[test]
fn the_policy_chooses_the_bulk_action() {
    let d = || PendingDoc {
        index: "logs".into(),
        id: Some("x".into()),
        body: b"{}".to_vec(),
    };
    let dedupe = String::from_utf8(bulk_body(&[d()], DocIdPolicy::Dedupe)).unwrap();
    let overwrite = String::from_utf8(bulk_body(&[d()], DocIdPolicy::Overwrite)).unwrap();
    assert!(dedupe.starts_with(r#"{"create":"#), "{dedupe}");
    assert!(overwrite.starts_with(r#"{"index":"#), "{overwrite}");
}

// --- reading the response ----------------------------------------------

#[test]
fn a_clean_response_reports_nothing_to_do() {
    let body = br#"{"took":3,"errors":false,"items":[{"create":{"status":201}}]}"#;
    assert_eq!(
        classify_response(body, 1, DocIdPolicy::Auto).unwrap(),
        Outcome::default()
    );
}

/// The whole hazard in one test: HTTP 200, `"errors": true`, and a
/// document that never landed. Trusting the status line loses it.
#[test]
fn a_two_hundred_with_errors_true_is_a_failure() {
    let body = br#"{"errors":true,"items":[
        {"create":{"status":201}},
        {"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad field"}}}
    ]}"#;
    let out = classify_response(body, 2, DocIdPolicy::Auto).unwrap();
    assert!(out.retryable.is_empty());
    assert_eq!(out.permanent.len(), 1);
    assert_eq!(out.permanent[0].0, 1, "the second document");
    assert!(out.permanent[0].1.contains("mapper_parsing_exception"));
    assert!(!out.is_clean());
}

/// Busy and broken need opposite treatment, and getting either backward
/// is an outage: retrying a mapping conflict stalls the stream on one
/// document, and DLQ-ing a 429 throws away data the cluster would have
/// taken a second later.
#[test]
fn a_busy_cluster_is_retryable_and_a_bad_document_is_not() {
    let body = br#"{"errors":true,"items":[
        {"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
        {"create":{"status":400,"error":{"type":"strict_dynamic_mapping_exception","reason":"no"}}},
        {"create":{"status":503,"error":{"type":"unavailable_shards_exception","reason":"later"}}},
        {"create":{"status":201}}
    ]}"#;
    let out = classify_response(body, 4, DocIdPolicy::Auto).unwrap();
    assert_eq!(out.retryable, vec![0, 2]);
    assert_eq!(out.permanent.len(), 1);
    assert_eq!(out.permanent[0].0, 1);
}

/// A version conflict under `Dedupe` is the at-least-once re-send arriving
/// twice -- which is the whole outcome that policy exists to produce.
/// Counting it as an error would turn the dedupe into a stall.
#[test]
fn a_version_conflict_counts_as_delivered_when_deduping() {
    let body = br#"{"errors":true,"items":[
        {"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"dup"}}}
    ]}"#;
    assert!(classify_response(body, 1, DocIdPolicy::Dedupe).unwrap().is_clean());
}

/// ...and only then. `Auto` sets no id, so it has nothing to conflict
/// with; `Overwrite` writes `index`, which replaces rather than conflicts.
/// A 409 under either means something outside smeltr is versioning the
/// document, and swallowing it would be the "assume it was fine" that
/// every other branch here refuses.
#[test]
fn a_version_conflict_is_a_real_failure_under_any_other_policy() {
    let body = br#"{"errors":true,"items":[
        {"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"dup"}}}
    ]}"#;
    for policy in [DocIdPolicy::Auto, DocIdPolicy::Overwrite] {
        let out = classify_response(body, 1, policy).unwrap();
        assert_eq!(out.permanent.len(), 1, "{policy:?} must not swallow it");
        assert!(out.permanent[0].1.contains("version_conflict"));
    }
}

/// A busy cluster sends every document again after a wait, and the wait counts as throttling.
#[test]
fn a_busy_cluster_sends_every_document_again() {
    for status in [429, 503] {
        let answer = Response {
            status,
            headers: Vec::new(),
            body: br#"{"error":{"type":"es_rejected_execution_exception","reason":"rejected execution"}}"#.to_vec(),
        };
        let docs = [doc("logs", "{}"), doc("logs", "{}"), doc("logs", "{}")];
        let outcome = Elastic::new("logs").read(&answer, &docs).unwrap();
        assert_eq!(
            (outcome.retryable, outcome.throttled),
            (vec![0, 1, 2], true),
            "{status}"
        );
    }
}

/// Every document has to be accounted for. A response that answers fewer
/// than it was asked about is a failure rather than a partial success --
/// assuming the rest were fine is exactly the silent loss this is for.
/// A status that says the cluster is down or refusing the credential answers no document, so the whole request
/// fails and is sent again; the error says why. Each body
/// is what Elasticsearch 8.15 sent for that status, or its shape.
#[test]
fn a_down_or_unauthorized_cluster_fails_the_whole_request_and_quotes_the_answer() {
    let answers: &[(u16, &str)] = &[
        (
            401,
            r#"{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}"#,
        ),
        (
            403,
            r#"{"error":{"type":"security_exception","reason":"action [indices:data/write/bulk] is unauthorized"},"status":403}"#,
        ),
        (502, "Bad Gateway"),
        (504, "Gateway Timeout"),
    ];
    for &(status, body) in answers {
        let answer = Response {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        };
        let e = Elastic::new("logs").read(&answer, &[doc("logs", "{}")]).unwrap_err();
        assert!(
            !e.is_rejection(),
            "{status} answers no document, so it refuses none: {e}"
        );
        let quoted: String = body.chars().take(40).collect();
        assert!(
            e.to_string().contains(&format!("HTTP {status}")) && e.to_string().contains(&quoted),
            "{e}"
        );
    }
}

/// A 400 or a 413 refuses the request whole without naming a document, so the request is split until the document at
/// fault is alone, and that one is refused with the cluster's answer. The rest land, each sent once.
#[test]
fn a_request_refused_whole_is_split_down_to_the_documents_at_fault() {
    for status in [400, 413] {
        let body = format!(r#"{{"error":{{"type":"refused","reason":"bad"}},"status":{status}}}"#);
        let refused = (status, body.as_str());
        let clean = (200, r#"{"errors":false}"#);
        // [0 1 2 3] refused; [0 1] lands; [2 3] refused; [2] lands; [3] refused alone.
        let (t, sent) = answered(&[Ok(refused), Ok(clean), Ok(refused), Ok(clean), Ok(refused)]);
        let mut d = HttpDestination::new(t, Elastic::new("logs"));
        let rows: Vec<_> = (0..4).map(|n| format!(r#"{{"n":{n}}}"#)).collect();
        let rows: Vec<_> = rows.iter().map(|r| OutgoingRecord::new(r.as_bytes())).collect();
        let rejections = deliver(&mut d, &rows).expect("a refused document is a rejection, not a failed hand-over");
        assert_eq!(rejections.len(), 1, "{status}");
        assert_eq!(rejections[0].doc, 3);
        assert_eq!(rejections[0].record.as_ref().unwrap().value, br#"{"n":3}"#);
        assert!(
            rejections[0].reason.contains(&format!("HTTP {status}")),
            "{}",
            rejections[0].reason
        );
        let sent = sent.lock().unwrap();
        let carried = |i: usize| {
            (0..4)
                .filter(|n| sent[i].contains(&format!(r#"{{"n":{n}}}"#)))
                .collect::<Vec<_>>()
        };
        let requests: Vec<_> = (0..sent.len()).map(carried).collect();
        assert_eq!(
            requests,
            vec![vec![0, 1, 2, 3], vec![0, 1], vec![2, 3], vec![2], vec![3]]
        );
    }
}

/// A document that does not fit the index's mapping is refused alone, inside a `200`: here the answer Elasticsearch
/// 8.15 gave for a data stream document with no `@timestamp`, a string in a `long` field, and a new field under
/// `dynamic: strict`, between documents that landed. Each comes back as its own row's rejection, with the cluster's
/// reason, and the rest are delivered.
#[test]
fn a_document_the_mapping_refuses_is_rejected_alone() {
    let (t, sent) = scripted(&[Ok(
        r#"{"errors":true,"items":[{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"[1:22] failed to parse: data stream timestamp field [@timestamp] is missing"}}},{"create":{"status":201}},{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"[1:6] failed to parse field [n] of type [long] in document with id 'tWhPBKEBBF-BgoODp5mw'. Preview of field's value: 'abc'"}}},{"create":{"status":201}},{"create":{"status":400,"error":{"type":"strict_dynamic_mapping_exception","reason":"[1:6] mapping set to strict, dynamic introduction of [b] within [_doc] is not allowed"}}}]}"#,
    )]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let rows = [
        OutgoingRecord::new(br#"{"msg":"no timestamp"}"#),
        OutgoingRecord::new(br#"{"@timestamp":"2026-10-04T00:00:00Z"}"#),
        OutgoingRecord::new(br#"{"n":"abc"}"#),
        OutgoingRecord::new(br#"{"other":1}"#),
        OutgoingRecord::new(br#"{"b":1}"#),
    ];
    let rejections = deliver(&mut d, &rows).expect("the documents that fit landed, so the hand-over succeeded");
    assert_eq!(sent.lock().unwrap().len(), 1, "a refusal is never retried");
    let refused: Vec<(usize, &str, &[u8])> = rejections
        .iter()
        .map(|r| (r.doc, r.reason.as_str(), r.record.as_ref().unwrap().value.as_slice()))
        .collect();
    assert_eq!(refused.len(), 3);
    for ((doc, reason, record), (want_doc, want_reason)) in refused.into_iter().zip([
        (0, "data stream timestamp field [@timestamp] is missing"),
        (2, "failed to parse field [n] of type [long]"),
        (4, "strict_dynamic_mapping_exception"),
    ]) {
        assert_eq!(doc, want_doc);
        assert!(reason.contains(want_reason), "{reason}");
        assert_eq!(record, rows[want_doc].value, "a rejection carries its own row");
    }
}

#[test]
fn a_response_that_answers_the_wrong_number_of_documents_is_an_error() {
    let body = br#"{"errors":true,"items":[{"create":{"status":201}}]}"#;
    let e = classify_response(body, 3, DocIdPolicy::Auto).unwrap_err();
    assert!(e.to_string().contains("answered 1 of 3"), "{e}");
}

#[test]
fn an_unreadable_response_is_an_error_not_a_success() {
    assert!(classify_response(b"not json at all", 1, DocIdPolicy::Auto).is_err());
    assert!(classify_response(br#"{"errors":true}"#, 1, DocIdPolicy::Auto).is_err());
}

// --- the retry and accounting loop --------------------------------------

/// A transport that replays canned responses and records what it was
/// asked to send, so a retry can be checked for containing *only* the
/// documents that failed.
struct ScriptedTransport {
    responses: Vec<Result<Response, String>>,
    sent: Arc<Mutex<Vec<String>>>,
}

impl Transport for ScriptedTransport {
    fn post(&mut self, _path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(body).into_owned());
        match self.responses.remove(0) {
            Ok(r) => Ok(r),
            Err(e) => Err(IoError::transient(e)),
        }
    }
}

fn scripted(responses: &[Result<&str, &str>]) -> (ScriptedTransport, Arc<Mutex<Vec<String>>>) {
    let answers: Vec<_> = responses.iter().map(|r| r.map(|body| (200, body))).collect();
    answered(&answers)
}

/// `scripted`, with the status of each answer given.
fn answered(responses: &[Result<(u16, &str), &str>]) -> (ScriptedTransport, Arc<Mutex<Vec<String>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    (
        ScriptedTransport {
            responses: responses
                .iter()
                .map(|r| {
                    r.map(|(status, body)| Response {
                        status,
                        headers: Vec::new(),
                        body: body.as_bytes().to_vec(),
                    })
                    .map_err(str::to_string)
                })
                .collect(),
            sent: Arc::clone(&sent),
        },
        sent,
    )
}

/// Hand `rows` over as one batch. These destinations have no senders, so the batch is posted and answered before
/// `deliver` returns.
fn deliver(
    d: &mut HttpDestination<Elastic, ScriptedTransport>,
    rows: &[OutgoingRecord<'_>],
) -> Result<Vec<Rejection>, IoError> {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(dispatched, Dispatched::Complete);
    Ok(refused)
}

#[test]
fn a_clean_hand_over_posts_once() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    deliver(
        &mut d,
        &[OutgoingRecord::new(br#"{"a":1}"#), OutgoingRecord::new(br#"{"b":2}"#)],
    )
    .unwrap();
    assert_eq!(sent.lock().unwrap().len(), 1);
    // A batch with nothing in it must not post an empty body.
    deliver(&mut d, &[]).unwrap();
    d.flush().unwrap();
    assert_eq!(sent.lock().unwrap().len(), 1);
}

/// The retry carries only the documents that failed. Resending the ones
/// the cluster already took would duplicate every one of them -- which is
/// the failure mode that makes "just retry the batch" wrong.
#[test]
fn a_retry_resends_only_the_failed_documents() {
    let (t, sent) = scripted(&[
        Ok(r#"{"errors":true,"items":[
            {"create":{"status":201}},
            {"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"busy"}}},
            {"create":{"status":201}}
        ]}"#),
        Ok(r#"{"errors":false}"#),
    ]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"n":1}"#),
            OutgoingRecord::new(br#"{"n":2}"#),
            OutgoingRecord::new(br#"{"n":3}"#),
        ],
    )
    .unwrap();

    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].contains(r#"{"n":1}"#) && sent[0].contains(r#"{"n":3}"#));
    assert!(
        sent[1].contains(r#"{"n":2}"#),
        "the retry must carry the failed document: {}",
        sent[1]
    );
    assert!(
        !sent[1].contains(r#"{"n":1}"#) && !sent[1].contains(r#"{"n":3}"#),
        "and must not carry the ones already indexed: {}",
        sent[1]
    );
}

/// A permanent failure is never retried -- retrying is guaranteed to fail
/// again -- and, just as importantly, it does **not** fail the hand-over. One
/// document the cluster refused must not leave a good batch uncommitted
/// behind it, which is a wedged partition rather than a protected record.
/// It comes back as a `Rejection` instead, carrying its document, for the
/// destination's reject policy to answer.
#[test]
fn a_permanent_failure_is_reported_as_a_rejection_not_a_flush_error() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":true,"items":[
        {"create":{"status":201}},
        {"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}
    ]}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let rejections = deliver(
        &mut d,
        &[OutgoingRecord::new(br#"{"a":1}"#), OutgoingRecord::new(br#"{"a":2}"#)],
    )
    .expect("the good document landed, so the hand-over succeeded");
    assert_eq!(sent.lock().unwrap().len(), 1, "no retry");

    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].doc, 1, "the second document handed over");
    assert!(rejections[0].reason.contains("mapper_parsing_exception"));
    assert_eq!(
        rejections[0].record.as_ref().map(|r| r.value.as_slice()),
        Some(&br#"{"a":2}"#[..]),
        "with its document, for a DLQ"
    );
}

/// A rejection's index has to survive a retry, which compacts the pending
/// list down to only the documents that failed. Document 2 is refused and
/// document 1 is merely busy, so after the retry the numbering must still
/// point at the second document handed over, not at its position in the
/// compacted list.
#[test]
fn a_rejection_index_survives_a_retry_compaction() {
    let (t, _sent) = scripted(&[
        Ok(r#"{"errors":true,"items":[
            {"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"busy"}}},
            {"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}},
            {"create":{"status":201}}
        ]}"#),
        Ok(r#"{"errors":false}"#),
    ]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let bodies: Vec<String> = (0..3).map(|n| format!("{{\"n\":{n}}}")).collect();
    let rows: Vec<OutgoingRecord<'_>> = bodies.iter().map(|b| OutgoingRecord::new(b.as_bytes())).collect();
    let rejections = deliver(&mut d, &rows).unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].doc, 1, "the second document handed over");
}

/// A bulk request is newline-delimited, so a document holding a newline would end its line early and every
/// action/document pair after it would be read against the wrong partner -- a whole request misfiled, from one
/// record. Refused at hand-over, which is where the destination gets to look at it.
///
/// Reachable now that a row can be delivered without a pipeline: an unmatched destination and the poison DLQ hand over the
/// record's own bytes, and a multi-line record is ordinary.
#[test]
fn a_document_holding_a_newline_is_refused_rather_than_breaking_the_request() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let rejections = deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"a":1}"#),
            OutgoingRecord::new(b"line one\nline two"),
            OutgoingRecord::new(br#"{"a":3}"#),
        ],
    )
    .unwrap();
    assert_eq!(
        rejections.len(),
        1,
        "refused, not failed: the same bytes would be refused forever"
    );
    assert_eq!(rejections[0].doc, 1);
    assert!(rejections[0].reason.contains("newline"), "{}", rejections[0].reason);

    let body = sent.lock().unwrap()[0].clone();
    assert_eq!(
        body, "{\"create\":{\"_index\":\"logs\"}}\n{\"a\":1}\n{\"create\":{\"_index\":\"logs\"}}\n{\"a\":3}\n",
        "the refused document is absent and the pairs around it are intact"
    );
}

/// A refused document still counts as handed over: `Rejection` reports against its position in the rows handed over.
#[test]
fn a_newline_refusal_does_not_shift_a_later_rejection_s_index() {
    let (t, _sent) = scripted(&[Ok(r#"{"errors":true,"items":[
        {"create":{"status":201}},
        {"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}
    ]}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let rejections = deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"a":0}"#),
            OutgoingRecord::new(b"two\nlines"),
            OutgoingRecord::new(br#"{"a":2}"#),
        ],
    )
    .unwrap();

    let docs: Vec<usize> = rejections.iter().map(|r| r.doc).collect();
    assert_eq!(
        docs,
        vec![1, 2],
        "the newline, then the third document handed over rather than the second posted"
    );
}

/// Retries are bounded by total time, not by attempt count, because the
/// budget belongs to Kafka's poll interval rather than to Elasticsearch.
#[test]
fn retries_stop_at_the_flush_budget() {
    let busy = r#"{"errors":true,"items":[
        {"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"busy"}}}
    ]}"#;
    let (t, sent) = scripted(&vec![Ok(busy); 40]);
    let mut d = HttpDestination::new(t, Elastic::new("logs")).with_stall_after(Duration::from_millis(300));

    let started = std::time::Instant::now();
    let e = deliver(&mut d, &[OutgoingRecord::new(br#"{"a":1}"#)]).unwrap_err();
    let elapsed = started.elapsed();

    assert!(e.to_string().contains("still undelivered"), "{e}");
    assert!(
        elapsed < Duration::from_secs(3),
        "the budget bounds the whole hand-over, not one attempt: {elapsed:?}"
    );
    assert!(
        sent.lock().unwrap().len() > 1,
        "it should have retried at least once before giving up"
    );
}

/// A row's `__destination.index` picks its index; a row without one goes to the
/// default.
#[test]
fn a_placement_picks_the_index() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("fallback"));
    deliver(
        &mut d,
        &[
            OutgoingRecord::new(b"{}").with_placement(Some("logs-2026-08-29")),
            OutgoingRecord::new(b"{}"),
        ],
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    assert!(sent[0].contains(r#""_index":"logs-2026-08-29""#), "{}", sent[0]);
    assert!(sent[0].contains(r#""_index":"fallback""#), "{}", sent[0]);
}

/// A transport failure fails the hand-over, and the destination keeps nothing of it: the caller hands the batch over
/// again, and each document goes once more, once.
#[test]
fn a_transport_failure_fails_the_hand_over_and_keeps_nothing() {
    let (t, sent) = scripted(&[Err("connection refused"), Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    let rows = [OutgoingRecord::new(br#"{"a":1}"#), OutgoingRecord::new(br#"{"a":2}"#)];
    let e = deliver(&mut d, &rows).unwrap_err();
    assert!(!e.is_rejection(), "no answer is worth another try: {e}");
    deliver(&mut d, &rows).unwrap();

    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2, "the second hand-over posts again");
    for doc in [r#"{"a":1}"#, r#"{"a":2}"#] {
        assert_eq!(sent[1].matches(doc).count(), 1, "{doc} goes once: {}", sent[1]);
    }
}

/// A batch larger than one bulk request is posted in more than one -- Elasticsearch's own sizing question, which is
/// separate from how many bytes a source poll returned.
#[test]
fn a_batch_past_the_bulk_size_is_posted_in_rounds() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#), Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs")).with_max_request_bytes(16);
    deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"a":"0123456789"}"#),
            OutgoingRecord::new(br#"{"b":1}"#),
        ],
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(
        sent[0].contains("0123456789") && !sent[0].contains(r#"{"b":1}"#),
        "{}",
        sent[0]
    );
    assert!(sent[1].contains(r#"{"b":1}"#), "{}", sent[1]);
}

// --- the document id policy ---------------------------------------------

/// Under the default policy a row's `__destination.id` is ignored outright. It is
/// not a free upgrade: keying documents on an id costs real indexing
/// throughput, so it has to be asked for by name -- and the same pipeline
/// may well be setting the field for a *different* destination on the
/// route.
#[test]
fn the_default_policy_ignores_a_rows_doc_id() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs"));
    deliver(
        &mut d,
        &[OutgoingRecord::new(br#"{"a":1}"#).with_doc_id(Some("orders-3-991"))],
    )
    .unwrap();
    assert!(!sent.lock().unwrap()[0].contains("_id"), "{}", sent.lock().unwrap()[0]);
}

/// A row that leaves `__destination.id` null under a policy that wants one still
/// gets indexed, with a generated id -- what it would have had anyway.
/// Refusing it would take a whole batch down over one row with no natural
/// identity; the runtime counts it instead.
#[test]
fn a_row_with_no_doc_id_still_indexes_under_a_policy_that_wants_one() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs").with_doc_id_policy(DocIdPolicy::Dedupe));
    deliver(
        &mut d,
        &[
            OutgoingRecord::new(br#"{"a":1}"#).with_doc_id(Some("has-one")),
            OutgoingRecord::new(br#"{"b":2}"#),
        ],
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    let lines: Vec<&str> = sent[0].lines().collect();
    assert_eq!(lines[0], r#"{"create":{"_index":"logs","_id":"has-one"}}"#);
    assert_eq!(lines[2], r#"{"create":{"_index":"logs"}}"#);
}

/// The two ids are separate channels, and this is the trap that made them
/// separate: `key` is Kafka's *partition* key. Routing a document id into
/// it would have moved every Kafka destination off librdkafka's balanced
/// spread onto a key hash -- changed partitioning and per-key ordering, on
/// a deployment that only meant to turn on Elasticsearch dedupe. So a
/// `doc_id` must never arrive as a `key`, nor a `key` as a `_id`.
#[test]
fn a_document_id_is_not_a_partition_key() {
    let (t, sent) = scripted(&[Ok(r#"{"errors":false}"#)]);
    let mut d = HttpDestination::new(t, Elastic::new("logs").with_doc_id_policy(DocIdPolicy::Dedupe));
    deliver(&mut d, &[OutgoingRecord::new(b"{}").with_key(Some(b"partition-key"))]).unwrap();
    assert!(
        !sent.lock().unwrap()[0].contains("partition-key"),
        "a partition key is not a document identity: {}",
        sent.lock().unwrap()[0]
    );
}

#[test]
fn a_failed_batch_comes_back_beside_the_batch_a_flush_collected() {
    let (sender, _) = scripted(&[Ok(r#"{"errors":false}"#), Err("cluster said no")]);
    let mut d = HttpDestination::new(scripted(&[]).0, Elastic::new("logs")).with_senders(vec![sender], 1);
    let row = [OwnedRecord::from(&OutgoingRecord::new(br#"{"a":1}"#))];
    assert_eq!(d.deliver(1, Cow::Borrowed(&row)).unwrap().0, Dispatched::InFlight);
    d.flush().unwrap();
    d.deliver(2, Cow::Borrowed(&row)).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let mut done = d.poll_completions().expect("the landed batch comes first");
    done.extend(d.poll_completions().expect("and the failed one beside it"));
    let got: Vec<_> = done
        .iter()
        .map(|c| (c.batch, c.undelivered.as_ref().map(|u| u.rows.clone())))
        .collect();
    assert_eq!(got, vec![(1, None), (2, Some(vec![0]))]);
}

/// The answer lists a status per document unless the request filters it, and a bulk of many small documents would
/// then pass the transport's read limit after the cluster had indexed them.
#[test]
fn a_bulk_request_asks_only_for_what_the_answer_is_read_for() {
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        // The whole request, body too: a socket closed with bytes unread resets, and the client loses the answer.
        let (head, _) = read_request(&mut conn, &mut Vec::new()).expect("a request");
        let body = r#"{"errors":false}"#;
        write!(
            conn,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        format!("{} {} HTTP/1.1", head.method, head.path)
    });
    let config = smeltr::io::http::agent_config(None, &smeltr::plan::topology::HeldSecrets::default()).unwrap();
    let mut transport = smeltr::io::elastic::transport(&url, None, &config);
    transport
        .post(Elastic::new("i").path(), b"{}\n", Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        server.join().unwrap(),
        "POST /_bulk?filter_path=errors,items.*.status,items.*.error.type,items.*.error.reason,items.*.failure_store HTTP/1.1"
    );
}

/// A loopback destination is this host, and a proxy named in the environment would carry its credential off it.
#[test]
fn a_loopback_destination_skips_the_environment_proxy() {
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    // SAFETY: nextest runs each test in its own process, and no other thread here reads the environment yet.
    unsafe {
        std::env::set_var("ALL_PROXY", "http://127.0.0.1:1");
        std::env::set_var("HTTP_PROXY", "http://127.0.0.1:1");
        std::env::set_var("NO_PROXY", ".internal");
    }
    smeltr::startup_config::never_proxy_this_host(None);
    let server = std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        read_request(&mut conn, &mut Vec::new()).expect("a request");
        conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"errors\":false}")
            .unwrap();
    });
    let config = smeltr::io::http::agent_config(None, &smeltr::plan::topology::HeldSecrets::default()).unwrap();
    smeltr::io::elastic::transport(&url, None, &config)
        .post(Elastic::new("i").path(), b"{}\n", Duration::from_secs(5))
        .unwrap();
    server.join().unwrap();
}

// -- mapping ------------------------------------------------------------------

use crate::harness;

/// A pipeline in, its mapping out: the compiled types alone, then with a sample filling the open record's keys.
#[test]
fn a_pipeline_maps_as_it_compiled_and_as_its_sample_shows() {
    use serde_json::json;
    use smeltr::io::elastic_mapping::mapping;
    use smeltr::plan::dictionary::{record_type, with_sample};

    let p = harness::pipeline(vec![harness::code(
        0,
        "status: i64; url: {path: str}; Object.assign($, json(_raw)); took = 1.5;",
    )]);
    let plan = smeltr::plan::compile::compile(&p, &smeltr::plan::topology::SourceMeta::NONE).unwrap();
    let record = record_type(&plan, "smeltr.exception");
    let compiled = json!({
        "_raw": { "type": "keyword", "ignore_above": 8191 },
        "smeltr": { "properties": { "exception": { "properties": {
            "route": { "type": "keyword", "ignore_above": 8191 },
            "pipeline": { "type": "keyword", "ignore_above": 8191 },
            "exceptions": { "properties": {
                "step": { "type": "keyword", "ignore_above": 8191 },
                "kind": { "type": "keyword", "ignore_above": 8191 },
                "message": { "type": "keyword", "ignore_above": 8191 },
            } },
        } } } },
        "status": { "type": "long" },
        "took": { "type": "double" },
        "url": { "properties": { "path": { "type": "wildcard" } } },
    });
    assert_eq!(mapping(&record), json!({ "mappings": { "properties": compiled } }));

    let rows = [
        json!({ "status": 200, "url": { "path": "/a" }, "user": "bob", "n": 1 }),
        json!({ "n": "x" }),
    ];
    let mut sampled = compiled;
    sampled["user"] = json!({ "type": "keyword", "ignore_above": 8191 });
    assert_eq!(
        mapping(&with_sample(&record, &rows)),
        json!({ "mappings": { "properties": sampled } })
    );
}

/// A data stream's failure store answers a document 201: written, though not to the index it was sent to.
#[test]
fn a_document_a_failure_store_took_lands_and_counts_unwritten() {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let unwritten =
        smeltr::io::http::Unwritten::new(smeltr::metrics::MetricsHandle::new(registry.clone(), "egress-4"), 4);
    let answer = Response {
        status: 200,
        headers: Vec::new(),
        body:
            br#"{"errors":false,"items":[{"create":{"status":201,"failure_store":"used"}},{"create":{"status":201}}]}"#
                .to_vec(),
    };
    let docs = [doc("logs", "{}"), doc("logs", "{}")];
    let outcome = Elastic::new("logs")
        .with_unwritten(unwritten)
        .read(&answer, &docs)
        .unwrap();
    assert!(outcome.is_clean(), "both landed: {outcome:?}");
    let counts: Vec<_> = (registry.snapshot().into_iter())
        .filter(|s| s.key.name == "smeltr_destination_unwritten_total")
        .map(|s| (s.key.labels, s.value))
        .collect();
    assert_eq!(counts.len(), 1, "{counts:?}");
    assert!(format!("{:?}", counts[0].0).contains("document"), "{counts:?}");
    assert!(
        matches!(counts[0].1, smeltr::metrics::MetricValue::Counter(1)),
        "{counts:?}"
    );
}
