//! A Pub/Sub subscription, as the Pub/Sub/GCS source's [`crate::io::object_source`] queue: its calls over Pub/Sub's
//! REST API, signed like the buckets it announces, and Cloud Storage's notifications read out of its messages.
//!
//! A message's ack id is its handle. It stays the same across every deadline change, so nothing is kept per message.
//! Poison messages end through the subscription's own dead-letter policy, as on SQS.

use std::sync::Arc;
use std::time::Duration;

use object_store::client::HttpRequestBody;
use serde_json::{Map, Value, json};

use super::IoError;
use super::object_source::{Message, ObjectRef, Queue, receive_polling};
use super::objects::{Gcp, Signed};

/// The most ack ids one call carries, well under Pub/Sub's request size.
const BATCH_MAX: usize = 1000;

/// How long a call may take before it counts as failed.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest ack deadline Pub/Sub takes.
const DEADLINE_MAX: Duration = Duration::from_secs(600);

/// A call's failure: why, and the error Pub/Sub answered, `Null` when none.
struct Failed {
    why: String,
    answer: Value,
}

/// One subscription, reached at its endpoint.
pub struct PubSub {
    /// `<endpoint>/v1/projects/<project>/subscriptions/<subscription>`, to which each call's `:<verb>` is added.
    url: String,
    signed: Signed<Gcp>,
}

impl PubSub {
    /// `subscription`, at `endpoint` or else Google's in `gcp`'s universe, signed with `gcp`'s credentials, its calls
    /// run on `runtime`.
    pub fn new(
        subscription: &str,
        endpoint: Option<&str>,
        gcp: Gcp,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<PubSub, IoError> {
        let google = gcp.service("pubsub");
        let endpoint = endpoint.unwrap_or(&google).trim().trim_end_matches('/');
        Ok(PubSub {
            url: format!("{endpoint}/v1/{}", subscription.trim()),
            signed: Signed::new(gcp, "Pub/Sub", endpoint, runtime)?,
        })
    }

    /// `verb`'s answer to `body`, or why there is none.
    fn call(&self, verb: &str, body: Value) -> Result<Value, Failed> {
        let request =
            http::Request::post(format!("{}:{verb}", self.url)).header(http::header::CONTENT_TYPE, "application/json");
        let answered = self
            .signed
            .call(request, HttpRequestBody::from(body.to_string()), verb, CALL_TIMEOUT)
            .map_err(|why| Failed {
                why,
                answer: Value::Null,
            })?;
        let answer: Value = serde_json::from_slice(&answered.body).unwrap_or(Value::Null);
        if !answered.status.is_success() {
            return Err(Failed {
                why: format!(
                    "Pub/Sub answered {} to {verb}: {}",
                    answered.status,
                    answer["error"]["message"].as_str().unwrap_or("")
                ),
                answer,
            });
        }
        Ok(answer)
    }

    /// Sets each of `handles`' ack deadline to `deadline` from now. Answers each handle that failed, and why.
    fn modify(&self, handles: &[String], deadline: Duration) -> Vec<(String, String)> {
        let secs = deadline.min(DEADLINE_MAX).as_secs();
        batch(handles, false, |ids| {
            self.call("modifyAckDeadline", json!({"ackIds": ids, "ackDeadlineSeconds": secs}))
        })
    }

    /// The messages one pull answers, each held for `visibility`.
    fn pull(&self, max: usize, visibility: Duration) -> Result<Vec<Message>, IoError> {
        // Answering at once is deprecated, and still the only way to bound the wait: a pull that waits is cut off by
        // the caller's timeout, and whatever it was answering stays leased to no one until its deadline lapses.
        let answer = self
            .call("pull", json!({"maxMessages": max.max(1), "returnImmediately": true}))
            .map_err(|f| IoError::transient(f.why))?;
        let mut messages = Vec::new();
        for m in answer["receivedMessages"].as_array().into_iter().flatten() {
            let Some(handle) = m["ackId"].as_str() else { continue };
            let empty = Map::new();
            let attributes = m["message"]["attributes"].as_object().unwrap_or(&empty);
            messages.push(Message {
                handle: handle.to_string(),
                objects: notification(attributes),
            });
        }
        if messages.is_empty() {
            return Ok(messages);
        }
        // A pull leases each message for the subscription's own deadline, ten seconds unless set otherwise: shorter
        // than the source holds a message between extensions. A message this fails for is not held long enough to
        // read, so it is left to Pub/Sub's own redelivery.
        let handles: Vec<String> = messages.iter().map(|m| m.handle.clone()).collect();
        let unheld = self.modify(&handles, visibility);
        if let Some((_, why)) = unheld.first() {
            tracing::warn!(
                target: "object_source.receive_failed",
                count = unheld.len(),
                error = %why,
                "messages pulled could not be held; they are left to be redelivered"
            );
        }
        messages.retain(|m| !unheld.iter().any(|(h, _)| *h == m.handle));
        Ok(messages)
    }
}

impl Queue for PubSub {
    fn name(&self) -> &str {
        &self.url
    }

    fn receive(&self, max: usize, wait: Duration, visibility: Duration) -> Result<Vec<Message>, IoError> {
        receive_polling(wait, || self.pull(max, visibility))
    }

    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)> {
        self.modify(handles, visibility)
    }

    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        batch(handles, true, |ids| self.call("acknowledge", json!({"ackIds": ids})))
    }
}

/// Runs `call` over `handles` in batches. Answers each handle that failed, and why.
///
/// On a subscription that delivers exactly once, Pub/Sub refuses a whole call over one bad ack id, and names each
/// one it refused. Only those failed: retrying the rest with them would fail again on every try. An ack id refused
/// for good has lapsed, its message is redelivered, so when `lapsed_is_done` it counts as done, as a delete of a
/// lapsed message does on the other queues.
fn batch(
    handles: &[String],
    lapsed_is_done: bool,
    call: impl Fn(&[String]) -> Result<Value, Failed>,
) -> Vec<(String, String)> {
    let mut failed = Vec::new();
    for group in handles.chunks(BATCH_MAX) {
        let Err(f) = call(group) else { continue };
        let Some(refused) = refused(&f.answer) else {
            failed.extend(group.iter().map(|h| (h.clone(), f.why.clone())));
            continue;
        };
        for handle in group {
            let Some(reason) = refused.get(handle.as_str()).and_then(Value::as_str) else {
                continue;
            };
            if lapsed_is_done && reason.starts_with("PERMANENT_FAILURE") {
                tracing::warn!(
                    target: "object_source.delete_failed",
                    reason,
                    "a finished message had already lapsed back onto the subscription; it will be read again"
                );
                continue;
            }
            failed.push((handle.clone(), format!("{}: {reason}", f.why)));
        }
    }
    failed
}

/// The ack ids an exactly-once subscription refused, each with why, from its error's `EXACTLY_ONCE_ACKID_FAILURE`
/// details. `None` when the error names none, so the whole call failed.
fn refused(answer: &Value) -> Option<&Map<String, Value>> {
    (answer["error"]["details"].as_array()?.iter())
        // Only this reason's metadata is keyed by ack id; another's, such as a quota's, names none.
        .filter(|d| d["reason"] == "EXACTLY_ONCE_ACKID_FAILURE")
        .find_map(|d| d["metadata"].as_object())
        .filter(|m| !m.is_empty())
}

/// The object a Cloud Storage notification announces, read from its message's `attributes`, so a notification with
/// payload format `NONE` reads the same as `JSON_API_V1`. Only `OBJECT_FINALIZE`, an object written, announces one;
/// every other event announces none. The generation announced is the one read.
pub fn notification(attributes: &Map<String, Value>) -> Result<Vec<ObjectRef>, String> {
    let get = |name: &str| attributes.get(name).and_then(Value::as_str);
    let Some(kind) = get("eventType") else {
        let names: Vec<&str> = attributes.keys().map(String::as_str).collect();
        return Err(format!(
            "the message is no Cloud Storage notification: it has no eventType attribute, only {names:?}"
        ));
    };
    if kind != "OBJECT_FINALIZE" {
        return Ok(Vec::new());
    }
    match (get("bucketId"), get("objectId")) {
        (Some(bucket), Some(key)) if !bucket.is_empty() && !key.is_empty() => Ok(vec![ObjectRef {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version: get("objectGeneration").filter(|g| !g.is_empty()).map(str::to_string),
        }]),
        _ => Err(format!(
            "an OBJECT_FINALIZE notification names no bucketId and objectId: {}",
            Value::Object(attributes.clone())
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn a_finalized_object_is_announced_at_its_generation() {
        let found = notification(&attributes(json!({
            "eventType": "OBJECT_FINALIZE",
            "payloadFormat": "NONE",
            "bucketId": "logs",
            "objectId": "in/a b+cé.log",
            "objectGeneration": "1700000000000001",
            "notificationConfig": "projects/_/buckets/logs/notificationConfigs/1"
        })))
        .unwrap();
        assert_eq!(
            found,
            vec![ObjectRef {
                bucket: "logs".to_string(),
                key: "in/a b+cé.log".to_string(),
                version: Some("1700000000000001".to_string()),
            }]
        );
    }

    #[test]
    fn every_other_event_announces_nothing() {
        for kind in ["OBJECT_DELETE", "OBJECT_ARCHIVE", "OBJECT_METADATA_UPDATE"] {
            let found = notification(&attributes(json!({
                "eventType": kind, "bucketId": "logs", "objectId": "a.log"
            })));
            assert_eq!(found, Ok(Vec::new()), "{kind}");
        }
    }

    #[test]
    fn a_message_that_is_no_notification_is_unreadable() {
        let why = notification(&attributes(json!({"origin": "elsewhere"}))).unwrap_err();
        assert!(why.contains("no eventType") && why.contains("origin"), "{why}");
        let why = notification(&attributes(json!({"eventType": "OBJECT_FINALIZE", "bucketId": "logs"}))).unwrap_err();
        assert!(why.contains("objectId"), "{why}");
    }

    #[test]
    fn only_the_ack_ids_an_exactly_once_subscription_names_are_refused() {
        let answer = json!({"error": {
            "code": 400,
            "message": "Some acknowledgement ids in the request were invalid.",
            "status": "INVALID_ARGUMENT",
            "details": [{
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "EXACTLY_ONCE_ACKID_FAILURE",
                "domain": "pubsub.googleapis.com",
                "metadata": {"a1": "PERMANENT_FAILURE_INVALID_ACK_ID", "a2": "TRANSIENT_FAILURE_UNORDERED_ACK_ID"}
            }]
        }});
        let refused = refused(&answer).unwrap();
        assert_eq!(refused.len(), 2);
        assert_eq!(refused["a1"], "PERMANENT_FAILURE_INVALID_ACK_ID");
        let plain = json!({"error": {"code": 403, "message": "denied", "status": "PERMISSION_DENIED"}});
        assert!(super::refused(&plain).is_none());
        assert!(super::refused(&Value::Null).is_none());
        let quota = json!({"error": {"details": [{
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "RATE_LIMIT_EXCEEDED",
            "metadata": {"consumer": "projects/1", "service": "pubsub.googleapis.com"}
        }]}});
        assert!(super::refused(&quota).is_none());

        let ids = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let call = |_: &[String]| {
            Err(Failed {
                why: "refused".to_string(),
                answer: answer.clone(),
            })
        };
        let failed = batch(&ids(&["a1", "a2", "a3"]), true, call);
        assert_eq!(
            failed,
            vec![(
                "a2".to_string(),
                "refused: TRANSIENT_FAILURE_UNORDERED_ACK_ID".to_string()
            )]
        );
        let failed: Vec<String> = batch(&ids(&["a1", "a2", "a3"]), false, call)
            .into_iter()
            .map(|(h, _)| h)
            .collect();
        assert_eq!(failed, ids(&["a1", "a2"]));
        let down = |_: &[String]| {
            Err(Failed {
                why: "down".to_string(),
                answer: plain.clone(),
            })
        };
        assert_eq!(batch(&ids(&["a1", "a3"]), true, down).len(), 2);
    }

    #[test]
    fn a_notification_without_a_generation_reads_the_latest() {
        let found = notification(&attributes(json!({
            "eventType": "OBJECT_FINALIZE", "bucketId": "logs", "objectId": "a.log"
        })))
        .unwrap();
        assert_eq!(found[0].version, None);
    }
}
