//! An Azure Storage queue, as the Queue/Azure Blob source's [`crate::io::object_source`] queue: its calls over the
//! Queue service's REST API, signed like the containers it announces, and Event Grid's `BlobCreated` events read out
//! of its messages.
//!
//! A Storage queue has no dead-letter queue of its own. A message received more than `max_deliveries` times is moved
//! to `<queue>-poison`, as Azure Functions does, so an unreadable blob stops coming back without being lost.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use http::Method;
use http::header::CONTENT_LENGTH;
use object_store::client::HttpRequestBody;
use serde_json::Value;

use super::http::percent_encode;
use super::object_source::{Message, ObjectRef, Queue, Unread, queue_lag, receive_polling};
use super::objects::{Azure, Signed};
use super::{IoError, PartitionLag};
use crate::format::bytes_text::BytesForm;
use crate::ops::webops::percent_decode;
use crate::plan::topology::AzureAccess;

/// The most messages one get takes. The Queue service's own limit.
const GET_MAX: usize = 32;

/// How long a call may take before it counts as failed.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The suffix of the queue a message is moved to once it has been received `max_deliveries` times.
pub const POISON_SUFFIX: &str = "-poison";

/// One Storage queue, reached at its URL.
pub struct StorageQueue {
    queue_url: String,
    poison_url: String,
    /// Every blob a notification names is under one of these addresses: the account's blob service, and its Data
    /// Lake service, which names the same blobs.
    blobs: Vec<String>,
    max_deliveries: u32,
    signed: Signed<Azure>,
    /// Each received message's current pop receipt, by its id, which is the handle it is held under. The receipt
    /// changes on every update, so the lock is held across each call that uses one: a delete never races an extend
    /// onto a receipt the extend has just replaced. A message received again takes its new receipt.
    receipts: Mutex<HashMap<String, String>>,
    /// Whether the poison queue is known to exist.
    poison_made: Mutex<bool>,
}

/// A call's failure: the status the service answered, or none, and why.
struct Failed {
    status: Option<http::StatusCode>,
    why: String,
}

impl StorageQueue {
    /// The queue at `queue_url`, announcing blobs in `access`'s account, signed with `azure`'s credentials, its calls
    /// run on `runtime`.
    pub fn new(
        queue_url: &str,
        access: &AzureAccess,
        max_deliveries: u32,
        azure: Azure,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<StorageQueue, IoError> {
        let queue_url = queue_url.trim().trim_end_matches('/').to_string();
        Ok(StorageQueue {
            poison_url: format!("{queue_url}{POISON_SUFFIX}"),
            signed: Signed::new(azure, "the queue", &queue_url, runtime)?,
            queue_url,
            blobs: blob_services(access),
            max_deliveries,
            receipts: Mutex::new(HashMap::new()),
            poison_made: Mutex::new(false),
        })
    }

    /// `method` on `url` with `body`, signed: the answer's headers and body, or why there is none.
    fn call(&self, method: Method, url: &str, body: String) -> Result<(http::HeaderMap, String), Failed> {
        let request = http::Request::builder()
            .method(method.clone())
            .uri(url)
            .header(CONTENT_LENGTH, body.len());
        let answered = self
            .signed
            .call(request, HttpRequestBody::from(body), method.as_str(), CALL_TIMEOUT)
            .map_err(|why| Failed { status: None, why })?;
        let body = String::from_utf8_lossy(&answered.body).into_owned();
        if !answered.status.is_success() {
            let code = tag(&body, "Code").unwrap_or_default();
            return Err(Failed {
                status: Some(answered.status),
                why: format!("the queue answered {} to {method}: {code}", answered.status),
            });
        }
        Ok((answered.headers, body))
    }

    /// The address of message `id` with `receipt`, and `more` query.
    fn message_url(&self, id: &str, receipt: &str, more: &str) -> String {
        format!(
            "{}/messages/{}?popreceipt={}{more}",
            self.queue_url,
            percent_encode(id),
            percent_encode(receipt)
        )
    }

    /// Moves a message's `text` to the poison queue, making that queue first if this source has not. The message
    /// itself is deleted by the caller once this succeeds.
    fn poison(&self, text: &str) -> Result<(), String> {
        let mut made = self.poison_made.lock().unwrap_or_else(PoisonError::into_inner);
        if !*made {
            // Made once and kept: 201 when new, 204 when it already exists.
            self.call(Method::PUT, &self.poison_url, String::new())
                .map_err(|f| f.why)?;
            *made = true;
        }
        // A poisoned message is kept until someone deletes it: -1 is "never expires".
        let body = format!(
            "<QueueMessage><MessageText>{}</MessageText></QueueMessage>",
            escape(text)
        );
        self.call(
            Method::POST,
            &format!("{}/messages?messagettl=-1", self.poison_url),
            body,
        )
        .map(|_| ())
        .map_err(|f| f.why)
    }

    /// The messages one get answers, hidden for `visibility`, the poisoned ones moved aside.
    fn get(&self, max: usize, visibility: Duration) -> Result<Vec<Message>, IoError> {
        let url = format!(
            "{}/messages?numofmessages={}&visibilitytimeout={}",
            self.queue_url,
            max.clamp(1, GET_MAX),
            visibility.as_secs().max(1)
        );
        let (_, body) = self
            .call(Method::GET, &url, String::new())
            .map_err(|f| IoError::transient(f.why))?;
        let mut messages = Vec::new();
        for m in elements(&body, "QueueMessage") {
            let (Some(id), Some(receipt)) = (tag(m, "MessageId"), tag(m, "PopReceipt")) else {
                continue;
            };
            let text = tag(m, "MessageText").unwrap_or_default();
            let deliveries: u32 = tag(m, "DequeueCount").and_then(|c| c.parse().ok()).unwrap_or(1);
            if deliveries > self.max_deliveries {
                self.move_to_poison(&id, &receipt, &text, deliveries);
                continue;
            }
            self.receipts().insert(id.clone(), receipt);
            messages.push(Message {
                objects: notification(&text, &self.blobs),
                handle: id,
            });
        }
        Ok(messages)
    }

    /// Copies message `id`'s `text` to the poison queue, then deletes the message. A failed copy leaves it where it
    /// is, to be tried on its next receive.
    fn move_to_poison(&self, id: &str, receipt: &str, text: &str, deliveries: u32) {
        self.receipts().remove(id);
        if let Err(why) = self.poison(text) {
            tracing::warn!(
                target: "object_source.poison",
                id,
                error = %why,
                "a message received too many times could not be moved to the poison queue; it is left where it is"
            );
            return;
        }
        tracing::warn!(
            target: "object_source.poison",
            id,
            deliveries,
            queue = %self.poison_url,
            "a message was received too many times without being read; it is moved to the poison queue"
        );
        if let Some(why) = self.delete_one(id, receipt) {
            tracing::warn!(
                target: "object_source.poison",
                id,
                error = %why,
                "a message moved to the poison queue was not deleted from its own; it will be moved again"
            );
        }
    }

    /// Deletes message `id`. A message the queue no longer holds under `receipt` has lapsed back onto it and been
    /// received again by another worker: it is not this receipt's to delete, so it counts as done.
    fn delete_one(&self, id: &str, receipt: &str) -> Option<String> {
        match self.call(Method::DELETE, &self.message_url(id, receipt, ""), String::new()) {
            Ok(_) => None,
            Err(f) if f.status == Some(http::StatusCode::NOT_FOUND) => {
                tracing::warn!(
                    target: "object_source.delete",
                    id,
                    error = %f.why,
                    "a finished message had already lapsed back onto the queue; it will be read again"
                );
                None
            }
            Err(f) => Some(f.why),
        }
    }

    fn receipts(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.receipts.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Queue for StorageQueue {
    fn receive(&self, max: usize, wait: Duration, visibility: Duration) -> Result<Vec<Message>, IoError> {
        receive_polling(wait, || self.get(max, visibility))
    }

    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)> {
        let mut failed = Vec::new();
        for id in handles {
            let mut receipts = self.receipts();
            // Deleted meanwhile: nothing is left to hide.
            let Some(receipt) = receipts.get(id) else { continue };
            let more = format!("&visibilitytimeout={}", visibility.as_secs());
            match self.call(Method::PUT, &self.message_url(id, receipt, &more), String::new()) {
                Ok((headers, _)) => match headers.get("x-ms-popreceipt").and_then(|r| r.to_str().ok()) {
                    Some(next) => {
                        receipts.insert(id.clone(), next.to_string());
                    }
                    None => failed.push((id.clone(), "the queue answered no new pop receipt".to_string())),
                },
                Err(f) => failed.push((id.clone(), f.why)),
            }
        }
        failed
    }

    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        let mut failed = Vec::new();
        for id in handles {
            let mut receipts = self.receipts();
            let Some(receipt) = receipts.get(id) else { continue };
            match self.delete_one(id, receipt) {
                None => {
                    receipts.remove(id);
                }
                Some(why) => failed.push((id.clone(), why)),
            }
        }
        failed
    }

    /// Denied once the queue refuses its metadata.
    fn depth(&self) -> Option<Result<PartitionLag, Unread>> {
        let url = format!("{}?comp=metadata", self.queue_url);
        let depth = match self.call(Method::GET, &url, String::new()) {
            Err(f) if f.status == Some(http::StatusCode::FORBIDDEN) => Err(Unread::Denied(f.why)),
            Err(f) => Err(Unread::Failed(f.why)),
            Ok((headers, _)) => headers
                .get("x-ms-approximate-messages-count")
                .and_then(|count| count.to_str().ok()?.parse::<i64>().ok())
                .ok_or_else(|| Unread::Failed("the queue answered no x-ms-approximate-messages-count".to_string())),
        };
        Some(depth.map(|depth| queue_lag(&self.queue_url, depth)))
    }
}

/// The addresses every blob a notification names is under: `access`'s blob service, and the Data Lake service an
/// account with a hierarchical namespace names its files under, `dfs` in the blob service's place.
pub fn blob_services(access: &AzureAccess) -> Vec<String> {
    let blob = match &access.endpoint {
        Some(endpoint) => endpoint.trim().trim_end_matches('/').to_string(),
        None => format!("https://{}.blob.core.windows.net", access.account),
    };
    let dfs = blob.replacen(".blob.", ".dfs.", 1);
    if dfs == blob { vec![blob] } else { vec![blob, dfs] }
}

/// The blobs an Event Grid message announces as created under one of `blobs`, the account's services. The event is read
/// in Event Grid's own schema or CloudEvents 1.0, alone or in an array, and its text as is or base64-encoded, as
/// Azure's SDKs write a queue message. Any other event announces none, as does Data Lake's `CreateFile`, which makes
/// an empty file that a `FlushWithClose` event announces again once it is written.
pub fn notification(text: &str, blobs: &[String]) -> Result<Vec<ObjectRef>, String> {
    let start: String = text.chars().take(200).collect();
    let stranger = || format!("the message is no Event Grid event: {start}");
    let trimmed = text.trim();
    let event: Value = match serde_json::from_str(trimmed) {
        Ok(event) => event,
        Err(_) => {
            let mut decoded = Vec::new();
            if !BytesForm::Base64.read(trimmed.as_bytes(), &mut decoded) {
                return Err(stranger());
            }
            serde_json::from_slice(&decoded).map_err(|_| stranger())?
        }
    };
    let events = match event {
        Value::Array(events) => events,
        event @ Value::Object(_) => vec![event],
        _ => return Err(stranger()),
    };
    let mut objects = Vec::new();
    for event in &events {
        let kind = event["eventType"]
            .as_str()
            .or(event["type"].as_str())
            .ok_or_else(stranger)?;
        if kind != "Microsoft.Storage.BlobCreated" || event["data"]["api"] == "CreateFile" {
            continue;
        }
        let url = event["data"]["url"]
            .as_str()
            .ok_or_else(|| format!("a BlobCreated event names no blob url: {start}"))?;
        objects.push(blob_of(url, blobs)?);
    }
    Ok(objects)
}

/// The container and blob `url` names under one of `blobs`.
fn blob_of(url: &str, blobs: &[String]) -> Result<ObjectRef, String> {
    let rest = blobs
        .iter()
        .find_map(|base| {
            url.get(..base.len())
                .filter(|start| start.eq_ignore_ascii_case(base))
                .and_then(|_| url[base.len()..].strip_prefix('/'))
        })
        .ok_or_else(|| {
            format!(
                "a BlobCreated event names {url}, which is not in this account's blob service {}",
                blobs[0]
            )
        })?;
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    match rest.split_once('/') {
        Some((container, blob)) if !container.is_empty() && !blob.is_empty() => Ok(ObjectRef {
            bucket: container.to_string(),
            // A bare `%` is kept, so the decoder never refuses.
            key: String::from_utf8_lossy(&percent_decode(blob.as_bytes(), false, true).unwrap_or_default())
                .into_owned(),
            version: None,
        }),
        _ => Err(format!(
            "a BlobCreated event names {url}, which names no container and blob"
        )),
    }
}

/// Each `<name>…</name>` element's content in `xml`, in order.
fn elements<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(at) = rest.find(&open) {
        let inner = &rest[at + open.len()..];
        let Some(end) = inner.find(&close) else { break };
        found.push(&inner[..end]);
        rest = &inner[end + close.len()..];
    }
    found
}

/// The text of the first `<name>` element in `xml`, unescaped.
fn tag(xml: &str, name: &str) -> Option<String> {
    elements(xml, name).first().map(|t| unescape(t))
}

/// XML's five entities and its character references, as the Queue service escapes a message's text.
fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let entity = &rest[at..];
        let Some(end) = entity.find(';') else {
            out.push_str(entity);
            return out;
        };
        let name = &entity[1..end];
        let ch = match name {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => name
                .strip_prefix("#x")
                .map(|h| u32::from_str_radix(h, 16))
                .or_else(|| name.strip_prefix('#').map(str::parse))
                .and_then(Result::ok)
                .and_then(char::from_u32),
        };
        match ch {
            Some(c) => out.push(c),
            None => out.push_str(&entity[..=end]),
        }
        rest = &entity[end + 1..];
    }
    out.push_str(rest);
    out
}

/// `text` as XML element content.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BLOB: &str = "https://acme.blob.core.windows.net";

    fn services() -> Vec<String> {
        vec![BLOB.to_string(), "https://acme.dfs.core.windows.net".to_string()]
    }

    fn created(url: &str) -> Value {
        json!({
            "topic": "/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/acme",
            "subject": "/blobServices/default/containers/logs/blobs/a b.log",
            "eventType": "Microsoft.Storage.BlobCreated",
            "data": {"api": "PutBlob", "url": url}
        })
    }

    fn blob(container: &str, key: &str) -> ObjectRef {
        ObjectRef {
            bucket: container.to_string(),
            key: key.to_string(),
            version: None,
        }
    }

    #[test]
    fn a_created_blob_is_announced_with_its_name_decoded() {
        let event = created("https://acme.blob.core.windows.net/logs/in/a%20b%2Bc%C3%A9.log").to_string();
        assert_eq!(
            notification(&event, &services()).unwrap(),
            vec![blob("logs", "in/a b+cé.log")]
        );
    }

    #[test]
    fn a_base64_message_a_cloud_event_and_an_array_are_read() {
        let event = created("https://acme.blob.core.windows.net/logs/a.log").to_string();
        let encoded = BytesForm::Base64.text(event.as_bytes());
        assert_eq!(
            notification(&encoded, &services()).unwrap(),
            vec![blob("logs", "a.log")]
        );
        // Data Lake names a file it wrote under the account's dfs service.
        let cloud = json!({
            "specversion": "1.0",
            "type": "Microsoft.Storage.BlobCreated",
            "data": {"api": "FlushWithClose", "url": "https://acme.dfs.core.windows.net/logs/b.log"}
        });
        let both = Value::Array(vec![created("https://acme.blob.core.windows.net/logs/a.log"), cloud]);
        assert_eq!(
            notification(&both.to_string(), &services()).unwrap(),
            vec![blob("logs", "a.log"), blob("logs", "b.log")]
        );
    }

    #[test]
    fn other_events_announce_nothing_and_a_stranger_is_refused() {
        let mut deleted = created("https://acme.blob.core.windows.net/logs/a.log");
        deleted["eventType"] = json!("Microsoft.Storage.BlobDeleted");
        assert_eq!(notification(&deleted.to_string(), &services()).unwrap(), vec![]);
        let mut empty_file = created("https://acme.blob.core.windows.net/logs/a.log");
        empty_file["data"]["api"] = json!("CreateFile");
        assert_eq!(notification(&empty_file.to_string(), &services()).unwrap(), vec![]);
        assert!(notification("hello", &services()).is_err());
        assert!(notification(r#"{"data":{}}"#, &services()).is_err());
        assert!(notification("[1]", &services()).is_err());
    }

    #[test]
    fn a_blob_outside_the_account_or_with_no_container_is_refused() {
        let other = created("https://other.blob.core.windows.net/logs/a.log").to_string();
        assert!(
            notification(&other, &services())
                .unwrap_err()
                .contains("not in this account's")
        );
        let prefix = created("https://acme.blob.core.windows.net.evil/logs/a.log").to_string();
        assert!(notification(&prefix, &services()).is_err());
        let bare = created("https://acme.blob.core.windows.net/logs").to_string();
        assert!(
            notification(&bare, &services())
                .unwrap_err()
                .contains("no container and blob")
        );
        // Azurite names its account in the path.
        let azurite = ["http://127.0.0.1:10000/devstoreaccount1".to_string()];
        let event = created("http://127.0.0.1:10000/devstoreaccount1/logs/a.log?sv=1").to_string();
        assert_eq!(notification(&event, &azurite).unwrap(), vec![blob("logs", "a.log")]);
    }

    #[test]
    fn a_get_answer_is_read_and_its_text_unescaped() {
        let answer = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessagesList>\
            <QueueMessage><MessageId>m1</MessageId><PopReceipt>AgAA+/==</PopReceipt><DequeueCount>2</DequeueCount>\
            <MessageText>{&quot;a&quot;:&quot;&lt;b&gt; &amp; &#233;&#x4e2d;&quot;}</MessageText></QueueMessage>\
            <QueueMessage><MessageId>m2</MessageId><PopReceipt>r2</PopReceipt><MessageText></MessageText>\
            </QueueMessage></QueueMessagesList>";
        let messages = elements(answer, "QueueMessage");
        assert_eq!(messages.len(), 2);
        assert_eq!(tag(messages[0], "PopReceipt").as_deref(), Some("AgAA+/=="));
        assert_eq!(tag(messages[0], "DequeueCount").as_deref(), Some("2"));
        assert_eq!(tag(messages[0], "MessageText").as_deref(), Some(r#"{"a":"<b> & é中"}"#));
        assert_eq!(tag(messages[1], "MessageText").as_deref(), Some(""));
        assert_eq!(unescape("a &bogus; &amp"), "a &bogus; &amp");
        assert_eq!(escape("<a & b>"), "&lt;a &amp; b&gt;");
    }

    #[test]
    fn the_blob_services_are_the_endpoint_or_the_accounts_own_with_data_lakes() {
        let access = |endpoint: Option<&str>| AzureAccess {
            account: "acme".to_string(),
            endpoint: endpoint.map(str::to_string),
            auth_endpoint: None,
            credential: None,
        };
        assert_eq!(blob_services(&access(None)), services());
        assert_eq!(
            blob_services(&access(Some("https://acme.blob.core.usgovcloudapi.net/"))),
            [
                "https://acme.blob.core.usgovcloudapi.net",
                "https://acme.dfs.core.usgovcloudapi.net"
            ]
        );
        assert_eq!(
            blob_services(&access(Some("http://127.0.0.1:10000/devstoreaccount1/"))),
            ["http://127.0.0.1:10000/devstoreaccount1"]
        );
    }
}
