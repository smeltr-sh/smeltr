//! An SQS queue, as the SQS/S3 source's [`crate::io::object_source`] queue: its calls over the AWS JSON protocol,
//! signed like the buckets it announces, and S3's event notifications read out of its messages.

use std::sync::Arc;
use std::time::Duration;

use object_store::client::HttpRequestBody;
use serde_json::{Value, json};

use super::object_source::{Message, ObjectRef, Queue, Unread, queue_lag};
use super::objects::{Aws, AwsService, Signed};
use super::{IoError, PartitionLag};

/// The most entries one batch call takes. SQS's own limit.
const BATCH_MAX: usize = 10;

/// The longest receive wait SQS takes.
const WAIT_MAX: Duration = Duration::from_secs(20);

/// How long a call may take beyond its wait before it counts as failed.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A call's failure: whether SQS denied the credentials a permission, and why.
struct Failed {
    denied: bool,
    why: String,
}

/// Whether an SQS error denies the credentials a permission: a 403, or an access-denied error, which the JSON
/// protocol answers with a 400.
fn denied(status: http::StatusCode, kind: &str) -> bool {
    status == http::StatusCode::FORBIDDEN || kind.rsplit('#').next().is_some_and(|k| k.starts_with("AccessDenied"))
}

/// One SQS queue, reached at its URL.
pub struct Sqs {
    queue_url: String,
    /// Where calls go: the queue URL's scheme and host.
    endpoint: String,
    signed: Signed<AwsService>,
}

impl Sqs {
    /// The queue at `queue_url`, signed with `aws`'s credentials, its calls run on `runtime`.
    pub fn new(queue_url: &str, aws: Aws, runtime: Arc<tokio::runtime::Runtime>) -> Result<Sqs, IoError> {
        let host_end = queue_url
            .find("://")
            .map(|at| at + 3)
            .and_then(|start| queue_url[start..].find('/').map(|end| start + end))
            .unwrap_or(queue_url.len());
        Ok(Sqs {
            queue_url: queue_url.to_string(),
            endpoint: format!("{}/", &queue_url[..host_end]),
            signed: Signed::new(aws.service("sqs"), "SQS", queue_url, runtime)?,
        })
    }

    /// `action`'s answer to `body`, or why there is none.
    fn call(&self, action: &str, body: Value, wait: Duration) -> Result<Value, Failed> {
        let request = http::Request::post(&self.endpoint)
            .header("X-Amz-Target", format!("AmazonSQS.{action}"))
            .header(http::header::CONTENT_TYPE, "application/x-amz-json-1.0");
        let answered = self
            .signed
            .call(
                request,
                HttpRequestBody::from(body.to_string()),
                action,
                wait + CALL_TIMEOUT,
            )
            .map_err(|why| Failed { denied: false, why })?;
        let answer: Value = serde_json::from_slice(&answered.body).unwrap_or(Value::Null);
        if !answered.status.is_success() {
            let kind = answer["__type"].as_str().unwrap_or("");
            let why = answer["message"].as_str().or(answer["Message"].as_str()).unwrap_or("");
            return Err(Failed {
                denied: denied(answered.status, kind),
                why: format!("SQS answered {} to {action}: {kind} {why}", answered.status),
            });
        }
        Ok(answer)
    }

    /// Runs `action` over `handles` in batches, each entry made by `entry`. Answers each handle that failed, and why.
    fn batch(&self, action: &str, handles: &[String], entry: impl Fn(&str, &str) -> Value) -> Vec<(String, String)> {
        let mut failed = Vec::new();
        for group in handles.chunks(BATCH_MAX) {
            let entries: Vec<Value> = group
                .iter()
                .enumerate()
                .map(|(i, h)| entry(&i.to_string(), h))
                .collect();
            match self.call(
                action,
                json!({"QueueUrl": self.queue_url, "Entries": entries}),
                Duration::ZERO,
            ) {
                Ok(answer) => {
                    for f in answer["Failed"].as_array().into_iter().flatten() {
                        let index = f["Id"].as_str().and_then(|id| id.parse::<usize>().ok());
                        if let Some(handle) = index.and_then(|i| group.get(i)) {
                            let why = format!(
                                "{}: {}",
                                f["Code"].as_str().unwrap_or(""),
                                f["Message"].as_str().unwrap_or("")
                            );
                            failed.push((handle.clone(), why));
                        }
                    }
                }
                Err(f) => failed.extend(group.iter().map(|h| (h.clone(), f.why.clone()))),
            }
        }
        failed
    }
}

impl Queue for Sqs {
    fn receive(&self, max: usize, wait: Duration, visibility: Duration) -> Result<Vec<Message>, IoError> {
        let wait = wait.min(WAIT_MAX);
        let body = json!({
            "QueueUrl": self.queue_url,
            "MaxNumberOfMessages": max.clamp(1, BATCH_MAX),
            "WaitTimeSeconds": wait.as_secs(),
            "VisibilityTimeout": visibility.as_secs().max(1),
        });
        let answer = self
            .call("ReceiveMessage", body, wait)
            .map_err(|f| IoError::transient(f.why))?;
        Ok(answer["Messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                Some(Message {
                    handle: m["ReceiptHandle"].as_str()?.to_string(),
                    objects: notification(m["Body"].as_str().unwrap_or("")),
                })
            })
            .collect())
    }

    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)> {
        let secs = visibility.as_secs();
        self.batch(
            "ChangeMessageVisibilityBatch",
            handles,
            |id, handle| json!({"Id": id, "ReceiptHandle": handle, "VisibilityTimeout": secs}),
        )
    }

    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        self.batch(
            "DeleteMessageBatch",
            handles,
            |id, handle| json!({"Id": id, "ReceiptHandle": handle}),
        )
    }

    /// Denied once SQS refuses `sqs:GetQueueAttributes`.
    fn depth(&self) -> Option<Result<PartitionLag, Unread>> {
        let body = json!({"QueueUrl": self.queue_url, "AttributeNames": DEPTH_ATTRIBUTES});
        let depth = match self.call("GetQueueAttributes", body, Duration::ZERO) {
            Err(f) if f.denied => Err(Unread::Denied(f.why)),
            Err(f) => Err(Unread::Failed(f.why)),
            Ok(answer) => depth(&answer).map_err(Unread::Failed),
        };
        Some(depth.map(|depth| queue_lag(&self.queue_url, depth)))
    }
}

/// What a queue holds, counted as SQS counts it: waiting, hidden while held, and delayed before it first shows.
const DEPTH_ATTRIBUTES: [&str; 3] = [
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
];

/// The messages a `GetQueueAttributes` answer counts, over [`DEPTH_ATTRIBUTES`].
fn depth(answer: &Value) -> Result<i64, String> {
    DEPTH_ATTRIBUTES.iter().try_fold(0, |sum, name| {
        let count = answer["Attributes"][name].as_str().and_then(|n| n.parse::<i64>().ok());
        count
            .map(|n| sum + n)
            .ok_or_else(|| format!("SQS answered no {name} to GetQueueAttributes"))
    })
}

/// The objects an S3 event notification announces as created, as S3 sends it, wrapped by SNS in a `Message`, or as
/// EventBridge's `Object Created` event. Any other event, and S3's test event, announce none.
pub fn notification(body: &str) -> Result<Vec<ObjectRef>, String> {
    let start: String = body.chars().take(200).collect();
    let event: Value =
        serde_json::from_str(body).map_err(|_| format!("the message is no S3 event notification: {start}"))?;
    if event["Type"] == "Notification"
        && let Some(message) = event["Message"].as_str()
    {
        return notification(message);
    }
    if event["source"] == "aws.s3" {
        return bridged(&event["detail-type"], &event["detail"], &start);
    }
    if event["Event"] == "s3:TestEvent" {
        return Ok(Vec::new());
    }
    let records = event["Records"]
        .as_array()
        .ok_or_else(|| format!("the message is no S3 event notification: {start}"))?;
    let mut objects = Vec::new();
    for record in records {
        if !record["eventName"]
            .as_str()
            .is_some_and(|e| e.starts_with("ObjectCreated:"))
        {
            continue;
        }
        let object = &record["s3"]["object"];
        let mut object = object_ref(&record["s3"]["bucket"], object, &object["versionId"], &start)?;
        // S3's own notification encodes the key; EventBridge's carries it as is.
        object.key = url_decode(&object.key);
        objects.push(object);
    }
    Ok(objects)
}

/// The one object an EventBridge S3 event announces, if it is `Object Created`.
fn bridged(kind: &Value, detail: &Value, start: &str) -> Result<Vec<ObjectRef>, String> {
    if kind != "Object Created" {
        return Ok(Vec::new());
    }
    let object = &detail["object"];
    Ok(vec![object_ref(
        &detail["bucket"],
        object,
        &object["version-id"],
        start,
    )?])
}

fn object_ref(bucket: &Value, object: &Value, version: &Value, start: &str) -> Result<ObjectRef, String> {
    let (Some(bucket), Some(key)) = (bucket["name"].as_str(), object["key"].as_str()) else {
        return Err(format!("an ObjectCreated event names no bucket and key: {start}"));
    };
    Ok(ObjectRef {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version: version.as_str().map(str::to_string),
    })
}

/// The region an AWS queue URL names in its host: `sqs.<region>.amazonaws.com[.cn]`, or the legacy
/// `<region>.queue.amazonaws.com[.cn]`. None for any other host.
pub fn region(queue_url: &str) -> Option<&str> {
    let host = queue_url.split_once("://")?.1.split(['/', ':']).next()?;
    let name = host
        .strip_suffix(".amazonaws.com")
        .or_else(|| host.strip_suffix(".amazonaws.com.cn"))?;
    name.strip_prefix("sqs.")
        .or_else(|| name.strip_suffix(".queue"))
        .filter(|region| !region.is_empty() && !region.contains('.'))
}

/// A key as S3's notification encodes it: `+` for a space and `%XX` for a byte.
fn url_decode(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'+', _) => out.push(b' '),
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 2;
            }
            (byte, _) => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn created(key: &str) -> String {
        format!(
            r#"{{"Records":[{{"eventName":"ObjectCreated:Put","s3":{{"bucket":{{"name":"logs"}},"object":{{"key":"{key}"}}}}}}]}}"#
        )
    }

    #[test]
    fn a_created_object_is_announced_with_its_key_decoded() {
        let objects = notification(&created("a+b%2Fc%C3%A9%zz.log")).unwrap();
        assert_eq!(
            objects,
            vec![ObjectRef {
                bucket: "logs".to_string(),
                key: "a b/cé%zz.log".to_string(),
                version: None,
            }]
        );
        let versioned = created("k").replace(r#""key":"k""#, r#""key":"k","versionId":"3HL4kqtJ""#);
        assert_eq!(
            notification(&versioned).unwrap()[0].version.as_deref(),
            Some("3HL4kqtJ")
        );
    }

    #[test]
    fn a_queue_holds_what_waits_what_is_held_and_what_is_delayed() {
        let answer = json!({"Attributes": {
            "ApproximateNumberOfMessages": "5",
            "ApproximateNumberOfMessagesNotVisible": "2",
            "ApproximateNumberOfMessagesDelayed": "1"
        }});
        assert_eq!(depth(&answer), Ok(8));
        let partial = json!({"Attributes": {"ApproximateNumberOfMessages": "5"}});
        assert!(
            depth(&partial)
                .unwrap_err()
                .contains("ApproximateNumberOfMessagesNotVisible")
        );
        assert_eq!(
            queue_lag("https://sqs.us-east-1.amazonaws.com/123456789012/orders", 8)
                .partition
                .topic
                .as_ref(),
            "orders"
        );
    }

    #[test]
    fn an_access_denied_error_denies_the_permission() {
        let bad = http::StatusCode::BAD_REQUEST;
        assert!(denied(bad, "com.amazon.coral.service#AccessDeniedException"));
        assert!(denied(bad, "AccessDenied"));
        assert!(denied(http::StatusCode::FORBIDDEN, ""));
        assert!(!denied(bad, "com.amazonaws.sqs#QueueDoesNotExist"));
    }

    #[test]
    fn a_queue_url_names_its_region() {
        assert_eq!(region("https://sqs.eu-west-1.amazonaws.com/1/q"), Some("eu-west-1"));
        assert_eq!(
            region("https://sqs.cn-north-1.amazonaws.com.cn/1/q"),
            Some("cn-north-1")
        );
        assert_eq!(region("https://us-west-2.queue.amazonaws.com/1/q"), Some("us-west-2"));
        assert_eq!(region("https://queue.amazonaws.com/1/q"), None);
        assert_eq!(region("http://localhost:4566/000000000000/q"), None);
        assert_eq!(region("https://vpce-1.sqs.us-east-1.vpce.amazonaws.com/1/q"), None);
    }

    #[test]
    fn other_events_announce_nothing_and_a_stranger_is_refused() {
        let test = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"logs"}"#;
        assert_eq!(notification(test).unwrap(), vec![]);
        let removed = created("k").replace("ObjectCreated:Put", "ObjectRemoved:Delete");
        assert_eq!(notification(&removed).unwrap(), vec![]);
        assert!(notification("hello").is_err());
        assert!(notification(r#"{"Message":"wrapped"}"#).is_err());
    }

    #[test]
    fn an_sns_envelope_is_unwrapped() {
        let wrapped =
            json!({"Type": "Notification", "TopicArn": "arn:aws:sns:us-east-1:1:t", "Message": created("a+b")});
        let objects = notification(&wrapped.to_string()).unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!((objects[0].bucket.as_str(), objects[0].key.as_str()), ("logs", "a b"));
        let test = json!({"Type": "Notification", "Message": r#"{"Event":"s3:TestEvent"}"#});
        assert_eq!(notification(&test.to_string()).unwrap(), vec![]);
        let stranger = json!({"Type": "Notification", "Message": "hello"});
        assert!(notification(&stranger.to_string()).is_err());
    }

    #[test]
    fn an_eventbridge_object_created_event_announces_its_object() {
        let event = |kind: &str| {
            json!({
                "source": "aws.s3",
                "detail-type": kind,
                "detail": {
                    "bucket": {"name": "logs"},
                    "object": {"key": "a b+c.log", "version-id": "3HL4kqtJ"}
                }
            })
            .to_string()
        };
        assert_eq!(
            notification(&event("Object Created")).unwrap(),
            vec![ObjectRef {
                bucket: "logs".to_string(),
                key: "a b+c.log".to_string(),
                version: Some("3HL4kqtJ".to_string()),
            }]
        );
        assert_eq!(notification(&event("Object Deleted")).unwrap(), vec![]);
        let keyless =
            json!({"source": "aws.s3", "detail-type": "Object Created", "detail": {"bucket": {"name": "logs"}}});
        assert!(notification(&keyless.to_string()).is_err());
    }
}
