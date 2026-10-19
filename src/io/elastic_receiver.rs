//! What an Elasticsearch receiver reads: the `_bulk` API, as Elasticsearch's clients send it.
//!
//! A bulk body is newline-delimited JSON: an action line, then, for `index`, `create` and `update`, a document line.
//! Each `index` or `create` action's document is one record, the document as it was sent. What the action names for it,
//! its index, id and ingest pipeline, rides with the record as its `__source` fields. An `update` or `delete` action
//! changes a stored document, which a receiver has none of, so it is answered with an error of its own and takes
//! nothing; so is a document that is not a JSON object. The rest of the request is taken all the same, as
//! Elasticsearch answers each action of a bulk request by itself.

use std::ops::Range;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;

use crate::format::json::write_json_string;
use crate::format::json_scan::{Next, Reader};

/// What a document is filed under, as `hec::Filed` is.
pub type Filed = super::hec::Filed;

/// The keys a document is filed under, beside `hec::NAMED`'s `index`, by the `__source` field each is read as.
pub const ID: &str = "id";
pub const PIPELINE: &str = "pipeline";

/// `hec::NAMED`'s key for the index, so a document's index is read as a HEC event's is.
const INDEX: &str = super::hec::NAMED[3];

/// What answering one action holds at most, besides the body it was read from: its item, kept until the request is
/// answered, and the item's JSON once it is.
pub const ANSWER_BYTES: usize = 256;

/// What reading `body` and answering its actions holds at most: the body, and an answer for each line, as each line
/// may be an action. Known before `body` is read, so a request too large to answer is refused before it is.
pub fn holds(body: &[u8]) -> usize {
    let lines = body.iter().filter(|b| **b == b'\n').count() + 1;
    body.len().saturating_add(lines.saturating_mul(ANSWER_BYTES))
}

/// The Elasticsearch version a receiver names itself as, which a client checks before it sends.
pub const VERSION: &str = "8.17.0";

/// What a request's path and query name for each action that names none itself.
#[derive(Default)]
pub struct Defaults<'a> {
    pub index: Option<&'a str>,
    pub pipeline: Option<&'a str>,
}

/// Why a whole bulk request is refused: Elasticsearch's error type and its reason.
#[derive(Debug, PartialEq)]
pub struct Invalid {
    pub kind: &'static str,
    pub reason: String,
}

impl Invalid {
    fn validation(reason: &str) -> Invalid {
        Invalid {
            kind: "action_request_validation_exception",
            reason: format!("Validation Failed: 1: {reason};"),
        }
    }

    fn malformed(line: usize, why: &str) -> Invalid {
        Invalid {
            kind: "illegal_argument_exception",
            reason: format!("Malformed action/metadata line [{line}], {why}"),
        }
    }
}

/// The documents of a bulk body that are taken, cut one at a time.
pub struct Documents {
    body: Vec<u8>,
    /// Each taken document's place in `body` and what it is filed under, in the order sent.
    taken: std::vec::IntoIter<(Range<usize>, Filed)>,
}

impl Documents {
    /// The next taken document, as sent, and what it is filed under.
    pub fn next_record(&mut self) -> Option<(Vec<u8>, Filed)> {
        let (range, filed) = self.taken.next()?;
        Some((self.body[range].to_vec(), filed))
    }
}

/// The answer to each action of a bulk body, in the order they were sent.
pub struct Items {
    items: Vec<Item>,
}

struct Item {
    action: &'static str,
    index: String,
    id: Option<String>,
    /// The error type and reason of an action that took nothing.
    failed: Option<(&'static str, String)>,
}

impl Items {
    /// The bulk answer once the taken documents are delivered, as JSON: `took`, `errors` and an item per action. Each
    /// item is written as it is made, so answering holds the answer's bytes and one item.
    pub fn answer(&self, took: Duration) -> Vec<u8> {
        let took = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
        let errors = self.items.iter().any(|item| item.failed.is_some());
        let mut answer = format!(r#"{{"took":{took},"errors":{errors},"items":["#).into_bytes();
        for (n, item) in self.items.iter().enumerate() {
            if n > 0 {
                answer.push(b',');
            }
            item.answer(&mut answer);
        }
        answer.extend_from_slice(b"]}");
        answer
    }
}

impl Item {
    /// The item's JSON, `{"<action>":{...}}`, appended to `out`.
    fn answer(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"{\"");
        out.extend_from_slice(self.action.as_bytes());
        out.extend_from_slice(b"\":{\"_index\":");
        write_json_string(self.index.as_bytes(), out);
        out.extend_from_slice(b",\"_id\":");
        match &self.id {
            Some(id) => write_json_string(id.as_bytes(), out),
            None => out.extend_from_slice(b"null"),
        }
        match &self.failed {
            None => out.extend_from_slice(
                br#","_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}}"#,
            ),
            Some((kind, reason)) => {
                out.extend_from_slice(br#","status":400,"error":{"type":"#);
                write_json_string(kind.as_bytes(), out);
                out.extend_from_slice(br#","reason":"#);
                write_json_string(reason.as_bytes(), out);
                out.extend_from_slice(b"}}}");
            }
        }
    }
}

/// `body`'s documents and the answer to each of its actions, or why the whole request is refused. Every line is read
/// here, so none of a refused request is taken.
pub fn read(body: Vec<u8>, defaults: Defaults<'_>) -> Result<(Documents, Items), Invalid> {
    let mut lines = Lines {
        body: &body,
        at: 0,
        n: 0,
    }
    .filter(|(_, range)| !body[range.clone()].trim_ascii().is_empty());
    let (mut taken, mut items) = (Vec::new(), Vec::new());
    while let Some((n, range)) = lines.next() {
        let (action, meta) = action(&body[range], n)?;
        // A value of another type names nothing, and is not taken for one that is absent.
        // A control character is refused, as serde_json refused it: the answer echoes `_index` and `_id`, and would
        // write each one as six bytes, past what `holds` charges.
        let text = |key: &str, value: &Option<Option<String>>| match value {
            None => Ok(None),
            Some(Some(text)) if text.chars().any(char::is_control) => {
                Err(Invalid::malformed(n, &format!("[{key}] holds a control character")))
            }
            Some(Some(text)) => Ok(Some(text.clone())),
            Some(None) => Err(Invalid::malformed(n, &format!("[{key}] is not a string"))),
        };
        let index = text("_index", &meta.index)?
            .or(defaults.index.map(str::to_string))
            .ok_or_else(|| Invalid::validation("index is missing"))?;
        let id = text("_id", &meta.id)?;
        let pipeline = text("pipeline", &meta.pipeline)?.or(defaults.pipeline.map(str::to_string));
        let document = match action {
            "delete" => None,
            _ => Some(
                lines
                    .next()
                    .ok_or_else(|| Invalid::malformed(n, &format!("the [{action}] action has no document line")))?
                    .1,
            ),
        };
        let failed = match (action, document) {
            ("index" | "create", Some(document)) if is_object(&body[document.clone()]) => {
                let mut filed = vec![(INDEX.to_string(), index.clone().into_bytes())];
                filed.extend(id.iter().map(|id| (ID.to_string(), id.clone().into_bytes())));
                filed.extend(pipeline.map(|p| (PIPELINE.to_string(), p.into_bytes())));
                taken.push((document, filed));
                None
            }
            ("index" | "create", _) => Some((
                "document_parsing_exception",
                "failed to parse: the document is not a JSON object".to_string(),
            )),
            _ => Some((
                "illegal_argument_exception",
                format!("a receiver takes [index] and [create] actions only: it holds no stored document to {action}"),
            )),
        };
        let id = match (&failed, id) {
            (None, None) => Some(generated_id()),
            (_, id) => id,
        };
        items.push(Item {
            action,
            index,
            id,
            failed,
        });
    }
    if items.is_empty() {
        return Err(Invalid::validation("no requests added"));
    }
    let documents = Documents {
        body,
        taken: taken.into_iter(),
    };
    Ok((documents, Items { items }))
}

/// A body's lines, each numbered from 1 with its place in the body, without its `\n` or `\r\n`.
struct Lines<'a> {
    body: &'a [u8],
    at: usize,
    n: usize,
}

impl Iterator for Lines<'_> {
    type Item = (usize, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.body.len() {
            return None;
        }
        let start = self.at;
        let end = self.body[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(self.body.len(), |i| start + i);
        self.at = end + 1;
        self.n += 1;
        let end = match self.body[start..end].last() {
            Some(b'\r') => end - 1,
            _ => end,
        };
        Some((self.n, start..end))
    }
}

/// What an action names for its document: each of its keys present, as text, or `None` where it is no text.
#[derive(Default)]
struct Meta {
    index: Option<Option<String>>,
    id: Option<Option<String>>,
    pipeline: Option<Option<String>>,
}

/// An action line's action and the metadata it names, or why the request is refused at line `n`.
fn action(line: &[u8], n: usize) -> Result<(&'static str, Meta), Invalid> {
    let not_json = || Invalid::malformed(n, "expected a JSON object");
    if !is_json(line) {
        return Err(not_json());
    }
    let mut r = Reader::new(line).map_err(|_| not_json())?;
    if r.value().map_err(|_| not_json())? != Next::Object {
        return Err(not_json());
    }
    let one = || Invalid::malformed(n, "expected an object with one action");
    let name = r.key().map_err(|_| not_json())?.ok_or_else(one)?;
    let meta = r.raw().map_err(|_| not_json())?;
    if r.key().map_err(|_| not_json())?.is_some() {
        return Err(one());
    }
    let name = ["index", "create", "update", "delete"]
        .into_iter()
        .find(|known| *known == name)
        .ok_or_else(|| Invalid::malformed(n, &format!("unknown action [{name}]")))?;
    let mut r = Reader::new(meta).map_err(|_| not_json())?;
    if r.value().map_err(|_| not_json())? != Next::Object {
        return Err(Invalid::malformed(
            n,
            &format!("expected an object of metadata for [{name}]"),
        ));
    }
    let mut named = Meta::default();
    while let Some(key) = r.key().map_err(|_| not_json())? {
        let slot = match &*key {
            "_index" => &mut named.index,
            "_id" => &mut named.id,
            "pipeline" => &mut named.pipeline,
            _ => {
                r.skip().map_err(|_| not_json())?;
                continue;
            }
        };
        *slot = Some(match r.raw().map_err(|_| not_json())? {
            raw if raw.first() == Some(&b'"') => match Reader::new(raw).map(|mut r| r.value()) {
                Ok(Ok(Next::Str(text))) => Some(text.into_owned()),
                _ => None,
            },
            _ => None,
        });
    }
    Ok((name, named))
}

/// Whether `text` is one JSON value and nothing more.
fn is_json(text: &[u8]) -> bool {
    Reader::new(text).is_ok_and(|mut r| r.skip().is_ok() && r.end())
}

/// Whether a document line is a JSON object, the only document Elasticsearch indexes.
fn is_object(document: &[u8]) -> bool {
    document.trim_ascii_start().first() == Some(&b'{') && is_json(document)
}

/// An id for a document whose action names none, 20 URL-safe base64 characters as Elasticsearch's are. The time this
/// process first made one, with a count after it, keeps each distinct from every other this process makes and from a
/// process started at another time.
fn generated_id() -> String {
    static STARTED: OnceLock<u64> = OnceLock::new();
    static COUNT: AtomicU64 = AtomicU64::new(0);
    let started = *STARTED.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64)
    });
    let count = COUNT.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 15];
    bytes[..8].copy_from_slice(&started.to_be_bytes());
    bytes[8..].copy_from_slice(&count.to_be_bytes()[1..]);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// An Elasticsearch error body, as JSON: its `error`, of `kind` for `reason`, and the `status` it is answered with.
pub fn error(status: u16, kind: &str, reason: &str) -> Vec<u8> {
    let (mut kind_json, mut reason_json) = (Vec::new(), Vec::new());
    write_json_string(kind.as_bytes(), &mut kind_json);
    write_json_string(reason.as_bytes(), &mut reason_json);
    let (kind, reason) = (
        String::from_utf8_lossy(&kind_json),
        String::from_utf8_lossy(&reason_json),
    );
    format!(
        r#"{{"error":{{"root_cause":[{{"type":{kind},"reason":{reason}}}],"type":{kind},"reason":{reason}}},"status":{status}}}"#
    )
    .into_bytes()
}

/// What `GET /_license` answers, as JSON: a basic license, active, which a Beat checks it may send under before it
/// sends.
pub fn license() -> Vec<u8> {
    br#"{"license":{"status":"active","uid":"c2tlbHRyLWxpY2Vuc2U","type":"basic","issue_date":"2024-12-11T00:00:00.000Z","issue_date_in_millis":1733875200000,"max_nodes":1000,"max_resource_units":null,"issued_to":"smeltr","issuer":"smeltr","start_date_in_millis":-1}}"#.to_vec()
}

/// What `GET /` answers, as JSON: the cluster a receiver names itself as, at [`VERSION`].
pub fn cluster() -> Vec<u8> {
    format!(
        r#"{{"name":"smeltr","cluster_name":"smeltr","cluster_uuid":"c2tlbHRyLXJlY2VpdmVy","version":{{"number":"{VERSION}","build_flavor":"default","build_type":"docker","build_hash":"0000000000000000000000000000000000000000","build_date":"2024-12-11T12:08:05.663969764Z","build_snapshot":false,"lucene_version":"9.12.0","minimum_wire_compatibility_version":"7.17.0","minimum_index_compatibility_version":"7.0.0"}},"tagline":"You Know, for Search"}}"#
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> Defaults<'static> {
        Defaults::default()
    }

    fn records(documents: &mut Documents) -> Vec<(String, Filed)> {
        std::iter::from_fn(|| documents.next_record())
            .map(|(record, filed)| (String::from_utf8(record).unwrap(), filed))
            .collect()
    }

    fn filed(pairs: &[(&str, &str)]) -> Filed {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect()
    }

    fn items(answer: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        answer["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                let (action, result) = item.as_object().unwrap().iter().next().unwrap();
                (action.clone(), result.clone())
            })
            .collect()
    }

    #[test]
    fn takes_index_and_create_and_answers_each_action_in_order() {
        let body = concat!(
            "{\"index\":{\"_index\":\"logs\",\"_id\":\"a\"}}\n",
            "{\"msg\":\"one\"}\n",
            "{\"delete\":{\"_index\":\"logs\",\"_id\":\"b\"}}\n",
            "{\"create\":{\"_index\":\"logs\"}}\n",
            "{\"msg\":\"two\"}\n",
            "{\"update\":{\"_index\":\"logs\",\"_id\":\"c\"}}\n",
            "{\"doc\":{\"msg\":\"three\"}}\n",
            "{\"index\":{\"_index\":\"logs\"}}\n",
            "[1,2]\n",
        );
        let (mut documents, items_) = read(body.as_bytes().to_vec(), defaults()).unwrap();
        assert_eq!(
            records(&mut documents),
            vec![
                ("{\"msg\":\"one\"}".to_string(), filed(&[("index", "logs"), (ID, "a")])),
                ("{\"msg\":\"two\"}".to_string(), filed(&[("index", "logs")])),
            ]
        );
        let answer = serde_json::from_slice::<serde_json::Value>(&items_.answer(Duration::from_millis(7))).unwrap();
        assert_eq!(answer["took"], 7);
        assert_eq!(answer["errors"], true);
        let items = items(&answer);
        let actions: Vec<_> = items.iter().map(|(a, _)| a.as_str()).collect();
        assert_eq!(actions, ["index", "delete", "create", "update", "index"]);
        let statuses: Vec<_> = items.iter().map(|(_, r)| r["status"].as_u64().unwrap()).collect();
        assert_eq!(statuses, [201, 400, 201, 400, 400]);
        assert_eq!(items[0].1["_id"], "a");
        assert_eq!(items[0].1["result"], "created");
        assert_eq!(items[0].1["_shards"]["successful"], 1);
        assert_eq!(items[1].1["_id"], "b");
        assert_eq!(items[1].1["error"]["type"], "illegal_argument_exception");
        assert_eq!(items[3].1["error"]["type"], "illegal_argument_exception");
        assert_eq!(items[4].1["error"]["type"], "document_parsing_exception");
        assert_eq!(items[4].1["_id"], serde_json::Value::Null);
        assert_eq!(items[4].1["_index"], "logs");
    }

    #[test]
    fn errors_is_false_when_every_action_is_taken() {
        let body = b"{\"index\":{\"_index\":\"i\"}}\n{\"a\":1}\n".to_vec();
        let (_, items) = read(body, defaults()).unwrap();
        let answer = serde_json::from_slice::<serde_json::Value>(&items.answer(Duration::ZERO)).unwrap();
        assert_eq!(answer["errors"], false);
        assert_eq!(answer["took"], 0);
    }

    #[test]
    fn reads_crlf_blank_lines_and_a_last_line_without_a_newline() {
        let body = b"\r\n{\"index\":{\"_index\":\"i\"}}\r\n\r\n  \n{\"a\":1}\r\n\n{\"create\":{\"_index\":\"i\"}}\r\n{\"b\":2}";
        let (mut documents, _) = read(body.to_vec(), defaults()).unwrap();
        let records: Vec<_> = records(&mut documents).into_iter().map(|(r, _)| r).collect();
        assert_eq!(records, ["{\"a\":1}", "{\"b\":2}"]);
    }

    #[test]
    fn a_record_is_its_document_line_as_sent() {
        let document = "{ \"b\" : 2,\t\"a\": [1, 2.50, \"\\u00e9\"] }";
        let body = format!("{{\"index\":{{\"_index\":\"i\"}}}}\n{document}\n");
        let (mut documents, _) = read(body.into_bytes(), defaults()).unwrap();
        assert_eq!(documents.next_record().unwrap().0, document.as_bytes());
        assert!(documents.next_record().is_none());
    }

    #[test]
    fn delete_has_no_document_line() {
        let body = b"{\"delete\":{\"_index\":\"i\",\"_id\":\"1\"}}\n{\"index\":{\"_index\":\"i\"}}\n{\"a\":1}\n";
        let (mut documents, items) = read(body.to_vec(), defaults()).unwrap();
        assert_eq!(records(&mut documents).len(), 1);
        let answer = serde_json::from_slice::<serde_json::Value>(&items.answer(Duration::ZERO)).unwrap();
        let actions: Vec<_> = self::items(&answer).into_iter().map(|(a, _)| a).collect();
        assert_eq!(actions, ["delete", "index"]);
    }

    #[test]
    fn a_missing_document_line_refuses_the_whole_request() {
        let body = b"{\"index\":{\"_index\":\"i\"}}\n{\"a\":1}\n{\"create\":{\"_index\":\"i\"}}\n\n";
        let invalid = read(body.to_vec(), defaults()).err().unwrap();
        assert_eq!(invalid.kind, "illegal_argument_exception");
        assert!(invalid.reason.contains("[3]"), "{}", invalid.reason);
    }

    #[test]
    fn a_malformed_action_line_refuses_the_whole_request() {
        for line in [
            "not json",
            "[1]",
            "{}",
            "{\"index\":{},\"create\":{}}",
            "{\"upsert\":{}}",
            "{\"index\":\"i\"}",
            "{\"index\":{\"_index\":7}}",
            "{\"create\":{\"_index\":\"i\",\"_id\":{\"a\":1}}}",
            "{\"index\":{\"_index\":\"i\",\"pipeline\":true}}",
        ] {
            let body = format!("{{\"index\":{{\"_index\":\"i\"}}}}\n{{}}\n{line}\n{{}}\n");
            let invalid = read(body.into_bytes(), defaults()).err().unwrap();
            assert_eq!(invalid.kind, "illegal_argument_exception", "{line}");
            assert!(invalid.reason.contains("[3]"), "{line}: {}", invalid.reason);
        }
    }

    #[test]
    fn what_a_body_holds_counts_an_answer_for_each_line() {
        assert_eq!(holds(b"{}"), 2 + ANSWER_BYTES);
        assert_eq!(holds(b"{\"delete\":{}}\n{\"delete\":{}}\n"), 28 + 3 * ANSWER_BYTES);
    }

    #[test]
    fn a_missing_index_refuses_the_whole_request() {
        let body = b"{\"index\":{\"_index\":\"i\"}}\n{}\n{\"index\":{}}\n{}\n";
        let invalid = read(body.to_vec(), defaults()).err().unwrap();
        assert_eq!(invalid.kind, "action_request_validation_exception");
        assert!(invalid.reason.contains("index is missing"));
    }

    #[test]
    fn a_control_character_in_an_index_or_id_refuses_the_whole_request() {
        for line in [
            "{\"delete\":{\"_index\":\"a\x01\"}}",
            r#"{"delete":{"_index":"i","_id":"\u0001"}}"#,
        ] {
            let invalid = read(format!("{line}\n").into_bytes(), defaults()).err().unwrap();
            assert!(
                invalid.reason.contains("control character"),
                "{line}: {}",
                invalid.reason
            );
        }
    }

    #[test]
    fn an_empty_body_is_refused() {
        for body in ["", "\n", " \r\n \n"] {
            let invalid = read(body.as_bytes().to_vec(), defaults()).err().unwrap();
            assert_eq!(invalid.kind, "action_request_validation_exception");
        }
    }

    #[test]
    fn defaults_apply_where_an_action_names_none() {
        let defaults = Defaults {
            index: Some("from-path"),
            pipeline: Some("from-query"),
        };
        let body = concat!(
            "{\"index\":{}}\n{\"a\":1}\n",
            "{\"index\":{\"_index\":\"own\",\"pipeline\":\"own-pipe\",\"_id\":\"x\",\"routing\":\"r\",\"_type\":\"_doc\"}}\n{\"a\":2}\n",
        );
        let (mut documents, items) = read(body.as_bytes().to_vec(), defaults).unwrap();
        assert_eq!(
            records(&mut documents),
            vec![
                (
                    "{\"a\":1}".to_string(),
                    filed(&[("index", "from-path"), (PIPELINE, "from-query")])
                ),
                (
                    "{\"a\":2}".to_string(),
                    filed(&[("index", "own"), (ID, "x"), (PIPELINE, "own-pipe")])
                ),
            ]
        );
        let answer = serde_json::from_slice::<serde_json::Value>(&items.answer(Duration::ZERO)).unwrap();
        let items = self::items(&answer);
        assert_eq!(items[0].1["_index"], "from-path");
        assert_eq!(items[1].1["_index"], "own");
    }

    #[test]
    fn generated_ids_are_distinct_and_shaped_as_elasticsearchs() {
        let body = "{\"index\":{\"_index\":\"i\"}}\n{}\n".repeat(100);
        let (mut documents, items) = read(body.into_bytes(), defaults()).unwrap();
        assert!(
            records(&mut documents)
                .iter()
                .all(|(_, filed)| filed == &self::filed(&[("index", "i")]))
        );
        let answer = serde_json::from_slice::<serde_json::Value>(&items.answer(Duration::ZERO)).unwrap();
        let mut ids: Vec<String> = self::items(&answer)
            .into_iter()
            .map(|(_, r)| r["_id"].as_str().unwrap().to_string())
            .collect();
        assert!(
            ids.iter()
                .all(|id| id.len() == 20 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
        );
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn error_and_cluster_are_elasticsearch_shaped() {
        let error: serde_json::Value =
            serde_json::from_slice(&error(400, "illegal_argument_exception", "why\"")).unwrap();
        assert_eq!(error["status"], 400);
        assert_eq!(error["error"]["type"], "illegal_argument_exception");
        assert_eq!(error["error"]["root_cause"][0]["reason"], "why\"");
        let license: serde_json::Value = serde_json::from_slice(&license()).unwrap();
        assert_eq!(license["license"]["status"], "active");
        let cluster: serde_json::Value = serde_json::from_slice(&cluster()).unwrap();
        assert_eq!(cluster["version"]["number"], VERSION);
        assert_eq!(cluster["tagline"], "You Know, for Search");
    }

    #[test]
    fn a_document_not_utf8_is_not_an_object() {
        assert!(!is_object(b"{\"a\":\"\xff\"}"));
        assert!(is_object("{\"a\":\"\u{e9}\"}".as_bytes()));
    }

    #[test]
    fn documents_and_items_cross_threads() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<Documents>();
        send_sync::<Items>();
    }
}
