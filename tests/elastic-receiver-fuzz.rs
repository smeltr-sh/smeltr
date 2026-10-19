//! Random `_bulk` requests against Elasticsearch receivers of random settings. A client of Elasticsearch acts on the
//! answer: it gives up on a request refused whole, and reads each item of one it answers to learn which documents were
//! created. So each answer must be shaped as Elasticsearch's, item by item, and each document an item says was created
//! must arrive once, as it was sent, filed under the index, id and pipeline its action or its request names. A receiver
//! holds no stored document and keeps no index, so it creates every `index` and `create` of an object, whatever index
//! and id it names.
//!
//! Each seed opens a receiver under settings an operator could save and sends it random requests, to every path it
//! serves and to paths beside them: bulk bodies of every action, whole or spoiled, under every way of presenting a
//! token, plain or gzip, within its limit or past it. A model built beside each request says what the receiver must
//! answer and which records it must give. A deterministic PRNG over a seed range, from `harness::fuzz`: a failure names
//! its seed, and the fuzzer checks that its seeds reached every option it knows.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{Receiver, SourceMeta};

use crate::harness::docker::{Container, Hook, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Answer, Pki, Presents, Request, Tls, commit};

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a sender reaches it.
    connect: SocketAddr,
    tokens: Vec<String>,
    tls: Tls,
    max_request_bytes: usize,
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (4 << 10, "limit under 4 KiB"),
    (64 << 10, "limit under 64 KiB"),
    (usize::MAX, "limit of 64 KiB or more"),
];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    // An API key's token is its `id:key`.
    let tokens = receiver::random_tokens(rng, tls, |rng, i| match rng.chance(2) {
        true => format!("id{i}:key{:x}", rng.next_u64()),
        false => format!("tok{i}-{:x}", rng.next_u64()),
    });
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tokens,
        tls,
        max_request_bytes: receiver::random_limit(rng, 11),
    }
}

fn open(settings: &Settings, pki: &Pki) -> Box<dyn Source> {
    let Settings {
        listen,
        tokens,
        tls,
        max_request_bytes,
        ..
    } = settings;
    receiver::build(
        Receiver::Elastic,
        *listen,
        *tls,
        tokens,
        "sender",
        *max_request_bytes,
        pki,
    )
}

/// A record a receiver gives: the document as sent, and what it is filed under.
#[derive(Clone, Debug, PartialEq)]
struct Taken {
    raw: String,
    index: String,
    id: Option<String>,
    pipeline: Option<String>,
}

/// What a bulk answer must say of one action.
#[derive(Clone, Debug)]
struct Item {
    action: &'static str,
    index: String,
    /// The id its action names; one created without one is given an id of the receiver's own.
    id: Option<String>,
    /// The error type of an action that took nothing.
    failed: Option<&'static str>,
}

/// Each route a request is sent to, `{index}` standing for an index its path names. A bulk route comes first.
const ROUTES: &[(&str, &str)] = &[
    ("POST", "/_bulk"),
    ("PUT", "/_bulk"),
    ("POST", "/{index}/_bulk"),
    ("PUT", "/{index}/_bulk"),
    ("POST", "/_bulk/"),
    ("PUT", "/{index}/_bulk/"),
    ("GET", "/"),
    ("HEAD", "/"),
    ("GET", "/_license"),
    ("GET", "/health"),
    ("GET", "/_bulk"),
    ("GET", "/{index}/_bulk"),
    ("DELETE", "/"),
    ("POST", "/_license"),
    ("POST", "/health"),
    ("PUT", "/_index_template/filebeat"),
    ("GET", "/_cat/indices"),
    ("POST", "/{index}/_doc"),
    ("POST", "/_bulk//"),
    ("POST", "/a/{index}/_bulk"),
];

/// The bulk routes, which most requests are sent to so most reach what the receiver makes of their bodies.
const BULK_ROUTES: usize = 6;

/// What a route serves.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Serves {
    Bulk,
    Cluster,
    License,
    Health,
    /// A path the receiver serves, by a method it does not take.
    Unmethod,
    Nothing,
}

fn serves(method: &str, route: &str) -> Serves {
    match (method, route) {
        ("POST" | "PUT", "/_bulk" | "/_bulk/" | "/{index}/_bulk" | "/{index}/_bulk/") => Serves::Bulk,
        ("GET" | "HEAD", "/") => Serves::Cluster,
        ("GET" | "HEAD", "/_license") => Serves::License,
        ("GET" | "HEAD", "/health") => Serves::Health,
        (_, "/" | "/_license" | "/_bulk" | "/_bulk/" | "/{index}/_bulk" | "/{index}/_bulk/" | "/health") => {
            Serves::Unmethod
        }
        _ => Serves::Nothing,
    }
}

/// An index a path names, as sent and as read.
const PATH_INDEXES: &[(&str, &str)] = &[
    ("logs", "logs"),
    ("filebeat-8.17.0-2026.10.14", "filebeat-8.17.0-2026.10.14"),
    ("logs-%C3%A9t%C3%A9", "logs-été"),
    ("a%2Fb", "a/b"),
    ("web+1", "web+1"),
];

/// A pipeline a query names, as sent and as read.
const QUERY_PIPELINES: &[(&str, &str)] = &[("tidy", "tidy"), ("geo+ip", "geo ip"), ("%C3%A9t%C3%A9", "été")];

/// Query keys a client sends that a receiver does not read: Filebeat's `filter_path` trims Elasticsearch's answer to
/// what it reads, which a receiver's whole answer holds.
const QUERY_EXTRAS: &[&str] = &[
    "refresh=true",
    "timeout=1m",
    "filter_path=errors,items.*.error,items.*.status",
];

const AUTHORIZATIONS: &[&str] = &[
    "no authorization",
    "a token it does not hold",
    "an API key it does not hold",
    "a password it does not hold",
    "a scheme Elasticsearch does not take",
    "a scheme and no token",
    "a token not ASCII",
    "Basic of no user and password",
    "Basic that is not base64",
    "a token as Bearer",
    "a token after a scheme of any case and two spaces",
    "an API key as sent",
    "an API key encoded",
    "a password",
];

/// The first of `AUTHORIZATIONS` a receiver admits.
const ADMITTED: usize = 9;

/// Why a receiver refuses a sender without credentials, and one whose credentials it does not hold.
const MISSING: &str = "missing authentication credentials";
const UNKNOWN: &str = "unable to authenticate with provided credentials";

const ENCODINGS: &[(Option<&str>, Option<bool>)] = &[
    (None, Some(false)),
    (Some("identity"), Some(false)),
    (Some("gzip"), Some(true)),
    (Some(" GZIP "), Some(true)),
    (Some("deflate"), None),
    (Some("gzip, identity"), None),
    (Some("br"), None),
];

const BODIES: &[&str] = &[
    "actions",
    "actions, one spoiling the request",
    "an empty body",
    "a body of blank lines",
    "a body past the limit",
    "a body of more actions than it answers",
];

/// Each action, by what it is given.
const ACTIONS: &[(&str, &str)] = &[
    ("index of an object", "index"),
    ("create of an object", "create"),
    ("index of a document not an object", "index"),
    ("create of a document not an object", "create"),
    ("update", "update"),
    ("delete", "delete"),
];

/// Document lines Elasticsearch does not index.
const NOT_OBJECTS: &[&[u8]] = &[
    b"[1,2]",
    b"\"text\"",
    b"42",
    b"null",
    b"true",
    b"  [ ]  ",
    b"{\"a\":",
    b"{\"a\":1} x",
    b"{\"a\":1}{\"b\":2}",
    b"not json",
    b"{\"a\":\"\xff\"}",
];

/// How a request is spoiled whole, and the action line that spoils it. The last two are built where they are sent.
const SPOILS: &[(&str, &str)] = &[
    ("an action line that is not JSON", "not json"),
    ("an action line that is not an object", "[1]"),
    ("an action line of no action", "{}"),
    ("an action line of two actions", r#"{"index":{},"create":{}}"#),
    ("an unknown action", r#"{"upsert":{}}"#),
    ("metadata that is not an object", r#"{"index":"logs"}"#),
    ("an index that is not a string", r#"{"index":{"_index":7}}"#),
    (
        "an id that is not a string",
        r#"{"create":{"_index":"logs","_id":{"a":1}}}"#,
    ),
    ("an id of null", r#"{"index":{"_index":"logs","_id":null}}"#),
    (
        "a pipeline that is not a string",
        r#"{"index":{"_index":"logs","pipeline":true}}"#,
    ),
    ("an action with no document line", ""),
    ("an action naming no index where the path names none", ""),
];

const NO_DOCUMENT: usize = SPOILS.len() - 2;
const NO_INDEX: usize = SPOILS.len() - 1;

/// What an action names, of each kind, as read.
const INDEXES: &[&str] = &["logs", "web-été", "filebeat-8.17.0", "a b", "say \"hi\""];
const IDS: &[&str] = &["a1", "évé", "x/y", "1"];
const PIPELINES: &[&str] = &["parse", "tidy", "geo ip"];

/// Metadata a client sends that a receiver does not read, as JSON.
const EXTRAS: &[(&str, &str)] = &[
    ("routing", r#""r1""#),
    ("_type", r#""_doc""#),
    ("require_alias", "false"),
    ("if_seq_no", "3"),
    ("dynamic_templates", r#"{"a":"b"}"#),
];

/// Whitespace JSON allows between tokens on one line.
const SPACES: &[&str] = &["", "", " ", "\t", "  "];

/// Lines of nothing but whitespace, which a bulk body may hold anywhere.
const BLANKS: &[&[u8]] = &[b"", b"  ", b"\t", b" \t "];

/// How far a receiver takes a request, in the order it takes it: its route, the token it presents, its size and
/// encoding as it is read, what its body reads as, and the answer to each of its actions.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum Stage {
    Route,
    Token,
    Read,
    Body,
    Items,
}

/// What a request tries, each by the stage the receiver acts on it at: a label is reached only by a request the
/// receiver took that far.
#[derive(Default)]
struct Labels(Vec<(Stage, String)>);

impl Labels {
    fn at(&mut self, stage: Stage, label: impl Into<String>) {
        self.0.push((stage, label.into()));
    }
}

/// A request's answer, as the receiver must give it.
#[derive(Debug)]
enum Expected {
    /// No HTTP at all: TLS refused the sender.
    Nothing,
    /// A status the receiver gives as any HTTP server does, saying nothing more.
    Status(u16),
    /// An Elasticsearch error: its status, its type, and words its reason holds.
    Error(u16, &'static str, Option<String>),
    Cluster,
    License,
    /// Each action answered, and the records taken.
    Bulk(Vec<Item>, Vec<Taken>),
}

/// One request a sender makes, and what the receiver must answer it.
struct Sent {
    method: &'static str,
    /// The route its path is one of.
    route: &'static str,
    path: String,
    /// Header lines, each ending in CRLF.
    headers: Vec<u8>,
    body: Vec<u8>,
    presents: Presents,
    expected: Expected,
    /// What it tries, and how far the receiver takes it: `None` where TLS refused it.
    labels: Labels,
    reached: Option<Stage>,
}

/// A JSON string holding `text`, every character of it written as a `\u` escape where `escaped`.
fn json_string(text: &str, escaped: bool) -> String {
    if !escaped {
        return serde_json::to_string(text).unwrap();
    }
    let mut units = [0u16; 2];
    let escapes = text
        .chars()
        .flat_map(|c| c.encode_utf16(&mut units).to_vec())
        .map(|unit| format!("\\u{unit:04x}"));
    format!("\"{}\"", escapes.collect::<String>())
}

/// A random JSON value on one line, `depth` deep in a document.
fn value(rng: &mut Rng, depth: usize) -> String {
    let kinds = if depth < 2 { 9 } else { 7 };
    match rng.below(kinds) {
        0 => json_string(
            rng.pick(&["a line", "été 🎉", "say \"hi\"", "tab\there", "back\\slash", ""]),
            false,
        ),
        1 => json_string("été", true),
        2 => (rng.next_u64() as i64 >> rng.below(64)).to_string(),
        3 => rng.pick(&["2.50", "-1e3", "0.1", "1E-7", "-0"]).to_string(),
        4 => rng.pick(&["true", "false"]).to_string(),
        5 => "null".to_string(),
        6 => r#""2026-10-14T12:00:00.000Z""#.to_string(),
        7 => {
            let s = rng.pick(SPACES);
            let items: Vec<String> = (0..rng.below(4)).map(|_| value(rng, depth + 1)).collect();
            format!("[{s}{}{s}]", items.join(&format!("{s},{s}")))
        }
        _ => object(rng, depth + 1),
    }
}

/// A random JSON object on one line, `depth` deep in a document.
fn object(rng: &mut Rng, depth: usize) -> String {
    let mut keys = vec!["message", "@timestamp", "host", "n", "été", "a\"b", "tags"];
    // Each key once: Elasticsearch refuses a document that names one twice.
    let members: Vec<String> = (0..rng.below(4) + usize::from(depth == 0))
        .map(|_| {
            let s = rng.pick(SPACES);
            let key = keys.remove(rng.below(keys.len()));
            let key = json_string(key, rng.chance(8));
            format!("{s}{key}{s}:{s}{}{s}", value(rng, depth))
        })
        .collect();
    format!("{{{}}}", members.join(","))
}

/// A bulk body as it is written: its lines, each numbered as the receiver numbers them, blank lines included.
struct Lines {
    body: Vec<u8>,
    n: usize,
    crlf: bool,
    blanks: bool,
    /// Whether a blank line was written.
    blanked: bool,
}

impl Lines {
    /// Writes `text` as the next line, after a blank line where the body has them, and gives its number.
    fn line(&mut self, rng: &mut Rng, text: &[u8]) -> usize {
        if self.blanks && rng.chance(3) {
            self.blanked = true;
            self.body.extend(*rng.pick(BLANKS));
            self.end(rng);
        }
        self.body.extend(text);
        self.end(rng);
        self.n
    }

    fn end(&mut self, rng: &mut Rng) {
        self.body.extend(if self.crlf && rng.chance(2) {
            &b"\r\n"[..]
        } else {
            b"\n"
        });
        self.n += 1;
    }
}

/// What a body reads as: each action answered and the records taken, or why the request is refused whole.
type BodyRead = Result<(Vec<Item>, Vec<Taken>), Expected>;

/// A bulk body to a path naming `path_index` and a query naming `query_pipeline`, with an action spoiling the request
/// where `spoiled`, and what it reads as.
fn bulk(
    rng: &mut Rng,
    path_index: Option<&str>,
    query_pipeline: Option<&str>,
    spoiled: bool,
    labels: &mut Labels,
) -> (Vec<u8>, BodyRead) {
    let mut lines = Lines {
        body: Vec::new(),
        n: 0,
        crlf: rng.chance(3),
        blanks: rng.chance(3),
        blanked: false,
    };
    let actions = 1 + rng.below(6);
    let spoil = spoiled.then(|| match rng.below(SPOILS.len()) {
        NO_INDEX if path_index.is_some() => rng.below(NO_INDEX),
        spoil => spoil,
    });
    // Where among the actions the spoiling line goes: an action missing its document line is the last line.
    let spoil_at = spoil.map(|spoil| match spoil {
        NO_DOCUMENT => actions,
        _ => rng.below(actions + 1),
    });
    let malformed = |n: usize| Expected::Error(400, "illegal_argument_exception", Some(format!("[{n}]")));
    let mut refused = None;
    let (mut items, mut taken) = (Vec::new(), Vec::new());
    for i in 0..=actions {
        if spoil_at == Some(i) {
            let spoil = spoil.unwrap();
            labels.at(Stage::Body, SPOILS[spoil].0);
            refused = Some(match spoil {
                NO_DOCUMENT => {
                    let action = rng.pick(&["index", "create", "update"]);
                    malformed(lines.line(rng, format!(r#"{{"{action}":{{"_index":"logs"}}}}"#).as_bytes()))
                }
                NO_INDEX => {
                    let action = rng.pick(&["index", "create", "delete"]);
                    lines.line(rng, format!(r#"{{"{action}":{{"_id":"x"}}}}"#).as_bytes());
                    if *action != "delete" {
                        lines.line(rng, b"{}");
                    }
                    Expected::Error(
                        400,
                        "action_request_validation_exception",
                        Some("index is missing".into()),
                    )
                }
                _ => malformed(lines.line(rng, SPOILS[spoil].1.as_bytes())),
            });
        }
        if i == actions {
            break;
        }
        let (item, record) = action(rng, &mut lines, path_index, query_pipeline, labels);
        items.push(item);
        taken.extend(record);
    }
    if rng.chance(3) {
        labels.at(Stage::Items, "a last line with no line end");
        let end = if lines.body.ends_with(b"\r\n") { 2 } else { 1 };
        lines.body.truncate(lines.body.len() - end);
    }
    if lines.crlf && lines.body.windows(2).any(|w| w == b"\r\n") {
        labels.at(Stage::Items, "CRLF line ends");
    }
    if lines.blanked {
        labels.at(Stage::Items, "blank lines between lines");
    }
    match refused {
        Some(refused) => (lines.body, Err(refused)),
        None => (lines.body, Ok((items, taken))),
    }
}

/// Writes a random action, and its document line where it has one: what its item must say, and the record it takes.
fn action(
    rng: &mut Rng,
    lines: &mut Lines,
    path_index: Option<&str>,
    query_pipeline: Option<&str>,
    labels: &mut Labels,
) -> (Item, Option<Taken>) {
    let (label, name) = *rng.pick(ACTIONS);
    labels.at(Stage::Items, label);
    let escaped = rng.chance(6);
    if escaped {
        labels.at(Stage::Items, "a name written with escapes");
    }
    // Each metadata key, as JSON, and its value, as JSON.
    let mut meta: Vec<(String, String)> = Vec::new();
    let own_index = (path_index.is_none() || rng.chance(2)).then(|| *rng.pick(INDEXES));
    if let Some(index) = own_index {
        meta.push((r#""_index""#.into(), json_string(index, escaped)));
    }
    labels.at(
        Stage::Items,
        match own_index {
            Some(_) => "an index in the action",
            None => "an index from the path",
        },
    );
    let id = rng.chance(2).then(|| *rng.pick(IDS));
    if let Some(id) = id {
        meta.push((json_string("_id", escaped), json_string(id, escaped)));
    }
    labels.at(Stage::Items, if id.is_some() { "an id" } else { "no id" });
    let own_pipeline = rng.chance(3).then(|| *rng.pick(PIPELINES));
    if let Some(pipeline) = own_pipeline {
        meta.push((r#""pipeline""#.into(), json_string(pipeline, escaped)));
    }
    labels.at(
        Stage::Items,
        match (own_pipeline, query_pipeline) {
            (Some(_), _) => "a pipeline in the action",
            (None, Some(_)) => "a pipeline from the query",
            (None, None) => "no pipeline",
        },
    );
    if rng.chance(4) {
        let (key, value) = *rng.pick(EXTRAS);
        meta.push((json_string(key, false), value.into()));
        labels.at(Stage::Items, "a key the receiver does not read");
    }
    for i in (1..meta.len()).rev() {
        meta.swap(i, rng.below(i + 1));
    }
    let s = |rng: &mut Rng| *rng.pick(SPACES);
    let members: Vec<String> = meta
        .iter()
        .map(|(key, value)| format!("{}{key}{}:{}{value}{}", s(rng), s(rng), s(rng), s(rng)))
        .collect();
    let line = format!(
        "{}{{{}{}{}:{}{{{}}}{}}}",
        s(rng),
        s(rng),
        json_string(name, escaped),
        s(rng),
        s(rng),
        members.join(","),
        s(rng)
    );
    lines.line(rng, line.as_bytes());

    let index = own_index.or(path_index).unwrap().to_string();
    let item = |failed| Item {
        action: name,
        index: index.clone(),
        id: id.map(str::to_string),
        failed,
    };
    match label {
        "index of an object" | "create of an object" => {
            let around = rng.chance(5);
            if around {
                labels.at(Stage::Items, "a document with space around it");
            }
            let document = match around {
                true => format!("{}{}{}", rng.pick(&[" ", "\t "]), object(rng, 0), rng.pick(&["", " "])),
                false => object(rng, 0),
            };
            lines.line(rng, document.as_bytes());
            let taken = Taken {
                raw: document,
                index: index.clone(),
                id: id.map(str::to_string),
                pipeline: own_pipeline.or(query_pipeline).map(str::to_string),
            };
            (item(None), Some(taken))
        }
        "update" => {
            let document = format!(r#"{{"doc":{}}}"#, object(rng, 1));
            lines.line(rng, document.as_bytes());
            (item(Some("illegal_argument_exception")), None)
        }
        "delete" => (item(Some("illegal_argument_exception")), None),
        _ => {
            let document = *rng.pick(NOT_OBJECTS);
            lines.line(rng, document);
            (item(Some("document_parsing_exception")), None)
        }
    }
}

/// Newlines enough that answering them holds more than a receiver holds: each line may be an action, answered in up to
/// 256 bytes (`elastic_receiver::ANSWER_BYTES`).
const TOO_MANY_LINES: usize = smeltr::io::receiver::INBOX_BYTES / 256 + 1;

/// A random request to `settings`'s receiver, and what it must answer: the route, then the token, then the request's
/// size and framing, then what its body reads as, as the receiver takes them in that order.
fn random_request(rng: &mut Rng, settings: &Settings, cov: &mut Coverage) -> Sent {
    let (method, route) = match rng.chance(5) {
        true => *rng.pick(ROUTES),
        false => *rng.pick(&ROUTES[..BULK_ROUTES]),
    };
    let mut labels = Labels::default();
    labels.at(Stage::Route, format!("{method} {route}"));
    let serves = serves(method, route);
    let path_index = route.contains("{index}").then(|| *rng.pick(PATH_INDEXES));
    let mut path = match path_index {
        Some((sent, _)) => route.replace("{index}", sent),
        None => route.to_string(),
    };
    let path_index = path_index.map(|(_, read)| read);
    let presents = match settings.tls {
        Tls::None => Presents::Nothing,
        _ => *rng.pick(&[
            Presents::Trusted,
            Presents::Trusted,
            Presents::Trusted,
            Presents::Nothing,
            Presents::Stranger,
        ]),
    };
    cov.reach(format!("presents {presents:?} under {:?}", settings.tls));
    // Most requests present a token the receiver holds, so most reach what follows the token.
    let well_sent = !rng.chance(4);

    let right = match settings.tokens.is_empty() {
        true => String::new(),
        false => rng.pick(&settings.tokens).clone(),
    };
    let authorization = match well_sent {
        true => ADMITTED + rng.below(AUTHORIZATIONS.len() - ADMITTED),
        false => rng.below(AUTHORIZATIONS.len()),
    };
    // An API key is its `id:key`; a token of no colon presented as one is a label of its own.
    labels.at(
        Stage::Token,
        match (authorization, right.contains(':')) {
            (11 | 12, false) => "a token of no colon as an API key",
            _ => AUTHORIZATIONS[authorization],
        },
    );
    let base64 = |text: &str| {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(text)
    };
    // An `Authorization` value, and why it is refused where it is.
    let (authorization, refused): (Option<Vec<u8>>, Option<&str>) = match authorization {
        0 => (None, Some(MISSING)),
        1 => (Some(format!("Bearer {right}x").into()), Some(UNKNOWN)),
        2 => (
            Some(format!("ApiKey {}", base64(&format!("{right}x"))).into()),
            Some(UNKNOWN),
        ),
        3 => (
            Some(format!("Basic {}", base64(&format!("elastic:{right}x"))).into()),
            Some(UNKNOWN),
        ),
        4 => (Some(format!("Splunk {right}").into()), Some(UNKNOWN)),
        5 => (Some(b"Bearer".to_vec()), Some(UNKNOWN)),
        6 => (Some(b"Bearer \xe9\xff".to_vec()), Some(UNKNOWN)),
        7 => (Some(format!("Basic {}", base64("elastic")).into()), Some(UNKNOWN)),
        8 => (Some(b"Basic !!!".to_vec()), Some(UNKNOWN)),
        9 => (Some(format!("Bearer {right}").into()), None),
        10 => (Some(format!("bEaReR  {right} ").into()), None),
        11 => (Some(format!("ApiKey {right}").into()), None),
        12 => (Some(format!("ApiKey {}", base64(&right)).into()), None),
        _ => (
            Some(format!("Basic {}", base64(&format!("elastic:{right}"))).into()),
            None,
        ),
    };
    // A receiver holding no token admits whoever TLS admitted.
    let refused = refused.filter(|_| !settings.tokens.is_empty());

    let max = settings.max_request_bytes;
    let mut headers = Vec::new();
    let (mut body, mut read, mut encoding, mut refused_reading) =
        (Vec::new(), Ok((Vec::new(), Vec::new())), None, None);
    if serves == Serves::Bulk {
        let mut query = Vec::new();
        let query_pipeline = rng.chance(4).then(|| *rng.pick(QUERY_PIPELINES));
        if let Some((sent, _)) = query_pipeline {
            query.push(format!("pipeline={sent}"));
        }
        if rng.chance(4) {
            query.push(rng.pick(QUERY_EXTRAS).to_string());
            labels.at(Stage::Items, "a query key the receiver does not read");
        }
        if !query.is_empty() {
            path = format!("{path}?{}", query.join("&"));
        }
        let query_pipeline = query_pipeline.map(|(_, read)| read);
        let kind = match (well_sent, rng.below(BODIES.len())) {
            (true, _) => rng.below(2),
            (false, 5) if max <= TOO_MANY_LINES => 0,
            (false, kind) => kind,
        };
        // A body past the limit is refused for its size, before what it reads as.
        let stage = if kind == 4 { Stage::Read } else { Stage::Body };
        labels.at(stage, BODIES[kind]);
        (body, read) = match kind {
            0 | 1 => bulk(rng, path_index, query_pipeline, kind == 1, &mut labels),
            2 | 3 => {
                let body = if kind == 2 { Vec::new() } else { b"\n \r\n\t\n".to_vec() };
                let why = Some("no requests added".to_string());
                (
                    body,
                    Err(Expected::Error(400, "action_request_validation_exception", why)),
                )
            }
            4 => {
                let document = format!(r#"{{"message":"{}"}}"#, "x".repeat(max + 1));
                let body = format!("{{\"index\":{{\"_index\":\"logs\"}}}}\n{document}\n");
                (body.into_bytes(), Err(Expected::Error(413, "content_too_long", None)))
            }
            _ => {
                let why = Some("fewer actions".to_string());
                (
                    vec![b'\n'; TOO_MANY_LINES],
                    Err(Expected::Error(413, "content_too_long", why)),
                )
            }
        };
        let gzip;
        (encoding, gzip) = match well_sent {
            true => ENCODINGS[rng.below(4)],
            false => *rng.pick(ENCODINGS),
        };
        let inflated = body.len();
        if gzip == Some(true) {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&body).unwrap();
            body = encoder.finish().unwrap();
        }
        // A body that says it is gzip and is not, which `well_sent` never sends.
        let mislabeled = gzip == Some(true) && !well_sent && kind != 4 && rng.chance(3);
        if mislabeled {
            labels.at(Stage::Read, "a body that is not the gzip it says");
            body = b"not gzip".to_vec();
        }
        // A body declared past the limit is refused before its encoding is read.
        if body.len() <= max {
            labels.at(Stage::Read, format!("Content-Encoding {encoding:?}"));
        }
        // Refused for its size or encoding, it never reaches what its body reads as.
        refused_reading = match () {
            _ if body.len() > max => Some(Expected::Error(413, "content_too_long", None)),
            _ if gzip.is_none() => Some(Expected::Error(415, "illegal_argument_exception", None)),
            _ if mislabeled => Some(Expected::Error(400, "parse_exception", None)),
            _ if inflated > max => Some(Expected::Error(413, "content_too_long", None)),
            _ => None,
        };
        if rng.chance(2) {
            headers.extend(b"Content-Type: application/x-ndjson\r\n");
        }
    }
    if let Some(authorization) = authorization {
        headers.extend(b"Authorization: ");
        headers.extend(authorization);
        headers.extend(b"\r\n");
    }
    if let Some(encoding) = encoding {
        headers.extend(format!("Content-Encoding: {encoding}\r\n").bytes());
    }

    let (expected, reached) = if settings.tls == Tls::Vouched && presents != Presents::Trusted {
        (Expected::Nothing, None)
    } else {
        let (expected, reached) = match serves {
            Serves::Unmethod => (Expected::Status(405), Stage::Route),
            Serves::Health => (Expected::Status(200), Stage::Route),
            _ if refused.is_some() => {
                let why = refused.map(str::to_string);
                (Expected::Error(401, "security_exception", why), Stage::Token)
            }
            Serves::Cluster => (Expected::Cluster, Stage::Token),
            Serves::License => (Expected::License, Stage::Token),
            Serves::Nothing => (Expected::Error(404, "resource_not_found_exception", None), Stage::Token),
            Serves::Bulk => match (refused_reading, read) {
                (Some(refused), _) => (refused, Stage::Read),
                (None, Ok((items, taken))) => (Expected::Bulk(items, taken), Stage::Items),
                (None, Err(refused)) => (refused, Stage::Body),
            },
        };
        (expected, Some(reached))
    };
    // A receiver holding no token reads none.
    labels
        .0
        .retain(|(stage, _)| *stage != Stage::Token || !settings.tokens.is_empty());
    Sent {
        method,
        route,
        path,
        headers,
        body,
        presents,
        expected,
        labels,
        reached,
    }
}

/// `sent` over a new connection to `at`, under TLS where `tls` says.
fn exchange(at: SocketAddr, tls: Tls, pki: &Pki, sent: &Sent) -> Answer {
    let request = Request {
        method: sent.method,
        path: &sent.path,
        headers: &sent.headers,
        body: &sent.body,
        chunked: false,
        presents: sent.presents,
    };
    receiver::exchange(at, tls, pki, &request)
}

/// What a receiver answers at `route` to `method`. 0 is no answer: a connection its TLS refused.
fn documented(method: &str, route: &str) -> &'static [u16] {
    match serves(method, route) {
        Serves::Bulk => &[0, 200, 400, 401, 408, 413, 415, 429],
        Serves::Cluster | Serves::License => &[0, 200, 401],
        Serves::Health => &[0, 200, 503],
        Serves::Unmethod => &[0, 405],
        Serves::Nothing => &[0, 401, 404],
    }
}

/// The records as [`Taken`]s, or why one is not filed as from `remote`, by an Elasticsearch receiver.
fn read_back(records: &[SourceRecord], remote: &str) -> Result<Vec<Taken>, String> {
    let objects = SourceMeta::ELASTIC_RECEIVER.objects(records, Some("es"));
    (records.iter().zip(objects))
        .map(|(r, object)| {
            let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
            if text("remote").as_deref() != Some(remote) || text("type").as_deref() != Some("elasticReceiver") {
                return Err(format!("a record filed as from another: {object:?}"));
            }
            Ok(Taken {
                raw: String::from_utf8_lossy(r.bytes().unwrap()).to_string(),
                index: text("index").ok_or_else(|| format!("a record filed under no index: {object:?}"))?,
                id: text("id"),
                pipeline: text("pipeline"),
            })
        })
        .collect()
}

/// Why `answer` is not what `sent` expected, or `Ok`. Each id the receiver made up goes in `made`, which none may
/// repeat.
fn check_answer(sent: &Sent, answer: &Answer, made: &mut BTreeSet<String>) -> Result<(), String> {
    let json: Value = serde_json::from_str(&answer.body).unwrap_or(Value::Null);
    let status = answer.status;
    // A client reads no answer from a server that does not name itself Elasticsearch.
    let named = answer.head.contains("\r\nx-elastic-product: elasticsearch");
    let wanted = match &sent.expected {
        Expected::Nothing => 0,
        Expected::Status(status) | Expected::Error(status, ..) => *status,
        Expected::Cluster | Expected::License | Expected::Bulk(..) => 200,
    };
    if status != wanted {
        return Err(format!("answered {status} {:?}, not {:?}", answer.body, sent.expected));
    }
    if sent.method == "HEAD" || matches!(sent.expected, Expected::Nothing | Expected::Status(_)) {
        return Ok(());
    }
    if !named {
        return Err(format!(
            "answered {status} without naming itself Elasticsearch: {:?}",
            answer.head
        ));
    }
    match &sent.expected {
        Expected::Error(status, kind, why) => {
            let reason = json["error"]["reason"].as_str().unwrap_or_default();
            if json["status"] != *status
                || json["error"]["type"] != *kind
                || json["error"]["root_cause"][0]["type"] != *kind
                || why.as_ref().is_some_and(|why| !reason.contains(why.as_str()))
            {
                return Err(format!("answered {json}, not {kind} for {why:?}"));
            }
            if *status == 401
                && !answer
                    .head
                    .contains("\r\nwww-authenticate: basic realm=\"security\", apikey, bearer")
            {
                return Err(format!("refused without naming its schemes: {:?}", answer.head));
            }
            Ok(())
        }
        // A client checks the major version it sends to, and a Beat the license it sends under.
        Expected::Cluster if json["version"]["number"].as_str().is_some_and(|v| v.starts_with("8.")) => Ok(()),
        Expected::License if json["license"]["status"] == "active" => Ok(()),
        Expected::Bulk(items, _) => check_items(items, &json, answer, made),
        expected => Err(format!("answered {json}, not {expected:?}")),
    }
}

/// Why a bulk answer `json` does not answer `items`, or `Ok`.
fn check_items(items: &[Item], json: &Value, answer: &Answer, made: &mut BTreeSet<String>) -> Result<(), String> {
    if !answer.head.contains("\r\ncontent-type: application/json") {
        return Err(format!("a bulk answer not of JSON: {:?}", answer.head));
    }
    let errors = items.iter().any(|item| item.failed.is_some());
    let answered = json["items"].as_array().cloned().unwrap_or_default();
    if !json["took"].is_u64() || json["errors"] != errors || answered.len() != items.len() {
        return Err(format!("answered {json}, not {} items, errors {errors}", items.len()));
    }
    for (n, (item, answered)) in items.iter().zip(&answered).enumerate() {
        let wrong = |why: &str| Err(format!("item {n}, {item:?}: {why}: {answered}"));
        let Some(result) = answered
            .as_object()
            .filter(|o| o.len() == 1)
            .and_then(|o| o.get(item.action))
        else {
            return wrong("not answered as its action");
        };
        if result["_index"] != item.index.as_str() {
            return wrong("answered under another index");
        }
        match (item.failed, &item.id) {
            (None, _) if result["status"] != 201 || result["result"] != "created" => return wrong("not created"),
            (Some(kind), _) if result["status"] != 400 || result["error"]["type"] != kind => {
                return wrong("not refused as it must be");
            }
            (_, Some(id)) if result["_id"] != id.as_str() => return wrong("answered under another id"),
            (Some(_), None) if !result["_id"].is_null() => return wrong("given an id though nothing was created"),
            (None, None) => {
                // Shaped as Elasticsearch's own: 20 URL-safe base64 characters.
                let id = result["_id"].as_str().unwrap_or_default();
                let shaped = id.len() == 20 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
                if !shaped || !made.insert(id.to_string()) {
                    return wrong("given an id not shaped as Elasticsearch's, or given before");
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// A receiver under any settings an operator can save, sent random requests to every path, answers each as
/// Elasticsearch does, item by item, and takes exactly the documents its answers say were created, as they were sent
/// and filed as their actions or their requests say.
#[test]
fn every_setting_answers_random_bulk_requests_as_elasticsearch_does() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let statuses = [0, 200, 400, 401, 404, 405, 413, 415];
    let mut cov = Coverage::new(
        receiver::listen_names(ipv6)
            .map(|ip| format!("listen {ip}"))
            .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
            .chain((0..=3).map(|n| format!("tokens {n}")))
            .chain(
                [
                    "an API key's id:key",
                    "a token of no colon",
                    "a token of no colon as an API key",
                ]
                .map(String::from),
            )
            .chain(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()))
            .chain(ROUTES.iter().map(|(method, route)| format!("{method} {route}")))
            .chain(["presents Nothing under None".to_string()])
            .chain(["Served", "Vouched"].iter().flat_map(|tls| {
                ["Nothing", "Trusted", "Stranger"].map(|presents| format!("presents {presents} under {tls}"))
            }))
            .chain(AUTHORIZATIONS.iter().map(|s| s.to_string()))
            .chain(ENCODINGS.iter().map(|(e, _)| format!("Content-Encoding {e:?}")))
            .chain(BODIES.iter().map(|s| s.to_string()))
            .chain(ACTIONS.iter().map(|(s, _)| s.to_string()))
            .chain(SPOILS.iter().map(|(s, _)| s.to_string()))
            .chain(
                [
                    "a body that is not the gzip it says",
                    "a name written with escapes",
                    "an index in the action",
                    "an index from the path",
                    "an id",
                    "no id",
                    "a pipeline in the action",
                    "a pipeline from the query",
                    "no pipeline",
                    "a key the receiver does not read",
                    "a query key the receiver does not read",
                    "a document with space around it",
                    "a last line with no line end",
                    "CRLF line ends",
                    "blank lines between lines",
                    "records of a request answered",
                ]
                .map(String::from),
            )
            .chain(statuses.iter().map(|status| format!("answered {status}"))),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("tokens {}", settings.tokens.len()));
        for token in &settings.tokens {
            cov.reach(match token.contains(':') {
                true => "an API key's id:key",
                false => "a token of no colon",
            });
        }
        let limit = RECEIVER_LIMITS
            .iter()
            .find(|(below, _)| settings.max_request_bytes < *below)
            .unwrap();
        cov.reach(limit.1);
        let mut source = open(&settings, &pki);
        let sent: Vec<Sent> = (0..12).map(|_| random_request(&mut rng, &settings, &mut cov)).collect();
        let sending = Arc::new(sent);
        let (connect, tls) = (settings.connect, settings.tls);
        let requests = sending.clone();
        let senders_pki = pki.clone();
        let sender = std::thread::spawn(move || {
            (requests.iter())
                .map(|sent| exchange(connect, tls, &senders_pki, sent))
                .collect::<Vec<_>>()
        });
        // A bulk request is answered once its documents are committed, so they are taken as they arrive.
        let remote = settings.connect.ip().to_string();
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut records = Vec::new();
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err("the sender is still waiting".into());
            }
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(10)).unwrap();
            commit(&mut source, &batch);
            records.extend(read_back(&batch, &remote)?);
        }
        let answers = sender.join().unwrap();
        let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(50)).unwrap();
        commit(&mut source, &batch);
        records.extend(read_back(&batch, &remote)?);

        let mut made = BTreeSet::new();
        let mut expected_records = Vec::new();
        for (i, (sent, answer)) in sending.iter().zip(&answers).enumerate() {
            let what = format!(
                "request {i} ({} {} under {:?}, presenting {:?}, {} bytes, limit {}, headers {:?}, body {:?})",
                sent.method,
                sent.path,
                settings.tls,
                sent.presents,
                sent.body.len(),
                settings.max_request_bytes,
                String::from_utf8_lossy(&sent.headers),
                String::from_utf8_lossy(&sent.body[..sent.body.len().min(2048)]),
            );
            if !documented(sent.method, sent.route).contains(&answer.status) {
                return Err(format!(
                    "{what}: answered {}, which it does not document",
                    answer.status
                ));
            }
            check_answer(sent, answer, &mut made).map_err(|why| format!("{what}: {why}"))?;
            cov.reach(format!("answered {}", answer.status));
            for (stage, label) in &sent.labels.0 {
                if sent.reached.is_some_and(|reached| *stage <= reached) {
                    cov.reach(label.clone());
                }
            }
            if let Expected::Bulk(_, taken) = &sent.expected {
                if !taken.is_empty() {
                    cov.reach("records of a request answered");
                }
                expected_records.extend(taken.iter().cloned());
            }
        }
        if records != expected_records {
            return Err(format!(
                "took {} records, not the {} its answers say were created:\ntook     {records:?}\nexpected \
                 {expected_records:?}",
                records.len(),
                expected_records.len()
            ));
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A real sender of Elasticsearch's `_bulk` API, tailing a file.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Sender {
    Filebeat,
    /// Fluent Bit's `es` output.
    FluentBit,
    /// The OpenTelemetry Collector's `elasticsearch` exporter.
    Collector,
}

/// How a sender presents its token.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Credentials {
    /// None: the receiver holds no token, and its authorities admit the sender.
    Nothing,
    /// An API key, as its `id:key`.
    ApiKey,
    /// A user and password, the password the token.
    Password,
}

/// What a live seed told its sender, and so what each record it sends must carry.
#[derive(Debug)]
struct Told {
    credentials: Credentials,
    gzip: bool,
    /// The index it sends to, where it is told one.
    index: Option<&'static str>,
    pipeline: Option<&'static str>,
    /// Fluent Bit: an index of the day, under this prefix.
    logstash: Option<&'static str>,
    /// Fluent Bit: an id of its own for each document.
    ids: bool,
    /// Fluent Bit: `create` actions rather than `index`.
    create: bool,
    /// Filebeat: the most documents to a request.
    bulk: Option<usize>,
    /// The collector: how it maps a log record to a document. Not `bodymap`, which drops a record whose body is no map,
    /// as a tailed line's is not.
    mapping: Option<&'static str>,
}

/// Filebeat's configuration: tail `app.log`, and send to `at` as `told` says, presenting `token`, over TLS when `tls`
/// is not None.
fn filebeat_config(at: SocketAddr, token: &str, tls: Tls, told: &Told) -> String {
    let scheme = if tls == Tls::None { "http" } else { "https" };
    let mut output = format!(
        "  hosts: [\"{scheme}://localhost:{}\"]\n  compression_level: {}\n  backoff.init: 100ms\n  backoff.max: 1s\n",
        at.port(),
        if told.gzip { 5 } else { 0 }
    );
    match told.credentials {
        Credentials::Nothing => {}
        Credentials::ApiKey => output.push_str(&format!("  api_key: \"{token}\"\n")),
        Credentials::Password => output.push_str(&format!("  username: elastic\n  password: \"{token}\"\n")),
    }
    if let Some(index) = told.index {
        output.push_str(&format!("  index: \"{index}\"\n"));
    }
    if let Some(pipeline) = told.pipeline {
        output.push_str(&format!("  pipeline: \"{pipeline}\"\n"));
    }
    if let Some(bulk) = told.bulk {
        output.push_str(&format!("  bulk_max_size: {bulk}\n"));
    }
    if tls != Tls::None {
        output.push_str("  ssl.certificate_authorities: [/smeltr/ca.pem]\n");
    }
    if tls == Tls::Vouched {
        output.push_str("  ssl.certificate: /smeltr/sender.pem\n  ssl.key: /smeltr/sender-key.pem\n");
    }
    // A receiver has no templates or lifecycle policies to set up, and an index of its own names a template.
    format!(
        "filebeat.inputs:\n  - type: filestream\n    id: app\n    paths: [/smeltr/app.log]\noutput.elasticsearch:\n\
         {output}setup.template.enabled: false\nsetup.template.name: fuzz\nsetup.template.pattern: \"fuzz-*\"\n\
         setup.ilm.enabled: false\n"
    )
}

/// Fluent Bit's configuration: tail `app.log`, and send to `at` as `told` says, presenting `token`, over TLS when `tls`
/// is not None.
fn fluent_bit_config(at: SocketAddr, token: &str, tls: Tls, told: &Told) -> String {
    let mut output = format!(
        "[OUTPUT]\n    Name es\n    Match *\n    Host localhost\n    Port {}\n    Suppress_Type_Name On\n    \
         Retry_Limit 10\n    Write_Operation {}\n",
        at.port(),
        if told.create { "create" } else { "index" }
    );
    if told.credentials == Credentials::Password {
        output.push_str(&format!("    HTTP_User elastic\n    HTTP_Passwd {token}\n"));
    }
    if let Some(index) = told.index {
        output.push_str(&format!("    Index {index}\n"));
    }
    if let Some(prefix) = told.logstash {
        output.push_str(&format!("    Logstash_Format On\n    Logstash_Prefix {prefix}\n"));
    }
    if let Some(pipeline) = told.pipeline {
        output.push_str(&format!("    Pipeline {pipeline}\n"));
    }
    if told.ids {
        output.push_str("    Generate_ID On\n");
    }
    if told.gzip {
        output.push_str("    Compress gzip\n");
    }
    if tls != Tls::None {
        output.push_str("    TLS On\n    TLS.Verify On\n    TLS.CA_File /smeltr/ca.pem\n    TLS.Vhost localhost\n");
    }
    if tls == Tls::Vouched {
        output.push_str("    TLS.Crt_File /smeltr/sender.pem\n    TLS.Key_File /smeltr/sender-key.pem\n");
    }
    format!(
        "[SERVICE]\n    Flush 1\n    Grace 1\n    Log_Level info\n[INPUT]\n    Name tail\n    Path /smeltr/app.log\n    \
         Read_from_Head On\n{output}"
    )
}

/// The collector's configuration: tail `app.log`, and export to `at` as `told` says, presenting `token`, over TLS when
/// `tls` is not None.
fn collector_config(at: SocketAddr, token: &str, tls: Tls, told: &Told) -> String {
    use base64::Engine;
    let scheme = if tls == Tls::None { "http" } else { "https" };
    let mut exporter = format!(
        "    endpoint: {scheme}://localhost:{}\n    compression: {}\n    retry: {{enabled: true, initial_interval: \
         100ms, max_interval: 1s}}\n",
        at.port(),
        if told.gzip { "gzip" } else { "none" }
    );
    match told.credentials {
        Credentials::Nothing => {}
        // The exporter sends an API key as it is given, which Elasticsearch takes encoded.
        Credentials::ApiKey => exporter.push_str(&format!(
            "    api_key: {}\n",
            base64::engine::general_purpose::STANDARD.encode(token)
        )),
        Credentials::Password => exporter.push_str(&format!("    user: elastic\n    password: \"{token}\"\n")),
    }
    if let Some(index) = told.index {
        exporter.push_str(&format!("    logs_index: {index}\n"));
    }
    if let Some(pipeline) = told.pipeline {
        exporter.push_str(&format!("    pipeline: {pipeline}\n"));
    }
    if let Some(mode) = told.mapping {
        exporter.push_str(&format!("    mapping: {{mode: {mode}}}\n"));
    }
    match tls {
        Tls::None => {}
        Tls::Served => exporter.push_str("    tls: {ca_file: /smeltr/ca.pem}\n"),
        Tls::Vouched => exporter.push_str(
            "    tls: {ca_file: /smeltr/ca.pem, cert_file: /smeltr/sender.pem, key_file: /smeltr/sender-key.pem}\n",
        ),
    }
    format!(
        "receivers:\n  filelog:\n    include: [/smeltr/app.log]\n    start_at: beginning\nexporters:\n  \
         elasticsearch:\n{exporter}service:\n  telemetry:\n    metrics: {{level: none}}\n  pipelines:\n    logs:\n      \
         receivers: [filelog]\n      exporters: [elasticsearch]\n"
    )
}

/// Whether `value` holds `line` as a string anywhere within it.
fn holds_line(value: &Value, line: &str) -> bool {
    match value {
        Value::String(s) => s == line,
        Value::Array(values) => values.iter().any(|v| holds_line(v, line)),
        Value::Object(members) => members.values().any(|v| holds_line(v, line)),
        _ => false,
    }
}

/// The line `record` carries, of those the sender tailed, or why it is not one filed as the sender was told.
fn check_live_record(
    sender: Sender,
    told: &Told,
    record: &SourceRecord,
    object: &serde_json::Map<String, Value>,
    lines: &[String],
) -> Result<String, String> {
    let document: Value =
        serde_json::from_slice(record.bytes().unwrap()).map_err(|e| format!("a document that is not JSON: {e}"))?;
    let line = (lines.iter())
        .find(|line| holds_line(&document, line))
        .ok_or_else(|| format!("{document} holds no line the sender tailed"))?;
    let filed = |key: &str| object.get(key).and_then(Value::as_str);
    let index = filed("index").unwrap_or_default();
    let indexed = match (told.index, told.logstash, sender) {
        (Some(told), ..) => index == told,
        (None, Some(prefix), _) => index.starts_with(&format!("{prefix}-")),
        (None, None, Sender::Filebeat) => index.starts_with("filebeat-"),
        (None, None, Sender::FluentBit) => index == "fluent-bit",
        (None, None, Sender::Collector) => !index.is_empty(),
    };
    if !indexed {
        return Err(format!("filed under index {index:?}"));
    }
    if filed("pipeline") != told.pipeline {
        return Err(format!(
            "filed under pipeline {:?}, not {:?}",
            filed("pipeline"),
            told.pipeline
        ));
    }
    if told.ids && filed("id").is_none_or(str::is_empty) {
        return Err("filed under no id, though the sender gives each one".into());
    }
    Ok(line.clone())
}

/// A real Filebeat, Fluent Bit or OpenTelemetry Collector tails a file of random lines and sends them to receivers of
/// random settings, under random configurations of its own: an API key or a password, gzip or not, an index and a
/// pipeline of its own or not, plain, over TLS, or over TLS with a client certificate. Every line arrives exactly once,
/// filed as the sender was told, the receiver never panics, and the sender logs no failure to send.
#[test]
fn live_senders_deliver_every_line_once_under_random_configurations() {
    const TEST: &str = "live_senders_deliver_every_line_once_under_random_configurations";
    let (Some(filebeat), Some(fluent_bit), Some(collector)) = (
        crate::live::filebeat(TEST),
        crate::live::fluent_bit(TEST),
        crate::live::otel_collector(TEST),
    ) else {
        return;
    };
    // A panic on the receiver's threads drops a connection and fails no test by itself.
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().name() == Some("smeltr-receiver") {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let senders = [Sender::Filebeat, Sender::FluentBit, Sender::Collector];
    let options = ["gzip", "index", "pipeline"];
    let mut cov =
        Coverage::new(
            (senders.iter())
                .flat_map(|s| ["None", "Served", "Vouched"].map(|tls| format!("{s:?} under {tls}")))
                .chain(senders.iter().flat_map(|s| {
                    (options.iter()).flat_map(move |o| [format!("{s:?} {o} on"), format!("{s:?} {o} off")])
                }))
                .chain(
                    ["Filebeat", "Collector"]
                        .iter()
                        .flat_map(|s| ["Nothing", "ApiKey", "Password"].map(|c| format!("{s} presenting {c}"))),
                )
                .chain(["FluentBit presenting Nothing", "FluentBit presenting Password"].map(String::from))
                .chain(
                    [
                        "FluentBit logstash on",
                        "FluentBit ids on",
                        "FluentBit create on",
                        "FluentBit create off",
                        "Filebeat bulk on",
                    ]
                    .map(String::from),
                )
                .chain(["none", "ecs", "otel", "raw"].map(|m| format!("Collector mapping {m}"))),
        );

    fuzz::run(1..31, |seed| {
        // A panic is the seed's own receiver's only while one seed runs.
        panicked.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut rng = Rng::seeded(seed);
        let sender = senders[seed as usize % senders.len()];
        let mut settings = random_settings(&mut rng, ipv6);
        // A sender reaches the receiver over IPv4, on the host's network, by the name its certificate is for.
        settings.connect = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), settings.connect.port());
        if !settings.listen.ip().is_unspecified() {
            settings.listen = settings.connect;
        }
        settings.max_request_bytes = *rng.pick(&[1 << 20, 8 << 20]);
        // Each sender under each way of serving TLS, and under authorities alone once, within thirty seeds.
        settings.tls = [Tls::None, Tls::Served, Tls::Vouched][(seed as usize / senders.len()) % 3];
        let alone = settings.tls == Tls::Vouched && seed / 9 == 1;
        // Fluent Bit presents no API key.
        let credentials = match (alone, sender, rng.chance(2)) {
            (true, ..) => Credentials::Nothing,
            (false, Sender::FluentBit, _) | (false, _, true) => Credentials::Password,
            (false, _, false) => Credentials::ApiKey,
        };
        let token = match credentials {
            Credentials::ApiKey => format!("id{seed}:key{:x}", rng.next_u64()),
            _ => format!("tok-{:x}", rng.next_u64()),
        };
        settings.tokens = match credentials {
            Credentials::Nothing => Vec::new(),
            _ => vec![token.clone()],
        };
        cov.reach(format!("{sender:?} under {:?}", settings.tls));
        cov.reach(format!("{sender:?} presenting {credentials:?}"));
        let index = rng.chance(2).then_some("fuzz-logs");
        let told = Told {
            credentials,
            gzip: rng.chance(2),
            index,
            pipeline: rng.chance(2).then_some("parse"),
            logstash: (sender == Sender::FluentBit && index.is_none() && rng.chance(2)).then_some("fuzz"),
            ids: sender == Sender::FluentBit && rng.chance(2),
            create: sender == Sender::FluentBit && rng.chance(2),
            bulk: (sender == Sender::Filebeat && rng.chance(2)).then(|| 1 + rng.below(50)),
            mapping: (sender == Sender::Collector).then(|| *rng.pick(&["none", "ecs", "otel", "raw"])),
        };
        for (option, on) in options
            .iter()
            .zip([told.gzip, told.index.is_some(), told.pipeline.is_some()])
        {
            cov.reach(format!("{sender:?} {option} {}", if on { "on" } else { "off" }));
        }
        for (option, on) in [
            ("FluentBit logstash on", told.logstash.is_some()),
            ("FluentBit ids on", told.ids),
            ("FluentBit create on", told.create),
            ("FluentBit create off", sender == Sender::FluentBit && !told.create),
            ("Filebeat bulk on", told.bulk.is_some()),
        ] {
            if on {
                cov.reach(option);
            }
        }
        if let Some(mode) = told.mapping {
            cov.reach(format!("Collector mapping {mode}"));
        }
        let what = format!(
            "{sender:?} under {:?}, listening on {}, told {told:?}",
            settings.tls, settings.listen
        );
        let mut source = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-es-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        let n = 1 + rng.below(300);
        // Each line names itself, and ends in no space, which the collector trims.
        let lines: Vec<String> = (0..n)
            .map(|i| format!("line {seed}-{i} {} été \"q\" \\", "x".repeat(rng.below(200))))
            .collect();
        let file: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(dir.join("app.log"), file).unwrap();
        std::fs::write(dir.join("ca.pem"), &pki.authority.pem).unwrap();
        std::fs::write(dir.join("sender.pem"), &pki.sender.certificate).unwrap();
        std::fs::write(dir.join("sender-key.pem"), &pki.sender.key).unwrap();
        let (image, config, args): (&str, _, Vec<&str>) = match sender {
            Sender::Filebeat => (
                &filebeat,
                (
                    "filebeat.yml",
                    filebeat_config(settings.connect, &token, settings.tls, &told),
                ),
                vec!["-e", "--strict.perms=false", "-c", "/smeltr/filebeat.yml"],
            ),
            Sender::FluentBit => (
                &fluent_bit,
                (
                    "fluent-bit.conf",
                    fluent_bit_config(settings.connect, &token, settings.tls, &told),
                ),
                vec!["/fluent-bit/bin/fluent-bit", "-c", "/smeltr/fluent-bit.conf"],
            ),
            Sender::Collector => (
                &collector,
                (
                    "config.yaml",
                    collector_config(settings.connect, &token, settings.tls, &told),
                ),
                vec!["--config", "/smeltr/config.yaml"],
            ),
        };
        std::fs::write(dir.join(config.0), &config.1).unwrap();
        share(dir);
        let name = format!("smeltr-es-fuzz-{}-{seed}", std::process::id());
        // Filebeat reads only files its own user owns, unless it runs as root.
        let user: &[&str] = if sender == Sender::Filebeat {
            &["--user", "root"]
        } else {
            &[]
        };
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &name])
            .args(user)
            .arg("-v")
            .arg(format!("{}:/smeltr:ro", dir.display()))
            .arg(image)
            .args(&args)
            .output()
            .expect("docker runs");
        // A run that fails may still have created its container.
        let container = Container(name);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }

        let meta = SourceMeta::ELASTIC_RECEIVER;
        let mut arrived: BTreeMap<String, usize> = BTreeMap::new();
        let deadline = Instant::now() + Duration::from_secs(90);
        let take = |batch: &[SourceRecord]| {
            for (record, object) in batch.iter().zip(meta.objects(batch, Some("es"))) {
                if std::env::var("SMELTR_ES_FUZZ_SHOW").is_ok() {
                    eprintln!("{:?} {object:?}", String::from_utf8_lossy(record.bytes().unwrap()));
                }
                let line = check_live_record(sender, &told, record, &object, &lines)
                    .map_err(|why| format!("{what}: {why}: {object:?}"))?;
                *arrived.entry(line).or_default() += 1;
            }
            if panicked.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(format!("{what}: the receiver panicked"));
            }
            Ok(arrived.len() == n)
        };
        // Once every line is in, a little longer, for any sent again.
        if !receiver::drain(
            &mut source,
            Duration::from_millis(200),
            Duration::from_secs(3),
            deadline,
            take,
        )? {
            return Err(format!(
                "{what}: {} of {n} lines arrived\n{}",
                arrived.len(),
                container_logs(&container.0)
            ));
        }
        if let Some((line, times)) = arrived.iter().find(|(_, times)| **times > 1) {
            return Err(format!("{what}: {line:?} arrived {times} times"));
        }
        let logs = container_logs(&container.0);
        let failed = [
            r#""log.level":"error""#,
            "Exporting failed",
            "\terror\t",
            "[error]",
            "[ warn]",
        ];
        if failed.iter().any(|f| logs.contains(f)) {
            return Err(format!("{what}: the sender logged a failure:\n{logs}"));
        }
        Ok(())
    });
    cov.assert_complete();
}
