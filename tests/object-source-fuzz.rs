//! The object store sources' fuzzers: `s3Sqs`, `s3List`, `gcsList`, `gcsPubSub`, `azureBlobList`, `azureBlobQueue`
//! and `sftp`. Each seed draws one source as a bundle names it, every option at random, and sometimes breaks one of its
//! settings.
//!
//! Offline, a source the save accepts loads as its bundle says, loads the same after a save, and builds its client
//! when this worker holds every credential it names and refuses to when it does not. Then drawn objects, in every
//! codec and framing, are read through the source's own queue over in-memory stores: a queue kind's notifications,
//! written as each service writes them, parse to the objects they announce, and a listing kind lists its prefix. Every
//! record comes out once, in order, under its object, and once committed each message is deleted, or each object kept,
//! deleted or moved as the source says. One the save refuses is refused on the setting that was broken. Any other
//! change to its JSON may be refused, and never panics.
//!
//! Live, the same draws, pointed at the emulators `just live-up` runs, read objects there through the real queue and
//! store.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::format::encoding::Encoding;
use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::object_source::{MAX_RECORD_BYTES, Message, ObjectRef, ObjectSource, Queue};
use smeltr::io::{CommitMap, IoError, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::{
    AfterCommit, Framing, ListPlace, ListStart, ObjectQueue, ObjectSourceConfig, SecretRef, SourceKind,
};

use crate::harness::destination::{self, loads};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;
use crate::store_access::{
    self, ACCESS_OPTIONS, AZURE_SAS, Broken, Compare, Reached, SFTP_HOST_KEY, SFTP_PASSWORD, Store, Target,
    UNSET_VARIABLE, access_breaks, address, authority_pem, break_access, compare_azure, compare_credentials,
    compare_gcs, compare_s3, compare_sftp, connector_secrets, draw_access, held, refusals, text, uint,
};

const SEEDS: u64 = 700;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    S3Sqs,
    S3List,
    GcsList,
    GcsPubSub,
    AzureBlobList,
    AzureBlobQueue,
    Sftp,
}

const KINDS: [Kind; 7] = [
    Kind::S3Sqs,
    Kind::S3List,
    Kind::GcsList,
    Kind::GcsPubSub,
    Kind::AzureBlobList,
    Kind::AzureBlobQueue,
    Kind::Sftp,
];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::S3Sqs => "s3Sqs",
            Kind::S3List => "s3List",
            Kind::GcsList => "gcsList",
            Kind::GcsPubSub => "gcsPubSub",
            Kind::AzureBlobList => "azureBlobList",
            Kind::AzureBlobQueue => "azureBlobQueue",
            Kind::Sftp => "sftp",
        }
    }

    fn store(self) -> Store {
        match self {
            Kind::S3Sqs | Kind::S3List => Store::S3,
            Kind::GcsList | Kind::GcsPubSub => Store::Gcs,
            Kind::AzureBlobList | Kind::AzureBlobQueue => Store::Azure,
            Kind::Sftp => Store::Sftp,
        }
    }

    /// Whether it finds objects by listing a prefix, rather than by a queue's notifications.
    fn listed(self) -> bool {
        matches!(self, Kind::S3List | Kind::GcsList | Kind::AzureBlobList | Kind::Sftp)
    }
}

/// One drawn source.
struct Draw {
    kind: Kind,
    /// The source's object in the bundle.
    source: Map<String, Value>,
    /// Whether the source trusts an authority of its own.
    tls: bool,
    /// The setting broken, if one was. `Some(None)` is a break the save refuses on no one field of it.
    broken: Option<Option<&'static str>>,
}

/// Every option the generator knows besides the access ones. Each must be drawn into a source the save accepts and
/// read through, or into the break it names, over the seed range.
const OPTIONS: &[&str] = &[
    "kind.s3Sqs",
    "kind.s3List",
    "kind.gcsList",
    "kind.gcsPubSub",
    "kind.azureBlobList",
    "kind.azureBlobQueue",
    "kind.sftp",
    "tls",
    "encoding",
    "framing.lines",
    "framing.object",
    "framing.escapedLines",
    "framing.lineBreaker",
    "format.avro",
    "avro.codec.null",
    "avro.codec.deflate",
    "avro.codec.snappy",
    "avro.codec.zstandard",
    "avro.unclaimed",
    "avro.undeclared",
    "avro.nested",
    "queueUrl.padded",
    "visibilitySecs",
    "maxDeliveries",
    "subscription.padded",
    "pubsubEndpoint",
    "bucket.padded",
    "container.padded",
    "prefix",
    "prefix.partial",
    "pollIntervalSecs",
    "start.new",
    "afterCommit.keep",
    "afterCommit.delete",
    "afterCommit.move",
    "compression.none",
    "compression.gzip",
    "compression.gzipMembers",
    "compression.zstd",
    "compression.lz4",
    "compression.snappy",
    "lines.crlf",
    "lines.empty",
    "lines.unterminated",
    "escaped.other",
    "object.empty",
    "object.large",
    "key.special",
    "listing.decoy",
    "message.several",
    "message.announcesNone",
    "message.version",
    "s3.records",
    "s3.sns",
    "s3.eventBridge",
    "s3.testEvent",
    "azure.cloudEvents",
    "azure.base64",
    "azure.createFile",
    "azure.query",
    "azure.dfs",
    "commit.asRead",
    "poll.small",
    "broken.queueUrl",
    "broken.visibilitySecs",
    "broken.maxDeliveries",
    "broken.queueName",
    "broken.subscription",
    "broken.pubsubEndpoint",
    "broken.bucket",
    "broken.prefix",
    "broken.pollIntervalSecs",
    "broken.start",
    "broken.movePrefix",
    "broken.lineBreaker",
    "broken.encoding",
    "broken.avroFields",
    "broken.avroFraming",
    "broken.avroEncoding",
    "broken.certificate",
    "broken.tlsPlain",
    "mangled",
];

/// Every option, the access ones too.
fn options() -> impl Iterator<Item = &'static str> {
    OPTIONS.iter().chain(ACCESS_OPTIONS).copied()
}

/// The line breakers drawn, each with how its events are written.
const LINE_BREAKERS: &[&str] = &[r"(\|\|)", r"([\r\n]+)\d{4}-", r"([\r\n]+)"];

// -- drawing a source -------------------------------------------------------------------------------------------

/// `value`, sometimes with blanks around it that the source drops.
fn padded(rng: &mut Rng, cov: &mut Reached, option: &str, value: &str) -> String {
    if rng.chance(3) {
        cov.reach(option);
        format!(" {value} ")
    } else {
        value.to_string()
    }
}

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target, only: Option<Kind>) -> Draw {
    let kind = only.unwrap_or_else(|| *rng.pick(&KINDS));
    cov.reach(format!("kind.{}", kind.name()));
    let mut d = Map::new();
    d.insert("id".into(), json!("s"));
    d.insert("type".into(), json!(kind.name()));
    draw_access(rng, cov, target, kind.store(), &mut d);
    match kind {
        Kind::S3Sqs => {
            let url = *rng.pick(&[
                "https://sqs.us-east-1.amazonaws.com/123456789012/logs",
                "https://sqs.eu-west-2.amazonaws.com/123456789012/logs.fifo",
                "https://eu-west-1.queue.amazonaws.com/123456789012/q",
                "http://localhost:4566/000000000000/logs",
                "http://127.0.0.1:4566/queue/us-east-1/000000000000/logs",
            ]);
            d.insert("queueUrl".into(), json!(padded(rng, cov, "queueUrl.padded", url)));
            draw_visibility(rng, cov, 43200, &mut d);
        }
        Kind::AzureBlobQueue => {
            let base = *rng.pick(&[
                "https://acme.queue.core.windows.net",
                "http://127.0.0.1:10001/devstoreaccount1",
            ]);
            // The longest name that leaves room for its poison queue's.
            let name = if rng.chance(4) {
                "q".repeat(56)
            } else {
                "logs".to_string()
            };
            let slash = if rng.chance(4) { "/" } else { "" };
            let url = format!("{base}/{name}{slash}");
            d.insert("queueUrl".into(), json!(padded(rng, cov, "queueUrl.padded", &url)));
            draw_visibility(rng, cov, 43200, &mut d);
            if rng.chance(2) {
                cov.reach("maxDeliveries");
                d.insert("maxDeliveries".into(), json!(rng.pick(&[1, 5, 1000])));
            }
        }
        Kind::GcsPubSub => {
            let name = *rng.pick(&[
                "projects/p/subscriptions/s",
                "projects/acme-logs/subscriptions/smeltr.logs",
            ]);
            d.insert(
                "subscription".into(),
                json!(padded(rng, cov, "subscription.padded", name)),
            );
            if rng.chance(2) {
                cov.reach("pubsubEndpoint");
                d.insert("pubsubEndpoint".into(), json!(address(rng)));
            }
            draw_visibility(rng, cov, 600, &mut d);
        }
        Kind::S3List => {
            d.insert("bucket".into(), json!(rng.pick(&["logs", "logs-2024", "a.b.c", "abc"])));
            draw_listing(rng, cov, &mut d);
        }
        Kind::GcsList => {
            let bucket = *rng.pick(&["logs", "logs_eu"]);
            d.insert("bucket".into(), json!(padded(rng, cov, "bucket.padded", bucket)));
            draw_listing(rng, cov, &mut d);
        }
        Kind::AzureBlobList => {
            let container = *rng.pick(&["logs", "logs-2024"]);
            d.insert(
                "container".into(),
                json!(padded(rng, cov, "container.padded", container)),
            );
            draw_listing(rng, cov, &mut d);
        }
        Kind::Sftp => draw_listing(rng, cov, &mut d),
    }
    draw_framing(rng, cov, &mut d);
    // Avro's strings are UTF-8, so an Avro source takes only the default.
    if d.contains_key("format") {
        if rng.chance(3) {
            d.insert("encoding".into(), json!("auto"));
        }
    } else if rng.chance(3) {
        cov.reach("encoding");
        d.insert(
            "encoding".into(),
            json!(rng.pick(&[
                "auto",
                "utf-8",
                "UTF-8",
                "none",
                "windows-1252",
                "utf-16le",
                "shift_jis"
            ])),
        );
    }
    // TLS settings are the client's, so only a source reached over https takes them.
    let tls = kind != Kind::Sftp && !target.live() && all_https(&d) && rng.chance(4);
    if tls {
        cov.reach("tls");
        d.insert("authorities".into(), json!(authority_pem()));
    }
    let mut draw = Draw {
        kind,
        source: d,
        tls,
        broken: None,
    };
    if !target.live() && rng.chance(2) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

/// Whether every address `d` names is https. An absent endpoint is the service's own, which is.
fn all_https(d: &Map<String, Value>) -> bool {
    ["endpoint", "authEndpoint", "queueUrl", "pubsubEndpoint"]
        .iter()
        .filter_map(|k| d.get(*k).and_then(Value::as_str))
        .all(|u| u.trim().starts_with("https://"))
}

fn draw_visibility(rng: &mut Rng, cov: &mut Reached, max: u64, d: &mut Map<String, Value>) {
    if rng.chance(2) {
        cov.reach("visibilitySecs");
        d.insert("visibilitySecs".into(), json!(rng.pick(&[1, 30, 60, max])));
    }
}

/// A listing source's prefix, poll, start and what it does with an object once committed.
fn draw_listing(rng: &mut Rng, cov: &mut Reached, d: &mut Map<String, Value>) {
    let after = *rng.pick(&["keep", "delete", "move"]);
    // A move under a listing of the whole bucket would list what it moved again.
    if after == "move" || !rng.chance(4) {
        let prefix = *rng.pick(&["in/", "in/a", "logs/2024/", "ünï/", "a b/", "x"]);
        cov.reach("prefix");
        if !prefix.ends_with('/') {
            cov.reach("prefix.partial");
        }
        d.insert("prefix".into(), json!(prefix));
    }
    if rng.chance(2) {
        cov.reach("pollIntervalSecs");
        d.insert("pollIntervalSecs".into(), json!(rng.pick(&[1, 60, 3600])));
    }
    if after != "keep" || rng.chance(2) {
        d.insert("afterCommit".into(), json!(after));
    }
    if after == "move" {
        let prefix = text(d, "prefix").unwrap_or_default();
        let to = if prefix.ends_with('/') { "done/" } else { "done" };
        d.insert("movePrefix".into(), json!(to));
    }
    if after == "keep" && rng.chance(3) {
        cov.reach("start.new");
        d.insert("start".into(), json!("new"));
    } else if rng.chance(4) {
        d.insert("start".into(), json!("everything"));
    }
}

fn draw_framing(rng: &mut Rng, cov: &mut Reached, d: &mut Map<String, Value>) {
    if rng.chance(5) {
        cov.reach("format.avro");
        d.insert("format".into(), json!("avro"));
        // Declaring nothing, its open root keeps every top-level field all the same.
        let fields = match rng.chance(4) {
            true => {
                cov.reach("avro.undeclared");
                json!([])
            }
            false => avro_fields(),
        };
        d.insert("fields".into(), fields);
        return;
    }
    let framing = *rng.pick(&["lines", "object", "escapedLines", "lineBreaker"]);
    cov.reach(format!("framing.{framing}"));
    match framing {
        "lineBreaker" => {
            d.insert("lineBreaker".into(), json!(rng.pick(LINE_BREAKERS)));
            if rng.chance(2) {
                d.insert("framing".into(), json!("lines"));
            }
        }
        "lines" if rng.chance(2) => {}
        _ => {
            d.insert("framing".into(), json!(framing));
        }
    }
}

/// Breaks one setting of `draw`, and answers the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> Option<&'static str> {
    let kind = draw.kind;
    let avro = draw.source.get("format").is_some_and(|f| f == "avro");
    let mut breaks: Vec<&'static str> = match avro {
        true => vec!["avroFields", "avroFraming", "avroEncoding"],
        false => vec!["lineBreaker", "encoding"],
    };
    if kind != Kind::Sftp {
        breaks.extend(["certificate", "tlsPlain"]);
    }
    match kind {
        Kind::S3Sqs => breaks.extend(["queueUrl", "visibilitySecs"]),
        Kind::AzureBlobQueue => breaks.extend(["queueUrl", "visibilitySecs", "maxDeliveries", "queueName"]),
        Kind::GcsPubSub => breaks.extend(["subscription", "pubsubEndpoint", "visibilitySecs"]),
        Kind::Sftp => breaks.extend(["prefix", "pollIntervalSecs", "start", "movePrefix"]),
        _ => breaks.extend(["bucket", "prefix", "pollIntervalSecs", "start", "movePrefix"]),
    }
    breaks.extend(access_breaks(kind.store()));
    let what = *rng.pick(&breaks);
    cov.reach(format!("broken.{what}"));
    let d = &mut draw.source;
    let mut set = |k: &str, v: Value| {
        d.insert(k.to_string(), v);
    };
    match what {
        "avroFields" => {
            set(
                "fields",
                json!(rng.pick(&[
                    json!([{"name": "n", "type": "i65"}]),
                    json!([{"name": "__source.object", "type": "str"}]),
                ])),
            );
            Some("fields")
        }
        "avroFraming" => {
            set("framing", json!(rng.pick(&["object", "escapedLines"])));
            Some("framing")
        }
        "avroEncoding" => {
            set("encoding", json!("utf-16le"));
            Some("encoding")
        }
        "lineBreaker" => {
            if rng.chance(2) {
                set("framing", json!(rng.pick(&["object", "escapedLines"])));
                set("lineBreaker", json!(r"(\n)"));
            } else {
                d.remove("framing");
                d.insert("lineBreaker".into(), json!(rng.pick(&[" ", "(", r"\n", "(x*)"])));
            }
            Some("lineBreaker")
        }
        "encoding" => {
            set("encoding", json!(rng.pick(&["klingon", "", "utf-9"])));
            Some("encoding")
        }
        "certificate" => {
            set("certificate", json!(" "));
            Some("certificate")
        }
        "tlsPlain" => {
            // TLS settings are refused where no address they would guard negotiates TLS, at the first of them.
            d.remove("authEndpoint");
            let (endpoint, field) = match kind {
                Kind::S3Sqs => {
                    d.insert("queueUrl".into(), json!("http://localhost:4566/000000000000/logs"));
                    ("http://localhost:4566", "queueUrl")
                }
                Kind::S3List => ("http://localhost:4566", "endpoint"),
                Kind::AzureBlobQueue => {
                    d.insert("queueUrl".into(), json!("http://127.0.0.1:10001/devstoreaccount1/logs"));
                    ("http://127.0.0.1:10000/devstoreaccount1", "queueUrl")
                }
                Kind::AzureBlobList => ("http://127.0.0.1:10000/devstoreaccount1", "endpoint"),
                Kind::GcsPubSub => {
                    d.insert("pubsubEndpoint".into(), json!("http://localhost:8085"));
                    ("http://localhost:4443", "pubsubEndpoint")
                }
                Kind::GcsList => ("http://localhost:4443", "endpoint"),
                Kind::Sftp => unreachable!("an SFTP source sets no TLS"),
            };
            d.insert("endpoint".into(), json!(endpoint));
            d.insert("authorities".into(), json!(authority_pem()));
            draw.tls = true;
            Some(field)
        }
        "queueUrl" => {
            set("queueUrl", json!(rng.pick(&["", "sqs/logs", "ftp://127.0.0.1/q"])));
            Some("queueUrl")
        }
        "queueName" => {
            set(
                "queueUrl",
                json!(format!("https://acme.queue.core.windows.net/{}", "q".repeat(57))),
            );
            Some("queueUrl")
        }
        "visibilitySecs" => {
            let max = if kind == Kind::GcsPubSub { 601 } else { 43201 };
            set("visibilitySecs", json!(rng.pick(&[0, max])));
            Some("visibilitySecs")
        }
        "maxDeliveries" => {
            set("maxDeliveries", json!(rng.pick(&[0, 1001])));
            Some("maxDeliveries")
        }
        "subscription" => {
            set(
                "subscription",
                json!(rng.pick(&["", " ", "s", "projects//subscriptions/s", "projects/p/topics/t"])),
            );
            Some("subscription")
        }
        "pubsubEndpoint" => {
            set("pubsubEndpoint", json!(rng.pick(&["", " "])));
            Some("pubsubEndpoint")
        }
        "bucket" => {
            if kind == Kind::AzureBlobList {
                set("container", json!(rng.pick(&["", "  "])));
                Some("container")
            } else {
                let bad: &[&str] = if kind == Kind::S3List {
                    &["", "s3://logs", "ab", "Logs", "-logs", "logs/in"]
                } else {
                    &["", "  "]
                };
                set("bucket", json!(rng.pick(bad)));
                Some("bucket")
            }
        }
        "prefix" => {
            set("prefix", json!("/in/"));
            if text(d, "afterCommit").as_deref() == Some("move") {
                d.insert("movePrefix".into(), json!("done/"));
            }
            Some("prefix")
        }
        "pollIntervalSecs" => {
            set("pollIntervalSecs", json!(0));
            Some("pollIntervalSecs")
        }
        "start" => {
            set("start", json!("new"));
            if rng.chance(2) {
                d.insert("afterCommit".into(), json!("delete"));
                d.remove("movePrefix");
            } else {
                d.insert("afterCommit".into(), json!("move"));
                d.insert("prefix".into(), json!("in/"));
                d.insert("movePrefix".into(), json!("done/"));
            }
            Some("start")
        }
        "movePrefix" => {
            let prefix = text(d, "prefix").unwrap_or_default();
            let mut to = |v: Option<&str>| {
                d.insert("afterCommit".into(), json!("move"));
                match v {
                    Some(v) => d.insert("movePrefix".into(), json!(v)),
                    None => d.remove("movePrefix"),
                };
            };
            match rng.below(8) {
                0 => to(None),
                1 => to(Some(" ")),
                2 => to(Some("/done/")),
                // A `/` on one and not the other.
                3 => to(Some(if prefix.is_empty() || prefix.ends_with('/') {
                    "done"
                } else {
                    "done/"
                })),
                // Under the prefix it lists, or the prefix under it.
                4 if !prefix.is_empty() => to(Some(&format!(
                    "{prefix}done{}",
                    if prefix.ends_with('/') { "/" } else { "" }
                ))),
                4 => to(Some("done/")),
                5 => to(Some(if prefix.is_empty() || prefix.ends_with('/') {
                    "a/../b/"
                } else {
                    "a/../b"
                })),
                // Named, and not moved to.
                _ => {
                    let after = *rng.pick(&["keep", "delete"]);
                    d.insert("afterCommit".into(), json!(after));
                    d.insert("movePrefix".into(), json!("done/"));
                }
            }
            Some("movePrefix")
        }
        access => break_access(rng, access, d),
    }
}

/// `draw` as a whole bundle: the source, and one route from it to a Kafka destination.
fn bundle(draw: &Draw) -> Value {
    json!({
        "version": 1,
        "sources": [Value::Object(draw.source.clone())],
        "destinations": [{"id": "out", "type": "kafka", "brokers": "b:9092", "defaultTopic": "t"}],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["out"]}]}],
    })
}

// -- what the save says ------------------------------------------------------------------------------------------

/// The source `json` loads as.
fn loaded(json: &str) -> Result<(ObjectSourceConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.sources[0].kind {
        SourceKind::Objects(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<ObjectSourceConfig>, String> {
    let config = store_access::check_save(json, draw.broken.map(Broken::Field), loads, loaded)?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    Ok(config.map(|(c, _)| c))
}

/// `d`'s string `key`, trimmed as load trims it.
fn trimmed(d: &Map<String, Value>, key: &str) -> Option<String> {
    text(d, key).map(|s| s.trim().to_string())
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &ObjectSourceConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.source;
    let mut cmp = Compare::default();
    cmp.expect("tls", draw.tls, c.tls.is_some());
    cmp.expect("framing", expected_framing(d), &c.framing);
    cmp.expect("format", text(d, "format"), c.avro.as_ref().map(|_| "avro".to_string()));
    let label = text(d, "encoding").unwrap_or("auto".into());
    cmp.expect("encoding", Encoding::for_label(&label), Some(c.encoding));
    let visibility = if draw.kind.listed() {
        60
    } else {
        uint(d, "visibilitySecs", 60)
    };
    cmp.expect("visibilitySecs", visibility, c.visibility.as_secs());
    match &c.queue {
        ObjectQueue::Sqs { queue_url, access } => {
            cmp.expect("kind", Kind::S3Sqs, draw.kind);
            cmp.expect("queueUrl", trimmed(d, "queueUrl").unwrap(), queue_url.clone());
            compare_s3(&mut cmp, d, access);
        }
        ObjectQueue::StorageQueue {
            queue_url,
            access,
            max_deliveries,
        } => {
            cmp.expect("kind", Kind::AzureBlobQueue, draw.kind);
            let url = trimmed(d, "queueUrl").unwrap();
            // The queue's client drops the blanks.
            cmp.expect(
                "queueUrl",
                url.trim_end_matches('/'),
                queue_url.trim().trim_end_matches('/'),
            );
            cmp.expect("maxDeliveries", uint(d, "maxDeliveries", 5), *max_deliveries as u64);
            compare_azure(&mut cmp, d, access);
        }
        ObjectQueue::PubSub {
            subscription,
            endpoint,
            access,
        } => {
            cmp.expect("kind", Kind::GcsPubSub, draw.kind);
            cmp.expect("subscription", trimmed(d, "subscription").unwrap(), subscription);
            cmp.expect("pubsubEndpoint", text(d, "pubsubEndpoint"), endpoint);
            compare_gcs(&mut cmp, d, access);
        }
        ObjectQueue::List {
            place,
            prefix,
            poll_interval,
            start,
            after_commit,
        } => {
            cmp.expect("prefix", text(d, "prefix").unwrap_or_default(), prefix);
            cmp.expect(
                "pollIntervalSecs",
                uint(d, "pollIntervalSecs", 60),
                poll_interval.as_secs(),
            );
            let drawn_start = match text(d, "start").as_deref() {
                Some("new") => ListStart::New,
                _ => ListStart::Everything,
            };
            cmp.expect("start", drawn_start, start);
            let drawn_after = match text(d, "afterCommit").as_deref() {
                Some("delete") => AfterCommit::Delete,
                Some("move") => AfterCommit::Move {
                    prefix: text(d, "movePrefix").unwrap(),
                },
                _ => AfterCommit::Keep,
            };
            cmp.expect("afterCommit", drawn_after, after_commit);
            match place {
                ListPlace::S3 { bucket, access } => {
                    cmp.expect("kind", Kind::S3List, draw.kind);
                    cmp.expect("bucket", trimmed(d, "bucket").unwrap(), bucket);
                    compare_s3(&mut cmp, d, access);
                }
                ListPlace::Gcs(g) => {
                    cmp.expect("kind", Kind::GcsList, draw.kind);
                    cmp.expect("bucket", trimmed(d, "bucket").unwrap(), &g.bucket);
                    compare_gcs(&mut cmp, d, &g.access);
                }
                ListPlace::Azure(a) => {
                    cmp.expect("kind", Kind::AzureBlobList, draw.kind);
                    cmp.expect("container", trimmed(d, "container").unwrap(), &a.container);
                    compare_azure(&mut cmp, d, &a.access);
                }
                ListPlace::Sftp(f) => {
                    cmp.expect("kind", Kind::Sftp, draw.kind);
                    compare_sftp(&mut cmp, d, &f.directory, &f.access);
                }
            }
        }
    }
    compare_credentials(&mut cmp, d, named);
    cmp.done()
}

fn expected_framing(d: &Map<String, Value>) -> Framing {
    match (text(d, "framing").as_deref(), text(d, "lineBreaker")) {
        (Some("object"), _) => Framing::Object,
        (Some("escapedLines"), _) => Framing::EscapedLines,
        (_, Some(pattern)) => Framing::LineBreaker(LineBreaker(regex::bytes::Regex::new(&pattern).unwrap())),
        _ => Framing::Lines,
    }
}

// -- objects ----------------------------------------------------------------------------------------------------

/// One object to read: where it is, what it holds, and the records it must become.
#[derive(Clone)]
struct Object {
    bucket: String,
    key: String,
    version: Option<String>,
    body: Vec<u8>,
    records: Vec<Vec<u8>>,
}

/// A line of text a record may be: never holding a line end.
fn line(rng: &mut Rng) -> String {
    let n = rng.below(1000);
    rng.pick(&[
        format!("event {n}"),
        format!(r#"{{"n":{n},"msg":"hi"}}"#),
        format!("ünï cödé {n} \u{1F600}"),
        format!("tab\there {n}"),
        format!(r"back\slash {n}"),
        "  padded  ".to_string(),
    ])
    .clone()
}

/// What a record's bytes may be under escaped framing: anything at all.
fn raw_bytes(rng: &mut Rng) -> Vec<u8> {
    let mut out = line(rng).into_bytes();
    for _ in 0..rng.below(3) {
        out.extend_from_slice(rng.pick(&[&b"\n"[..], b"\r\n", b"\\", b"\\n", b"\x00\xff", b"\r"]));
        out.extend(line(rng).into_bytes());
    }
    out
}

/// `bytes` as a `bytes` object destination escapes a line.
fn escaped(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for &b in bytes {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b => out.push(b),
        }
    }
    out
}

/// A line end between two lines.
fn line_end(rng: &mut Rng, cov: &mut Reached) -> &'static [u8] {
    if rng.chance(3) {
        cov.reach("lines.crlf");
        b"\r\n"
    } else {
        b"\n"
    }
}

/// The record an Avro source declares: what `avro_body` writes, less `extra`, which its open root keeps.
fn avro_fields() -> Value {
    json!([{"name": "n", "type": "i64"}, {"name": "msg", "type": "str"}, {"name": "user.id", "type": "i64"}])
}

/// An Avro container file's bytes, before any codec, and the records they must become, each as `record_json` writes
/// it.
fn avro_body(rng: &mut Rng, cov: &mut Reached) -> (Vec<u8>, Vec<Vec<u8>>) {
    use apache_avro::types::Value as Avro;
    let schema = apache_avro::Schema::parse_str(AVRO_SCHEMA).unwrap();
    let n = if rng.chance(15) {
        cov.reach("object.large");
        3000 + rng.below(2000)
    } else if rng.chance(8) {
        cov.reach("object.empty");
        return (Vec::new(), Vec::new());
    } else {
        1 + rng.below(8)
    };
    let codec = match rng.below(4) {
        0 => {
            cov.reach("avro.codec.null");
            apache_avro::Codec::Null
        }
        1 => {
            cov.reach("avro.codec.deflate");
            apache_avro::Codec::Deflate(Default::default())
        }
        2 => {
            cov.reach("avro.codec.snappy");
            apache_avro::Codec::Snappy
        }
        _ => {
            cov.reach("avro.codec.zstandard");
            apache_avro::Codec::Zstandard(Default::default())
        }
    };
    let mut writer = apache_avro::Writer::builder()
        .schema(&schema)
        .writer(Vec::new())
        .codec(codec)
        .build()
        .unwrap();
    let mut records = Vec::new();
    for i in 0..n {
        let msg = line(rng);
        let mut want = json!({"n": i, "msg": msg});
        let inner = match rng.chance(3) {
            true => {
                cov.reach("avro.nested");
                let a = rng.below(100) as i64;
                want["user"] = json!({"id": a});
                Avro::Union(1, Box::new(Avro::Record(vec![("id".into(), Avro::Long(a))])))
            }
            false => Avro::Union(0, Box::new(Avro::Null)),
        };
        let extra = match rng.chance(3) {
            true => {
                cov.reach("avro.unclaimed");
                let e = format!("e{}", rng.below(10));
                want["extra"] = json!(e);
                Avro::Union(1, Box::new(Avro::String(e)))
            }
            false => Avro::Union(0, Box::new(Avro::Null)),
        };
        let value = Avro::Record(vec![
            ("n".into(), Avro::Long(i as i64)),
            ("msg".into(), Avro::String(msg)),
            ("user".into(), inner),
            ("extra".into(), extra),
        ]);
        writer.append_value(value).unwrap();
        records.push(serde_json::to_vec(&want).unwrap());
    }
    (writer.into_inner().unwrap(), records)
}

/// What `avro_body` writes with.
const AVRO_SCHEMA: &str = r#"{"type": "record", "name": "Row", "fields": [
    {"name": "n", "type": "long"},
    {"name": "msg", "type": "string"},
    {"name": "user", "type": ["null", {"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}]},
    {"name": "extra", "type": ["null", "string"]}
]}"#;

/// An object's bytes under `framing`, before any codec, and the records they must become.
fn body(rng: &mut Rng, cov: &mut Reached, framing: &Framing) -> (Vec<u8>, Vec<Vec<u8>>) {
    let large = rng.chance(15);
    let n = if large {
        cov.reach("object.large");
        3000 + rng.below(2000)
    } else if rng.chance(8) {
        0
    } else {
        1 + rng.below(8)
    };
    if n == 0 {
        cov.reach("object.empty");
        return (Vec::new(), Vec::new());
    }
    match framing {
        Framing::Object => {
            let mut bytes = Vec::new();
            for _ in 0..n {
                bytes.extend(raw_bytes(rng));
            }
            (bytes.clone(), vec![bytes])
        }
        Framing::Lines | Framing::EscapedLines => {
            let mut bytes = Vec::new();
            let mut records = Vec::new();
            for i in 0..n {
                let record = if *framing == Framing::Lines {
                    if rng.chance(8) {
                        cov.reach("lines.empty");
                        Vec::new()
                    } else {
                        line(rng).into_bytes()
                    }
                } else if rng.chance(8) {
                    // A backslash before anything but `n`, `r` or another backslash is left as it is.
                    cov.reach("escaped.other");
                    let raw = raw_bytes(rng);
                    let mut line = escaped(&raw);
                    line.extend_from_slice(b"\\q");
                    bytes.extend(&line);
                    records.push([raw, b"\\q".to_vec()].concat());
                    if i + 1 < n || rng.chance(2) {
                        bytes.extend(line_end(rng, cov));
                    }
                    continue;
                } else {
                    raw_bytes(rng)
                };
                let line = if *framing == Framing::Lines {
                    record.clone()
                } else {
                    escaped(&record)
                };
                bytes.extend(&line);
                let last = i + 1 == n;
                if !last || rng.chance(2) {
                    bytes.extend(line_end(rng, cov));
                } else {
                    cov.reach("lines.unterminated");
                    // An unterminated empty last line is no line at all.
                    if line.is_empty() {
                        continue;
                    }
                }
                records.push(record);
            }
            (bytes, records)
        }
        Framing::LineBreaker(breaker) => {
            let pattern = breaker.0.as_str();
            let events: Vec<Vec<u8>> = (0..n)
                .map(|_| match pattern {
                    r"(\|\|)" => line(rng).replace('|', "/").into_bytes(),
                    r"([\r\n]+)\d{4}-" => {
                        let mut event = format!("2026-10-{:02} INFO {}", 1 + rng.below(28), line(rng));
                        for _ in 0..rng.below(3) {
                            event.push_str(if rng.chance(2) {
                                "\n    at a.b.C.run(C.java:1)"
                            } else {
                                "\r\n\tcaused"
                            });
                        }
                        event.into_bytes()
                    }
                    _ => line(rng).into_bytes(),
                })
                .map(|e| if e.trim_ascii().is_empty() { b"x".to_vec() } else { e })
                .collect();
            let mut bytes = Vec::new();
            for (i, event) in events.iter().enumerate() {
                if i > 0 {
                    let gap: &[u8] = match pattern {
                        r"(\|\|)" => b"||",
                        _ => rng.pick(&[&b"\n"[..], b"\r\n", b"\n\n", b"\r\n\r\n"]),
                    };
                    bytes.extend(gap);
                }
                bytes.extend(event);
            }
            if rng.chance(2) {
                // The last line end is dropped from the last event.
                bytes.extend(line_end(rng, cov));
            }
            (bytes, events)
        }
    }
}

/// `bytes` in a codec drawn at random, as a writer would put them.
fn compressed(rng: &mut Rng, cov: &mut Reached, bytes: &[u8]) -> Vec<u8> {
    let codec = *rng.pick(&["none", "gzip", "zstd", "lz4", "snappy"]);
    cov.reach(format!("compression.{codec}"));
    match codec {
        "none" => bytes.to_vec(),
        "gzip" => {
            // A gzip file may be several members end to end, as a writer appending to it makes.
            let at = if rng.chance(3) {
                cov.reach("compression.gzipMembers");
                bytes.len() / 2
            } else {
                bytes.len()
            };
            let mut out = Vec::new();
            for part in [&bytes[..at], &bytes[at..]] {
                if part.is_empty() && !out.is_empty() {
                    continue;
                }
                let mut member = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                member.write_all(part).unwrap();
                out.extend(member.finish().unwrap());
            }
            out
        }
        "zstd" => zstd::encode_all(bytes, 3).unwrap(),
        "lz4" => {
            let mut w = lz4_flex::frame::FrameEncoder::new(Vec::new());
            w.write_all(bytes).unwrap();
            w.finish().unwrap()
        }
        _ => {
            let mut w = snap::write::FrameEncoder::new(Vec::new());
            w.write_all(bytes).unwrap();
            w.into_inner().unwrap()
        }
    }
}

/// A key: sometimes with what a notification must encode, and unique by `i`.
fn key_name(rng: &mut Rng, cov: &mut Reached, i: usize) -> String {
    let name = *rng.pick(&["a", "event", "c+d", "x y", "100%", "é", "a=b&c"]);
    if !name.chars().all(|c| c.is_ascii_alphanumeric()) {
        cov.reach("key.special");
    }
    let dir = *rng.pick(&["", "sub/", "a b/deep/"]);
    let ext = *rng.pick(&[".log", ".log.gz", ".json", ""]);
    format!("{dir}{name}-{i}{ext}")
}

/// Puts `bytes` at `key` in `store`.
fn put(store: &dyn ObjectStore, runtime: &tokio::runtime::Runtime, key: &str, bytes: &[u8]) -> Result<(), String> {
    let path = Path::parse(key).map_err(|e| format!("{key:?} is no path: {e}"))?;
    runtime
        .block_on(store.put(&path, PutPayload::from(bytes.to_vec())))
        .map(|_| ())
        .map_err(|e| format!("put {key:?}: {e}"))
}

/// Every key in `store`, with its bytes.
fn all(store: &dyn ObjectStore, runtime: &tokio::runtime::Runtime) -> BTreeMap<String, Vec<u8>> {
    runtime.block_on(async {
        use futures_util::TryStreamExt;
        let metas: Vec<_> = store.list(None).try_collect().await.unwrap();
        let mut out = BTreeMap::new();
        for m in metas {
            let bytes = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            out.insert(m.location.to_string(), bytes.to_vec());
        }
        out
    })
}

// -- notifications ----------------------------------------------------------------------------------------------

/// `key` as S3's own notification writes it: form-encoded, a space a `+`.
fn form_encoded(key: &str) -> String {
    let mut out = String::new();
    for b in key.bytes() {
        match b {
            b' ' => out.push('+'),
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => out.push(b as char),
            b => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `key` as a blob URL's path writes it.
fn percent_encoded(key: &str) -> String {
    form_encoded(key).replace('+', "%20")
}

/// A message's text, as the kind's service writes one announcing `objects`, which may be none.
struct Note {
    /// SQS's body or a Storage queue message's text; for Pub/Sub, the attributes as JSON.
    text: String,
}

/// S3's notification of `objects`, in one of the shapes it reaches a queue in.
fn s3_note(rng: &mut Rng, cov: &mut Reached, objects: &[Object]) -> Note {
    if objects.is_empty() {
        let text = match rng.below(3) {
            0 => {
                cov.reach("s3.testEvent");
                json!({"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "logs"})
            }
            1 => json!({"source": "aws.s3", "detail-type": "Object Deleted",
                        "detail": {"bucket": {"name": "logs"}, "object": {"key": "gone"}}}),
            _ => json!({"Records": [{"eventName": "ObjectRemoved:Delete",
                        "s3": {"bucket": {"name": "logs"}, "object": {"key": "gone"}}}]}),
        };
        return Note { text: text.to_string() };
    }
    if objects.len() == 1 && rng.chance(3) {
        cov.reach("s3.eventBridge");
        let o = &objects[0];
        let mut object = json!({"key": o.key, "size": o.body.len()});
        if let Some(v) = &o.version {
            object["version-id"] = json!(v);
        }
        let event = json!({"version": "0", "source": "aws.s3", "detail-type": "Object Created",
                           "detail": {"bucket": {"name": o.bucket}, "object": object}});
        return Note {
            text: event.to_string(),
        };
    }
    cov.reach("s3.records");
    let mut records = Vec::new();
    for o in objects {
        if rng.chance(5) {
            records.push(json!({"eventName": "ObjectRemoved:Delete",
                                "s3": {"bucket": {"name": o.bucket}, "object": {"key": "gone"}}}));
        }
        let mut object = json!({"key": form_encoded(&o.key), "size": o.body.len()});
        if let Some(v) = &o.version {
            object["versionId"] = json!(v);
        }
        let name = *rng.pick(&[
            "ObjectCreated:Put",
            "ObjectCreated:CompleteMultipartUpload",
            "ObjectCreated:Copy",
        ]);
        records.push(
            json!({"eventVersion": "2.1", "eventSource": "aws:s3", "eventName": name,
                            "s3": {"bucket": {"name": o.bucket}, "object": object}}),
        );
    }
    let event = json!({ "Records": records }).to_string();
    if rng.chance(3) {
        cov.reach("s3.sns");
        let wrapped = json!({"Type": "Notification", "MessageId": "1", "Message": event});
        return Note {
            text: wrapped.to_string(),
        };
    }
    Note { text: event }
}

/// Event Grid's notification of `objects` under `blobs`, the account's blob services.
fn azure_note(rng: &mut Rng, cov: &mut Reached, objects: &[Object], blobs: &[String]) -> Note {
    let cloud = rng.chance(2);
    if cloud {
        cov.reach("azure.cloudEvents");
    }
    let event = |kind: &str, api: &str, url: String| {
        let data = json!({"api": api, "url": url, "blobType": "BlockBlob"});
        if cloud {
            json!({"specversion": "1.0", "type": kind, "source": "/subscriptions/x", "id": "1", "data": data})
        } else {
            json!({"eventType": kind, "subject": "/blobServices/default", "id": "1", "data": data})
        }
    };
    let mut events = Vec::new();
    for o in objects {
        let base = if blobs.len() > 1 && rng.chance(2) {
            cov.reach("azure.dfs");
            &blobs[1]
        } else {
            &blobs[0]
        };
        let mut url = format!("{base}/{}/{}", o.bucket, percent_encoded(&o.key));
        if rng.chance(4) {
            cov.reach("azure.query");
            url.push_str("?sv=2021-08-06&sig=x");
        }
        let api = if base.contains(".dfs.") {
            "FlushWithClose"
        } else {
            "PutBlob"
        };
        events.push(event("Microsoft.Storage.BlobCreated", api, url));
    }
    if events.is_empty() || rng.chance(5) {
        if rng.chance(2) {
            cov.reach("azure.createFile");
            events.push(event(
                "Microsoft.Storage.BlobCreated",
                "CreateFile",
                format!("{}/logs/new", blobs[0]),
            ));
        } else {
            events.push(event(
                "Microsoft.Storage.BlobDeleted",
                "DeleteBlob",
                format!("{}/logs/gone", blobs[0]),
            ));
        }
    }
    let text = if events.len() == 1 && rng.chance(2) {
        events.pop().unwrap().to_string()
    } else {
        Value::Array(events).to_string()
    };
    if rng.chance(2) {
        cov.reach("azure.base64");
        return Note {
            text: smeltr::format::bytes_text::BytesForm::Base64.text(text.as_bytes()),
        };
    }
    Note { text }
}

/// Cloud Storage's notification of `objects`, at most one, as Pub/Sub message attributes.
fn pubsub_note(rng: &mut Rng, objects: &[Object]) -> Note {
    let attributes = match objects.first() {
        Some(o) => {
            let mut a = json!({"eventType": "OBJECT_FINALIZE", "payloadFormat": "NONE", "bucketId": o.bucket,
                               "objectId": o.key, "notificationConfig": "projects/_/buckets/b/notificationConfigs/1"});
            if let Some(v) = &o.version {
                a["objectGeneration"] = json!(v);
            }
            a
        }
        None => json!({"eventType": rng.pick(&["OBJECT_DELETE", "OBJECT_METADATA_UPDATE", "OBJECT_ARCHIVE"]),
                       "bucketId": "logs", "objectId": "gone"}),
    };
    Note {
        text: attributes.to_string(),
    }
}

/// What the kind's parser reads `note` as.
fn parse(kind: Kind, note: &Note, blobs: &[String]) -> Result<Vec<ObjectRef>, String> {
    match kind {
        Kind::S3Sqs => smeltr::io::sqs::notification(&note.text),
        Kind::AzureBlobQueue => smeltr::io::storage_queue::notification(&note.text, blobs),
        Kind::GcsPubSub => {
            let attributes: Map<String, Value> = serde_json::from_str(&note.text).unwrap();
            smeltr::io::pubsub::notification(&attributes)
        }
        _ => unreachable!("a listing has no notifications"),
    }
}

// -- reading ----------------------------------------------------------------------------------------------------

/// How long a seed waits: for one poll, and for what it waits on to come.
#[derive(Clone, Copy)]
struct Pace {
    poll: Duration,
    patience: Duration,
}

const OFFLINE: Pace = Pace {
    poll: Duration::from_millis(20),
    patience: Duration::from_secs(30),
};

/// A live listing lists once a second, and a steady one reads an object at the second listing that finds it.
const LIVE: Pace = Pace {
    poll: Duration::from_millis(250),
    patience: Duration::from_secs(120),
};

/// How a seed reads: rows and bytes per poll, and whether it commits each batch as it comes or all at the end.
struct Reading {
    max_rows: usize,
    max_bytes: usize,
    as_read: bool,
    pace: Pace,
}

impl Reading {
    fn draw(rng: &mut Rng, cov: &mut Reached, pace: Pace) -> Reading {
        let small = rng.chance(3);
        if small {
            cov.reach("poll.small");
        }
        let as_read = rng.chance(2);
        if as_read {
            cov.reach("commit.asRead");
        }
        Reading {
            max_rows: if small { *rng.pick(&[1, 7]) } else { 10_000 },
            max_bytes: if small { *rng.pick(&[64, 4096]) } else { 64 << 20 },
            as_read,
            pace,
        }
    }

    fn poll(&self, source: &mut dyn Source) -> Result<Vec<SourceRecord>, String> {
        source
            .poll_batch(self.max_rows, self.max_bytes, self.pace.poll)
            .map_err(|e| format!("poll: {e:?}"))
    }

    /// One poll, which must read nothing: everything there was is read.
    fn quiet(&self, source: &mut dyn Source) -> Result<(), String> {
        match self.poll(source)?.len() {
            0 => Ok(()),
            n => Err(format!("{n} records more than there were")),
        }
    }

    /// Polls `source` until `want` records came, committing as drawn, and answers them.
    fn read(&self, source: &mut dyn Source, want: usize) -> Result<Vec<SourceRecord>, String> {
        let until = Instant::now() + self.pace.patience;
        let mut records = Vec::new();
        while records.len() < want {
            if Instant::now() > until {
                return Err(format!("only {} of {want} records came", records.len()));
            }
            let batch = self.poll(source)?;
            if self.as_read {
                commit(source, &batch)?;
            }
            records.extend(batch);
        }
        if !self.as_read {
            commit(source, &records)?;
        }
        Ok(records)
    }
}

/// The watermark the reader would commit for `records`.
fn commit(source: &mut dyn Source, records: &[SourceRecord]) -> Result<(), String> {
    let mut offsets = CommitMap::new();
    for r in records {
        let at = offsets
            .entry(TopicPartition::new(r.topic.clone(), r.partition))
            .or_insert(r.offset);
        *at = (*at).max(r.offset);
    }
    source.commit(&offsets).map_err(|e| format!("commit: {e:?}"))
}

/// Checks `records` are `objects`' records, each object's in order under its own topic and offsets, and nothing more.
/// An Avro record is its record object as JSON, whose keys `serde_json` writes in order.
fn check_records(c: &ObjectSourceConfig, records: &[SourceRecord], objects: &[Object]) -> Result<(), String> {
    let mut got: BTreeMap<String, Vec<(i32, i64, Vec<u8>)>> = BTreeMap::new();
    for r in records {
        let bytes = match &c.avro {
            Some(avro) => {
                let json = avro.meta.record_json(r, "");
                let value: Value =
                    serde_json::from_slice(&json).map_err(|e| format!("a record that is no JSON: {e}"))?;
                serde_json::to_vec(&value).unwrap()
            }
            None => r.bytes().ok_or("a record with no bytes")?.to_vec(),
        };
        got.entry(r.topic.to_string())
            .or_default()
            .push((r.partition, r.offset, bytes));
    }
    let mut want: BTreeMap<String, &Vec<Vec<u8>>> = BTreeMap::new();
    for o in objects.iter().filter(|o| !o.records.is_empty()) {
        want.insert(format!("{}/{}", o.bucket, o.key), &o.records);
    }
    let (got_topics, want_topics): (BTreeSet<&String>, BTreeSet<&String>) =
        (got.keys().collect(), want.keys().collect());
    if got_topics != want_topics {
        return Err(format!("read the objects {got_topics:?}, wanted {want_topics:?}"));
    }
    for (topic, rows) in &got {
        let partitions: BTreeSet<i32> = rows.iter().map(|r| r.0).collect();
        if partitions.len() != 1 {
            return Err(format!("{topic} came on the partitions {partitions:?}"));
        }
        let offsets: Vec<i64> = rows.iter().map(|r| r.1).collect();
        if offsets != (0..rows.len() as i64).collect::<Vec<_>>() {
            return Err(format!("{topic} came at the offsets {offsets:?}"));
        }
        let bytes: Vec<&Vec<u8>> = rows.iter().map(|r| &r.2).collect();
        let wanted: Vec<&Vec<u8>> = want[topic].iter().collect();
        if bytes != wanted {
            let show = |b: &[&Vec<u8>]| -> Vec<String> {
                b.iter()
                    .take(12)
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .collect()
            };
            return Err(format!(
                "{topic} read {} records, wanted {}:\n  {:?}\n  {:?}",
                bytes.len(),
                wanted.len(),
                show(&bytes),
                show(&wanted)
            ));
        }
    }
    Ok(())
}

/// Draws `n` objects in `buckets`, under `prefix`, each announced at a version when `versions`.
fn objects(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &ObjectSourceConfig,
    buckets: &[&str],
    prefix: &str,
    n: usize,
    versions: bool,
) -> Vec<Object> {
    (0..n)
        .map(|i| {
            let (bytes, records) = match c.avro {
                Some(_) => avro_body(rng, cov),
                None => body(rng, cov, &c.framing),
            };
            let version = (versions && rng.chance(4)).then(|| {
                cov.reach("message.version");
                format!("{}", 1_700_000_000_000_000 + rng.below(1000))
            });
            Object {
                bucket: rng.pick(buckets).to_string(),
                key: format!("{prefix}{}", key_name(rng, cov, i)),
                version,
                body: compressed(rng, cov, &bytes),
                records,
            }
        })
        .collect()
}

/// Where a queue kind's notifications go, and what is left on it.
trait Announcer {
    /// Puts the message `note`, which announces `objects`.
    fn send(&mut self, note: &Note, objects: Vec<ObjectRef>) -> Result<(), String>;

    /// How many messages sent are still on the queue.
    fn left(&mut self) -> Result<usize, String>;

    /// Whether objects may be announced at a version: one the store does not hold cannot be read. The in-memory store
    /// reads its one version whatever is asked, so only the parse of the notification checks the version.
    fn versions(&self) -> bool {
        false
    }

    /// For a queue that counts no message received and not yet deleted, how long one stays hidden: `left` sees an
    /// undeleted one only once the source closed and that lapsed.
    fn lapse(&self) -> Option<Duration> {
        None
    }
}

#[derive(Default)]
struct Calls {
    inbox: Vec<Message>,
    deleted: Vec<String>,
}

/// Hands out what is in its inbox once, and remembers every delete.
#[derive(Clone, Default)]
struct FakeQueue(Arc<Mutex<Calls>>);

impl Queue for FakeQueue {
    fn receive(&self, max: usize, _: Duration, _: Duration) -> Result<Vec<Message>, IoError> {
        let mut calls = self.0.lock().unwrap();
        let n = calls.inbox.len().min(max);
        Ok(calls.inbox.drain(..n).collect())
    }

    fn extend(&self, _: &[String], _: Duration) -> Vec<(String, String)> {
        Vec::new()
    }

    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        self.0.lock().unwrap().deleted.extend_from_slice(handles);
        Vec::new()
    }
}

/// A [`FakeQueue`] each message goes on as its kind parsed it.
#[derive(Default)]
struct FakeAnnouncer {
    queue: FakeQueue,
    sent: BTreeSet<String>,
}

impl Announcer for FakeAnnouncer {
    fn send(&mut self, _: &Note, objects: Vec<ObjectRef>) -> Result<(), String> {
        let handle = format!("m{}", self.sent.len());
        self.sent.insert(handle.clone());
        self.queue.0.lock().unwrap().inbox.push(Message {
            handle,
            objects: Ok(objects),
        });
        Ok(())
    }

    fn left(&mut self) -> Result<usize, String> {
        let deleted: BTreeSet<String> = self.queue.0.lock().unwrap().deleted.iter().cloned().collect();
        Ok(self.sent.difference(&deleted).count())
    }

    fn versions(&self) -> bool {
        true
    }
}

/// Reads drawn objects in `stores`, by bucket, through the queue kind's notifications, each parsed as the kind parses
/// it and sent with `announcer`, from the source `open` makes, and checks every message is deleted once its records
/// are committed.
#[allow(clippy::too_many_arguments)]
fn read_queue(
    rng: &mut Rng,
    cov: &mut Reached,
    kind: Kind,
    c: &ObjectSourceConfig,
    stores: &BTreeMap<String, Arc<dyn ObjectStore>>,
    announcer: &mut dyn Announcer,
    open: impl FnOnce() -> Result<Box<dyn Source>, String>,
    runtime: &tokio::runtime::Runtime,
    pace: Pace,
) -> Result<(), String> {
    let buckets: Vec<&str> = stores.keys().map(String::as_str).collect();
    let blobs = match &c.queue {
        ObjectQueue::StorageQueue { access, .. } => smeltr::io::storage_queue::blob_services(access),
        _ => Vec::new(),
    };
    let mut all_objects = Vec::new();
    for m in 0..1 + rng.below(4) {
        // Pub/Sub announces one object a message.
        let most = if kind == Kind::GcsPubSub { 1 } else { 3 };
        let n = if rng.chance(6) { 0 } else { 1 + rng.below(most) };
        match n {
            0 => cov.reach("message.announcesNone"),
            2.. => cov.reach("message.several"),
            _ => {}
        }
        let mut announced = objects(rng, cov, c, &buckets, "", n, announcer.versions());
        for (i, o) in announced.iter_mut().enumerate() {
            o.key = format!("m{m}/{i}/{}", o.key);
            put(stores[&o.bucket].as_ref(), runtime, &o.key, &o.body)?;
        }
        let note = match kind {
            Kind::S3Sqs => s3_note(rng, cov, &announced),
            Kind::AzureBlobQueue => azure_note(rng, cov, &announced, &blobs),
            _ => pubsub_note(rng, &announced),
        };
        let parsed = parse(kind, &note, &blobs).map_err(|e| format!("{e}\n{}", note.text))?;
        let wanted: Vec<ObjectRef> = announced
            .iter()
            .map(|o| ObjectRef {
                bucket: o.bucket.clone(),
                key: o.key.clone(),
                // A blob URL names no version.
                version: o.version.clone().filter(|_| kind != Kind::AzureBlobQueue),
            })
            .collect();
        if parsed != wanted {
            return Err(format!(
                "the notification parsed as {parsed:?}, announced {wanted:?}\n{}",
                note.text
            ));
        }
        announcer.send(&note, parsed)?;
        all_objects.extend(announced);
    }
    let mut source = open()?;
    let reading = Reading::draw(rng, cov, pace);
    let want = all_objects.iter().map(|o| o.records.len()).sum();
    let records = reading.read(source.as_mut(), want)?;
    check_records(c, &records, &all_objects)?;
    if let Some(lapse) = announcer.lapse() {
        // Whatever was not deleted shows again once its visibility lapses, held by no one. A last message that
        // announces nothing to read may still be on its way in: poll for as long as it stays hidden.
        let until = Instant::now() + lapse;
        while Instant::now() < until {
            reading.quiet(source.as_mut())?;
        }
        drop(source);
        std::thread::sleep(lapse * 2);
        return match announcer.left()? {
            0 => Ok(()),
            n => Err(format!("{n} messages were never deleted")),
        };
    }
    let until = Instant::now() + pace.patience;
    loop {
        let left = announcer.left()?;
        if left == 0 {
            return Ok(());
        }
        if Instant::now() > until {
            return Err(format!("{left} messages were never deleted"));
        }
        reading.quiet(source.as_mut())?;
    }
}

/// Reads drawn objects put in `store`, the bucket the source `open` makes lists, and checks each is kept, deleted or
/// moved once committed, and nothing outside the prefix is read or touched.
#[allow(clippy::too_many_arguments)]
fn read_listing(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &ObjectSourceConfig,
    store: Arc<dyn ObjectStore>,
    open: impl FnOnce() -> Result<Box<dyn Source>, String>,
    runtime: &tokio::runtime::Runtime,
    pace: Pace,
) -> Result<(), String> {
    let ObjectQueue::List {
        place,
        prefix,
        start,
        after_commit,
        ..
    } = &c.queue
    else {
        unreachable!("a listing kind loads as a listing");
    };
    cov.reach(match after_commit {
        AfterCommit::Keep => "afterCommit.keep",
        AfterCommit::Delete => "afterCommit.delete",
        AfterCommit::Move { .. } => "afterCommit.move",
    });
    let bucket = list_root(place);
    let mut decoys = BTreeMap::new();
    if !prefix.is_empty() && rng.chance(2) {
        cov.reach("listing.decoy");
        for key in ["zz-decoy/1.log", "zz-decoy.log"] {
            put(store.as_ref(), runtime, key, b"never read\n")?;
            decoys.insert(key.to_string(), b"never read\n".to_vec());
        }
    }
    let mut old = Vec::new();
    if *start == ListStart::New {
        let n = 1 + rng.below(2);
        old = objects(rng, cov, c, &[&bucket], prefix, n, false);
        for o in &mut old {
            o.key = format!("{prefix}old-{}", &o.key[prefix.len()..]);
            put(store.as_ref(), runtime, &o.key, &o.body)?;
        }
    }
    let mut source = open()?;
    source
        .resume(smeltr::io::Progress::default())
        .map_err(|e| format!("resume: {e:?}"))?;
    let reading = Reading::draw(rng, cov, pace);
    if *start == ListStart::New {
        // The first listing records what is there as read, and says so with its cursor. A start at new objects keeps
        // what it reads, so its listing hands out progress.
        let until = Instant::now() + pace.patience;
        while (source.take_progress()).is_none_or(|p| p.cursor.as_deref() != Some("listed")) {
            if Instant::now() > until {
                return Err("the first listing never finished".into());
            }
            reading
                .quiet(source.as_mut())
                .map_err(|e| format!("started at new objects, and read old ones: {e}"))?;
        }
    }
    let n = 1 + rng.below(4);
    let new = objects(rng, cov, c, &[&bucket], prefix, n, false);
    for o in &new {
        put(store.as_ref(), runtime, &o.key, &o.body)?;
    }
    let want = new.iter().map(|o| o.records.len()).sum();
    let records = reading.read(source.as_mut(), want)?;
    check_records(c, &records, &new)?;

    // What the bucket must hold once each object read is committed.
    let mut after: BTreeMap<String, Vec<u8>> = decoys.clone();
    for o in old.iter().chain(&new) {
        match after_commit {
            AfterCommit::Keep => {
                after.insert(o.key.clone(), o.body.clone());
            }
            AfterCommit::Delete => {}
            AfterCommit::Move { prefix: to } => {
                after.insert(format!("{to}{}", &o.key[prefix.len()..]), o.body.clone());
            }
        }
    }
    let until = Instant::now() + pace.patience;
    loop {
        reading.quiet(source.as_mut())?;
        let held = all(store.as_ref(), runtime);
        if held == after {
            break;
        }
        if Instant::now() > until {
            let (held, after): (Vec<&String>, Vec<&String>) = (held.keys().collect(), after.keys().collect());
            return Err(format!("{after_commit:?} left {held:?}, wanted {after:?}"));
        }
    }
    if *after_commit == AfterCommit::Keep {
        // What was read is kept once its delete reaches the listing, which may be after the commit returns. An empty
        // object is never read, so never kept.
        let read: BTreeSet<String> = (new.iter().filter(|o| !o.body.is_empty()))
            .map(|o| o.key.clone())
            .collect();
        let mut kept = BTreeSet::new();
        let until = Instant::now() + pace.patience;
        while !read.is_subset(&kept) {
            if Instant::now() > until {
                return Err(format!("kept {kept:?}, read {read:?}"));
            }
            while let Some(progress) = source.take_progress() {
                kept.extend(progress.added.into_iter().map(|e| e.item));
            }
            reading.quiet(source.as_mut())?;
        }
    }
    Ok(())
}

/// The bucket, container or directory a listing lists, as each object's topic starts.
fn list_root(place: &ListPlace) -> String {
    match place {
        ListPlace::S3 { bucket, .. } => bucket.clone(),
        ListPlace::Gcs(g) => g.bucket.clone(),
        ListPlace::Azure(a) => a.container.clone(),
        ListPlace::Sftp(f) => f.directory.clone(),
    }
}

// -- the offline fuzzer -----------------------------------------------------------------------------------------

/// Each seed's source, against the save, the client it builds, and objects read through its queue over in-memory
/// stores.
#[test]
fn object_sources_fuzz() {
    crate::certs::install_provider();
    let mut cov = Coverage::new(options());
    let runtime = smeltr::io::objects::runtime().unwrap();
    fuzz::run(0..SEEDS, |seed| {
        let mut reached = Reached::default();
        let checked = offline_seed(seed, &mut reached, &runtime);
        if checked.is_ok() {
            reached.credit(&mut cov);
        }
        checked
    });
    cov.assert_complete();
}

fn offline_seed(seed: u64, reached: &mut Reached, runtime: &Arc<tokio::runtime::Runtime>) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let mut draw = draw(&mut rng, reached, Target::Offline, None);
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled && destination::mangle(&mut rng, reached, &mut draw.source, &["unknownSetting"]);
    let json = bundle(&draw).to_string();
    if mangled {
        let refused = refusals(&json);
        let loads = load_str(&json).is_ok();
        if unknown && (refused.is_ok_and(|r| r.is_empty()) || loads) {
            return Err(format!("a key it does not know is taken\n{json}"));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(|e| format!("{e}\n{json}"))? else {
        return Ok(());
    };

    let (secrets, unheld) = held(&draw.source);
    match (smeltr::io::object_source::open(&c, &secrets), unheld) {
        (Ok(_), false) => {}
        (Err(e), true) if format!("{e:?}").contains(UNSET_VARIABLE) => {}
        (Ok(_), true) => return Err(format!("built with a credential it does not hold\n{json}")),
        (Err(e), _) => return Err(format!("does not build: {e:?}\n{json}")),
    }

    let read = if draw.kind.listed() {
        let store = Arc::new(InMemory::new());
        let ObjectQueue::List {
            place,
            prefix,
            start,
            after_commit,
            ..
        } = &c.queue
        else {
            unreachable!("a listing kind loads as a listing");
        };
        // The listing `open` makes, over the in-memory store and listing as often as it is asked.
        let open = || {
            let mut listing = smeltr::io::listing::Listing::new(
                store.clone(),
                &list_root(place),
                prefix,
                Duration::ZERO,
                *start,
                after_commit.clone(),
                runtime.clone(),
            )
            .map_err(|e| format!("no listing: {e:?}"))?;
            if matches!(place, ListPlace::Azure(_) | ListPlace::Sftp(_)) {
                listing = listing.when_steady();
            }
            in_memory(Arc::new(listing), store.clone(), &c, runtime)
        };
        read_listing(&mut rng, reached, &c, store.clone(), open, runtime, OFFLINE)
    } else {
        let stores: BTreeMap<String, Arc<dyn ObjectStore>> = ["logs", "other"]
            .iter()
            .map(|b| (b.to_string(), Arc::new(InMemory::new()) as Arc<dyn ObjectStore>))
            .collect();
        let mut announcer = FakeAnnouncer::default();
        let queue = announcer.queue.clone();
        let by_bucket = stores.clone();
        let (framing, visibility, avro) = (c.framing.clone(), c.visibility, c.avro.clone());
        let open = move || {
            let source = ObjectSource::new(
                Arc::new(queue),
                Box::new(move |bucket: &str| {
                    (by_bucket.get(bucket).cloned()).ok_or_else(|| IoError::transient(format!("no bucket {bucket}")))
                }),
                runtime.clone(),
                framing,
                visibility,
                MAX_RECORD_BYTES,
            )
            .map_err(|e| format!("no source: {e:?}"))?;
            Ok(Box::new(reading(source, avro.as_ref())?) as Box<dyn Source>)
        };
        read_queue(
            &mut rng,
            reached,
            draw.kind,
            &c,
            &stores,
            &mut announcer,
            open,
            runtime,
            OFFLINE,
        )
    };
    read.map_err(|e| format!("{e}\n{json}"))
}

/// An object source reading what `queue` offers from `store`, framed as `c` says.
fn in_memory(
    queue: Arc<dyn Queue>,
    store: Arc<InMemory>,
    c: &ObjectSourceConfig,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<Box<dyn Source>, String> {
    let source = ObjectSource::new(
        queue,
        Box::new(move |_: &str| Ok(store.clone() as Arc<dyn ObjectStore>)),
        runtime.clone(),
        c.framing.clone(),
        c.visibility,
        MAX_RECORD_BYTES,
    )
    .map_err(|e| format!("no source: {e:?}"))?;
    Ok(Box::new(reading(source, c.avro.as_ref())?))
}

/// `source`, reading Avro where `avro` says, as `object_source::open` builds it.
fn reading(source: ObjectSource, avro: Option<&smeltr::plan::topology::AvroRead>) -> Result<ObjectSource, String> {
    Ok(match avro {
        Some(avro) => source.reading_avro(
            smeltr::io::avro::AvroReader::new(avro, None, &Default::default()).map_err(|e| format!("{e:?}"))?,
        ),
        None => source,
    })
}

// -- the live fuzzers -------------------------------------------------------------------------------------------

const LIVE_SEEDS: u64 = 30;

/// A name no earlier run used, fit for a bucket, a queue or a directory: lowercase letters, digits and `-`.
fn unique(prefix: &str) -> String {
    // The count keeps apart two names asked for within one tick of a coarse clock.
    static COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}-{n}")
}

/// What a live seed made fresh for its draw: the buckets a queue kind's objects go in, and its queue.
struct Made {
    buckets: Vec<String>,
    announcer: Option<Box<dyn Announcer>>,
}

/// Runs the live seeds of `kind`, drawn against the store at `address`. `make` makes what each draw reads fresh, and
/// names it in the draw.
fn live_fuzz(
    kind: Kind,
    address: &str,
    mut make: impl FnMut(&mut Rng, &mut Map<String, Value>) -> Result<Made, String>,
) {
    crate::certs::install_provider();
    let runtime = smeltr::io::objects::runtime().unwrap();
    let mut cov = Coverage::new(live_options(kind));
    fuzz::run(0..LIVE_SEEDS, |seed| {
        let mut reached = Reached::default();
        let mut rng = Rng::seeded(seed);
        let mut draw = draw(&mut rng, &mut reached, Target::Live(address), Some(kind));
        let mut made = make(&mut rng, &mut draw.source)?;
        let json = bundle(&draw).to_string();
        let c = check_save(&draw, &json)
            .map_err(|e| format!("{e}\n{json}"))?
            .expect("a live draw is never broken");
        let (secrets, _) = held(&draw.source);
        let stores = live_stores(&c, &secrets, &runtime, &made.buckets).map_err(|e| format!("{e:?}\n{json}"))?;
        let open = || smeltr::io::object_source::open(&c, &secrets).map_err(|e| format!("does not build: {e:?}"));
        let read = match made.announcer.as_mut() {
            None => {
                let store = stores.into_values().next().expect("a listing's store");
                read_listing(&mut rng, &mut reached, &c, store, open, &runtime, LIVE)
            }
            Some(announcer) => read_queue(
                &mut rng,
                &mut reached,
                kind,
                &c,
                &stores,
                announcer.as_mut(),
                open,
                &runtime,
                LIVE,
            ),
        };
        read.map_err(|e| format!("{e}\n{json}"))?;
        reached.credit(&mut cov);
        Ok(())
    });
    cov.assert_complete();
}

/// What a live draw of `kind` reaches: every option an emulator serves, signed in as it takes.
fn live_options(kind: Kind) -> Vec<String> {
    let mut options = vec![
        "secret.leaderStored",
        "framing.lines",
        "framing.object",
        "framing.escapedLines",
        "framing.lineBreaker",
        "compression.none",
        "compression.gzip",
        "compression.zstd",
        "compression.lz4",
        "compression.snappy",
        "key.special",
        "poll.small",
        "commit.asRead",
        "format.avro",
        "avro.codec.null",
        "avro.codec.deflate",
        "avro.unclaimed",
        "avro.undeclared",
        "avro.nested",
    ];
    if kind.listed() {
        options.extend([
            "prefix",
            "afterCommit.keep",
            "afterCommit.delete",
            "afterCommit.move",
            "start.new",
        ]);
    } else {
        options.push("message.announcesNone");
    }
    match kind {
        Kind::S3Sqs => options.extend([
            "s3.roleArn",
            "message.several",
            "s3.records",
            "s3.sns",
            "s3.eventBridge",
            "s3.testEvent",
        ]),
        Kind::S3List => options.push("s3.roleArn"),
        Kind::AzureBlobQueue => options.extend([
            "azure.accessKey",
            "azure.sasToken",
            "message.several",
            "azure.cloudEvents",
            "azure.base64",
            "azure.createFile",
            "azure.query",
        ]),
        Kind::AzureBlobList => options.extend(["azure.accessKey", "azure.sasToken"]),
        Kind::Sftp => options.extend(["sftp.password", "sftp.privateKey"]),
        Kind::GcsList | Kind::GcsPubSub => {}
    }
    let mut options: Vec<String> = options.into_iter().map(String::from).collect();
    options.push(format!("kind.{}", kind.name()));
    options
}

/// A writer to each of `buckets`, or to the one a listing lists, signed as `c`'s source is.
fn live_stores(
    c: &ObjectSourceConfig,
    secrets: &smeltr::plan::topology::HeldSecrets,
    runtime: &Arc<tokio::runtime::Runtime>,
    buckets: &[String],
) -> Result<BTreeMap<String, Arc<dyn ObjectStore>>, IoError> {
    use smeltr::io::objects::{Aws, Azure, Gcp};
    let _entered = runtime.enter();
    fn each(
        buckets: &[String],
        bucket: impl Fn(&str) -> Result<Arc<dyn ObjectStore>, IoError>,
    ) -> Result<BTreeMap<String, Arc<dyn ObjectStore>>, IoError> {
        (buckets.iter()).map(|b| Ok((b.clone(), bucket(b)?))).collect()
    }
    match &c.queue {
        ObjectQueue::Sqs { access, .. } => {
            let aws = Aws::new(access, None, secrets, runtime)?;
            each(buckets, |b| aws.bucket(b))
        }
        ObjectQueue::StorageQueue { access, .. } => {
            let azure = Azure::new(access, None, secrets, runtime)?;
            each(buckets, |b| azure.container(b))
        }
        ObjectQueue::PubSub { access, .. } => {
            let gcp = Gcp::new(access, None, secrets, runtime)?;
            each(buckets, |b| gcp.bucket(b))
        }
        ObjectQueue::List { place, .. } => {
            let store: Arc<dyn ObjectStore> = match place {
                ListPlace::S3 { bucket, access } => Aws::new(access, None, secrets, runtime)?.bucket(bucket)?,
                ListPlace::Gcs(g) => Gcp::new(&g.access, None, secrets, runtime)?.bucket(&g.bucket)?,
                ListPlace::Azure(a) => Azure::new(&a.access, None, secrets, runtime)?.container(&a.container)?,
                ListPlace::Sftp(f) => Arc::new(smeltr::io::sftp::Sftp::new(f, "", true, secrets)?),
            };
            Ok(BTreeMap::from([(list_root(place), store)]))
        }
    }
}

/// An HTTP call to an emulator, which needs no signature here: its answer's status, headers and body.
fn call(method: &str, url: &str, body: &str) -> Result<(u16, ureq::http::HeaderMap, String), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let request = ureq::http::Request::builder()
        .method(method)
        .uri(url)
        .header(
            "Content-Type",
            if body.starts_with('<') {
                "application/xml"
            } else {
                "application/json"
            },
        )
        .body(body.to_string())
        .map_err(|e| e.to_string())?;
    let mut response = agent.run(request).map_err(|e| format!("{method} {url}: {e}"))?;
    let text = response.body_mut().read_to_string().unwrap_or_default();
    let status = response.status().as_u16();
    if status >= 300 {
        return Err(format!("{method} {url}: {status} {text}"));
    }
    Ok((status, response.headers().clone(), text))
}

/// SQS's `action` on LocalStack, which takes any signature.
fn sqs(endpoint: &str, action: &str, body: Value) -> Result<Value, String> {
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let request = ureq::http::Request::builder()
        .method("POST")
        .uri(endpoint)
        .header("X-Amz-Target", format!("AmazonSQS.{action}"))
        .header("Content-Type", "application/x-amz-json-1.0")
        .body(body.to_string())
        .map_err(|e| e.to_string())?;
    let mut response = agent.run(request).map_err(|e| format!("sqs {action}: {e}"))?;
    let text = response.body_mut().read_to_string().unwrap_or_default();
    if !response.status().is_success() {
        return Err(format!("sqs {action}: {} {text}", response.status()));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// An SQS queue on LocalStack.
struct SqsQueue {
    endpoint: String,
    url: String,
}

impl Announcer for SqsQueue {
    fn send(&mut self, note: &Note, _: Vec<ObjectRef>) -> Result<(), String> {
        sqs(
            &self.endpoint,
            "SendMessage",
            json!({"QueueUrl": self.url, "MessageBody": note.text}),
        )
        .map(|_| ())
    }

    /// Received and not deleted, a message still counts, hidden.
    fn left(&mut self) -> Result<usize, String> {
        let attributes = sqs(
            &self.endpoint,
            "GetQueueAttributes",
            json!({"QueueUrl": self.url, "AttributeNames": ["All"]}),
        )?;
        let count = |name: &str| -> Result<usize, String> {
            (attributes["Attributes"][name].as_str())
                .and_then(|n| n.parse().ok())
                .ok_or(format!("no {name} in {attributes}"))
        };
        Ok(count("ApproximateNumberOfMessages")? + count("ApproximateNumberOfMessagesNotVisible")?)
    }
}

/// A Storage queue on Azurite, reached with the account's SAS.
struct StorageQueue {
    url: String,
}

impl Announcer for StorageQueue {
    fn send(&mut self, note: &Note, _: Vec<ObjectRef>) -> Result<(), String> {
        let text = note
            .text
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        let body = format!("<QueueMessage><MessageText>{text}</MessageText></QueueMessage>");
        call("POST", &format!("{}/messages?{AZURE_SAS}", self.url), &body).map(|_| ())
    }

    /// Received and not deleted, a message still counts, hidden.
    fn left(&mut self) -> Result<usize, String> {
        let (_, headers, _) = call("GET", &format!("{}?comp=metadata&{AZURE_SAS}", self.url), "")?;
        (headers.get("x-ms-approximate-messages-count"))
            .and_then(|n| n.to_str().ok()?.parse().ok())
            .ok_or(format!("no message count in {headers:?}"))
    }
}

/// A Pub/Sub topic and its subscription on the emulator.
struct Subscription {
    topic: String,
    subscription: String,
    visibility: Duration,
}

impl Announcer for Subscription {
    fn send(&mut self, note: &Note, _: Vec<ObjectRef>) -> Result<(), String> {
        let attributes: Value = serde_json::from_str(&note.text).unwrap();
        let message =
            json!({"data": smeltr::format::bytes_text::BytesForm::Base64.text(b"{}"), "attributes": attributes});
        let body = json!({ "messages": [message] }).to_string();
        call("POST", &format!("{}:publish", self.topic), &body).map(|_| ())
    }

    /// Pulls a few times, since one immediate pull may answer nothing while messages are left. Each one pulled is
    /// held from the next pull, so none counts twice.
    fn left(&mut self) -> Result<usize, String> {
        let body = json!({"maxMessages": 100, "returnImmediately": true}).to_string();
        let mut left = 0;
        for _ in 0..5 {
            let (_, _, text) = call("POST", &format!("{}:pull", self.subscription), &body)?;
            let pulled: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            left += pulled["receivedMessages"].as_array().map_or(0, Vec::len);
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(left)
    }

    fn lapse(&self) -> Option<Duration> {
        Some(self.visibility)
    }
}

/// Makes `bucket` on LocalStack.
fn s3_bucket(endpoint: &str, bucket: &str) -> Result<(), String> {
    call("PUT", &format!("{endpoint}/{bucket}"), "").map(|_| ())
}

/// Makes `bucket` on the Cloud Storage emulator.
fn gcs_bucket(endpoint: &str, bucket: &str) -> Result<(), String> {
    let body = json!({ "name": bucket }).to_string();
    call("POST", &format!("{endpoint}/storage/v1/b?project=smeltr"), &body).map(|_| ())
}

/// Makes `container` on Azurite.
fn azure_container(endpoint: &str, container: &str) -> Result<(), String> {
    call(
        "PUT",
        &format!("{endpoint}/{container}?restype=container&{AZURE_SAS}"),
        "",
    )
    .map(|_| ())
}

#[test]
fn live_s3_sqs_fuzz() {
    let Some(endpoint) = live::s3("live_s3_sqs_fuzz") else {
        return;
    };
    live_fuzz(Kind::S3Sqs, &endpoint, |_, d| {
        let buckets = vec![unique("smeltr-fuzz"), unique("smeltr-fuzz")];
        for b in &buckets {
            s3_bucket(&endpoint, b)?;
        }
        let name = unique("smeltr-fuzz");
        sqs(&endpoint, "CreateQueue", json!({ "QueueName": name }))?;
        let url = format!("{endpoint}/000000000000/{name}");
        d.insert("queueUrl".into(), json!(url));
        let announcer = Box::new(SqsQueue {
            endpoint: endpoint.clone(),
            url,
        });
        Ok(Made {
            buckets,
            announcer: Some(announcer),
        })
    });
}

#[test]
fn live_s3_list_fuzz() {
    let Some(endpoint) = live::s3("live_s3_list_fuzz") else {
        return;
    };
    live_fuzz(Kind::S3List, &endpoint, |_, d| {
        let bucket = unique("smeltr-fuzz");
        s3_bucket(&endpoint, &bucket)?;
        d.insert("bucket".into(), json!(bucket));
        d.insert("pollIntervalSecs".into(), json!(1));
        Ok(Made {
            buckets: Vec::new(),
            announcer: None,
        })
    });
}

#[test]
fn live_gcs_list_fuzz() {
    let Some(endpoint) = live::gcs("live_gcs_list_fuzz") else {
        return;
    };
    live_fuzz(Kind::GcsList, &endpoint, |_, d| {
        let bucket = unique("smeltr-fuzz");
        gcs_bucket(&endpoint, &bucket)?;
        d.insert("bucket".into(), json!(bucket));
        d.insert("pollIntervalSecs".into(), json!(1));
        Ok(Made {
            buckets: Vec::new(),
            announcer: None,
        })
    });
}

#[test]
fn live_gcs_pubsub_fuzz() {
    let test = "live_gcs_pubsub_fuzz";
    let (Some(gcs), Some(pubsub)) = (live::gcs(test), live::pubsub(test)) else {
        return;
    };
    live_fuzz(Kind::GcsPubSub, &gcs, |rng, d| {
        let buckets = vec![unique("smeltr-fuzz"), unique("smeltr-fuzz")];
        for b in &buckets {
            gcs_bucket(&gcs, b)?;
        }
        let name = unique("smeltr-fuzz");
        let topic = format!("{pubsub}/v1/projects/smeltr/topics/{name}");
        call("PUT", &topic, "{}")?;
        let subscription = format!("projects/smeltr/subscriptions/{name}");
        let body = json!({ "topic": format!("projects/smeltr/topics/{name}") }).to_string();
        call("PUT", &format!("{pubsub}/v1/{subscription}"), &body)?;
        // Short, so an unacknowledged message shows again soon after the source closes.
        let visibility = 1 + rng.below(3) as u64;
        d.insert("subscription".into(), json!(subscription));
        d.insert("pubsubEndpoint".into(), json!(pubsub));
        d.insert("visibilitySecs".into(), json!(visibility));
        let announcer = Box::new(Subscription {
            topic,
            subscription: format!("{pubsub}/v1/{subscription}"),
            visibility: Duration::from_secs(visibility),
        });
        Ok(Made {
            buckets,
            announcer: Some(announcer),
        })
    });
}

#[test]
fn live_azure_blob_list_fuzz() {
    let Some(endpoint) = live::azure("live_azure_blob_list_fuzz") else {
        return;
    };
    live_fuzz(Kind::AzureBlobList, &endpoint, |_, d| {
        let container = unique("smeltr-fuzz");
        azure_container(&endpoint, &container)?;
        d.insert("container".into(), json!(container));
        d.insert("pollIntervalSecs".into(), json!(1));
        // Azurite answers a blob batch, which is how a delete is sent, 500 under a SAS.
        if d.contains_key("sasToken") {
            d.remove("afterCommit");
            d.remove("movePrefix");
        }
        Ok(Made {
            buckets: Vec::new(),
            announcer: None,
        })
    });
}

#[test]
fn live_azure_blob_queue_fuzz() {
    let test = "live_azure_blob_queue_fuzz";
    let (Some(blobs), Some(queues)) = (live::azure(test), live::azure_queue(test)) else {
        return;
    };
    live_fuzz(Kind::AzureBlobQueue, &blobs, |_, d| {
        let buckets = vec![unique("smeltr-fuzz"), unique("smeltr-fuzz")];
        for b in &buckets {
            azure_container(&blobs, b)?;
        }
        let url = format!("{queues}/{}", unique("smeltr-fuzz"));
        call("PUT", &format!("{url}?{AZURE_SAS}"), "")?;
        d.insert("queueUrl".into(), json!(url));
        Ok(Made {
            buckets,
            announcer: Some(Box::new(StorageQueue { url })),
        })
    });
}

#[test]
fn live_sftp_fuzz() {
    let Some((host, port)) = live::sftp("live_sftp_fuzz") else {
        return;
    };
    let runtime = smeltr::io::objects::runtime().unwrap();
    live_fuzz(Kind::Sftp, &format!("{host}:{port}"), |_, d| {
        // The server's upload directory is there already; one under it is made by putting a file in it.
        let mut secrets = smeltr::plan::topology::HeldSecrets::default();
        secrets.set_stored([("sftp-password".to_string(), 1, SFTP_PASSWORD.to_string())]);
        let upload = smeltr::plan::topology::SftpStore {
            directory: "upload".to_string(),
            access: smeltr::plan::topology::SftpAccess {
                host: host.clone(),
                port,
                username: "smeltr".to_string(),
                credential: smeltr::plan::topology::SftpCredential::Password(
                    smeltr::plan::topology::SecretRef::LeaderStored {
                        name: "sftp-password".to_string(),
                        version: 1,
                    },
                ),
                host_key: SFTP_HOST_KEY.trim().to_string(),
            },
        };
        let writer = smeltr::io::sftp::Sftp::new(&upload, "", false, &secrets).map_err(|e| format!("{e:?}"))?;
        let directory = unique("smeltr-fuzz");
        let made = Path::from(format!("{directory}/.made"));
        put(&writer, &runtime, made.as_ref(), b"")?;
        runtime.block_on(writer.delete(&made)).map_err(|e| e.to_string())?;
        d.insert("directory".into(), json!(format!("upload/{directory}")));
        d.insert("pollIntervalSecs".into(), json!(1));
        Ok(Made {
            buckets: Vec::new(),
            announcer: None,
        })
    });
}
