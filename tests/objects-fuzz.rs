//! The object store destinations' fuzzers: S3, Google Cloud Storage, Azure Blob Storage and SFTP. Each seed draws one
//! destination as a bundle names it, every option at random, and sometimes breaks one of its settings.
//!
//! Offline, a destination the save accepts loads as its bundle says, loads the same after a save, builds its client
//! when this worker holds every credential it names and refuses to when it does not, and writes drawn batches into an
//! in-memory store that read back as the rows, each under its prefix, in its format and codec. A row whose prefix
//! would misplace its object, or whose bytes hold a line break the destination refuses, is refused alone. One the
//! save refuses is refused on the setting that was broken. Any other change to its JSON may be refused, and never
//! panics.
//!
//! Live, the same draws, pointed at the emulators `just live-up` runs and signed with what they take, write there and
//! read back the same.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Map, Value, json};
use smeltr::config_file::load_str;
use smeltr::io::{OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Cloud, Compression, DestinationKind, HeldSecrets, Newlines, ObjectFormat, ObjectsDestinationConfig, Records,
    SecretRef, SftpCredential,
};

use crate::harness::destination;
use crate::harness::fuzz::Rng;
use crate::harness::{self};
use crate::live;
use crate::store_access::{
    self, ACCESS_OPTIONS, AZURE_SAS, Broken, Compare, GCS_KEY, Reached, SFTP_HOST_KEY, SFTP_PASSWORD, Store, Target,
    UNSET_VARIABLE, access_breaks, authority_pem, break_access, compare_azure, compare_credentials, compare_gcs,
    compare_s3, compare_sftp, connector_secrets, draw_access, held, refusals, text, uint,
};

const SEEDS: u64 = 800;
const LIVE_SEEDS: u64 = 100;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// What a Parquet destination's rows are: a pipeline declaring two columns and parsing `_raw` into them.
const PARQUET_PIPELINE: &str = "n: i64; msg: str; Object.assign($, json(_raw));";

/// The destination type that writes to `store`.
fn type_name(store: Store) -> &'static str {
    match store {
        Store::S3 => "s3",
        Store::Gcs => "gcs",
        Store::Azure => "azureBlob",
        Store::Sftp => "sftp",
    }
}

/// One drawn destination.
struct Draw {
    store: Store,
    /// The destination's object in the bundle.
    destination: Map<String, Value>,
    /// Whether the destination trusts an authority of its own.
    tls: bool,
    /// The setting broken, if one was. `Some(None)` is a break the save refuses on no one field of it.
    broken: Option<Option<&'static str>>,
}

/// Every option the generator knows besides the access ones. Each must be drawn into a destination the save accepts,
/// or into the break it names, over the seed range.
const OPTIONS: &[&str] = &[
    "store.s3",
    "store.gcs",
    "store.azureBlob",
    "store.sftp",
    "prefix",
    "prefix.placeholders",
    "format.ndjson",
    "format.json",
    "format.parquet",
    "format.avro",
    "format.csv",
    "format.text",
    "format.bytes",
    "field.text",
    "field.bytes",
    "newlines.escape",
    "newlines.refuse",
    "compression.none",
    "compression.gzip",
    "compression.snappy",
    "compression.lz4",
    "compression.zstd",
    "inFlight",
    "maxObjectBytes",
    "bufferBytes",
    "stallAfterSecs",
    "fileIntervalSecs",
    "storageClass",
    "tls",
    "s3.kmsKeyId",
    "row.prefix",
    "row.prefixRefused",
    "row.lineBreakRefused",
    "batches.several",
    "objects.several",
    "broken.prefix",
    "broken.field",
    "broken.newlines",
    "broken.compression",
    "broken.maxObjectBytes",
    "broken.bufferBytes",
    "broken.inFlight",
    "broken.stallAfterSecs",
    "broken.fileIntervalSecs",
    "broken.storageClass",
    "broken.tls",
    "broken.bucket",
    "broken.s3.kmsKeyId",
    "mangled",
];

/// Every option, the access ones too.
fn options() -> impl Iterator<Item = &'static str> {
    OPTIONS.iter().chain(ACCESS_OPTIONS).copied()
}

// -- drawing a destination --------------------------------------------------------------------------------------

fn draw(rng: &mut Rng, cov: &mut Reached, target: Target, only: Option<Store>) -> Draw {
    let store = only.unwrap_or_else(|| *rng.pick(&[Store::S3, Store::Gcs, Store::Azure, Store::Sftp]));
    cov.reach(format!("store.{}", type_name(store)));
    let mut d = Map::new();
    d.insert("id".into(), json!("lake"));
    d.insert("type".into(), json!(type_name(store)));
    let mut tls = false;
    match store {
        Store::S3 => {
            d.insert("bucket".into(), json!(rng.pick(&["logs", "logs-2024", "a.b.c"])));
        }
        Store::Gcs => {
            d.insert("bucket".into(), json!(rng.pick(&["logs", "logs_eu", "a-b"])));
        }
        Store::Azure => {
            d.insert("container".into(), json!(rng.pick(&["logs", "logs-2024", "abc"])));
        }
        Store::Sftp => {}
    }
    draw_access(rng, cov, target, store, &mut d);
    if store == Store::S3 && !target.live() && rng.chance(4) {
        cov.reach("s3.kmsKeyId");
        d.insert("kmsKeyId".into(), json!("alias/logs"));
    }
    if store != Store::Sftp {
        // An archived object cannot be read back until it is rehydrated, so a live draw writes none.
        let class = storage_class(rng, store)
            .filter(|c| !(target.live() && matches!(*c, "Archive" | "GLACIER" | "DEEP_ARCHIVE")));
        if let Some(class) = class {
            cov.reach("storageClass");
            d.insert("storageClass".into(), json!(class));
        }
        // TLS settings are the client's: an emulator speaks plain HTTP, which load refuses them on.
        let https = d
            .get("endpoint")
            .and_then(Value::as_str)
            .is_none_or(|e| e.starts_with("https://"));
        if !target.live() && https && rng.chance(4) {
            cov.reach("tls");
            tls = true;
            d.insert("authorities".into(), json!(authority_pem()));
        }
    }
    draw_objects(rng, cov, store, &mut d);
    let mut draw = Draw {
        store,
        destination: d,
        tls,
        broken: None,
    };
    if !target.live() && rng.chance(3) {
        draw.broken = Some(break_one(rng, cov, &mut draw));
    }
    draw
}

fn storage_class(rng: &mut Rng, store: Store) -> Option<&'static str> {
    let classes = match store {
        Store::S3 => smeltr::io::objects::S3_STORAGE_CLASSES,
        Store::Gcs => smeltr::io::objects::GCS_STORAGE_CLASSES,
        Store::Azure => smeltr::io::objects::AZURE_ACCESS_TIERS,
        Store::Sftp => return None,
    };
    rng.chance(3).then(|| *rng.pick(classes))
}

/// The settings every object store destination shares.
fn draw_objects(rng: &mut Rng, cov: &mut Reached, store: Store, d: &mut Map<String, Value>) {
    if rng.chance(2) {
        let prefix = *rng.pick(&[
            "",
            "logs/",
            "a/b/c/",
            "%Y/%m/%d/%H/",
            "fw/%Y-%m-%d/%H%M/",
            "ünï/cödé/",
            "plain",
        ]);
        cov.reach("prefix");
        if prefix.contains("%Y") {
            cov.reach("prefix.placeholders");
        }
        d.insert("prefix".into(), json!(prefix));
    }
    let format = *rng.pick(&["ndjson", "json", "parquet", "avro", "csv", "text", "bytes"]);
    cov.reach(format!("format.{format}"));
    if format != "ndjson" || rng.chance(2) {
        d.insert("format".into(), json!(format));
    }
    match format {
        "text" if rng.chance(2) => {
            cov.reach("field.text");
            d.insert("field".into(), json!(rng.pick(&["_raw", "message"])));
        }
        "bytes" => {
            if rng.chance(2) {
                cov.reach("field.bytes");
                d.insert("field".into(), json!("__source.bytes"));
            }
            if rng.chance(2) {
                let newlines = *rng.pick(&["escape", "refuse"]);
                cov.reach(format!("newlines.{newlines}"));
                d.insert("newlines".into(), json!(newlines));
            }
        }
        _ => {}
    }
    // Avro has no lz4 codec: drawn there, it is a break.
    let compression = match format {
        "avro" => *rng.pick(&["none", "gzip", "snappy", "zstd"]),
        _ => *rng.pick(&["none", "gzip", "snappy", "lz4", "zstd"]),
    };
    cov.reach(format!("compression.{compression}"));
    if compression != "gzip" || rng.chance(2) {
        d.insert("compression".into(), json!(compression));
    }
    if rng.chance(2) {
        cov.reach("inFlight");
        d.insert("inFlight".into(), json!(rng.pick(&[1, 2, 4, 16, 64])));
    }
    if rng.chance(2) {
        cov.reach("maxObjectBytes");
        d.insert(
            "maxObjectBytes".into(),
            json!(rng.pick(&[64 * KIB, 64 * KIB, MIB, 64 * MIB, 64 * GIB])),
        );
    }
    if rng.chance(2) {
        cov.reach("bufferBytes");
        d.insert(
            "bufferBytes".into(),
            json!(rng.pick(&[4 * MIB, 64 * MIB, 32 * 1024 * GIB])),
        );
    }
    let floor = if store == Store::Sftp { 15 } else { 1 };
    if store == Store::Sftp || rng.chance(2) {
        cov.reach("stallAfterSecs");
        d.insert("stallAfterSecs".into(), json!(rng.pick(&[floor, 30, 60, 120])));
    }
    if rng.chance(2) {
        cov.reach("fileIntervalSecs");
        d.insert("fileIntervalSecs".into(), json!(rng.pick(&[5, 60, 3600])));
    }
}

/// Breaks one setting of `draw`, and answers the field the save must refuse it on.
fn break_one(rng: &mut Rng, cov: &mut Reached, draw: &mut Draw) -> Option<&'static str> {
    let d = &mut draw.destination;
    let format = d.get("format").and_then(Value::as_str).unwrap_or("ndjson").to_string();
    let mut breaks: Vec<&'static str> = vec![
        "prefix",
        "maxObjectBytes",
        "bufferBytes",
        "inFlight",
        "stallAfterSecs",
        "fileIntervalSecs",
    ];
    if !matches!(format.as_str(), "text" | "bytes") {
        breaks.push("field");
    }
    if format != "bytes" {
        breaks.push("newlines");
    }
    if format == "avro" {
        breaks.push("compression");
    }
    if draw.store != Store::Sftp {
        breaks.extend(["storageClass", "tls", "bucket"]);
    }
    if draw.store == Store::S3 {
        breaks.push("s3.kmsKeyId");
    }
    breaks.extend(access_breaks(draw.store));
    let what = *rng.pick(&breaks);
    cov.reach(format!("broken.{what}"));
    let mut set = |k: &str, v: Value| {
        d.insert(k.to_string(), v);
    };
    match what {
        "prefix" => {
            set("prefix", json!(rng.pick(&["/abs/", "%Q/", "a//b/", "../up/", "100%/"])));
            Some("prefix")
        }
        "field" => {
            set("field", json!("_raw"));
            Some("field")
        }
        "newlines" => {
            set("newlines", json!("refuse"));
            Some("newlines")
        }
        "compression" => {
            set("compression", json!("lz4"));
            Some("compression")
        }
        "maxObjectBytes" => {
            set("maxObjectBytes", json!(rng.pick(&[0, 64 * KIB - 1, 64 * GIB + 1])));
            Some("maxObjectBytes")
        }
        "bufferBytes" => {
            set("bufferBytes", json!(rng.pick(&[0, 4 * MIB - 1, 32 * 1024 * GIB + 1])));
            Some("bufferBytes")
        }
        "inFlight" => {
            set("inFlight", json!(rng.pick(&[0, 65])));
            Some("inFlight")
        }
        "stallAfterSecs" => {
            let low = if draw.store == Store::Sftp { 14 } else { 0 };
            set("stallAfterSecs", json!(rng.pick(&[low, 121])));
            Some("stallAfterSecs")
        }
        "fileIntervalSecs" => {
            set("fileIntervalSecs", json!(rng.pick(&[0, 4, 3601])));
            Some("fileIntervalSecs")
        }
        "storageClass" => {
            set("storageClass", json!(rng.pick(&["COLD_STORAGE", " ", "standard"])));
            Some("storageClass")
        }
        "tls" => {
            // A certificate by a blank name, or an authority reached over plain HTTP.
            if rng.chance(2) {
                set("certificate", json!(" "));
                Some("certificate")
            } else {
                set("authorities", json!(authority_pem()));
                set("endpoint", json!("http://127.0.0.1:9000"));
                draw.tls = true;
                Some("endpoint")
            }
        }
        "bucket" => {
            let key = if draw.store == Store::Azure {
                "container"
            } else {
                "bucket"
            };
            set(key, json!(rng.pick(&["", "  "])));
            Some(key)
        }
        "s3.kmsKeyId" => {
            set("kmsKeyId", json!(" "));
            Some("kmsKeyId")
        }
        access => break_access(rng, access, d),
    }
}

/// `draw` as a whole bundle: a Kafka source, and one route through `pipeline` to the destination.
fn bundle(draw: &Draw, pipeline: &str) -> Value {
    json!({
        "version": 1,
        "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
        "destinations": [Value::Object(draw.destination.clone())],
        "pipelines": [{"id": "p", "name": "p", "steps": pipeline_steps(pipeline)}],
        "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["lake"]}]}],
    })
}

fn pipeline_steps(code: &str) -> Value {
    if code.is_empty() {
        json!([])
    } else {
        json!([{"id": "parse", "type": "code", "code": code}])
    }
}
// -- what the save says ------------------------------------------------------------------------------------------

/// The destination `json` loads as.
fn loaded(json: &str) -> Result<(ObjectsDestinationConfig, Vec<SecretRef>), String> {
    let system = load_str(json).map_err(|e| format!("{e:?}"))?;
    match &system.topology.destinations[0].kind {
        DestinationKind::Objects(c) => Ok((c.clone(), connector_secrets(&system.topology))),
        other => Err(format!("loaded as {other:?}")),
    }
}

/// Checks the save: a broken draw is refused on its field, and an unbroken one is accepted, loads as drawn, and
/// loads the same once saved.
fn check_save(draw: &Draw, json: &str) -> Result<Option<ObjectsDestinationConfig>, String> {
    let config = store_access::check_save(json, draw.broken.map(Broken::Field), destination::loads, loaded)?;
    if let Some((c, named)) = &config {
        check_loaded(draw, c, named)?;
    }
    Ok(config.map(|(c, _)| c))
}

/// What `draw` says, against what it loaded as.
fn check_loaded(draw: &Draw, c: &ObjectsDestinationConfig, named: &[SecretRef]) -> Result<(), String> {
    let d = &draw.destination;
    let mut cmp = Compare::default();
    cmp.expect("prefix", text(d, "prefix").unwrap_or("%Y/%m/%d/%H/".into()), &c.prefix);
    let compression = match text(d, "compression").as_deref().unwrap_or("gzip") {
        "none" => Compression::None,
        "gzip" => Compression::Gzip,
        "snappy" => Compression::Snappy,
        "lz4" => Compression::Lz4,
        _ => Compression::Zstd,
    };
    cmp.expect("compression", compression, c.compression);
    let field = text(d, "field");
    let format = match text(d, "format").as_deref().unwrap_or("ndjson") {
        "ndjson" => ObjectFormat::Ndjson,
        "json" => ObjectFormat::Json,
        "parquet" => ObjectFormat::Parquet,
        "avro" => ObjectFormat::Avro,
        "csv" => ObjectFormat::Csv,
        "text" => ObjectFormat::Text {
            field: field.unwrap_or("_raw".to_string()).into(),
        },
        _ => ObjectFormat::Bytes {
            field: field.unwrap_or("__source.bytes".to_string()).into(),
            newlines: match text(d, "newlines").as_deref() {
                Some("refuse") => Newlines::Refuse,
                _ => Newlines::Escape,
            },
        },
    };
    cmp.expect("format", format, &c.format);
    cmp.expect("inFlight", uint(d, "inFlight", 4), c.in_flight as u64);
    cmp.expect(
        "maxObjectBytes",
        uint(d, "maxObjectBytes", 64 * MIB),
        c.max_object_bytes,
    );
    cmp.expect("bufferBytes", uint(d, "bufferBytes", 256 * MIB), c.buffer_bytes);
    cmp.expect("stallAfterSecs", uint(d, "stallAfterSecs", 30), c.stall_after.as_secs());
    cmp.expect(
        "fileIntervalSecs",
        uint(d, "fileIntervalSecs", 60),
        c.file_interval.as_secs(),
    );
    cmp.expect("storageClass", text(d, "storageClass"), &c.storage_class);
    cmp.expect("tls", draw.tls, c.tls.is_some());
    match &c.cloud {
        Cloud::S3(s3) => {
            cmp.expect("bucket", text(d, "bucket").unwrap(), &s3.bucket);
            cmp.expect("kmsKeyId", text(d, "kmsKeyId"), &s3.kms_key_id);
            compare_s3(&mut cmp, d, &s3.access);
        }
        Cloud::Gcs(gcs) => {
            cmp.expect("bucket", text(d, "bucket").unwrap(), &gcs.bucket);
            compare_gcs(&mut cmp, d, &gcs.access);
        }
        Cloud::Azure(azure) => {
            cmp.expect("container", text(d, "container").unwrap(), &azure.container);
            compare_azure(&mut cmp, d, &azure.access);
        }
        Cloud::Sftp(sftp) => compare_sftp(&mut cmp, d, &sftp.directory, &sftp.access),
    }
    compare_credentials(&mut cmp, d, named);
    cmp.done()
}

// -- writing ----------------------------------------------------------------------------------------------------

/// A row and what it must become: its line in the object, under the prefix it goes to. `None` is refused alone.
struct Sent {
    row: OwnedRecord,
    /// The prefix its object's key starts with: the row's own, literal, or `None` for the destination's.
    prefix: Option<String>,
    line: Option<Vec<u8>>,
}

/// A JSON record, maybe padded with what the line formats trim.
fn record_text(rng: &mut Rng, n: usize) -> (String, String, String) {
    let msg: String = (0..rng.below(12))
        .map(|_| *rng.pick(&['a', 'b', 'z', '0', '9', 'é', '"', '\\', ' ', ',', '\n', '\u{2028}']))
        .collect();
    let body = serde_json::to_string(&json!({"n": n, "msg": msg})).unwrap();
    let padded = match rng.below(4) {
        0 => format!(" {body}\n"),
        1 => format!("{body}\r\n"),
        _ => body.clone(),
    };
    (padded, body, msg)
}

/// Bytes in any encoding, sometimes with a line break.
fn raw_bytes(rng: &mut Rng) -> Vec<u8> {
    (0..1 + rng.below(16))
        .map(|_| *rng.pick(&[b'a', b'0', b' ', b'\\', b'\n', b'\r', 0xe9, 0xff, 0x00, b'%']))
        .collect()
}

fn escaped(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for &b in bytes {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            _ => out.push(b),
        }
    }
    out
}

/// A row's own prefix: none, one that places it, or one that would misplace its object. Each starts `row`, which no
/// destination's prefix does and no object id can, so an object's key says which it went under.
fn row_prefix(rng: &mut Rng, cov: &mut Reached) -> (Option<String>, bool) {
    match rng.below(6) {
        0..=2 => (None, true),
        3..=4 => {
            cov.reach("row.prefix");
            (
                Some(
                    rng.pick(&["row-a/", "row-b/c/", "row100%/", "row-ünï/", "rowx"])
                        .to_string(),
                ),
                true,
            )
        }
        _ => {
            cov.reach("row.prefixRefused");
            (Some(rng.pick(&["/abs/", "a//b/", "../", "./x/"]).to_string()), false)
        }
    }
}

/// A batch of `rows` rows for `c`, numbered from `first`. Parquet rows are cut from `plan` over their records.
fn rows_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &ObjectsDestinationConfig,
    plan: Option<&smeltr::plan::plan::Plan>,
    first: usize,
    rows: usize,
) -> Vec<Sent> {
    let mut drawn: Vec<(Option<String>, bool)> = (0..rows).map(|_| row_prefix(rng, cov)).collect();
    match &c.format {
        format if format.writes_columns() => {
            let plan = plan.expect("a plan for Parquet, Avro and CSV");
            let drawn_records: Vec<(String, String, String)> = (0..rows).map(|i| record_text(rng, first + i)).collect();
            let refs: Vec<&str> = drawn_records.iter().map(|(_, body, _)| body.as_str()).collect();
            let chunk = harness::cut(plan, &refs);
            drawn
                .drain(..)
                .enumerate()
                .map(|(i, (prefix, placed))| Sent {
                    row: OwnedRecord {
                        payload: Payload::Record,
                        placement: prefix.clone(),
                        columns: Some(smeltr::io::parquet::Columns {
                            chunk: chunk.clone(),
                            row: i as u32,
                        }),
                        ..OwnedRecord::default()
                    },
                    prefix,
                    // The columns as the reader prints them, CSV's cells all quoted (`csv_rows`).
                    line: placed.then(|| {
                        let n = first + i;
                        let n = if c.format == ObjectFormat::Csv {
                            format!("\"{n}\"")
                        } else {
                            n.to_string()
                        };
                        format!("msg: \"{}\", n: {n}, ", drawn_records[i].2).into_bytes()
                    }),
                })
                .collect()
        }
        format => drawn
            .drain(..)
            .enumerate()
            .map(|(i, (prefix, placed))| {
                let (value, payload, line) = match format {
                    ObjectFormat::Ndjson | ObjectFormat::Json => {
                        let (padded, body, _) = record_text(rng, first + i);
                        (padded.into_bytes(), Payload::Record, Some(body.into_bytes()))
                    }
                    ObjectFormat::Text { field } => {
                        let text = record_text(rng, first + i).1;
                        (
                            text.clone().into_bytes(),
                            Payload::Text { field: field.clone() },
                            Some(text.into_bytes()),
                        )
                    }
                    ObjectFormat::Bytes { field, newlines } => {
                        let bytes = raw_bytes(rng);
                        let broken = bytes.iter().any(|b| matches!(b, b'\n' | b'\r'));
                        let line = match newlines {
                            Newlines::Refuse if broken => {
                                cov.reach("row.lineBreakRefused");
                                None
                            }
                            Newlines::Refuse => Some(bytes.clone()),
                            Newlines::Escape => Some(escaped(&bytes)),
                        };
                        (bytes, Payload::Bytes { field: field.clone() }, line)
                    }
                    ObjectFormat::Parquet | ObjectFormat::Avro | ObjectFormat::Csv => unreachable!(),
                };
                Sent {
                    row: OwnedRecord {
                        value,
                        payload,
                        placement: prefix.clone(),
                        ..OwnedRecord::default()
                    },
                    prefix,
                    line: if placed { line } else { None },
                }
            })
            .collect(),
    }
}

/// Every object in `store`, by key, as it was written.
fn read_all(store: &dyn ObjectStore, runtime: &tokio::runtime::Runtime) -> Vec<(String, Vec<u8>)> {
    runtime.block_on(async {
        let mut out = Vec::new();
        let mut todo = vec![None::<Path>];
        while let Some(at) = todo.pop() {
            let listed = store.list_with_delimiter(at.as_ref()).await.unwrap();
            todo.extend(listed.common_prefixes.into_iter().map(Some));
            for meta in listed.objects {
                let bytes = store.get(&meta.location).await.unwrap().bytes().await.unwrap();
                out.push((meta.location.to_string(), bytes.to_vec()));
            }
        }
        out.sort();
        out
    })
}

fn decode(codec: Compression, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let read = match codec {
        Compression::None => {
            out.extend_from_slice(bytes);
            Ok(0)
        }
        Compression::Gzip => flate2::read::GzDecoder::new(bytes).read_to_end(&mut out),
        Compression::Snappy => snap::read::FrameDecoder::new(bytes).read_to_end(&mut out),
        Compression::Lz4 => lz4_flex::frame::FrameDecoder::new(bytes).read_to_end(&mut out),
        Compression::Zstd => zstd::stream::decode_all(bytes).map(|d| {
            out = d;
            0
        }),
    };
    read.map(|_| out)
        .map_err(|e| format!("does not decode as {codec:?}: {e}"))
}

/// `prefix` with its placeholders filled for `at`, UTC.
fn expand(prefix: &str, at: SystemTime) -> String {
    let t = time::OffsetDateTime::from(at);
    prefix
        .replace("%Y", &format!("{:04}", t.year()))
        .replace("%m", &format!("{:02}", u8::from(t.month())))
        .replace("%d", &format!("{:02}", t.day()))
        .replace("%H", &format!("{:02}", t.hour()))
        .replace("%M", &format!("{:02}", t.minute()))
}

/// How long `prefix`, filled for some minute in `between`, is where it starts `key`. A live write may span several
/// minutes, and a file may be named in any of them.
fn under(key: &str, prefix: &str, between: (SystemTime, SystemTime)) -> Option<usize> {
    let mut at = between.0;
    loop {
        let filled = expand(prefix, at);
        if key.starts_with(filled.as_str()) {
            return Some(filled.len());
        }
        if at >= between.1 {
            return None;
        }
        at = (at + Duration::from_secs(60)).min(between.1);
    }
}

fn extension(c: &ObjectsDestinationConfig) -> String {
    let stem = match c.format {
        ObjectFormat::Ndjson => "ndjson",
        ObjectFormat::Json => "json",
        ObjectFormat::Csv => "csv",
        ObjectFormat::Parquet => return ".parquet".to_string(),
        ObjectFormat::Avro => return ".avro".to_string(),
        ObjectFormat::Text { .. } => "txt",
        ObjectFormat::Bytes { .. } => "raw",
    };
    let suffix = match c.compression {
        Compression::None => "",
        Compression::Gzip => ".gz",
        Compression::Snappy => ".sz",
        Compression::Lz4 => ".lz4",
        Compression::Zstd => ".zst",
    };
    format!(".{stem}{suffix}")
}

/// The lines of one object: its rows, one per line, or a JSON array's elements, or a Parquet, Avro or CSV file's rows
/// as the reader prints them.
fn lines_of(c: &ObjectsDestinationConfig, body: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if c.format == ObjectFormat::Avro {
        return avro_rows(body);
    }
    if c.format == ObjectFormat::Parquet {
        use parquet::file::reader::FileReader;
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("smeltr-objects-fuzz-{}-{n}.parquet", std::process::id()));
        std::fs::write(&path, body).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let reader =
            parquet::file::reader::SerializedFileReader::new(file).map_err(|e| format!("not a Parquet file: {e}"))?;
        return reader
            .get_row_iter(None)
            .map_err(|e| e.to_string())?
            .map(|r| r.map(|r| r.to_string().into_bytes()).map_err(|e| e.to_string()))
            .collect();
    }
    let plain = decode(c.compression, body)?;
    let Some(text) = plain.strip_suffix(b"\n") else {
        return Err(format!(
            "does not end in a line break: {:?}",
            String::from_utf8_lossy(&plain)
        ));
    };
    if c.format == ObjectFormat::Csv {
        return csv_rows(text);
    }
    if c.format == ObjectFormat::Json {
        let array: Vec<Value> = serde_json::from_slice(text).map_err(|e| format!("not one JSON array: {e}"))?;
        return Ok(array.iter().map(|v| serde_json::to_vec(v).unwrap()).collect());
    }
    Ok(text.split(|b| *b == b'\n').map(<[u8]>::to_vec).collect())
}

/// An Avro file's rows, as Avro's own reader reads them, each as Parquet's reader prints one: `name: value, ` for
/// every field, a string quoted.
fn avro_rows(body: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    use apache_avro::types::Value as Avro;
    fn print(v: &Avro) -> String {
        match v {
            Avro::Union(_, inner) => print(inner),
            Avro::Null => "null".to_string(),
            Avro::Long(n) => n.to_string(),
            Avro::String(s) => format!("\"{s}\""),
            other => format!("{other:?}"),
        }
    }
    let line = |fields: &[(String, Avro)]| -> Vec<u8> {
        let line: String = fields.iter().map(|(k, v)| format!("{k}: {}, ", print(v))).collect();
        line.into_bytes()
    };
    let reader = apache_avro::Reader::new(body).map_err(|e| format!("Avro's reader refuses it: {e}"))?;
    reader
        .map(|v| match v.map_err(|e| e.to_string())? {
            Avro::Record(fields) => Ok(line(&fields)),
            _ => Err("a value that is no record".to_string()),
        })
        .collect()
}

/// The leaves of `PARQUET_PIPELINE`'s record, as a CSV file's header names them.
const CSV_HEADER: &str = "_raw,msg,n,smeltr.exception.route,smeltr.exception.pipeline,smeltr.exception.exceptions,rest";

/// A CSV file's rows after its header, each split by smeltr's own CSV reader (`csv_scan`) and printed as Avro's are
/// (`avro_rows`) but every cell quoted: `name: "value", ` for a cell, `name: null, ` for an empty one.
fn csv_rows(text: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    // A record ends at a line break outside quotes; a doubled quote inside a cell toggles twice.
    let mut records: Vec<&[u8]> = Vec::new();
    let (mut start, mut quoted) = (0, false);
    for (i, &b) in text.iter().enumerate() {
        match b {
            b'"' => quoted = !quoted,
            b'\n' if !quoted => {
                records.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    records.push(&text[start..]);
    let cells = |record: &[u8]| -> Result<Vec<Option<String>>, String> {
        let fields = smeltr::format::csv_scan::split_line(record, b',')
            .ok_or_else(|| format!("not one CSV record: {:?}", String::from_utf8_lossy(record)))?;
        Ok(fields
            .into_iter()
            .map(|f| (f.quoted || !f.text.is_empty()).then(|| f.text.into_owned()))
            .collect())
    };
    let mut records = records.into_iter();
    let header: Vec<String> = cells(records.next().unwrap_or_default())?
        .into_iter()
        .flatten()
        .collect();
    if header.join(",") != CSV_HEADER {
        return Err(format!("the header is {header:?}, not {CSV_HEADER}"));
    }
    records
        .map(|record| {
            let r = cells(record)?;
            if r.len() != header.len() {
                return Err(format!(
                    "a row of {} cells under a header of {}: {r:?}",
                    r.len(),
                    header.len()
                ));
            }
            let line: String = (header.iter().zip(&r))
                .map(|(name, cell)| match cell {
                    Some(v) => format!("{name}: \"{v}\", "),
                    None => format!("{name}: null, "),
                })
                .collect();
            Ok(line.into_bytes())
        })
        .collect()
}

/// Checks that `objects` hold exactly the rows of `sent` not refused, each once, under its prefix, and that what was
/// refused is what had to be.
fn check_written(
    c: &ObjectsDestinationConfig,
    sent: &[Vec<Sent>],
    refused: &[Vec<Rejection>],
    objects: &[(String, Vec<u8>)],
    between: (SystemTime, SystemTime),
) -> Result<(), String> {
    destination::check_refused(sent, refused, |s| s.line.is_none())?;
    let ext = extension(c);
    // Each written row, by the prefix it was placed under, in the form the object reads back as.
    let mut want: BTreeMap<Option<String>, Vec<Vec<u8>>> = BTreeMap::new();
    for s in sent.iter().flatten() {
        if let Some(line) = &s.line {
            want.entry(s.prefix.clone()).or_default().push(line.clone());
        }
    }
    let mut got: BTreeMap<Option<String>, Vec<Vec<u8>>> = BTreeMap::new();
    for (key, body) in objects {
        if !key.ends_with(&ext) {
            return Err(format!("{key} does not end in {ext}"));
        }
        // A row's own prefix is literal and wins over the destination's; a row naming none goes under that.
        let placed = want
            .keys()
            .flatten()
            .filter(|p| key.starts_with(p.as_str()))
            .max_by_key(|p| p.len())
            .cloned();
        let id_at = match &placed {
            Some(p) => p.len(),
            None => under(key, &c.prefix, between).ok_or_else(|| format!("{key} is under no prefix written"))?,
        };
        let id = &key[id_at..key.len() - ext.len()];
        if id.len() != 32 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("{key}: {id:?} is not an object id"));
        }
        let lines = lines_of(c, body).map_err(|e| format!("{key}: {e}"))?;
        got.entry(placed).or_default().extend(lines);
    }
    for (prefix, lines) in want.iter_mut() {
        let found = got.remove(prefix).unwrap_or_default();
        if c.format.writes_columns() {
            // A Parquet, Avro or CSV row prints every column; the row is the one holding its columns.
            for line in lines.iter() {
                let n = String::from_utf8_lossy(line);
                let holding = found
                    .iter()
                    .filter(|f| String::from_utf8_lossy(f).contains(&*n))
                    .count();
                if holding != 1 {
                    return Err(format!("under {prefix:?}, {holding} rows hold `{n}`: {found:?}"));
                }
            }
            if found.len() != lines.len() {
                return Err(format!("under {prefix:?}, {} rows, not {}", found.len(), lines.len()));
            }
            continue;
        }
        let mut found = found;
        found.sort();
        lines.sort();
        if found != *lines {
            return Err(format!(
                "under {prefix:?}:\n  wrote {:?}\n  found {:?}",
                lines.iter().map(|l| String::from_utf8_lossy(l)).collect::<Vec<_>>(),
                found.iter().map(|l| String::from_utf8_lossy(l)).collect::<Vec<_>>(),
            ));
        }
    }
    if let Some((prefix, _)) = got.into_iter().next() {
        return Err(format!("objects under {prefix:?}, where nothing was written"));
    }
    Ok(())
}

/// The plan a Parquet destination's rows come from, and the records it writes.
fn parquet_plan() -> (smeltr::plan::plan::Plan, Records) {
    let plan = harness::compile_ok(vec![harness::code(0, PARQUET_PIPELINE)]);
    let records = Records::from([(None, harness::record(&plan))]);
    (plan, records)
}

/// One to three batches for `c`, sometimes large enough to fill several objects.
fn batches_for(
    rng: &mut Rng,
    cov: &mut Reached,
    c: &ObjectsDestinationConfig,
    plan: Option<&smeltr::plan::plan::Plan>,
) -> Vec<Vec<Sent>> {
    let count = 1 + rng.below(3);
    if count > 1 {
        cov.reach("batches.several");
    }
    let mut first = 0;
    (0..count)
        .map(|_| {
            // The smallest object is 64 KiB. Uncompressed, a JSON row is some 30 bytes and a bytes row some 10, so
            // these fill a few.
            let large = if matches!(c.format, ObjectFormat::Bytes { .. }) {
                24_000
            } else {
                8000
            };
            let rows = if c.max_object_bytes <= 64 * 1024 && c.compression == Compression::None && rng.chance(2) {
                large
            } else {
                1 + rng.below(12)
            };
            let batch = rows_for(rng, cov, c, plan, first, rows);
            first += rows;
            batch
        })
        .collect()
}

// -- the offline fuzzer -----------------------------------------------------------------------------------------

/// Each seed's destination, against the save, the client it builds, and an in-memory store it writes to.
#[test]
fn object_destinations_fuzz() {
    crate::certs::install_provider();
    let (plan, records) = parquet_plan();
    let runtime = smeltr::io::objects::runtime().unwrap();
    destination::run(0..SEEDS, options(), |seed, reached| {
        offline_seed(seed, reached, &plan, &records, &runtime)
    });
}

fn offline_seed(
    seed: u64,
    reached: &mut Reached,
    plan: &smeltr::plan::plan::Plan,
    records: &Records,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let mut draw = draw(&mut rng, reached, Target::Offline, None);
    let mangled = draw.broken.is_none() && rng.chance(12);
    reached.only_breaks = mangled || draw.broken.is_some();
    let unknown = mangled && destination::mangle(&mut rng, reached, &mut draw.destination, &["unknownSetting"]);
    let json = bundle(&draw, "").to_string();
    if mangled {
        let refused = refusals(&json);
        let loads = load_str(&json).is_ok();
        if unknown && (refused.is_ok() || loads) {
            return Err(format!("a key it does not know is taken\n{json}"));
        }
        return Ok(());
    }
    let Some(c) = check_save(&draw, &json).map_err(|e| format!("{e}\n{json}"))? else {
        return Ok(());
    };

    let (secrets, unheld) = held(&draw.destination);
    match (smeltr::io::objects::store(&c, &secrets, runtime), unheld) {
        (Ok(_), false) => {}
        (Err(e), true) if format!("{e:?}").contains(UNSET_VARIABLE) => {}
        (Ok(_), true) => return Err(format!("built with a credential it does not hold\n{json}")),
        (Err(e), _) => return Err(format!("does not build: {e:?}\n{json}")),
    }

    let store = Arc::new(InMemory::new());
    let parquet = c.format.writes_columns();
    let mut d = smeltr::io::objects::destination(&c, parquet.then_some(records), store.clone(), runtime.clone())
        .map_err(|e| format!("no destination: {e}\n{json}"))?;
    let sent = batches_for(&mut rng, reached, &c, Some(plan));
    let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
    let start = SystemTime::now();
    let refused = destination::write(&mut d, &batches, Duration::from_secs(30)).map_err(|e| format!("{e}\n{json}"))?;
    let objects = read_all(store.as_ref(), runtime);
    if several_under_one_prefix(&c, &objects) {
        reached.reach("objects.several");
    }
    check_written(&c, &sent, &refused, &objects, (start, SystemTime::now())).map_err(|e| format!("{e}\n{json}"))
}

/// Whether two objects share a prefix: what was written there outgrew one.
fn several_under_one_prefix(c: &ObjectsDestinationConfig, objects: &[(String, Vec<u8>)]) -> bool {
    let tail = 32 + extension(c).len();
    let mut prefixes: Vec<&str> = objects.iter().map(|(k, _)| &k[..k.len() - tail]).collect();
    let all = prefixes.len();
    prefixes.sort();
    prefixes.dedup();
    prefixes.len() < all
}

// -- the live fuzzers -------------------------------------------------------------------------------------------

/// A name no earlier run used.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Runs the live seeds on `store` at `address`: each draws a destination, makes its bucket with `make`, writes there,
/// and reads it back with `reader`.
fn live_fuzz(
    store: Store,
    address: &str,
    make: impl Fn(&mut Map<String, Value>),
    reader: impl Fn(&Map<String, Value>) -> Arc<dyn ObjectStore>,
) {
    crate::certs::install_provider();
    let runtime = smeltr::io::objects::runtime().unwrap();
    let (plan, records) = parquet_plan();
    destination::run(0..LIVE_SEEDS, live_options(store), |seed, reached| {
        let mut rng = Rng::seeded(seed);
        let mut draw = draw(&mut rng, reached, Target::Live(address), Some(store));
        make(&mut draw.destination);
        let json = bundle(&draw, "").to_string();
        let c = check_save(&draw, &json)
            .map_err(|e| format!("{e}\n{json}"))?
            .expect("a live draw is never broken");
        let (secrets, _) = held(&draw.destination);
        let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "fuzz");
        let parquet = c.format.writes_columns();
        let mut d = smeltr::io::build_destination(
            &DestinationKind::Objects(c.clone()),
            0,
            metrics,
            &secrets,
            parquet.then_some(&records),
        )
        .map_err(|e| format!("does not build: {e:?}\n{json}"))?;
        let sent = batches_for(&mut rng, reached, &c, Some(&plan));
        let batches: Vec<Vec<OwnedRecord>> = sent.iter().map(|b| b.iter().map(|s| s.row.clone()).collect()).collect();
        let start = SystemTime::now();
        let refused =
            destination::write(d.as_mut(), &batches, Duration::from_secs(120)).map_err(|e| format!("{e}\n{json}"))?;
        let objects: Vec<(String, Vec<u8>)> = read_all(reader(&draw.destination).as_ref(), &runtime)
            .into_iter()
            .filter(|(k, _)| !k.ends_with(".keep"))
            .collect();
        check_written(&c, &sent, &refused, &objects, (start, SystemTime::now())).map_err(|e| format!("{e}\n{json}"))
    });
}

/// What a live draw on `store` reaches: every option an emulator serves, signed in as it takes.
fn live_options(store: Store) -> Vec<&'static str> {
    let mut options: Vec<&str> = options()
        .filter(|o| {
            !o.starts_with("broken.")
                && !o.starts_with("store.")
                && !["mangled", "tls", "secret.workerLocal", "objects.several"].contains(o)
                && !["s3.", "gcs.", "azure.", "sftp."].iter().any(|p| o.starts_with(p))
        })
        .collect();
    match store {
        Store::S3 => options.extend(["store.s3", "s3.roleArn"]),
        Store::Gcs => options.push("store.gcs"),
        Store::Azure => options.extend(["store.azureBlob", "azure.accessKey", "azure.sasToken"]),
        Store::Sftp => {
            options.retain(|o| *o != "storageClass");
            options.extend(["store.sftp", "sftp.password", "sftp.privateKey"]);
        }
    }
    options
}

#[test]
fn live_s3_fuzz() {
    let Some(endpoint) = live::s3("live_s3_fuzz") else {
        return;
    };
    live_fuzz(
        Store::S3,
        &endpoint,
        |d| {
            let bucket = unique("smeltr-fuzz");
            ureq::put(format!("{endpoint}/{bucket}"))
                .send_empty()
                .expect("bucket made");
            d.insert("bucket".into(), json!(bucket));
        },
        |d| {
            Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_endpoint(&endpoint)
                    .with_region("us-east-1")
                    .with_access_key_id("test")
                    .with_secret_access_key("test")
                    .with_allow_http(true)
                    .with_bucket_name(d["bucket"].as_str().unwrap())
                    .build()
                    .unwrap(),
            )
        },
    );
}

#[test]
fn live_gcs_fuzz() {
    let Some(endpoint) = live::gcs("live_gcs_fuzz") else {
        return;
    };
    live_fuzz(
        Store::Gcs,
        &endpoint,
        |d| {
            let bucket = unique("smeltr-fuzz");
            ureq::post(format!("{endpoint}/storage/v1/b?project=smeltr"))
                .send_json(json!({ "name": bucket }))
                .expect("bucket made");
            d.insert("bucket".into(), json!(bucket));
        },
        |d| {
            Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_service_account_key(GCS_KEY)
                    .with_base_url(&endpoint)
                    .with_bucket_name(d["bucket"].as_str().unwrap())
                    .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
                    .build()
                    .unwrap(),
            )
        },
    );
}

#[test]
fn live_azure_fuzz() {
    let Some(endpoint) = live::azure("live_azure_fuzz") else {
        return;
    };
    live_fuzz(
        Store::Azure,
        &endpoint,
        |d| {
            let container = unique("smeltr-fuzz");
            ureq::put(format!("{endpoint}/{container}?restype=container&{AZURE_SAS}"))
                .send_empty()
                .expect("container made");
            d.insert("container".into(), json!(container));
        },
        |d| {
            Arc::new(
                object_store::azure::MicrosoftAzureBuilder::new()
                    .with_use_emulator(true)
                    .with_container_name(d["container"].as_str().unwrap())
                    .build()
                    .unwrap(),
            )
        },
    );
}

#[test]
fn live_sftp_fuzz() {
    let Some((host, port)) = live::sftp("live_sftp_fuzz") else {
        return;
    };
    let address = format!("{host}:{port}");
    // The upload directory, signed in with the password, pinned to the server's key.
    let upload = |directory: &str| smeltr::plan::topology::SftpStore {
        directory: directory.to_string(),
        access: smeltr::plan::topology::SftpAccess {
            host: host.clone(),
            port,
            username: "smeltr".to_string(),
            credential: SftpCredential::Password(smeltr::plan::topology::SecretRef::LeaderStored {
                name: "sftp-password".to_string(),
                version: 1,
            }),
            host_key: SFTP_HOST_KEY.trim().to_string(),
        },
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("sftp-password".to_string(), 1, SFTP_PASSWORD.to_string())]);
    let sftp = |directory: &str| smeltr::io::sftp::Sftp::new(&upload(directory), "", false, &secrets).unwrap();
    live_fuzz(
        Store::Sftp,
        &address,
        |d| {
            // A directory must be there, as a bucket must: a file put under it makes it.
            let name = unique("smeltr-fuzz");
            let runtime = smeltr::io::objects::runtime().unwrap();
            runtime
                .block_on(sftp("upload").put(&Path::from(format!("{name}/.keep")), b"".to_vec().into()))
                .expect("directory made");
            d.insert("directory".into(), json!(format!("upload/{name}")));
        },
        |d| Arc::new(sftp(d["directory"].as_str().unwrap())),
    );
}
