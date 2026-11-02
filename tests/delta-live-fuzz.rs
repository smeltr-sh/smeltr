//! Generated cases against the Delta destination on LocalStack's S3, as `just live-up` runs it.
//!
//! Each seed draws a record, the table's partitioning, the file settings and the batches, and one or two destinations
//! write them at once to a new table, found by its location or through a Unity Catalog workspace. The table is then
//! read back from the bucket, and must hold:
//!
//! - a log of consecutive versions, each readable;
//! - each delivered row in exactly one added file, under the row's own partition values, and no file added twice;
//! - every added file in the bucket, and every data file in the bucket added;
//! - each checkpoint what replaying the commits up to it gives;
//! - where the table has an identity column, a value of it in every row, on its grid, within the mark the row's
//!   commit leaves, and in no other row;
//! - where the table keeps in-commit timestamps, each commit opening with one later than the commit's before it.
//!
//! Point it at `S3_TEST_ENDPOINT`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::harness;
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value, json};
use smeltr::io::parquet::Columns;
use smeltr::io::{Destination, Dispatched, OwnedRecord};
use smeltr::plan::topology::{
    AccessKey, Compression, DeltaDestinationConfig, DestinationKind, HeldSecrets, S3Access, SecretRef,
    UnityCatalogTable,
};
use tokio_util::bytes::Bytes;

const BUCKET: &str = "delta-live";

/// Each seed makes a table and writes a few commits to it; this many run in about a minute against LocalStack.
const SEEDS: std::ops::Range<u64> = 0..24;

/// A multipart upload's part, as the destination cuts them: a file past it goes up in parts.
const PART_BYTES: u64 = 8 * 1024 * 1024;

/// Partition values that a path or a log escapes, or reads as null.
const ODD: [&str; 22] = [
    "",
    "a",
    "A",
    " ",
    "a b",
    "é",
    "日本語",
    "🦀",
    "/",
    "a/b",
    "=",
    "k=v",
    "%",
    "100%",
    "%2F",
    "+",
    ".",
    "..",
    "x?y#z",
    "\"q\"",
    "back\\slash",
    "tab\there",
];

const LONGS: [i64; 6] = [0, 1, -1, 42, i64::MIN, i64::MAX];
const DOUBLES: [f64; 6] = [0.0, 1.5, -2.25, 1e300, -1e-300, 3.0];
const TIMES: [&str; 3] = [
    "2024-01-01T00:00:00.000000Z",
    "1999-12-31T23:59:59.999999Z",
    "2030-06-15T12:30:45.123456Z",
];
const COMPRESSIONS: [(Compression, &str); 5] = [
    (Compression::None, "none"),
    (Compression::Snappy, "snappy"),
    (Compression::Gzip, "gzip"),
    (Compression::Lz4, "lz4"),
    (Compression::Zstd, "zstd"),
];

/// A field's declared type.
#[derive(Clone, Debug)]
enum Ty {
    Bool,
    Long,
    Double,
    Str,
    Time,
    Struct { fields: Vec<(String, Ty)>, open: bool },
    Array(Box<Ty>),
    Variant,
}

impl Ty {
    fn declared(&self) -> String {
        match self {
            Ty::Bool => "bool".into(),
            Ty::Long => "i64".into(),
            Ty::Double => "f64".into(),
            Ty::Str => "str".into(),
            Ty::Time => "datetime".into(),
            Ty::Struct { fields, open } => {
                let mut parts: Vec<String> = fields.iter().map(|(n, t)| format!("{n}: {}", t.declared())).collect();
                if *open {
                    parts.push("...".into());
                }
                format!("{{{}}}", parts.join(", "))
            }
            Ty::Array(element) => format!("array<{}>", element.declared()),
            Ty::Variant => "variant".into(),
        }
    }

    /// Its name in the coverage.
    fn name(&self) -> &'static str {
        match self {
            Ty::Bool => "bool",
            Ty::Long => "i64",
            Ty::Double => "f64",
            Ty::Str => "str",
            Ty::Time => "datetime",
            Ty::Struct { open: false, .. } => "struct",
            Ty::Struct { open: true, .. } => "open struct",
            Ty::Array(_) => "array",
            Ty::Variant => "variant",
        }
    }

    /// Whether the destination writes a partition value of it.
    fn partitionable(&self) -> bool {
        matches!(self, Ty::Bool | Ty::Long | Ty::Str | Ty::Time)
    }
}

fn ty(rng: &mut Rng, depth: usize, coverage: &mut Coverage) -> Ty {
    let t = match rng.below(if depth < 2 { 10 } else { 6 }) {
        0 => Ty::Bool,
        1 => Ty::Long,
        2 => Ty::Double,
        3 | 4 => Ty::Str,
        5 => Ty::Time,
        6 => Ty::Variant,
        7 | 8 => Ty::Struct {
            fields: (0..1 + rng.below(3))
                .map(|i| (format!("s{i}"), ty(rng, depth + 1, coverage)))
                .collect(),
            open: rng.chance(2),
        },
        _ => Ty::Array(Box::new(ty(rng, depth + 1, coverage))),
    };
    coverage.reach(format!("type {}", t.name()));
    t
}

/// Any JSON, for a variant or an open struct's other keys.
fn any(rng: &mut Rng, depth: usize) -> Value {
    match rng.below(if depth < 2 { 7 } else { 5 }) {
        0 => Value::Null,
        1 => json!(rng.chance(2)),
        2 => json!(*rng.pick(&LONGS)),
        3 => json!(*rng.pick(&DOUBLES)),
        4 => json!(text(rng)),
        5 => Value::Array((0..rng.below(3)).map(|_| any(rng, depth + 1)).collect()),
        _ => Value::Object(
            (0..rng.below(3))
                .map(|_| (rng.pick(&ODD).to_string(), any(rng, depth + 1)))
                .collect(),
        ),
    }
}

/// A string: one of `ODD`, or up to a few hundred characters drawn from ASCII and beyond.
fn text(rng: &mut Rng) -> String {
    if rng.chance(2) {
        return rng.pick(&ODD).to_string();
    }
    const CHARS: [char; 12] = ['a', 'Z', '0', ' ', '"', '\\', '/', '\n', '\u{1}', 'é', '中', '😀'];
    (0..rng.below(300)).map(|_| *rng.pick(&CHARS)).collect()
}

/// `len` letters and digits that no codec shrinks much.
fn noise(rng: &mut Rng, len: usize) -> String {
    const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..len).map(|_| *rng.pick(ALNUM) as char).collect()
}

fn time(rng: &mut Rng) -> String {
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        1970 + rng.below(100),
        1 + rng.below(12),
        1 + rng.below(28),
        rng.below(24),
        rng.below(60),
        rng.below(60),
        rng.below(1_000_000)
    )
}

/// A value of `ty`, or null one time in six.
fn value(rng: &mut Rng, ty: &Ty) -> Value {
    if rng.chance(6) {
        return Value::Null;
    }
    match ty {
        Ty::Bool => json!(rng.chance(2)),
        Ty::Long => json!(if rng.chance(2) {
            *rng.pick(&LONGS)
        } else {
            rng.next_u64() as i64
        }),
        Ty::Double => json!(*rng.pick(&DOUBLES)),
        Ty::Str => json!(text(rng)),
        Ty::Time => json!(time(rng)),
        Ty::Struct { fields, open } => {
            let mut object = Map::new();
            for (name, t) in fields {
                if !rng.chance(5) {
                    object.insert(name.clone(), value(rng, t));
                }
            }
            if *open && rng.chance(2) {
                object.insert(rng.pick(&ODD).to_string(), any(rng, 1));
            }
            Value::Object(object)
        }
        Ty::Array(element) => Value::Array((0..rng.below(4)).map(|_| value(rng, element)).collect()),
        Ty::Variant => any(rng, 0),
    }
}

/// A partition column's value, from a few per type so rows share partitions.
fn partition_value(rng: &mut Rng, ty: &Ty) -> Value {
    if rng.chance(8) {
        return Value::Null;
    }
    match ty {
        Ty::Bool => json!(rng.chance(2)),
        Ty::Long => json!(*rng.pick(&LONGS)),
        Ty::Str => json!(rng.pick(&ODD)),
        Ty::Time => json!(rng.pick(&TIMES)),
        other => unreachable!("{other:?} is no partition column"),
    }
}

/// The partition value the log holds for a row's `value`: null for a missing, null or empty one.
fn expected_partition(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// What a writer does after handing over a batch.
#[derive(Clone, Copy, Debug)]
enum Then {
    Nothing,
    Flush,
    /// Waits out the file interval, so the commit that ends it is the writer's own.
    WaitInterval,
}

struct Batch {
    id: u64,
    rows: Vec<String>,
    then: Then,
}

struct Case {
    fields: Vec<(String, Ty)>,
    partition_by: Vec<String>,
    unity: bool,
    compression: Compression,
    max_object_bytes: usize,
    buffer_bytes: usize,
    file_interval: Duration,
    /// The `delta.checkpointInterval` another writer sets right after the table is made, or the default's.
    checkpoint_interval: Option<u32>,
    /// The start and step of the identity column `seq` another writer adds before any row is written.
    identity: Option<(i64, i64)>,
    /// Whether another writer enables in-commit timestamps before any row is written.
    in_commit_timestamps: bool,
    writers: Vec<Vec<Batch>>,
    /// Each row's partition values by its id, in `partition_by`'s order.
    rows: BTreeMap<i64, Vec<Option<String>>>,
}

impl Case {
    /// The case without its rows, for a failure's message.
    fn summary(&self) -> String {
        let fields: Vec<String> = self
            .fields
            .iter()
            .map(|(n, t)| format!("{n}: {}", t.declared()))
            .collect();
        let writers: Vec<Vec<String>> = (self.writers.iter())
            .map(|w| {
                w.iter()
                    .map(|b| format!("{}x{} {:?}", b.id, b.rows.len(), b.then))
                    .collect()
            })
            .collect();
        format!(
            "fields [{}], partitionBy {:?}, unity {}, {:?}, maxObjectBytes {}, bufferBytes {}, fileInterval {:?}, \
             checkpointInterval {:?}, identity {:?}, inCommitTimestamps {}, batches {writers:?}",
            fields.join("; "),
            self.partition_by,
            self.unity,
            self.compression,
            self.max_object_bytes,
            self.buffer_bytes,
            self.file_interval,
            self.checkpoint_interval,
            self.identity,
            self.in_commit_timestamps
        )
    }
}

/// The case of `seed`. Every eighth is big, since each costs a dozen megabytes, and their buffers alternate.
fn case(seed: u64, rng: &mut Rng, coverage: &mut Coverage) -> Case {
    // A big case writes files past a multipart part; a wide one rolls files at their size.
    let size = if seed.is_multiple_of(8) {
        "big"
    } else if rng.chance(3) {
        "wide"
    } else {
        "narrow"
    };
    coverage.reach(format!("rows {size}"));
    let mut fields = Vec::new();
    for i in 0..1 + rng.below(5) {
        let t = if i == 0 && !rng.chance(3) {
            coverage.reach("type str");
            Ty::Str
        } else {
            ty(rng, 0, coverage)
        };
        fields.push((format!("f{i}"), t));
    }
    let mut partition_by = Vec::new();
    if size != "big" {
        for (name, t) in &fields {
            if t.partitionable() && partition_by.len() < 2 && !rng.chance(3) {
                coverage.reach(format!("partition by {}", t.name()));
                partition_by.push(name.clone());
            }
        }
    }
    coverage.reach(format!("partition columns {}", partition_by.len()));
    let pad = match size {
        "big" => {
            fields.push(("pad".into(), Ty::Str));
            Some(6000..6001)
        }
        "wide" => {
            fields.push(("pad".into(), Ty::Str));
            Some(1000..2000)
        }
        _ => None,
    };
    let unity = rng.chance(3);
    coverage.reach(if unity { "unity catalog" } else { "location" });
    let (compression, codec) = if size == "big" {
        COMPRESSIONS[0]
    } else {
        *rng.pick(&COMPRESSIONS)
    };
    coverage.reach(format!("compression {codec}"));
    let (max_object_bytes, buffer_bytes) = if size == "big" {
        let buffer = if (seed / 8).is_multiple_of(2) {
            4 << 20
        } else {
            64 << 20
        };
        coverage.reach(format!("big buffer {}MiB", buffer >> 20));
        (64 << 20, buffer)
    } else {
        (
            *rng.pick(&[64 << 10, 80 << 10, 128 << 10, 1 << 20]),
            (4 << 20) + rng.below(4 << 20),
        )
    };
    let file_interval = Duration::from_secs(5 + rng.below(3) as u64);
    let checkpoint_interval = (!rng.chance(2)).then(|| 2 + rng.below(3) as u32);
    coverage.reach(match checkpoint_interval {
        Some(_) => "checkpoint interval set",
        None => "checkpoint interval default",
    });
    let writer_count = if size == "big" { 1 } else { 1 + rng.below(2) };
    coverage.reach(format!("writers {writer_count}"));
    let wait_once = rng.chance(6);
    let mut rows = BTreeMap::new();
    let mut writers = Vec::new();
    let mut batch_id = 0;
    for w in 0..writer_count {
        let mut batches = Vec::new();
        let count = if size == "big" {
            1 + rng.below(4)
        } else {
            1 + rng.below(6)
        };
        for b in 0..count {
            batch_id += 1;
            let row_count = if size == "big" { 2000 / count } else { 1 + rng.below(60) };
            let mut texts = Vec::new();
            for _ in 0..row_count {
                let id = rows.len() as i64 + 1;
                let mut object = Map::new();
                object.insert("id".into(), json!(id));
                for (name, t) in &fields {
                    if name == "pad" {
                        let range = pad.clone().unwrap();
                        let len = range.start + rng.below(range.end - range.start);
                        object.insert(name.clone(), json!(noise(rng, len)));
                        continue;
                    }
                    // A field left out reads as null.
                    if rng.chance(10) {
                        continue;
                    }
                    let v = if partition_by.contains(name) {
                        partition_value(rng, t)
                    } else {
                        value(rng, t)
                    };
                    object.insert(name.clone(), v);
                }
                rows.insert(
                    id,
                    (partition_by.iter())
                        .map(|p| expected_partition(object.get(p)))
                        .collect(),
                );
                texts.push(Value::Object(object).to_string());
            }
            let then = if wait_once && w == 0 && b == 0 {
                Then::WaitInterval
            } else if size != "big" && rng.chance(3) {
                Then::Flush
            } else {
                Then::Nothing
            };
            coverage.reach(format!("then {then:?}"));
            batches.push(Batch {
                id: batch_id,
                rows: texts,
                then,
            });
        }
        writers.push(batches);
    }
    let identity = rng
        .chance(2)
        .then(|| (*rng.pick(&[1, -100, 1 << 40]), *rng.pick(&[1, 3, -2])));
    coverage.reach(match identity {
        Some(_) if writer_count == 2 => "identity two writers",
        Some(_) => "identity one writer",
        None => "no identity column",
    });
    let in_commit_timestamps = rng.chance(2);
    coverage.reach(if in_commit_timestamps {
        "in-commit timestamps"
    } else {
        "no in-commit timestamps"
    });
    Case {
        fields,
        partition_by,
        unity,
        compression,
        max_object_bytes,
        buffer_bytes,
        file_interval,
        checkpoint_interval,
        identity,
        in_commit_timestamps,
        writers,
        rows,
    }
}

/// What the run saw, for the coverage.
#[derive(Default)]
struct Seen {
    checkpoints: usize,
    multipart: bool,
    rolled: bool,
}

#[test]
fn live_generated_tables_hold_every_row_once_in_a_consistent_log() {
    let Some(s3) = live::s3("live_generated_tables_hold_every_row_once_in_a_consistent_log") else {
        return;
    };
    // LocalStack answers a bucket made already as made.
    ureq::put(format!("{s3}/{BUCKET}")).send_empty().expect("bucket made");
    let mut coverage = Coverage::new(
        [
            "type bool",
            "type i64",
            "type f64",
            "type str",
            "type datetime",
            "type struct",
            "type open struct",
            "type array",
            "type variant",
            "partition by bool",
            "partition by i64",
            "partition by str",
            "partition by datetime",
            "partition columns 0",
            "partition columns 1",
            "partition columns 2",
            "rows big",
            "rows wide",
            "rows narrow",
            "big buffer 4MiB",
            "big buffer 64MiB",
            "unity catalog",
            "location",
            "compression none",
            "compression snappy",
            "compression gzip",
            "compression lz4",
            "compression zstd",
            "checkpoint interval set",
            "checkpoint interval default",
            "identity one writer",
            "identity two writers",
            "no identity column",
            "in-commit timestamps",
            "no in-commit timestamps",
            "writers 1",
            "writers 2",
            "then Nothing",
            "then Flush",
            "then WaitInterval",
            "checkpoint written",
            "multipart upload",
            "file rolled",
        ]
        .map(String::from),
    );
    fuzz::run(SEEDS, |seed| {
        let mut rng = Rng::seeded(seed);
        let case = case(seed, &mut rng, &mut coverage);
        let seen =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&s3, seed, &case))).map_err(|panic| {
                let why = (panic.downcast_ref::<String>().cloned())
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                format!("{why}\n  case: {}", case.summary())
            })?;
        if seen.checkpoints > 0 {
            coverage.reach("checkpoint written");
        }
        if seen.multipart {
            coverage.reach("multipart upload");
        }
        if seen.rolled {
            coverage.reach("file rolled");
        }
        Ok(())
    });
    coverage.assert_complete();
}

fn stored(name: &str) -> SecretRef {
    SecretRef::LeaderStored {
        name: name.to_string(),
        version: 1,
    }
}

/// Writes `case` to a new table and checks what the bucket then holds.
fn run(s3: &str, seed: u64, case: &Case) -> Seen {
    let root = format!(
        "fuzz{seed}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let declarations: String = (case.fields.iter())
        .map(|(n, t)| format!("{n}: {}; ", t.declared()))
        .collect();
    let plan = harness::compile_ok(vec![harness::code(
        0,
        &format!("id: i64; {declarations}Object.assign($, json(_raw));"),
    )]);
    let record = harness::record(&plan);
    let config = DeltaDestinationConfig {
        location: if case.unity {
            String::new()
        } else {
            format!("s3://{BUCKET}/{root}")
        },
        unity_catalog: case.unity.then(|| UnityCatalogTable {
            workspace: workspace(format!("s3://{BUCKET}/{root}")),
            table: "catalog.web.access".to_string(),
            client_id: "app".to_string(),
            client_secret: stored("client-secret"),
        }),
        partition_by: case.partition_by.clone(),
        storage: S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3.to_string()),
            auth_endpoint: None,
            access_key: (!case.unity).then(|| AccessKey {
                id: stored("key-id"),
                secret: stored("key"),
            }),
            role: None,
        },
        compression: case.compression,
        tls: None,
        in_flight: 2,
        max_object_bytes: case.max_object_bytes,
        buffer_bytes: case.buffer_bytes,
        stall_after: Duration::from_secs(30),
        file_interval: case.file_interval,
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
        ("client-secret".to_string(), 1, "s3cr3t".to_string()),
    ]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    let records = smeltr::plan::topology::Records::from([(None, record)]);
    let build = || {
        smeltr::io::build_destination(
            &DestinationKind::Delta(config.clone()),
            0,
            "d",
            metrics.clone(),
            &secrets,
            Some(&records),
        )
        .expect("built")
    };
    let writers: Vec<Vec<(u64, Vec<OwnedRecord>, Then)>> = (case.writers.iter())
        .map(|batches| {
            (batches.iter())
                .map(|b| {
                    let texts: Vec<&str> = b.rows.iter().map(String::as_str).collect();
                    let chunk = harness::cut(&plan, &texts);
                    let rows = (0..texts.len() as u32)
                        .map(|row| OwnedRecord {
                            payload: smeltr::plan::config::Payload::Record,
                            columns: Some(Columns {
                                chunk: chunk.clone(),
                                row,
                            }),
                            ..OwnedRecord::default()
                        })
                        .collect();
                    (b.id, rows, b.then)
                })
                .collect()
        })
        .collect();
    // Setting the interval needs the table: the first writer makes it, and finds the change on its first commit.
    // An identity column and in-commit timestamps are there before any writer opens the table, as they are for a
    // table another engine made. Otherwise the writers race to make it.
    let before = case.identity.is_some() || case.in_commit_timestamps;
    let mut first = match (before, case.checkpoint_interval) {
        (true, interval) => {
            drop(build());
            amend(s3, &root, interval, case.identity, case.in_commit_timestamps);
            None
        }
        (false, Some(interval)) => {
            let d = build();
            amend(s3, &root, Some(interval), None, false);
            Some(d)
        }
        (false, None) => None,
    };
    std::thread::scope(|scope| {
        for batches in writers {
            let made = first.take();
            let build = &build;
            let interval = case.file_interval;
            scope.spawn(move || {
                let mut d = made.unwrap_or_else(build);
                drive(d.as_mut(), batches, interval);
            });
        }
    });
    verify(s3, &root, case)
}

/// Sets the table's `delta.checkpointInterval` in commit 1, adds the identity column `seq` of `identity`'s start and
/// step, and enables in-commit timestamps where `timestamps` says, as another writer would.
fn amend(s3: &str, root: &str, interval: Option<u32>, identity: Option<(i64, i64)>, timestamps: bool) {
    let created = commit(s3, root, 0).expect("commit 0");
    let mut metadata = created
        .iter()
        .find_map(|a| a.get("metaData"))
        .expect("metaData")
        .clone();
    let mut actions = vec![json!({"commitInfo": {"timestamp": 0, "operation": "SET TBLPROPERTIES",
                                                  "engineInfo": "fuzz"}})];
    if let Some(interval) = interval {
        metadata["configuration"]["delta.checkpointInterval"] = json!(interval.to_string());
    }
    let mut features = Vec::new();
    if timestamps {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        actions[0]["commitInfo"]["inCommitTimestamp"] = json!(now);
        metadata["configuration"]["delta.enableInCommitTimestamps"] = json!("true");
        features.push("inCommitTimestamp");
    }
    if let Some((start, step)) = identity {
        let mut schema: Value = serde_json::from_str(metadata["schemaString"].as_str().unwrap()).unwrap();
        schema["fields"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "seq", "type": "long", "nullable": false,
            "metadata": {"delta.identity.start": start, "delta.identity.step": step,
                         "delta.identity.allowExplicitInsert": false}}));
        metadata["schemaString"] = json!(schema.to_string());
        features.push("identityColumns");
    }
    if !features.is_empty() {
        actions.push(json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 7, "writerFeatures": features}}));
    }
    actions.push(json!({"metaData": metadata}));
    let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
    ureq::put(object_url(s3, &format!("{root}/_delta_log/{:020}.json", 1)))
        .header("If-None-Match", "*")
        .send(body)
        .expect("commit 1");
}

/// Hands `batches` to `d` as egress would, and waits until every one has landed.
fn drive(d: &mut dyn Destination, batches: Vec<(u64, Vec<OwnedRecord>, Then)>, interval: Duration) {
    let mut waiting = HashSet::new();
    let take = |d: &mut dyn Destination, waiting: &mut HashSet<u64>| {
        for c in d.poll_completions().expect("landed") {
            assert!(c.rejections.is_empty(), "{:?}", c.rejections);
            assert!(waiting.remove(&c.batch), "batch {} completed unasked", c.batch);
        }
    };
    for (batch, rows, then) in batches {
        let (dispatched, refused) = d.deliver(batch, Cow::Owned(rows)).expect("delivered");
        assert!(refused.is_empty(), "{refused:?}");
        match dispatched {
            Dispatched::InFlight => {
                waiting.insert(batch);
            }
            Dispatched::Complete => {}
            Dispatched::NoRoom { .. } => panic!("batch {batch} found no room"),
        }
        match then {
            Then::Nothing => {}
            Then::Flush => d.flush().expect("flushed"),
            Then::WaitInterval => std::thread::sleep(interval + Duration::from_millis(500)),
        }
        take(d, &mut waiting);
    }
    d.flush().expect("flushed");
    let deadline = Instant::now() + Duration::from_secs(120);
    while !waiting.is_empty() {
        take(d, &mut waiting);
        assert!(Instant::now() < deadline, "batches {waiting:?} never landed");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The table's state at one version.
#[derive(Clone, Debug, Default, PartialEq)]
struct State {
    protocol: (i64, i64),
    metadata_id: String,
    schema: String,
    partition_columns: Vec<String>,
    txns: BTreeMap<String, i64>,
    files: BTreeSet<String>,
}

/// Reads the table at `root` back from the bucket and checks it against `case`.
fn verify(s3: &str, root: &str, case: &Case) -> Seen {
    let mut seen = Seen::default();
    let keys = keys(s3, &format!("{root}/"));
    let log_prefix = format!("{root}/_delta_log/");
    let mut versions = Vec::new();
    let mut checkpoints = Vec::new();
    for name in keys.iter().filter_map(|k| k.strip_prefix(&log_prefix)) {
        if let Some(v) = name.strip_suffix(".json").filter(|v| v.len() == 20) {
            versions.push(v.parse::<i64>().unwrap_or_else(|_| panic!("{name} is no version")));
        } else if let Some(v) = name.strip_suffix(".checkpoint.parquet") {
            checkpoints.push(v.parse::<i64>().unwrap_or_else(|_| panic!("{name} is no checkpoint")));
        }
    }
    versions.sort();
    assert_eq!(
        versions,
        (0..versions.len() as i64).collect::<Vec<_>>(),
        "the log's versions are not consecutive from 0"
    );

    // Replay, keeping the state at each checkpoint's version.
    let mut state = State::default();
    let mut at = BTreeMap::new();
    let mut ever = HashSet::new();
    let mut adds: BTreeMap<String, (i64, Value)> = BTreeMap::new();
    // `seq`'s high-water mark as each version leaves it.
    let mut marks: BTreeMap<i64, Option<i64>> = BTreeMap::new();
    let mut timestamp = None;
    for &version in &versions {
        let actions = commit(s3, root, version).unwrap_or_else(|e| panic!("version {version} unreadable: {e}"));
        assert!(!actions.is_empty(), "version {version} is empty");
        if case.in_commit_timestamps && version > 0 {
            let t = actions[0]["commitInfo"]["inCommitTimestamp"].as_i64();
            let t = t.unwrap_or_else(|| panic!("version {version} opens with no inCommitTimestamp: {actions:?}"));
            assert!(
                timestamp < Some(t),
                "version {version}'s inCommitTimestamp {t} is not after {timestamp:?}"
            );
            timestamp = Some(t);
        }
        for action in actions {
            if let Some(p) = action.get("protocol") {
                state.protocol = (
                    p["minReaderVersion"].as_i64().unwrap(),
                    p["minWriterVersion"].as_i64().unwrap(),
                );
            }
            if let Some(m) = action.get("metaData") {
                state.metadata_id = m["id"].as_str().unwrap().to_string();
                state.schema = m["schemaString"].as_str().unwrap().to_string();
                state.partition_columns = serde_json::from_value(m["partitionColumns"].clone()).unwrap();
            }
            if let Some(t) = action.get("txn") {
                (state.txns).insert(t["appId"].as_str().unwrap().to_string(), t["version"].as_i64().unwrap());
            }
            if let Some(add) = action.get("add") {
                let path = add["path"].as_str().unwrap().to_string();
                assert!(
                    ever.insert(path.clone()),
                    "{path} added twice, again in version {version}"
                );
                state.files.insert(path.clone());
                adds.insert(path, (version, add.clone()));
            }
            if let Some(remove) = action.get("remove") {
                let path = remove["path"].as_str().unwrap();
                state.files.remove(path);
                adds.remove(path);
            }
        }
        let schema: Value = serde_json::from_str(&state.schema).unwrap();
        let seq = (schema["fields"].as_array().unwrap().iter()).find(|f| f["name"] == "seq");
        marks.insert(
            version,
            seq.and_then(|f| f["metadata"]["delta.identity.highWaterMark"].as_i64()),
        );
        if checkpoints.contains(&version) {
            at.insert(version, state.clone());
        }
    }
    assert_eq!(state.partition_columns, case.partition_by);

    for &version in &checkpoints {
        let replayed = at
            .get(&version)
            .unwrap_or_else(|| panic!("checkpoint {version} is past the log's last version"));
        let key = format!("{log_prefix}{version:020}.checkpoint.parquet");
        let read = checkpoint(&get(s3, &key));
        assert_eq!(
            &read, replayed,
            "checkpoint {version} differs from replaying the commits"
        );
        seen.checkpoints += 1;
    }
    if !checkpoints.is_empty() {
        let last: Value = serde_json::from_slice(&get(s3, &format!("{log_prefix}_last_checkpoint"))).unwrap();
        let named = last["version"].as_i64().unwrap();
        assert!(
            checkpoints.contains(&named),
            "_last_checkpoint names {named}, of {checkpoints:?}"
        );
    }

    // Each delivered row is in exactly one added file, under its own partition values.
    let mut found: BTreeMap<i64, usize> = BTreeMap::new();
    let mut per_commit_partition = HashSet::new();
    let mut sequence = HashSet::new();
    for (path, (version, add)) in &adds {
        let key = format!("{root}/{}", percent_decode(path));
        assert!(keys.contains(&key), "{path} is added and not in the bucket");
        let bytes = get(s3, &key);
        assert_eq!(add["size"].as_u64(), Some(bytes.len() as u64), "{path}'s size");
        if bytes.len() as u64 > PART_BYTES {
            seen.multipart = true;
        }
        let values = &add["partitionValues"];
        let mut count = 0;
        for batch in batches(bytes) {
            let ids = (batch.column_by_name("id"))
                .unwrap_or_else(|| panic!("{path} has no id column"))
                .as_primitive::<Int64Type>();
            for id in ids.iter() {
                let id = id.unwrap_or_else(|| panic!("{path} has a row with no id"));
                let expected = case
                    .rows
                    .get(&id)
                    .unwrap_or_else(|| panic!("{path} holds row {id}, never sent"));
                for (column, value) in case.partition_by.iter().zip(expected) {
                    assert_eq!(
                        values[column].as_str(),
                        value.as_deref(),
                        "{path} holds row {id}, whose {column} is {value:?}: {values}"
                    );
                }
                *found.entry(id).or_default() += 1;
                count += 1;
            }
            if let Some((start, step)) = case.identity {
                let seq = (batch.column_by_name("seq"))
                    .unwrap_or_else(|| panic!("{path} has no seq column"))
                    .as_primitive::<Int64Type>();
                let mark = marks[version].unwrap_or_else(|| panic!("version {version} adds seq values and no mark"));
                for v in seq.iter() {
                    let v = v.unwrap_or_else(|| panic!("{path} has a row with no seq"));
                    let k = (v as i128 - start as i128) / step as i128;
                    assert!(
                        k >= 0 && start as i128 + k * step as i128 == v as i128,
                        "{path}'s seq {v} is off its grid"
                    );
                    assert!(
                        (v as i128 - mark as i128) * (step.signum() as i128) <= 0,
                        "{path}'s seq {v} is past {mark}"
                    );
                    assert!(sequence.insert(v), "seq {v} is in two rows");
                }
            }
        }
        if let Some(stats) = add["stats"].as_str() {
            let stats: Value = serde_json::from_str(stats).unwrap();
            assert_eq!(stats["numRecords"].as_u64(), Some(count), "{path}'s numRecords");
        }
        // Two files of one partition in one commit: the first rolled at its size.
        let dir = path.rsplit_once('/').map_or("", |(d, _)| d);
        if !per_commit_partition.insert((*version, dir)) {
            seen.rolled = true;
        }
    }
    let twice: Vec<_> = found.iter().filter(|(_, n)| **n > 1).collect();
    assert!(twice.is_empty(), "rows in more than one file: {twice:?}");
    let missing: Vec<_> = case.rows.keys().filter(|id| !found.contains_key(id)).collect();
    assert!(missing.is_empty(), "rows delivered and in no added file: {missing:?}");

    // And the bucket holds no data file the log does not add.
    let added: BTreeSet<String> = adds.keys().map(|p| format!("{root}/{}", percent_decode(p))).collect();
    let data: BTreeSet<String> = keys.iter().filter(|k| !k.starts_with(&log_prefix)).cloned().collect();
    assert_eq!(data, added, "the bucket's data files and the log's adds differ");
    seen
}

/// A checkpoint's state, as the Parquet file holds it.
fn checkpoint(bytes: &[u8]) -> State {
    let mut state = State::default();
    for batch in batches(bytes.to_vec()) {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("the checkpoint has no {name}"))
                .as_struct()
                .clone()
        };
        let (add, remove, txn, metadata, protocol) = (
            column("add"),
            column("remove"),
            column("txn"),
            column("metaData"),
            column("protocol"),
        );
        let string = |s: &arrow_array::StructArray, field: &str, i: usize| {
            s.column_by_name(field).unwrap().as_string::<i32>().value(i).to_string()
        };
        for i in 0..batch.num_rows() {
            if add.is_valid(i) {
                state.files.insert(string(&add, "path", i));
            }
            if remove.is_valid(i) {
                // A tombstone: the file is not in the table.
                state.files.remove(&string(&remove, "path", i));
            }
            if txn.is_valid(i) {
                let version = txn
                    .column_by_name("version")
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .value(i);
                state.txns.insert(string(&txn, "appId", i), version);
            }
            if metadata.is_valid(i) {
                state.metadata_id = string(&metadata, "id", i);
                state.schema = string(&metadata, "schemaString", i);
                let columns = metadata
                    .column_by_name("partitionColumns")
                    .unwrap()
                    .as_list::<i32>()
                    .value(i);
                state.partition_columns = (columns.as_string::<i32>().iter())
                    .map(|c| c.unwrap().to_string())
                    .collect();
            }
            if protocol.is_valid(i) {
                let int = |field: &str| {
                    protocol
                        .column_by_name(field)
                        .unwrap()
                        .as_primitive::<Int32Type>()
                        .value(i) as i64
                };
                state.protocol = (int("minReaderVersion"), int("minWriterVersion"));
            }
        }
    }
    state
}

fn batches(bytes: Vec<u8>) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .expect("a Parquet file")
        .build()
        .unwrap()
        .map(|b| b.expect("a readable row group"))
        .collect()
}

/// `key`'s URL. Every byte past the unreserved is escaped: LocalStack reads a `+` in a path as a space.
fn object_url(s3: &str, key: &str) -> String {
    format!("{s3}/{BUCKET}/{}", escaped(key, b"/"))
}

/// `s` with every byte but the unreserved and `keep` percent-escaped.
fn escaped(s: &str, keep: &[u8]) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) || keep.contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn get(s3: &str, key: &str) -> Vec<u8> {
    ureq::get(object_url(s3, key))
        .call()
        .unwrap_or_else(|e| panic!("{key}: {e}"))
        .body_mut()
        .with_config()
        .limit(256 << 20)
        .read_to_vec()
        .unwrap()
}

/// Each JSON line of commit `version` of the table at `root`, or why it is not readable.
fn commit(s3: &str, root: &str, version: i64) -> Result<Vec<Value>, String> {
    let body =
        String::from_utf8(get(s3, &format!("{root}/_delta_log/{version:020}.json"))).map_err(|e| e.to_string())?;
    (body.lines())
        .map(|l| serde_json::from_str(l).map_err(|e| format!("{e}: {l}")))
        .collect()
}

/// The keys under `prefix` in the bucket, every page of them.
fn keys(s3: &str, prefix: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{s3}/{BUCKET}?list-type=2&prefix={prefix}");
        if let Some(t) = &token {
            url.push_str("&continuation-token=");
            url.push_str(&escaped(t, b""));
        }
        let listing = ureq::get(url)
            .call()
            .expect("listed")
            .body_mut()
            .read_to_string()
            .unwrap();
        for k in listing.split("<Key>").skip(1) {
            keys.insert(xml_text(&k[..k.find("</Key>").unwrap()]));
        }
        token = listing
            .split_once("<NextContinuationToken>")
            .map(|(_, rest)| xml_text(&rest[..rest.find("</NextContinuationToken>").unwrap()]));
        if token.is_none() {
            return keys;
        }
    }
}

fn xml_text(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// A Databricks workspace on 127.0.0.1 whose Unity Catalog places `catalog.web.access` at `location` and vends
/// LocalStack's keys for it, to every caller, until the test ends.
fn workspace(location: String) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let mut reader = BufReader::new(conn.unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let Some((name, value)) = header.trim_end().split_once(':') else {
                    break;
                };
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
            }
            reader.read_exact(&mut vec![0; length]).unwrap();
            let in_an_hour = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 3_600_000;
            let body = if line.starts_with("POST /oidc/v1/token") {
                json!({"access_token": "t", "expires_in": 3600})
            } else if line.starts_with("GET /api/2.1/unity-catalog/tables/catalog.web.access") {
                json!({
                    "table_id": "6f1c",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": location,
                })
            } else {
                json!({
                    "aws_temp_credentials": {"access_key_id": "test", "secret_access_key": "test", "session_token": "x"},
                    "expiration_time": in_an_hour as u64,
                })
            }
            .to_string();
            let mut conn = reader.into_inner();
            write!(
                conn,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    url
}
