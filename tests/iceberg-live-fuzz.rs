//! Generated cases against the Iceberg destination, through Apache Iceberg's own REST fixture keeping its tables in
//! LocalStack's S3, as `just live-up` runs them.
//!
//! Each seed draws a record, a partitioning, a namespace and table name, the file settings and the batches, and maybe
//! a table an earlier record made. One or two destinations write the batches at once, so their commits race at the
//! catalog. The table is then read back from the catalog and the bucket, and must hold:
//!
//! - every column of the record, and the partitioning drawn, or the earlier table's own;
//! - each delivered row in exactly one live data file, with the values it was delivered with;
//! - each data file's row count and partition values its rows';
//! - every live data file in the bucket, and every data file in the bucket live;
//! - a current snapshot whose summary counts every row.
//!
//! Point it at `ICEBERG_TEST_CATALOG` and `S3_TEST_ENDPOINT`. Under `SMELTR_LIVE_TESTS=required` a missing one fails
//! rather than skips.

use crate::harness;
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value, json};
use smeltr::io::iceberg::manifest::{Status, read_manifest, read_manifest_list};
use smeltr::io::iceberg::metadata::{Literal, NestedField, TableMetadata, Type};
use smeltr::io::iceberg::transform::{apply, partition_name, partition_term};
use smeltr::io::parquet::Columns;
use smeltr::io::{Destination, Dispatched, OwnedRecord};
use smeltr::ops::civil::{MICROS_PER_DAY, civil_from_days, days_from_civil};
use smeltr::plan::topology::{
    AccessKey, CatalogSignIn, Compression, DestinationKind, HeldSecrets, IcebergDestinationConfig, S3Access, SecretRef,
};
use tokio_util::bytes::Bytes;

/// The bucket the fixture's warehouse is in (`tests/live/compose.live.yaml`).
const BUCKET: &str = "iceberg-live";

/// Each seed makes a table and writes a few commits to it; this many run in under a minute against the fixture.
const SEEDS: std::ops::Range<u64> = 0..64;

/// A column a record may write: its name and its type, as the code step declares it.
const COLUMNS: &[(&str, &str)] = &[
    ("host", "str"),
    ("n", "i64"),
    ("ratio", "f64"),
    ("ok", "bool"),
    ("ts", "datetime"),
    ("ctx", "{region: str, zone: i64}"),
    ("tags", "array<str>"),
];

const COMPRESSIONS: [(Compression, &str); 5] = [
    (Compression::None, "none"),
    (Compression::Snappy, "snappy"),
    (Compression::Gzip, "gzip"),
    (Compression::Lz4, "lz4"),
    (Compression::Zstd, "zstd"),
];

/// Table and namespace names that a path or a URL escapes.
const NAMES: [&str; 5] = ["web", "Web Logs", "ünï", "a%b", "x+y"];

/// The leaves a row holds values in, by dotted path, and their kinds. A list's are never read back.
fn leaves(columns: &[(&str, &str)]) -> Vec<(String, &'static str)> {
    let mut leaves = vec![("id".to_string(), "i64")];
    for (name, kind) in columns {
        match *kind {
            "str" => leaves.push((name.to_string(), "str")),
            "i64" => leaves.push((name.to_string(), "i64")),
            "f64" => leaves.push((name.to_string(), "f64")),
            "bool" => leaves.push((name.to_string(), "bool")),
            "datetime" => leaves.push((name.to_string(), "datetime")),
            "array<str>" => {}
            _ => {
                leaves.push((format!("{name}.region"), "str"));
                leaves.push((format!("{name}.zone"), "i64"));
            }
        }
    }
    leaves
}

/// One value of a row, as it was delivered and as it reads back.
#[derive(Clone, Debug, PartialEq)]
enum Val {
    Null,
    Bool(bool),
    Long(i64),
    /// Its bits, so values compare exactly.
    Double(u64),
    Micros(i64),
    Str(String),
}

impl Val {
    /// The value as a partition source holds it: a time is `timestamptz`.
    fn literal(&self) -> Option<Literal> {
        Some(match self {
            Val::Null => return None,
            Val::Bool(b) => Literal::Boolean(*b),
            Val::Long(v) => Literal::Long(*v),
            Val::Double(bits) => Literal::Double(f64::from_bits(*bits)),
            Val::Micros(us) => Literal::Timestamptz(*us),
            Val::Str(s) => Literal::String(s.clone()),
        })
    }

    fn json(&self) -> Value {
        match self {
            Val::Null => Value::Null,
            Val::Bool(b) => json!(b),
            Val::Long(v) => json!(v),
            Val::Double(bits) => json!(f64::from_bits(*bits)),
            Val::Micros(us) => json!(iso(*us)),
            Val::Str(s) => json!(s),
        }
    }
}

/// `us` as the ISO 8601 text a row carries it in.
fn iso(us: i64) -> String {
    let days = us.div_euclid(MICROS_PER_DAY);
    let rest = us.rem_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:06}Z",
        rest / 3_600_000_000,
        rest / 60_000_000 % 60,
        rest / 1_000_000 % 60,
        rest % 1_000_000
    )
}

/// Strings that break paths, URLs and escapes, and a few that repeat so rows share partitions.
fn odd_string(rng: &mut Rng) -> String {
    match rng.below(12) {
        0 => String::new(),
        1 => "/".into(),
        2 => "a=b/c".into(),
        3 => "%2F%".into(),
        4 => "a b+c?d#e".into(),
        5 => "ünï日本🦀".into(),
        6 => "\u{1}\n\t\"\\".into(),
        7 => "x".repeat(100 + rng.below(200)),
        8 => "null".into(),
        _ => format!("v{}", rng.below(4)),
    }
}

/// A value of `kind`, or null one time in eight.
fn value(rng: &mut Rng, kind: &str) -> Val {
    if rng.chance(8) {
        return Val::Null;
    }
    match kind {
        "str" => Val::Str(odd_string(rng)),
        "i64" => {
            let random = rng.next_u64() as i64;
            Val::Long(*rng.pick(&[0, -1, 1, i64::MIN, i64::MAX, 42, random]))
        }
        "f64" => Val::Double(rng.pick(&[0.0f64, 1.5, -2.25, 1e300, -1e-300, 0.1]).to_bits()),
        "bool" => Val::Bool(rng.chance(2)),
        // Either side of 1970, so the time transforms floor rather than truncate.
        _ => {
            let day = days_from_civil(
                1900 + rng.below(200) as i64,
                1 + rng.below(12) as u32,
                1 + rng.below(28) as u32,
            );
            Val::Micros(day * MICROS_PER_DAY + rng.below(86_400) as i64 * 1_000_000 + rng.below(1_000_000) as i64)
        }
    }
}

/// A partition term over `columns` the record can fill.
fn term(rng: &mut Rng, columns: &[(&str, &str)]) -> Option<String> {
    // A time column takes four transforms, so it is drawn more often.
    let times: Vec<_> = columns.iter().filter(|(_, k)| *k == "datetime").collect();
    let (name, kind) = match times.first() {
        Some(time) if rng.chance(2) => **time,
        _ => *rng.pick(columns),
    };
    Some(match (kind, rng.below(4)) {
        ("datetime", 0) => format!("year({name})"),
        ("datetime", 1) => format!("month({name})"),
        ("datetime", 2) => format!("day({name})"),
        ("datetime", _) => format!("hour({name})"),
        ("str" | "i64", 0) => format!("bucket({}, {name})", 1 + rng.below(16)),
        ("str" | "i64", 1) => format!("truncate({}, {name})", 1 + rng.below(8)),
        ("str" | "i64" | "bool" | "f64", _) => name.to_string(),
        ("array<str>", _) => return None,
        (_, 0) => format!("bucket(4, {name}.zone)"),
        _ => format!("{name}.region"),
    })
}

struct Batch {
    id: u64,
    /// Each row's values by leaf, `id` among them.
    rows: Vec<BTreeMap<String, Val>>,
    flush: bool,
}

struct Case {
    columns: Vec<(&'static str, &'static str)>,
    partition_by: Vec<String>,
    namespace: Vec<String>,
    table: String,
    /// The columns of an earlier record that made the table, and the batch it wrote.
    earlier: Option<(Vec<(&'static str, &'static str)>, Batch)>,
    compression: Compression,
    in_flight: usize,
    max_object_bytes: usize,
    buffer_bytes: usize,
    writers: Vec<Vec<Batch>>,
}

impl Case {
    /// The case without its rows, for a failure's message.
    fn summary(&self) -> String {
        let writers: Vec<Vec<String>> = (self.writers.iter())
            .map(|w| {
                w.iter()
                    .map(|b| format!("{}x{} flush {}", b.id, b.rows.len(), b.flush))
                    .collect()
            })
            .collect();
        format!(
            "columns {:?}, partitionBy {:?}, namespace {:?}, table {:?}, earlier {:?}, {:?}, inFlight {}, \
             maxObjectBytes {}, bufferBytes {}, batches {writers:?}",
            self.columns,
            self.partition_by,
            self.namespace,
            self.table,
            self.earlier.as_ref().map(|(c, _)| c),
            self.compression,
            self.in_flight,
            self.max_object_bytes,
            self.buffer_bytes
        )
    }
}

fn case(rng: &mut Rng, coverage: &mut Coverage) -> Case {
    let mut pool = COLUMNS.to_vec();
    let mut columns = Vec::new();
    for _ in 0..1 + rng.below(5) {
        let at = rng.below(pool.len());
        columns.push(pool.remove(at));
    }
    for (name, _) in &columns {
        coverage.reach(format!("column {name}"));
    }
    let leaves = leaves(&columns);

    let earlier = rng.chance(3).then(|| {
        let keep_all = rng.chance(2);
        let earlier: Vec<_> = columns.iter().copied().filter(|_| keep_all || rng.chance(2)).collect();
        coverage.reach(if earlier.len() < columns.len() {
            "existing table evolved"
        } else {
            "existing table"
        });
        earlier
    });
    if earlier.is_none() {
        coverage.reach("new table");
    }
    let mut partition_by = Vec::new();
    let mut names = Vec::new();
    for _ in 0..rng.below(4) {
        let Some(term) = term(rng, &columns) else {
            continue;
        };
        let (transform, path) = partition_term(&term).unwrap();
        // Two terms naming one partition field are refused at save.
        let name = partition_name(&transform, &path);
        if names.contains(&name) {
            continue;
        }
        names.push(name);
        // An existing table keeps its own partitioning.
        if earlier.is_none() {
            coverage.reach(format!(
                "partition {}",
                transform.to_string().split('[').next().unwrap()
            ));
            if path.len() > 1 {
                coverage.reach("partition nested");
            }
        }
        partition_by.push(term);
    }
    let namespace: Vec<String> = (0..1 + rng.below(2)).map(|_| rng.pick(&NAMES).to_string()).collect();
    if namespace.len() > 1 {
        coverage.reach("namespace nested");
    }
    let table = rng.pick(&NAMES).to_string();
    coverage.reach(format!("table {table}"));
    let (compression, codec) = *rng.pick(&COMPRESSIONS);
    coverage.reach(format!("compression {codec}"));
    let in_flight = 1 + rng.below(4);
    coverage.reach(if in_flight == 1 {
        "inFlight 1"
    } else {
        "inFlight several"
    });
    let max_object_bytes = *rng.pick(&[64 << 10, 100 << 10, 1 << 20, 64 << 20]);

    let wide = rng.chance(3);
    if wide {
        coverage.reach("rows wide");
    }
    let mut next_id = 0i64;
    let mut batch_id = 0;
    let mut row = |rng: &mut Rng, leaves: &[(String, &str)]| {
        next_id += 1;
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), Val::Long(next_id));
        for (leaf, kind) in leaves.iter().skip(1) {
            let v = if wide && *kind == "str" && !rng.chance(4) {
                Val::Str(
                    (0..2000 + rng.below(4000))
                        .map(|_| *rng.pick(&['a', 'Z', '0', 'é']))
                        .collect(),
                )
            } else {
                value(rng, kind)
            };
            row.insert(leaf.clone(), v);
        }
        row
    };
    let earlier = earlier.map(|earlier| {
        batch_id += 1;
        let rows = (0..1 + rng.below(20))
            .map(|_| row(rng, &self::leaves(&earlier)))
            .collect();
        (
            earlier,
            Batch {
                id: batch_id,
                rows,
                flush: false,
            },
        )
    });
    let writer_count = 1 + rng.below(2);
    coverage.reach(format!("writers {writer_count}"));
    let writers = (0..writer_count)
        .map(|_| {
            let count = 1 + rng.below(4);
            if count > 1 {
                coverage.reach("batches several");
            }
            (0..count)
                .map(|_| {
                    batch_id += 1;
                    let flush = rng.chance(3);
                    if flush {
                        coverage.reach("flush between");
                    }
                    Batch {
                        id: batch_id,
                        rows: (0..1 + rng.below(40)).map(|_| row(rng, &leaves)).collect(),
                        flush,
                    }
                })
                .collect()
        })
        .collect();
    Case {
        columns,
        partition_by,
        namespace,
        table,
        earlier,
        compression,
        in_flight,
        max_object_bytes,
        buffer_bytes: (4 << 20) + rng.below(4 << 20),
        writers,
    }
}

/// What the run saw, for the coverage.
#[derive(Default)]
struct Seen {
    rolled: bool,
    /// A partition too long for a key, its files under its digest.
    shortened: bool,
    null_partition: bool,
}

#[test]
fn live_generated_tables_hold_every_row_once() {
    let name = "live_generated_tables_hold_every_row_once";
    let Some(catalog) = live::iceberg(name) else {
        return;
    };
    let Some(s3) = live::s3(name) else {
        return;
    };
    // The fixture keeps its warehouse in a bucket it does not make. LocalStack answers a bucket made already as made.
    ureq::put(format!("{s3}/{BUCKET}")).send_empty().expect("bucket made");
    let mut expected: Vec<String> = COLUMNS.iter().map(|(n, _)| format!("column {n}")).collect();
    expected.extend(NAMES.iter().map(|n| format!("table {n}")));
    expected.extend(COMPRESSIONS.iter().map(|(_, c)| format!("compression {c}")));
    expected.extend(
        [
            "partition identity",
            "partition year",
            "partition month",
            "partition day",
            "partition hour",
            "partition bucket",
            "partition truncate",
            "partition nested",
            "partition null",
            "partition path shortened",
            "new table",
            "existing table",
            "existing table evolved",
            "namespace nested",
            "inFlight 1",
            "inFlight several",
            "rows wide",
            "file rolled",
            "writers 1",
            "writers 2",
            "batches several",
            "flush between",
        ]
        .map(String::from),
    );
    let mut coverage = Coverage::new(expected);
    fuzz::run(SEEDS, |seed| {
        let mut rng = Rng::seeded(seed);
        let case = case(&mut rng, &mut coverage);
        let seen = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&catalog, &s3, seed, &case))).map_err(
            |panic| {
                let why = (panic.downcast_ref::<String>().cloned())
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                format!("{why}\n  case: {}", case.summary())
            },
        )?;
        if seen.rolled {
            coverage.reach("file rolled");
        }
        if seen.shortened {
            coverage.reach("partition path shortened");
        }
        if seen.null_partition {
            coverage.reach("partition null");
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

/// `levels` as the REST protocol puts a namespace in a path: joined by the unit separator, and escaped.
fn namespace_path(levels: &[String]) -> String {
    escaped(&levels.join("\u{1f}"), b"")
}

/// Writes `case` to a new table and checks what the catalog and the bucket then hold.
fn run(catalog: &str, s3: &str, seed: u64, case: &Case) -> Seen {
    // A namespace no earlier run used, its levels made one by one.
    let mut namespace = vec![format!(
        "fuzz{seed}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )];
    namespace.extend(case.namespace.iter().cloned());
    for depth in 1..=namespace.len() {
        ureq::post(format!("{catalog}/v1/namespaces"))
            .send_json(json!({ "namespace": namespace[..depth] }))
            .expect("namespace made");
    }

    let config = IcebergDestinationConfig {
        catalog: catalog.to_string(),
        warehouse: None,
        sign_in: CatalogSignIn::None,
        namespace: namespace.clone(),
        table: case.table.clone(),
        partition_by: case.partition_by.clone(),
        // The fixture vends LocalStack's address on its own network.
        storage: Some(S3Access {
            region: Some("us-east-1".to_string()),
            endpoint: Some(s3.to_string()),
            auth_endpoint: None,
            access_key: Some(AccessKey {
                id: stored("key-id"),
                secret: stored("key"),
            }),
            role: None,
        }),
        compression: case.compression,
        tls: None,
        in_flight: case.in_flight,
        max_object_bytes: case.max_object_bytes,
        buffer_bytes: case.buffer_bytes,
        stall_after: Duration::from_secs(30),
        file_interval: Duration::from_secs(60),
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("key-id".to_string(), 1, "test".to_string()),
        ("key".to_string(), 1, "test".to_string()),
    ]);
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "live");
    let build = |config: &IcebergDestinationConfig, columns: &[(&str, &str)]| {
        let plan = plan(columns);
        let records = smeltr::plan::topology::Records::from([(None, harness::record(&plan))]);
        let d = smeltr::io::build_destination(
            &DestinationKind::Iceberg(config.clone()),
            0,
            metrics.clone(),
            &secrets,
            Some(&records),
        )
        .expect("built");
        (d, plan)
    };

    let mut sent: BTreeMap<i64, BTreeMap<String, Val>> = BTreeMap::new();
    if let Some((columns, batch)) = &case.earlier {
        let unpartitioned = IcebergDestinationConfig {
            partition_by: Vec::new(),
            ..config.clone()
        };
        let (mut d, plan) = build(&unpartitioned, columns);
        drive(d.as_mut(), &plan, has_tags(columns), std::slice::from_ref(batch));
        sent.extend(batch.rows.iter().map(|r| (id_of(r), r.clone())));
    }
    std::thread::scope(|scope| {
        for batches in &case.writers {
            let (mut d, plan) = build(&config, &case.columns);
            scope.spawn(move || drive(d.as_mut(), &plan, has_tags(&case.columns), batches));
        }
    });
    for batch in case.writers.iter().flatten() {
        sent.extend(batch.rows.iter().map(|r| (id_of(r), r.clone())));
    }
    verify(catalog, s3, &namespace, case, &sent)
}

fn has_tags(columns: &[(&str, &str)]) -> bool {
    columns.iter().any(|(n, _)| *n == "tags")
}

fn id_of(row: &BTreeMap<String, Val>) -> i64 {
    match row["id"] {
        Val::Long(id) => id,
        _ => unreachable!("every row has an id"),
    }
}

/// The plan reading a row of `columns` from its JSON.
fn plan(columns: &[(&str, &str)]) -> smeltr::plan::plan::Plan {
    let declarations: String = columns.iter().map(|(n, t)| format!("{n}: {t}; ")).collect();
    harness::compile_ok(vec![harness::code(
        0,
        &format!("id: i64; {declarations}Object.assign($, json(_raw));"),
    )])
}

/// A row's JSON: each leaf at its path, and a list of a few strings when the record has one. An even row leaves its
/// nulls out, which read as null all the same.
fn row_json(row: &BTreeMap<String, Val>, tags: bool) -> String {
    let mut object = Map::new();
    let even = id_of(row) % 2 == 0;
    for (leaf, v) in row {
        if *v == Val::Null && even {
            continue;
        }
        match leaf.split_once('.') {
            Some((parent, child)) => {
                let parent = object.entry(parent).or_insert_with(|| json!({}));
                parent[child] = v.json();
            }
            None => {
                object.insert(leaf.clone(), v.json());
            }
        }
    }
    if tags {
        object.insert("tags".into(), json!(["a", "", "ü"]));
    }
    Value::Object(object).to_string()
}

/// Hands `batches` over as egress would, and waits until every one has landed.
fn drive(d: &mut dyn Destination, plan: &smeltr::plan::plan::Plan, tags: bool, batches: &[Batch]) {
    let mut waiting = HashSet::new();
    let take = |d: &mut dyn Destination, waiting: &mut HashSet<u64>| {
        for c in d.poll_completions().expect("landed") {
            assert!(c.rejections.is_empty(), "{:?}", c.rejections);
            assert!(waiting.remove(&c.batch), "batch {} completed unasked", c.batch);
        }
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    for batch in batches {
        let texts: Vec<String> = batch.rows.iter().map(|r| row_json(r, tags)).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        let chunk = harness::cut(plan, &texts);
        let rows: Vec<OwnedRecord> = (0..texts.len() as u32)
            .map(|row| OwnedRecord {
                payload: smeltr::plan::config::Payload::Record,
                columns: Some(Columns {
                    chunk: chunk.clone(),
                    row,
                }),
                ..OwnedRecord::default()
            })
            .collect();
        loop {
            let (dispatched, refused) = d.deliver(batch.id, Cow::Borrowed(&rows)).expect("delivered");
            assert!(refused.is_empty(), "{refused:?}");
            match dispatched {
                Dispatched::InFlight => {
                    waiting.insert(batch.id);
                    break;
                }
                Dispatched::Complete => break,
                Dispatched::NoRoom { .. } => {
                    assert!(Instant::now() < deadline, "batch {} never found room", batch.id);
                    take(d, &mut waiting);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
        if batch.flush {
            d.flush().expect("flushed");
        }
        take(d, &mut waiting);
    }
    d.flush().expect("flushed");
    while !waiting.is_empty() {
        take(d, &mut waiting);
        assert!(Instant::now() < deadline, "batches {waiting:?} never landed");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The field at `path` among `fields`.
fn field_at<'a>(fields: &'a [NestedField], path: &[&str]) -> Option<&'a NestedField> {
    let field = fields.iter().find(|f| f.name == path[0])?;
    match (&field.field_type, path.len()) {
        (_, 1) => Some(field),
        (Type::Struct(s), _) => field_at(&s.fields, &path[1..]),
        _ => None,
    }
}

/// The dotted path of the field `id` among `fields`.
fn path_of(fields: &[NestedField], id: i32) -> Option<String> {
    fields.iter().find_map(|f| {
        if f.id == id {
            return Some(f.name.clone());
        }
        match &f.field_type {
            Type::Struct(s) => path_of(&s.fields, id).map(|p| format!("{}.{p}", f.name)),
            _ => None,
        }
    })
}

/// The value at `path` of `row` in a Parquet file's batch. A column the file has not is null.
fn read_cell(columns: &StructArray, path: &str, kind: &str, row: usize) -> Val {
    let mut array: &dyn Array = columns;
    for name in path.split('.') {
        let s = array.as_struct();
        if s.is_null(row) {
            return Val::Null;
        }
        match s.column_by_name(name) {
            Some(c) => array = c.as_ref(),
            None => return Val::Null,
        }
    }
    if array.is_null(row) {
        return Val::Null;
    }
    match kind {
        "str" => Val::Str(array.as_string::<i32>().value(row).to_string()),
        "i64" => Val::Long(array.as_primitive::<Int64Type>().value(row)),
        "f64" => Val::Double(array.as_primitive::<Float64Type>().value(row).to_bits()),
        "datetime" => Val::Micros(array.as_primitive::<TimestampMicrosecondType>().value(row)),
        _ => Val::Bool(array.as_boolean().value(row)),
    }
}

/// The table read back from the catalog and the bucket, against the rows sent.
fn verify(
    catalog: &str,
    s3: &str,
    namespace: &[String],
    case: &Case,
    sent: &BTreeMap<i64, BTreeMap<String, Val>>,
) -> Seen {
    let mut seen = Seen::default();
    let answer: Value = ureq::get(format!(
        "{catalog}/v1/namespaces/{}/tables/{}",
        namespace_path(namespace),
        escaped(&case.table, b"")
    ))
    .call()
    .expect("table loaded")
    .body_mut()
    .read_json()
    .unwrap();
    let table: TableMetadata = serde_json::from_value(answer["metadata"].clone()).expect("the table's metadata");
    let schema = table.schema().expect("a current schema");
    let leaves = leaves(&case.columns);
    for (leaf, _) in &leaves {
        let path: Vec<&str> = leaf.split('.').collect();
        assert!(
            field_at(&schema.fields, &path).is_some(),
            "the schema has no column {leaf}"
        );
    }
    let spec = table.spec().expect("a partition spec");
    let drawn: Vec<(String, String)> = if case.earlier.is_some() {
        Vec::new()
    } else {
        (case.partition_by.iter())
            .map(|t| {
                let (transform, path) = partition_term(t).unwrap();
                (partition_name(&transform, &path), transform.to_string())
            })
            .collect()
    };
    let held: Vec<(String, String)> = (spec.fields.iter())
        .map(|f| (f.name.clone(), f.transform.to_string()))
        .collect();
    assert_eq!(held, drawn, "the table's partitioning");

    let snapshot = table.current_snapshot().expect("a current snapshot");
    let total = snapshot.summary.get("total-records").cloned().unwrap_or_default();
    assert_eq!(total, sent.len().to_string(), "the snapshot's total-records");
    let location = table.location.trim_end_matches('/');
    let prefix = format!("s3://{BUCKET}/");
    let key_of = |uri: &str| {
        uri.strip_prefix(&prefix)
            .unwrap_or_else(|| panic!("{uri} is outside the bucket"))
            .to_string()
    };
    let mut found: BTreeMap<i64, usize> = BTreeMap::new();
    let mut live = BTreeSet::new();
    let mut partitions = BTreeSet::new();
    for m in read_manifest_list(&get(s3, &key_of(&snapshot.manifest_list))).unwrap() {
        for entry in read_manifest(&get(s3, &key_of(&m.manifest_path)), &m).unwrap() {
            if entry.status == Status::Deleted {
                continue;
            }
            let path = &entry.file_path;
            assert!(
                path.starts_with(&format!("{location}/data/")),
                "{path} is outside the table's data"
            );
            assert!(live.insert(key_of(path)), "{path} is live twice");
            seen.shortened |= path.contains("/partition-");
            let mut count = 0;
            let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(get(s3, &key_of(path))))
                .expect("a Parquet file")
                .build()
                .unwrap();
            for batch in reader {
                let columns = StructArray::from(batch.expect("a readable row group"));
                for row in 0..columns.len() {
                    count += 1;
                    let Val::Long(id) = read_cell(&columns, "id", "i64", row) else {
                        panic!("{path} has a row with no id");
                    };
                    *found.entry(id).or_default() += 1;
                    let want = sent
                        .get(&id)
                        .unwrap_or_else(|| panic!("{path} holds row {id}, never sent"));
                    for (leaf, kind) in &leaves {
                        let got = read_cell(&columns, leaf, kind, row);
                        let expected = want.get(leaf).cloned().unwrap_or(Val::Null);
                        assert_eq!(got, expected, "row {id}'s {leaf} in {path}");
                    }
                    for (i, field) in spec.fields.iter().enumerate() {
                        let source = path_of(&schema.fields, field.source_id).expect("a partition source");
                        let value = want.get(&source).cloned().unwrap_or(Val::Null);
                        let expected = value.literal().and_then(|l| apply(&field.transform, &l));
                        seen.null_partition |= expected.is_none();
                        let got = entry.partition.get(i).cloned().flatten();
                        assert_eq!(
                            format!("{got:?}"),
                            format!("{expected:?}"),
                            "row {id} is in {path}, whose {} is not its own",
                            field.name
                        );
                    }
                }
            }
            assert_eq!(entry.record_count, count, "{path}'s record_count");
            // Two files of one partition from one commit: the first rolled at its size.
            if !partitions.insert(format!("{:?} {:?}", entry.snapshot_id, entry.partition)) {
                seen.rolled = true;
            }
        }
    }
    let twice: Vec<_> = found.iter().filter(|(_, n)| **n > 1).collect();
    assert!(twice.is_empty(), "rows in more than one file: {twice:?}");
    let missing: Vec<_> = sent.keys().filter(|id| !found.contains_key(id)).collect();
    assert!(missing.is_empty(), "rows sent and in no live file: {missing:?}");
    let data = keys(s3, &format!("{}/data/", key_of(location)));
    assert_eq!(data, live, "the bucket's data files and the table's live files differ");
    seen
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

/// The keys under `prefix` in the bucket, every page of them.
fn keys(s3: &str, prefix: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{s3}/{BUCKET}?list-type=2&prefix={}", escaped(prefix, b""));
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
