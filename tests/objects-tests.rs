//! The object store destinations (`io::objects`), writing into `object_store`'s in-memory store: how rows are grouped
//! into objects by prefix, what an object holds in each format and codec, how it is named and typed, and when a batch
//! is acknowledged. Every format runs on the file destination, a file open across batches until its interval or
//! `flush`.

use crate::harness;

use std::borrow::Cow;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{Attribute, Attributes, ObjectStore, ObjectStoreExt};
use smeltr::core::column::DataType;
use smeltr::io::parquet::{Chunk, Columns};
use smeltr::io::{BatchId, Destination, Dispatched, OwnedRecord};
use smeltr::plan::plan::Plan;
use smeltr::plan::topology::{
    Cloud, Compression, Newlines, ObjectFormat, ObjectsDestinationConfig, Records, S3Access, S3Store,
};

/// The destination is the same for every cloud; S3 stands in for them all.
fn config(format: ObjectFormat, compression: Compression) -> ObjectsDestinationConfig {
    ObjectsDestinationConfig {
        cloud: Cloud::S3(S3Store {
            bucket: "logs".to_string(),
            kms_key_id: None,
            access: S3Access::default(),
        }),
        prefix: "default/".to_string(),
        format,
        compression,
        storage_class: None,
        identity: None,
        in_flight: 1,
        max_object_bytes: 1024 * 1024,
        buffer_bytes: 1024 * 1024,
        stall_after: Duration::from_secs(5),
        file_interval: Duration::from_secs(60),
    }
}

fn row(value: &str, prefix: Option<&str>) -> OwnedRecord {
    OwnedRecord {
        value: value.as_bytes().to_vec(),
        payload: smeltr::plan::config::Payload::Record,
        placement: prefix.map(str::to_string),
        ..OwnedRecord::default()
    }
}

fn text(value: &str, prefix: Option<&str>) -> OwnedRecord {
    OwnedRecord {
        payload: smeltr::plan::config::Payload::Text { field: "_raw".into() },
        ..row(value, prefix)
    }
}

struct Fixture {
    store: Arc<InMemory>,
    runtime: Arc<tokio::runtime::Runtime>,
    destination: Box<dyn Destination>,
}

/// A destination writing `records`, route by route, which only Parquet reads.
fn open(config: &ObjectsDestinationConfig, records: Option<&Records>) -> Fixture {
    let store = Arc::new(InMemory::new());
    let runtime = smeltr::io::objects::runtime().unwrap();
    let destination = smeltr::io::objects::destination(config, records, store.clone(), runtime.clone()).unwrap();
    Fixture {
        store,
        runtime,
        destination: Box::new(destination),
    }
}

/// A destination in one of the line formats.
fn fixture(config: &ObjectsDestinationConfig) -> Fixture {
    open(config, None)
}

/// A Parquet destination writing `records`, route by route.
fn parquet(config: &ObjectsDestinationConfig, records: &Records) -> Fixture {
    open(config, Some(records))
}

/// A Parquet destination writing `record` for rows no route made.
fn writing(config: &ObjectsDestinationConfig, record: &DataType) -> Fixture {
    parquet(config, &Records::from([(None, record.clone())]))
}

impl Fixture {
    /// Every object in the store, by key, as it was written.
    fn objects(&self) -> Vec<(String, Vec<u8>)> {
        self.runtime.block_on(async {
            let mut out = Vec::new();
            let mut todo = vec![None::<Path>];
            while let Some(at) = todo.pop() {
                let listed = self.store.list_with_delimiter(at.as_ref()).await.unwrap();
                todo.extend(listed.common_prefixes.into_iter().map(Some));
                for meta in listed.objects {
                    let bytes = self.store.get(&meta.location).await.unwrap().bytes().await.unwrap();
                    out.push((meta.location.to_string(), bytes.to_vec()));
                }
            }
            out.sort();
            out
        })
    }

    /// What the object at `key` was written with.
    fn attributes(&self, key: &str) -> Attributes {
        self.runtime.block_on(async {
            let got = self.store.get(&Path::parse(key).unwrap()).await.unwrap();
            got.attributes.clone()
        })
    }

    /// Writes `rows` as one batch into files of their own, and answers what was refused.
    fn deliver(&mut self, rows: Vec<OwnedRecord>) -> Vec<smeltr::io::Rejection> {
        let refused = self.hand_over(0, rows);
        assert_eq!(self.finish(), [0]);
        refused
    }

    /// Hands `rows` over as batch `batch`, which stays in flight until its file is finished.
    fn hand_over(&mut self, batch: BatchId, rows: Vec<OwnedRecord>) -> Vec<smeltr::io::Rejection> {
        let (dispatched, refused) = self.destination.deliver(batch, Cow::Owned(rows)).unwrap();
        assert_eq!(dispatched, Dispatched::InFlight);
        refused
    }

    /// Finishes every open file, and answers the batches that completed, in order.
    fn finish(&mut self) -> Vec<BatchId> {
        self.destination.flush().unwrap();
        let done = self.destination.poll_completions().unwrap();
        assert!(done.iter().all(|c| c.rejections.is_empty()), "{done:?}");
        done.iter().map(|c| c.batch).collect()
    }
}

fn gunzip(bytes: &[u8]) -> String {
    let mut out = String::new();
    flate2::read::GzDecoder::new(bytes).read_to_string(&mut out).unwrap();
    out
}

/// `bytes` decompressed as `codec`'s own tool reads them.
fn decode(codec: Compression, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    match codec {
        Compression::None => out.extend_from_slice(bytes),
        Compression::Gzip => drop(flate2::read::GzDecoder::new(bytes).read_to_end(&mut out).unwrap()),
        Compression::Snappy => drop(snap::read::FrameDecoder::new(bytes).read_to_end(&mut out).unwrap()),
        Compression::Lz4 => drop(lz4_flex::frame::FrameDecoder::new(bytes).read_to_end(&mut out).unwrap()),
        Compression::Zstd => out = zstd::stream::decode_all(bytes).unwrap(),
    }
    out
}

/// Rows naming different prefixes never share an object; a row naming none goes under the configured one. A line
/// format writes every route's rows alike.
#[test]
fn each_prefix_gets_an_object_of_its_own() {
    let mut f = fixture(&config(ObjectFormat::Ndjson, Compression::Gzip));
    let refused = f.deliver(vec![
        row(r#"{"n":1}"#, Some("a/")),
        row(r#"{"n":2}"#, Some("b/")),
        OwnedRecord {
            route: Some(5),
            ..row(r#"{"n":3}"#, Some("a/"))
        },
        row(r#"{"n":4}"#, None),
    ]);
    assert!(refused.is_empty(), "{refused:?}");
    let objects = f.objects();
    let under = |prefix: &str| -> Vec<String> {
        objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix) && k.ends_with(".ndjson.gz"))
            .map(|(_, v)| gunzip(v))
            .collect()
    };
    assert_eq!(
        objects.len(),
        3,
        "{:?}",
        objects.iter().map(|o| &o.0).collect::<Vec<_>>()
    );
    assert_eq!(under("a/"), ["{\"n\":1}\n{\"n\":3}\n"]);
    assert_eq!(under("b/"), ["{\"n\":2}\n"]);
    assert_eq!(under("default/"), ["{\"n\":4}\n"]);
}

/// What one object holding `lines` was when each batch was put whole: the lines framed for `format`, before the
/// codec.
fn whole_body(format: &ObjectFormat, lines: &[&[u8]]) -> Vec<u8> {
    let mut plain = Vec::new();
    if *format == ObjectFormat::Json {
        plain.push(b'[');
        for (i, line) in lines.iter().enumerate() {
            if i > 0 {
                plain.push(b',');
            }
            plain.extend_from_slice(line);
        }
        plain.extend_from_slice(b"]\n");
    } else {
        for line in lines {
            plain.extend_from_slice(line);
            plain.push(b'\n');
        }
    }
    plain
}

/// A line format, its extension and content type uncompressed, the rows written, and each row's line.
type LineCase<'a> = (ObjectFormat, &'a str, &'a str, Vec<OwnedRecord>, Vec<&'a [u8]>);

/// Each format and codec names its object and types it for what it holds, and the object decodes to the body a
/// whole put of the same rows held: NDJSON and JSON trimmed, JSON one array, text as it is, bytes escaped.
#[test]
fn every_line_format_and_codec_names_types_and_reads_back() {
    let bytes = ObjectFormat::Bytes {
        field: smeltr::plan::topology::SOURCE_BYTES.into(),
        newlines: Newlines::Escape,
    };
    let formats: [LineCase; 4] = [
        (
            ObjectFormat::Ndjson,
            "ndjson",
            "application/x-ndjson",
            vec![row(" {\"n\":1}\n", None), row(r#"{"n":2}"#, None)],
            vec![br#"{"n":1}"#, br#"{"n":2}"#],
        ),
        (
            ObjectFormat::Json,
            "json",
            "application/json",
            vec![row(r#"{"n":1}"#, None), row("{\"n\":2}\r\n", None)],
            vec![br#"{"n":1}"#, br#"{"n":2}"#],
        ),
        (
            ObjectFormat::Text { field: "_raw".into() },
            "txt",
            "text/plain; charset=utf-8",
            vec![text(" first line ", None), text("second", None)],
            vec![b" first line ", b"second"],
        ),
        (
            bytes,
            "raw",
            "application/octet-stream",
            vec![
                OwnedRecord {
                    placement: None,
                    ..source_bytes(b"caf\xe9")
                },
                OwnedRecord {
                    placement: None,
                    ..source_bytes(b"two\r\nlines \\")
                },
            ],
            vec![b"caf\xe9", b"two\\r\\nlines \\\\"],
        ),
    ];
    for (format, stem, plain_type, rows, lines) in formats {
        for (codec, suffix, packed_type) in [
            (Compression::None, "", plain_type),
            (Compression::Gzip, ".gz", "application/gzip"),
            (Compression::Snappy, ".sz", "application/x-snappy-framed"),
            (Compression::Lz4, ".lz4", "application/x-lz4"),
            (Compression::Zstd, ".zst", "application/zstd"),
        ] {
            let mut f = fixture(&config(format.clone(), codec));
            let refused = f.deliver(rows.clone());
            assert!(refused.is_empty(), "{format:?} {codec:?}: {refused:?}");
            let objects = f.objects();
            assert_eq!(objects.len(), 1, "{format:?} {codec:?}");
            let (key, body) = &objects[0];
            let extension = format!(".{stem}{suffix}");
            assert!(
                key.starts_with("default/") && key.ends_with(&extension),
                "{format:?} {codec:?}: {key}"
            );
            assert_eq!(
                f.attributes(key).get(&Attribute::ContentType).map(|v| v.as_ref()),
                Some(packed_type),
                "{format:?} {codec:?}"
            );
            assert_eq!(decode(codec, body), whole_body(&format, &lines), "{format:?} {codec:?}");
        }
    }
}

/// An object takes the configured storage class; with none, it names none and takes the bucket's default.
#[test]
fn an_object_takes_its_storage_class() {
    for format in [ObjectFormat::Ndjson, ObjectFormat::Parquet] {
        for class in [None, Some("GLACIER")] {
            let mut c = config(format.clone(), Compression::None);
            c.storage_class = class.map(str::to_string);
            let mut f = match format {
                ObjectFormat::Parquet => {
                    let plan = parsing("n: i64;");
                    let chunk = harness::cut(&plan, &[r#"{"n":1}"#]);
                    let mut f = writing(&c, &harness::record(&plan));
                    f.hand_over(0, vec![cut_row(&chunk, 0, None)]);
                    f
                }
                _ => {
                    let mut f = fixture(&c);
                    f.hand_over(0, vec![row(r#"{"n":1}"#, None)]);
                    f
                }
            };
            assert_eq!(f.finish(), [0]);
            let key = f.objects().remove(0).0;
            assert_eq!(
                f.attributes(&key).get(&Attribute::StorageClass).map(|v| v.as_ref()),
                class,
                "{format:?}"
            );
        }
    }
}

/// Identical rows in two files are two objects: a key never names what an earlier object holds.
#[test]
fn identical_files_are_two_objects() {
    let mut f = fixture(&config(ObjectFormat::Ndjson, Compression::None));
    let rows = || vec![row(r#"{"status":"ok"}"#, Some("p/"))];
    f.deliver(rows());
    f.deliver(rows());
    let objects = f.objects();
    assert_eq!(objects.len(), 2, "{objects:?}");
    for (_, body) in &objects {
        assert_eq!(String::from_utf8_lossy(body), "{\"status\":\"ok\"}\n");
    }
}

#[test]
fn json_writes_one_array_and_text_writes_one_field() {
    let mut f = fixture(&config(ObjectFormat::Json, Compression::None));
    f.deliver(vec![row(r#"{"n":1}"#, Some("j/")), row(r#"{"n":2}"#, Some("j/"))]);
    let objects = f.objects();
    assert!(objects[0].0.ends_with(".json"), "{}", objects[0].0);
    assert_eq!(String::from_utf8_lossy(&objects[0].1), "[{\"n\":1},{\"n\":2}]\n");

    let plain = ObjectFormat::Text { field: "_raw".into() };
    let mut f = fixture(&config(plain, Compression::Zstd));
    let lacking = OwnedRecord {
        unfit: Some("it has no _raw to write as text".to_string()),
        ..text("", Some("t/"))
    };
    let refused = f.deliver(vec![
        text("first line", Some("t/")),
        lacking,
        text("a raw record", Some("t/")),
    ]);
    assert_eq!(refused.len(), 1, "the row with no _raw is refused, alone: {refused:?}");
    assert_eq!(refused[0].doc, 1);
    let objects = f.objects();
    assert!(objects[0].0.ends_with(".txt.zst"), "{}", objects[0].0);
    let plain = zstd::stream::decode_all(objects[0].1.as_slice()).unwrap();
    assert_eq!(String::from_utf8_lossy(&plain), "first line\na raw record\n");
}

fn source_bytes(bytes: &[u8]) -> OwnedRecord {
    OwnedRecord {
        value: bytes.to_vec(),
        payload: smeltr::plan::config::Payload::source_bytes(),
        placement: Some("r/".to_string()),
        ..OwnedRecord::default()
    }
}

/// A bytes object holds each row's unmodified source bytes, one per line, any encoding, its line breaks escaped.
#[test]
fn a_source_bytes_object_holds_them_unmodified() {
    let raw = ObjectFormat::Bytes {
        field: smeltr::plan::topology::SOURCE_BYTES.into(),
        newlines: Newlines::Escape,
    };
    let mut f = fixture(&config(raw, Compression::None));
    let refused = f.deliver(vec![source_bytes(b"caf\xe9"), source_bytes(b"two\r\nlines \\")]);
    assert!(refused.is_empty(), "{refused:?}");
    let objects = f.objects();
    assert!(objects[0].0.ends_with(".raw"), "{}", objects[0].0);
    assert_eq!(objects[0].1, b"caf\xe9\ntwo\\r\\nlines \\\\\n");
}

/// A bytes object that refuses line breaks refuses a row holding one, alone.
#[test]
fn a_source_bytes_object_may_refuse_line_breaks() {
    let raw = ObjectFormat::Bytes {
        field: smeltr::plan::topology::SOURCE_BYTES.into(),
        newlines: Newlines::Refuse,
    };
    let mut f = fixture(&config(raw, Compression::None));
    let refused = f.deliver(vec![
        source_bytes(b"one"),
        source_bytes(b"two\nlines"),
        source_bytes(b"cr\r"),
        source_bytes(b"a \\"),
    ]);
    assert_eq!(refused.iter().map(|r| r.doc).collect::<Vec<_>>(), [1, 2]);
    assert!(
        refused[0]
            .reason
            .contains("it holds a line break, which this bytes object refuses"),
        "{}",
        refused[0].reason
    );
    assert_eq!(f.objects()[0].1, b"one\na \\\n");
}

/// A row's prefix is taken as it stands, and one that would misplace its object is refused for that row alone.
#[test]
fn a_row_prefix_is_literal_and_checked() {
    let mut f = fixture(&config(ObjectFormat::Ndjson, Compression::None));
    let refused = f.deliver(vec![
        row(r#"{"n":1}"#, Some("100%/")),
        row(r#"{"n":2}"#, Some("/abs/")),
        row(r#"{"n":3}"#, Some("a//b/")),
    ]);
    assert_eq!(refused.iter().map(|r| r.doc).collect::<Vec<_>>(), [1, 2]);
    let objects = f.objects();
    assert_eq!(objects.len(), 1);
    assert!(objects[0].0.starts_with("100%/"), "{}", objects[0].0);
}

/// Past the object size a prefix's rows are written in more than one object, each a complete file.
#[test]
fn a_large_batch_is_written_in_several_objects() {
    // Uncompressed, so 64 written bytes is a few rows.
    let mut c = config(ObjectFormat::Ndjson, Compression::None);
    c.max_object_bytes = 64;
    let mut f = fixture(&c);
    let rows: Vec<OwnedRecord> = (0..20)
        .map(|i| row(&format!(r#"{{"n":{}}}"#, i + 10), Some("big/")))
        .collect();
    f.deliver(rows);
    let objects = f.objects();
    assert!(objects.len() > 1, "{objects:?}");
    let mut lines: Vec<String> = objects
        .iter()
        .flat_map(|(_, v)| {
            String::from_utf8_lossy(v)
                .lines()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect();
    lines.sort();
    let expected: Vec<String> = (0..20).map(|i| format!(r#"{{"n":{}}}"#, i + 10)).collect();
    assert_eq!(lines, expected, "every row once");
}

/// A batch whose rows go under several prefixes completes once, when every file holding its rows is finished.
#[test]
fn a_batch_completes_once_every_prefix_lands() {
    let mut f = fixture(&config(ObjectFormat::Ndjson, Compression::None));
    let refused = f.hand_over(7, vec![row(r#"{"n":1}"#, Some("x/")), row(r#"{"n":2}"#, Some("y/"))]);
    assert!(refused.is_empty(), "{refused:?}");
    assert_eq!(f.finish(), [7], "one completion for the batch");
    assert_eq!(f.objects().len(), 2);
}

/// Batches to one prefix within the file interval share one object, a JSON array among them, and none is
/// acknowledged before it is finished.
#[test]
fn batches_within_the_interval_share_one_line_object() {
    for (format, want) in [
        (ObjectFormat::Ndjson, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"),
        (ObjectFormat::Json, "[{\"n\":1},{\"n\":2},{\"n\":3}]\n"),
    ] {
        let mut f = fixture(&config(format.clone(), Compression::Gzip));
        assert!(f.hand_over(1, vec![row(r#"{"n":1}"#, None)]).is_empty());
        assert!(
            f.hand_over(2, vec![row(r#"{"n":2}"#, None), row(r#"{"n":3}"#, None)])
                .is_empty()
        );
        assert!(
            f.destination.poll_completions().unwrap().is_empty(),
            "a batch waits for its file"
        );
        assert_eq!(f.finish(), [1, 2]);
        let objects = f.objects();
        assert_eq!(objects.len(), 1, "{format:?}");
        assert_eq!(gunzip(&objects[0].1), want, "{format:?}");
    }
}

/// A line object is finished at its interval with no `flush`, and its batch acknowledged then.
#[test]
fn a_line_object_is_finished_at_its_interval() {
    let mut c = config(ObjectFormat::Ndjson, Compression::Zstd);
    c.file_interval = Duration::from_millis(200);
    let mut f = fixture(&c);
    assert!(f.hand_over(4, vec![row(r#"{"n":1}"#, None)]).is_empty());
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let done = loop {
        let done = f.destination.poll_completions().unwrap();
        if !done.is_empty() {
            break done;
        }
        assert!(std::time::Instant::now() < deadline, "no completion at the interval");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), [4]);
    let objects = f.objects();
    assert_eq!(objects.len(), 1);
    assert_eq!(decode(Compression::Zstd, &objects[0].1), b"{\"n\":1}\n");
}

/// `pipeline`'s row `at` over `texts`, as egress hands it to a Parquet destination: its place in the chunk, from
/// route `route`.
fn cut_row(chunk: &Arc<Chunk>, at: u32, route: Option<u32>) -> OwnedRecord {
    OwnedRecord {
        payload: smeltr::plan::config::Payload::Record,
        route,
        columns: Some(Columns {
            chunk: chunk.clone(),
            row: at,
        }),
        ..OwnedRecord::default()
    }
}

/// `declarations`, then the JSON parse: a pipeline whose record a Parquet destination writes.
fn parsing(declarations: &str) -> Plan {
    harness::compile_ok(vec![harness::code(
        0,
        &format!("{declarations} Object.assign($, json(_raw));"),
    )])
}

/// A Parquet object holds the record's columns, compressed inside rather than wrapped, and a row that does not fit
/// the record is refused alone.
#[test]
fn a_parquet_object_holds_the_records_columns() {
    use parquet::file::reader::FileReader;
    let plan = parsing("n: i64; msg: str;");
    let chunk = harness::cut(&plan, &[r#"{"n":1,"msg":"a"}"#, r#"{"n":2,"smeltr":5}"#, r#"{"n":3}"#]);
    let record = harness::record(&plan);
    let mut f = writing(&config(ObjectFormat::Parquet, Compression::Gzip), &record);
    let refused = f.hand_over(0, (0..3).map(|at| cut_row(&chunk, at, None)).collect());
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(f.finish(), [0]);
    let objects = f.objects();
    assert_eq!(objects.len(), 1);
    let (key, body) = &objects[0];
    assert!(key.ends_with(".parquet"), "{key}");
    assert_eq!(
        f.attributes(key).get(&Attribute::ContentType).map(|v| v.as_ref()),
        Some("application/vnd.apache.parquet")
    );
    let file = read_parquet(body);
    let schema = file.metadata().file_metadata().schema_descr();
    let columns: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        columns,
        [
            "_raw", "msg", "n", "route", "pipeline", "step", "kind", "message", "rest"
        ]
    );
    let rows: Vec<String> = file
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap().to_string())
        .collect();
    assert_eq!(
        rows,
        [
            r#"{_raw: "{"n":1,"msg":"a"}", msg: "a", n: 1, smeltr: null, rest: null}"#,
            r#"{_raw: "{"n":3}", msg: null, n: 3, smeltr: null, rest: null}"#
        ]
    );
}

/// A CSV object holds a header of the record's leaves, then a line per row: a struct's fields under its path, a
/// variant, an array and `rest` as JSON, a null empty and an empty string quoted.
#[test]
fn a_csv_object_holds_the_records_leaves() {
    let plan = parsing("n: i64; msg: str; f: f64; v: variant; tags: array<str>; o: {a: i64, ...};");
    let chunk = harness::cut(
        &plan,
        &[
            r#"{"n":1,"msg":"a, \"b\"","f":2,"v":{"x":1},"tags":["p","q"],"o":{"a":5,"z":"y"},"extra":true}"#,
            r#"{"n":2,"msg":""}"#,
        ],
    );
    let record = harness::record(&plan);
    let mut f = writing(&config(ObjectFormat::Csv, Compression::Gzip), &record);
    let refused = f.hand_over(0, (0..2).map(|at| cut_row(&chunk, at, None)).collect());
    assert!(refused.is_empty(), "{refused:?}");
    assert_eq!(f.finish(), [0]);
    let objects = f.objects();
    assert_eq!(objects.len(), 1);
    let (key, body) = &objects[0];
    assert!(key.ends_with(".csv.gz"), "{key}");
    assert_eq!(
        f.attributes(key).get(&Attribute::ContentType).map(|v| v.as_ref()),
        Some("application/gzip")
    );
    let text = gunzip(body);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines,
        [
            "_raw,f,msg,n,o.a,o.rest,tags,v,smeltr.exception.route,smeltr.exception.pipeline,\
             smeltr.exception.exceptions,rest",
            r#""{""n"":1,""msg"":""a, \""b\"""",""f"":2,""v"":{""x"":1},""tags"":[""p"",""q""],""o"":{""a"":5,""z"":""y""},""extra"":true}",2.0,"a, ""b""",1,5,"{""z"":""y""}","[""p"",""q""]","{""x"":1}",,,,"{""extra"":true}""#,
            r#""{""n"":2,""msg"":""""}",,"",2,,,,,,,,"#,
        ]
    );
}

/// `body` as a Parquet file a reader opens.
fn read_parquet(body: &[u8]) -> parquet::file::reader::SerializedFileReader<std::fs::File> {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("smeltr-objects-{}-{n}.parquet", std::process::id()));
    std::fs::write(&path, body).unwrap();
    let file = parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    std::fs::remove_file(&path).unwrap();
    file
}

/// Every row of `body`, a Parquet file, as the reader prints it.
fn parquet_rows(body: &[u8]) -> Vec<String> {
    use parquet::file::reader::FileReader;
    read_parquet(body)
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap().to_string())
        .collect()
}

/// Each route's rows go under that route's own record, so two routes writing one prefix make two objects.
#[test]
fn each_routes_rows_make_objects_of_their_own() {
    let (int, float) = (parsing("n: i64;"), parsing("n: f64;"));
    let records = Records::from([(Some(0), harness::record(&int)), (Some(1), harness::record(&float))]);
    let mut f = parquet(&config(ObjectFormat::Parquet, Compression::None), &records);
    let (ints, floats) = (
        harness::cut(&int, &[r#"{"n":1}"#]),
        harness::cut(&float, &[r#"{"n":1.5}"#]),
    );
    let refused = f.hand_over(
        0,
        vec![
            cut_row(&ints, 0, Some(0)),
            cut_row(&floats, 0, Some(1)),
            cut_row(&ints, 0, Some(2)),
        ],
    );
    assert_eq!(
        refused.len(),
        1,
        "a route that writes nothing here has no columns: {refused:?}"
    );
    assert_eq!(f.finish(), [0]);
    assert_eq!(f.objects().len(), 2);
}

/// Batches to one prefix within the file interval share one Parquet object, and none is acknowledged before it is
/// finished.
#[test]
fn batches_within_the_interval_share_one_parquet_object() {
    let plan = parsing("n: i64;");
    let chunk = harness::cut(&plan, &[r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#]);
    let mut f = writing(
        &config(ObjectFormat::Parquet, Compression::None),
        &harness::record(&plan),
    );
    assert!(f.hand_over(1, vec![cut_row(&chunk, 0, None)]).is_empty());
    assert!(
        f.hand_over(2, vec![cut_row(&chunk, 1, None), cut_row(&chunk, 2, None)])
            .is_empty()
    );
    assert!(
        f.destination.poll_completions().unwrap().is_empty(),
        "a batch waits for its file"
    );
    assert_eq!(f.finish(), [1, 2]);
    let objects = f.objects();
    assert_eq!(
        objects.len(),
        1,
        "{:?}",
        objects.iter().map(|o| &o.0).collect::<Vec<_>>()
    );
    assert!(
        objects[0].0.starts_with("default/") && objects[0].0.ends_with(".parquet"),
        "{}",
        objects[0].0
    );
    let ns: Vec<String> = parquet_rows(&objects[0].1)
        .iter()
        .map(|r| r[r.find("n: ").unwrap()..].split(',').next().unwrap().to_string())
        .collect();
    assert_eq!(ns, ["n: 1", "n: 2", "n: 3"]);
}

/// A file is finished at its interval with no `flush`, and its batch acknowledged then.
#[test]
fn a_parquet_object_is_finished_at_its_interval() {
    let plan = parsing("n: i64;");
    let chunk = harness::cut(&plan, &[r#"{"n":1}"#]);
    let mut c = config(ObjectFormat::Parquet, Compression::None);
    c.file_interval = Duration::from_millis(200);
    let mut f = writing(&c, &harness::record(&plan));
    assert!(f.hand_over(4, vec![cut_row(&chunk, 0, None)]).is_empty());
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let done = loop {
        let done = f.destination.poll_completions().unwrap();
        if !done.is_empty() {
            break done;
        }
        assert!(std::time::Instant::now() < deadline, "no completion at the interval");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), [4]);
    assert_eq!(f.objects().len(), 1);
}

/// A Parquet row's `__destination.prefix` puts it under that prefix, taken as it stands; one that would misplace its
/// object is refused for that row alone, and a row naming none goes under the configured prefix.
#[test]
fn parquet_rows_land_under_their_own_prefix() {
    let plan = parsing("n: i64;");
    let chunk = harness::cut(
        &plan,
        &[r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#, r#"{"n":4}"#, r#"{"n":5}"#],
    );
    let mut f = writing(
        &config(ObjectFormat::Parquet, Compression::None),
        &harness::record(&plan),
    );
    let placed = |at: u32, prefix: Option<&str>| OwnedRecord {
        placement: prefix.map(str::to_string),
        ..cut_row(&chunk, at, None)
    };
    let refused = f.hand_over(
        0,
        vec![
            placed(0, Some("a/")),
            placed(1, Some("100%/")),
            placed(2, Some("a/")),
            placed(3, Some("/abs/")),
            placed(4, None),
        ],
    );
    assert_eq!(refused.iter().map(|r| r.doc).collect::<Vec<_>>(), [3]);
    assert!(
        refused[0]
            .reason
            .contains(r#"its __destination.prefix "/abs/" is refused: a key does not start with /"#),
        "{}",
        refused[0].reason
    );
    assert_eq!(f.finish(), [0]);
    let objects = f.objects();
    let under = |prefix: &str| -> Vec<usize> {
        objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix) && k.ends_with(".parquet"))
            .map(|(_, v)| parquet_rows(v).len())
            .collect()
    };
    assert_eq!(
        objects.len(),
        3,
        "{:?}",
        objects.iter().map(|o| &o.0).collect::<Vec<_>>()
    );
    assert_eq!(under("a/"), [2]);
    assert_eq!(under("100%/"), [1]);
    assert_eq!(under("default/"), [1]);
}
