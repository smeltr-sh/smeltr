//! Parquet columns cut from a pipeline's own batch, as egress cuts them, written and read back.

use crate::harness;

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, cast::AsArray, types};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use smeltr::io::parquet::{Chunk, Columns, write};
use smeltr::plan::topology::Compression;

use harness::{code, compile_ok, cut};

const JSON: &str = "Object.assign($, json(_raw));";

/// `declarations` then the JSON parse, over `rows`: the chunk egress would cut.
fn chunk(declarations: &str, rows: &[&str]) -> Arc<Chunk> {
    let plan = compile_ok(vec![code(0, &format!("{declarations} {JSON}"))]);
    cut(&plan, rows)
}

/// Every row of `chunk`, written as one file and read back as one batch, checking it holds one row group.
fn round_trip(chunk: &Arc<Chunk>, rows: u32, codec: Compression) -> RecordBatch {
    let rows: Vec<Columns> = (0..rows)
        .map(|row| Columns {
            chunk: chunk.clone(),
            row,
        })
        .collect();
    let path = std::env::temp_dir().join(format!(
        "smeltr-parquet-{}-{:?}.parquet",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, write(&rows, codec)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
    assert_eq!(builder.metadata().num_row_groups(), 1);
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(batches.len(), 1, "the rows fit the reader's default batch");
    batches.into_iter().next().unwrap()
}

fn strs(a: &dyn Array) -> Vec<Option<&str>> {
    a.as_string::<i32>().iter().collect()
}

#[test]
fn every_scalar_type_is_its_column_and_an_absent_field_is_null() {
    let c = chunk(
        "b: bool; i: i64; f: f64; t: datetime; d: duration; s: str;",
        &[
            r#"{"b":true,"i":-5,"f":1,"t":"2023-11-14T22:13:20.000001Z","d":90,"s":"é\""}"#,
            r#"{"f":2.5}"#,
        ],
    );
    let b = round_trip(&c, 2, Compression::None);
    let col = |n: &str| b.column_by_name(n).unwrap().clone();
    assert_eq!(col("b").as_boolean().iter().collect::<Vec<_>>(), [Some(true), None]);
    assert_eq!(
        col("i").as_primitive::<types::Int64Type>().iter().collect::<Vec<_>>(),
        [Some(-5), None]
    );
    assert_eq!(
        col("f").as_primitive::<types::Float64Type>().iter().collect::<Vec<_>>(),
        [Some(1.0), Some(2.5)]
    );
    let t = col("t");
    assert_eq!(
        t.as_primitive::<types::TimestampMicrosecondType>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(1_700_000_000_000_001), None]
    );
    assert_eq!(
        col("d").as_primitive::<types::Int64Type>().iter().collect::<Vec<_>>(),
        [Some(90), None]
    );
    assert_eq!(strs(&col("s")), [Some("é\""), None]);
}

#[test]
fn nested_structs_lists_variants_and_rest_are_their_columns() {
    let c = chunk(
        "o: {x: i64, ...}; l: array<{s: str}>; v: variant;",
        &[
            r#"{"o":{"x":1,"k":[1, 2],"q\"":"w"},"l":[{"s":"a"},{},null],"v":"a","z":true}"#,
            r#"{"o":{"x":2},"l":[],"v":{"a" : 1}}"#,
            r#"{"v":null}"#,
        ],
    );
    let b = round_trip(&c, 3, Compression::None);
    let o = b.column_by_name("o").unwrap().as_struct();
    assert_eq!(o.nulls().unwrap().iter().collect::<Vec<_>>(), [true, true, false]);
    assert_eq!(o.column(0).as_primitive::<types::Int64Type>().values()[..2], [1, 2]);
    assert_eq!(
        strs(o.column_by_name("rest").unwrap())[..2],
        [Some(r#"{"k":[1,2],"q\"":"w"}"#), None]
    );
    let l = b.column_by_name("l").unwrap().as_list::<i32>();
    assert_eq!(l.value_offsets(), [0, 3, 3, 3]);
    assert!(l.is_null(2));
    // An element with no field set is null, as the JSON writer writes it.
    let elems = l.values().as_struct();
    assert_eq!(strs(elems.column(0)), [Some("a"), None, None]);
    assert_eq!(
        strs(b.column_by_name("v").unwrap()),
        [Some(r#""a""#), Some(r#"{"a":1}"#), None]
    );
    assert_eq!(
        strs(b.column_by_name("rest").unwrap()),
        [Some(r#"{"z":true}"#), None, None]
    );
}

#[test]
fn every_codec_is_readable() {
    let c = chunk("s: str;", &[r#"{"s":"a"}"#; 3]);
    for codec in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let b = round_trip(&c, 3, codec);
        assert_eq!(strs(b.column_by_name("s").unwrap()), [Some("a"); 3], "{codec:?}");
    }
}

/// The tag field's head, `smeltr`, is a struct of the record; a row whose own `smeltr` key holds something else does
/// not fit it, and is refused alone.
#[test]
fn a_row_that_does_not_fit_the_record_is_marked_with_its_path() {
    let c = chunk("s: str;", &[r#"{"s":"a"}"#, r#"{"s":"b","smeltr":5}"#]);
    assert_eq!(c.unfit(0), None);
    assert_eq!(c.unfit(1), Some("smeltr is 5, which is no struct"));
}

/// A row's bytes are its own: a long string costs its row, not every row of the chunk.
#[test]
fn a_row_counts_its_own_bytes() {
    let long = "x".repeat(10_000);
    let c = chunk(
        "s: str; n: i64;",
        &[r#"{"s":"a","n":1}"#, &format!(r#"{{"s":"{long}","n":2}}"#)],
    );
    assert!(c.row_bytes(0) < 100, "{}", c.row_bytes(0));
    assert!(c.row_bytes(1) > 10_000, "{}", c.row_bytes(1));
}

/// Queued rows hold what they share whole, and once: one row left behind holds every row of its batch, and rows
/// unrolled from one record hold its kept form once.
#[test]
fn queued_rows_hold_their_chunk_whole_and_once() {
    let long = "x".repeat(10_000);
    let c = chunk("s: str;", &[r#"{"s":"a"}"#, &format!(r#"{{"s":"{long}"}}"#)]);
    let row = |at: u32| smeltr::io::OwnedRecord {
        columns: Some(Columns {
            chunk: c.clone(),
            row: at,
        }),
        ..smeltr::io::OwnedRecord::default()
    };
    let one = smeltr::io::held_bytes(&[row(0)]);
    assert!(one >= c.bytes() && c.bytes() > 10_000, "{one} {}", c.bytes());
    assert_eq!(smeltr::io::held_bytes(&[row(0), row(1)]), one);

    let form: Arc<[u8]> = Arc::from(&b"one source record"[..]);
    let unrolled = |at: u32| smeltr::io::OwnedRecord {
        kept: vec![(smeltr::plan::config::Payload::source_bytes(), form.clone())],
        ..row(at)
    };
    assert_eq!(smeltr::io::held_bytes(&[unrolled(0), unrolled(1)]), one + form.len());
}

/// A row taken alone holds only itself, and keeps why it does not fit.
#[test]
fn a_row_alone_is_a_chunk_of_one() {
    let c = chunk("s: str; n: i64;", &[r#"{"s":"a","n":1}"#, r#"{"s":"bb","n":2}"#]);
    let alone = Columns {
        chunk: c.clone(),
        row: 1,
    }
    .alone();
    assert_eq!(alone.row, 0);
    assert_eq!(alone.chunk.row_bytes(0), c.row_bytes(1));
    let b = round_trip(&alone.chunk, 1, Compression::None);
    assert_eq!(strs(b.column_by_name("s").unwrap()), [Some("bb")]);
    assert_eq!(
        b.column_by_name("n")
            .unwrap()
            .as_primitive::<types::Int64Type>()
            .value(0),
        2
    );
}

#[test]
fn bytes_are_a_binary_column_as_they_are() {
    let plan = compile_ok(vec![code(
        0,
        r#"s: str; Object.assign($, json(_raw)); b = encode(s, "latin1");"#,
    )]);
    let c = cut(&plan, &[r#"{"s":"café"}"#, r#"{}"#]);
    let b = round_trip(&c, 2, Compression::None);
    let col = b.column_by_name("b").unwrap();
    assert_eq!(col.data_type(), &arrow_schema::DataType::Binary);
    assert_eq!(
        col.as_binary::<i32>().iter().collect::<Vec<_>>(),
        [Some(&b"caf\xe9"[..]), None]
    );
}
