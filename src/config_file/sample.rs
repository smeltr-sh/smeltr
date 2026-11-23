//! A sample: records under a name, stored beside the config they are authored against.
//!
//! A bundle file that only the leader reads. `load_bundle_from` reads the files it names and every pipeline; a
//! sample is neither, so it travels with the config in one history and one access path without ever reaching a
//! worker.
//!
//! A sample holds a capture's table as the capture held it: its columns, and under them each row's values and which
//! of them failed their type. Records a person pastes are a table of one column that holds each whole.

use serde::{Deserialize, Serialize};

use super::bundle::{BundleError, FileSet, slugify};

/// Where samples live inside a bundle, beside `pipelines/`.
pub const SAMPLES_DIR: &str = "samples";

/// The most records one sample may hold.
///
/// Not a memory ceiling: a preview carries its own budget, and running a
/// record at a time is flat in the size of the sample. Two other things set
/// this. A preview recomputes as its author types and a step's diff is two
/// runs of the whole sample, so the sample's size is a keystroke's latency --
/// around 80us a record measured over a real pipeline, so a thousand is a
/// sixth of a second for the pair. And a test locked from a sample stores one
/// expected document per record, in a file that travels with the bundle
/// forever; at this size that is about a megabyte, and ten times it is not a
/// file anyone wants in a history.
///
/// Enforced where a sample is written rather than where one is read: a bundle
/// that already holds a larger sample still loads, because refusing to read it
/// would take the whole library down with it.
pub const MAX_SAMPLE_RECORDS: usize = 1000;

/// `Err` with what to tell the author when a sample of `records` records cannot be stored: none, or more than
/// [`MAX_SAMPLE_RECORDS`].
pub fn refuse_sample_size(records: usize) -> Result<(), String> {
    if records == 0 {
        return Err(
            "a sample holds at least one record. One with none previews as an empty pipeline rather than \
                    as the problem it is"
                .to_string(),
        );
    }
    if records <= MAX_SAMPLE_RECORDS {
        return Ok(());
    }
    Err(format!(
        "a sample holds at most {MAX_SAMPLE_RECORDS} records and this one has {records}. A preview runs the \
         whole sample every time the pipeline changes, and a test locked from it stores one expected document \
         per record, so a larger sample is slower to build against and heavier to carry than it is useful."
    ))
}

/// One set of records under a name.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleDto {
    /// Stable across renames. The filename is its slug.
    pub id: String,
    pub name: String,
    /// The source these records stand for. Routing selects on a source, so a sample naming none can be run through
    /// a pipeline but not through a route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The records, as their capture held them (`sample_table`): a record's fields, then its `__source` fields, at a
    /// destination its `__destination` fields, and then its `__capture` fields, how the capture was taken
    /// (`leader::capture::Captures::capture_fields`) or how a pipeline's output was made
    /// (`leader::capture::output_fields`). Pasted records are `CaptureBatch::pasted`.
    #[serde(flatten)]
    pub table: crate::heartbeat::CaptureBatch,
}

/// What a library listing shows: everything but the records.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleSummary {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub records: usize,
    /// When the leader last wrote the sample, in unix seconds: the newest commit that touched its file. The listing
    /// fills it from `config.git`'s history; a sample is not stored with it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub written: Option<i64>,
    /// When its records were captured: the first row's `__capture.timestamp`. A sample pasted or uploaded has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub captured: Option<String>,
    /// The file it was read from, within its bundle.
    #[serde(skip)]
    pub path: String,
}

impl SampleDto {
    pub fn summary(&self) -> SampleSummary {
        SampleSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            source: self.source.clone(),
            records: self.table.rows.len(),
            written: None,
            captured: (self.table.column("__capture.timestamp"))
                .and_then(|c| self.table.rows.first()?.values.get(c)?.as_str())
                .map(str::to_string),
            path: String::new(),
        }
    }
}

/// The file `id` is stored in, relative to the bundle.
///
/// An id with no alphanumeric character has no slug, and is refused rather than given a shared fallback name: two
/// such ids would name one file, and the second save would silently replace the first.
pub fn sample_path(id: &str) -> Result<String, BundleError> {
    if !id.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(BundleError::Io {
            path: format!("{SAMPLES_DIR}/"),
            message: format!("a sample id needs at least one letter or digit; {id:?} has none"),
        });
    }
    Ok(format!("{SAMPLES_DIR}/{}.json", slugify(id)))
}

/// A DTO is plain string-and-number data; serializing it cannot fail.
pub fn sample_bytes(sample: &SampleDto) -> Vec<u8> {
    serde_json::to_vec_pretty(sample).expect("a sample always serializes")
}

/// The sample stored under `id`, or `None` if the bundle holds no such file.
pub fn read_sample(files: &mut dyn FileSet, id: &str) -> Result<Option<SampleDto>, BundleError> {
    let path = sample_path(id)?;
    let Some(bytes) = files.read(&path).map_err(|e| BundleError::Io {
        path: path.clone(),
        message: e.to_string(),
    })?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| BundleError::Json { path, source })
}

/// Every sample in the bundle, and every path under `samples/` that did not read, paired with why.
///
/// Lenient like every other read the GUI makes (`read_raw_from`): one malformed sample is reported beside the rest
/// rather than emptying the library. A directory that could not be listed at all is reported the same way, under
/// `SAMPLES_DIR` itself -- otherwise a broken listing and an empty one both come back as "no samples", and only one
/// of those is true.
pub fn list_samples(files: &mut dyn FileSet) -> (Vec<SampleSummary>, Vec<(String, String)>) {
    let mut found = Vec::new();
    let mut unreadable = Vec::new();
    let paths = match files.json_paths_under(SAMPLES_DIR) {
        Ok(paths) => paths,
        Err(e) => {
            unreadable.push((SAMPLES_DIR.to_string(), e.to_string()));
            return (found, unreadable);
        }
    };
    for path in paths {
        match files.read(&path) {
            Ok(Some(bytes)) => match serde_json::from_slice::<SampleDto>(&bytes) {
                Ok(sample) => found.push(SampleSummary {
                    path: path.clone(),
                    ..sample.summary()
                }),
                Err(e) => unreadable.push((path, e.to_string())),
            },
            Ok(None) => {}
            Err(e) => unreadable.push((path, e.to_string())),
        }
    }
    found.sort_by(|a, b| a.name.cmp(&b.name));
    (found, unreadable)
}

/// The one table a sample of `batches` holds: each column of every batch, in the order first seen, and every row under
/// them, null where its batch had no such column. Batches differ where workers run different builds, or where a
/// destination is fed by several routes; a name they type differently becomes a variant, which holds a value of any
/// type.
pub fn sample_table(batches: &[crate::heartbeat::CaptureBatch]) -> crate::heartbeat::CaptureBatch {
    let mut table = crate::heartbeat::CaptureBatch::default();
    for batch in batches {
        let at: Vec<usize> = (batch.columns.iter())
            .map(|column| match table.column(&column.name) {
                Some(seen) => {
                    if table.columns[seen].ty != column.ty {
                        table.columns[seen].ty = "variant".to_string();
                    }
                    seen
                }
                None => {
                    table.columns.push(column.clone());
                    table.columns.len() - 1
                }
            })
            .collect();
        table.rows.extend(batch.rows.iter().map(|row| {
            let mut values = vec![serde_json::Value::Null; table.columns.len()];
            for (value, &to) in row.values.iter().zip(&at) {
                values[to] = value.clone();
            }
            crate::heartbeat::CapturedRow {
                values,
                failed: row.failed.iter().map(|&c| at[c]).collect(),
                worker: row.worker.clone(),
            }
        }));
    }
    let width = table.columns.len();
    for row in &mut table.rows {
        row.values.resize(width, serde_json::Value::Null);
    }
    table
}

/// Set `value` at `path`, a record field's name, in `object`: under each of its keys in turn (`plan::tree::segments`),
/// as `io::value_at` reads it back.
pub fn put_path(object: &mut serde_json::Map<String, serde_json::Value>, path: &str, value: serde_json::Value) {
    let keys: Vec<_> = crate::plan::tree::segments(path).collect();
    let Some((last, parents)) = keys.split_last() else {
        return;
    };
    let mut at = object;
    for key in parents {
        let inner = at
            .entry(key.as_ref())
            .or_insert_with(|| serde_json::Value::Object(Default::default()));
        let serde_json::Value::Object(inner) = inner else {
            return;
        };
        at = inner;
    }
    at.insert(last.to_string(), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::DirFileSet;
    use crate::heartbeat::{CaptureBatch, CaptureColumn, CapturedRow};

    fn temp_bundle(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-sample-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(SAMPLES_DIR)).unwrap();
        dir
    }

    fn write(dir: &std::path::Path, name: &str, body: &str) {
        std::fs::write(dir.join(SAMPLES_DIR).join(name), body).unwrap();
    }

    #[test]
    fn a_sample_at_the_limit_is_stored_and_one_past_it_is_refused() {
        assert!(refuse_sample_size(1).is_ok());
        assert!(refuse_sample_size(MAX_SAMPLE_RECORDS).is_ok());
        let refused = refuse_sample_size(MAX_SAMPLE_RECORDS + 1).unwrap_err();
        // The author is told the limit and what they actually have, since
        // "too big" alone does not say how much to cut.
        assert!(refused.contains(&MAX_SAMPLE_RECORDS.to_string()), "{refused}");
        assert!(refused.contains(&(MAX_SAMPLE_RECORDS + 1).to_string()), "{refused}");
    }

    #[test]
    fn a_sample_of_no_records_is_refused() {
        assert!(refuse_sample_size(0).is_err());
    }

    #[test]
    fn a_sample_already_larger_than_the_limit_still_reads() {
        // The limit is on writing. A bundle holding an older, larger sample
        // lists and opens rather than taking the library down with it.
        let dir = temp_bundle("oversized-reads");
        let records = (0..MAX_SAMPLE_RECORDS + 5).map(|i| crate::io::raw_object(format!("line {i}")));
        let big = SampleDto {
            id: "big".to_string(),
            name: "big".to_string(),
            source: None,
            table: CaptureBatch::pasted(records.collect()),
        };
        write(&dir, "big.json", &String::from_utf8(sample_bytes(&big)).unwrap());
        let (found, unreadable) = list_samples(&mut DirFileSet { root: &dir });
        assert!(unreadable.is_empty(), "{unreadable:?}");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].records, MAX_SAMPLE_RECORDS + 5);
    }

    fn column(name: &str, ty: &str) -> CaptureColumn {
        CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        }
    }

    fn row(values: serde_json::Value, failed: &[usize]) -> CapturedRow {
        CapturedRow {
            values: values.as_array().unwrap().clone(),
            failed: failed.to_vec(),
            worker: None,
        }
    }

    #[test]
    fn a_sample_round_trips_through_its_file() {
        let dir = temp_bundle("roundtrip");
        let sample = SampleDto {
            id: "orders-west, ordinary hour".to_string(),
            name: "orders-west, ordinary hour".to_string(),
            source: Some("orders-west".to_string()),
            table: CaptureBatch {
                columns: vec![
                    column("_raw", "str"),
                    column("__source.offset", "i64"),
                    column("__capture.timestamp", "datetime"),
                ],
                rows: vec![
                    row(
                        serde_json::json!([r#"{"a":1}"#, 4_120_880, "2026-10-25T13:57:12.000000Z"]),
                        &[],
                    ),
                    row(
                        serde_json::json!(["caf\u{fffd}", 4_120_881, "2026-10-25T13:57:12.000000Z"]),
                        &[0],
                    ),
                ],
            },
        };
        let path = sample_path(&sample.id).unwrap();
        std::fs::write(dir.join(&path), sample_bytes(&sample)).unwrap();

        let read = read_sample(&mut DirFileSet { root: &dir }, &sample.id)
            .unwrap()
            .unwrap();
        assert_eq!(read.name, sample.name);
        assert_eq!(read.source.as_deref(), Some("orders-west"));
        assert_eq!(read.table, sample.table, "every cell, and which of them failed");
        assert_eq!(
            read.summary().captured.as_deref(),
            Some("2026-10-25T13:57:12.000000Z"),
            "a listing says when a captured sample was taken"
        );
    }

    /// Pasted records are a table of one column, which holds each whole.
    #[test]
    fn pasted_records_are_stored_whole() {
        let sample = SampleDto {
            id: "p".to_string(),
            name: "p".to_string(),
            source: None,
            table: CaptureBatch::pasted(vec![crate::io::raw_object("{}")]),
        };
        assert_eq!(
            serde_json::to_value(&sample).unwrap(),
            serde_json::json!({"id": "p", "name": "p", "columns": [{"name": "...", "type": "variant"}],
                "rows": [{"values": [{"_raw": "{}"}]}]})
        );
    }

    #[test]
    fn a_malformed_sample_is_reported_beside_the_rest() {
        let dir = temp_bundle("malformed");
        write(
            &dir,
            "good.json",
            r#"{"id":"good","name":"good","columns":[{"name":"...","type":"variant"}],"rows":[{"values":[{"_raw":"{}"}]}]}"#,
        );
        write(&dir, "bad.json", "{not json");

        let (found, unreadable) = list_samples(&mut DirFileSet { root: &dir });
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].records, 1);
        assert_eq!(unreadable.len(), 1);
        assert_eq!(unreadable[0].0, format!("{SAMPLES_DIR}/bad.json"));
    }

    /// A `FileSet` whose directory listing always fails, for exercising that path without depending on filesystem
    /// permissions.
    struct UnlistableFileSet;

    impl FileSet for UnlistableFileSet {
        fn read(&mut self, _path: &str) -> std::io::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn json_paths_under(&mut self, _dir: &str) -> std::io::Result<Vec<String>> {
            Err(std::io::Error::other("permission denied"))
        }
    }

    /// A samples directory that cannot be listed is not the same as one with nothing in it, and must not be reported
    /// as such.
    #[test]
    fn a_listing_failure_is_reported_rather_than_read_as_no_samples() {
        let (found, unreadable) = list_samples(&mut UnlistableFileSet);
        assert!(found.is_empty());
        assert_eq!(unreadable.len(), 1);
        assert_eq!(unreadable[0].0, SAMPLES_DIR);
        assert!(unreadable[0].1.contains("permission denied"), "{:?}", unreadable[0]);
    }

    #[test]
    fn an_id_with_nothing_to_slug_is_refused() {
        assert!(sample_path("...").is_err());
        assert_eq!(sample_path("Orders West!").unwrap(), "samples/orders-west.json");
    }

    /// A sample of a capture reads back as the capture held it: each cell under its column. A cell that failed
    /// fails again with its text; a value that failed as an open object is kept as that text, under none of the
    /// object's fields.
    #[test]
    fn a_sample_reads_back_as_its_capture_held_it() {
        let batch = CaptureBatch {
            columns: vec![
                column("id", "i64"),
                column("user.name", "str"),
                column("labels....", "{...}"),
                column("__source.offset", "i64"),
            ],
            rows: vec![
                row(serde_json::json!([1, "ann", {"env": "prod"}, 7]), &[]),
                row(serde_json::json!(["true", null, "\"oops\"", 8]), &[0, 2]),
            ],
        };
        let table = sample_table(std::slice::from_ref(&batch));
        assert_eq!(table, batch, "one batch is the table");
        let meta = crate::plan::topology::SourceMeta::of_columns(&table.columns).unwrap();
        let read = meta.read_table(&table).unwrap();
        assert_eq!(
            meta.record_object(&read[0], ""),
            *serde_json::json!({"id": 1, "user": {"name": "ann"}, "labels": {"env": "prod"}, "__source": {"offset": 7}})
                .as_object()
                .unwrap()
        );
        assert_eq!(
            read[1].chunk.failed(),
            [(1, 0, "true".to_string()), (1, 2, "\"oops\"".to_string())]
        );
        assert_eq!(read[1].offset, 8);
    }

    /// Batches of several routes save as one table: each column once, a row null where its batch had no such column,
    /// its failed cells still the ones that failed. A name the batches type two ways is a variant.
    #[test]
    fn a_sample_holds_every_batch_s_columns_once() {
        let mut first = CaptureBatch::new(vec![column("a", "i64"), column("b", "str")]);
        first.rows.push(row(serde_json::json!([1, "x"]), &[]));
        let mut second = CaptureBatch::new(vec![column("c", "f64"), column("a", "str")]);
        second.rows.push(row(serde_json::json!([0.5, "two"]), &[1]));
        let table = sample_table(&[first, second]);
        assert_eq!(
            table.columns,
            [column("a", "variant"), column("b", "str"), column("c", "f64")],
            "a variant holds either"
        );
        assert_eq!(
            table.rows,
            [
                row(serde_json::json!([1, "x", null]), &[]),
                row(serde_json::json!(["two", null, 0.5]), &[0]),
            ]
        );
    }
}
