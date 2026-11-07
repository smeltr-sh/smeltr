//! A sample: records under a name, stored beside the config they are authored against.
//!
//! A bundle file that only the leader reads. `load_bundle_from` reads the files it names and every pipeline; a
//! sample is neither, so it travels with the config in one history and one access path without ever reaching a
//! worker.
//!
//! Records are record objects, as their source produced them. Whoever produced a byte record resolved its bytes -- a
//! paste arrives as text already, and a capture decodes with its source's encoding -- so `_raw` is filled from a sample
//! without decoding twice.

use serde::{Deserialize, Serialize};

use super::bundle::{BundleError, FileSet, slugify};
use crate::plan::tree::UNCLAIMED;

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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SampleDto {
    /// Stable across renames. The filename is its slug.
    pub id: String,
    pub name: String,
    /// The source these records stand for. Routing selects on a source, so a sample naming none can be run through
    /// a pipeline but not through a route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The columns the records were captured under, each named and typed once: the record's fields, then its
    /// `__source` fields, at a destination its `__destination` fields, and then its `__capture` fields. A sample with
    /// none is `_raw` text, as a paste is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<crate::heartbeat::CaptureColumn>,
    /// Each record as the root object a pipeline reads, `$`: every field its source produced, and under `__source`
    /// every field the source it was read from set (`plan::topology::SourceMeta`). A byte source's record is
    /// `{"_raw": text}`. A pasted record is that, and was read from nowhere, so it has no `__source`. A record captured
    /// at a destination is the row its pipeline finished, with its `__destination` fields. A captured record has under
    /// `__capture` how its capture was taken (`leader::capture::Captures::capture_fields`), and a record of a pipeline's
    /// output how that output was made (`leader::capture::output_fields`). A value that failed its type is stored as
    /// the source spelled it, its field listed under `io::MISTYPED_FIELDS` (`io::take_mistyped`), and fails again when
    /// the sample is read.
    pub records: Vec<crate::io::RecordObject>,
}

/// `record`'s `__source` fields, empty for a record read from nowhere.
pub fn record_source(record: &crate::io::RecordObject) -> crate::io::SourceFields {
    match record.get("__source") {
        Some(serde_json::Value::Object(source)) => source.clone(),
        _ => Default::default(),
    }
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
    /// When its records were captured: the first record's `__capture.timestamp`. A sample pasted or uploaded has none.
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
            records: self.records.len(),
            written: None,
            captured: self
                .records
                .first()
                .and_then(|r| r.get("__capture")?.get("timestamp")?.as_str())
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

/// A batch's rows as a sample stores them: each column in the record by its path, `__source` and `__destination`
/// columns included, and the keys of an open record's unclaimed column (`UNCLAIMED`) back in the record. A null is a
/// field the row did not have. A failed value is its original text, and the record lists its field under
/// `io::MISTYPED_FIELDS` (`io::take_mistyped`), so a preview of the sample fails it as the pipeline did.
pub fn sample_records(batch: &crate::heartbeat::CaptureBatch) -> impl Iterator<Item = crate::io::RecordObject> + '_ {
    batch.rows.iter().map(|row| {
        let mut record = crate::io::RecordObject::new();
        for (column, value) in batch.columns.iter().zip(&row.values) {
            if value.is_null() {
                continue;
            }
            match (column.name.as_str(), value) {
                (UNCLAIMED, serde_json::Value::Object(keys)) => record.extend(keys.clone()),
                _ => put_path(&mut record, &column.name, value.clone()),
            }
        }
        if !row.failed.is_empty() {
            let names = row
                .failed
                .iter()
                .map(|&c| batch.columns[c].name.clone().into())
                .collect();
            put_path(&mut record, crate::io::MISTYPED_FIELDS, serde_json::Value::Array(names));
        }
        record
    })
}

/// The one set of columns a sample of `batches` holds: each column of every batch, in the order first seen. An open
/// record's unclaimed column (`UNCLAIMED`) becomes one column for each key its rows hold, typed as a variant, which
/// holds a value of any type. Batches differ where workers run different builds, or where a destination is fed by
/// several routes; a name they type differently becomes a variant.
pub fn sample_columns(batches: &[crate::heartbeat::CaptureBatch]) -> Vec<crate::heartbeat::CaptureColumn> {
    let unclaimed = |batch: &crate::heartbeat::CaptureBatch, at: usize| {
        let mut keys: Vec<crate::heartbeat::CaptureColumn> = Vec::new();
        for row in &batch.rows {
            if let Some(serde_json::Value::Object(object)) = row.values.get(at) {
                for key in object.keys().map(|k| crate::plan::tree::spell(k).into_owned()) {
                    if !keys.iter().any(|c| c.name == key) {
                        keys.push(crate::heartbeat::CaptureColumn {
                            name: key,
                            ty: "variant".to_string(),
                        });
                    }
                }
            }
        }
        keys
    };
    let each = batches.iter().flat_map(|b| {
        b.columns
            .iter()
            .enumerate()
            .flat_map(move |(at, c)| match c.name == UNCLAIMED {
                true => unclaimed(b, at),
                false => vec![c.clone()],
            })
    });
    let mut columns: Vec<crate::heartbeat::CaptureColumn> = Vec::new();
    for column in each {
        match columns.iter_mut().find(|c| c.name == column.name) {
            None => columns.push(column),
            Some(seen) if seen.ty != column.ty => seen.ty = "variant".to_string(),
            Some(_) => {}
        }
    }
    columns
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
        let records: Vec<String> = (0..MAX_SAMPLE_RECORDS + 5)
            .map(|i| format!(r#"{{"_raw":"line {i}"}}"#))
            .collect();
        write(
            &dir,
            "big.json",
            &format!(r#"{{"id":"big","name":"big","records":[{}]}}"#, records.join(",")),
        );
        let (found, unreadable) = list_samples(&mut DirFileSet { root: &dir });
        assert!(unreadable.is_empty(), "{unreadable:?}");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].records, MAX_SAMPLE_RECORDS + 5);
    }

    #[test]
    fn a_sample_round_trips_through_its_file() {
        let dir = temp_bundle("roundtrip");
        let sample = SampleDto {
            id: "orders-west, ordinary hour".to_string(),
            name: "orders-west, ordinary hour".to_string(),
            source: Some("orders-west".to_string()),
            records: vec![
                serde_json::json!({"_raw": r#"{"a":1}"#, "__source": {"name": "orders-west",
                    "topic": "orders", "offset": 4_120_880, "headers": {"trace": "t1"}},
                    "__capture": {"timestamp": "2026-10-25T13:57:12.000000Z", "source": "orders-west", "group": "west"}})
                .as_object()
                .unwrap()
                .clone(),
            ],
            columns: Vec::new(),
        };
        let path = sample_path(&sample.id).unwrap();
        std::fs::write(dir.join(&path), sample_bytes(&sample)).unwrap();

        let read = read_sample(&mut DirFileSet { root: &dir }, &sample.id)
            .unwrap()
            .unwrap();
        assert_eq!(read.name, sample.name);
        assert_eq!(read.source.as_deref(), Some("orders-west"));
        assert_eq!(read.records[0], sample.records[0]);
        assert_eq!(
            read.summary().captured.as_deref(),
            Some("2026-10-25T13:57:12.000000Z"),
            "a listing says when a captured sample was taken"
        );
    }

    /// A record is stored as `$` is, and a pasted one was read from nowhere, so it has no `__source` at all.
    #[test]
    fn a_pasted_record_is_stored_as_itself_with_no_source_fields() {
        let sample: SampleDto = serde_json::from_str(r#"{"id":"p","name":"p","records":[{"_raw":"{}"}]}"#).unwrap();
        assert!(record_source(&sample.records[0]).is_empty());
        let json = serde_json::to_string(&sample.records).unwrap();
        assert_eq!(json, r#"[{"_raw":"{}"}]"#);
    }

    #[test]
    fn a_malformed_sample_is_reported_beside_the_rest() {
        let dir = temp_bundle("malformed");
        write(
            &dir,
            "good.json",
            r#"{"id":"good","name":"good","records":[{"_raw":"{}"}]}"#,
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

    /// A source batch saves as the sample records a paste would make: its fields by path in the record, its
    /// `__source` columns under its `__source`, a field the row did not have left out, and a failed value as its
    /// original text, its field listed under `io::MISTYPED_FIELDS`.
    #[test]
    fn a_source_batch_saves_as_sample_records() {
        use crate::heartbeat::{CaptureBatch, CaptureColumn, CapturedRow};
        let column = |name: &str, ty: &str| CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let batch = CaptureBatch {
            columns: vec![
                column("user.name", "str"),
                column(r#"labels["a.b"]"#, "i64"),
                column("id", "i64"),
                column("__source.offset", "i64"),
            ],
            rows: vec![CapturedRow {
                values: vec![
                    serde_json::json!("ann"),
                    serde_json::Value::Null,
                    serde_json::json!("true"),
                    serde_json::json!(7),
                ],
                failed: vec![2],
                worker: Some("w1".to_string()),
            }],
        };
        let saved: Vec<_> = sample_records(&batch).collect();
        assert_eq!(
            serde_json::to_value(&saved).unwrap(),
            serde_json::json!([{"user": {"name": "ann"}, "id": "true", "__source": {"offset": 7},
                "smeltr": {"mistyped": ["id"]}}])
        );
        let mut one = batch.clone();
        one.rows[0].values[1] = serde_json::json!(2);
        let saved = sample_records(&one).next().unwrap();
        assert_eq!(
            saved["labels"],
            serde_json::json!({"a.b": 2}),
            "a quoted key is one key"
        );
    }

    /// A destination batch saves as one object per row: its fields, its `__source` and `__destination` columns, and an
    /// open record's unclaimed keys back in it, as in the document as sent to a destination.
    #[test]
    fn a_destination_batch_saves_its_placement_in_the_record() {
        use crate::heartbeat::{CaptureBatch, CaptureColumn, CapturedRow};
        let column = |name: &str, ty: &str| CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let batch = CaptureBatch {
            columns: vec![
                column("n", "i64"),
                column(UNCLAIMED, "{...}"),
                column("__source.topic", "str"),
                column("__destination.route", "str"),
            ],
            rows: vec![CapturedRow {
                values: vec![
                    serde_json::json!(2),
                    serde_json::json!({"extra": "x"}),
                    serde_json::json!("t"),
                    serde_json::json!("orders"),
                ],
                failed: Vec::new(),
                worker: None,
            }],
        };
        let saved: Vec<_> = sample_records(&batch).collect();
        assert_eq!(
            serde_json::to_value(&saved).unwrap(),
            serde_json::json!([{"n": 2, "extra": "x", "__source": {"topic": "t"},
                "__destination": {"route": "orders"}}])
        );
        assert_eq!(
            sample_columns(&[batch])
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["n", "extra", "__source.topic", "__destination.route"],
            "each unclaimed key is a column of its own"
        );
    }

    /// Batches of several routes save as one set of columns, unless they type one name two ways.
    #[test]
    fn a_sample_holds_every_batch_s_columns_once() {
        use crate::heartbeat::{CaptureBatch, CaptureColumn};
        let batch = |columns: &[(&str, &str)]| {
            CaptureBatch::new(
                columns
                    .iter()
                    .map(|(name, ty)| CaptureColumn {
                        name: name.to_string(),
                        ty: ty.to_string(),
                    })
                    .collect(),
            )
        };
        let columns = sample_columns(&[
            batch(&[("a", "i64"), ("b", "str")]),
            batch(&[("a", "i64"), ("c", "f64")]),
        ]);
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        let typed_twice = sample_columns(&[batch(&[("a", "i64")]), batch(&[("a", "str")])]);
        assert_eq!(typed_twice[0].ty, "variant", "a variant holds either");
    }
}
