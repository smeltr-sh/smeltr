//! A test: a sample run through a pipeline, or routed end to end, and some of what it made checked.
//!
//! A test checks one thing: the fields it names, on the rows it names, and whether each of those rows was dropped.
//! Nothing else the run makes is held, so a change to another field or another row never fails it, and the file
//! says exactly what the test checks. Its fields and rows are what the records grid showed when it was locked
//! (`TestDto::fields`).
//!
//! Two kinds share one shape -- `TestDto.pipeline` is the discriminant. `Some` checks one pipeline's output rows over
//! the sample, run exactly as a pipeline preview runs it. `None` checks an end-to-end outcome instead: the sample
//! routed by its source (the route preview), which needs a sample naming one (`SampleDto.source`), with where each
//! document went.
//!
//! A bundle file only the leader reads, like a sample. `load_bundle_from` reads the files it names and every
//! pipeline; a test is neither, so it travels with the config in one history and never reaches a worker. Changing
//! what a test expects lands in the diff, which is what makes "change the config or change the test" a decision a
//! reviewer sees rather than a silent edit.
//!
//! What is locked is the documents, not the exceptions. `ExceptionKind::label` collapses every kind to one string
//! with no payload, and a preview reports exceptions a delivered row never carries -- a test locking those would
//! fail on noise nothing downstream sees.
//!
//! The instant is locked with them. `now()` and a reference-less `parseTime` read the clock, so an output is not a
//! function of the config alone; re-running against the instant the test was locked at is what makes two runs
//! comparable. A pipeline that reads no clock is unaffected by it.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::bundle::{BundleError, FileSet, slugify};

/// Where tests live inside a bundle, beside `pipelines/` and `samples/`.
pub const TESTS_DIR: &str = "tests";

/// A sample, what it is run through, and what the run made of the rows and fields the test checks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestDto {
    /// Stable across renames. The filename is its slug.
    pub id: String,
    pub name: String,
    /// The pipeline whose output is checked. `None` routes the sample by its source instead -- the route preview,
    /// which needs a sample naming one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// The sample it runs over, by id. Held by reference so the two stay one thing: editing the sample is visible
    /// as a test that no longer passes, rather than silently diverging from a copy.
    pub sample: String,
    /// The instant the test was first locked, in unix microseconds: what `now()` reads on every run of it, and on
    /// every lock again, so that diff holds only what really changed.
    pub locked_at: i64,
    /// Records the sample held when the test was locked. A sample that has since gained or lost records is a
    /// changed input, which the run reports rather than absorbs: it would also renumber the rows.
    pub rows_in: usize,
    /// The fields checked, each named as the records grid names a column: a leaf of the document, its keys joined
    /// by `plan::tree::push_key`.
    pub fields: Vec<String>,
    /// What the run made of each checked row, in the order it made them.
    pub rows: Vec<Outcome>,
}

/// One document a run made on a checked row, cut down to the test's fields, or the row making none.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Outcome {
    /// The row as the records grid numbers it: a pipeline test's output row, or an end-to-end test's record of the
    /// sample, which makes one document for each route that carries it.
    pub row: usize,
    /// Each of the test's fields the document holds, by name. A field it lacks is absent here too, so gaining it
    /// later is a difference. `None` is a row that made no document: a pipeline dropped it, or no route kept it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Map<String, Value>>,
    /// Where an end-to-end test's document went. `None` for a pipeline test, and for a row making no document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent: Option<Sent>,
}

/// Where a document went: the pipeline that made it, the route that carried it and the destinations it reached, by
/// their names -- never the runtime ids those name, which shift when a destination is added or removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Sent {
    pub pipeline: String,
    pub route: String,
    pub destinations: Vec<String>,
}

/// One document a run made, whole, before a test cuts it down: what a run hands [`compare`].
#[derive(Clone, Debug, PartialEq)]
pub struct Made {
    pub row: usize,
    /// `None` is a row that made no document.
    pub document: Option<Value>,
    pub sent: Option<Sent>,
}

impl Made {
    /// What `fields` checks of it.
    pub fn cut(&self, fields: &[String]) -> Outcome {
        let values = self.document.as_ref().map(|document| {
            let mut leaves = Map::new();
            leaves_of(document, &mut String::new(), &mut leaves);
            fields
                .iter()
                .filter_map(|f| leaves.remove(f).map(|v| (f.clone(), v)))
                .collect()
        });
        Outcome {
            row: self.row,
            values,
            sent: self.sent.clone(),
        }
    }
}

/// Every leaf of `value` under `path`, by its name, as the records grid names its columns: an object's keys are
/// walked, and anything else is a leaf -- an array, a scalar, and an empty object, which has no leaves of its own.
fn leaves_of(value: &Value, path: &mut String, into: &mut Map<String, Value>) {
    match value {
        Value::Object(object) if !object.is_empty() => {
            for (key, v) in object {
                let len = path.len();
                crate::plan::tree::push_key(path, key);
                leaves_of(v, path, into);
                path.truncate(len);
            }
        }
        _ if !path.is_empty() => {
            into.insert(path.clone(), value.clone());
        }
        _ => {}
    }
}

/// What locking a test asks for: a [`TestDto`] without what the run made. The leader makes that itself
/// ([`Lock::locked`]) from the same run a test run compares, so a test passes the moment it is locked.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Lock {
    pub name: String,
    /// `None` routes the sample by its source, as [`TestDto::pipeline`].
    #[serde(default)]
    pub pipeline: Option<String>,
    pub sample: String,
    pub locked_at: i64,
    /// The fields to check. `None` checks every field the run made.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// The rows to check, as the records grid numbers them. `None` checks every row the run made.
    #[serde(default)]
    pub rows: Option<Vec<usize>>,
}

/// What a lock holds of a run: [`TestDto::rows_in`], [`TestDto::fields`] and [`TestDto::rows`].
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Locked {
    pub rows_in: usize,
    pub fields: Vec<String>,
    pub rows: Vec<Outcome>,
}

impl Lock {
    /// What `made`, a run over a sample of `rows_in` records, holds of these fields and rows.
    pub fn locked(&self, rows_in: usize, made: &[Made]) -> Locked {
        let fields = self.fields.clone().unwrap_or_else(|| {
            let mut leaves = Map::new();
            for document in made.iter().filter_map(|m| m.document.as_ref()) {
                leaves_of(document, &mut String::new(), &mut leaves);
            }
            leaves.into_iter().map(|(field, _)| field).collect()
        });
        let rows = self
            .rows
            .clone()
            .unwrap_or_else(|| made.iter().map(|m| m.row).collect());
        Locked {
            rows_in,
            rows: first_seen(rows)
                .into_iter()
                .flat_map(|row| made_on(row, &fields, made))
                .collect(),
            fields,
        }
    }

    /// The test `id` holding `locked`.
    pub fn test(self, id: String, locked: Locked) -> TestDto {
        TestDto {
            id,
            name: self.name,
            pipeline: self.pipeline,
            sample: self.sample,
            locked_at: self.locked_at,
            rows_in: locked.rows_in,
            fields: locked.fields,
            rows: locked.rows,
        }
    }
}

/// Each of `items` once, in the order first seen.
fn first_seen<T: PartialEq>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut seen = Vec::new();
    for item in items {
        if !seen.contains(&item) {
            seen.push(item);
        }
    }
    seen
}

/// What a library listing shows: everything but the outcomes.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestSummary {
    pub id: String,
    pub name: String,
    /// `None` names an end-to-end test.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    pub sample: String,
    pub rows_in: usize,
    /// The fields it checks.
    pub fields: Vec<String>,
    /// How many rows it checks.
    pub rows: usize,
    /// Every pipeline this test exercises: what `pipeline` names, for a pipeline test; each route's pipeline, read off
    /// the outcomes, for an end-to-end test. A record can match more than one route, so this is a list even where
    /// `pipeline` names none.
    pub pipelines: Vec<String>,
    /// Every route the outcomes carry a document through. Empty for a pipeline test, which has no route.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<String>,
}

impl TestDto {
    /// The rows it checks, each once, in order.
    pub fn rows_checked(&self) -> Vec<usize> {
        first_seen(self.rows.iter().map(|o| o.row))
    }

    /// Every pipeline this test exercises, in the order first seen.
    pub fn pipelines_covered(&self) -> Vec<String> {
        match &self.pipeline {
            Some(name) => vec![name.clone()],
            None => self.sent_names(|s| &s.pipeline),
        }
    }

    /// Every route this test's outcomes carry a document through, in the order first seen. Empty for a pipeline
    /// test.
    pub fn routes_covered(&self) -> Vec<String> {
        self.sent_names(|s| &s.route)
    }

    /// The distinct names `pick` reads off where the outcomes went, in the order first seen.
    fn sent_names(&self, pick: impl Fn(&Sent) -> &String) -> Vec<String> {
        first_seen(self.rows.iter().filter_map(|o| o.sent.as_ref()).map(pick).cloned())
    }

    pub fn summary(&self) -> TestSummary {
        TestSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            pipeline: self.pipeline.clone(),
            sample: self.sample.clone(),
            rows_in: self.rows_in,
            fields: self.fields.clone(),
            rows: self.rows_checked().len(),
            pipelines: self.pipelines_covered(),
            routes: self.routes_covered(),
        }
    }
}

/// Where a run first stopped matching what a test locked: on a checked row, what the test expects and what the run
/// made, one document at a time. Either side is `None` past its own end -- an end-to-end row whose record now goes
/// through more routes, or fewer, and a pipeline row the run no longer reaches.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Difference {
    pub row: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<Outcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<Outcome>,
}

/// What a run made of a test.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_difference: Option<Difference>,
    /// Present when the sample no longer holds the records it held at lock. The input changed, so the test fails
    /// whatever the output says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_in: Option<RowsIn>,
}

/// How many records a test's sample held when it was locked, and how many it holds now.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct RowsIn {
    pub locked: usize,
    pub now: usize,
}

/// Compare a run against what a test locked. `rows_in` is how many records the sample holds now. `made` is every
/// document the run made, rows it made none on included -- the shape a preview already answers in.
///
/// Row by checked row, in the test's order: a row's documents are compared in the order they were made, each cut
/// down to the test's fields; a row the run never reached made nothing, like a dropped one. A row the test does not
/// check is never read.
pub fn compare(test: &TestDto, rows_in: usize, made: &[Made]) -> Verdict {
    let first = test.rows_checked().into_iter().find_map(|row| {
        let mut want = test.rows.iter().filter(|o| o.row == row).cloned();
        let mut got = made_on(row, &test.fields, made).into_iter();
        loop {
            match (want.next(), got.next()) {
                (None, None) => return None,
                (Some(w), Some(g)) if w.matches(&g) => continue,
                (expected, actual) => {
                    return Some(Difference { row, expected, actual });
                }
            }
        }
    });
    let rows_in = (rows_in != test.rows_in).then_some(RowsIn {
        locked: test.rows_in,
        now: rows_in,
    });
    Verdict {
        passed: first.is_none() && rows_in.is_none(),
        first_difference: first,
        rows_in,
    }
}

/// What a test checks of `row`: each document the run made on it, in order, cut down to `fields`. A row the run never
/// reached made nothing, as a dropped row did, so locking it again keeps it checked.
fn made_on(row: usize, fields: &[String], made: &[Made]) -> Vec<Outcome> {
    let got: Vec<Outcome> = made.iter().filter(|m| m.row == row).map(|m| m.cut(fields)).collect();
    if got.is_empty() {
        return vec![
            Made {
                row,
                document: None,
                sent: None,
            }
            .cut(fields),
        ];
    }
    got
}

impl Outcome {
    /// Whether a run's `other` makes what this locked: the same row, sent the same way, holding the same fields with
    /// [`same`] values.
    fn matches(&self, other: &Outcome) -> bool {
        self.row == other.row
            && self.sent == other.sent
            && match (&self.values, &other.values) {
                (Some(a), Some(b)) => same_members(a, b),
                (a, b) => a == b,
            }
    }
}

/// How far apart two floats may be, relative to the larger, and still be one value. A float's last bits depend on the
/// order its arithmetic ran in, which a test does not check.
const FLOAT_TOLERANCE: f64 = 1e-9;

/// Whether `a` and `b` are one value to a test: two integers must be equal, so ids that differ in the last digit are
/// two values; any other two numbers are one value within [`FLOAT_TOLERANCE`]; arrays and objects compare member by
/// member.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) if x.is_f64() || y.is_f64() => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y || (x - y).abs() <= FLOAT_TOLERANCE * x.abs().max(y.abs()),
            _ => false,
        },
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y)),
        (Value::Object(x), Value::Object(y)) => same_members(x, y),
        _ => a == b,
    }
}

/// Whether `a` and `b` hold the same keys with [`same`] values.
fn same_members(a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| same(v, w)))
}

/// The file `id` is stored in, relative to the bundle.
///
/// An id with no alphanumeric character has no slug, and is refused rather than given a shared fallback name: two
/// such ids would name one file, and the second save would silently replace the first.
pub fn test_path(id: &str) -> Result<String, BundleError> {
    if !id.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(BundleError::Io {
            path: format!("{TESTS_DIR}/"),
            message: format!("a test id needs at least one letter or digit; {id:?} has none"),
        });
    }
    Ok(format!("{TESTS_DIR}/{}.json", slugify(id)))
}

/// A DTO is plain data; serializing it cannot fail.
pub fn test_bytes(test: &TestDto) -> Vec<u8> {
    serde_json::to_vec_pretty(test).expect("a test always serializes")
}

/// The test stored under `id`, or `None` if the bundle holds no such file.
pub fn read_test(files: &mut dyn FileSet, id: &str) -> Result<Option<TestDto>, BundleError> {
    let path = test_path(id)?;
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

/// Every test in the bundle, and every path under `tests/` that did not read, paired with why.
///
/// Lenient like every other read the GUI makes: one malformed test is reported beside the rest rather than emptying
/// the library, and a directory that could not be listed at all is reported under `TESTS_DIR` itself -- otherwise a
/// broken listing and an empty one both come back as "no tests", and only one of those is true.
pub fn list_tests(files: &mut dyn FileSet) -> (Vec<TestSummary>, Vec<(String, String)>) {
    let mut found = Vec::new();
    let mut unreadable = Vec::new();
    let paths = match files.json_paths_under(TESTS_DIR) {
        Ok(paths) => paths,
        Err(e) => {
            unreadable.push((TESTS_DIR.to_string(), e.to_string()));
            return (found, unreadable);
        }
    };
    for path in paths {
        match files.read(&path) {
            Ok(Some(bytes)) => match serde_json::from_slice::<TestDto>(&bytes) {
                Ok(test) => found.push(test.summary()),
                Err(e) => unreadable.push((path, e.to_string())),
            },
            Ok(None) => {}
            Err(e) => unreadable.push((path, e.to_string())),
        }
    }
    found.sort_by(|a, b| a.name.cmp(&b.name));
    (found, unreadable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::DirFileSet;
    use serde_json::json;

    fn temp_bundle(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-test-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(TESTS_DIR)).unwrap();
        dir
    }

    /// A pipeline test checking `fields` on what `made` holds of the rows it names.
    fn locked(id: &str, fields: &[&str], made: &[Made]) -> TestDto {
        let fields: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
        TestDto {
            id: id.to_string(),
            name: id.to_string(),
            pipeline: Some("orders-normalize".to_string()),
            sample: "orders-west, ordinary hour".to_string(),
            locked_at: 1_757_142_012_000_000,
            rows_in: 3,
            rows: made.iter().map(|m| m.cut(&fields)).collect(),
            fields,
        }
    }

    fn row(row: usize, document: Value) -> Made {
        Made {
            row,
            document: Some(document),
            sent: None,
        }
    }

    fn dropped(row: usize) -> Made {
        Made {
            row,
            document: None,
            sent: None,
        }
    }

    fn sent(row: usize, document: Value, pipeline: &str, route: &str) -> Made {
        Made {
            row,
            document: Some(document),
            sent: Some(Sent {
                pipeline: pipeline.to_string(),
                route: route.to_string(),
                destinations: vec!["es".to_string()],
            }),
        }
    }

    /// An Unroll that makes fewer rows leaves a checked row unreached: it fails there, and locked again it checks
    /// that the row makes nothing rather than checking one row fewer.
    #[test]
    fn a_checked_row_the_run_never_reaches_fails_and_locks_again_as_making_nothing() {
        let test = locked("ids", &["id"], &[row(0, json!({"id": 1})), row(1, json!({"id": 2}))]);
        let shrunk = [row(0, json!({"id": 1}))];
        let verdict = compare(&test, test.rows_in, &shrunk);
        assert_eq!(verdict.first_difference.as_ref().map(|d| d.row), Some(1));
        assert!(verdict.first_difference.unwrap().actual.unwrap().values.is_none());

        let again = TestDto {
            rows: vec![shrunk[0].cut(&test.fields), dropped(1).cut(&test.fields)],
            ..test
        };
        assert!(compare(&again, again.rows_in, &shrunk).passed);
        assert!(
            !compare(
                &again,
                again.rows_in,
                &[row(0, json!({"id": 1})), row(1, json!({"id": 2}))]
            )
            .passed
        );
    }

    /// A float's last bits depend on the order its arithmetic ran in, and a float with no fraction is the integer it
    /// equals; an integer is exact, however long.
    #[test]
    fn floats_match_within_a_tolerance_and_integers_exactly() {
        let test = locked(
            "numbers",
            &["f", "whole", "id", "list"],
            &[row(
                0,
                json!({"f": 0.3, "whole": 10.0, "id": 9_007_199_254_740_993_u64, "list": [0.1, {"g": 2.5}]}),
            )],
        );
        let near = [row(
            0,
            json!({"f": 0.1 + 0.2, "whole": 10, "id": 9_007_199_254_740_993_u64, "list": [0.1 + 1e-17, {"g": 2.5}]}),
        )];
        assert!(compare(&test, test.rows_in, &near).passed);
        for far in [
            json!({"f": 0.300_001, "whole": 10.0, "id": 9_007_199_254_740_993_u64, "list": [0.1, {"g": 2.5}]}),
            json!({"f": 0.3, "whole": 10.0, "id": 9_007_199_254_740_992_u64, "list": [0.1, {"g": 2.5}]}),
            json!({"f": 0.3, "whole": 10.0, "id": 9_007_199_254_740_993_u64, "list": [0.1, {"g": 2.6}]}),
            json!({"f": "0.3", "whole": 10.0, "id": 9_007_199_254_740_993_u64, "list": [0.1, {"g": 2.5}]}),
        ] {
            assert!(!compare(&test, test.rows_in, &[row(0, far.clone())]).passed, "{far}");
        }
    }

    /// A lock holds what the run made of the fields and rows it names, each row once, and passes against that run.
    #[test]
    fn a_lock_holds_its_fields_and_rows_and_passes_its_own_run() {
        let made = [
            row(0, json!({"a": 1.5, "b": {"c": 2}})),
            dropped(1),
            row(2, json!({"a": 3.0})),
        ];
        let lock = |fields: Option<&[&str]>, rows: Option<Vec<usize>>| Lock {
            name: "n".to_string(),
            pipeline: Some("p".to_string()),
            sample: "s".to_string(),
            locked_at: 7,
            fields: fields.map(|f| f.iter().map(|f| f.to_string()).collect()),
            rows,
        };

        let chosen = lock(Some(&["a"]), Some(vec![2, 1, 2, 5])).locked(3, &made);
        assert_eq!(chosen.fields, ["a"]);
        assert_eq!(chosen.rows.iter().map(|o| o.row).collect::<Vec<_>>(), [2, 1, 5]);
        assert_eq!(
            chosen.rows[0].values,
            Some(json!({"a": 3.0}).as_object().unwrap().clone())
        );
        assert!(chosen.rows[1].values.is_none() && chosen.rows[2].values.is_none());

        let everything = lock(None, None);
        let test = everything.clone().test("t".to_string(), everything.locked(3, &made));
        assert_eq!(test.fields, ["a", "b.c"]);
        assert_eq!(test.rows_checked(), [0, 1, 2]);
        assert_eq!((test.rows_in, test.locked_at), (3, 7));
        assert!(compare(&test, 3, &made).passed);
    }

    #[test]
    fn a_test_round_trips_through_its_file() {
        let dir = temp_bundle("roundtrip");
        let test = locked(
            "keeps the vendor discount field",
            &["total"],
            &[row(0, json!({"order_id": "A-1", "total": 48.2})), dropped(1)],
        );
        let path = test_path(&test.id).unwrap();
        std::fs::write(dir.join(&path), test_bytes(&test)).unwrap();

        let read = read_test(&mut DirFileSet { root: &dir }, &test.id).unwrap().unwrap();
        assert_eq!(read.pipeline.as_deref(), Some("orders-normalize"));
        assert_eq!(read.locked_at, test.locked_at);
        assert_eq!(read.rows, test.rows);
        assert_eq!(
            read.rows[0].values,
            Some(Map::from_iter([("total".into(), json!(48.2))]))
        );
        assert_eq!(read.rows[1].values, None, "a dropped row holds no values");
    }

    /// The test holds only the fields it checks: a nested one by its name, a key the grid quotes the way it quotes it.
    #[test]
    fn a_document_is_cut_down_to_the_fields_the_grid_names() {
        let made = row(
            0,
            json!({"user": {"name": "ana", "id": 7}, "labels": {"app.kubernetes.io/name": "web"}, "tags": [1, 2], "x": {}}),
        );
        let fields: Vec<String> = [
            "user.name",
            r#"labels["app.kubernetes.io/name"]"#,
            "tags",
            "x",
            "absent",
        ]
        .iter()
        .map(|f| f.to_string())
        .collect();
        let values = made.cut(&fields).values.unwrap();
        assert_eq!(
            values,
            Map::from_iter([
                ("user.name".into(), json!("ana")),
                (r#"labels["app.kubernetes.io/name"]"#.into(), json!("web")),
                ("tags".into(), json!([1, 2])),
                ("x".into(), json!({})),
            ]),
            "an array and an empty object are leaves; a field the document lacks is absent"
        );
    }

    #[test]
    fn a_run_that_matches_passes() {
        let made = [row(0, json!({"a": 1})), dropped(1), row(2, json!({"a": 3}))];
        let test = locked("same", &["a"], &made);
        let verdict = compare(&test, 3, &made);
        assert!(verdict.passed);
        assert_eq!(verdict.first_difference, None);
    }

    /// A change to a field the test does not check, or to a row it does not check, never fails it.
    #[test]
    fn a_change_outside_what_the_test_checks_passes() {
        let test = locked(
            "severity",
            &["severity"],
            &[row(1, json!({"severity": "high", "port": 22}))],
        );
        let made = [
            row(0, json!({"severity": "CHANGED"})),
            row(1, json!({"severity": "high", "port": 2222, "new": true})),
        ];
        assert!(compare(&test, 3, &made).passed);
    }

    #[test]
    fn a_changed_field_is_reported_on_its_row() {
        let test = locked("severity", &["severity"], &[row(1, json!({"severity": "high"}))]);
        let verdict = compare(&test, 3, &[row(0, json!({})), row(1, json!({"severity": "critical"}))]);
        assert!(!verdict.passed);
        let d = verdict.first_difference.expect("it differs");
        assert_eq!(d.row, 1);
        assert_eq!(d.expected.unwrap().values.unwrap()["severity"], json!("high"));
        assert_eq!(d.actual.unwrap().values.unwrap()["severity"], json!("critical"));
    }

    /// A field the row lacked when locked is locked as absent: the row gaining it is a difference.
    #[test]
    fn a_field_the_row_gains_is_a_difference() {
        let test = locked("no user", &["user"], &[row(0, json!({"a": 1}))]);
        let verdict = compare(&test, 3, &[row(0, json!({"a": 1, "user": "ana"}))]);
        assert!(!verdict.passed);
    }

    /// A row the test locked as kept and the run drops is a difference, and the other way round.
    #[test]
    fn a_row_dropped_or_kept_against_the_lock_is_a_difference() {
        let kept = locked("kept", &["a"], &[row(0, json!({"a": 1}))]);
        let d = compare(&kept, 3, &[dropped(0)]).first_difference.expect("it differs");
        assert_eq!(d.actual.unwrap().values, None);

        let gone = locked("dropped", &[], &[dropped(2)]);
        assert!(compare(&gone, 3, &[dropped(2)]).passed);
        assert!(!compare(&gone, 3, &[row(2, json!({}))]).passed);
    }

    /// A row the run no longer reaches -- an unroll that made fewer rows -- has nothing on the run's side.
    #[test]
    fn a_row_the_run_does_not_reach_is_reported_as_making_nothing() {
        let test = locked("last", &["a"], &[row(4, json!({"a": 1}))]);
        let d = compare(&test, 3, &[row(0, json!({"a": 1}))])
            .first_difference
            .expect("it differs");
        assert_eq!(d.row, 4);
        assert!(d.expected.is_some() && d.actual.is_some_and(|a| a.values.is_none()));
    }

    /// An end-to-end row holds a document per route that carried its record; one route more is a difference.
    #[test]
    fn an_end_to_end_row_compares_every_document_its_record_made() {
        let one = [sent(0, json!({"a": 1}), "p1", "r1")];
        let test = TestDto {
            pipeline: None,
            ..locked("routed", &["a"], &one)
        };
        assert!(compare(&test, 3, &one).passed);
        let two = [
            sent(0, json!({"a": 1}), "p1", "r1"),
            sent(0, json!({"a": 1}), "p2", "r2"),
        ];
        let d = compare(&test, 3, &two).first_difference.expect("it differs");
        assert!(d.expected.is_none(), "the test locks nothing past the first route");
        assert_eq!(d.actual.unwrap().sent.unwrap().route, "r2");
    }

    /// A sample that gained a record leaves the checked rows as they were; the count alone tells.
    #[test]
    fn a_sample_that_changed_size_fails_with_both_counts() {
        let made = [row(0, json!({"a": 1}))];
        let test = locked("grew", &["a"], &made);
        let verdict = compare(&test, 4, &made);
        assert!(!verdict.passed);
        assert_eq!(verdict.first_difference, None);
        assert_eq!(verdict.rows_in, Some(RowsIn { locked: 3, now: 4 }));
        assert_eq!(
            compare(&test, 3, &made).rows_in,
            None,
            "an unchanged count is not reported"
        );
    }

    /// A record matching two routes covers both pipelines -- and both routes, in the order they were locked.
    #[test]
    fn a_record_matching_two_routes_covers_both_pipelines() {
        let test = TestDto {
            pipeline: None,
            ..locked(
                "matches two",
                &["order_id"],
                &[
                    sent(0, json!({"order_id": "A-1"}), "orders-normalize", "r1"),
                    sent(0, json!({"order_id": "A-1"}), "orders-audit", "r2"),
                ],
            )
        };
        let summary = test.summary();
        assert_eq!(summary.pipeline, None);
        assert_eq!(summary.pipelines, vec!["orders-normalize", "orders-audit"]);
        assert_eq!(summary.routes, vec!["r1", "r2"]);
        assert_eq!(summary.rows, 1, "two documents of one record are one row");
    }

    #[test]
    fn a_pipeline_test_covers_only_the_pipeline_it_names() {
        let test = locked("plain", &["a"], &[row(0, json!({"a": 1})), row(3, json!({"a": 2}))]);
        assert_eq!(test.pipelines_covered(), vec!["orders-normalize"]);
        assert!(test.routes_covered().is_empty());
        assert_eq!(test.summary().fields, vec!["a"]);
        assert_eq!(test.summary().rows, 2);
    }

    #[test]
    fn a_malformed_test_is_reported_beside_the_rest() {
        let dir = temp_bundle("malformed");
        std::fs::write(
            dir.join(TESTS_DIR).join("good.json"),
            test_bytes(&locked("good", &["a"], &[row(0, json!({}))])),
        )
        .unwrap();
        std::fs::write(dir.join(TESTS_DIR).join("bad.json"), "{not json").unwrap();

        let (found, unreadable) = list_tests(&mut DirFileSet { root: &dir });
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].rows_in, 3);
        assert_eq!(unreadable.len(), 1);
        assert_eq!(unreadable[0].0, format!("{TESTS_DIR}/bad.json"));
    }

    /// A test locked whole, before tests named what they check, does not read: it is listed as unreadable, where it
    /// can be deleted, rather than run as something it never said.
    #[test]
    fn a_test_locking_the_whole_output_does_not_read() {
        let old = r#"{
          "id": "keeps the vendor discount field",
          "name": "keeps the vendor discount field",
          "pipeline": "orders-normalize",
          "sample": "orders-west, ordinary hour",
          "lockedAt": 1757142012000000,
          "rowsIn": 2,
          "expected": [{"order_id": "A-1", "total": 48.2}, null]
        }"#;
        assert!(serde_json::from_str::<TestDto>(old).is_err());
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

    /// A tests directory that cannot be listed is not the same as one with nothing in it.
    #[test]
    fn a_listing_failure_is_reported_rather_than_read_as_no_tests() {
        let (found, unreadable) = list_tests(&mut UnlistableFileSet);
        assert!(found.is_empty());
        assert_eq!(unreadable.len(), 1);
        assert_eq!(unreadable[0].0, TESTS_DIR);
    }

    #[test]
    fn an_id_with_nothing_to_slug_is_refused() {
        assert!(test_path("...").is_err());
        assert_eq!(
            test_path("Keeps the vendor discount!").unwrap(),
            "tests/keeps-the-vendor-discount.json"
        );
    }
}
