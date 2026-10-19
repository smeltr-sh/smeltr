//! A test: a sample with the output locked, stored beside the config it is authored against.
//!
//! Two kinds share one shape -- `TestDto.pipeline` is the discriminant. `Some` locks one pipeline's rows over the
//! sample, run exactly as a pipeline preview runs it. `None` locks an end-to-end outcome instead: the sample routed
//! by its source (the route preview), which needs a sample naming one (`SampleDto.source`).
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

use super::bundle::{BundleError, FileSet, slugify};

/// Where tests live inside a bundle, beside `pipelines/` and `samples/`.
pub const TESTS_DIR: &str = "tests";

/// A pipeline's input and expected output, or a sample's end-to-end outcome, both fixed. `pipeline` is the
/// discriminant.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestDto {
    /// Stable across renames. The filename is its slug.
    pub id: String,
    pub name: String,
    /// The pipeline whose output is locked. `None` routes the sample by its source instead -- the route preview,
    /// which needs a sample naming one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// The sample it runs over, by id. Held by reference so the two stay one thing: editing the sample is visible
    /// as a test that no longer passes, rather than silently diverging from a copy.
    pub sample: String,
    /// The instant the test was first locked, in unix microseconds: what `now()` reads on every run of it, and on
    /// every re-lock, so a re-lock's diff holds only what really changed.
    pub locked_at: i64,
    /// Records the sample held when the output was locked. A sample that has since gained or lost records is a
    /// changed input, which the run reports rather than absorbs.
    pub rows_in: usize,
    /// A pipeline test: each document the pipeline produced, in order, as it would leave. `null` is a row the
    /// pipeline dropped.
    ///
    /// An end-to-end test: each document a route kept, in order, packed with where it went by
    /// [`TestDto::end_to_end_document`] -- never `null`, since a route that drops a record contributes no entry rather than
    /// one recording nothing. Compared the same way as a pipeline test: a change to what left the pipeline or to
    /// where it was sent is the same kind of difference.
    pub expected: Vec<Option<serde_json::Value>>,
}

/// The route and pipeline `TestDto::end_to_end_document` packed into `v`, or `None` when `v` is a pipeline test's
/// plain document instead.
fn end_to_end_route_and_pipeline(v: &serde_json::Value) -> Option<(&str, &str)> {
    Some((v.get("route")?.as_str()?, v.get("pipeline")?.as_str()?))
}

/// The distinct values `pick` reads off `expected`'s documents, in the order first seen.
fn dedup_field(
    expected: &[Option<serde_json::Value>],
    pick: impl Fn(&serde_json::Value) -> Option<&str>,
) -> Vec<String> {
    let mut seen = Vec::new();
    for v in expected.iter().flatten() {
        if let Some(s) = pick(v)
            && !seen.iter().any(|existing: &String| existing == s)
        {
            seen.push(s.to_string());
        }
    }
    seen
}

/// What a library listing shows: everything but the documents.
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
    /// Documents kept. Fewer than `rows_in` means a pipeline test dropped rows; an end-to-end test whose records
    /// matched no route looks the same, since neither stores a placeholder for what was never produced.
    pub rows_out: usize,
    /// Every pipeline this test exercises: what `pipeline` names, for a pipeline test; each matched route's
    /// pipeline, read off the locked outcomes, for an end-to-end test. A record can match more than one route, so this
    /// is a list even where `pipeline` names none.
    pub pipelines: Vec<String>,
    /// Every route the locked outcomes carry a document through. Empty for a pipeline test, which has no route.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<String>,
}

impl TestDto {
    /// Packs one document an end-to-end test kept with where it went, into what `expected` stores for it.
    /// `pipeline` and `route` are the pipeline the record ran through and the route that carried the result;
    /// `destinations` are the destination names it reached -- resolved through `RoutePreview::destination_names`
    /// at lock and at run, never the runtime `u32` ids those name, which shift when a destination is added or
    /// removed.
    pub fn end_to_end_document(
        document: serde_json::Value,
        pipeline: &str,
        route: &str,
        destinations: Vec<String>,
    ) -> serde_json::Value {
        serde_json::json!({
            "document": document,
            "pipeline": pipeline,
            "route": route,
            "destinations": destinations,
        })
    }

    /// Documents kept. A dropped row is a `None` and is not one.
    pub fn rows_out(&self) -> usize {
        self.expected.iter().filter(|d| d.is_some()).count()
    }

    /// Every pipeline this test exercises, in the order first seen.
    pub fn pipelines_covered(&self) -> Vec<String> {
        match &self.pipeline {
            Some(name) => vec![name.clone()],
            None => dedup_field(&self.expected, |v| end_to_end_route_and_pipeline(v).map(|(_, p)| p)),
        }
    }

    /// Every route this test's locked outcomes carry a document through, in the order first seen. Empty for a
    /// pipeline test.
    pub fn routes_covered(&self) -> Vec<String> {
        if self.pipeline.is_some() {
            return Vec::new();
        }
        dedup_field(&self.expected, |v| end_to_end_route_and_pipeline(v).map(|(r, _)| r))
    }

    pub fn summary(&self) -> TestSummary {
        TestSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            pipeline: self.pipeline.clone(),
            sample: self.sample.clone(),
            rows_in: self.rows_in,
            rows_out: self.rows_out(),
            pipelines: self.pipelines_covered(),
            routes: self.routes_covered(),
        }
    }
}

/// Where a run first stopped matching what a test locked. Either side is `None` past its own end.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Difference {
    /// Position in the output, counting from zero. What the storyboard calls a row.
    pub at: usize,
    /// What the test locked there. Absent when the run produced more documents than the test expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<serde_json::Value>,
    /// What the run produced there. Absent when the run produced fewer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<serde_json::Value>,
    /// Whether the test expected this row to be dropped, or the run dropped it.
    pub expected_dropped: bool,
    pub actual_dropped: bool,
}

/// What a run made of a test.
///
/// The first difference and the two counts, and no per-row tally. `Op::Unroll` changes how many rows one record
/// produces, so a config edit can shift every position after it; counting differences by position would then report
/// one record's change as every later row changing, which is a lie about what happened.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    pub passed: bool,
    /// Documents the test locks, dropped rows included.
    pub expected_rows: usize,
    /// Documents the run produced, dropped rows included.
    pub actual_rows: usize,
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

/// Compare a run against what a test locked. `rows_in` is how many records the sample holds now. `actual` is one
/// entry per row the run produced, `None` where the pipeline dropped it -- the shape a preview already answers in.
pub fn compare(test: &TestDto, rows_in: usize, actual: &[Option<serde_json::Value>]) -> Verdict {
    let mut first = None;
    for at in 0..test.expected.len().max(actual.len()) {
        let (want, got) = (test.expected.get(at), actual.get(at));
        if want.map(Option::as_ref) == got.map(Option::as_ref) {
            continue;
        }
        first = Some(Difference {
            at,
            expected: want.cloned().flatten(),
            actual: got.cloned().flatten(),
            expected_dropped: matches!(want, Some(None)),
            actual_dropped: matches!(got, Some(None)),
        });
        break;
    }
    // A record added that no route matches leaves an end-to-end test's output unchanged; only the count shows it.
    let rows_in = (rows_in != test.rows_in).then_some(RowsIn {
        locked: test.rows_in,
        now: rows_in,
    });
    Verdict {
        passed: first.is_none() && rows_in.is_none(),
        expected_rows: test.expected.len(),
        actual_rows: actual.len(),
        first_difference: first,
        rows_in,
    }
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

    fn temp_bundle(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-test-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(TESTS_DIR)).unwrap();
        dir
    }

    fn locked(id: &str, expected: Vec<Option<serde_json::Value>>) -> TestDto {
        TestDto {
            id: id.to_string(),
            name: id.to_string(),
            pipeline: Some("orders-normalize".to_string()),
            sample: "orders-west, ordinary hour".to_string(),
            locked_at: 1_757_142_012_000_000,
            rows_in: expected.len(),
            expected,
        }
    }

    /// An end-to-end test: `pipeline` names none, and `expected` packs where each document went.
    fn locked_end_to_end(id: &str, expected: Vec<Option<serde_json::Value>>) -> TestDto {
        TestDto {
            id: id.to_string(),
            name: id.to_string(),
            pipeline: None,
            sample: "orders-west, ordinary hour".to_string(),
            locked_at: 1_757_142_012_000_000,
            rows_in: expected.len(),
            expected,
        }
    }

    #[test]
    fn a_test_round_trips_through_its_file() {
        let dir = temp_bundle("roundtrip");
        let test = locked(
            "keeps the vendor discount field",
            vec![Some(serde_json::json!({"order_id": "A-1", "total": 48.2})), None],
        );
        let path = test_path(&test.id).unwrap();
        std::fs::write(dir.join(&path), test_bytes(&test)).unwrap();

        let read = read_test(&mut DirFileSet { root: &dir }, &test.id).unwrap().unwrap();
        assert_eq!(read.pipeline.as_deref(), Some("orders-normalize"));
        assert_eq!(read.locked_at, test.locked_at);
        assert_eq!(read.expected.len(), 2);
        assert_eq!(read.expected[1], None, "a dropped row is a null, not an absence");
    }

    fn doc(order: &str) -> Option<serde_json::Value> {
        Some(serde_json::json!({ "order_id": order }))
    }

    #[test]
    fn a_run_that_matches_passes() {
        let test = locked("same", vec![doc("A-1"), None, doc("A-3")]);
        let verdict = compare(&test, test.rows_in, &[doc("A-1"), None, doc("A-3")]);
        assert!(verdict.passed);
        assert_eq!((verdict.expected_rows, verdict.actual_rows), (3, 3));
        assert_eq!(verdict.first_difference, None);
    }

    /// The storyboard's own sentence: row 3 of 500 differs from what the test locks.
    #[test]
    fn a_changed_document_is_reported_where_it_is() {
        let test = locked("changed", vec![doc("A-1"), doc("A-2"), doc("A-3")]);
        let verdict = compare(&test, test.rows_in, &[doc("A-1"), doc("A-2"), doc("CHANGED")]);
        assert!(!verdict.passed);
        let d = verdict.first_difference.expect("it differs somewhere");
        assert_eq!(d.at, 2);
        assert_eq!(d.expected, doc("A-3"));
        assert_eq!(d.actual, doc("CHANGED"));
        assert!(!d.expected_dropped && !d.actual_dropped);
    }

    /// A row the pipeline used to keep and now drops is a difference, and says which way round it went.
    #[test]
    fn a_row_that_started_being_dropped_says_so() {
        let test = locked("drops", vec![doc("A-1"), doc("A-2")]);
        let verdict = compare(&test, test.rows_in, &[doc("A-1"), None]);
        let d = verdict.first_difference.expect("it differs");
        assert_eq!(d.at, 1);
        assert!(!d.expected_dropped, "the test expected a document");
        assert!(d.actual_dropped, "and the run dropped the row");
        assert_eq!(d.actual, None);
    }

    /// A run that produces more rows than the test locks -- an unroll that grew -- reports the first row the test
    /// has nothing to say about, and both counts.
    #[test]
    fn a_longer_run_reports_the_counts_rather_than_a_per_row_tally() {
        let test = locked("shorter", vec![doc("A-1")]);
        let verdict = compare(&test, test.rows_in, &[doc("A-1"), doc("A-2"), doc("A-3")]);
        assert!(!verdict.passed);
        assert_eq!((verdict.expected_rows, verdict.actual_rows), (1, 3));
        let d = verdict.first_difference.expect("it differs");
        assert_eq!(d.at, 1);
        assert_eq!(d.expected, None, "the test locks nothing there");
        assert_eq!(d.actual, doc("A-2"));
    }

    /// The same, the other way: a run that produced fewer rows than the test expects.
    #[test]
    fn a_shorter_run_reports_where_it_ran_out() {
        let test = locked("longer", vec![doc("A-1"), doc("A-2")]);
        let verdict = compare(&test, test.rows_in, &[doc("A-1")]);
        let d = verdict.first_difference.expect("it differs");
        assert_eq!(d.at, 1);
        assert_eq!(d.expected, doc("A-2"));
        assert_eq!(d.actual, None);
        assert!(
            !d.actual_dropped,
            "the run produced no row here at all, which is not a drop"
        );
    }

    /// A sample that gained a record no route matches leaves the output as it was; the count alone tells.
    #[test]
    fn a_sample_that_changed_size_fails_with_both_counts() {
        let test = locked_end_to_end("grew", vec![doc("A-1")]);
        let verdict = compare(&test, 2, &[doc("A-1")]);
        assert!(!verdict.passed);
        assert_eq!(verdict.first_difference, None);
        assert_eq!(verdict.rows_in, Some(RowsIn { locked: 1, now: 2 }));

        let unchanged = compare(&test, 1, &[doc("A-1")]);
        assert!(unchanged.passed);
        assert_eq!(unchanged.rows_in, None, "an unchanged count is not reported");
    }

    /// A record matching two routes derives both pipelines, not one -- and the routes covered are the two that carried it, in the order they were locked.
    #[test]
    fn a_record_matching_two_routes_derives_both_pipelines() {
        let test = locked_end_to_end(
            "matches two",
            vec![
                Some(TestDto::end_to_end_document(
                    serde_json::json!({"order_id": "A-1"}),
                    "orders-normalize",
                    "r1",
                    vec!["es".to_string()],
                )),
                Some(TestDto::end_to_end_document(
                    serde_json::json!({"order_id": "A-1"}),
                    "orders-audit",
                    "r2",
                    vec!["audit-log".to_string()],
                )),
            ],
        );
        assert_eq!(test.pipelines_covered(), vec!["orders-normalize", "orders-audit"]);
        assert_eq!(test.routes_covered(), vec!["r1", "r2"]);
        let summary = test.summary();
        assert_eq!(summary.pipeline, None);
        assert_eq!(summary.pipelines, vec!["orders-normalize", "orders-audit"]);
        assert_eq!(summary.routes, vec!["r1", "r2"]);
    }

    /// A pipeline test names one pipeline and no route -- the ordinary case, unaffected by the split.
    #[test]
    fn a_pipeline_test_covers_only_the_pipeline_it_names() {
        let test = locked("plain", vec![doc("A-1")]);
        assert_eq!(test.pipelines_covered(), vec!["orders-normalize"]);
        assert!(test.routes_covered().is_empty());
    }

    #[test]
    fn a_malformed_test_is_reported_beside_the_rest() {
        let dir = temp_bundle("malformed");
        std::fs::write(
            dir.join(TESTS_DIR).join("good.json"),
            test_bytes(&locked("good", vec![Some(serde_json::json!({}))])),
        )
        .unwrap();
        std::fs::write(dir.join(TESTS_DIR).join("bad.json"), "{not json").unwrap();

        let (found, unreadable) = list_tests(&mut DirFileSet { root: &dir });
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].rows_in, 1);
        assert_eq!(unreadable.len(), 1);
        assert_eq!(unreadable[0].0, format!("{TESTS_DIR}/bad.json"));
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

    /// A test locked before `pipeline` became optional: the field is a bare string, exactly what every test on disk
    /// holds today. It still deserializes as `Some`, the shape test_api.rs's fuller test exercises end to end.
    #[test]
    fn a_narrow_test_file_written_before_the_split_still_deserializes() {
        let old = r#"{
          "id": "keeps the vendor discount field",
          "name": "keeps the vendor discount field",
          "pipeline": "orders-normalize",
          "sample": "orders-west, ordinary hour",
          "lockedAt": 1757142012000000,
          "rowsIn": 2,
          "expected": [{"order_id": "A-1", "total": 48.2}, null]
        }"#;
        let test: TestDto = serde_json::from_str(old).unwrap();
        assert_eq!(test.pipeline.as_deref(), Some("orders-normalize"));
        assert_eq!(test.expected.len(), 2);
        assert_eq!(test.expected[1], None);
        assert_eq!(test.pipelines_covered(), vec!["orders-normalize"]);
    }
}
