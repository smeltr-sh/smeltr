//! The file destination (`io::files`): files open across batches, a commit on the interval, on the bytes held or at the
//! barrier, and a batch complete only once its commit lands. Against an in-memory store, a recording committer and a
//! partitioner by the row's text.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use smeltr::core::column::DataType;
use smeltr::io::files::{
    CommitError, Committer, DataFile, FileConfig, FileDestination, FileEncoder, FileFormat, FileRow, ParquetFormat,
    Partition, Partitioner, TAKE_BYTES, Unpartitioned,
};
use smeltr::io::parquet::{Chunk, Columns, Schema};
use smeltr::io::{BatchCompletion, Destination, Dispatched, IoError, OwnedRecord, Rejection, held_bytes};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::Compression;

const INTERVAL: Duration = Duration::from_millis(300);
const LONG: Duration = Duration::from_secs(60);
const DEADLINE: Duration = Duration::from_secs(10);

fn raw_schema() -> Schema {
    Schema::new(&DataType::Struct(vec![("_raw".into(), DataType::Str)], false)).unwrap()
}

/// `texts` as the rows of one `{_raw: str}` chunk, each one a record whose value is its text.
fn rows(schema: &Schema, texts: &[&str]) -> Vec<OwnedRecord> {
    let chunk = Arc::new(Chunk::raw(schema, texts.iter().copied()));
    texts
        .iter()
        .enumerate()
        .map(|(row, text)| OwnedRecord {
            value: text.as_bytes().to_vec(),
            payload: Payload::Text { field: "_raw".into() },
            columns: Some(Columns {
                chunk: chunk.clone(),
                row: row as u32,
            }),
            ..OwnedRecord::default()
        })
        .collect()
}

// --- the partitioner ----------------------------------------------------------

/// Partitions a row by the first character of its text: `k=<char>`. A text starting with `!` is refused. The chunk
/// keeps its columns to itself, so the text is looked up by chunk and row, as `partitioned_rows` registered it; a row
/// without columns goes to `k=?`, for the format to refuse.
struct ByFirstChar;

/// Every test row's text by its chunk's address and row. The rows hold their chunk alive, so its address names it
/// for as long as they do.
static TEXTS: LazyLock<Mutex<HashMap<(usize, u32), String>>> = LazyLock::new(Mutex::default);

fn key(columns: &Columns) -> (usize, u32) {
    (Arc::as_ptr(&columns.chunk) as usize, columns.row)
}

/// `rows`, with each row's text registered for `ByFirstChar`.
fn partitioned_rows(schema: &Schema, texts: &[&str]) -> Vec<OwnedRecord> {
    let rows = rows(schema, texts);
    let mut registry = TEXTS.lock().unwrap();
    for (row, text) in rows.iter().zip(texts) {
        registry.insert(key(row.columns.as_ref().unwrap()), text.to_string());
    }
    rows
}

impl Partitioner for ByFirstChar {
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String> {
        let Some(columns) = row.columns.as_ref() else {
            return Ok(Partition {
                part: 0,
                path: vec!["k=?".into()],
                values: vec![("k".into(), "?".into())],
            });
        };
        let text = TEXTS
            .lock()
            .unwrap()
            .get(&key(columns))
            .cloned()
            .expect("a row of a registered chunk");
        let first = text.chars().next().unwrap_or(' ');
        if first == '!' {
            return Err(format!("{text:?} starts with `!`, which the partitioner refuses"));
        }
        Ok(Partition {
            part: 0,
            path: vec![format!("k={first}")],
            values: vec![("k".into(), first.to_string())],
        })
    }
}

/// Partitions a row into the part its value's first byte names, `0` or `1`, under `p=<part>`. The value is the
/// row's text, so the part need not be looked up.
struct ByPartDigit;

impl Partitioner for ByPartDigit {
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String> {
        let part = (row.value[0] - b'0') as usize;
        Ok(Partition {
            part,
            path: vec![format!("p={part}")],
            values: Vec::new(),
        })
    }
}

// --- the committer ------------------------------------------------------------

#[derive(Clone, Copy)]
enum Answer {
    Ok,
    Conflict,
    Transient,
    Unknown,
}

#[derive(Default)]
struct Recorded {
    /// Every `commit` call's files, in order.
    commits: Vec<Vec<DataFile>>,
    /// Every path `file_path` named, in order.
    paths: Vec<String>,
}

type Record = Arc<Mutex<Recorded>>;

/// Names files `data/<partition>/<id>.parquet`, records each commit and answers it from the script, repeating the
/// script's last answer once it runs out; `Ok` for an empty script.
struct Recording {
    answers: VecDeque<Answer>,
    record: Record,
}

impl Committer for Recording {
    fn file_path(&self, partition: &Partition, id: &str) -> String {
        assert_eq!(id.len(), 32, "a 32-character id");
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()), "a hex id: {id}");
        let mut path = String::from("data");
        for segment in &partition.path {
            path.push('/');
            path.push_str(segment);
        }
        path.push_str(&format!("/{id}.parquet"));
        self.record.lock().unwrap().paths.push(path.clone());
        path
    }

    fn commit(&mut self, files: &[DataFile]) -> Result<(), CommitError> {
        self.record.lock().unwrap().commits.push(files.to_vec());
        let answer = match self.answers.len() {
            0 => Answer::Ok,
            1 => self.answers[0],
            _ => self.answers.pop_front().expect("some"),
        };
        match answer {
            Answer::Ok => Ok(()),
            Answer::Conflict => Err(CommitError::Conflict),
            Answer::Transient => Err(CommitError::Transient("the catalog is away".into())),
            Answer::Unknown => Err(CommitError::Unknown("the answer was lost".into())),
        }
    }
}

// --- the destination ----------------------------------------------------------

struct Table {
    d: FileDestination,
    store: Arc<InMemory>,
    runtime: Arc<tokio::runtime::Runtime>,
    record: Record,
}

fn config(schema: &Schema, partitioner: Arc<dyn Partitioner>) -> FileConfig {
    FileConfig {
        format: Arc::new(ParquetFormat::new(vec![schema.clone()], Compression::None.into())),
        partitioner,
        commit_interval: INTERVAL,
        max_file_bytes: 64 * 1024 * 1024,
        take_bytes: smeltr::io::files::TAKE_BYTES,
        buffer_bytes: 64 * 1024 * 1024,
        stall_after: Duration::from_secs(30),
        attributes: Attributes::new(),
        parts_in_flight: 4,
    }
}

fn table(config: FileConfig, answers: &[Answer]) -> Table {
    let store = Arc::new(InMemory::new());
    let runtime = smeltr::io::objects::runtime().unwrap();
    let record: Record = Arc::default();
    let d = FileDestination::new(
        "table",
        "d",
        config,
        store.clone(),
        runtime.clone(),
        Box::new(Recording {
            answers: answers.iter().copied().collect(),
            record: record.clone(),
        }),
    );
    Table {
        d,
        store,
        runtime,
        record,
    }
}

impl Table {
    /// Hands `rows` over as `batch`, which the destination takes in flight.
    fn deliver(&mut self, batch: u64, rows: &[OwnedRecord]) -> Vec<Rejection> {
        let (dispatched, refused) = self.d.deliver(batch, Cow::Borrowed(rows)).unwrap();
        assert_eq!(dispatched, Dispatched::InFlight);
        refused
    }

    /// Polls, as egress does, until a completion comes or `within` passes: what came.
    fn completions_within(&mut self, within: Duration) -> Result<Vec<BatchCompletion>, IoError> {
        let deadline = Instant::now() + within;
        loop {
            let got = self.d.poll_completions()?;
            if !got.is_empty() || Instant::now() >= deadline {
                return Ok(got);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Polls until `poll_completions` errs or `within` passes.
    fn error_within(&mut self, within: Duration) -> Option<IoError> {
        let deadline = Instant::now() + within;
        loop {
            match self.d.poll_completions() {
                Err(e) => return Some(e),
                Ok(_) if Instant::now() >= deadline => return None,
                Ok(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    /// The batches that complete, waiting up to `DEADLINE` for `n` of them.
    fn completed(&mut self, n: usize) -> Vec<u64> {
        let deadline = Instant::now() + DEADLINE;
        let mut ids = Vec::new();
        while ids.len() < n && Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            ids.extend(self.completions_within(left).unwrap().into_iter().map(|c| c.batch));
        }
        ids.sort_unstable();
        ids
    }

    fn commits(&self) -> Vec<Vec<DataFile>> {
        self.record.lock().unwrap().commits.clone()
    }

    /// Every object in the store, by path.
    fn objects(&self) -> Vec<String> {
        let store = self.store.clone();
        let metas: Vec<object_store::ObjectMeta> = self
            .runtime
            .block_on(async move { store.list(None).try_collect().await })
            .unwrap();
        let mut paths: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        paths.sort();
        paths
    }

    /// The file at `path`, read back: its metadata and its `_raw` texts.
    fn read_back(&self, path: &str) -> (Arc<ParquetMetaData>, Vec<String>) {
        self.read_column(path, "_raw")
    }

    /// The file at `path`, read back: its metadata and the texts of its string column `column`.
    fn read_column(&self, path: &str, column: &str) -> (Arc<ParquetMetaData>, Vec<String>) {
        let store = self.store.clone();
        let location = Path::from(path);
        let bytes = self
            .runtime
            .block_on(async move { store.get(&location).await.unwrap().bytes().await })
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let metadata = builder.metadata().clone();
        let mut texts = Vec::new();
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            let values = batch.column_by_name(column).unwrap().as_string::<i32>();
            texts.extend(values.iter().map(|v| v.unwrap().to_string()));
        }
        (metadata, texts)
    }
}

/// The `_raw` column's min and max over `metadata`'s first row group.
fn min_max(metadata: &ParquetMetaData) -> (String, String) {
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("statistics are written");
    let text = |b: Option<&[u8]>| String::from_utf8(b.expect("a bound").to_vec()).unwrap();
    (text(stats.min_bytes_opt()), text(stats.max_bytes_opt()))
}

// --- the cases ----------------------------------------------------------------

#[test]
fn rows_of_two_partitions_make_two_files_under_the_committers_paths() {
    let schema = raw_schema();
    let mut t = table(config(&schema, Arc::new(ByFirstChar)), &[]);
    let rows = partitioned_rows(&schema, &["apple", "banana", "avocado", "blueberry", "apricot"]);
    assert!(t.deliver(1, &rows).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1, "one commit: {commits:?}");
    let mut files = commits[0].clone();
    files.sort_by(|a, b| a.partition.path.cmp(&b.partition.path));
    assert_eq!(files.len(), 2, "a file per partition: {files:?}");

    let a = &files[0];
    assert_eq!(a.partition.path, vec!["k=a".to_string()]);
    assert_eq!(a.partition.values, vec![("k".to_string(), "a".to_string())]);
    assert!(
        a.path.starts_with("data/k=a/") && a.path.ends_with(".parquet"),
        "{}",
        a.path
    );
    assert_eq!(a.record_count, 3);
    let (metadata, mut texts) = t.read_back(&a.path);
    texts.sort();
    assert_eq!(texts, vec!["apple", "apricot", "avocado"]);
    assert_eq!(metadata.file_metadata().num_rows(), 3);
    assert_eq!(min_max(&metadata), ("apple".to_string(), "avocado".to_string()));
    let written = a.metadata.as_ref().expect("a Parquet file's metadata");
    assert_eq!(written.file_metadata().num_rows(), 3);
    assert_eq!(min_max(written), ("apple".to_string(), "avocado".to_string()));

    let b = &files[1];
    assert_eq!(b.partition.path, vec!["k=b".to_string()]);
    assert!(b.path.starts_with("data/k=b/"), "{}", b.path);
    assert_eq!(b.record_count, 2);
    let (metadata, mut texts) = t.read_back(&b.path);
    texts.sort();
    assert_eq!(texts, vec!["banana", "blueberry"]);
    assert_eq!(min_max(&metadata), ("banana".to_string(), "blueberry".to_string()));

    // The store holds exactly the committed files, each as large as the committer was told.
    let mut paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    paths.sort();
    assert_eq!(t.objects(), paths);
    let store = t.store.clone();
    for f in &files {
        let location = Path::from(f.path.as_str());
        let store = store.clone();
        let meta = t.runtime.block_on(async move { store.head(&location).await }).unwrap();
        assert_eq!(meta.size, f.size_bytes, "{}", f.path);
    }
}

#[test]
fn a_batch_completes_only_after_its_commit() {
    let schema = raw_schema();
    let mut t = table(config(&schema, Arc::new(Unpartitioned)), &[]);
    let rows = rows(&schema, &["one", "two"]);
    assert!(t.deliver(7, &rows).is_empty());

    // Nothing before the interval.
    let early = t.completions_within(INTERVAL / 3).unwrap();
    assert!(early.is_empty(), "completed before the interval: {early:?}");
    assert!(t.commits().is_empty(), "committed before the interval");

    // One completion after it, and the commit it came from.
    assert_eq!(t.completed(1), vec![7]);
    assert_eq!(t.commits().len(), 1);
    assert!(
        t.completions_within(INTERVAL * 2).unwrap().is_empty(),
        "completed twice"
    );
}

#[test]
fn open_files_report_what_they_buffer_until_the_commit_closes_them() {
    let schema = raw_schema();
    let mut t = table(config(&schema, Arc::new(Unpartitioned)), &[]);
    assert_eq!(t.d.open_bytes(), Some(0));
    assert!(t.deliver(7, &rows(&schema, &["one", "two"])).is_empty());
    // The writer thread measures after it takes the batch.
    let open = |t: &Table, held: fn(usize) -> bool| {
        let deadline = Instant::now() + DEADLINE;
        while !held(t.d.open_bytes().unwrap()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        t.d.open_bytes().unwrap()
    };
    assert!(open(&t, |b| b > 0) > 0, "nothing reported open before the commit");
    assert_eq!(t.completed(1), vec![7]);
    assert_eq!(open(&t, |b| b == 0), 0);
}

#[test]
fn the_interval_fires_with_no_new_batch() {
    let schema = raw_schema();
    let mut t = table(config(&schema, Arc::new(Unpartitioned)), &[]);
    let rows = rows(&schema, &["only"]);
    assert!(t.deliver(3, &rows).is_empty());
    // Egress goes idle: nothing is handed over, it only comes back to poll.
    std::thread::sleep(INTERVAL * 3);
    let got = t.d.poll_completions().unwrap();
    assert_eq!(got.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![3]);
    assert_eq!(t.commits().len(), 1);
}

#[test]
fn bytes_held_near_the_buffer_force_a_commit_before_the_interval() {
    let schema = raw_schema();
    // The budget is three times what the first batch holds, as the egress queue counts it. A budget this small is
    // committed at half of it, so the first batch waits for the interval.
    let small = rows(&schema, &["a"]);
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.commit_interval = LONG;
    c.buffer_bytes = held_bytes(&small) * 3;
    let mut t = table(c, &[]);
    assert!(t.deliver(1, &small).is_empty());
    assert!(t.completions_within(Duration::from_millis(500)).unwrap().is_empty());
    assert!(t.commits().is_empty());

    // The second takes what is held past half the budget, and forces the commit.
    let big: String = "x".repeat(held_bytes(&small) * 2);
    let large = rows(&schema, &[big.as_str()]);
    assert!(t.deliver(2, &large).is_empty());
    assert_eq!(t.completed(2), vec![1, 2]);
    let commits = t.commits();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].iter().map(|f| f.record_count).sum::<u64>(), 2);
}

#[test]
fn a_group_larger_than_a_row_group_is_written_in_several_row_groups() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.max_file_bytes = 4 * TAKE_BYTES;
    c.buffer_bytes = 8 * TAKE_BYTES;
    let mut t = table(c, &[]);
    // Forty distinct texts of a mebibyte each, uncompressed: more than one row group's worth in one batch.
    let texts: Vec<String> = (0..40u8)
        .map(|i| ((b'a' + i) as char).to_string().repeat(1024 * 1024))
        .collect();
    let rows = rows(&schema, &texts.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(t.deliver(1, &rows).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].len(), 1, "one file: {:?}", commits[0]);
    let file = &commits[0][0];
    assert_eq!(file.record_count, 40);
    let groups = file
        .metadata
        .as_ref()
        .expect("a Parquet file's metadata")
        .num_row_groups();
    assert!(groups >= 2, "{groups} row groups");
    let (metadata, mut read) = t.read_back(&file.path);
    assert_eq!(metadata.file_metadata().num_rows(), 40);
    read.sort();
    assert_eq!(read, texts);
}

#[test]
fn a_row_group_ends_at_the_take_bytes_asked_past_a_files_share_or_short_of_it() {
    let schema = raw_schema();
    // Forty distinct texts of a mebibyte each, uncompressed.
    let texts: Vec<String> = (0..40u8)
        .map(|i| ((b'a' + i) as char).to_string().repeat(1024 * 1024))
        .collect();
    for (take_bytes, want) in [(2 * TAKE_BYTES, 1..=1), (4 * 1024 * 1024, 10..=14)] {
        let mut c = config(&schema, Arc::new(Unpartitioned));
        c.max_file_bytes = 4 * TAKE_BYTES;
        c.buffer_bytes = 8 * TAKE_BYTES;
        c.take_bytes = take_bytes;
        let mut t = table(c, &[]);
        let rows = rows(&schema, &texts.iter().map(String::as_str).collect::<Vec<_>>());
        assert!(t.deliver(1, &rows).is_empty());
        assert_eq!(t.completed(1), vec![1]);
        let commits = t.commits();
        let metadata = commits[0][0].metadata.as_ref().expect("a Parquet file's metadata");
        let groups = metadata.num_row_groups();
        assert!(want.contains(&groups), "{groups} row groups of {take_bytes} bytes");
    }
}

#[test]
fn one_batch_larger_than_max_file_bytes_makes_several_files_none_far_past_the_limit() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.max_file_bytes = 2 * 1024 * 1024;
    let mut t = table(c, &[]);
    // Forty texts of 128 KiB each, all different: five mebibytes, past twice the limit.
    let texts: Vec<String> = (0..40u32).map(|i| format!("{i:04}").repeat(32 * 1024)).collect();
    let rows = rows(&schema, &texts.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(t.deliver(1, &rows).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let files = &t.commits()[0];
    assert!(files.len() >= 3, "{} files", files.len());
    assert!(
        // A file can pass the limit by its page headers and footer.
        files.iter().all(|f| f.size_bytes <= 2 * 1024 * 1024 + 32 * 1024),
        "{:?}",
        files.iter().map(|f| f.size_bytes).collect::<Vec<_>>()
    );
    assert_eq!(files.iter().map(|f| f.record_count).sum::<u64>(), 40);
}

#[test]
fn max_file_bytes_closes_a_file_early_even_within_a_batch_and_the_next_commit_carries_all() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.max_file_bytes = 1;
    c.commit_interval = LONG;
    let mut t = table(c, &[]);
    let first = rows(&schema, &["first", "second"]);
    let second = rows(&schema, &["third"]);
    assert!(t.deliver(1, &first).is_empty());
    assert!(t.deliver(2, &second).is_empty());
    t.d.flush().unwrap();
    assert_eq!(t.completed(2), vec![1, 2]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1, "one commit: {commits:?}");
    let files = &commits[0];
    // A row is past a one-byte limit, so the first batch's two rows go to two files.
    assert_eq!(files.len(), 3, "a file per row of the one partition: {files:?}");
    assert!(
        files
            .iter()
            .all(|f| f.partition == files[0].partition && f.record_count == 1)
    );
    let mut texts: Vec<String> = files.iter().flat_map(|f| t.read_back(&f.path).1).collect();
    texts.sort();
    assert_eq!(texts, vec!["first", "second", "third"]);
}

#[test]
fn a_refused_partition_a_row_without_columns_and_an_unfit_row_are_rejected_at_deliver() {
    let schema = raw_schema();
    let mut t = table(config(&schema, Arc::new(ByFirstChar)), &[]);
    let mut rows = partitioned_rows(&schema, &["fine", "!refused", "no columns", "unfit"]);
    rows[2].columns = None;
    rows[3].unfit = Some("it is not a record".into());

    let mut refused = t.deliver(1, &rows);
    refused.sort_by_key(|r| r.doc);
    let docs: Vec<usize> = refused.iter().map(|r| r.doc).collect();
    assert_eq!(docs, vec![1, 2, 3], "{refused:?}");
    assert!(refused[0].reason.contains("refuses"), "{}", refused[0].reason);
    assert!(refused[1].reason.contains("columns"), "{}", refused[1].reason);
    assert!(refused[2].reason.contains("not a record"), "{}", refused[2].reason);
    for r in &refused {
        let record = r.record.as_ref().expect("deliver holds the row it refuses");
        assert_eq!(record.value, rows[r.doc].value);
    }

    // The one fine row is written and the batch completes with no further refusal.
    let completions = t.completions_within(DEADLINE).unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].batch, 1);
    assert!(completions[0].rejections.is_empty(), "{:?}", completions[0].rejections);
    let commits = t.commits();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].len(), 1);
    assert_eq!(commits[0][0].record_count, 1);
    assert_eq!(t.read_back(&commits[0][0].path).1, vec!["fine"]);
}

#[test]
fn a_commit_transient_past_stall_after_fails_the_destination_and_leaves_no_finished_file() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.stall_after = Duration::from_secs(1);
    let mut t = table(c, &[Answer::Transient]);
    let rows = rows(&schema, &["lost", "and found"]);
    assert!(t.deliver(1, &rows).is_empty());

    let started = Instant::now();
    let error = t.error_within(DEADLINE).expect("the destination fails");
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "failed after {:?}, before stall_after",
        started.elapsed()
    );
    assert!(error.message.contains("catalog is away"), "{}", error.message);
    assert!(t.commits().len() >= 2, "retried: {} attempts", t.commits().len());

    // The failure is reported once; egress hands the batch over again.
    assert!(t.completions_within(INTERVAL * 2).unwrap().is_empty());
    assert_eq!(t.objects(), Vec::<String>::new(), "the uncommitted data left a file");
}

#[test]
fn a_destination_that_gives_up_after_a_commit_that_may_have_landed_keeps_its_files() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.stall_after = Duration::from_secs(1);
    let mut t = table(c, &[Answer::Unknown, Answer::Transient]);
    assert!(t.deliver(1, &rows(&schema, &["maybe"])).is_empty());

    t.error_within(DEADLINE).expect("the destination fails");
    let commits = t.commits();
    assert_eq!(
        t.objects(),
        vec![commits[0][0].path.clone()],
        "a snapshot that landed may name the file"
    );
}

#[test]
fn a_conflict_is_retried_at_once_and_the_commit_carries_every_file() {
    let schema = raw_schema();
    let mut t = table(
        config(&schema, Arc::new(ByFirstChar)),
        &[Answer::Conflict, Answer::Conflict, Answer::Ok],
    );
    let rows = partitioned_rows(&schema, &["alpha", "beta", "gamma"]);
    assert!(t.deliver(1, &rows).is_empty());
    let started = Instant::now();
    assert_eq!(t.completed(1), vec![1]);
    assert!(
        started.elapsed() < INTERVAL * 3,
        "a conflict waited: {:?}",
        started.elapsed()
    );

    let commits = t.commits();
    assert_eq!(commits.len(), 3, "two conflicts and the commit: {commits:?}");
    for attempt in &commits {
        assert_eq!(attempt.len(), 3, "every file, each time: {attempt:?}");
        let mut paths: Vec<&str> = attempt.iter().map(|f| f.path.as_str()).collect();
        paths.sort_unstable();
        let mut first: Vec<&str> = commits[0].iter().map(|f| f.path.as_str()).collect();
        first.sort_unstable();
        assert_eq!(paths, first, "the same files again");
    }
    let partitions: HashSet<&Partition> = commits[2].iter().map(|f| &f.partition).collect();
    assert_eq!(partitions.len(), 3);
    assert_eq!(t.objects().len(), 3);
}

#[test]
fn flush_commits_at_once_and_the_completions_come_from_the_next_poll() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.commit_interval = LONG;
    let mut t = table(c, &[]);
    let rows = rows(&schema, &["now", "please"]);
    assert!(t.deliver(1, &rows).is_empty());
    assert!(t.deliver(2, &rows).is_empty());

    let started = Instant::now();
    t.d.flush().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "flush waited for the interval"
    );
    assert_eq!(t.commits().len(), 1, "the barrier committed");
    assert_eq!(t.commits()[0].iter().map(|f| f.record_count).sum::<u64>(), 4);

    let mut got: Vec<u64> = t.d.poll_completions().unwrap().into_iter().map(|c| c.batch).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2]);

    // A barrier with nothing in flight commits nothing.
    t.d.flush().unwrap();
    assert_eq!(t.commits().len(), 1);
    assert!(t.d.poll_completions().unwrap().is_empty());
}

#[test]
fn the_sixty_fifth_partition_closes_the_least_recently_written_file_and_both_are_committed() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(ByFirstChar));
    c.commit_interval = LONG;
    let mut t = table(c, &[]);
    // 65 partitions: one per distinct first character. Two batches, so that the first partition's file is the least
    // recently written when the 65th opens.
    let chars: Vec<char> = ('a'..='z')
        .chain('A'..='Z')
        .chain('0'..='9')
        .chain("-_.".chars())
        .collect();
    assert_eq!(chars.len(), 65);
    let first_texts: Vec<String> = chars[..1].iter().map(|c| format!("{c}-first")).collect();
    let rest_texts: Vec<String> = chars[1..].iter().map(|c| format!("{c}-rest")).collect();
    let first = partitioned_rows(&schema, &first_texts.iter().map(String::as_str).collect::<Vec<_>>());
    let rest = partitioned_rows(&schema, &rest_texts.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(t.deliver(1, &first).is_empty());
    assert!(t.deliver(2, &rest).is_empty());
    t.d.flush().unwrap();
    assert_eq!(t.completed(2), vec![1, 2]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1, "one commit: {} commits", commits.len());
    let files = &commits[0];
    assert_eq!(files.len(), 65, "a file per partition");
    let partitions: HashSet<&Partition> = files.iter().map(|f| &f.partition).collect();
    assert_eq!(partitions.len(), 65);
    assert!(files.iter().all(|f| f.record_count == 1));
    let closed_early = files
        .iter()
        .find(|f| f.partition.path == vec![format!("k={}", chars[0])])
        .expect("the first partition's file");
    assert_eq!(t.read_back(&closed_early.path).1, vec![first_texts[0].clone()]);
    assert_eq!(t.objects().len(), 65);
}

/// The record of `{text: str}`: a second schema beside `raw_schema`'s.
fn text_schema() -> Schema {
    Schema::new(&DataType::Struct(vec![("text".into(), DataType::Str)], false)).unwrap()
}

#[test]
fn each_part_is_written_on_its_own_schema() {
    let raw = raw_schema();
    let text = text_schema();
    let mut c = config(&raw, Arc::new(ByPartDigit));
    c.format = Arc::new(ParquetFormat::new(
        vec![raw.clone(), text.clone()],
        Compression::None.into(),
    ));
    let mut t = table(c, &[]);
    let mut mixed = rows(&raw, &["0-first", "0-second"]);
    mixed.extend(rows(&text, &["1-first"]));
    assert!(t.deliver(1, &mixed).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1, "one commit: {commits:?}");
    let mut files = commits[0].clone();
    files.sort_by_key(|f| f.partition.part);
    assert_eq!(files.len(), 2, "a file per part: {files:?}");

    assert!(files[0].path.starts_with("data/p=0/"), "{}", files[0].path);
    let (metadata, mut texts) = t.read_back(&files[0].path);
    texts.sort();
    assert_eq!(texts, vec!["0-first", "0-second"]);
    assert_eq!(metadata.file_metadata().schema_descr().column(0).name(), "_raw");

    assert!(files[1].path.starts_with("data/p=1/"), "{}", files[1].path);
    let (metadata, texts) = t.read_column(&files[1].path, "text");
    assert_eq!(texts, vec!["1-first"]);
    assert_eq!(metadata.file_metadata().schema_descr().column(0).name(), "text");
}

#[test]
fn a_row_not_of_its_parts_schema_or_of_a_part_with_no_schema_is_refused_at_deliver() {
    let raw = raw_schema();
    let text = text_schema();
    let mut c = config(&raw, Arc::new(ByPartDigit));
    c.format = Arc::new(ParquetFormat::new(
        vec![raw.clone(), text.clone()],
        Compression::None.into(),
    ));
    let mut t = table(c, &[]);
    // Part 1 is of `text`; part 2 has no schema.
    let mut mixed = rows(&raw, &["0-fine", "1-raw", "2-nowhere"]);
    mixed.extend(rows(&text, &["0-text"]));

    let mut refused = t.deliver(1, &mixed);
    refused.sort_by_key(|r| r.doc);
    let docs: Vec<usize> = refused.iter().map(|r| r.doc).collect();
    assert_eq!(docs, vec![1, 2, 3], "{refused:?}");
    assert!(refused[0].reason.contains("schema"), "{}", refused[0].reason);
    assert!(refused[1].reason.contains("part 2"), "{}", refused[1].reason);
    assert!(refused[2].reason.contains("schema"), "{}", refused[2].reason);

    assert_eq!(t.completed(1), vec![1]);
    let commits = t.commits();
    assert_eq!(commits[0].len(), 1, "{:?}", commits[0]);
    assert_eq!(t.read_back(&commits[0][0].path).1, vec!["0-fine"]);
}

#[test]
fn the_configured_attributes_reach_every_uploaded_file() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    let mut attributes = Attributes::new();
    attributes.insert(
        Attribute::ContentType,
        AttributeValue::from("application/vnd.apache.parquet"),
    );
    attributes.insert(Attribute::StorageClass, AttributeValue::from("STANDARD_IA"));
    c.attributes = attributes.clone();
    let mut t = table(c, &[]);
    let rows = rows(&schema, &["labelled"]);
    assert!(t.deliver(1, &rows).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    let location = Path::from(commits[0][0].path.as_str());
    let store = t.store.clone();
    let got = t
        .runtime
        .block_on(async move { store.get(&location).await })
        .unwrap()
        .attributes;
    assert_eq!(got, attributes);
}

/// Each row's value as a line, buffered whole until taken.
struct Lines;

#[derive(Default)]
struct LineEncoder {
    buffered: Vec<u8>,
    taken: usize,
    rows: u64,
}

impl FileFormat for Lines {
    fn admit(&self, row: &OwnedRecord, _partition: &Partition) -> Result<FileRow, String> {
        Ok(FileRow::Line(row.value.clone()))
    }

    fn open(&self, _partition: &Partition) -> Box<dyn FileEncoder> {
        Box::new(LineEncoder::default())
    }
}

impl FileEncoder for LineEncoder {
    fn write(&mut self, rows: &[FileRow]) {
        for row in rows {
            let FileRow::Line(line) = row else {
                panic!("Lines admits only lines")
            };
            self.buffered.extend_from_slice(line);
            self.buffered.push(b'\n');
            self.rows += 1;
        }
    }

    fn in_progress_bytes(&self) -> usize {
        self.buffered.len()
    }

    fn take(&mut self) -> Vec<u8> {
        self.taken += self.buffered.len();
        std::mem::take(&mut self.buffered)
    }

    fn bytes_written(&self) -> usize {
        self.taken + self.buffered.len()
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn finish(self: Box<Self>) -> (Vec<u8>, Option<ParquetMetaData>) {
        (self.buffered, None)
    }
}

#[test]
fn a_format_other_than_parquet_writes_its_own_bytes() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.format = Arc::new(Lines);
    c.commit_interval = LONG;
    let mut t = table(c, &[]);
    assert!(t.deliver(1, &rows(&schema, &["one", "two"])).is_empty());
    assert!(t.deliver(2, &rows(&schema, &["three"])).is_empty());
    t.d.flush().unwrap();
    assert_eq!(t.completed(2), vec![1, 2]);

    let commits = t.commits();
    assert_eq!(commits.len(), 1, "one commit: {commits:?}");
    assert_eq!(commits[0].len(), 1, "one file: {:?}", commits[0]);
    let file = &commits[0][0];
    assert_eq!(file.record_count, 3);
    assert!(file.metadata.is_none(), "metadata of a file that is not Parquet");
    let store = t.store.clone();
    let location = Path::from(file.path.as_str());
    let bytes = t
        .runtime
        .block_on(async move { store.get(&location).await.unwrap().bytes().await })
        .unwrap();
    assert_eq!(&bytes[..], b"one\ntwo\nthree\n");
    assert_eq!(file.size_bytes, bytes.len() as u64);
}

/// A file past one part goes up in parts and reads back whole. Every smaller file in this module is a single put.
#[test]
fn a_file_larger_than_one_part_reads_back_whole() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.format = Arc::new(Lines);
    let mut t = table(c, &[]);
    // Forty mebibytes of lines: one take of the encoder's output fills several parts.
    let texts: Vec<String> = (0..40u8)
        .map(|i| ((b'a' + i % 26) as char).to_string().repeat(1024 * 1024))
        .collect();
    assert!(
        t.deliver(1, &rows(&schema, &texts.iter().map(String::as_str).collect::<Vec<_>>()))
            .is_empty()
    );
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    assert_eq!(commits[0].len(), 1, "one file: {:?}", commits[0]);
    let file = &commits[0][0];
    let store = t.store.clone();
    let location = Path::from(file.path.as_str());
    let bytes = t
        .runtime
        .block_on(async move { store.get(&location).await.unwrap().bytes().await })
        .unwrap();
    let mut expected = texts.join("\n").into_bytes();
    expected.push(b'\n');
    assert!(
        bytes[..] == expected[..],
        "{} bytes, expected {}",
        bytes.len(),
        expected.len()
    );
    assert_eq!(file.size_bytes, bytes.len() as u64);
}

/// A compressed file is held to its limit by its written size: rows that compress well fill one file far past the
/// limit in raw bytes.
#[test]
fn a_compressed_file_is_finished_by_its_written_size() {
    let schema = raw_schema();
    let mut c = config(&schema, Arc::new(Unpartitioned));
    c.format = Arc::new(smeltr::io::lines::LineFormat::new(
        smeltr::plan::topology::ObjectFormat::Ndjson,
        smeltr::plan::topology::Compression::Gzip,
    ));
    c.max_file_bytes = 1024 * 1024;
    let mut t = table(c, &[]);
    // Eight mebibytes of one repeated line, a few kilobytes gzipped.
    let line = format!("{{\"m\":\"{}\"}}", "x".repeat(1000));
    let lines = vec![line.as_str(); 8 * 1024];
    assert!(t.deliver(1, &rows(&schema, &lines)).is_empty());
    assert_eq!(t.completed(1), vec![1]);

    let commits = t.commits();
    assert_eq!(commits[0].len(), 1, "one file: {:?}", commits[0]);
    assert_eq!(commits[0][0].record_count, lines.len() as u64);
}
