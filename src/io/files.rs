//! The file destination: data files that stay open across batches, written into object storage in a [`FileFormat`]
//! and handed to a [`Committer`], each batch acknowledged once the commit that holds its last row lands. An object
//! store destination is one, in any format, whose committer only names its files; each table format (Iceberg, Delta) is a
//! committer that records them in the table.
//!
//! ```mermaid
//! flowchart LR
//!   E[egress thread<br/>deliver] -->|Job: batch, rows by partition| W[writer thread]
//!   W -->|encoded bytes| U[put, or multipart upload<br/>object_store]
//!   W -->|on interval, buffer or barrier| C[Committer::commit files]
//!   C -->|Ok| D[completions for every batch in those files]
//!   D --> P[poll_completions]
//! ```
//!
//! A row is refused only at `deliver`: it is `unfit`, the partitioner refuses it, or the format does not admit it (for
//! Parquet: it carries no columns, names a part the destination has no schema for, or its chunk is not its part's
//! schema). After that a failure is of a file or the commit, and transient: open uploads are aborted, closed files
//! are deleted, best effort, `poll_completions` answers the error once and egress hands every in-flight batch over
//! again. A batch delivered twice is its rows twice in the table, as for every at-least-once destination.
//!
//! Every open file closes at the commit, so the batches a commit completes are exactly the ones delivered before it
//! began. The commit happens when the interval has passed since the last one, when the bytes held since the last one
//! reach [`commit_at`] of the egress budget, or when `flush` asks.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, channel, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::{
    Attributes, MultipartUpload, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart, WriteMultipart,
};
use parquet::file::metadata::ParquetMetaData;
use tokio_util::bytes::Bytes;

use super::parquet::{Columns, FileWriter, Schema, WriteOptions};
use super::{BatchCompletion, BatchId, Destination, Dispatched, IoError, OwnedRecord, Rejection};

/// Files open per destination. Opening one more closes the least recently written.
pub const MAX_OPEN_FILES: usize = 64;

/// Bytes an encoder buffers before its output goes to the upload: a Parquet row group, or that much of lines. What
/// the worker reserves for each open file.
pub const TAKE_BYTES: usize = 32 * 1024 * 1024;

/// What every open file's encoder may buffer together. A file may take more than `TAKE_BYTES` while others take less:
/// a row group grows to its table's size where few files are open.
const TAKE_POOL_BYTES: usize = MAX_OPEN_FILES * TAKE_BYTES;

/// Commits retried at once after a `Conflict` before the destination fails.
pub const MAX_CONFLICT_RETRIES: usize = 8;

/// Bytes of one multipart part. A file that ends within one goes up as a single put.
const PART_BYTES: usize = 8 * 1024 * 1024;

/// What one open file may hold: the take its encoder buffers, the part its upload is filling, and `parts_in_flight`
/// parts on their way.
pub fn open_file_bytes(parts_in_flight: usize) -> usize {
    TAKE_BYTES + (parts_in_flight + 1) * PART_BYTES
}

/// Bounds of the wait between retries of a `Transient` commit failure.
const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Jobs the egress thread may hand over before `deliver` waits for the writer.
const JOB_QUEUE: usize = 4;

/// What a commit leaves free of the egress budget: room for the batch egress is handing over and the jobs queued
/// behind it, each at most one poll. Egress refuses a batch past the budget, so a writer that waited for it to fill
/// would never see the batch that fills it, and its sources would wait out the interval.
const COMMIT_ROOM: usize = (JOB_QUEUE + 1) * crate::worker::TICK_MAX_BYTES;

/// How often the writer measures its open files when no batch comes.
const MEASURE_EVERY: Duration = Duration::from_secs(1);

/// The bytes held at which a commit is forced: all of `buffer_bytes` but [`COMMIT_ROOM`], and never under half of it.
fn commit_at(buffer_bytes: usize) -> usize {
    buffer_bytes.saturating_sub(COMMIT_ROOM).max(buffer_bytes / 2)
}

/// A table's partition for one row: the schema its file holds, the directory its file goes under, and the values a
/// commit records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Partition {
    /// Which of the format's records this partition's files hold (for Parquet, an index into its schemas). 0 for a
    /// table, which has one.
    pub part: usize,
    /// Path segments under the data location, Hive style: `["dt=2026-10-10", "host=a"]`. Empty for an
    /// unpartitioned table.
    pub path: Vec<String>,
    /// Each partition field's name and value as a string, in the spec's order.
    pub values: Vec<(String, String)>,
}

/// Decides a row's partition.
pub trait Partitioner: Send + Sync + 'static {
    /// The partition of `row`, which is not `unfit`. `Err` is why the row cannot be partitioned, and refuses it.
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String>;
}

/// Every row in one partition.
pub struct Unpartitioned;

impl Partitioner for Unpartitioned {
    fn partition(&self, _row: &OwnedRecord) -> Result<Partition, String> {
        Ok(Partition {
            part: 0,
            path: Vec::new(),
            values: Vec::new(),
        })
    }
}

/// One data file written and closed, ready to be committed.
#[derive(Clone, Debug)]
pub struct DataFile {
    /// Where the file is, as the committer named it (`Committer::file_path`).
    pub path: String,
    pub partition: Partition,
    pub size_bytes: u64,
    pub record_count: u64,
    /// The file's Parquet metadata, when it is Parquet: a table format derives its statistics from it.
    pub metadata: Option<Arc<ParquetMetaData>>,
}

/// Why a commit did not land.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitError {
    /// Another writer committed first; the committer has reloaded the table, and the same files may be committed
    /// again at once.
    Conflict,
    /// Anything else, retried with backoff until the stall threshold.
    Transient(String),
    /// The commit may have landed: its answer was lost. Retried as `Transient`; the committer looks for it in the table
    /// before committing again. A destination that gives up after one keeps its files, which the table may name.
    Unknown(String),
}

/// Commits of one set of files whose answers were lost. Any of them may still land, so each commit of those files
/// first looks for one in the table. `Id` is what the table records of a commit: Delta's transaction version,
/// Iceberg's snapshot id.
pub struct Lost<Id> {
    /// The files' paths, in the order they were committed, and each lost commit's id.
    held: Option<(Vec<String>, Vec<Id>)>,
}

impl<Id> Lost<Id> {
    /// Nothing lost yet.
    pub fn none() -> Lost<Id> {
        Lost { held: None }
    }

    /// Whether lost commits of `paths` are held. Held commits of other files are forgotten first: those failed the
    /// destination, and their batches come again in files of their own.
    pub fn same_files(&mut self, paths: &[&str]) -> bool {
        if self.held.as_ref().is_some_and(|(held, _)| held != paths) {
            self.held = None;
        }
        self.held.is_some()
    }

    /// Holds the commit `id` of `paths`, whose answer was lost.
    pub fn note(&mut self, paths: &[&str], id: Id) {
        let (_, ids) = self
            .held
            .get_or_insert_with(|| (paths.iter().map(|p| p.to_string()).collect(), Vec::new()));
        ids.push(id);
    }

    /// Whether a lost commit is in the table as last read, by `found`; forgets them all when one is, since their
    /// files are then in it.
    pub fn landed(&mut self, found: impl Fn(&Id) -> bool) -> bool {
        let landed = self.held.as_ref().is_some_and(|(_, ids)| ids.iter().any(found));
        if landed {
            self.held = None;
        }
        landed
    }

    /// Forgets the lost commits: a commit of their files landed in plain sight.
    pub fn clear(&mut self) {
        self.held = None;
    }
}

/// The table: where its files go and how they are committed. One per destination, owned by the writer thread.
pub trait Committer: Send + 'static {
    /// The object store path of a new data file of `partition`, under the table's location. `id` is 32 hex
    /// characters, unique.
    fn file_path(&self, partition: &Partition, id: &str) -> String;
    /// Makes `files` part of the table, atomically. Called from the writer thread, which waits for it with nothing
    /// else running: every call it makes must have a timeout of its own, as `io::http` and `io::objects` give theirs,
    /// so a network that goes quiet comes back as `Transient` and not as a hang of the destination.
    fn commit(&mut self, files: &[DataFile]) -> Result<(), CommitError>;
}

/// One admitted row as its file's encoder takes it.
#[derive(Clone, Debug)]
pub enum FileRow {
    /// A row of a Parquet chunk, cut to its part's schema.
    Columns(Columns),
    /// A row's line, without its separator (the encoder adds framing).
    Line(Vec<u8>),
}

impl FileRow {
    /// What the row holds in memory; bounds a slice against the encoder's take size and the file's limit.
    pub fn bytes(&self) -> usize {
        match self {
            FileRow::Columns(c) => c.chunk.row_bytes(c.row),
            FileRow::Line(line) => line.len() + 1,
        }
    }
}

/// How rows become a destination's files.
pub trait FileFormat: Send + Sync + 'static {
    /// `row`, whose partition is `partition`, as its file takes it; `Err` is why it is refused.
    fn admit(&self, row: &OwnedRecord, partition: &Partition) -> Result<FileRow, String>;
    /// A new file's encoder for `partition`.
    fn open(&self, partition: &Partition) -> Box<dyn FileEncoder>;
}

/// One file being written.
pub trait FileEncoder: Send {
    /// Appends `rows`, each admitted by this encoder's format for its partition.
    fn write(&mut self, rows: &[FileRow]);
    /// Bytes buffered since the last `take`, as rows hold them.
    fn in_progress_bytes(&self) -> usize;
    /// Every encoded byte not yet taken, ending a Parquet row group. Empty when nothing is buffered.
    fn take(&mut self) -> Vec<u8>;
    /// The bytes the finished file will have had so far: taken, plus an estimate of what is buffered.
    fn bytes_written(&self) -> usize;
    /// Rows written, buffered included.
    fn rows(&self) -> u64;
    /// The file's remaining bytes, and its Parquet metadata when it is Parquet.
    fn finish(self: Box<Self>) -> (Vec<u8>, Option<ParquetMetaData>);
}

/// Parquet files, one record per part: each row's chunk is cut to its part's schema.
pub struct ParquetFormat {
    /// The records the destination writes, one per part. A table has one.
    schemas: Vec<Schema>,
    options: WriteOptions,
}

impl ParquetFormat {
    pub fn new(schemas: Vec<Schema>, options: WriteOptions) -> ParquetFormat {
        ParquetFormat { schemas, options }
    }
}

/// `row` as a file of one record per part takes it: its columns, cut to `schemas[partition.part]`.
pub fn admit_columns(schemas: &[Schema], row: &OwnedRecord, partition: &Partition) -> Result<FileRow, String> {
    let Some(columns) = &row.columns else {
        return Err("the row has no columns".into());
    };
    let Some(schema) = schemas.get(partition.part) else {
        return Err(format!(
            "the row's partition is of part {}, and the destination has {} schemas",
            partition.part,
            schemas.len()
        ));
    };
    if !schema.matches(columns) {
        return Err("the row's record is not its part's schema".into());
    }
    if let Some(why) = schema.unfit(columns) {
        return Err(why);
    }
    // Egress made the row in this destination's form; a row the cut could not fit is marked in its chunk.
    if let Some(why) = columns.chunk.unfit(columns.row) {
        return Err(why.to_string());
    }
    // Last, as a computed column may set a value aside for each row it admits.
    schema.admit(columns)?;
    Ok(FileRow::Columns(columns.clone()))
}

impl FileFormat for ParquetFormat {
    fn admit(&self, row: &OwnedRecord, partition: &Partition) -> Result<FileRow, String> {
        admit_columns(&self.schemas, row, partition)
    }

    fn open(&self, partition: &Partition) -> Box<dyn FileEncoder> {
        // `admit` let through only parts this format has a schema for.
        Box::new(ParquetEncoder(FileWriter::of_partition(
            &self.schemas[partition.part],
            &self.options,
            &partition.values,
        )))
    }
}

/// A Parquet file being written; `take` ends a row group.
struct ParquetEncoder(FileWriter);

impl FileEncoder for ParquetEncoder {
    fn write(&mut self, rows: &[FileRow]) {
        self.0.write(rows.iter().map(|row| match row {
            FileRow::Columns(c) => c,
            FileRow::Line(_) => unreachable!("ParquetFormat admits only columns"),
        }));
    }

    fn in_progress_bytes(&self) -> usize {
        self.0.in_progress_bytes()
    }

    fn take(&mut self) -> Vec<u8> {
        self.0.take_row_group()
    }

    fn bytes_written(&self) -> usize {
        self.0.bytes_written()
    }

    fn rows(&self) -> u64 {
        self.0.rows()
    }

    fn finish(self: Box<Self>) -> (Vec<u8>, Option<ParquetMetaData>) {
        let (tail, metadata) = self.0.finish();
        (tail, Some(metadata))
    }
}

pub struct FileConfig {
    /// How rows become files.
    pub format: Arc<dyn FileFormat>,
    pub partitioner: Arc<dyn Partitioner>,
    /// How often open files are closed and committed.
    pub commit_interval: Duration,
    /// Bytes a data file may reach before it is closed early.
    pub max_file_bytes: usize,
    /// Bytes an encoder buffers before its output goes to the upload: for Parquet, a row group's size. Less where
    /// every open file's together reach what the worker reserves for them.
    pub take_bytes: usize,
    /// The destination's egress budget; a commit is forced short of it (`commit_at`).
    pub buffer_bytes: usize,
    /// How long a commit may take, retries included, before the destination fails.
    pub stall_after: Duration,
    /// Set on every data file's upload: content type, storage class. Empty for a table.
    pub attributes: Attributes,
    /// Parts of one file uploading at once. A take waits for room, so a slow store holds the writer back rather than
    /// growing a queue of parts.
    pub parts_in_flight: usize,
}

/// One batch's rows that passed admission, by partition, and what they hold of the egress budget.
struct Job {
    batch: BatchId,
    groups: Vec<(Partition, Vec<FileRow>)>,
    /// What the egress queue holds for the batch while it is in flight (`held_bytes`): the refused rows and the
    /// chunk whole included, since the queue lets none of it go before the batch completes.
    bytes: usize,
    /// The destination's generation when the job was handed over. A failure ends a generation: egress hands every
    /// batch of it over again, so a job of that generation still in the inbox is one the writer must not write.
    generation: u64,
}

/// What the egress thread sends the writer thread.
enum Message {
    Job(Job),
    /// Commit now, and answer on the channel instead of the results channel.
    Barrier(Sender<Result<Vec<BatchCompletion>, IoError>>),
}

pub struct FileDestination {
    format: Arc<dyn FileFormat>,
    partitioner: Arc<dyn Partitioner>,
    /// `None` once dropping has closed the channel, which ends the writer thread.
    jobs: Option<SyncSender<Message>>,
    results: Receiver<Result<Vec<BatchCompletion>, IoError>>,
    thread: Option<JoinHandle<()>>,
    /// Completions a barrier `flush` collected, held for the next `poll_completions`; dropping them would leave
    /// those batches uncommitted.
    drained: Vec<BatchCompletion>,
    /// A failure `poll_completions` read beside completions it returned first, held for the next call.
    failed: Option<IoError>,
    /// Counts the failures reported to the caller; each job carries the count at its hand-over.
    generation: u64,
    /// What the writer's open files buffer, as it last measured.
    open_bytes: Arc<AtomicUsize>,
}

impl FileDestination {
    /// Starts the writer thread. `store` and `runtime` are the table location's (`io::objects::store`,
    /// `io::objects::runtime`); `name` names the destination's kind in errors, `subject` the destination in its lines.
    pub fn new(
        name: &'static str,
        subject: &str,
        config: FileConfig,
        store: Arc<dyn ObjectStore>,
        runtime: Arc<tokio::runtime::Runtime>,
        committer: Box<dyn Committer>,
    ) -> FileDestination {
        let format = config.format.clone();
        let partitioner = config.partitioner.clone();
        let (jobs, inbox) = sync_channel(JOB_QUEUE);
        let (reports, results) = channel();
        let open_bytes = Arc::new(AtomicUsize::new(0));
        let measured = open_bytes.clone();
        let subject = subject.to_string();
        let thread = std::thread::Builder::new()
            .name(format!("{name}-file-writer"))
            .spawn(move || {
                let mut writer = Writer {
                    name,
                    subject,
                    config,
                    store,
                    runtime,
                    committer,
                    open: HashMap::new(),
                    closed: Vec::new(),
                    pending: Vec::new(),
                    held: 0,
                    next_commit: Instant::now(),
                    seen: 0,
                    ended: None,
                    open_bytes: measured,
                };
                writer.next_commit += writer.config.commit_interval;
                writer.run(inbox, reports);
            })
            .expect("a thread for the table writer");
        FileDestination {
            format,
            partitioner,
            jobs: Some(jobs),
            results,
            thread: Some(thread),
            drained: Vec::new(),
            failed: None,
            generation: 0,
            open_bytes,
        }
    }

    fn stopped(&self) -> IoError {
        IoError::transient("the table writer thread has stopped")
    }

    /// A failure on its way to the caller, who takes every batch in flight back: the generation ends with it.
    fn report(&mut self, e: IoError) -> IoError {
        self.generation += 1;
        e
    }
}

impl Destination for FileDestination {
    /// Admits each row and hands the batch to the writer thread by partition. A batch with no row admitted is
    /// complete at once.
    fn deliver(
        &mut self,
        batch: BatchId,
        rows: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut refused = Vec::new();
        let mut groups: Vec<(Partition, Vec<FileRow>)> = Vec::new();
        let mut at: HashMap<Partition, usize> = HashMap::new();
        let bytes = super::held_bytes(&rows);
        for (i, row) in rows.iter().enumerate() {
            if let Some(why) = &row.unfit {
                refused.push(Rejection::of(&rows, i, why.clone()));
                continue;
            }
            let partition = match self.partitioner.partition(row) {
                Ok(p) => p,
                Err(why) => {
                    refused.push(Rejection::of(&rows, i, why));
                    continue;
                }
            };
            let admitted = match self.format.admit(row, &partition) {
                Ok(r) => r,
                Err(why) => {
                    refused.push(Rejection::of(&rows, i, why));
                    continue;
                }
            };
            let group = match at.get(&partition) {
                Some(&g) => g,
                None => {
                    at.insert(partition.clone(), groups.len());
                    groups.push((partition, Vec::new()));
                    groups.len() - 1
                }
            };
            groups[group].1.push(admitted);
        }
        // Nothing for the table, so nothing to wait for: the refusals go back now.
        if groups.is_empty() {
            return Ok((Dispatched::Complete, refused));
        }
        let jobs = self.jobs.as_ref().ok_or_else(|| self.stopped())?;
        let generation = self.generation;
        jobs.send(Message::Job(Job {
            batch,
            groups,
            bytes,
            generation,
        }))
        .map_err(|_| self.stopped())?;
        Ok((Dispatched::InFlight, refused))
    }

    /// Commits now and waits for it. `flush` cannot return the completions, so they are held for the next
    /// `poll_completions`.
    fn flush(&mut self) -> Result<(), IoError> {
        let (reply, answer) = channel();
        let jobs = self.jobs.as_ref().ok_or_else(|| self.stopped())?;
        jobs.send(Message::Barrier(reply)).map_err(|_| self.stopped())?;
        let answer = answer.recv().map_err(|_| self.stopped())?;
        // The writer answered the barrier after every earlier commit's result was sent, so all of those are here.
        let mut failed = None;
        while let Ok(result) = self.results.try_recv() {
            match result {
                Ok(done) => self.drained.extend(done),
                Err(e) => failed = Some(e),
            }
        }
        match answer {
            Ok(done) => self.drained.extend(done),
            Err(e) => failed = Some(e),
        }
        match failed {
            Some(e) => Err(self.report(e)),
            None => Ok(()),
        }
    }

    fn open_bytes(&self) -> Option<usize> {
        Some(self.open_bytes.load(Ordering::Relaxed))
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        // What a barrier saw land is reported before the writer is asked: a failure there would otherwise take it
        // back.
        let drained = std::mem::take(&mut self.drained);
        if !drained.is_empty() {
            return Ok(drained);
        }
        if let Some(e) = self.failed.take() {
            return Err(self.report(e));
        }
        let mut done = Vec::new();
        loop {
            match self.results.try_recv() {
                Ok(Ok(more)) => done.extend(more),
                // Completions beside the failure are real commits; a failure returned now would take them back, so it
                // waits for the next call.
                Ok(Err(e)) if done.is_empty() => return Err(self.report(e)),
                Ok(Err(e)) => {
                    self.failed = Some(e);
                    return Ok(done);
                }
                Err(TryRecvError::Empty) => return Ok(done),
                Err(TryRecvError::Disconnected) if done.is_empty() => {
                    let e = self.stopped();
                    return Err(self.report(e));
                }
                Err(TryRecvError::Disconnected) => return Ok(done),
            }
        }
    }
}

impl Drop for FileDestination {
    fn drop(&mut self) {
        // Closing the job channel ends the writer loop, which aborts what is open.
        self.jobs = None;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// A data file being written: its encoder's output going out as one put, or as multipart parts once it outgrows one.
struct OpenFile {
    encoder: Box<dyn FileEncoder>,
    upload: Upload,
    path: String,
    partition: Partition,
    /// Bytes handed to the upload so far.
    uploaded: usize,
    last_written: Instant,
}

/// The writer thread's state. Owns the committer and every file between its opening and its commit.
struct Writer {
    name: &'static str,
    /// The destination, as its author named it, for its lines.
    subject: String,
    config: FileConfig,
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    committer: Box<dyn Committer>,
    open: HashMap<Partition, OpenFile>,
    /// Closed since the last commit.
    closed: Vec<DataFile>,
    /// Batches handed over since the last commit; they complete together.
    pending: Vec<BatchId>,
    /// Bytes of the egress budget those batches hold.
    held: usize,
    /// When the interval next commits what is pending.
    next_commit: Instant,
    /// The newest generation a job carried, and the generation the last failure ended: a job of that one or an older
    /// one is a batch egress has already taken back.
    seen: u64,
    ended: Option<u64>,
    /// Where it reports what its open files buffer.
    open_bytes: Arc<AtomicUsize>,
}

impl Writer {
    fn run(&mut self, inbox: Receiver<Message>, reports: Sender<Result<Vec<BatchCompletion>, IoError>>) {
        loop {
            // Woken at least every `MEASURE_EVERY`, so what lands while no batch comes shows as it lands.
            let wait = self
                .next_commit
                .saturating_duration_since(Instant::now())
                .min(MEASURE_EVERY);
            match inbox.recv_timeout(wait) {
                Ok(Message::Job(job)) => {
                    if let Err(e) = self.take(job) {
                        let e = self.fail(e);
                        if reports.send(Err(e)).is_err() {
                            break;
                        }
                    }
                }
                Ok(Message::Barrier(reply)) => {
                    let _ = reply.send(self.commit());
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let due = self.held >= commit_at(self.config.buffer_bytes) || Instant::now() >= self.next_commit;
            if due {
                if self.pending.is_empty() && self.open.is_empty() && self.closed.is_empty() {
                    // Nothing to commit: the interval runs from now.
                    self.next_commit = Instant::now() + self.config.commit_interval;
                } else if reports.send(self.commit()).is_err() {
                    break;
                }
            }
            let open = self
                .open
                .values()
                .map(|f| f.encoder.in_progress_bytes() + f.upload.pending())
                .sum();
            self.open_bytes.store(open, Ordering::Relaxed);
        }
        // Stopped with files between a close and their commit: as on a failure, nothing of them is kept.
        self.abort_open();
        self.delete_closed();
    }

    /// Writes one batch's rows into their partitions' files. A job of an ended generation is skipped: its batch comes
    /// again under a new one.
    fn take(&mut self, job: Job) -> Result<(), String> {
        if self.ended.is_some_and(|ended| job.generation <= ended) {
            return Ok(());
        }
        self.seen = self.seen.max(job.generation);
        for (partition, rows) in job.groups {
            self.write(partition, &rows)?;
        }
        self.pending.push(job.batch);
        self.held += job.bytes;
        Ok(())
    }

    fn write(&mut self, partition: Partition, rows: &[FileRow]) -> Result<(), String> {
        // A group goes in slices, each filling what is left of the take in progress and of the file by the rows' own
        // sizes, so the writer holds about one take and a file ends near its limit whatever the batch brought. A row's
        // own size is at least what it adds encoded, so a file passes its limit only by its framing (for Parquet, page
        // headers and footer).
        let mut rest = rows;
        while !rest.is_empty() {
            if !self.open.contains_key(&partition) {
                if self.open.len() >= MAX_OPEN_FILES {
                    let oldest = self
                        .open
                        .iter()
                        .min_by_key(|(_, f)| f.last_written)
                        .map(|(p, _)| p.clone())
                        .expect("a full map has an oldest file");
                    let file = self.open.remove(&oldest).expect("found just now");
                    tracing::debug!(target: "files.file_rotated", subject = %self.subject, open = MAX_OPEN_FILES, "the oldest open file was closed to make room for another partition's file");
                    self.close(file)?;
                }
                let file = self.open_file(partition.clone())?;
                self.open.insert(partition.clone(), file);
            }
            let pooled = self.make_room()?;
            let take_bytes = self.config.take_bytes;
            let file = self.open.get_mut(&partition).expect("opened above");
            file.last_written = Instant::now();
            let room = take_bytes
                .saturating_sub(file.encoder.in_progress_bytes())
                .min(pooled)
                .min(self.config.max_file_bytes.saturating_sub(file.encoder.bytes_written()));
            let mut end = 1;
            let mut bytes = rest[0].bytes();
            while end < rest.len() && bytes + rest[end].bytes() <= room {
                bytes += rest[end].bytes();
                end += 1;
            }
            let (slice, later) = rest.split_at(end);
            file.encoder.write(slice);
            rest = later;
            if file.encoder.in_progress_bytes() >= take_bytes {
                upload_taken(file)?;
            }
            // Within a 64th of its limit a file is full: a slice there would be a few rows at a time.
            let full = |f: &OpenFile| {
                f.encoder.bytes_written() + self.config.max_file_bytes / 64 >= self.config.max_file_bytes
            };
            // The estimate counts what a codec still holds as written; a take measures it.
            if full(file) && file.encoder.in_progress_bytes() > 0 {
                upload_taken(file)?;
            }
            if full(file) {
                let file = self.open.remove(&partition).expect("opened above");
                self.close(file)?;
            }
        }
        Ok(())
    }

    /// What the open files' encoders may buffer more together: when they have buffered it all, the largest take goes
    /// to its upload first.
    fn make_room(&mut self) -> Result<usize, String> {
        let buffered: usize = self.open.values().map(|f| f.encoder.in_progress_bytes()).sum();
        if buffered < TAKE_POOL_BYTES {
            return Ok(TAKE_POOL_BYTES - buffered);
        }
        let largest = (self.open.values_mut())
            .max_by_key(|f| f.encoder.in_progress_bytes())
            .expect("a full pool has a file");
        let freed = largest.encoder.in_progress_bytes();
        upload_taken(largest)?;
        Ok(TAKE_POOL_BYTES - buffered + freed)
    }

    fn open_file(&mut self, partition: Partition) -> Result<OpenFile, String> {
        let path = self.committer.file_path(&partition, &super::objects::object_id());
        let upload = Upload::new(
            self.store.clone(),
            self.runtime.clone(),
            self.config.stall_after,
            self.config.parts_in_flight,
            self.config.attributes.clone(),
            &self.subject,
            &path,
        )?;
        Ok(OpenFile {
            encoder: self.config.format.open(&partition),
            upload,
            path,
            partition,
            uploaded: 0,
            last_written: Instant::now(),
        })
    }

    /// Ends the file and its upload; the file waits closed for the next commit.
    fn close(&mut self, file: OpenFile) -> Result<(), String> {
        let OpenFile {
            encoder,
            upload,
            path,
            partition,
            uploaded,
            ..
        } = file;
        let record_count = encoder.rows();
        let (tail, metadata) = encoder.finish();
        let size_bytes = (uploaded + tail.len()) as u64;
        let mut upload = upload;
        upload.write(tail)?;
        upload.finish()?;
        self.closed.push(DataFile {
            path,
            partition,
            size_bytes,
            record_count,
            metadata: metadata.map(Arc::new),
        });
        Ok(())
    }

    /// Closes every open file and commits them with what was closed before; every pending batch completes. `Err`
    /// is the destination failed: nothing is open, closed or pending any more.
    fn commit(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        match self.try_commit() {
            Ok(()) => {
                self.held = 0;
                self.next_commit = Instant::now() + self.config.commit_interval;
                Ok(self
                    .pending
                    .drain(..)
                    .map(|batch| BatchCompletion {
                        batch,
                        rejections: Vec::new(),
                        throttled: false,
                        undelivered: None,
                    })
                    .collect())
            }
            Err(e) => Err(self.fail(e)),
        }
    }

    fn try_commit(&mut self) -> Result<(), String> {
        let open = self.open.drain().map(|(_, f)| f).collect::<Vec<_>>();
        for file in open {
            // The files behind a failed one are dropped, which aborts their uploads.
            self.close(file)?;
        }
        if self.closed.is_empty() {
            // A batch with nothing for the table completes at hand-over, so nothing is pending either.
            return Ok(());
        }
        let mut retry = Retry::new(Instant::now(), self.config.stall_after);
        let mut maybe_landed = false;
        // Whether a failure other than a conflict was logged: the table stays unwritten while it is tried again.
        let mut warned = false;
        loop {
            let error = match self.committer.commit(&self.closed) {
                Ok(()) => {
                    tracing::info!(
                        target: "files.committed",
                        subject = %self.subject,
                        files = self.closed.len(),
                        rows = self.closed.iter().map(|f| f.record_count).sum::<u64>(),
                        after_failure = warned.then_some(true),
                        "files were committed to the table"
                    );
                    self.closed.clear();
                    return Ok(());
                }
                Err(e) => e,
            };
            maybe_landed |= matches!(error, CommitError::Unknown(_));
            match retry.next(&error, Instant::now()) {
                Some(wait) => {
                    match &error {
                        // Another writer committed first: ordinary for a shared table, so no warning.
                        CommitError::Conflict => tracing::debug!(
                            target: "files.commit_conflict",
                            subject = %self.subject,
                            "another writer committed first; committing again"
                        ),
                        CommitError::Transient(why) | CommitError::Unknown(why) if !warned => {
                            warned = true;
                            tracing::warn!(
                                target: "files.commit_failed",
                                subject = %self.subject,
                                error = %why,
                                stall_after_secs = self.config.stall_after.as_secs(),
                                "a commit failed; trying again until the destination stalls"
                            );
                        }
                        CommitError::Transient(_) | CommitError::Unknown(_) => {}
                    }
                    std::thread::sleep(wait)
                }
                None => {
                    let why = match error {
                        CommitError::Conflict => {
                            format!("commit lost to another writer {MAX_CONFLICT_RETRIES} times over")
                        }
                        CommitError::Transient(why) | CommitError::Unknown(why) => {
                            format!("commit did not land within {:?}: {why}", self.config.stall_after)
                        }
                    };
                    // Only this line can say the rows may land twice: the stall that follows says the commit failed.
                    tracing::error!(
                        target: "files.commit_abandoned",
                        subject = %self.subject,
                        files = self.closed.len(),
                        rows = self.closed.iter().map(|f| f.record_count).sum::<u64>(),
                        maybe_landed,
                        error = %why,
                        "a commit was given up on, so its rows are sent again; a commit whose answer was lost may \
                         have landed, and then the rows are in the table twice"
                    );
                    if maybe_landed {
                        // A snapshot that landed names these files: deleting them would leave the table naming
                        // files that are gone. Their rows are sent again, and may be in the table twice.
                        self.closed.clear();
                    }
                    return Err(why);
                }
            }
        }
    }

    /// The destination failed: aborts and forgets everything, so the batches sent again start clean. A closed file is
    /// deleted, best effort; one the store keeps is an orphan the table's own cleanup removes.
    fn fail(&mut self, why: String) -> IoError {
        tracing::warn!(
            target: "files.writer_reset",
            subject = %self.subject,
            files_aborted = self.open.len(),
            files_deleted = self.closed.len(),
            batches = self.pending.len(),
            error = %why,
            "the writer started over: its open uploads are aborted, its closed files deleted and its batches sent again"
        );
        self.abort_open();
        self.delete_closed();
        self.ended = Some(self.seen);
        self.pending.clear();
        self.held = 0;
        self.next_commit = Instant::now() + self.config.commit_interval;
        IoError::transient(format!("{}: {why}", self.name))
    }

    fn abort_open(&mut self) {
        // A dropped upload aborts.
        self.open.clear();
    }

    fn delete_closed(&mut self) {
        let mut left = 0;
        let mut first = None;
        for file in self.closed.drain(..) {
            if let Ok(location) = Path::parse(&file.path)
                && let Err(e) = wait(&self.runtime, self.config.stall_after, self.store.delete(&location))
            {
                left += 1;
                first.get_or_insert_with(|| format!("{}: {e}", file.path));
            }
        }
        if let Some(error) = first {
            tracing::warn!(
                target: "files.orphans_left",
                subject = %self.subject,
                count = left,
                error,
                "files of a commit that did not land could not be deleted, so the store keeps them as orphans"
            );
        }
    }
}

/// When a failed commit is tried again. A `Conflict` is tried at once, up to `MAX_CONFLICT_RETRIES`; a `Transient`
/// failure waits, doubling from `BACKOFF_MIN` to `BACKOFF_MAX`, until `stall_after` from the first attempt.
pub(crate) struct Retry {
    deadline: Instant,
    conflicts: usize,
    backoff: Duration,
}

impl Retry {
    pub(crate) fn new(first_attempt: Instant, stall_after: Duration) -> Retry {
        Retry {
            deadline: first_attempt + stall_after,
            conflicts: 0,
            backoff: BACKOFF_MIN,
        }
    }

    /// How long to wait before the next attempt, or `None` to give up.
    pub(crate) fn next(&mut self, error: &CommitError, now: Instant) -> Option<Duration> {
        match error {
            CommitError::Conflict => {
                self.conflicts += 1;
                (self.conflicts <= MAX_CONFLICT_RETRIES).then_some(Duration::ZERO)
            }
            CommitError::Transient(_) | CommitError::Unknown(_) => {
                if now >= self.deadline {
                    return None;
                }
                // The last attempt is at the deadline, not past it.
                let wait = self.backoff.min(self.deadline - now);
                self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
                Some(wait)
            }
        }
    }
}

/// An object written as it is made: its bytes held until they fill a part, then a multipart upload sent a part at a
/// time, each part started once fewer than `parts_in_flight` are uploading. One dropped before it finishes is aborted,
/// so the store keeps none of its parts.
pub(crate) struct Upload {
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    stall_after: Duration,
    parts_in_flight: usize,
    attributes: Attributes,
    /// The destination, as its author named it, for its lines.
    subject: String,
    /// The object's name in messages.
    name: String,
    location: Path,
    held: Vec<Bytes>,
    held_bytes: usize,
    parts: Option<WriteMultipart>,
    /// Bytes written and not yet in the store: held, waiting for a part, or in a part on its way.
    pending: Arc<AtomicUsize>,
}

impl Upload {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        runtime: Arc<tokio::runtime::Runtime>,
        stall_after: Duration,
        parts_in_flight: usize,
        attributes: Attributes,
        subject: &str,
        name: &str,
    ) -> Result<Upload, String> {
        let location = Path::parse(name).map_err(|e| format!("{name:?} is no path: {e}"))?;
        Ok(Upload {
            store,
            runtime,
            stall_after,
            parts_in_flight,
            attributes,
            subject: subject.to_string(),
            name: name.to_string(),
            location,
            held: Vec::new(),
            held_bytes: 0,
            parts: None,
            pending: Arc::default(),
        })
    }

    /// Bytes written and not yet in the store.
    pub(crate) fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// Adds `bytes`, taken whole: no copy of them is made.
    pub(crate) fn write(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        self.pending.fetch_add(bytes.len(), Ordering::Relaxed);
        self.held_bytes += bytes.len();
        self.held.push(Bytes::from(bytes));
        if self.parts.is_none() && self.held_bytes < PART_BYTES {
            return Ok(());
        }
        let parts = match &mut self.parts {
            Some(parts) => parts,
            None => {
                let options = PutMultipartOptions {
                    attributes: self.attributes.clone(),
                    ..PutMultipartOptions::default()
                };
                let opened = self.store.put_multipart_opts(&self.location, options);
                let parts =
                    wait(&self.runtime, self.stall_after, opened).map_err(|e| format!("opening {}: {e}", self.name))?;
                let parts = Box::new(Counted {
                    inner: parts,
                    pending: self.pending.clone(),
                });
                self.parts
                    .insert(WriteMultipart::new_with_chunk_size(parts, PART_BYTES))
            }
        };
        self.held_bytes = 0;
        for mut bytes in std::mem::take(&mut self.held) {
            while !bytes.is_empty() {
                // A part that failed on the way is found here, before more is written behind it. At most a part's
                // worth starts at most one part.
                wait(
                    &self.runtime,
                    self.stall_after,
                    parts.wait_for_capacity(self.parts_in_flight),
                )
                .map_err(|e| format!("uploading {}: {e}", self.name))?;
                let piece = bytes.split_to(bytes.len().min(PART_BYTES));
                // Parts start as tasks on the runtime.
                let _enter = self.runtime.enter();
                parts.put(piece);
            }
        }
        Ok(())
    }

    /// Ends the upload: the object is in the store.
    pub(crate) fn finish(mut self) -> Result<(), String> {
        let held = std::mem::take(&mut self.held);
        match self.parts.take() {
            None => {
                let options = PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                };
                let put = self.store.put_opts(&self.location, held.into_iter().collect(), options);
                wait(&self.runtime, self.stall_after, put).map_err(|e| format!("writing {}: {e}", self.name))?;
            }
            Some(mut parts) => {
                for bytes in held {
                    let _enter = self.runtime.enter();
                    parts.put(bytes);
                }
                // A finish that fails aborts the upload itself.
                wait(&self.runtime, self.stall_after, parts.finish())
                    .map_err(|e| format!("finishing {}: {e}", self.name))?;
            }
        }
        Ok(())
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if let Some(parts) = self.parts.take()
            && let Err(e) = wait(&self.runtime, self.stall_after, parts.abort())
        {
            // A part left behind is an orphan the store's own lifecycle removes.
            tracing::debug!(target: "files.upload_not_aborted", subject = %self.subject, object = %self.name, error = %e, "an upload could not be aborted, so the store keeps its parts until its lifecycle removes them");
        }
    }
}

/// A multipart upload that takes each part out of its upload's pending bytes once the part lands.
#[derive(Debug)]
struct Counted {
    inner: Box<dyn MultipartUpload>,
    pending: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MultipartUpload for Counted {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length();
        let part = self.inner.put_part(data);
        let pending = self.pending.clone();
        Box::pin(async move {
            let landed = part.await;
            pending.fetch_sub(bytes, Ordering::Relaxed);
            landed
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

/// Takes what `file`'s encoder has encoded and sends it to the upload.
fn upload_taken(file: &mut OpenFile) -> Result<(), String> {
    let bytes = file.encoder.take();
    file.uploaded += bytes.len();
    file.upload.write(bytes)
}

/// Runs one store call on `runtime`, for at most `after`: a store that stops answering fails the destination rather than
/// holding the writer, and egress behind it, for good.
pub(crate) fn wait<T, E: std::fmt::Display>(
    runtime: &tokio::runtime::Runtime,
    after: Duration,
    call: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    // The timer is made inside the runtime, which is where a timer may be made.
    match runtime.block_on(async { tokio::time::timeout(after, call).await }) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("no answer within {after:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_upload_counts_what_is_not_yet_in_the_store() {
        let mut upload = Upload::new(
            Arc::new(object_store::memory::InMemory::new()),
            super::super::objects::runtime().unwrap(),
            Duration::from_secs(5),
            2,
            Attributes::new(),
            "d",
            "a/file",
        )
        .unwrap();
        upload.write(vec![0; 10]).unwrap();
        assert_eq!(upload.pending(), 10, "held before any part starts");
        upload.write(vec![0; PART_BYTES + 5]).unwrap();
        // One part goes up; the rest waits to fill the next.
        let deadline = Instant::now() + Duration::from_secs(5);
        while upload.pending() > 15 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(upload.pending(), 15);
        upload.finish().unwrap();
    }

    #[test]
    fn a_commit_leaves_room_for_the_batches_on_their_way() {
        let mib = 1024 * 1024;
        assert_eq!(commit_at(512 * mib), 512 * mib - COMMIT_ROOM);
        // A buffer smaller than twice the room commits at half of it.
        assert_eq!(commit_at(4 * mib), 2 * mib);
    }

    #[test]
    fn conflicts_retry_at_once_up_to_the_limit() {
        let start = Instant::now();
        let mut retry = Retry::new(start, Duration::from_secs(60));
        for _ in 0..MAX_CONFLICT_RETRIES {
            assert_eq!(retry.next(&CommitError::Conflict, start), Some(Duration::ZERO));
        }
        assert_eq!(retry.next(&CommitError::Conflict, start), None);
    }

    #[test]
    fn transient_backs_off_doubling_to_the_cap_until_the_deadline() {
        let start = Instant::now();
        let stall_after = Duration::from_secs(30);
        let mut retry = Retry::new(start, stall_after);
        let transient = CommitError::Transient("down".into());
        let mut now = start;
        let mut waits = Vec::new();
        while let Some(wait) = retry.next(&transient, now) {
            waits.push(wait);
            now += wait;
        }
        assert_eq!(
            &waits[..6],
            &[200, 400, 800, 1600, 3200, 5000].map(Duration::from_millis)
        );
        assert!(waits[6..].iter().all(|w| *w <= BACKOFF_MAX));
        assert_eq!(now, start + stall_after, "the last attempt is at the deadline");
    }

    #[test]
    fn conflicts_do_not_count_against_the_deadline_and_vice_versa() {
        let start = Instant::now();
        let mut retry = Retry::new(start, Duration::from_secs(1));
        let transient = CommitError::Transient("down".into());
        assert_eq!(retry.next(&CommitError::Conflict, start), Some(Duration::ZERO));
        assert_eq!(retry.next(&transient, start), Some(Duration::from_millis(200)));
        assert_eq!(retry.next(&transient, start + Duration::from_secs(1)), None);
    }
}
