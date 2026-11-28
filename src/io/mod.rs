//! `Source` and `Destination`: the boundary the runtime is built against (design doc, Runtime stages: Source,
//! Destination). `kafka`, `logs`, `http` (with `elastic`, `splunk` and `http_post` its protocols) and
//! `objects` (on the file destination, `files`) are the live implementations; `fake` serves every test that needs no
//! broker.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use self::parquet::Columns;
use crate::core::column::DataType;
use crate::core::id::DestId;
use crate::plan::config::Payload;
use std::sync::Arc;
use std::time::Duration;

pub mod api;
mod app_sign_in;
pub mod avro;
mod batch_wait;
pub(crate) mod client_credentials;
pub mod connections;
mod csv;
pub mod delta;
pub(crate) mod discovery;
pub mod elastic;
pub mod elastic_mapping;
pub mod elastic_nodes;
mod elastic_receiver;
pub(crate) mod entra;
pub mod files;
pub mod framing;
mod gzip;
pub(crate) mod hec;
pub mod http;
mod http_api;
pub mod http_post;
pub mod iceberg;
pub mod kafka;
pub mod line_breaker;
pub mod lines;
pub mod listing;
pub mod logs;
pub mod mysql;
mod oauth;
pub mod object_source;
pub mod objects;
mod okta;
pub mod otlp;
mod pacer;
pub mod parquet;
mod parquet_variant;
pub mod pipelined;
pub mod postgres;
pub mod prometheus;
pub mod pubsub;
pub mod raw;
mod reads;
pub mod receiver;
pub mod remote_write;
pub mod s2s;
mod s2s_receiver;
pub mod senders;
pub mod sftp;
mod socket_receiver;
pub mod splunk;
pub mod sql;
pub mod sql_source;
pub mod sqs;
pub mod storage_queue;
mod stored;
mod table;
pub mod trust;
pub mod values;
mod vended;

pub use stored::{Cells, RecordObject, SourceFields, raw_object, unclaimed_of, value_at};

/// Opens `kind` as a live source. `secrets` is what this worker can obtain a credential from; one it cannot is an
/// error here, never a connection that silently fails to authenticate. A metrics source labels its subjects by the
/// names in `subjects`. A receiver borrows above its reserve from `receivers`, when given.
pub fn build_source(
    kind: &crate::plan::topology::SourceKind,
    registry: &std::sync::Arc<crate::metrics::Metrics>,
    subjects: &std::sync::Arc<crate::metrics::Subjects>,
    secrets: &crate::plan::topology::HeldSecrets,
    receivers: Option<&std::sync::Arc<crate::egress::QueuePool>>,
) -> Result<Box<dyn Source>, IoError> {
    use crate::plan::topology::SourceKind;
    match kind {
        SourceKind::Kafka(cfg) => Ok(Box::new(kafka::KafkaSource::new(cfg, secrets)?)),
        SourceKind::Metrics(cfg) => Ok(Box::new(
            crate::metrics::MetricsSource::new(registry.clone(), *cfg).with_subjects(subjects.clone()),
        )),
        SourceKind::Logs(cfg) => Ok(Box::new(logs::LogsSource::new(*cfg))),
        SourceKind::Objects(cfg) => object_source::open(cfg, secrets),
        SourceKind::Api(cfg) => api::open(cfg, secrets),
        SourceKind::Receiver(cfg) => receiver::open(cfg, secrets, receivers),
        SourceKind::Sql(cfg) => match &cfg.database {
            crate::plan::topology::SqlDatabase::Postgres(connection) => Ok(Box::new(sql_source::SqlSource::new(
                postgres::PostgresDriver::new(connection.clone(), secrets)?,
                cfg.source.clone(),
            ))),
            crate::plan::topology::SqlDatabase::MySql(connection) => Ok(Box::new(sql_source::SqlSource::new(
                mysql::MySqlDriver::new(connection.clone(), secrets)?,
                cfg.source.clone(),
            ))),
        },
    }
}

/// Leave to spawn one more sender: see [`Destination::lend_senders`].
type Lend = Box<dyn FnMut() -> bool + Send>;

/// Opens `kind` as a live destination. `records` is what it writes, route by route, which only a Parquet destination
/// needs (`CompiledTopology::records`). Every kind is compiled into every build, so any binary can run any bundle.
///
/// `metrics` reaches a transport that reports on itself: librdkafka's statistics arrive on its own thread, and an HTTP
/// destination's senders each run on one, not on the egress thread that owns this client. What an Elasticsearch or
/// Prometheus store says it did not write is in its answer, which only the protocol reads.
pub fn build_destination(
    kind: &crate::plan::topology::DestinationKind,
    id: crate::core::id::DestId,
    name: &str,
    metrics: crate::metrics::MetricsHandle,
    secrets: &crate::plan::topology::HeldSecrets,
    records: Option<&crate::plan::topology::Records>,
) -> Result<Box<dyn Destination>, IoError> {
    use crate::plan::topology::DestinationKind;
    match kind {
        DestinationKind::Kafka(c) => Ok(Box::new(kafka::KafkaDestination::new(c, id, metrics, secrets)?)),
        DestinationKind::Elasticsearch(c) => elastic::open(c, id, name, metrics, secrets),
        DestinationKind::SplunkHec(c) => splunk::open(c, id, name, metrics, secrets),
        DestinationKind::Http(c) => http_post::open(c, id, name, metrics, secrets),
        DestinationKind::LogAnalytics(c) => http_post::open_log_analytics(c, id, name, metrics, secrets),
        DestinationKind::Otlp(c) => otlp::export::open(c, id, name, metrics, secrets),
        DestinationKind::Prometheus(c) => remote_write::open(c, id, name, metrics, secrets),
        DestinationKind::Objects(c) => objects::open(c, name, records, secrets),
        DestinationKind::Iceberg(c) => Ok(Box::new(iceberg::table::destination(c, name, records, secrets)?)),
        DestinationKind::Delta(c) => Ok(Box::new(delta::table::destination(c, name, records, secrets)?)),
    }
}

/// One record read from a source: its row of the chunk its poll's records share, and the coordinates it commits
/// against. Which of those coordinates a pipeline may read is the source kind's to declare
/// (`plan::topology::SourceMeta`). `topic` is an `Arc<str>` so a source interns each name once per poll.
#[derive(Clone, Debug)]
pub struct SourceRecord {
    /// What the source read, as one row of `chunk`. Every record one `Source::poll_batch` of typed rows returns points
    /// into the same chunk; a poll of bytes may point into several.
    pub chunk: Arc<RecordChunk>,
    pub row: u32,
    pub offset: i64,
    pub partition: i32,
    pub topic: Arc<str>,
    /// Each header's key and value, in the record's order. A header with no value is left out. `None` where the source
    /// was not keeping them (`Source::keep_headers`), so a record without headers is told apart from one whose headers
    /// were not read.
    pub headers: Option<Vec<(String, Vec<u8>)>>,
}

impl SourceRecord {
    /// The bytes this record read, if its source reads bytes.
    pub fn bytes(&self) -> Option<&[u8]> {
        self.chunk.bytes(self.row as usize)
    }

    /// The bytes of what this record read: its own row's values, not the whole chunk it points into.
    pub(crate) fn read_size(&self) -> usize {
        self.chunk.row_size(self.row as usize)
    }

    /// The bytes this record holds: what it read, every header it kept, and what its chunk files it under, which a
    /// reader that needs rows copies to each. What batch and queue limits count.
    pub fn size(&self) -> usize {
        let filed: usize = self
            .chunk
            .filed()
            .into_iter()
            .flatten()
            .map(|(k, v)| k.len() + v.len())
            .sum();
        self.read_size() + headers_size(&self.headers) + filed
    }
}

/// Point `records` at chunks of their own rows only, where they share a chunk with records not among them: a record
/// kept past its batch would otherwise keep its whole poll in memory. Records that are every row of their chunk keep
/// it, and records that shared a chunk share the new one.
pub(crate) fn own_rows(records: &mut [SourceRecord]) {
    let mut chunks: Vec<(Arc<RecordChunk>, Vec<usize>)> = Vec::new();
    for (i, record) in records.iter().enumerate() {
        match chunks.iter_mut().find(|(c, _)| Arc::ptr_eq(c, &record.chunk)) {
            Some((_, at)) => at.push(i),
            None => chunks.push((record.chunk.clone(), vec![i])),
        }
    }
    for (chunk, at) in chunks {
        if at.len() == chunk.len() {
            continue;
        }
        let rows: Vec<u32> = at.iter().map(|&i| records[i].row).collect();
        let own = Arc::new(chunk.take_rows(&rows));
        for (row, i) in at.into_iter().enumerate() {
            records[i].chunk = own.clone();
            records[i].row = row as u32;
        }
    }
}

/// The bytes `headers` hold, as `SourceRecord::size` counts them.
fn headers_size(headers: &Option<Vec<(String, Vec<u8>)>>) -> usize {
    headers.iter().flatten().map(|(k, v)| k.len() + v.len()).sum()
}

/// Where a record was read, as `SourceRecord` carries it: its offset, partition, topic and headers.
type ReadAt = (i64, i32, Arc<str>, Option<Vec<(String, Vec<u8>)>>);

/// A reader thread's buffers for the chunks it builds. Each chunk gives its buffers back when its last record lets go,
/// on whatever thread that is, so a reader's next poll reuses them rather than mapping fresh pages; and the next poll
/// reserves what the last one took, so it does not grow by copying.
struct Home {
    pool: crate::core::buffer::BufferPool,
    rows: usize,
    bytes: usize,
}

thread_local! {
    static HOME: Arc<crate::sync::Mutex<Home>> = Arc::new(crate::sync::Mutex::new(Home {
        pool: crate::core::buffer::BufferPool::new(),
        rows: 0,
        bytes: 0,
    }));
}

/// Builds one poll's records of a byte source: each record's bytes go into the chunk the poll's records share, copied
/// once from wherever the source read them.
pub struct RecordChunkBuilder {
    home: Arc<crate::sync::Mutex<Home>>,
    raw: crate::core::column::StrBuilder,
    at: Vec<ReadAt>,
    /// Each record's time as its source stamped it, in microseconds since the epoch.
    time: Vec<Option<i64>>,
    size: usize,
}

impl Default for RecordChunkBuilder {
    fn default() -> Self {
        let home = HOME.with(Arc::clone);
        let (raw, rows) = {
            let mut h = home.lock();
            let (rows, bytes) = (h.rows, h.bytes);
            let raw = crate::core::column::StrBuilder::growing_with(
                &mut h.pool,
                crate::core::column::VarType::Bytes,
                rows,
                bytes,
            );
            (raw, rows)
        };
        RecordChunkBuilder {
            home,
            raw,
            at: Vec::with_capacity(rows),
            time: Vec::with_capacity(rows),
            size: 0,
        }
    }
}

impl RecordChunkBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// A record of `raw`, read at `offset` of `partition` of `topic`, with `headers`.
    pub fn push(
        &mut self,
        raw: &[u8],
        offset: i64,
        partition: i32,
        topic: Arc<str>,
        headers: Option<Vec<(String, Vec<u8>)>>,
    ) {
        self.push_stamped(raw, offset, partition, topic, headers, None);
    }

    /// A record as `push` takes it, stamped by its source at `time`, in microseconds since the epoch.
    pub(crate) fn push_stamped(
        &mut self,
        raw: &[u8],
        offset: i64,
        partition: i32,
        topic: Arc<str>,
        headers: Option<Vec<(String, Vec<u8>)>>,
        time: Option<i64>,
    ) {
        self.time.push(time);
        if self.raw.fits(raw.len()) {
            self.raw.push(raw);
        } else {
            let mut home = self.home.lock();
            self.raw.push_grow(&mut home.pool, raw);
        }
        self.size += raw.len() + headers_size(&headers);
        self.at.push((offset, partition, topic, headers));
    }

    pub fn len(&self) -> usize {
        self.at.len()
    }

    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    /// The bytes of the records pushed, as `SourceRecord::size` counts each.
    fn size(&self) -> usize {
        self.size
    }

    /// Take back the record last pushed: its bytes, and its offset, partition, topic and headers.
    pub fn pop(&mut self) -> Option<(Vec<u8>, ReadAt)> {
        let at = self.at.pop()?;
        self.time.pop();
        let raw = self.raw.last().to_vec();
        self.raw.pop_last();
        self.size -= raw.len() + headers_size(&at.3);
        Some((raw, at))
    }

    /// The records pushed, in order, sharing one chunk.
    pub fn finish(self) -> Vec<SourceRecord> {
        let (chunk, at) = self.split();
        let chunk = Arc::new(chunk);
        at.into_iter()
            .enumerate()
            .map(|(row, (offset, partition, topic, headers))| SourceRecord {
                chunk: chunk.clone(),
                row: row as u32,
                offset,
                partition,
                topic,
                headers,
            })
            .collect()
    }

    /// The records pushed, in order, as one chunk, without where each was read.
    fn into_chunk(self) -> RecordChunk {
        self.split().0
    }

    fn split(self) -> (RecordChunk, Vec<ReadAt>) {
        let column = {
            let mut home = self.home.lock();
            let bytes = self.size - self.at.iter().map(|at| headers_size(&at.3)).sum::<usize>();
            (home.rows, home.bytes) = (self.at.len(), bytes);
            let column = self.raw.finish_growing();
            // Frees what the polls before outgrew: the next poll reserves its room up front.
            home.pool.trim();
            column
        };
        let mut chunk = RecordChunk::new(vec![column], Vec::new());
        chunk.home = Some(self.home);
        // A source that stamps nothing pays nothing past the poll.
        if self.time.iter().any(Option::is_some) {
            chunk.time = Some(self.time);
        }
        (chunk, self.at)
    }
}

/// The rows of a chunk cut where its records arrived, which its source's reader hands out in order.
struct ChunkRows {
    chunk: Arc<RecordChunk>,
    at: usize,
}

impl ChunkRows {
    fn new(chunk: RecordChunk) -> ChunkRows {
        ChunkRows {
            chunk: Arc::new(chunk),
            at: 0,
        }
    }

    /// The row taken next.
    fn at(&self) -> usize {
        self.at
    }

    fn is_done(&self) -> bool {
        self.at == self.chunk.len()
    }

    /// What the next row holds.
    fn next_size(&self) -> usize {
        self.chunk.row_size(self.at)
    }

    /// What the rows not yet taken hold.
    fn untaken(&self) -> usize {
        (self.at..self.chunk.len()).map(|row| self.chunk.row_size(row)).sum()
    }

    /// Takes the next row.
    fn take(&mut self) -> (Arc<RecordChunk>, u32) {
        let row = self.at as u32;
        self.at += 1;
        (self.chunk.clone(), row)
    }
}

/// One poll's rows of a source whose data arrives already in typed columns: a column per field the source declares,
/// in declaration order, and each value that could not be read as its field's type. Lives as long as any record that
/// points into it.
pub struct RecordChunk {
    columns: Vec<crate::core::column::Column>,
    /// Each value that is not its field's type, null in its column: its row, its field's index, and the value as the
    /// source spelled it, by row ascending.
    failed: Vec<(u32, usize, String)>,
    /// Where its buffers go back to: the reader thread's that built it, if one did.
    home: Option<Arc<crate::sync::Mutex<Home>>>,
    /// What every record of it is filed under (`hec::Filed`), where its source files them under one value. A record's
    /// own `headers` are `None` then.
    filed: Option<Arc<hec::Filed>>,
    /// Each row's time as its source stamped it, in microseconds since the epoch, where its source stamps one.
    time: Option<Vec<Option<i64>>>,
    /// Each row whose payload did not read at all, and why, by row ascending. Its every field is null.
    unread: Vec<(u32, String)>,
    /// The Avro writers' schemas its rows were read with, newest last, where its source reads Avro.
    avro_writers: Vec<Arc<avro::Parsed>>,
    /// What each row's values hold, counted once when the chunk is made: every limit asks it of each record.
    sizes: Vec<u32>,
}

impl RecordChunk {
    /// A chunk of `columns`, each as long as the others, in the order the source declares its fields.
    pub(crate) fn new(columns: Vec<crate::core::column::Column>, mut failed: Vec<(u32, usize, String)>) -> RecordChunk {
        let rows = columns.first().map_or(0, crate::core::column::Column::len);
        assert!(
            columns.iter().all(|c| c.len() == rows),
            "every column of a chunk has the chunk's rows"
        );
        failed.sort_by_key(|(row, field, _)| (*row, *field));
        let mut sizes = vec![0; rows];
        for column in &columns {
            column.add_row_sizes(&mut sizes);
        }
        RecordChunk {
            sizes,
            columns,
            failed,
            home: None,
            filed: None,
            time: None,
            unread: Vec::new(),
            avro_writers: Vec::new(),
        }
    }

    /// This chunk, each row stamped by its source at `time`, as `RecordChunkBuilder::push_stamped` takes it.
    fn stamped(mut self, time: Vec<Option<i64>>) -> RecordChunk {
        assert_eq!(time.len(), self.len(), "a time per row");
        if time.iter().any(Option::is_some) {
            self.time = Some(time);
        }
        self
    }

    /// This chunk, each of `unread`'s rows one whose payload did not read, and why.
    pub(crate) fn with_unread(mut self, mut unread: Vec<(u32, String)>) -> RecordChunk {
        unread.sort_by_key(|(row, _)| *row);
        self.unread = unread;
        self
    }

    /// Why row `row`'s payload did not read, if it did not.
    pub fn unread(&self, row: u32) -> Option<&str> {
        let at = self.unread.binary_search_by_key(&row, |(r, _)| *r).ok()?;
        Some(&self.unread[at].1)
    }

    /// This chunk, its rows read with `writers`, newest last.
    pub(crate) fn read_with(mut self, writers: Vec<Arc<avro::Parsed>>) -> RecordChunk {
        self.avro_writers = writers;
        self
    }

    /// The Avro writers' schemas this chunk's rows were read with, newest last.
    pub(crate) fn avro_writers(&self) -> &[Arc<avro::Parsed>] {
        &self.avro_writers
    }

    /// This chunk, its every record filed under `filed`.
    pub(crate) fn filed_under(mut self, filed: Arc<hec::Filed>) -> RecordChunk {
        self.filed = Some(filed);
        self
    }

    /// What every record of this chunk is filed under, where they share one value.
    pub fn filed(&self) -> Option<&hec::Filed> {
        self.filed.as_deref()
    }

    /// Row `row`'s time as its source stamped it, in microseconds since the epoch, or `None` where it stamped none.
    pub(crate) fn time(&self, row: usize) -> Option<i64> {
        self.time.as_ref()?[row]
    }

    /// The rows the chunk holds.
    fn len(&self) -> usize {
        self.columns.first().map_or(0, crate::core::column::Column::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A one-row chunk of `raw`, as a byte source's poll of one record makes.
    pub fn of_bytes(raw: impl AsRef<[u8]>) -> Arc<RecordChunk> {
        let mut pool = crate::core::buffer::BufferPool::new();
        let mut b = crate::core::column::StrBuilder::new(&mut pool, crate::core::column::VarType::Bytes, 1);
        b.push_grow(&mut pool, raw.as_ref());
        Arc::new(RecordChunk::new(vec![b.finish()], Vec::new()))
    }

    /// Row `row`'s bytes, where this is a byte source's chunk: one column, of the bytes each record read.
    fn bytes(&self, row: usize) -> Option<&[u8]> {
        self.read().map(|v| v.get(row))
    }

    /// Every row's bytes, where this is a byte source's chunk.
    pub(crate) fn read(&self) -> Option<crate::core::column::VarLen<'_>> {
        match &self.columns[..] {
            [column] if column.data_type() == DataType::Bytes => column.varlen().ok(),
            _ => None,
        }
    }

    /// A chunk of only `rows` of this one, in that order, each failed value moved with its row.
    fn take_rows(&self, rows: &[u32]) -> RecordChunk {
        let mut pool = crate::core::buffer::BufferPool::new();
        let columns = self.columns.iter().map(|c| c.gather(&mut pool, rows)).collect();
        let failed = rows
            .iter()
            .enumerate()
            .flat_map(|(to, &row)| {
                let from = self.failed.partition_point(|(r, _, _)| *r < row);
                self.failed[from..]
                    .iter()
                    .take_while(move |(r, _, _)| *r == row)
                    .map(move |(_, field, value)| (to as u32, *field, value.clone()))
            })
            .collect();
        let mut chunk = RecordChunk::new(columns, failed);
        chunk.filed = self.filed.clone();
        chunk.avro_writers = self.avro_writers.clone();
        chunk.time = (self.time.as_ref()).map(|time| rows.iter().map(|&row| time[row as usize]).collect());
        chunk.unread = (rows.iter().enumerate())
            .filter_map(|(to, &row)| Some((to as u32, self.unread(row)?.to_string())))
            .collect();
        chunk
    }

    pub fn column(&self, field: usize) -> &crate::core::column::Column {
        &self.columns[field]
    }

    /// Each value that is not its field's type: its row, its field's index, and the value as the source spelled it.
    pub fn failed(&self) -> &[(u32, usize, String)] {
        &self.failed
    }

    /// What one row's values hold, as `Column::add_row_sizes` counts them. A chunk of no columns holds nothing.
    fn row_size(&self, row: usize) -> usize {
        self.sizes.get(row).map_or(0, |&size| size as usize)
    }
}

impl fmt::Debug for RecordChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RecordChunk")
            .field("columns", &self.columns.len())
            .field("failed", &self.failed)
            .finish()
    }
}

impl Drop for RecordChunk {
    /// The last record to let go of a chunk may be on any thread: its columns go back to the reader that built it, or
    /// to a pool of their own.
    fn drop(&mut self) {
        match &self.home {
            Some(home) => {
                let mut home = home.lock();
                for column in self.columns.drain(..) {
                    column.reclaim(&mut home.pool);
                }
            }
            None => {
                let mut pool = crate::core::buffer::BufferPool::new();
                for column in self.columns.drain(..) {
                    column.reclaim(&mut pool);
                }
            }
        }
    }
}

/// Which topic-partition an offset belongs to. Keyed on the partition alone, two topics sharing a number would commit
/// over each other.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicPartition {
    pub topic: Arc<str>,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: Arc<str>, partition: i32) -> TopicPartition {
        TopicPartition { topic, partition }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}

/// Per-topic-partition watermark to commit.
pub type CommitMap = HashMap<TopicPartition, i64>;

/// What a single-worker source has delivered, in one of two shapes or both. A cursor is one position, for a source
/// whose own protocol orders what it holds. A ledger names each item read, for one that does not: `added` replaces
/// an item's entry, `removed` forgets an item the source no longer holds, and removals apply first. A change carries
/// only what is new; a resume carries the whole.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<crate::heartbeat::LedgerEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
}

impl Progress {
    /// Drops what is over the limits a ledger keeps (`heartbeat::MAX_CURSOR`, `MAX_LEDGER_ITEM`,
    /// `MAX_LEDGER_ENTRY`), and answers whether the cursor went and how many entries did. What is dropped is read
    /// again by the next holder: duplicates, never lost rows.
    pub(crate) fn admit(&mut self) -> (bool, usize) {
        use crate::heartbeat::{MAX_CURSOR, MAX_LEDGER_ENTRY, MAX_LEDGER_ITEM};
        let cursor_refused = self.cursor.as_ref().is_some_and(|c| c.len() > MAX_CURSOR);
        if cursor_refused {
            self.cursor = None;
        }
        let before = self.added.len() + self.removed.len();
        self.added
            .retain(|e| e.item.len() <= MAX_LEDGER_ITEM && e.entry.len() <= MAX_LEDGER_ENTRY);
        // An item past the limit was never kept, so there is nothing to forget.
        self.removed.retain(|item| item.len() <= MAX_LEDGER_ITEM);
        (cursor_refused, before - self.added.len() - self.removed.len())
    }
}

/// One partition's lag, as a source last sampled it: what the source has yet to finish there (E6 -- lag as an operator
/// would confirm it, not the in-flight read position). For Kafka, the broker's high-watermark minus the committed
/// offset; for a queue, every message it holds, as one partition named for the queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionLag {
    pub partition: TopicPartition,
    pub lag: i64,
}

/// Something went wrong talking to the outside world.
#[derive(Debug)]
pub struct IoError {
    pub message: String,
    kind: IoErrorKind,
}

/// Whether retrying the same data could ever succeed. A destination that cannot be reached is waited for: at-least-once
/// forbids committing past unacked data. One that has looked at a record and refused it will refuse it identically
/// forever, and retrying that wedges the partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum IoErrorKind {
    /// Unreachable, timed out, busy, queue full. Retried indefinitely, and reported through
    /// `smeltr_destination_healthy`.
    #[default]
    Transient,
    /// The destination examined this record and refused it. Never retried; the destination's answer to `Refused`
    /// decides.
    Rejected,
    /// The answer was larger than the worker holds. The same request draws the same answer, so it is never sent again
    /// whole: a batch is split, and one record is refused.
    Oversized,
    /// The service asked to be sent less, and to wait at least this long before asking again. Not a stall: the
    /// service answered. Egress sends the rows again once the wait is over, and the pacing model counts them lost.
    Throttled(Duration),
}

impl IoError {
    pub fn transient(message: impl Into<String>) -> IoError {
        IoError {
            message: message.into(),
            kind: IoErrorKind::Transient,
        }
    }

    /// The destination refused this record and always will.
    fn rejected(message: impl Into<String>) -> IoError {
        IoError {
            message: message.into(),
            kind: IoErrorKind::Rejected,
        }
    }

    pub fn is_rejection(&self) -> bool {
        self.kind == IoErrorKind::Rejected
    }

    /// The answer could not be held whole.
    pub fn oversized(message: impl Into<String>) -> IoError {
        IoError {
            message: message.into(),
            kind: IoErrorKind::Oversized,
        }
    }

    fn is_oversized(&self) -> bool {
        self.kind == IoErrorKind::Oversized
    }

    /// The service asked to be sent less, and to wait `wait` first.
    pub fn throttled(message: impl Into<String>, wait: Duration) -> IoError {
        IoError {
            message: message.into(),
            kind: IoErrorKind::Throttled(wait),
        }
    }

    /// The wait the service asked for, when it asked to be sent less.
    pub fn throttled_wait(&self) -> Option<Duration> {
        match self.kind {
            IoErrorKind::Throttled(wait) => Some(wait),
            _ => None,
        }
    }
}

/// Where Entra signs in a client of the Azure service at `address`, a host or a URL: in the national cloud whose
/// domain its host is under, else in Azure's public cloud.
pub(crate) fn entra_authority(address: &str) -> &'static str {
    const CLOUDS: &[(&str, &str)] = &[
        (".usgovcloudapi.net", "https://login.microsoftonline.us"),
        (".azure.us", "https://login.microsoftonline.us"),
        (".chinacloudapi.cn", "https://login.chinacloudapi.cn"),
        (".azure.cn", "https://login.chinacloudapi.cn"),
    ];
    let address = address.trim();
    let url = address
        .contains("://")
        .then(|| reqwest::Url::parse(address).ok())
        .flatten();
    let host = match &url {
        Some(url) => url.host_str().unwrap_or_default(),
        None => address,
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    CLOUDS
        .iter()
        .find(|(domain, _)| host.ends_with(domain))
        .map_or("https://login.microsoftonline.com", |(_, authority)| authority)
}

/// The value of the credential `what` takes its `name` from. One this worker cannot obtain is an error, in the one
/// sentence every connector gives for it, never a connection that silently goes without.
fn held(
    secrets: &crate::plan::topology::HeldSecrets,
    reference: &crate::plan::topology::SecretRef,
    what: &str,
    name: &str,
) -> Result<String, IoError> {
    secrets.value(reference).ok_or_else(|| {
        IoError::transient(format!(
            "{what} takes its {name} from {}, which this worker does not have",
            reference.describe()
        ))
    })
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// One record a destination refused, by its position in the records sent to that destination for the batch. A
/// destination whose acknowledgment is batched refuses after `send` has returned, and the position is enough to
/// find the record again.
#[derive(Clone, Debug, PartialEq)]
pub struct Rejection {
    pub doc: usize,
    pub reason: String,
    /// The record as it was sent, from whoever still held it when the refusal came back: `send` has it,
    /// and so does egress; a completion from a transport does not. What a DLQ is sent.
    pub record: Option<OwnedRecord>,
}

/// Refusals a destination set to block keeps and sends again on its own, reported so the source can be blocked. The
/// batch is not finished: its completion follows once the destination takes them.
#[derive(Clone, Debug, PartialEq)]
pub struct HeldRefusal {
    pub batch: BatchId,
    pub rejections: Vec<Rejection>,
}

impl std::error::Error for IoError {}

/// A source of records, driven by one reader thread. `Send` so it may be built on one thread and run on another.
///
/// # A source must bring its own work assignment
///
/// Nothing in smeltr decides which worker reads which shard: every worker in a group runs the same bundle, so a source
/// whose shards are not divided by something outside smeltr is read in full by every worker -- permanent N-fold
/// duplication, not redelivery. An implementation must satisfy one of: the transport assigns (Kafka's group protocol,
/// SQS's visibility timeout); the store can be made to assign (a lease claimed where the data lives -- timeouts, not
/// fencing, since a split-brain reader only duplicates); or its kind declares itself single-worker
/// (`SourceKind::single_worker`), the leader holds it for one worker of the group, and it hands its progress over
/// through `take_progress` and `resume`.
pub trait Source: Send {
    /// Blocking. Up to `max_rows` records and `max_bytes` of raw payload, whichever binds first; a single record over
    /// `max_bytes` still comes back alone, since refusing it would stall the partition forever. Returns what it has
    /// once no more is ready, without waiting to fill the batch; with nothing, gives up after `timeout`.
    ///
    /// Bytes are the real cap. `max_rows` is a backstop: per-row overhead is fixed and independent of payload, so a
    /// batch of many tiny records blows past a byte budget in column and tracking memory while still measuring small.
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError>;

    /// Record `offsets` as durable progress. Called only once every destination a batch reached has acknowledged it.
    /// A source may answer before the offsets are durable and report the refusal on a later call.
    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError>;

    /// Stop supplying without leaving the group: the reader keeps calling `poll_batch` and gets nothing. Pausing the
    /// partitions rather than stopping the poll is the point -- a consumer that stops polling is evicted after
    /// `max.poll.interval.ms`, and the rebalance spreads one destination's problem across every partition the group
    /// owns. Idempotent; a source with no membership does nothing.
    fn block(&mut self) -> Result<(), IoError> {
        Ok(())
    }

    /// Resume after `block`. Idempotent.
    fn release(&mut self) -> Result<(), IoError> {
        Ok(())
    }

    /// Its deployment is stopping: take nothing more from outside, and say whether it still holds what it took and
    /// must hand over before it ends. The reader polls it while it says so. The default holds nothing: a source that
    /// reads at its own pace reads again after a restart.
    fn stop_taking(&mut self) -> bool {
        false
    }

    /// Records the source let go before they were read, since the last call: ones a sender could not be made to wait
    /// for, which arrived while the source had no room. The default is none: a source that can wait never lets go.
    fn take_dropped(&mut self) -> u64 {
        0
    }

    /// How often, since the last call, the source had no room for what a sender offered: a request it answered busy,
    /// or a connection it stopped reading. A receiver's memory, not its sender, set the pace each time. The default is
    /// never.
    fn take_full(&mut self) -> u64 {
        0
    }

    /// The bytes it holds now that its reader has not taken: what senders sent a receiver, its reserve's and what it
    /// shares, what a shared client routed to its inbox, or what a poll or an object read and has not handed out.
    /// `None` for a source that holds nothing of its own.
    fn held_bytes(&self) -> Option<u64> {
        None
    }

    /// Whether [`Source::lag`] counts messages rather than records, drained as [`Source::take_finished`] reports them.
    /// Asked once, when the source starts. The default is records.
    fn counts_messages(&self) -> bool {
        false
    }

    /// Messages the source has finished with since the last call, for a source that [counts
    /// messages](Source::counts_messages): what it drains its lag at. The default is none.
    fn take_finished(&mut self) -> u64 {
        0
    }

    /// Requests the source answered with a refusal since the last call, counted by reason: a sender with the wrong
    /// token, a body it cannot read. The default is none: only a source that senders talk to can refuse one.
    fn take_refused(&mut self) -> Vec<(&'static str, u64)> {
        Vec::new()
    }

    /// Copy what arrives to `tap` before it is framed, while it is watching. Only a receiver has bytes a sender
    /// sent before they are records, so the default copies nothing.
    fn tap_raw(&mut self, _tap: std::sync::Arc<dyn raw::RawTap>) {}

    /// Whether to fill `SourceRecord::headers` from now on. Copying them costs an allocation per header, so a source
    /// leaves them out until a reader asks. A source whose records have no headers does nothing.
    fn keep_headers(&mut self, _keep: bool) {}

    /// Partitions the source has lost since the last call: its group moved them to another member. A batch read from
    /// one of them before the move must not commit there. The default is none: a source with no membership keeps what
    /// it has.
    fn take_revoked(&mut self) -> Vec<TopicPartition> {
        Vec::new()
    }

    /// Take nothing, and serve for up to `timeout` what the source's protocol asks of a member: called in place of
    /// `poll_batch` while the reader can take no more work, so a source with group membership neither holds up its
    /// group's rebalances nor is evicted. What a source supplies anyway is kept for the next `poll_batch`. The default
    /// does nothing: a source with no membership has nothing to keep.
    fn keep_membership(&mut self, _timeout: Duration) -> Result<(), IoError> {
        Ok(())
    }

    /// `true` once this source will never produce another record: every partition it reads is at its stop. A source
    /// with no stop never is. Read by one-shot drivers, not by the runtime.
    fn is_exhausted(&self) -> bool {
        false
    }

    /// Every `(topic, partition)` this source owns, for the heartbeat.
    fn assigned_partitions(&self) -> Vec<(String, i32)> {
        Vec::new()
    }

    /// This call's lag samples (E6): a source kind declares whether it can report lag at all by overriding this,
    /// and only those that can show it. The default answers "cannot". A sample costs a call to the broker or the
    /// queue's service, which a live source kind pays on its own cadence and never on the caller's thread, so an empty
    /// result also covers "can, but nothing new since the last sample."
    fn lag(&mut self) -> Vec<PartitionLag> {
        Vec::new()
    }

    /// What this source has delivered since the last call, for a single-worker source to resume from on whichever
    /// worker holds it next. Read after each poll and each commit. The default is none: a source whose progress lives
    /// in its transport keeps it there.
    fn take_progress(&mut self) -> Option<Progress> {
        None
    }

    /// Resume from what an earlier holder delivered. Called once, before the first `poll_batch`, on a source opened
    /// for a hold. The default ignores it.
    fn resume(&mut self, _from: Progress) -> Result<(), IoError> {
        Ok(())
    }

    /// This source's most recent client-connectivity error, if it has not proven itself reachable since (E4): a
    /// broker that does not resolve, refuses the connection, or is unreachable behind a firewall -- the failure a
    /// source with nothing to read is otherwise indistinguishable from. `None` for a source kind that cannot
    /// observe this at the client level, or one that is currently healthy; the common case, so this costs nothing
    /// for most polls and for every source kind that has not overridden it.
    fn connection_error(&self) -> Option<String> {
        None
    }

    /// The name the bundle gives this source, for the lines it logs itself. The default keeps none: a source that
    /// logs nothing of its own has no use for it.
    fn named(&mut self, _name: &str) {}
}

/// One record on its way out. `key` and `doc_id` are separate channels: a partition key changes how every record on a
/// Kafka destination is placed, and a document identity must never be routed into it.
#[derive(Clone, Debug)]
pub struct OutgoingRecord<'a> {
    pub value: &'a [u8],
    /// What `value` is: what the destination it is bound for writes.
    pub payload: &'a Payload,
    /// Why the row has no `payload` to write, so its destination refuses it.
    pub unfit: Option<Cow<'a, str>>,
    /// The row's value of the `Placement` this destination reads: its topic, index or key prefix. `None` leaves the
    /// destination on its configured one.
    pub placement: Option<&'a str>,
    /// `__destination.id`, for a destination that can key a document on one.
    doc_id: Option<&'a str>,
    /// The partition key. Nothing in the pipeline sets it yet.
    pub key: Option<&'a [u8]>,
    /// A record another destination refused, on its way to a DLQ. The DLQ's own answer to `Refused` answers if it
    /// refuses it too; dropped there, it is counted lost.
    salvaged: bool,
    /// Every DLQ down its destination's chain, and how a refusal of this record reaches each. `None` sends it to
    /// every DLQ its destination names, keyed as here.
    salvage_to: Option<&'a [SalvageTarget]>,
    /// The row in the forms the DLQs down its destination's chain write, but its own.
    kept: &'a [(Payload, Arc<[u8]>)],
    /// The route whose pipeline made this record, by its index in `CompiledTopology::routes`. `None` for a record no
    /// pipeline ran on. A destination that writes a schema writes each route's records under that route's record.
    route: Option<u32>,
    /// The record as Parquet columns, for a destination that writes them (`io::parquet`). Cut at egress for a row
    /// whose destinations, or the DLQs down their chains, write columns.
    columns: Option<&'a Columns>,
}

/// One DLQ a refused record may be sent to, as worked out while the row's fields were still at hand.
#[derive(Clone, Debug, PartialEq)]
pub struct SalvageTarget {
    pub dlq: DestId,
    /// The identity the record is keyed on there.
    pub doc_id: Option<String>,
    /// A delivery rule already sent the record there, so a refusal does not send it again.
    pub reached: bool,
}

impl<'a> OutgoingRecord<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        OutgoingRecord {
            value,
            payload: &Payload::Record,
            unfit: None,
            placement: None,
            doc_id: None,
            key: None,
            salvaged: false,
            salvage_to: None,
            kept: &[],
            route: None,
            columns: None,
        }
    }

    pub fn with_salvage_to(mut self, plan: Option<&'a [SalvageTarget]>) -> Self {
        self.salvage_to = plan;
        self
    }

    pub fn salvaged(mut self) -> Self {
        self.salvaged = true;
        self
    }

    pub fn with_placement(mut self, placement: Option<&'a str>) -> Self {
        self.placement = placement;
        self
    }

    pub fn with_doc_id(mut self, doc_id: Option<&'a str>) -> Self {
        self.doc_id = doc_id;
        self
    }

    pub fn with_key(mut self, key: Option<&'a [u8]>) -> Self {
        self.key = key;
        self
    }

    /// What this record holds once owned, less what it shares with other records -- the chunk its columns point into
    /// and the source forms it kept -- which [`held_bytes`] counts: an identity, a placement or a salvage plan is
    /// held as long as the record is. A plan names every DLQ down the chain, so a long chain can outweigh a small
    /// record.
    fn bytes(&self) -> usize {
        self.value.len()
            + self.key.map_or(0, <[u8]>::len)
            + self.placement.map_or(0, str::len)
            + self.doc_id.map_or(0, str::len)
            + self.unfit.as_deref().map_or(0, str::len)
            + self
                .salvage_to
                .unwrap_or_default()
                .iter()
                .map(|t| std::mem::size_of::<SalvageTarget>() + t.doc_id.as_ref().map_or(0, String::len))
                .sum::<usize>()
    }
}

/// An [`OutgoingRecord`] held past the batch it borrows from: queued for a destination on another thread, or handed
/// back with its refusal.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OwnedRecord {
    pub value: Vec<u8>,
    pub payload: Payload,
    pub unfit: Option<String>,
    pub placement: Option<String>,
    pub doc_id: Option<String>,
    pub key: Option<Vec<u8>>,
    pub salvaged: bool,
    pub salvage_to: Option<Vec<SalvageTarget>>,
    pub kept: Vec<(Payload, Arc<[u8]>)>,
    pub route: Option<u32>,
    pub columns: Option<Columns>,
    /// The other rows of this record's source record, sent to a DLQ that writes source bytes as this one copy. They
    /// hold no value of their own: refused, each takes this one's back and goes on as itself.
    pub riders: Vec<OwnedRecord>,
}

impl OwnedRecord {
    /// The rows this record stands for: itself and its riders.
    pub(crate) fn rows(&self) -> u64 {
        1 + self.riders.len() as u64
    }

    pub fn from(r: &OutgoingRecord<'_>) -> Self {
        OwnedRecord {
            value: r.value.to_vec(),
            payload: r.payload.clone(),
            unfit: r.unfit.as_deref().map(str::to_string),
            placement: r.placement.map(str::to_string),
            doc_id: r.doc_id.map(str::to_string),
            key: r.key.map(<[u8]>::to_vec),
            salvaged: r.salvaged,
            salvage_to: r.salvage_to.map(<[_]>::to_vec),
            kept: r.kept.to_vec(),
            route: r.route,
            columns: r.columns.cloned(),
            riders: Vec::new(),
        }
    }

    fn as_outgoing(&self) -> OutgoingRecord<'_> {
        OutgoingRecord {
            value: &self.value,
            payload: &self.payload,
            unfit: self.unfit.as_deref().map(Cow::Borrowed),
            placement: self.placement.as_deref(),
            doc_id: self.doc_id.as_deref(),
            key: self.key.as_deref(),
            salvaged: self.salvaged,
            salvage_to: self.salvage_to.as_deref(),
            kept: &self.kept,
            route: self.route,
            columns: self.columns.as_ref(),
        }
    }

    /// This record as `dlq`, which writes `payload`, is sent it in salvage: keyed as its plan says, or `None` when a
    /// delivery rule already sent it there. A DLQ the plan does not know of is sent it keyed as here. The plan goes
    /// along, for the next DLQ down the chain if `dlq` refuses it too. The placement stays behind: it routes within the
    /// destination it was computed for.
    pub(crate) fn salvage_for<'s>(&'s self, dlq: DestId, payload: &'s Payload) -> Option<OutgoingRecord<'s>> {
        let doc_id = match self.salvage_to.iter().flatten().find(|t| t.dlq == dlq) {
            Some(t) if t.reached => return None,
            Some(t) => t.doc_id.as_deref(),
            None => self.doc_id.as_deref(),
        };
        let (value, unfit) = match self.kept.iter().find(|(p, _)| p == payload) {
            _ if *payload == self.payload => (&self.value[..], self.unfit.as_deref().map(Cow::Borrowed)),
            Some((_, kept)) => (&kept[..], None),
            None => (&[][..], Some(Cow::Owned(no_form(payload)))),
        };
        Some(OutgoingRecord {
            value,
            payload,
            unfit,
            placement: None,
            doc_id,
            key: self.key.as_deref(),
            salvaged: true,
            salvage_to: self.salvage_to.as_deref(),
            kept: &self.kept,
            route: self.route,
            columns: self.columns.as_ref(),
        })
    }

    /// Everything the record owns, as [`OutgoingRecord::bytes`] counts it.
    fn bytes(&self) -> usize {
        self.as_outgoing().bytes()
    }

    /// What the record sends: its encoded bytes, or its row of columns.
    pub(crate) fn sent_bytes(&self) -> usize {
        self.value.len() + self.columns.as_ref().map_or(0, |c| c.chunk.row_bytes(c.row))
    }
}

/// Why a row has no `payload` form to write. Only a text field the row lacks is missing from a row egress made.
pub(crate) fn no_form(payload: &Payload) -> String {
    match payload {
        Payload::Text { field } => format!("it has no {field} to write as text"),
        Payload::Record => "it carries no record".to_string(),
        Payload::Bytes { field } => format!("it has no {field} to write as bytes"),
    }
}

/// What `rows` hold in memory: each row's own bytes, and everything rows share -- a chunk their columns point into, a
/// source form they kept -- whole and once. One row left in a queue holds all it shares, whoever else let go of it.
pub fn held_bytes(rows: &[OwnedRecord]) -> usize {
    let mut chunks: Vec<&Arc<parquet::Chunk>> = Vec::new();
    // By address: most rows keep forms of their own, so a delivery unit can hold as many as it has rows.
    let mut kept: HashMap<*const u8, usize> = HashMap::new();
    let rows: Vec<&OwnedRecord> = rows.iter().flat_map(|r| std::iter::once(r).chain(&r.riders)).collect();
    for r in &rows {
        if let Some(c) = &r.columns
            && !chunks.iter().any(|held| Arc::ptr_eq(held, &c.chunk))
        {
            chunks.push(&c.chunk);
        }
        for (_, form) in &r.kept {
            kept.insert(form.as_ptr(), form.len());
        }
    }
    rows.iter().map(|r| r.bytes()).sum::<usize>()
        + chunks.iter().map(|c| c.bytes()).sum::<usize>()
        + kept.values().sum::<usize>()
}

/// Names one batch handed to a destination, so its acknowledgment can be recognized when it arrives.
pub type BatchId = u64;

/// One batch a destination has finished with, and what it refused. The rejections travel with the completion because
/// their indices are into that batch's records.
#[derive(Debug)]
pub struct BatchCompletion {
    pub batch: BatchId,
    pub rejections: Vec<Rejection>,
    /// The destination asked to be sent less while sending it: the pacing model cuts what it may hold.
    pub throttled: bool,
    /// Records that were not delivered, for the caller to send again; the rest of the batch was delivered or refused.
    /// `None` is a batch wholly answered for.
    pub undelivered: Option<Undelivered>,
}

/// What of a batch was not delivered, and why.
#[derive(Debug)]
pub struct Undelivered {
    pub error: IoError,
    /// Positions in the batch's rows, as a [`Rejection`]'s `doc` is.
    pub rows: Vec<usize>,
}

/// What [`Destination::send`] did with a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatched {
    /// Acknowledged before `send` returned; no completion follows.
    Complete,
    /// Handed to the transport. The batch's id comes back from [`Destination::poll_completions`] once it is
    /// acknowledged.
    InFlight,
    /// Not taken: the destination's queue has no room for a batch of `bytes`. Nothing was sent and the rows are
    /// dropped, so the caller keeps the batch and runs it again once [`Destination::has_room`] says so. A batch
    /// kept waiting longer than [`Destination::stall_after`] is a stall, reported as a block.
    NoRoom { bytes: usize },
}

/// A destination for payloads, driven by one egress thread. `Send` so it may be built on one thread and run
/// on another.
///
/// # Hand over, report
///
/// `send` takes one batch's records whole and starts sending them without waiting, and `poll_completions` reports
/// the batches that were delivered, in any order. A transport with no asynchronous report of its own answers
/// `Dispatched::Complete` and waits inside `send`. A record the destination examined and refused is not a
/// failed send: it comes back against its position in the records sent, from `send` if it is known then
/// and with the batch's completion if not.
///
/// A failed send loses no record: whatever was not acknowledged goes again, and where a completion names the
/// records that were not delivered, only they do. The re-send can carry a record the destination already holds, so a
/// record may arrive more than once -- at-least-once.
pub trait Destination: Send {
    /// Hand over `batch`'s records here and start sending them without waiting. A destination ignores whichever of
    /// `OwnedRecord`'s fields it has no notion of. `records` is owned by a caller that has no further use for them, and
    /// borrowed by one that keeps them to send again: a destination that queues them takes them without a copy.
    ///
    /// Answers the refusals known now, each carrying its record. Under `Complete` that is every one; under `InFlight`
    /// the rest come with the completion. `Err` is a failed send of this batch alone: nothing of it may be
    /// committed, and the caller hands it over again. What is already in flight is still answered for by
    /// `poll_completions`, by a completion or by its `Err`.
    fn send(
        &mut self,
        batch: BatchId,
        records: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError>;

    /// The barrier: block until everything handed over so far is acknowledged. `Err` means nothing in flight may be
    /// committed.
    fn flush(&mut self) -> Result<(), IoError>;

    /// Batches answered for since the last call, in any order. A completion naming `undelivered` records says the
    /// destination delivered the rest: only those go again. `Err` means a send failed: nothing in flight may be
    /// committed.
    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        Ok(Vec::new())
    }

    /// Refusals this destination is holding to send again, drained: it is set to block, and those batches complete
    /// only once it takes them. Empty for a destination that does not hold its own.
    fn take_held(&mut self) -> Vec<HeldRefusal> {
        Vec::new()
    }

    /// Where a destination that spawns a sender when every one it has is busy gets leave to spawn one past those it
    /// started with: `lend` answers whether the memory it holds is lent. Given once, before the first delivery. One
    /// that spawns none ignores it.
    fn lend_senders(&mut self, _lend: Lend) {}

    /// Its deployment is stopping: from now on, finish what it holds as soon as it can, rather than when its own
    /// interval says. Returns at once; what is delivered is reported by `poll_completions`. The default does nothing: a
    /// destination that acknowledges as it delivers has nothing to finish sooner.
    fn finish(&mut self) {}

    /// How many requests this destination has open, for a transport that opens them itself. `None` from one whose
    /// requests are not its own to report -- librdkafka's are inside librdkafka.
    fn concurrency(&mut self) -> Option<usize> {
        None
    }

    /// The bytes its open files buffer that are not yet uploaded, for a destination that writes files. `None` for one
    /// that writes none.
    fn open_bytes(&self) -> Option<usize> {
        None
    }

    /// Why this destination reaches nothing right now, when its client knows before any send fails: a broker that
    /// does not resolve, refuses the connection, or rejects the credentials. `None` for one that is reachable, or a
    /// kind that learns it only from a send. Asked by the egress thread on every pass.
    fn connection_error(&self) -> Option<String> {
        None
    }

    /// Whether a batch of `bytes` would be taken now. Asked before handing rows over, so rows a full queue would drop
    /// wait for room instead, and before re-running a batch `send` answered [`Dispatched::NoRoom`] to. A
    /// transport that never answers `NoRoom` always has room.
    fn has_room(&self, _bytes: usize) -> bool {
        true
    }

    /// How long a batch may wait for room before the wait is a stall, reported as a block. Asked only of a
    /// destination that had no room.
    fn stall_after(&self) -> std::time::Duration {
        std::time::Duration::MAX
    }
}

impl Rejection {
    /// A refusal of `rows[doc]`, carrying the row.
    /// The rows this refusal stands for: its record's, or one when it kept none.
    pub(crate) fn rows(&self) -> u64 {
        self.record.as_ref().map_or(1, OwnedRecord::rows)
    }

    pub fn of(rows: &[OwnedRecord], doc: usize, reason: impl Into<String>) -> Rejection {
        Rejection {
            doc,
            reason: reason.into(),
            record: rows.get(doc).cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entra_signs_in_in_the_cloud_a_host_is_in() {
        assert_eq!(
            entra_authority("acme.servicebus.windows.net"),
            "https://login.microsoftonline.com"
        );
        assert_eq!(
            entra_authority("acme.blob.core.usgovcloudapi.net"),
            "https://login.microsoftonline.us"
        );
        assert_eq!(
            entra_authority(" ACME.servicebus.usgovcloudapi.net. "),
            "https://login.microsoftonline.us"
        );
        assert_eq!(
            entra_authority("acme.servicebus.chinacloudapi.cn"),
            "https://login.chinacloudapi.cn"
        );
        assert_eq!(
            entra_authority("https://dce.usgovvirginia-1.ingest.monitor.azure.us"),
            "https://login.microsoftonline.us"
        );
        assert_eq!(
            entra_authority("https://monitor.azure.cn"),
            "https://login.chinacloudapi.cn"
        );
        assert_eq!(
            entra_authority("usgovcloudapi.net.evil"),
            "https://login.microsoftonline.com"
        );
        assert_eq!(entra_authority("127.0.0.1"), "https://login.microsoftonline.com");
        assert_eq!(
            entra_authority("https://acme.servicebus.usgovcloudapi.net/.default"),
            "https://login.microsoftonline.us"
        );
    }

    fn poll(raws: &[&[u8]]) -> Vec<SourceRecord> {
        let mut b = RecordChunkBuilder::new();
        for (i, raw) in raws.iter().enumerate() {
            b.push(raw, i as i64, 0, Arc::from("t"), None);
        }
        b.finish()
    }

    /// A poll's records share one chunk, each reading back exactly the bytes it was given, invalid UTF-8 included.
    #[test]
    fn a_polls_records_share_one_chunk_and_keep_their_bytes() {
        let records = poll(&[b"a", b"\xff\xfe", b""]);
        assert!(records.iter().all(|r| Arc::ptr_eq(&r.chunk, &records[0].chunk)));
        let read: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
        assert_eq!(read, [&b"a"[..], b"\xff\xfe", b""]);
        assert_eq!(
            records.iter().map(SourceRecord::read_size).collect::<Vec<_>>(),
            [1, 2, 0]
        );
    }

    /// A record taken back leaves the chunk as if it was never pushed, and comes back whole.
    #[test]
    fn a_record_taken_back_comes_back_whole() {
        let mut b = RecordChunkBuilder::new();
        b.push(b"one", 1, 0, Arc::from("t"), None);
        b.push(b"two", 2, 3, Arc::from("u"), Some(vec![("k".into(), b"v".to_vec())]));
        assert_eq!(b.size(), 3 + 3 + 2);
        let (raw, (offset, partition, topic, headers)) = b.pop().unwrap();
        assert_eq!((&raw[..], offset, partition, &*topic), (&b"two"[..], 2, 3, "u"));
        assert_eq!(headers, Some(vec![("k".into(), b"v".to_vec())]));
        assert_eq!(b.size(), 3);
        let records = b.finish();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].bytes(), Some(&b"one"[..]));
    }

    /// Records kept past their batch hold only their own rows; records that are their whole chunk keep it.
    #[test]
    fn records_kept_hold_only_their_own_rows() {
        let records = poll(&[b"a", b"b", b"c", b"d"]);
        let mut whole = records.clone();
        own_rows(&mut whole);
        assert!(Arc::ptr_eq(&whole[0].chunk, &records[0].chunk), "a whole chunk is kept");

        let mut some = vec![records[3].clone(), records[1].clone()];
        own_rows(&mut some);
        assert_eq!(some[0].chunk.len(), 2);
        assert!(
            Arc::ptr_eq(&some[0].chunk, &some[1].chunk),
            "records that shared a chunk share the new one"
        );
        let read: Vec<&[u8]> = some.iter().map(|r| r.bytes().unwrap()).collect();
        assert_eq!(read, [&b"d"[..], b"b"]);
        assert_eq!((some[0].offset, some[1].offset), (3, 1));
    }
}
