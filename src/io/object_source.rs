//! A source of the objects a queue announces: each object read whole, decompressed, and split into records, and each
//! message deleted only once every record of every object it names is committed.
//!
//! ```text
//!   receive ──► message: objects ──► GET in ranges ──► decompress by its magic ──► lines, events or the whole object
//!                  │                                                                        │
//!                  └──── extended while held ◄──── deleted once every object is committed ◄─┘
//! ```
//!
//! Each read of an object is its own partition, numbered by this source, and a record's offset is its place in the
//! object, so the reader's watermark per partition says how far each read is committed. The cloud is the [`Queue`]'s
//! business and the bucket's store's; everything here holds for every cloud.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

use crate::io::avro::AvroReader;
use crate::io::avro::container::Container;
use crate::io::line_breaker::{BREAK_SLACK, LineBreaker};
use crate::io::lines::unescape_line;
use crate::io::{CommitMap, IoError, PartitionLag, Progress, Source, SourceRecord, TopicPartition};
use crate::logs::Relog;
use crate::plan::topology::{AfterCommit, Framing, HeldSecrets, ListPlace, ObjectQueue, ObjectSourceConfig};

/// How much of an object one GET fetches: what the source holds of an object at most, before decompression.
const CHUNK: u64 = 8 * 1024 * 1024;

/// The fewest bytes read before a line breaker's search runs again.
const READ_AHEAD: usize = 64 * 1024;

/// How long one GET may take before the object counts as unreadable.
const GET_TIMEOUT: Duration = Duration::from_secs(60);

/// The most messages one receive asks for. SQS's own limit.
const RECEIVE_MAX: usize = 10;

/// How often a queue is asked for its depth. Kafka's lag sampler's cadence, so a queue's lag reads like a topic's.
const LAG_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// How long one receive waits for a message to arrive. SQS's own limit: it answers as soon as one does.
const RECEIVE_WAIT: Duration = Duration::from_secs(20);

/// The longest record read: one record must fit in memory. A longer one, such as a decompression bomb's, makes its
/// object unreadable, which leaves its message to the queue's redrive policy.
pub const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

/// What a queue announces and is told: received messages, kept hidden while held, deleted when done.
/// Shared, because a receive waits on a thread of its own while the source extends and deletes.
pub trait Queue: Send + Sync {
    /// Up to `max` messages, each hidden for `visibility` from now, waiting up to `wait` for the first.
    fn receive(&self, max: usize, wait: Duration, visibility: Duration) -> Result<Vec<Message>, IoError>;

    /// Hides each of `handles` for `visibility` from now; zero shows them again at once. Answers each handle it could
    /// not, and why.
    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)>;

    /// Deletes each of `handles`. Answers each handle it could not, and why.
    fn delete(&self, handles: &[String]) -> Vec<(String, String)>;
    /// What an operator calls this queue: its URL or subscription, for the lines about it.
    fn name(&self) -> &str;

    /// What this queue has had deleted since the last call, for [`Source::take_progress`]. None for a queue whose
    /// transport keeps its own progress.
    fn take_progress(&self) -> Option<Progress> {
        None
    }

    /// Resume from what an earlier holder's [`Queue::take_progress`] gave, before the first receive.
    fn resume(&self, _from: Progress) -> Result<(), IoError> {
        Ok(())
    }

    /// The source is closing: a receive under way may end early. Nothing for a queue whose receive ends within its
    /// wait.
    fn stop(&self) {}

    /// This queue's lag (E6): every message it holds, those hidden while held among them, as one partition named for
    /// the queue. None for a queue that cannot count them, [`Unread`] for one that did not this time. A call to the
    /// queue's service, so it is made off the reader's thread.
    fn depth(&self) -> Option<Result<PartitionLag, Unread>> {
        None
    }
}

/// Why a queue's depth went unread.
pub enum Unread {
    /// The credentials lack the permission to read it, which is optional: it is not asked for again.
    Denied(String),
    /// It could not be read this time.
    Failed(String),
}

/// The longest pause between two asks that found nothing. A service without long poll answers an empty queue at
/// once, so an idle receive asks again, less often the longer the queue stays empty.
const IDLE_MAX: Duration = Duration::from_secs(5);

/// A receive on a service without long poll: `ask` is asked again until it answers messages or `wait` is up.
pub(crate) fn receive_polling(
    wait: Duration,
    mut ask: impl FnMut() -> Result<Vec<Message>, IoError>,
) -> Result<Vec<Message>, IoError> {
    let deadline = Instant::now() + wait;
    let mut idle = Duration::from_millis(500);
    loop {
        let messages = ask()?;
        let now = Instant::now();
        if !messages.is_empty() || now >= deadline {
            return Ok(messages);
        }
        std::thread::sleep(idle.min(deadline - now));
        idle = (idle * 2).min(IDLE_MAX);
    }
}

/// A queue's lag of `depth` messages, as the one partition named for the queue: the last part of its URL.
pub(crate) fn queue_lag(queue_url: &str, depth: i64) -> PartitionLag {
    let name = queue_url.trim_end_matches('/').rsplit('/').next().unwrap_or(queue_url);
    PartitionLag {
        partition: TopicPartition::new(Arc::from(name), 0),
        lag: depth,
    }
}

/// One received message.
#[derive(Clone, Debug)]
pub struct Message {
    /// What extends and deletes it.
    pub handle: String,
    /// The objects it announces to read, or why it is no announcement. Empty is a message with nothing to read, deleted
    /// as it is.
    pub objects: Result<Vec<ObjectRef>, String>,
}

/// An object to read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub bucket: String,
    pub key: String,
    /// The version announced, read rather than whatever the key holds now. None in a bucket without versioning.
    pub version: Option<String>,
}

/// Opens `config` as a live source.
pub fn open(config: &ObjectSourceConfig, secrets: &HeldSecrets) -> Result<Box<dyn Source>, IoError> {
    let runtime = crate::io::objects::runtime()?;
    let entered = runtime.clone();
    let (queue, stores): (Arc<dyn Queue>, Stores) = match &config.queue {
        ObjectQueue::Sqs { queue_url, access } => {
            // The bundle's region, else the queue URL's, else the host's: S3 notifies only a queue in its bucket's
            // region, so the queue's region is the buckets' too, and it is the one SQS must be signed for.
            let mut access = access.clone();
            access.region = access
                .region
                .or_else(|| crate::io::sqs::region(queue_url).map(str::to_string));
            let aws = crate::io::objects::Aws::new(&access, config.tls.as_ref(), secrets, &runtime)?;
            let queue = crate::io::sqs::Sqs::new(queue_url, aws.clone(), runtime.clone())?;
            let stores = move |bucket: &str| {
                let _entered = entered.enter();
                aws.bucket(bucket)
            };
            (Arc::new(queue), Box::new(stores))
        }
        ObjectQueue::List {
            place,
            prefix,
            poll_interval,
            start,
            after_commit,
        } => {
            let tls = config.tls.as_ref();
            let (store, bucket) = match place {
                ListPlace::S3 { bucket, access } => {
                    let aws = crate::io::objects::Aws::new(access, tls, secrets, &runtime)?;
                    let _entered = runtime.enter();
                    (aws.bucket(bucket)?, bucket)
                }
                ListPlace::Gcs(g) => {
                    let gcp = crate::io::objects::Gcp::new(&g.access, tls, secrets, &runtime)?;
                    let _entered = runtime.enter();
                    (gcp.bucket(&g.bucket)?, &g.bucket)
                }
                ListPlace::Azure(a) => {
                    let azure = crate::io::objects::Azure::new(&a.access, tls, secrets, &runtime)?;
                    let _entered = runtime.enter();
                    (azure.container(&a.container)?, &a.container)
                }
                ListPlace::Sftp(f) => {
                    let sftp: Arc<dyn ObjectStore> = Arc::new(crate::io::sftp::Sftp::new(
                        f,
                        prefix,
                        *after_commit != AfterCommit::Keep,
                        secrets,
                    )?);
                    (sftp, &f.directory)
                }
            };
            let mut queue = crate::io::listing::Listing::new(
                store.clone(),
                bucket,
                prefix,
                *poll_interval,
                *start,
                after_commit.clone(),
                runtime.clone(),
            )?;
            // An Azure append blob grows, and a file server shows an upload under way: an object there is read once
            // it has stopped changing. S3 and GCS show an object only once it is whole.
            if matches!(place, ListPlace::Azure(_) | ListPlace::Sftp(_)) {
                queue = queue.when_steady();
            }
            let stores = move |_: &str| Ok(store.clone());
            (Arc::new(queue), Box::new(stores))
        }
        ObjectQueue::StorageQueue {
            queue_url,
            access,
            max_deliveries,
        } => {
            let azure = crate::io::objects::Azure::new(access, config.tls.as_ref(), secrets, &runtime)?;
            let queue = crate::io::storage_queue::StorageQueue::new(
                queue_url,
                access,
                *max_deliveries,
                azure.clone(),
                runtime.clone(),
            )?;
            let stores = move |container: &str| {
                let _entered = entered.enter();
                azure.container(container)
            };
            (Arc::new(queue), Box::new(stores))
        }
        ObjectQueue::PubSub {
            subscription,
            endpoint,
            access,
        } => {
            let gcp = crate::io::objects::Gcp::new(access, config.tls.as_ref(), secrets, &runtime)?;
            let queue =
                crate::io::pubsub::PubSub::new(subscription, endpoint.as_deref(), gcp.clone(), runtime.clone())?;
            let stores = move |bucket: &str| {
                let _entered = entered.enter();
                gcp.bucket(bucket)
            };
            (Arc::new(queue), Box::new(stores))
        }
    };
    let source = ObjectSource::new(
        queue,
        stores,
        runtime,
        config.framing.clone(),
        config.visibility,
        MAX_RECORD_BYTES,
    )?;
    Ok(Box::new(match &config.avro {
        Some(avro) => source.reading_avro(crate::io::avro::AvroReader::new(avro, config.tls.as_ref(), secrets)?),
        None => source,
    }))
}

/// The store a bucket is read from, built on its first object.
pub type Stores = Box<dyn FnMut(&str) -> Result<Arc<dyn ObjectStore>, IoError> + Send>;

/// A message received and not yet deleted.
struct Held {
    handle: String,
    /// When it shows again to every other worker unless extended.
    until: Instant,
    /// Its objects not yet read to the end and committed.
    open: usize,
}

/// One read of one object, until it is read to the end and committed.
struct ReadOf {
    message: u64,
    topic: Arc<str>,
    /// Records handed out so far.
    records: i64,
    /// Read to the end: `records` is all there is.
    ended: bool,
    committed: i64,
}

/// The object being read now.
struct Current {
    partition: i32,
    lines: Box<dyn BufRead + Send>,
    /// A record read but not handed out: it did not fit the last batch.
    held_back: Option<Vec<u8>>,
    /// What is read and not yet cut into events, under `Framing::LineBreaker`.
    unbroken: Unbroken,
    /// Its header, once read, where it is an Avro container file.
    container: Option<Container>,
    /// The values of its block read last and not yet handed out, each framed with the object's own schema id.
    values: VecDeque<Vec<u8>>,
}

/// Bytes of an object read and not yet cut into events by a line breaker.
#[derive(Default)]
struct Unbroken {
    bytes: Vec<u8>,
    /// Where in `bytes` the next event starts: the bytes before it are taken.
    start: usize,
    /// How many bytes from `start` the last search found no break in.
    searched: usize,
    /// The object has no more bytes to come.
    ended: bool,
}

/// A receive under way, the time it started and where its answer arrives.
type Receiving = (Instant, Receiver<Result<Vec<Message>, IoError>>);

/// Messages held, by this source's number for each. Shared with the thread that extends them.
type HeldMap = Arc<Mutex<HashMap<u64, Held>>>;

/// The objects a [`Queue`] announces, as records.
pub struct ObjectSource {
    queue: Arc<dyn Queue>,
    stores: Stores,
    built: HashMap<String, Arc<dyn ObjectStore>>,
    runtime: Arc<tokio::runtime::Runtime>,
    framing: Framing,
    /// How each object reads where it is an Avro container file, in place of `framing`: by the schema in its header,
    /// told as the object's partition (`AvroReader::tell`).
    avro: Option<AvroReader>,
    /// The partitions of objects ended since the last poll, whose schemas go once that poll's values are read.
    ended: Vec<i32>,
    visibility: Duration,
    max_record: u64,
    held: HeldMap,
    /// Stops the thread that extends `held` when dropped, and that thread.
    keeper: Option<(Sender<()>, std::thread::JoinHandle<()>)>,
    /// The queue's latest depth not yet taken, from the thread that asks for it.
    lag: Arc<Mutex<Vec<PartitionLag>>>,
    /// Stops the thread that asks for the queue's depth when dropped. That thread is not joined: it may be waiting on
    /// the queue's service, and what it holds is its own.
    _sampler: Sender<()>,
    /// Messages finished since the last [`Source::take_finished`].
    messages_finished: u64,
    next_message: u64,
    /// Objects received and not yet started, with the message that named each.
    waiting: VecDeque<(u64, ObjectRef)>,
    current: Option<Current>,
    reads: HashMap<i32, ReadOf>,
    next_partition: i32,
    /// Handles of finished messages a delete has not yet taken.
    finished: Vec<String>,
    blocked: bool,
    /// Objects and messages this source could not read since it opened, carried by each report of one.
    unreadable: u64,
    /// Why the last receive failed, until one succeeds.
    receive_error: Option<String>,
    /// The one receive waiting on the queue. It waits on a thread of its own so an idle source asks rarely, while
    /// the reader keeps polling at its own pace.
    receiving: Option<Receiving>,
    /// A receive failing, said once per change and on the interval.
    receive_failing: Relog,
    /// Deleting finished messages failing, said once per change and on the interval: every tend tries again.
    delete_failing: Relog,
}

impl ObjectSource {
    /// Reads what `queue` announces from the store `stores` gives for each bucket, on `runtime`. A message is received
    /// hidden for `visibility`, and extended before that runs out while it is held. An object with a record longer
    /// than `max_record` bytes is unreadable; [`MAX_RECORD_BYTES`] but in tests.
    pub fn new(
        queue: Arc<dyn Queue>,
        stores: Stores,
        runtime: Arc<tokio::runtime::Runtime>,
        framing: Framing,
        visibility: Duration,
        max_record: u64,
    ) -> Result<ObjectSource, IoError> {
        let held = HeldMap::default();
        let (stop, stopped) = std::sync::mpsc::channel();
        let (q, h) = (queue.clone(), held.clone());
        let thread = std::thread::Builder::new()
            .name("object-source-keep".to_string())
            .spawn(move || keep(&*q, &h, visibility, &stopped))
            .map_err(|e| IoError::transient(format!("could not start extending held messages: {e}")))?;
        let lag = Arc::new(Mutex::new(Vec::new()));
        let (sampler, stopped) = std::sync::mpsc::channel();
        let (q, l) = (queue.clone(), lag.clone());
        std::thread::Builder::new()
            .name("object-source-lag".to_string())
            .spawn(move || sample_lag(&*q, &l, &stopped))
            .map_err(|e| IoError::transient(format!("could not start sampling the queue's depth: {e}")))?;
        Ok(ObjectSource {
            queue,
            stores,
            built: HashMap::new(),
            runtime,
            framing,
            avro: None,
            ended: Vec::new(),
            visibility,
            max_record,
            held,
            keeper: Some((stop, thread)),
            lag,
            _sampler: sampler,
            messages_finished: 0,
            next_message: 0,
            waiting: VecDeque::new(),
            current: None,
            reads: HashMap::new(),
            next_partition: 0,
            finished: Vec::new(),
            blocked: false,
            unreadable: 0,
            receive_error: None,
            receiving: None,
            receive_failing: Relog::default(),
            delete_failing: Relog::default(),
        })
    }

    /// This source reading each object as an Avro container file with `reader`.
    pub fn reading_avro(mut self, reader: AvroReader) -> ObjectSource {
        self.avro = Some(reader);
        self
    }

    /// Takes a receive that answered meanwhile, and deletes what is finished.
    fn tend(&mut self) {
        if let Err(e) = self.receive(Duration::ZERO, false) {
            if let Some(error) = self.receive_failing.fail_due(e.message.clone()) {
                tracing::warn!(target: "object_source.receive_failed", queue = %self.queue.name(), error, "a source could not receive from its queue; trying again");
            }
        } else if self.receive_failing.clear() {
            tracing::info!(target: "object_source.receive_recovered", queue = %self.queue.name(), "the queue answers receives again");
        }
        self.delete_finished();
    }

    fn delete_finished(&mut self) {
        if self.finished.is_empty() {
            return;
        }
        let finished = std::mem::take(&mut self.finished);
        let failed = self.queue.delete(&finished);
        self.messages_finished += finished.len().saturating_sub(failed.len()) as u64;
        match failed.first() {
            Some((_, why)) => {
                if let Some(error) = self.delete_failing.fail_due(why.clone()) {
                    tracing::warn!(
                        target: "object_source.delete_failed",
                        queue = %self.queue.name(),
                        count = failed.len(),
                        error,
                        "a source could not delete its finished messages; trying again"
                    );
                }
            }
            None => {
                self.delete_failing.clear();
            }
        }
        for (handle, _) in failed {
            self.finished.push(handle);
        }
    }

    /// Takes what the receive under way answers within `timeout` into `held` and `waiting`, starting one first if
    /// `ask` and none is.
    fn receive(&mut self, timeout: Duration, ask: bool) -> Result<(), IoError> {
        if self.receiving.is_none() && ask {
            let (tx, rx) = std::sync::mpsc::channel();
            let (queue, visibility) = (self.queue.clone(), self.visibility);
            std::thread::Builder::new()
                .name("object-source-receive".to_string())
                .spawn(move || {
                    let _ = tx.send(queue.receive(RECEIVE_MAX, RECEIVE_WAIT, visibility));
                })
                .map_err(|e| IoError::transient(format!("could not start a receive: {e}")))?;
            self.receiving = Some((Instant::now(), rx));
        }
        let Some((started, rx)) = &self.receiving else {
            return Ok(());
        };
        let answer = match rx.recv_timeout(timeout) {
            Ok(answer) => answer,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Ok(()),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(IoError::transient("a receive ended without an answer".to_string()))
            }
        };
        // Visibility runs from when the receive began, the earliest SQS can have started it.
        let until = *started + self.visibility;
        self.receiving = None;
        let messages = match answer {
            Ok(messages) => messages,
            Err(e) => {
                self.receive_error = Some(e.message.clone());
                return Err(e);
            }
        };
        self.receive_error = None;
        for message in messages {
            let objects = match message.objects {
                Ok(objects) => objects,
                Err(why) => {
                    // Left unextended, so the queue shows it again and in time dead-letters it.
                    self.unreadable += 1;
                    tracing::warn!(
                        target: "object_source.unreadable",
                        error = %why,
                        unreadable = self.unreadable,
                        "a message announces no objects; it is left on the queue"
                    );
                    continue;
                }
            };
            if objects.is_empty() {
                self.finished.push(message.handle);
                continue;
            }
            let id = self.next_message;
            self.next_message += 1;
            lock(&self.held).insert(
                id,
                Held {
                    handle: message.handle,
                    until,
                    open: objects.len(),
                },
            );
            self.waiting.extend(objects.into_iter().map(|o| (id, o)));
        }
        Ok(())
    }

    /// Opens the next waiting object as `current`. One that cannot be opened gives up its message.
    fn open_next(&mut self) -> bool {
        while let Some((message, object)) = self.waiting.pop_front() {
            if !lock(&self.held).contains_key(&message) {
                continue;
            }
            match self.open_object(&object) {
                Ok(lines) => {
                    let partition = self.next_partition;
                    self.next_partition = self.next_partition.wrapping_add(1);
                    self.reads.insert(
                        partition,
                        ReadOf {
                            message,
                            topic: Arc::from(format!("{}/{}", object.bucket, object.key)),
                            records: 0,
                            ended: false,
                            committed: -1,
                        },
                    );
                    self.current = Some(Current {
                        partition,
                        lines,
                        held_back: None,
                        unbroken: Unbroken::default(),
                        container: None,
                        values: VecDeque::new(),
                    });
                    return true;
                }
                Err(why) => self.give_up(message, &object.bucket, &object.key, &why),
            }
        }
        false
    }

    fn open_object(&mut self, object: &ObjectRef) -> Result<Box<dyn BufRead + Send>, String> {
        let store = match self.built.get(&object.bucket) {
            Some(store) => store.clone(),
            None => {
                let store = (self.stores)(&object.bucket).map_err(|e| e.message)?;
                self.built.insert(object.bucket.clone(), store.clone());
                store
            }
        };
        let path = Path::parse(&object.key).map_err(|e| format!("the key is no path the store can read: {e}"))?;
        let head = GetOptions {
            head: true,
            version: object.version.clone(),
            ..GetOptions::default()
        };
        let meta = within(&self.runtime, GET_TIMEOUT, store.get_opts(&path, head))?
            .map_err(|e| e.to_string())?
            .meta;
        let mut raw = Ranges {
            store,
            runtime: self.runtime.clone(),
            path,
            e_tag: meta.e_tag,
            version: object.version.clone(),
            at: 0,
            size: meta.size,
            chunk: Vec::new(),
            taken: 0,
        };
        let magic = raw.fill_buf().map_err(|e| e.to_string())?;
        Ok(if magic.starts_with(&[0x1f, 0x8b]) {
            Box::new(BufReader::new(flate2::bufread::MultiGzDecoder::new(raw)))
        } else if magic.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            Box::new(BufReader::new(
                zstd::stream::read::Decoder::with_buffer(raw).map_err(|e| e.to_string())?,
            ))
        } else if magic.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
            Box::new(BufReader::new(lz4_flex::frame::FrameDecoder::new(raw)))
        } else if magic.starts_with(b"\xff\x06\x00\x00sNaPpY") {
            Box::new(BufReader::new(snap::read::FrameDecoder::new(raw)))
        } else {
            Box::new(raw)
        })
    }

    /// Reports `bucket/key` unreadable and lets its message go back to the queue: no longer extended, never deleted.
    fn give_up(&mut self, message: u64, bucket: &str, key: &str, why: &str) {
        self.unreadable += 1;
        tracing::warn!(
            target: "object_source.unreadable",
            bucket,
            key,
            error = why,
            unreadable = self.unreadable,
            "an object could not be read; its message is left on the queue"
        );
        lock(&self.held).remove(&message);
    }

    /// The next record of `current`, or `None` at its end.
    fn next_record(&mut self) -> Result<Option<Vec<u8>>, String> {
        let current = self.current.as_mut().expect("a current object");
        if let Some(record) = current.held_back.take() {
            return Ok(Some(record));
        }
        if let Some(avro) = &mut self.avro {
            return next_value(current, avro, self.max_record);
        }
        let mut record = Vec::new();
        // The longest and a `\r\n`, so a line of the longest fits with its line end, and a record over it shows as such
        // without more of it ever held.
        let mut lines = (&mut current.lines).take(self.max_record + 2);
        match self.framing {
            Framing::Object => {
                let ended = self.reads[&current.partition].records > 0;
                if ended {
                    return Ok(None);
                }
                lines.read_to_end(&mut record).map_err(|e| e.to_string())?;
                if record.is_empty() {
                    return Ok(None);
                }
            }
            Framing::Lines | Framing::EscapedLines => {
                if lines.read_until(b'\n', &mut record).map_err(|e| e.to_string())? == 0 {
                    return Ok(None);
                }
                trim_line_end(&mut record);
                if self.framing == Framing::EscapedLines {
                    record = unescape_line(&record);
                }
            }
            Framing::LineBreaker(ref line_breaker) => match next_event(current, line_breaker, self.max_record)? {
                Some(event) => record = event,
                None => return Ok(None),
            },
        }
        if record.len() as u64 > self.max_record {
            return Err(format!("a record is longer than {} bytes", self.max_record));
        }
        Ok(Some(record))
    }

    /// Ends `current`: read to the end, so done once committed up to its last record.
    fn end_current(&mut self) {
        let partition = self.current.take().expect("a current object").partition;
        self.ended.push(partition);
        if let Some(read) = self.reads.get_mut(&partition) {
            read.ended = true;
        }
        self.settle(partition);
    }

    /// Drops the read on `partition` if it is done, and finishes its message if that was its last open object.
    fn settle(&mut self, partition: i32) {
        let Some(read) = self.reads.get(&partition) else { return };
        if !read.ended || read.committed < read.records - 1 {
            return;
        }
        let message = read.message;
        self.reads.remove(&partition);
        let mut held = lock(&self.held);
        if let Some(h) = held.get_mut(&message) {
            h.open -= 1;
            if h.open == 0 {
                let h = held.remove(&message).expect("held");
                self.finished.push(h.handle);
            }
        }
    }
}

impl Drop for ObjectSource {
    /// Shows every message this source holds again at once, rather than when its visibility lapses, those a receive
    /// still under way returns among them.
    fn drop(&mut self) {
        // Stopped first, so it extends nothing shown again below.
        if let Some((stop, thread)) = self.keeper.take() {
            drop(stop);
            let _ = thread.join();
        }
        self.delete_finished();
        let mut handles = Vec::new();
        self.queue.stop();
        if let Some((_, rx)) = self.receiving.take()
            // Bounded: a receive ends within its wait and its call's timeout.
            && let Ok(Ok(messages)) = rx.recv()
        {
            handles.extend(messages.into_iter().map(|m| m.handle));
        }
        handles.extend(lock(&self.held).drain().map(|(_, h)| h.handle));
        if let Some((_, why)) = self.queue.extend(&handles, Duration::ZERO).first() {
            tracing::warn!(
                target: "object_source.extend",
                error = %why,
                "held messages were not shown again; they show when their visibility lapses"
            );
        }
    }
}

/// Extends each message in `held` before it shows again, until `stop` disconnects. On a thread of its own, so a read
/// blocked on the store cannot let them lapse.
fn keep(queue: &dyn Queue, held: &Mutex<HashMap<u64, Held>>, visibility: Duration, stop: &Receiver<()>) {
    // A quarter, so each is looked at at least once while between half and a quarter of its time is left.
    while let Err(RecvTimeoutError::Timeout) = stop.recv_timeout(visibility / 4) {
        let now = Instant::now();
        let due: Vec<String> = lock(held)
            .values()
            .filter(|h| h.until.saturating_duration_since(now) < visibility / 2)
            .map(|h| h.handle.clone())
            .collect();
        if due.is_empty() {
            continue;
        }
        let failed = queue.extend(&due, visibility);
        for h in lock(held).values_mut() {
            if due.contains(&h.handle) && !failed.iter().any(|(f, _)| *f == h.handle) {
                h.until = now + visibility;
            }
        }
        if let Some((_, why)) = failed.first() {
            tracing::warn!(
                target: "object_source.extend",
                failed = failed.len(),
                error = %why,
                "held messages were not extended; trying again"
            );
        }
    }
}

/// Puts `queue`'s depth in `lag` every [`LAG_SAMPLE_INTERVAL`], the first at once, until `stop` disconnects. Ends at
/// once for a queue that cannot count its messages or may not. A depth it could not read is logged, and again every
/// [`crate::logs::RELOG_EVERY`] while it stays unread, so its warning stands as long as the fault does.
fn sample_lag(queue: &dyn Queue, lag: &Mutex<Vec<PartitionLag>>, stop: &Receiver<()>) {
    let mut failing = Relog::default();
    loop {
        match queue.depth() {
            None => return,
            Some(Err(Unread::Denied(why))) => {
                tracing::warn!(
                    target: "object_source.depth",
                    queue = %queue.name(),
                    error = %why,
                    "the queue's depth could not be read, so its lag is not reported"
                );
                return;
            }
            Some(Ok(depth)) => {
                if failing.clear() {
                    tracing::info!(target: "object_source.depth", "the queue's depth is read again");
                }
                *lag.lock().unwrap_or_else(PoisonError::into_inner) = vec![depth];
            }
            Some(Err(Unread::Failed(why))) => {
                failing.fail(why);
                if let Some(why) = failing.due() {
                    tracing::warn!(
                        target: "object_source.depth",
                        error = %why,
                        "the queue's depth could not be read, so its lag is not reported"
                    );
                }
            }
        }
        if !matches!(stop.recv_timeout(LAG_SAMPLE_INTERVAL), Err(RecvTimeoutError::Timeout)) {
            return;
        }
    }
}

/// `held`, whatever a panicking holder left: each entry is whole at every point a lock is released.
fn lock(held: &Mutex<HashMap<u64, Held>>) -> MutexGuard<'_, HashMap<u64, Held>> {
    held.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The next event of `current` that `line_breaker` cuts, its break dropped, or `None` at the end of the object. An
/// event of nothing but whitespace is passed over. No more than `max_record` and [`BREAK_SLACK`] bytes are held
/// waiting for a break.
fn next_event(current: &mut Current, line_breaker: &LineBreaker, max_record: u64) -> Result<Option<Vec<u8>>, String> {
    let most = max_record as usize + BREAK_SLACK;
    let u = &mut current.unbroken;
    loop {
        let rest = u.bytes.len() - u.start;
        if rest > u.searched || u.ended {
            let bytes = &u.bytes[u.start..];
            let mut events = line_breaker.events(bytes, u.ended);
            let event = events.next().map(|event| bytes[event].to_vec());
            u.start += events.rest();
            match event {
                Some(event) => {
                    u.searched = 0;
                    return Ok(Some(event));
                }
                None if u.ended => {
                    u.bytes.clear();
                    u.start = 0;
                    return Ok(None);
                }
                None => u.searched = u.bytes.len() - u.start,
            }
        }
        // Less than before, by the blank events passed over.
        let rest = u.bytes.len() - u.start;
        if rest > most {
            return Err(format!("a record is longer than {max_record} bytes"));
        }
        u.bytes.drain(..u.start);
        u.start = 0;
        // At least as many again as were searched in vain, so a long event is searched a bounded number of times over.
        let want = (rest + u.searched.max(READ_AHEAD)).min(most + 1);
        while u.bytes.len() < want {
            let ready = current.lines.fill_buf().map_err(|e| e.to_string())?;
            if ready.is_empty() {
                u.ended = true;
                break;
            }
            let n = ready.len().min(want - u.bytes.len());
            u.bytes.extend_from_slice(&ready[..n]);
            current.lines.consume(n);
        }
    }
}

/// The next value of `current`, an Avro container file, framed with its partition as the schema's id. The header is
/// read at the first, and its schema told to `avro`. `None` past its last block; an empty object has none.
fn next_value(current: &mut Current, avro: &mut AvroReader, max_record: u64) -> Result<Option<Vec<u8>>, String> {
    let id = current.partition as u32;
    loop {
        if let Some(value) = current.values.pop_front() {
            return Ok(Some(crate::io::avro::frame(id, &value)));
        }
        let container = match &current.container {
            Some(container) => container,
            None if current.lines.fill_buf().map_err(|e| e.to_string())?.is_empty() => return Ok(None),
            None => {
                let container = Container::open(&mut *current.lines)?;
                avro.tell(id, container.schema.clone());
                current.container.insert(container)
            }
        };
        match container.block(&mut *current.lines, max_record)? {
            Some(values) => current.values.extend(values),
            None => return Ok(None),
        }
    }
}

/// Takes a line's `\n` off, and a `\r` before it.
fn trim_line_end(line: &mut Vec<u8>) {
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
}

impl Source for ObjectSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        let deadline = Instant::now() + timeout;
        self.tend();
        if self.blocked {
            // Held messages are kept, and extended meanwhile; nothing new is read, not even the rest of an object.
            std::thread::sleep(timeout);
            return Ok(Vec::new());
        }
        let mut out = crate::io::RecordChunkBuilder::new();
        let mut bytes = 0;
        while out.len() < max_rows && (out.is_empty() || Instant::now() < deadline) {
            if self.current.is_none() && !self.open_next() {
                if !out.is_empty() || !self.waiting.is_empty() || Instant::now() >= deadline {
                    break;
                }
                self.receive(deadline.saturating_duration_since(Instant::now()), true)?;
                continue;
            }
            match self.next_record() {
                Ok(Some(record)) => {
                    if !out.is_empty() && bytes + record.len() > max_bytes {
                        self.current.as_mut().expect("current").held_back = Some(record);
                        break;
                    }
                    let partition = self.current.as_ref().expect("current").partition;
                    let read = self.reads.get_mut(&partition).expect("a read per current object");
                    bytes += record.len();
                    out.push(&record, read.records, partition, read.topic.clone(), None);
                    read.records += 1;
                }
                Ok(None) => self.end_current(),
                Err(why) => {
                    let partition = self.current.take().expect("current").partition;
                    self.ended.push(partition);
                    let read = self.reads.remove(&partition).expect("a read per current object");
                    let (bucket, key) = read.topic.split_once('/').unwrap_or((&read.topic, ""));
                    let (bucket, key) = (bucket.to_string(), key.to_string());
                    self.give_up(read.message, &bucket, &key, &why);
                }
            }
        }
        let records = out.finish();
        let Some(avro) = &mut self.avro else {
            return Ok(records);
        };
        let records = match records.is_empty() {
            true => records,
            false => avro.read(records),
        };
        for partition in self.ended.drain(..) {
            avro.forget(partition as u32);
        }
        Ok(records)
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        for (at, &offset) in offsets {
            if let Some(read) = self.reads.get_mut(&at.partition)
                && read.topic == at.topic
            {
                read.committed = read.committed.max(offset);
                self.settle(at.partition);
            }
        }
        self.tend();
        Ok(())
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.blocked = true;
        Ok(())
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.blocked = false;
        Ok(())
    }

    fn keep_membership(&mut self, _timeout: Duration) -> Result<(), IoError> {
        self.tend();
        Ok(())
    }

    fn connection_error(&self) -> Option<String> {
        self.receive_error.clone()
    }

    /// What the object being read holds outside its reader: the record held back and what awaits the line breaker.
    fn held_bytes(&self) -> Option<u64> {
        let held = self
            .current
            .as_ref()
            .map_or(0, |c| c.held_back.as_ref().map_or(0, Vec::len) + c.unbroken.bytes.len());
        Some(held as u64)
    }

    /// The queue's depth, as its sampler last measured it. Never a call to the queue on this thread.
    fn lag(&mut self) -> Vec<PartitionLag> {
        std::mem::take(&mut *self.lag.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn counts_messages(&self) -> bool {
        true
    }

    fn take_finished(&mut self) -> u64 {
        std::mem::take(&mut self.messages_finished)
    }

    fn take_progress(&mut self) -> Option<Progress> {
        self.queue.take_progress()
    }

    fn resume(&mut self, from: Progress) -> Result<(), IoError> {
        self.queue.resume(from)
    }
}

/// What `call` answers on `runtime`, or why it gave none within `limit`.
pub(crate) fn within<T>(
    runtime: &tokio::runtime::Runtime,
    limit: Duration,
    call: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    runtime
        .block_on(async { tokio::time::timeout(limit, call).await })
        .map_err(|_| format!("no answer within {limit:?}"))
}

/// An object read one ranged GET at a time, each pinned to the version first seen: one replaced mid-read fails rather
/// than splicing two versions.
struct Ranges {
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    path: Path,
    e_tag: Option<String>,
    version: Option<String>,
    /// Where the next GET starts.
    at: u64,
    size: u64,
    chunk: Vec<u8>,
    taken: usize,
}

impl Read for Ranges {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let ready = self.fill_buf()?;
        let n = ready.len().min(out.len());
        out[..n].copy_from_slice(&ready[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for Ranges {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.taken == self.chunk.len() && self.at < self.size {
            let options = GetOptions {
                if_match: self.e_tag.clone(),
                version: self.version.clone(),
                range: Some(GetRange::Bounded(self.at..(self.at + CHUNK).min(self.size))),
                ..GetOptions::default()
            };
            let get = async { self.store.get_opts(&self.path, options).await?.bytes().await };
            let chunk = within(&self.runtime, GET_TIMEOUT, get)
                .map_err(std::io::Error::other)?
                .map_err(std::io::Error::other)?;
            if chunk.is_empty() {
                return Err(std::io::Error::other("the object ended before its size"));
            }
            self.at += chunk.len() as u64;
            self.chunk = chunk.to_vec();
            self.taken = 0;
        }
        Ok(&self.chunk[self.taken..])
    }

    fn consume(&mut self, amount: usize) {
        self.taken += amount;
    }
}
