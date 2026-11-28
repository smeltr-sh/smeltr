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

use crate::sync::{Condvar, Mutex, MutexGuard};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

use crate::io::avro::AvroReader;
use crate::io::avro::Parsed;
use crate::io::avro::container::Container;
use crate::io::framing::{Framer, Messages};
use crate::io::{
    ChunkRows, CommitMap, IoError, PartitionLag, Progress, RecordChunk, RecordChunkBuilder, Source, SourceRecord,
    TopicPartition,
};
use crate::logs::Relog;
use crate::plan::topology::{AfterCommit, Framing, HeldSecrets, ListPlace, ObjectQueue, ObjectSourceConfig};

/// How much of an object one GET fetches: what the source holds of an object at most, before decompression.
const CHUNK: u64 = 8 * 1024 * 1024;

/// The most of an object's bytes one push hands its framer.
const READ_AHEAD: usize = 64 * 1024;

/// The most objects fetched, decompressed and cut ahead of the one being read, each on a thread of its own, so round
/// trips, decompression and framing overlap the reader and each other. A source starts with one and fetches more only
/// while its reader waits for the next object (`ObjectSource::ahead_target`).
const AHEAD: usize = 8;

/// The bytes of records an object fetched ahead holds before its fetch waits for the reader. It always holds at least
/// one chunk, so a record longer than this still passes. Together with [`CHUNK`] and [`AHEAD`], it bounds a source's
/// memory.
const QUEUED: u64 = 4 * 1024 * 1024;

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
pub(crate) type Stores = Box<dyn FnMut(&str) -> Result<Arc<dyn ObjectStore>, IoError> + Send>;

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
    fetched: Fetched,
    /// The rows of the chunk its fetch sent last.
    rows: Option<ChunkRows>,
}

impl Current {
    /// Whether a row is ready in `rows`, waiting on the fetch for one; `false` at the end of the object. A schema the
    /// fetch sends is told to `avro`.
    fn ready(&mut self, avro: &mut Option<AvroReader>) -> Result<bool, String> {
        while self.rows.as_ref().is_none_or(ChunkRows::is_done) {
            match self.fetched.recv()? {
                Fetch::Rows(chunk, _) => self.rows = Some(ChunkRows::new(chunk)),
                Fetch::Schema(schema) => {
                    if let Some(avro) = avro {
                        avro.tell(self.partition as u32, schema);
                    }
                }
                Fetch::End => return Ok(false),
            }
        }
        Ok(true)
    }
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
    /// Objects received and not yet fetched, with the message that named each.
    waiting: VecDeque<(u64, ObjectRef)>,
    /// Objects being fetched, in the order they are read, with the message that named each.
    ahead: VecDeque<(u64, ObjectRef, Fetched)>,
    /// How many objects to fetch ahead, from 1 to [`AHEAD`]. When an object is opened it doubles if that object had
    /// nothing ready for the reader, and falls by one if its fetch had to wait for the reader.
    ahead_target: usize,
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
        let thread = crate::sync::spawn("object-source-keep", move || keep(&*q, &h, visibility, &stopped))
            .map_err(|e| IoError::transient(format!("could not start extending held messages: {e}")))?;
        let lag = Arc::new(Mutex::new(Vec::new()));
        let (sampler, stopped) = std::sync::mpsc::channel();
        let (q, l) = (queue.clone(), lag.clone());
        crate::sync::spawn("object-source-lag", move || sample_lag(&*q, &l, &stopped))
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
            ahead: VecDeque::new(),
            ahead_target: 1,
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
                tracing::warn!(name: "receive_failed", target: "sources", queue = %self.queue.name(), error, "a source could not receive from its queue; trying again");
            }
        } else if self.receive_failing.clear() {
            tracing::info!(name: "receive_recovered", target: "sources", queue = %self.queue.name(), "the queue answers receives again");
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
                        name: "delete_failed", target: "sources",
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
                        name: "object_unreadable", target: "sources",
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
            self.held.lock().insert(
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

    /// Starts fetching waiting objects until `ahead_target` are being fetched. One whose fetch cannot be started gives
    /// up its message; one whose fetch fails is unreadable when its turn comes.
    fn fetch_ahead(&mut self) {
        while self.ahead.len() < self.ahead_target
            && let Some((message, object)) = self.waiting.pop_front()
        {
            if !self.held.lock().contains_key(&message) {
                continue;
            }
            match self.start_fetch(&object) {
                Ok(fetched) => self.ahead.push_back((message, object, fetched)),
                Err(why) => self.give_up(message, &object.bucket, &object.key, &why),
            }
        }
    }

    /// Opens the next object fetched as `current`.
    fn open_next(&mut self) -> bool {
        self.fetch_ahead();
        while let Some((message, object, fetched)) = self.ahead.pop_front() {
            self.ahead_target = if fetched.shared.stalled.load(Ordering::Relaxed) {
                self.ahead_target.saturating_sub(1).max(1)
            } else if fetched.shared.queued() == 0 {
                (self.ahead_target * 2).min(AHEAD)
            } else {
                self.ahead_target
            };
            self.fetch_ahead();
            // Its message was given up meanwhile: dropping `fetched` ends its fetch.
            if !self.held.lock().contains_key(&message) {
                continue;
            }
            let partition = fetched.partition;
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
                fetched,
                rows: None,
            });
            return true;
        }
        false
    }

    /// Starts fetching `object` on a thread of its own, which also cuts it into records, as the next partition.
    fn start_fetch(&mut self, object: &ObjectRef) -> Result<Fetched, String> {
        let store = match self.built.get(&object.bucket) {
            Some(store) => store.clone(),
            None => {
                let store = (self.stores)(&object.bucket).map_err(|e| e.message)?;
                self.built.insert(object.bucket.clone(), store.clone());
                store
            }
        };
        let path = Path::parse(&object.key).map_err(|e| format!("the key is no path the store can read: {e}"))?;
        let partition = self.next_partition;
        self.next_partition = self.next_partition.wrapping_add(1);
        let cutting = match self.avro {
            Some(_) => Cutting::Avro(partition as u32, self.max_record),
            None => Cutting::Frames(Framer::refusing(self.framing.clone(), self.max_record as usize)),
        };
        let (tx, blocks) = std::sync::mpsc::channel();
        let shared = Arc::new(Shared::default());
        let (runtime, version, counted) = (self.runtime.clone(), object.version.clone(), shared.clone());
        std::thread::Builder::new()
            .name("object-source-fetch".to_string())
            .spawn(move || fetch(store, runtime, path, version, cutting, &tx, &counted))
            .map_err(|e| format!("could not start its fetch: {e}"))?;
        Ok(Fetched {
            blocks,
            shared,
            partition,
        })
    }

    /// Reports `bucket/key` unreadable and lets its message go back to the queue: no longer extended, never deleted.
    fn give_up(&mut self, message: u64, bucket: &str, key: &str, why: &str) {
        self.unreadable += 1;
        tracing::warn!(
            name: "object_unreadable", target: "sources",
            bucket,
            key,
            error = why,
            unreadable = self.unreadable,
            "an object could not be read; its message is left on the queue"
        );
        self.held.lock().remove(&message);
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
        let mut held = self.held.lock();
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
        handles.extend(self.held.lock().drain().map(|(_, h)| h.handle));
        if let Some((_, why)) = self.queue.extend(&handles, Duration::ZERO).first() {
            tracing::warn!(
                name: "extend", target: "sources",
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
        let due: Vec<String> = held
            .lock()
            .values()
            .filter(|h| h.until.saturating_duration_since(now) < visibility / 2)
            .map(|h| h.handle.clone())
            .collect();
        if due.is_empty() {
            continue;
        }
        let failed = queue.extend(&due, visibility);
        for h in held.lock().values_mut() {
            if due.contains(&h.handle) && !failed.iter().any(|(f, _)| *f == h.handle) {
                h.until = now + visibility;
            }
        }
        if let Some((_, why)) = failed.first() {
            tracing::warn!(
                name: "extend", target: "sources",
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
                    name: "depth_unread", target: "sources",
                    queue = %queue.name(),
                    error = %why,
                    "the queue's depth could not be read, so its lag is not reported"
                );
                return;
            }
            Some(Ok(depth)) => {
                if failing.clear() {
                    tracing::info!(name: "depth_unread", target: "sources", "the queue's depth is read again");
                }
                *lag.lock() = vec![depth];
            }
            Some(Err(Unread::Failed(why))) => {
                failing.fail(why);
                if let Some(why) = failing.due() {
                    tracing::warn!(
                        name: "depth_unread", target: "sources",
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
        // Receive again before the waiting objects run out, so the next ones arrive while these are fetched and read.
        if self.waiting.len() + self.ahead.len() < AHEAD {
            self.receive(Duration::ZERO, true)?;
        }
        let mut records = Vec::new();
        let mut bytes = 0;
        while records.len() < max_rows && (records.is_empty() || Instant::now() < deadline) {
            if self.current.is_none() && !self.open_next() {
                if !records.is_empty() || !self.waiting.is_empty() || Instant::now() >= deadline {
                    break;
                }
                self.receive(deadline.saturating_duration_since(Instant::now()), true)?;
                continue;
            }
            let current = self.current.as_mut().expect("current");
            match current.ready(&mut self.avro) {
                Ok(true) => {
                    let rows = current.rows.as_mut().expect("a row ready");
                    let size = rows.next_size();
                    if !records.is_empty() && bytes + size > max_bytes {
                        break;
                    }
                    let read = self
                        .reads
                        .get_mut(&current.partition)
                        .expect("a read per current object");
                    let (chunk, row) = rows.take();
                    records.push(SourceRecord {
                        chunk,
                        row,
                        offset: read.records,
                        partition: current.partition,
                        topic: read.topic.clone(),
                        headers: None,
                    });
                    read.records += 1;
                    bytes += size;
                }
                Ok(false) => self.end_current(),
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

    /// What the objects hold outside their readers: for the one being read, the rows its reader has not yet handed out
    /// and what its fetch has queued; for each fetched ahead, what its fetch has queued.
    fn held_bytes(&self) -> Option<u64> {
        let held = self.current.as_ref().map_or(0, |c| {
            c.rows.as_ref().map_or(0, ChunkRows::untaken) as u64 + c.fetched.shared.queued()
        });
        let ahead: u64 = self.ahead.iter().map(|(_, _, f)| f.shared.queued()).sum();
        Some(held + ahead)
    }

    /// The queue's depth, as its sampler last measured it. Never a call to the queue on this thread.
    fn lag(&mut self) -> Vec<PartitionLag> {
        std::mem::take(&mut *self.lag.lock())
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

/// What a fetch sends its reader, in order: an object's records, cut.
enum Fetch {
    /// Its next records, and what they hold.
    Rows(RecordChunk, usize),
    /// Its schema, before its first value, where it is an Avro container file.
    Schema(Arc<Parsed>),
    End,
}

/// How a fetch cuts an object into records.
enum Cutting {
    Frames(Framer),
    /// As an Avro container file: each value framed with this schema id, none longer than this many bytes.
    Avro(u32, u64),
}

/// Why a fetch stops early: its reader is gone.
const READER_GONE: &str = "its reader is gone";

/// Reads the object at `path` to its end, decompressed, cuts it as `cutting` says and sends it into `tx`, counting in
/// `shared` the bytes sent and not yet taken and whether it waited for the reader. An error, after the records before
/// it, says why it is unreadable. Ends early once the receiver of `tx` is dropped.
fn fetch(
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    path: Path,
    version: Option<String>,
    cutting: Cutting,
    tx: &Sender<Result<Fetch, String>>,
    shared: &Shared,
) {
    let send = |fetch: Fetch| -> Result<(), String> {
        if let Fetch::Rows(_, size) = &fetch {
            let mut waiting = shared.waiting();
            if waiting.bytes >= QUEUED {
                shared.stalled.store(true, Ordering::Relaxed);
            }
            while waiting.bytes >= QUEUED && !waiting.gone {
                waiting = shared.taken.wait(waiting);
            }
            waiting.bytes += *size as u64;
        }
        tx.send(Ok(fetch)).map_err(|_| READER_GONE.to_string())
    };
    let cut = open_object(store, runtime, path, version).and_then(|mut lines| match cutting {
        Cutting::Frames(framer) => frames(&mut *lines, framer, &send),
        Cutting::Avro(id, max_record) => values(&mut *lines, id, max_record, &send),
    });
    match cut {
        Ok(()) => drop(send(Fetch::End)),
        Err(why) if why == READER_GONE => {}
        Err(why) => drop(tx.send(Err(why))),
    }
}

/// Cuts `lines` into records with `framer`, sending each block's as it is cut. Err says why the object is unreadable:
/// a record the framer refuses, one over the longest, or one the object ends partway through.
fn frames(
    lines: &mut dyn BufRead,
    mut framer: Framer,
    send: &impl Fn(Fetch) -> Result<(), String>,
) -> Result<(), String> {
    let nowhere: Arc<str> = Arc::from("");
    loop {
        let ready = lines.fill_buf().map_err(|e| e.to_string())?;
        let n = ready.len().min(READ_AHEAD);
        let mut frames = Messages::default();
        let cut = match n {
            0 => framer.finish(&mut frames),
            n => {
                framer.push(&ready[..n]);
                lines.consume(n);
                framer.cut(&mut frames).map(|()| 0)
            }
        };
        let mut out = RecordChunkBuilder::new();
        frames.each(|record| out.push(record, 0, 0, nowhere.clone(), None));
        send_rows(out, send)?;
        match cut {
            Err(malformed) => return Err(format!("{}, at byte {}", malformed.what, malformed.at)),
            Ok(partial) if partial > 0 => {
                return Err(format!("it ends partway through a record, {partial} bytes of it"));
            }
            Ok(_) if n == 0 => return Ok(()),
            Ok(_) => {}
        }
    }
}

/// Reads `lines` as an Avro container file, sending its schema and then each block's values framed with `id`. An
/// empty object has none.
fn values(
    lines: &mut dyn BufRead,
    id: u32,
    max_record: u64,
    send: &impl Fn(Fetch) -> Result<(), String>,
) -> Result<(), String> {
    if lines.fill_buf().map_err(|e| e.to_string())?.is_empty() {
        return Ok(());
    }
    let container = Container::open(lines)?;
    send(Fetch::Schema(container.schema.clone()))?;
    let nowhere: Arc<str> = Arc::from("");
    while let Some(values) = container.block(lines, max_record)? {
        let mut out = RecordChunkBuilder::new();
        for value in values {
            out.push(&crate::io::avro::frame(id, &value), 0, 0, nowhere.clone(), None);
        }
        send_rows(out, send)?;
    }
    Ok(())
}

/// Sends the records pushed to `out` as one chunk, if there are any.
fn send_rows(out: RecordChunkBuilder, send: &impl Fn(Fetch) -> Result<(), String>) -> Result<(), String> {
    if out.is_empty() {
        return Ok(());
    }
    let size = out.size();
    send(Fetch::Rows(out.into_chunk(), size))
}

/// The object at `path`, read at the version named and the ETag its head returned, decompressed as its first bytes
/// say.
fn open_object(
    store: Arc<dyn ObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
    path: Path,
    version: Option<String>,
) -> Result<Box<dyn BufRead + Send>, String> {
    // One GET reads the first chunk, and the whole of an object no larger, with the size and version the ranges after
    // it need. Unranged, since a range of an empty object is refused, and some stores answer a range with all of it.
    let get = GetOptions {
        version: version.clone(),
        ..GetOptions::default()
    };
    let first = async {
        let got = store.get_opts(&path, get).await?;
        let (meta, mut body, mut chunk) = (got.meta.clone(), got.into_stream(), Vec::new());
        while (chunk.len() as u64) < CHUNK
            && let Some(bytes) = body.next().await
        {
            chunk.extend_from_slice(&bytes?);
        }
        chunk.truncate(CHUNK as usize);
        Ok::<_, object_store::Error>((meta, chunk))
    };
    let (meta, chunk) = within(&runtime, GET_TIMEOUT, first)?.map_err(|e| e.to_string())?;
    let mut raw = Ranges {
        store,
        runtime,
        path,
        e_tag: meta.e_tag,
        version,
        at: chunk.len() as u64,
        size: meta.size,
        chunk,
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

/// What an object's fetch and its reader share.
#[derive(Default)]
struct Shared {
    waiting: Mutex<Waiting>,
    /// Told when the reader takes a chunk or is gone.
    taken: Condvar,
    /// The fetch found [`QUEUED`] bytes waiting and waited for the reader to take some.
    stalled: AtomicBool,
}

#[derive(Default)]
struct Waiting {
    /// The bytes of records sent and not yet taken.
    bytes: u64,
    /// The reader is dropped, so nothing more is taken.
    gone: bool,
}

impl Shared {
    fn waiting(&self) -> MutexGuard<'_, Waiting> {
        self.waiting.lock()
    }

    /// The bytes of records sent and not yet taken.
    fn queued(&self) -> u64 {
        self.waiting().bytes
    }
}

/// An object as its fetch sends it, cut into records.
struct Fetched {
    blocks: Receiver<Result<Fetch, String>>,
    shared: Arc<Shared>,
    /// The partition its records are read as.
    partition: i32,
}

impl Fetched {
    /// What its fetch sent next, waiting for it. Err says why the object is unreadable.
    fn recv(&self) -> Result<Fetch, String> {
        // A fetch gone without sending its end panicked: what it sent is not the whole object.
        let fetch = self
            .blocks
            .recv()
            .unwrap_or_else(|_| Err("its fetch stopped before the end of the object".to_string()))?;
        if let Fetch::Rows(_, size) = &fetch {
            self.shared.waiting().bytes -= *size as u64;
            self.shared.taken.notify_one();
        }
        Ok(fetch)
    }
}

impl Drop for Fetched {
    /// Lets a fetch waiting for room end.
    fn drop(&mut self) {
        self.shared.waiting().gone = true;
        self.shared.taken.notify_one();
    }
}

/// [`Read::read`] for a reader whose [`BufRead`] is its own.
fn read_buffered(from: &mut impl BufRead, out: &mut [u8]) -> std::io::Result<usize> {
    let ready = from.fill_buf()?;
    let n = ready.len().min(out.len());
    out[..n].copy_from_slice(&ready[..n]);
    from.consume(n);
    Ok(n)
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
        read_buffered(self, out)
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
