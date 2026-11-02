//! The consumer a deployment's Kafka sources share: one librdkafka client for every source whose client
//! configuration is the same, polled on one thread, each record handed to the inbox of the source that reads its
//! topic.
//!
//! ```text
//!   client thread ──poll──► route by topic ──► inbox of source 1 ──► reader 1 (KafkaSource::poll_batch)
//!                                         └──► inbox of source N ──► reader N
//!   readers ──commit, place, pause──► the shared consumer
//! ```
//!
//! One client has one `group.id`, so sources share a client only when they share a group, brokers, TLS,
//! properties, `fetchBytes` and whether a fresh partition starts at the earliest or the latest record. Topics, the
//! start a source places a fresh partition at, and its stop are each source's own. Two sources may not read one
//! topic in one group: the group would split its partitions between them.
//!
//! A source's partitions are paused while its inbox is full, while it is blocked or keeping its membership, and
//! while a partition waits to be placed or is at its stop. [`Slot::sync`] is the one place that decides.

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rdkafka::ClientContext;
use rdkafka::bindings;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, RebalanceProtocol};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use regex::Regex;

use crate::io::{IoError, PartitionLag, TopicPartition};
use crate::plan::topology::{HeldSecrets, KafkaSourceConfig, StartPosition};

use super::{
    CLIENT_ID, LAG_QUERY_TIMEOUT, LAG_SAMPLE_INTERVAL, MAX_POLL_INTERVAL, PLACE_TIMEOUT, apply_client_config,
    fetch_request_bytes, headers_of, receive_message_bytes, topic_pattern,
};

/// How long the client thread waits in one poll: how soon it sees a stop, a subscription to send, or a reader that
/// stopped reading.
const CLIENT_POLL: Duration = Duration::from_millis(100);

/// How long a changed subscription waits for the next change before it is sent. A deployment attaches its sources
/// one after another, and every subscription sent is a rebalance.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(200);

/// How often the client thread looks for readers that stopped reading.
const STALL_CHECK_EVERY: Duration = Duration::from_secs(1);

/// How often a client whose sources name topic patterns lists the cluster's topics: how soon a new topic a pattern
/// matches is read.
const TOPIC_LIST_EVERY: Duration = Duration::from_secs(30);

/// How often the sampler thread wakes to see what is due.
const SAMPLER_TICK: Duration = Duration::from_secs(1);

/// A partition the group handed over: its topic and its number.
pub(super) type Assigned = (String, i32);

/// Every shared client of one deployment. A source attaches to the client its configuration names, which is built
/// on the first attach and closed when its last source is dropped.
#[derive(Default)]
pub struct Clients {
    entries: Mutex<Vec<Entry>>,
    /// Numbers each source in the order it attached, so of two that may not both read a topic the later one is
    /// refused.
    attached: AtomicU64,
}

/// One client, as the registry knows it. `routes` outlives the client, so comparing two clients never keeps one
/// alive.
struct Entry {
    key: Vec<(String, String)>,
    /// The static member it joined its group as.
    instance: Option<String>,
    client: Weak<Client>,
    routes: Routes,
}

impl Clients {
    /// Attaches a source reading `config` to the client its configuration names, building that client if none is
    /// open. A source that would read a topic another source on the same cluster reads in the same group is
    /// attached refused: it reads nothing and says why (`Slot::refused`).
    pub(super) fn attach(
        self: &Arc<Clients>,
        config: &KafkaSourceConfig,
        secrets: &HeldSecrets,
    ) -> Result<(Arc<Client>, Arc<Slot>), IoError> {
        for topic in &config.topics {
            if let Some(Err(e)) = topic_pattern(topic) {
                return Err(IoError::transient(format!(
                    "topic pattern {topic:?} does not compile: {e}"
                )));
            }
        }
        let client_config = client_config(config, secrets)?;
        let key: Vec<(String, String)> = client_config
            .config_map()
            .into_keys()
            // `config_map` masks secrets, and two sources that differ only in a password are two identities.
            .map(|k| (k.to_string(), client_config.get(k).unwrap_or_default().to_string()))
            .collect();
        let slot = Arc::new(Slot::new(config, self.attached.fetch_add(1, Ordering::Relaxed)));

        let mut entries = self.entries.lock().unwrap();
        entries.retain(|e| e.client.strong_count() > 0);
        let client = match entries
            .iter()
            .find_map(|e| (e.key == key).then(|| e.client.upgrade()).flatten())
        {
            Some(client) => client,
            None => {
                let mut client_config = client_config;
                let instance = config.member.as_ref().map(|member| {
                    // The first place no open client of the group holds: two members under one id fence each other.
                    let held: HashSet<&str> = entries
                        .iter()
                        .filter(|e| e.routes.0.group == config.group_id)
                        .filter_map(|e| e.instance.as_deref())
                        .collect();
                    let id = (0..)
                        .map(|n| instance_id(member, n))
                        .find(|id| !held.contains(id.as_str()))
                        .unwrap();
                    client_config.set("group.instance.id", &id);
                    id
                });
                let client = Client::start(&client_config, config, Arc::downgrade(self))?;
                entries.push(Entry {
                    instance,
                    key,
                    client: Arc::downgrade(&client),
                    routes: client.routes.clone(),
                });
                client
            }
        };
        client.routes.attach(&slot);
        if let Some(cluster) = client.routes.0.cluster.get() {
            refuse_overlaps(&entries, &client.routes, cluster);
        }
        Ok((client, slot))
    }

    /// Records that `routes`' client is connected to `cluster`, and settles each topic two sources on that cluster
    /// read in one group from different clients (`refuse_overlaps`): broker lists that differ can name one cluster,
    /// and only the cluster can say so.
    fn note_cluster(&self, routes: &Routes, cluster: String) {
        let entries = self.entries.lock().unwrap();
        let cluster = routes.0.cluster.get_or_init(|| cluster);
        refuse_overlaps(&entries, routes, cluster);
    }

    /// What `routes`' patterns must skip because a source on another client reads it (`read_elsewhere`). Nothing
    /// until the client knows its cluster: `note_cluster` settles what was matched before then.
    fn read_elsewhere(&self, routes: &Routes) -> HashSet<Arc<str>> {
        let Some(cluster) = routes.0.cluster.get() else {
            return HashSet::new();
        };
        read_elsewhere(&self.entries.lock().unwrap(), routes, cluster)
    }
}

/// Settles each topic two sources read in `routes`' group on `cluster` from different clients. A source whose pattern
/// matched it skips it (`Routes::skip`), the later one when both did. Of two that name it, the later is refused.
fn refuse_overlaps(entries: &[Entry], routes: &Routes, cluster: &str) {
    for other in entries {
        let theirs = &other.routes;
        if Arc::ptr_eq(&theirs.0, &routes.0)
            || other.client.strong_count() == 0
            || theirs.0.group != routes.0.group
            || theirs.0.cluster.get().map(String::as_str) != Some(cluster)
        {
            continue;
        }
        let (my_slots, their_slots) = (routes.slots(), theirs.slots());
        for mine in &my_slots {
            for their in &their_slots {
                let (later, later_routes, earlier, earlier_routes) = if mine.seq > their.seq {
                    (mine, routes, their, theirs)
                } else {
                    (their, theirs, mine, routes)
                };
                let earlier_names = earlier.names();
                for topic in later.names() {
                    if later.lock().refused.is_some() || earlier.lock().refused.is_some() {
                        break;
                    }
                    if !earlier_names.contains(&topic) {
                        continue;
                    }
                    if !later.topics.contains(&topic) {
                        later_routes.skip(later, &topic);
                        continue;
                    }
                    if !earlier.topics.contains(&topic) {
                        earlier_routes.skip(earlier, &topic);
                        continue;
                    }
                    later_routes.refuse(
                        later,
                        format!(
                            "topic {topic:?} is already read in consumer group {:?} by another source on this worker, \
                             whose brokers {:?} name the same cluster, so the group would split its partitions between \
                             them and each would see part of it. Read it from one source, or give one of them its own \
                             group",
                            routes.0.group, earlier_routes.0.brokers
                        ),
                    );
                }
            }
        }
    }
}

/// Every topic a source not refused reads in `routes`' group on `cluster` through another client: what its patterns
/// skip.
fn read_elsewhere(entries: &[Entry], routes: &Routes, cluster: &str) -> HashSet<Arc<str>> {
    entries
        .iter()
        .filter(|e| {
            !Arc::ptr_eq(&e.routes.0, &routes.0)
                && e.client.strong_count() > 0
                && e.routes.0.group == routes.0.group
                && e.routes.0.cluster.get().map(String::as_str) == Some(cluster)
        })
        .flat_map(|e| e.routes.slots())
        .filter(|s| s.lock().refused.is_none())
        .flat_map(|s| s.names())
        .collect()
}

/// The static member a worker's client joins its group as: the worker, and the client's place among this deployment's
/// clients of the group. Sources attach in the order the bundle lists them, so a redeploy that keeps the group's clients
/// gives each the id it had. Kafka takes ASCII letters, digits, `.`, `_` and `-`, up to 249 of them.
fn instance_id(member: &str, place: usize) -> String {
    let suffix = format!("-{place}");
    member
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(249 - suffix.len())
        .chain(suffix.chars())
        .collect()
}

/// The configuration every source reading `config` through one client shares: what makes two sources' clients the
/// same client.
fn client_config(config: &KafkaSourceConfig, secrets: &HeldSecrets) -> Result<ClientConfig, IoError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        // The runtime commits explicitly, once a batch's destinations have all flushed (design doc, Runtime
        // stages).
        .set("enable.auto.commit", "false")
        // The client's queue, before its records are routed to each source's inbox.
        .set("queued.max.messages.kbytes", (config.fetch_bytes / 1024).to_string())
        // One fetch may carry half the queue, so a fetch always lands with room behind it. Both caps the same:
        // one hot partition may fill a fetch, and the request cap is what bounds the whole. What one round trip
        // to the broker brings back follows from `fetchBytes` and nothing else. The queue bound above is where
        // fetching stops, not where the memory does: a fetch already asked for from each broker still lands, so
        // the client may hold the queue plus half of it per broker.
        .set("fetch.max.bytes", fetch_request_bytes(config.fetch_bytes).to_string())
        .set(
            "fetch.message.max.bytes",
            fetch_request_bytes(config.fetch_bytes).to_string(),
        )
        // librdkafka refuses a fetch cap within 512 bytes of what it will receive in one message, and leaves
        // that at 100 MB. Raised only when the fetch needs it, so a small `fetchBytes` changes nothing.
        .set(
            "receive.message.max.bytes",
            receive_message_bytes(config.fetch_bytes).to_string(),
        )
        .set("max.poll.interval.ms", MAX_POLL_INTERVAL.as_millis().to_string())
        // librdkafka's default assignors are eager: every rebalance revokes every partition from every member. A
        // group here spans many topics and routinely thousands of partitions, so one restart would stop all of
        // them; cooperative rebalancing revokes only what moves.
        //
        // Cooperative and eager members must not share a group, so a fleet upgrade is a stop-and-restart, not a
        // rolling one.
        .set("partition.assignment.strategy", "cooperative-sticky")
        // Broker-side logs, metrics and quotas key on this; the default is the literal string "rdkafka".
        .set("client.id", CLIENT_ID);
    if let Some(rack) = &config.rack {
        client_config.set("client.rack", rack);
    }

    // After smeltr's own keys. Load-time validation already refuses the reserved ones
    // (`config_file::load::RESERVED_KAFKA_PROPS`).
    apply_client_config(&mut client_config, &config.props, config.tls.as_ref(), secrets)?;

    // `auto.offset.reset` places a partition with no committed offset at earliest or latest. For a time,
    // `Routes::place` sets it, and this is the fallback when that cannot.
    client_config.set(
        "auto.offset.reset",
        match config.start {
            StartPosition::Latest => "latest",
            StartPosition::Earliest | StartPosition::Timestamp(_) => "earliest",
        },
    );
    Ok(client_config)
}

/// One shared consumer, its thread, and its lag sampler. Dropped with its last source: the thread is joined, then
/// the consumer leaves its group.
pub(super) struct Client {
    pub(super) consumer: Arc<BaseConsumer<Routes>>,
    pub(super) routes: Routes,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    sampler: Option<Sampler>,
}

impl Client {
    fn start(
        client_config: &ClientConfig,
        config: &KafkaSourceConfig,
        clients: Weak<Clients>,
    ) -> Result<Arc<Client>, IoError> {
        let routes = Routes(Arc::new(RoutesInner {
            group: config.group_id.clone(),
            brokers: config.brokers.clone(),
            ..RoutesInner::default()
        }));
        let consumer: Arc<BaseConsumer<Routes>> = Arc::new(client_config.create_with_context(routes.clone())?);
        let stop = Arc::new(AtomicBool::new(false));
        let sampler = Sampler::start(Arc::clone(&consumer), routes.clone(), clients.clone());
        let thread = {
            let (consumer, routes, stop) = (Arc::clone(&consumer), routes.clone(), Arc::clone(&stop));
            std::thread::Builder::new()
                .name("kafka-client".to_string())
                .spawn(move || run(&consumer, &routes, &stop, &clients))
                .map_err(|e| IoError::transient(format!("could not start the Kafka client's thread: {e}")))?
        };
        Ok(Arc::new(Client {
            consumer,
            routes,
            stop,
            thread: Some(thread),
            sampler: Some(sampler),
        }))
    }

    /// Takes `slot` off this client: its topics leave the subscription, and nothing more is routed to it.
    pub(super) fn detach(&self, slot: &Arc<Slot>) {
        self.routes.detach(slot);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        // The sampler shares the consumer, so it goes before the consumer closes.
        self.sampler.take();
    }
}

/// The client thread: polls the consumer, routes each record, and keeps the subscription to what its sources read.
fn run(consumer: &BaseConsumer<Routes>, routes: &Routes, stop: &AtomicBool, clients: &Weak<Clients>) {
    let mut cluster_known = false;
    let mut stalls_checked = Instant::now();
    // A poll failing: said once per change and on the interval, not per poll.
    let mut polling = crate::logs::Relog::default();
    while !stop.load(Ordering::Relaxed) {
        routes.resubscribe_if_due(consumer);
        routes.refetch_if_due(consumer);
        if stalls_checked.elapsed() >= STALL_CHECK_EVERY {
            stalls_checked = Instant::now();
            routes.note_stalls();
        }
        // The cluster id comes with the first metadata the client receives, before the group can hand over a
        // partition, so asking before every poll refuses an overlap before a record arrives. Asked with no wait, so a
        // broker that never sends one costs nothing.
        if !cluster_known && let Some(cluster) = consumer.client().fetch_cluster_id(Duration::ZERO) {
            cluster_known = true;
            if let Some(clients) = clients.upgrade() {
                clients.note_cluster(routes, cluster);
            }
        }
        match consumer.poll(Timeout::from(CLIENT_POLL)) {
            // The timeout, or an event the client handled on the way: a rebalance, a commit's answer, an error.
            None => {}
            Some(Ok(msg)) => {
                // A fetch that returned a real record proves this broker is reachable. The other proof, a completed
                // assignment, clears from `Routes::post_rebalance`.
                routes.clear_error();
                routes.route(consumer, &msg);
            }
            // The client instance is unusable: every source on it says so on its next poll.
            Some(Err(KafkaError::MessageConsumptionFatal(code))) => {
                routes.fail(KafkaError::MessageConsumptionFatal(code));
                std::thread::sleep(CLIENT_POLL);
            }
            // Any other consumption error is transient (the client reconnects underneath), or says a partition is
            // caught up.
            Some(Err(KafkaError::MessageConsumption(_) | KafkaError::PartitionEOF(_))) => {}
            Some(Err(e)) => {
                if let Some(error) = polling.fail_due(e.to_string()) {
                    tracing::warn!(target: "kafka.poll_failed", group = %routes.group(), error, "a source could not read from Kafka; trying again");
                }
            }
        }
    }
}

/// A record taken off the client's queue and not yet read by its source.
pub(super) struct Pending {
    pub(super) raw: Vec<u8>,
    pub(super) offset: i64,
    pub(super) partition: i32,
    pub(super) topic: Arc<str>,
    pub(super) headers: Option<Vec<(String, Vec<u8>)>>,
    pub(super) timestamp: Option<i64>,
}

impl Pending {
    /// What `SourceRecord::size` counts of it.
    pub(super) fn size(&self) -> usize {
        self.raw.len() + crate::io::headers_size(&self.headers)
    }
}

/// One source on a client: its topics, where it places a fresh partition, and what it holds between the client
/// thread and its reader.
pub(super) struct Slot {
    /// The topics it names outright.
    pub(super) topics: Vec<Arc<str>>,
    /// The topics it names by pattern (`topic_pattern`). Each topic of the cluster one matches joins
    /// `SlotState::matched` once the client lists it.
    patterns: Vec<Regex>,
    /// Whether the client has listed the cluster's topics since it attached.
    listed: AtomicBool,
    /// Where it reads from (`resolve_starts`).
    pub(super) start: StartPosition,
    /// The deployed commit's committer time, in unix seconds: what orders one `start` after another.
    pub(super) at: i64,
    /// What the inbox holds before the source's partitions pause: one fetch.
    cap: usize,
    /// The order it attached in (`Clients::attached`).
    seq: u64,
    pub(super) state: Mutex<SlotState>,
    /// Signalled when a record arrives, a rebalance touches this source, or the client fails.
    pub(super) arrived: Condvar,
}

/// What a source and the client thread share, under the slot's lock.
pub(super) struct SlotState {
    /// Records routed here and not read yet, oldest first.
    pub(super) inbox: VecDeque<Pending>,
    /// What `inbox` holds, as `Pending::size` counts it.
    bytes: usize,
    /// Paused because the inbox reached its cap, until the reader drains it to half.
    full: bool,
    /// Paused by `block`, until `release`.
    pub(super) blocked: bool,
    /// Paused by `keep_membership`, until the next `poll_batch`.
    pub(super) keeping: bool,
    /// Whether the client copies each record's headers.
    pub(super) keep_headers: bool,
    /// Headers were asked for while the inbox held records copied without them. The client fetches those again
    /// (`Slot::refetch`), and the reader takes nothing until it has.
    pub(super) refetch: bool,
    /// Every partition of this source's topics the group has assigned to this client.
    pub(super) owned: HashSet<(String, i32)>,
    /// Partitions at their stop. A partition leaves when the group takes it away or hands it over again.
    pub(super) exhausted: HashSet<(String, i32)>,
    /// Partitions assigned but not yet placed at `start`. A record from one is dropped: it is not where the
    /// partition starts.
    pub(super) unplaced: HashSet<(String, i32)>,
    /// Partitions the group took, oldest first, until the source takes them.
    pub(super) revoked: Vec<(String, i32)>,
    /// The tag each placed partition's commits carry (`resolve_starts`).
    pub(super) tags: HashMap<(String, i32), String>,
    /// Each commit's answer, oldest first, until `KafkaSource::commit` takes them.
    pub(super) commit_results: Vec<Result<(), KafkaError>>,
    /// The sampler's last measurement of this source, not yet taken.
    pub(super) lag: Vec<PartitionLag>,
    /// Set once the client is unusable.
    pub(super) fatal: Option<KafkaError>,
    /// Why this source reads nothing, when another source reads one of its topics in its group.
    pub(super) refused: Option<String>,
    /// When the reader last asked for records or kept its membership.
    seen: Instant,
    /// Its topics are out of the subscription because its reader stopped reading.
    stalled: bool,
    /// The topics of the cluster its patterns matched, in the order they were found.
    matched: Vec<Arc<str>>,
    /// Topics its patterns match that another source reads, each warned of once while it stays skipped.
    skipped: HashSet<Arc<str>>,
}

impl Slot {
    fn new(config: &KafkaSourceConfig, seq: u64) -> Slot {
        Slot {
            topics: config
                .topics
                .iter()
                .filter(|t| topic_pattern(t).is_none())
                .map(|t| Arc::from(t.as_str()))
                .collect(),
            patterns: config.topics.iter().filter_map(|t| topic_pattern(t)?.ok()).collect(),
            listed: AtomicBool::new(false),
            start: config.start,
            at: config.deployed_at,
            cap: fetch_request_bytes(config.fetch_bytes),
            seq,
            state: Mutex::new(SlotState {
                inbox: VecDeque::new(),
                bytes: 0,
                full: false,
                blocked: false,
                keeping: false,
                keep_headers: false,
                refetch: false,
                owned: HashSet::new(),
                exhausted: HashSet::new(),
                unplaced: HashSet::new(),
                revoked: Vec::new(),
                tags: HashMap::new(),
                commit_results: Vec::new(),
                lag: Vec::new(),
                fatal: None,
                refused: None,
                seen: Instant::now(),
                stalled: false,
                matched: Vec::new(),
                skipped: HashSet::new(),
            }),
            arrived: Condvar::new(),
        }
    }

    /// What the inbox holds now, as its cap counts it.
    pub(super) fn held(&self) -> usize {
        self.lock().bytes
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, SlotState> {
        self.state.lock().unwrap()
    }

    /// This source's name for `topic`, interned: a reference count, not an allocation.
    fn intern(&self, state: &SlotState, topic: &str) -> Arc<str> {
        self.topics
            .iter()
            .chain(&state.matched)
            .find(|t| t.as_ref() == topic)
            .cloned()
            .unwrap_or_else(|| Arc::from(topic))
    }

    /// How it places a partition the group hands it.
    pub(super) fn placing(&self) -> Placing {
        Placing {
            start: self.start,
            at: self.at,
        }
    }

    /// Every topic it reads: those it names, and those its patterns matched.
    fn names(&self) -> Vec<Arc<str>> {
        let state = self.lock();
        self.topics.iter().chain(&state.matched).cloned().collect()
    }

    /// Whether one of its patterns matches `topic`.
    fn matches(&self, topic: &str) -> bool {
        self.patterns.iter().any(|p| p.is_match(topic))
    }

    /// Pauses every partition this source owns that it should not be fetching, and resumes the rest: all of them
    /// while it is blocked, keeping its membership, full or refused; otherwise those at their stop or waiting to be
    /// placed.
    pub(super) fn sync(&self, consumer: &BaseConsumer<Routes>, state: &SlotState) -> Result<(), IoError> {
        let hold_all = state.blocked || state.keeping || state.full || state.refused.is_some();
        let (mut paused, mut running) = (TopicPartitionList::new(), TopicPartitionList::new());
        for key in &state.owned {
            if hold_all || state.exhausted.contains(key) || state.unplaced.contains(key) {
                paused.add_partition(&key.0, key.1);
            } else {
                running.add_partition(&key.0, key.1);
            }
        }
        if paused.count() > 0 {
            consumer.pause(&paused)?;
        }
        if running.count() > 0 {
            consumer.resume(&running)?;
        }
        Ok(())
    }

    /// The oldest record in the inbox, taken out. Draining a full inbox to half its cap lets its partitions fetch
    /// again.
    pub(super) fn take(&self, consumer: &BaseConsumer<Routes>, state: &mut SlotState) -> Option<Pending> {
        let p = state.inbox.pop_front()?;
        state.bytes -= p.size();
        if state.full && state.bytes <= self.cap / 2 {
            state.full = false;
            if let Err(e) = self.sync(consumer, state) {
                tracing::warn!(target: "kafka.pause", error = %e, "could not resume a drained source");
            }
        }
        Some(p)
    }

    /// Drops what the inbox holds and seeks each partition it came from back to its oldest record, so the client
    /// fetches them again, now with what `keep_headers` asks of them. Runs on the client thread between polls, so no
    /// record taken off the queue before the seek is still on its way to the inbox; librdkafka discards those still
    /// queued. A partition not placed yet or at its stop is left as it is: what it holds is dropped when read. So is
    /// one whose seek fails, its records read as they were copied.
    fn refetch(&self, consumer: &BaseConsumer<Routes>, state: &mut SlotState) {
        let mut oldest: HashMap<(Arc<str>, i32), i64> = HashMap::new();
        for r in &state.inbox {
            let key = (r.topic.to_string(), r.partition);
            if state.owned.contains(&key) && !state.unplaced.contains(&key) && !state.exhausted.contains(&key) {
                let at = oldest.entry((Arc::clone(&r.topic), r.partition)).or_insert(r.offset);
                *at = (*at).min(r.offset);
            }
        }
        if oldest.is_empty() {
            return;
        }
        let mut seeks = TopicPartitionList::new();
        for ((topic, partition), offset) in &oldest {
            if let Err(e) = seeks.add_partition_offset(topic, *partition, Offset::Offset(*offset)) {
                tracing::warn!(target: "kafka.headers", error = %e, "could not ask to fetch records again");
                return;
            }
        }
        let sought: HashSet<(String, i32)> = match consumer.seek_partitions(seeks, PLACE_TIMEOUT) {
            Ok(sought) => sought
                .elements()
                .iter()
                .filter(|e| e.error().is_ok())
                .map(|e| (e.topic().to_string(), e.partition()))
                .collect(),
            Err(e) => {
                tracing::warn!(target: "kafka.headers", error = %e, "could not fetch records again with their headers");
                return;
            }
        };
        state
            .inbox
            .retain(|r| !sought.contains(&(r.topic.to_string(), r.partition)));
        state.bytes = state.inbox.iter().map(Pending::size).sum();
        if state.full && state.bytes <= self.cap / 2 {
            state.full = false;
            if let Err(e) = self.sync(consumer, state) {
                tracing::warn!(target: "kafka.pause", error = %e, "could not resume a drained source");
            }
        }
    }

    /// The reader asked for records or kept its membership. Answers whether that brings a stalled source back.
    pub(super) fn touch(&self) -> bool {
        let mut state = self.lock();
        state.seen = Instant::now();
        std::mem::take(&mut state.stalled)
    }
}

/// The client's context: routes what librdkafka reports back to the sources it concerns. Every callback here runs
/// from `poll`, on the client thread, or from the consumer's close.
#[derive(Clone, Default)]
pub(super) struct Routes(Arc<RoutesInner>);

#[derive(Default)]
struct RoutesInner {
    group: String,
    brokers: String,
    /// Every source attached, refused or stalled included.
    slots: Mutex<Vec<Arc<Slot>>>,
    /// The source reading each topic. A refused source has none.
    by_topic: Mutex<HashMap<String, Arc<Slot>>>,
    /// Since when the subscription has waited to be sent.
    changed: Mutex<Option<Instant>>,
    /// The client's most recent global error, until a fetch or an assignment proves it reachable (E4).
    last_error: Mutex<Option<String>>,
    /// Signing in refused, said once per change and on the interval: librdkafka asks again every few seconds.
    sign_in: Mutex<crate::logs::Relog>,
    /// The cluster the client is connected to, once it knows.
    cluster: OnceLock<String>,
    /// Some source waits for its inbox to be fetched again (`SlotState::refetch`).
    refetch: AtomicBool,
}

impl Routes {
    fn slots(&self) -> Vec<Arc<Slot>> {
        self.0.slots.lock().unwrap().clone()
    }

    fn slot_for(&self, topic: &str) -> Option<Arc<Slot>> {
        self.0.by_topic.lock().unwrap().get(topic).cloned()
    }

    /// Marks the subscription to be sent once changes settle.
    pub(super) fn changed(&self) {
        *self.0.changed.lock().unwrap() = Some(Instant::now());
    }

    /// The consumer group this client is in, for its lines.
    pub(super) fn group(&self) -> &str {
        &self.0.group
    }

    /// What `KafkaSource::connection_error` reads of the client.
    pub(super) fn last_error(&self) -> Option<String> {
        self.0.last_error.lock().unwrap().clone()
    }

    /// Recovery. Called on a fetched record and a completed assignment: either proves the brokers reachable.
    fn clear_error(&self) {
        *self.0.last_error.lock().unwrap() = None;
    }

    fn attach(&self, slot: &Arc<Slot>) {
        self.0.slots.lock().unwrap().push(Arc::clone(slot));
        let taken = {
            let mut by_topic = self.0.by_topic.lock().unwrap();
            match slot.topics.iter().find(|t| by_topic.contains_key(t.as_ref())) {
                Some(topic) => Some(topic.clone()),
                None => {
                    for topic in &slot.topics {
                        by_topic.insert(topic.to_string(), Arc::clone(slot));
                    }
                    None
                }
            }
        };
        match taken {
            Some(topic) => self.refuse(
                slot,
                format!(
                    "topic {topic:?} is already read in consumer group {:?} by another source on this worker, so the \
                     group would split its partitions between them and each would see part of it. Read it from one \
                     source, or give one of them its own group",
                    self.0.group
                ),
            ),
            None => self.changed(),
        }
    }

    fn detach(&self, slot: &Arc<Slot>) {
        self.0.slots.lock().unwrap().retain(|s| !Arc::ptr_eq(s, slot));
        self.0.by_topic.lock().unwrap().retain(|_, s| !Arc::ptr_eq(s, slot));
        self.changed();
    }

    /// Takes `slot`'s topics out of this client and says why on every call its source makes.
    fn refuse(&self, slot: &Arc<Slot>, why: String) {
        {
            let mut state = slot.lock();
            if state.refused.is_some() {
                return;
            }
            tracing::warn!(target: "kafka.source_refused", topics = ?slot.topics, why = %why, "a Kafka source reads nothing");
            state.refused = Some(why);
            state.inbox.clear();
            state.bytes = 0;
            // The group's revoke finds no route to this source once it is refused, so it gives its partitions up here.
            let owned = std::mem::take(&mut state.owned);
            state.exhausted.clear();
            state.unplaced.clear();
            state.tags.clear();
            state.revoked.extend(owned);
        }
        self.0.by_topic.lock().unwrap().retain(|_, s| !Arc::ptr_eq(s, slot));
        slot.arrived.notify_all();
        self.changed();
    }

    /// Whether a source that names a topic pattern has attached since the client last listed the cluster's topics.
    fn awaits_listing(&self) -> bool {
        self.slots()
            .iter()
            .any(|s| !s.patterns.is_empty() && !s.listed.load(Ordering::Acquire))
    }

    /// Whether `slot` is attached and not refused. Asked under the `by_topic` lock before a route to it is added:
    /// `detach` and `refuse` take its routes out after they change these, so a route added once they have would stay.
    fn routable(&self, slot: &Arc<Slot>) -> bool {
        slot.lock().refused.is_none() && self.0.slots.lock().unwrap().iter().any(|s| Arc::ptr_eq(s, slot))
    }

    /// Whether any source on this client names a topic pattern.
    fn has_patterns(&self) -> bool {
        self.slots().iter().any(|s| !s.patterns.is_empty())
    }

    /// Hands each topic of `listed` that a source's pattern matches to that source, and subscribes to it, and takes
    /// back each one it matched that `listed` no longer holds: a topic deleted from the cluster. A topic another source
    /// already reads, on this client or in `elsewhere`, the pattern skips (`skip`). Answers whether anything was
    /// handed over.
    fn match_topics(&self, listed: &[String], elsewhere: &HashSet<Arc<str>>) -> bool {
        let mut found = false;
        let present: HashSet<&str> = listed.iter().map(String::as_str).collect();
        for slot in self.slots() {
            if slot.patterns.is_empty() {
                continue;
            }
            slot.listed.store(true, Ordering::Release);
            let gone: Vec<Arc<str>> = {
                let mut state = slot.lock();
                state.skipped.retain(|t| present.contains(t.as_ref()));
                let (kept, gone) = std::mem::take(&mut state.matched)
                    .into_iter()
                    .partition(|t| present.contains(t.as_ref()));
                state.matched = kept;
                gone
            };
            if !gone.is_empty() {
                let mut by_topic = self.0.by_topic.lock().unwrap();
                for topic in &gone {
                    tracing::info!(target: "kafka.topic_gone", topic = %topic, "a topic a Kafka source's pattern matched is gone");
                    if by_topic.get(topic.as_ref()).is_some_and(|s| Arc::ptr_eq(s, &slot)) {
                        by_topic.remove(topic.as_ref());
                    }
                }
                drop(by_topic);
                self.changed();
            }
            for topic in listed {
                if !slot.matches(topic) {
                    continue;
                }
                if elsewhere.contains(topic.as_str()) {
                    self.skip(&slot, topic);
                    continue;
                }
                {
                    let mut by_topic = self.0.by_topic.lock().unwrap();
                    match by_topic.get(topic.as_str()) {
                        Some(owner) if Arc::ptr_eq(owner, &slot) => continue,
                        Some(_) => {
                            drop(by_topic);
                            self.skip(&slot, topic);
                            continue;
                        }
                        None if !self.routable(&slot) => break,
                        None => {
                            by_topic.insert(topic.clone(), Arc::clone(&slot));
                        }
                    }
                }
                tracing::info!(target: "kafka.topic_matched", topic = %topic, "a Kafka source's pattern matched a topic");
                let mut state = slot.lock();
                state.skipped.remove(topic.as_str());
                state.matched.push(Arc::from(topic.as_str()));
                found = true;
            }
        }
        if found {
            self.changed();
        }
        found
    }

    /// Leaves `topic` to the source that reads it: `slot`'s pattern matched it, and another source on the cluster
    /// reads it in this group. Takes it back if `slot` holds it, and warns once while it stays skipped. The next
    /// listing after that source lets it go hands it to `slot`.
    fn skip(&self, slot: &Arc<Slot>, topic: &str) {
        let held = {
            let mut state = slot.lock();
            if !state.skipped.contains(topic) {
                tracing::warn!(
                    target: "kafka.topic_skipped",
                    topic = %topic,
                    group = %self.0.group,
                    "a Kafka source's pattern matches a topic another source on this worker reads in its consumer group, \
                     so the source leaves it to that one"
                );
                state.skipped.insert(Arc::from(topic));
            }
            let before = state.matched.len();
            state.matched.retain(|t| t.as_ref() != topic);
            state.matched.len() != before
        };
        if held {
            let mut by_topic = self.0.by_topic.lock().unwrap();
            if by_topic.get(topic).is_some_and(|s| Arc::ptr_eq(s, slot)) {
                by_topic.remove(topic);
            }
            drop(by_topic);
            self.changed();
        }
    }

    /// Asks the client thread to fetch again what a source's inbox holds (`SlotState::refetch`).
    pub(super) fn request_refetch(&self) {
        self.0.refetch.store(true, Ordering::Release);
    }

    /// Fetches again the inbox of every source that asked, and wakes its reader.
    fn refetch_if_due(&self, consumer: &BaseConsumer<Routes>) {
        if !self.0.refetch.swap(false, Ordering::Acquire) {
            return;
        }
        for slot in self.slots() {
            let mut state = slot.lock();
            if std::mem::take(&mut state.refetch) {
                slot.refetch(consumer, &mut state);
                drop(state);
                slot.arrived.notify_all();
            }
        }
    }

    /// Sends the subscription once it has settled: every topic of every source attached, but those refused and those
    /// whose reader stopped reading.
    fn resubscribe_if_due(&self, consumer: &BaseConsumer<Routes>) {
        {
            let mut changed = self.0.changed.lock().unwrap();
            match *changed {
                Some(at) if at.elapsed() >= SUBSCRIBE_SETTLE => *changed = None,
                _ => return,
            }
        }
        let mut topics: Vec<String> = Vec::new();
        for slot in self.slots() {
            let state = slot.lock();
            if state.refused.is_none() && !state.stalled {
                topics.extend(slot.topics.iter().chain(&state.matched).map(|t| t.to_string()));
            }
        }
        let sent = if topics.is_empty() {
            consumer.unsubscribe();
            Ok(())
        } else {
            let names: Vec<&str> = topics.iter().map(String::as_str).collect();
            consumer.subscribe(&names)
        };
        match sent {
            Err(e) => {
                tracing::warn!(target: "kafka.subscribe_failed", group = %self.group(), error = %e, "a source could not subscribe to its topics; trying again");
                self.changed();
            }
            Ok(()) => {
                tracing::info!(target: "kafka.subscribed", group = %self.group(), topics = topics.len(), "the client subscribed to its topics")
            }
        }
    }

    /// Takes out of the subscription the topics of every source whose reader has not read for
    /// [`MAX_POLL_INTERVAL`], so the group gives their partitions to members that read. The client keeps polling, so
    /// without this a reader that stopped would hold its partitions forever.
    fn note_stalls(&self) {
        for slot in self.slots() {
            let mut state = slot.lock();
            if !state.stalled && state.refused.is_none() && state.seen.elapsed() >= MAX_POLL_INTERVAL {
                state.stalled = true;
                tracing::warn!(
                    target: "kafka.reader_stalled",
                    topics = ?slot.topics,
                    "a Kafka source's reader has not read for five minutes; its partitions go to the group's other \
                     members until it reads again"
                );
                drop(state);
                self.changed();
            }
        }
    }

    /// Hands `msg` to the inbox of the source reading its topic. A full inbox pauses that source's partitions.
    fn route(&self, consumer: &BaseConsumer<Routes>, msg: &impl Message) {
        let Some(slot) = self.slot_for(msg.topic()) else {
            return;
        };
        let mut state = slot.lock();
        let record = Pending {
            raw: msg.payload().unwrap_or(&[]).to_vec(),
            offset: msg.offset(),
            partition: msg.partition(),
            topic: slot.intern(&state, msg.topic()),
            headers: headers_of(msg, state.keep_headers),
            timestamp: msg.timestamp().to_millis(),
        };
        // A reader waits only on an empty inbox, so only the first record into one needs to wake it.
        let wake = state.inbox.is_empty();
        state.bytes += record.size();
        state.inbox.push_back(record);
        if !state.full && state.bytes >= slot.cap {
            state.full = true;
            if let Err(e) = slot.sync(consumer, &state) {
                tracing::warn!(target: "kafka.pause", error = %e, "could not pause a full source");
            }
        }
        drop(state);
        if wake {
            slot.arrived.notify_all();
        }
    }

    /// The client is unusable.
    fn fail(&self, error: KafkaError) {
        for slot in self.slots() {
            slot.lock().fatal = Some(error.clone());
            slot.arrived.notify_all();
        }
    }

    /// Sets where each newly assigned partition starts (`resolve_starts`), and answers which the broker could not
    /// place, each with the source it belongs to and the tag it carries. Runs inside the group's hand-over, so the
    /// hand-over waits for it. Every partition is asked about, so each knows the tag its commits carry.
    fn place(
        &self,
        consumer: &BaseConsumer<Routes>,
        tpl: &mut TopicPartitionList,
    ) -> Vec<(Arc<Slot>, Assigned, bool, String)> {
        let mut handed = Vec::new();
        let mut query = Vec::new();
        for e in tpl.elements() {
            let Some(slot) = self.slot_for(e.topic()) else {
                continue;
            };
            let key = (e.topic().to_string(), e.partition());
            query.push((key.clone(), slot.placing()));
            handed.push((slot, key));
        }
        let mut tags = HashMap::new();
        let mut unplaced = HashSet::new();
        if !query.is_empty() {
            match resolve_starts(consumer, &query) {
                Ok(resolved) => {
                    for ((t, p), off, tag) in resolved {
                        tags.insert((t.clone(), p), tag);
                        if matches!(off, Offset::Invalid) {
                            continue;
                        }
                        if tpl.set_partition_offset(&t, p, off).is_err() {
                            unplaced.insert((t, p));
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "kafka.placement_failed",
                        error = %e,
                        "a source could not place the partitions it was assigned; they wait, paused, and are placed again on a later poll"
                    );
                    unplaced.extend(query.into_iter().map(|(key, _)| key));
                }
            }
        }
        handed
            .into_iter()
            .map(|(slot, key)| {
                let held = unplaced.contains(&key);
                let tag = tags.get(&key).cloned().unwrap_or_default();
                (slot, key, held, tag)
            })
            .collect()
    }
}

impl ClientContext for Routes {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        config: Option<&str>,
    ) -> Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error>> {
        // librdkafka asks again on its own schedule, so a refusal stalls the client until one is answered.
        match super::entra::token(config) {
            Ok(token) => {
                self.0.sign_in.lock().unwrap().clear();
                tracing::debug!(target: "kafka.token_refreshed", group = %self.group(), principal = %token.principal_name, "signed in to Kafka as the pod's workload identity");
                Ok(token)
            }
            Err(e) => {
                if let Some(error) = self.0.sign_in.lock().unwrap().fail_due(e.to_string()) {
                    tracing::warn!(target: "kafka.sign_in_failed", group = %self.group(), error, "a source could not sign in to Kafka as the pod's workload identity, so it cannot connect until a sign-in works");
                }
                Err(e)
            }
        }
    }

    /// The most recent global error wins: a DNS failure that later becomes a connection refusal should read as the
    /// latter, not stay frozen on the first symptom seen.
    fn error(&self, error: KafkaError, reason: &str) {
        *self.0.last_error.lock().unwrap() = Some(format!("{error}: {reason}"));
    }
}

impl ConsumerContext for Routes {
    /// rdkafka's own handler with two steps added: an assignment is placed (`place`) before it is taken, and each
    /// source pauses what it should not fetch once it is.
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: bindings::rd_kafka_resp_err_t,
        tpl: &mut TopicPartitionList,
    ) {
        let assign = matches!(err, bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS);
        let revoke = matches!(err, bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS);
        let handed = if assign {
            self.place(base_consumer, tpl)
        } else {
            Vec::new()
        };
        let rebalance = if assign {
            Rebalance::Assign(tpl)
        } else if revoke {
            Rebalance::Revoke(tpl)
        } else {
            tracing::warn!(target: "kafka.rebalance_failed", group = %self.group(), error = %KafkaError::Rebalance(err.into()), "a rebalance failed, so this client's partitions stay as they were");
            Rebalance::Error(KafkaError::Rebalance(err.into()))
        };
        self.pre_rebalance(base_consumer, &rebalance);
        let rk = base_consumer.client().native_ptr();
        let cooperative = matches!(base_consumer.rebalance_protocol(), RebalanceProtocol::Cooperative);
        // SAFETY: `rk` is the live client calling this handler, and `tpl` is the list it handed over, alive for the
        // call. The same calls rdkafka's default handler makes.
        unsafe {
            match (assign, cooperative) {
                (true, true) => {
                    bindings::rd_kafka_incremental_assign(rk, tpl.ptr());
                }
                (true, false) => {
                    bindings::rd_kafka_assign(rk, tpl.ptr());
                }
                (false, true) => {
                    bindings::rd_kafka_incremental_unassign(rk, tpl.ptr());
                }
                (false, false) => {
                    bindings::rd_kafka_assign(rk, std::ptr::null());
                }
            }
        }
        // Each source takes its partitions now that they are assigned: a pause before the assignment would be lost.
        let mut touched: Vec<Arc<Slot>> = Vec::new();
        for (slot, key, held, tag) in handed {
            {
                let mut state = slot.lock();
                state.exhausted.remove(&key);
                state.unplaced.remove(&key);
                if held {
                    state.unplaced.insert(key.clone());
                } else {
                    state.tags.insert(key.clone(), tag);
                }
                state.owned.insert(key);
            }
            if !touched.iter().any(|s| Arc::ptr_eq(s, &slot)) {
                touched.push(slot);
            }
        }
        for slot in touched {
            if let Err(e) = slot.sync(base_consumer, &slot.lock()) {
                tracing::warn!(target: "kafka.placement_failed", error = %e, "a source could not hold the partitions it was assigned; they are placed again on a later poll");
            }
            slot.arrived.notify_all();
        }
        self.post_rebalance(base_consumer, &rebalance);
    }

    /// A completed assignment means this client just reached the group coordinator and the brokers owning its
    /// partitions -- proof of reachability that holds even for a topic that goes on to receive nothing, the
    /// ordinary idle case `error()` must not be mistaken for (E4's requirement: idle and unreachable read alike
    /// otherwise). A revoke or a rebalance error proves nothing either way, so only `Assign` clears.
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                tracing::info!(target: "kafka.assigned", group = %self.group(), partitions = tpl.count(), "the group assigned partitions to this client");
                self.clear_error();
            }
            Rebalance::Revoke(tpl) => {
                tracing::info!(target: "kafka.revoked", group = %self.group(), partitions = tpl.count(), "the group took partitions from this client");
            }
            Rebalance::Error(_) => {}
        }
    }

    /// A revoke, cooperative or a lost assignment, names what this client no longer owns. Each source forgets the
    /// partitions it lost, drops what its inbox holds of them, and is told. A record held from a revoked partition is
    /// its new owner's to read, and were the partition to come back, the record would belong to an ownership that
    /// has ended.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(tpl) = rebalance else {
            return;
        };
        for e in tpl.elements() {
            let Some(slot) = self.slot_for(e.topic()) else {
                continue;
            };
            let key = (e.topic().to_string(), e.partition());
            let mut state = slot.lock();
            state.owned.remove(&key);
            state.exhausted.remove(&key);
            state.unplaced.remove(&key);
            state.tags.remove(&key);
            let before = state.inbox.len();
            state
                .inbox
                .retain(|r| !(r.partition == key.1 && r.topic.as_ref() == key.0.as_str()));
            if state.inbox.len() != before {
                state.bytes = state.inbox.iter().map(Pending::size).sum();
                // Drained by the revoke rather than by its reader, which may now have nothing to take.
                if state.full && state.bytes <= slot.cap / 2 {
                    state.full = false;
                    if let Err(e) = slot.sync(consumer, &state) {
                        tracing::warn!(target: "kafka.pause", error = %e, "could not resume a drained source");
                    }
                }
            }
            state.revoked.push(key);
            drop(state);
            slot.arrived.notify_all();
        }
    }

    /// The broker's answer to a commit `KafkaSource::commit_async` handed the client, kept for the source whose
    /// topics it names. An answer naming no partition is every source's.
    fn commit_callback(&self, result: Result<(), KafkaError>, offsets: &rdkafka::TopicPartitionList) {
        let mut named: Vec<Arc<Slot>> = Vec::new();
        for e in offsets.elements() {
            if let Some(slot) = self.slot_for(e.topic())
                && !named.iter().any(|s| Arc::ptr_eq(s, &slot))
            {
                named.push(slot);
            }
        }
        if offsets.count() == 0 {
            named = self.slots();
        }
        for slot in named {
            slot.lock().commit_results.push(result.clone());
        }
    }
}

/// How a source places a partition the group hands it: its `start`, and the time of the deployed commit it runs.
#[derive(Clone, Copy, Debug)]
pub(super) struct Placing {
    pub(super) start: StartPosition,
    /// The deployed commit's committer time, in unix seconds.
    pub(super) at: i64,
}

/// A partition, where it starts, and the tag its commits carry from now on.
pub(super) type Start = ((String, i32), Offset, String);

/// What a commit's metadata says before its tag, so a string another tool committed is not read as one.
const START_TAG: &str = "smeltr-start=";

/// The `start` a committed offset descends from, and the time of the deployed commit that placed it there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Tag {
    pub(super) start: String,
    pub(super) at: i64,
}

impl Tag {
    pub(super) fn new(start: StartPosition, at: i64) -> Tag {
        let start = match start {
            StartPosition::Earliest => "earliest".to_string(),
            StartPosition::Latest => "latest".to_string(),
            StartPosition::Timestamp(ms) => format!("ts:{ms}"),
        };
        Tag { start, at }
    }

    /// The tag a commit's metadata carries: `None` for one made by anything but smeltr.
    pub(super) fn parse(metadata: &str) -> Option<Tag> {
        let (start, at) = metadata.strip_prefix(START_TAG)?.rsplit_once(";at=")?;
        Some(Tag {
            start: start.to_string(),
            at: at.parse().ok()?,
        })
    }

    pub(super) fn metadata(&self) -> String {
        format!("{START_TAG}{};at={}", self.start, self.at)
    }
}

/// Where each of `partitions` starts, and the tag its commits carry.
///
/// A partition with a committed offset reads on from it, unless its tag names another `start` than its source's
/// and was made under an older deployed commit: then it moves to its source's `start`, committed at once under the
/// new tag, so a rebalance before its first record is read finds it moved and does not resolve latest or a time
/// again. A worker still on an older commit keeps the newer tag, so a rolling deploy never moves a partition back.
/// An untagged commit, from an older build or another tool, is honoured and committed again at once under its
/// source's tag. A partition with no committed offset starts at its source's `start`: a time at each partition's
/// first record at or after it, or the end when there is none yet; `Offset::Invalid` leaves earliest and latest to the client
/// and `auto.offset.reset`.
///
/// One round trip for the committed offsets, and one for every time asked. A move adds a watermark query for each
/// partition it moves to earliest or latest, and one synchronous commit.
pub(super) fn resolve_starts(
    consumer: &BaseConsumer<Routes>,
    partitions: &[((String, i32), Placing)],
) -> Result<Vec<Start>, KafkaError> {
    let mut query = TopicPartitionList::new();
    for ((t, p), _) in partitions {
        query.add_partition(t, *p);
    }
    let placing: HashMap<(&str, i32), Placing> =
        partitions.iter().map(|((t, p), at)| ((t.as_str(), *p), *at)).collect();
    let committed = consumer.committed_offsets(query, PLACE_TIMEOUT)?;
    let mut starts = Vec::new();
    let mut times = TopicPartitionList::new();
    let mut moves = TopicPartitionList::new();
    let mut move_times = TopicPartitionList::new();
    let mut tags: HashMap<(String, i32), String> = HashMap::new();
    for e in committed.elements() {
        let key = (e.topic().to_string(), e.partition());
        let Some(at) = placing.get(&(e.topic(), e.partition())).copied() else {
            continue;
        };
        let own = Tag::new(at.start, at.at);
        if let Offset::Offset(v) = e.offset() {
            match Tag::parse(e.metadata()) {
                Some(held) if held.start != own.start && own.at > held.at => {}
                // A newer commit with the same start claims the partition at once, so a rolling deploy's older
                // commit, with another start, finds it newer than itself and moves nothing.
                Some(held) if own.at > held.at => {
                    tags.insert(key.clone(), own.metadata());
                    moves.add_partition_offset(&key.0, key.1, Offset::Offset(v))?;
                    continue;
                }
                Some(held) => {
                    starts.push((key, Offset::Offset(v), held.metadata()));
                    continue;
                }
                // Tagged at once, as a move is: a quiet partition would otherwise stay untagged until its next
                // record, and a `start` changed before then would find nothing to move.
                None => {
                    tags.insert(key.clone(), own.metadata());
                    moves.add_partition_offset(&key.0, key.1, Offset::Offset(v))?;
                    continue;
                }
            }
            tags.insert(key.clone(), own.metadata());
            match at.start {
                StartPosition::Timestamp(ms) => move_times.add_partition_offset(&key.0, key.1, Offset::Offset(ms))?,
                StartPosition::Earliest | StartPosition::Latest => {
                    let (low, high) = consumer.fetch_watermarks(&key.0, key.1, PLACE_TIMEOUT)?;
                    let v = if at.start == StartPosition::Earliest { low } else { high };
                    moves.add_partition_offset(&key.0, key.1, Offset::Offset(v))?;
                }
            }
            continue;
        }
        match at.start {
            StartPosition::Earliest | StartPosition::Latest => starts.push((key, Offset::Invalid, own.metadata())),
            StartPosition::Timestamp(ms) => {
                times.add_partition_offset(&key.0, key.1, Offset::Offset(ms))?;
                tags.insert(key, own.metadata());
            }
        }
    }
    let tag_of =
        |tags: &HashMap<(String, i32), String>, key: &(String, i32)| tags.get(key).cloned().unwrap_or_default();
    if times.count() > 0 {
        for e in consumer.offsets_for_times(times, PLACE_TIMEOUT)?.elements() {
            let off = match e.offset() {
                Offset::Offset(v) => Offset::Offset(v),
                // Nothing at or after the time yet: wait for new data rather than replay from the beginning.
                _ => Offset::End,
            };
            let key = (e.topic().to_string(), e.partition());
            let tag = tag_of(&tags, &key);
            starts.push((key, off, tag));
        }
    }
    if move_times.count() > 0 {
        for e in consumer.offsets_for_times(move_times, PLACE_TIMEOUT)?.elements() {
            let v = match e.offset() {
                Offset::Offset(v) => v,
                // Nothing at or after the time yet: the end, as a start at that time would wait there.
                _ => consumer.fetch_watermarks(e.topic(), e.partition(), PLACE_TIMEOUT)?.1,
            };
            moves.add_partition_offset(e.topic(), e.partition(), Offset::Offset(v))?;
        }
    }
    if moves.count() > 0 {
        let mut tagged = TopicPartitionList::new();
        for e in moves.elements() {
            let key = (e.topic().to_string(), e.partition());
            let tag = tag_of(&tags, &key);
            tagged.add_partition_offset(&key.0, key.1, e.offset())?;
            if let Some(mut entry) = tagged.find_partition(&key.0, key.1) {
                entry.set_metadata(tag.clone());
            }
            starts.push((key, e.offset(), tag));
        }
        consumer.commit(&tagged, CommitMode::Sync)?;
    }
    Ok(starts)
}

/// What the client thread must never wait on a broker for, asked on a thread of its own for every source on one
/// client: each source's lag, and the topics its patterns match.
///
/// A reader must never wait on a broker to report lag, and lag must keep updating while a source is blocked or
/// backpressured -- the states in which librdkafka's own statistics stop refreshing the high watermark, because a
/// paused or queue-full partition sends no fetches. So the two broker queries stay, and this thread pays for them.
///
/// ```text
///   sampler thread ──(every LAG_SAMPLE_INTERVAL)──► committed_offsets, fetch_watermarks ──► each source's slot
///                  ──(every TOPIC_LIST_EVERY, while a source names a pattern)──► the cluster's topics ──► Routes
///   reader thread  ──(every step)──► KafkaSource::lag: take its slot's
/// ```
struct Sampler {
    /// `true` once the client is dropping. The condvar wakes the thread from its sleep.
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Sampler {
    fn start(consumer: Arc<BaseConsumer<Routes>>, routes: Routes, clients: Weak<Clients>) -> Sampler {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = {
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("kafka-sampler".to_string())
                .spawn(move || {
                    // Both are due at once: a fresh source should show its backlog on the first heartbeat, and read
                    // what its patterns match without waiting out a listing.
                    let mut sampled: Option<Instant> = None;
                    let listed: Cell<Option<Instant>> = Cell::new(None);
                    // Also run between lag queries, so a slow broker behind one partition does not hold back a new
                    // topic. A source that names a pattern and attached since the last listing is listed for now.
                    let list_if_due = || {
                        if routes.has_patterns()
                            && (routes.awaits_listing()
                                || listed.get().is_none_or(|at| at.elapsed() >= TOPIC_LIST_EVERY))
                        {
                            listed.set(Some(Instant::now()));
                            list_topics(&consumer, &routes, &clients);
                        }
                    };
                    loop {
                        let stopped = || *stop.0.lock().unwrap();
                        list_if_due();
                        if sampled.is_none_or(|at| at.elapsed() >= LAG_SAMPLE_INTERVAL) {
                            sampled = Some(Instant::now());
                            measure_lag(&consumer, &routes, &stopped, &list_if_due);
                        }
                        let (flag, wake) = &*stop;
                        let guard = flag.lock().unwrap();
                        let (guard, _) = wake
                            .wait_timeout_while(guard, SAMPLER_TICK, |stopped| !*stopped)
                            .unwrap();
                        if *guard {
                            return;
                        }
                    }
                })
                .expect("spawning the Kafka sampler thread")
        };
        Sampler {
            stop,
            thread: Some(thread),
        }
    }
}

/// Lists the cluster's topics and hands those a pattern matches to its source. A topic whose name starts with `__`
/// is the cluster's own, and no pattern reads it. Once a topic is handed over, the sources on other clients on the
/// same cluster are checked against it.
fn list_topics(consumer: &BaseConsumer<Routes>, routes: &Routes, clients: &Weak<Clients>) {
    let metadata = match consumer.fetch_metadata(None, LAG_QUERY_TIMEOUT) {
        Ok(metadata) => metadata,
        Err(e) => {
            tracing::debug!(target: "kafka.topic_list_failed", group = %routes.group(), error = %e, "the cluster's topics could not be listed, so sources on other clients are not checked against them this round");
            return;
        }
    };
    let listed: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|t| !t.starts_with("__"))
        .collect();
    let clients = clients.upgrade();
    let elsewhere = clients.as_ref().map(|c| c.read_elsewhere(routes)).unwrap_or_default();
    if routes.match_topics(&listed, &elsewhere)
        && let Some(cluster) = routes.0.cluster.get()
        && let Some(clients) = clients
    {
        clients.note_cluster(routes, cluster.clone());
    }
}

impl Drop for Sampler {
    /// Joins the thread, so the consumer it shares is closed when the client is gone and not when the thread next
    /// wakes. Waits at most one query in flight ([`LAG_QUERY_TIMEOUT`]).
    fn drop(&mut self) {
        let (flag, wake) = &*self.stop;
        *flag.lock().unwrap() = true;
        wake.notify_all();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// A partition and the offset committed on it.
type Committed = (String, i32, i64);

/// Every assigned partition's lag against the *committed* offset, not the read position: what durable progress each
/// source can prove, the same coordinate `commit` writes. A blocked or rebalancing source still has committed offsets
/// and a broker to ask, so this reports through both -- exactly when lag is worth seeing grow. Each source's lag is
/// handed to it as soon as its own partitions are measured, so a slow broker behind one source does not hold back the
/// others. Nothing is handed over when the broker did not answer, or once `stopped` turns true.
fn measure_lag(consumer: &BaseConsumer<Routes>, routes: &Routes, stopped: &dyn Fn() -> bool, between: &dyn Fn()) {
    let assignment = match consumer.assignment() {
        Ok(assignment) => assignment,
        Err(e) => {
            tracing::debug!(target: "kafka.lag_unmeasured", group = %routes.group(), error = %e, "the assignment could not be read, so lag is not measured this round");
            return;
        }
    };
    if assignment.elements().is_empty() {
        return;
    }
    // One batched call for every partition's committed offset.
    let committed = match consumer.committed_offsets(assignment, LAG_QUERY_TIMEOUT) {
        Ok(committed) => committed,
        Err(e) => {
            tracing::debug!(target: "kafka.lag_unmeasured", group = %routes.group(), error = %e, "the committed offsets could not be read, so lag is not measured this round");
            return;
        }
    };

    // Each source's partitions, with the offset committed on each.
    let mut by_slot: Vec<(Arc<Slot>, Vec<Committed>)> = Vec::new();
    for elem in committed.elements() {
        // No durable offset yet on this partition: nothing to measure it against.
        let Offset::Offset(committed_offset) = elem.offset() else {
            continue;
        };
        let Some(slot) = routes.slot_for(elem.topic()) else {
            continue;
        };
        let partition = (elem.topic().to_string(), elem.partition(), committed_offset);
        match by_slot.iter_mut().find(|(s, _)| Arc::ptr_eq(s, &slot)) {
            Some((_, partitions)) => partitions.push(partition),
            None => by_slot.push((slot, vec![partition])),
        }
    }

    for (slot, partitions) in by_slot {
        let mut lags = Vec::new();
        for (topic, partition, committed_offset) in partitions {
            // One query per partition, so a dropping client is not made to wait for all of them.
            if stopped() {
                return;
            }
            between();
            // No batched form of this call exists in librdkafka -- one round trip per partition.
            let (_, high) = match consumer.fetch_watermarks(&topic, partition, LAG_QUERY_TIMEOUT) {
                Ok(marks) => marks,
                Err(e) => {
                    tracing::debug!(target: "kafka.lag_unmeasured", group = %routes.group(), %topic, partition, error = %e, "a partition's high watermark could not be read, so its lag is not measured this round");
                    continue;
                }
            };
            let topic = slot
                .topics
                .iter()
                .find(|t| t.as_ref() == topic)
                .cloned()
                .unwrap_or_else(|| Arc::from(topic.as_str()));
            lags.push(PartitionLag {
                partition: TopicPartition::new(topic, partition),
                lag: (high - committed_offset).max(0),
            });
        }
        if !lags.is_empty() {
            slot.lock().lag = lags;
        }
    }
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::RDKafkaErrorCode;

    #[test]
    fn a_client_starts_clear_and_keeps_its_most_recent_error() {
        let routes = Routes::default();
        assert_eq!(routes.last_error(), None, "nothing has gone wrong yet");

        ClientContext::error(
            &routes,
            KafkaError::Rebalance(RDKafkaErrorCode::BrokerTransportFailure),
            "first symptom: DNS",
        );
        assert!(routes.last_error().unwrap().contains("first symptom"));

        // A later, different error replaces it: the operator should see what is wrong *now*, not what was wrong
        // first.
        ClientContext::error(
            &routes,
            KafkaError::Rebalance(RDKafkaErrorCode::BrokerTransportFailure),
            "second symptom: refused",
        );
        assert!(routes.last_error().unwrap().contains("second symptom"));
    }

    /// Only a completed assignment counts as proof of reachability. A revoke or a rebalance error reached this
    /// callback at all only because the broker *is* reachable, but neither is itself evidence of that -- a revoke
    /// fires on the way out of a group too, and an error is the opposite of reachable -- so recorded errors must
    /// survive both.
    #[test]
    fn only_a_completed_assignment_clears_the_recorded_error() {
        let routes = Routes::default();
        let tpl = TopicPartitionList::new();
        // The callbacks name the consumer they belong to; this one is never polled.
        let consumer: BaseConsumer<Routes> = ClientConfig::new().create_with_context(routes.clone()).unwrap();

        ClientContext::error(
            &routes,
            KafkaError::Rebalance(RDKafkaErrorCode::BrokerTransportFailure),
            "down",
        );
        routes.post_rebalance(&consumer, &Rebalance::Revoke(&tpl));
        assert!(routes.last_error().is_some(), "a revoke is not proof of reachability");

        routes.post_rebalance(
            &consumer,
            &Rebalance::Error(KafkaError::Rebalance(RDKafkaErrorCode::BrokerTransportFailure)),
        );
        assert!(
            routes.last_error().is_some(),
            "a rebalance error is not proof of reachability either"
        );

        routes.post_rebalance(&consumer, &Rebalance::Assign(&tpl));
        assert_eq!(
            routes.last_error(),
            None,
            "an assignment is proof: it reached the group coordinator"
        );
    }

    fn config(topics: &[&str]) -> KafkaSourceConfig {
        KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: "127.0.0.1:1".to_string(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            group_id: "g".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        }
    }

    /// A commit's answer goes to the source whose topic it names, in order and once; one naming no partition is every
    /// source's.
    #[test]
    fn a_commit_answer_goes_to_the_source_whose_topic_it_names() {
        let routes = Routes::default();
        let (a, b) = (
            Arc::new(Slot::new(&config(&["a"]), 0)),
            Arc::new(Slot::new(&config(&["b"]), 1)),
        );
        routes.attach(&a);
        routes.attach(&b);

        let mut names_b = TopicPartitionList::new();
        names_b.add_partition("b", 0);
        let refused = || Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::RebalanceInProgress));
        routes.commit_callback(Ok(()), &names_b);
        routes.commit_callback(refused(), &names_b);
        routes.commit_callback(Ok(()), &TopicPartitionList::new());

        assert_eq!(a.lock().commit_results, vec![Ok(())]);
        assert_eq!(b.lock().commit_results, vec![Ok(()), refused(), Ok(())]);
    }

    /// A source refused after the group handed it partitions gives them up: it owns nothing, and its reader hears
    /// them revoked.
    #[test]
    fn a_source_refused_after_its_assignment_gives_its_partitions_up() {
        let routes = Routes::default();
        let slot = Arc::new(Slot::new(&config(&["a"]), 0));
        routes.attach(&slot);
        {
            let mut state = slot.lock();
            for p in 0..2 {
                state.owned.insert(("a".to_string(), p));
            }
            state.exhausted.insert(("a".to_string(), 0));
            state.unplaced.insert(("a".to_string(), 1));
        }
        routes.refuse(&slot, "refused".to_string());

        let mut state = slot.lock();
        assert!(state.owned.is_empty() && state.exhausted.is_empty() && state.unplaced.is_empty());
        state.revoked.sort();
        assert_eq!(state.revoked, [("a".to_string(), 0), ("a".to_string(), 1)]);
    }

    /// Of two sources reading one topic in one group on one client, the later reads nothing and says why.
    #[test]
    fn a_second_source_on_a_topic_is_refused() {
        let routes = Routes::default();
        let first = Arc::new(Slot::new(&config(&["a", "b"]), 0));
        let second = Arc::new(Slot::new(&config(&["c", "b"]), 1));
        routes.attach(&first);
        routes.attach(&second);

        assert!(first.lock().refused.is_none());
        let why = second
            .lock()
            .refused
            .clone()
            .expect("the second reader of b is refused");
        assert!(why.contains("\"b\""), "{why}");
        assert!(Arc::ptr_eq(&routes.slot_for("b").unwrap(), &first));
        assert!(routes.slot_for("c").is_none(), "a refused source routes nothing");
    }

    /// A pattern matches whole topic names, and only once the client lists them; each topic it matches routes to its
    /// source and joins the subscription.
    #[test]
    fn a_pattern_reads_the_listed_topics_it_matches_whole() {
        let routes = Routes::default();
        let slot = Arc::new(Slot::new(&config(&["^orders-.*", "audit"]), 0));
        routes.attach(&slot);
        assert!(
            routes.slot_for("orders-eu").is_none(),
            "nothing is matched before a listing"
        );

        let listed = ["orders-eu", "orders-us", "old-orders-eu", "orders", "audit"].map(String::from);
        assert!(routes.match_topics(&listed, &HashSet::new()));
        for topic in ["orders-eu", "orders-us", "audit"] {
            assert!(Arc::ptr_eq(&routes.slot_for(topic).unwrap(), &slot), "{topic}");
        }
        for topic in ["old-orders-eu", "orders"] {
            assert!(routes.slot_for(topic).is_none(), "{topic} is not matched whole");
        }
        let mut names: Vec<String> = slot.names().iter().map(|t| t.to_string()).collect();
        names.sort();
        assert_eq!(names, ["audit", "orders-eu", "orders-us"]);
        assert!(
            !routes.match_topics(&listed, &HashSet::new()),
            "a second listing of the same topics hands nothing over"
        );

        // Deleted from the cluster: taken back, and handed over again once it is back.
        let without = ["orders-us", "audit"].map(String::from);
        routes.match_topics(&without, &HashSet::new());
        assert!(routes.slot_for("orders-eu").is_none(), "a deleted topic routes nowhere");
        let mut names: Vec<String> = slot.names().iter().map(|t| t.to_string()).collect();
        names.sort();
        assert_eq!(
            names,
            ["audit", "orders-us"],
            "a literal topic stays whatever a listing says"
        );
        assert!(routes.match_topics(&listed, &HashSet::new()));
        assert!(Arc::ptr_eq(&routes.slot_for("orders-eu").unwrap(), &slot));
    }

    /// A topic a pattern matches that another source on the client already reads is skipped: the pattern's source
    /// reads the rest, and the other keeps it. A source that names a topic a pattern already holds is refused.
    #[test]
    fn a_pattern_skips_a_topic_another_source_reads() {
        let listed = ["orders-eu", "orders-us", "billing-eu"].map(String::from);
        let none = HashSet::new();

        for seqs in [(0, 1), (1, 0)] {
            let routes = Routes::default();
            let literal = Arc::new(Slot::new(&config(&["orders-eu"]), seqs.0));
            let pattern = Arc::new(Slot::new(&config(&["^orders-.*"]), seqs.1));
            routes.attach(&literal);
            routes.attach(&pattern);
            routes.match_topics(&listed, &none);
            assert!(literal.lock().refused.is_none(), "{seqs:?}");
            assert!(pattern.lock().refused.is_none(), "{seqs:?}");
            assert!(
                Arc::ptr_eq(&routes.slot_for("orders-eu").unwrap(), &literal),
                "{seqs:?}"
            );
            assert!(
                Arc::ptr_eq(&routes.slot_for("orders-us").unwrap(), &pattern),
                "{seqs:?}"
            );
        }

        let routes = Routes::default();
        let orders = Arc::new(Slot::new(&config(&["^orders-.*"]), 0));
        let eu = Arc::new(Slot::new(&config(&["^.*-eu"]), 1));
        routes.attach(&orders);
        routes.attach(&eu);
        routes.match_topics(&listed, &none);
        assert!(orders.lock().refused.is_none());
        assert!(eu.lock().refused.is_none(), "overlapping patterns are both read");
        assert!(Arc::ptr_eq(&routes.slot_for("orders-eu").unwrap(), &orders));
        assert!(Arc::ptr_eq(&routes.slot_for("billing-eu").unwrap(), &eu));
        assert!(eu.lock().skipped.contains("orders-eu"));
        assert!(
            !routes.match_topics(&listed, &none),
            "a skipped topic stays skipped while its reader reads it"
        );

        // Once the source that reads it is gone, the next listing hands it over.
        routes.detach(&orders);
        assert!(routes.match_topics(&listed, &none));
        assert!(Arc::ptr_eq(&routes.slot_for("orders-eu").unwrap(), &eu));
        assert!(eu.lock().skipped.is_empty());

        // A source that names a topic a pattern already holds is refused.
        let late = Arc::new(Slot::new(&config(&["billing-eu"]), 2));
        routes.attach(&late);
        assert!(late.lock().refused.is_some());
        assert!(Arc::ptr_eq(&routes.slot_for("billing-eu").unwrap(), &eu));
    }

    /// A topic a pattern matches that a source on another client on the same cluster reads is skipped, whichever
    /// came first.
    #[test]
    fn a_pattern_skips_a_topic_read_through_another_client_on_one_cluster() {
        let listed = ["orders-eu", "orders-us"].map(String::from);
        let clients = Arc::new(Clients::default());
        let (first_client, first) = clients.attach(&config(&["orders-eu"]), &Default::default()).unwrap();
        let mut elsewhere = config(&["^orders-.*"]);
        elsewhere.brokers = "127.0.0.1:2".to_string();
        let (second_client, second) = clients.attach(&elsewhere, &Default::default()).unwrap();

        // Matched before the client knew its cluster: settled once it does.
        second_client
            .routes
            .match_topics(&listed, &clients.read_elsewhere(&second_client.routes));
        assert_eq!(second.names().len(), 2);
        clients.note_cluster(&first_client.routes, "cluster-1".to_string());
        clients.note_cluster(&second_client.routes, "cluster-1".to_string());
        assert!(first.lock().refused.is_none());
        assert!(second.lock().refused.is_none(), "the pattern's source is not refused");
        assert_eq!(second.names(), [Arc::<str>::from("orders-us")]);
        assert!(second_client.routes.slot_for("orders-eu").is_none());

        // Matched once the cluster is known: skipped from the start.
        assert!(
            !second_client
                .routes
                .match_topics(&listed, &clients.read_elsewhere(&second_client.routes))
        );
        assert_eq!(second.names(), [Arc::<str>::from("orders-us")]);

        // A source on another client that names a topic a pattern holds takes it: the pattern lets it go.
        let mut late = config(&["orders-us"]);
        late.brokers = "127.0.0.1:3".to_string();
        let (late_client, late) = clients.attach(&late, &Default::default()).unwrap();
        clients.note_cluster(&late_client.routes, "cluster-1".to_string());
        assert!(late.lock().refused.is_none());
        assert!(second.lock().refused.is_none());
        assert!(second.names().is_empty());
        assert!(second_client.routes.slot_for("orders-us").is_none());

        // A pattern that attached first but matched before its client knew the cluster still leaves the topic to the
        // source that names it.
        let clients = Arc::new(Clients::default());
        let mut early = config(&["^a-.*"]);
        early.brokers = "127.0.0.1:2".to_string();
        let (pattern_client, pattern) = clients.attach(&early, &Default::default()).unwrap();
        let (literal_client, literal) = clients.attach(&config(&["a-1"]), &Default::default()).unwrap();
        pattern_client
            .routes
            .match_topics(&["a-1".to_string()], &clients.read_elsewhere(&pattern_client.routes));
        clients.note_cluster(&literal_client.routes, "cluster-1".to_string());
        clients.note_cluster(&pattern_client.routes, "cluster-1".to_string());
        assert!(
            literal.lock().refused.is_none(),
            "the source that names it is not refused"
        );
        assert!(pattern.lock().refused.is_none());
        assert!(pattern.names().is_empty());
    }

    /// A pattern that does not compile is refused when its source attaches.
    #[test]
    fn a_pattern_that_does_not_compile_is_refused_on_attach() {
        let clients = Arc::new(Clients::default());
        let Err(e) = clients.attach(&config(&["^orders-("]), &Default::default()) else {
            panic!("attached")
        };
        assert!(e.to_string().contains("orders-("), "{e}");
    }

    /// A worker's zone is its consumers' rack.
    #[test]
    fn a_zone_is_the_rack() {
        let mut zoned = config(&["a"]);
        zoned.rack = Some("eu-west-1a".to_string());
        let built = client_config(&zoned, &Default::default()).unwrap();
        assert_eq!(built.get("client.rack"), Some("eu-west-1a"));
        assert_eq!(
            client_config(&config(&["a"]), &Default::default())
                .unwrap()
                .get("client.rack"),
            None
        );
    }

    /// A static member's id is the worker's and its place among the group's clients, in what Kafka takes.
    #[test]
    fn a_static_members_id_is_the_worker_and_its_place() {
        assert_eq!(instance_id("host-1.zone_a", 0), "host-1.zone_a-0");
        assert_eq!(instance_id("host 1/ä", 2), "host_1__-2");
        let long = instance_id(&"w".repeat(300), 12);
        assert_eq!(long.len(), 249);
        assert!(long.ends_with("w-12"));
    }

    /// Each client of a group joins under a place no open client of the group holds.
    #[test]
    fn each_client_of_a_group_joins_under_its_own_id() {
        let clients = Arc::new(Clients::default());
        let member = |brokers: &str, group: &str| {
            let mut c = config(&["a"]);
            c.member = Some("w".to_string());
            c.brokers = brokers.to_string();
            c.group_id = group.to_string();
            c
        };
        // Each open client's group and id, in the order they attached.
        let ids = |clients: &Clients| -> Vec<(String, Option<String>)> {
            let entries = clients.entries.lock().unwrap();
            entries
                .iter()
                .filter(|e| e.client.strong_count() > 0)
                .map(|e| (e.routes.0.group.clone(), e.instance.clone()))
                .collect()
        };
        let joined = |group: &str, id: &str| (group.to_string(), Some(id.to_string()));
        let (first, _) = clients
            .attach(&member("127.0.0.1:1", "g"), &Default::default())
            .unwrap();
        let (second, _) = clients
            .attach(&member("127.0.0.1:2", "g"), &Default::default())
            .unwrap();
        let (_other, _) = clients
            .attach(&member("127.0.0.1:1", "h"), &Default::default())
            .unwrap();
        assert_eq!(
            ids(&clients),
            [joined("g", "w-0"), joined("g", "w-1"), joined("h", "w-0")]
        );

        // The place a closed client let go is taken again, rather than a second w-1.
        drop(first);
        let (_third, _) = clients
            .attach(&member("127.0.0.1:3", "g"), &Default::default())
            .unwrap();
        assert_eq!(
            ids(&clients),
            [joined("g", "w-1"), joined("h", "w-0"), joined("g", "w-0")]
        );
        drop(second);
    }

    /// Sources with the same client configuration share one client; another group is another client.
    #[test]
    fn sources_with_one_configuration_share_one_client() {
        let clients = Arc::new(Clients::default());
        let (one, _) = clients.attach(&config(&["a"]), &Default::default()).unwrap();
        let (two, _) = clients.attach(&config(&["b"]), &Default::default()).unwrap();
        let mut other = config(&["c"]);
        other.group_id = "h".to_string();
        let (three, _) = clients.attach(&other, &Default::default()).unwrap();

        assert!(Arc::ptr_eq(&one, &two));
        assert!(!Arc::ptr_eq(&one, &three));
    }

    /// Two sources that differ only in a password are two identities, each on a client of its own.
    #[test]
    fn sources_that_differ_only_in_a_password_do_not_share_a_client() {
        let clients = Arc::new(Clients::default());
        let with_password = |password: &str| {
            let mut c = config(&["a"]);
            c.props.literal = vec![
                ("sasl.username".to_string(), "u".to_string()),
                ("sasl.password".to_string(), password.to_string()),
            ];
            c
        };
        let (one, _) = clients.attach(&with_password("p1"), &Default::default()).unwrap();
        let (two, _) = clients.attach(&with_password("p2"), &Default::default()).unwrap();

        assert!(!Arc::ptr_eq(&one, &two));
    }

    /// Two clients whose broker lists differ but name one cluster refuse the later of two sources reading one topic
    /// in one group.
    #[test]
    fn a_second_source_on_a_topic_through_another_client_on_one_cluster_is_refused() {
        let clients = Arc::new(Clients::default());
        let (first_client, first) = clients.attach(&config(&["a"]), &Default::default()).unwrap();
        let mut elsewhere = config(&["a"]);
        elsewhere.brokers = "127.0.0.1:2".to_string();
        let (second_client, second) = clients.attach(&elsewhere, &Default::default()).unwrap();
        assert!(!Arc::ptr_eq(&first_client, &second_client));

        clients.note_cluster(&first_client.routes, "cluster-1".to_string());
        assert!(
            second.lock().refused.is_none(),
            "the second client's cluster is not known yet"
        );
        clients.note_cluster(&second_client.routes, "cluster-1".to_string());
        assert!(first.lock().refused.is_none());
        let why = second.lock().refused.clone().expect("the later source is refused");
        assert!(
            why.contains("127.0.0.1:1"),
            "it names the other source's brokers: {why}"
        );
    }
}
