//! Kafka sources against a live broker, under random configurations and random operations. Each seed writes a few
//! topics, opens one to four sources on one `Clients` -- topics named outright and `^` patterns from one pool, so they
//! overlap; one or two consumer groups and two spellings of the brokers, so some sources share a client and some share
//! a group across clients -- and runs random steps on them: polls of random sizes, headers kept and dropped, blocks and
//! releases, kept membership, commits, records written, and topics created and deleted mid-run. It then drains every
//! source and holds:
//!
//! 1. Of each consumer group, one source reads each topic: the one that names it, or else one whose pattern matches
//!    it. Every record of it arrives there exactly once, in offset order per partition, from the source's start to its
//!    stop, as it was written. A record is read again only after its partition was revoked, from where the group last
//!    committed.
//! 2. A record carries its headers whenever they were asked for when it was read, and none before they ever were.
//! 3. A source that names a topic another source of its group read first is refused: it reads nothing and says why. A
//!    source refused has such a source to blame, and a source that names no topic outright is never refused: its
//!    pattern skips what another reads.
//! 4. No sequence panics, fails a poll, or hangs.
//!
//! Seeds run at once, each on topics and groups of its own: a topic created mid-run waits for the next listing, up to
//! thirty seconds, and seeds run one after another would wait for it in turn.
//!
//! `event_hubs_sources_read_each_record_this_run_sent_once_whatever_the_sequence` holds what it can of the same for
//! `eventHubs` sources against the Event Hubs emulator, whose one hub and consumer group every run shares.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use smeltr::format::encoding::Encoding;
use smeltr::io::kafka::{Clients, KafkaSource};
use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::{KafkaService, KafkaSourceConfig, StartPosition, StopPosition};

use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

const SEEDS: std::ops::Range<u64> = 0..8;

/// How long the drain waits for every topic to be read through: the listing that finds a topic created mid-run comes
/// up to thirty seconds after the last, and the group hands it over after that.
const DRAIN: Duration = Duration::from_secs(90);

/// How long one seed may run before it is taken to hang.
const SEED_BUDGET: Duration = Duration::from_secs(240);

/// The two fetch sizes: the least the loader allows, whose inbox a large topic fills, and the default.
const SMALL_FETCH: usize = 4 * 1024 * 1024;
const LARGE_FETCH: usize = 64 * 1024 * 1024;

/// Every partition starts with at least this many records, so an offset or time start inside it always exists: a
/// start past the end would leave it to `auto.offset.reset` or the end, and a record written then would race it.
const LEAST_RECORDS: usize = 4;
/// The committer time every source here runs under.
const DEPLOYED_AT: i64 = 2;
/// A commit made under `latest` and a deployed commit older than [`DEPLOYED_AT`], so every source's `start` moves it.
const OLDER_TAG: &str = "smeltr-start=latest;at=1";

/// Patterns over the topic names `{prefix}-{family}{index}`, `@` standing for the escaped prefix.
const PATTERNS: &[&str] = &["^@-a.*", "^@-b.*", "^@-[bc].*", "^@-(a|b)[0-2]", "^@-.[13579]", "^@-.*"];

const COVERAGE: &[&str] = &[
    "a client shared",
    "a group across clients",
    "two groups",
    "a source refused",
    "a pattern read a topic",
    "a pattern skipped a topic",
    "a topic created mid-run read",
    "a topic deleted",
    "several partitions",
    "start earliest",
    "start time",
    "a start moved over an older commit",
    "stop time",
    "an empty range",
    "no stop",
    "headers kept",
    "headers dropped after kept",
    "block",
    "release",
    "keep membership",
    "commit",
    "an inbox over its cap",
    "a batch over its bytes",
];

#[test]
fn kafka_sources_read_each_record_once_at_its_owner_whatever_the_sequence() {
    let Some(brokers) = live::kafka("kafka_sources_read_each_record_once_at_its_owner_whatever_the_sequence") else {
        return;
    };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seeds = fuzz::seeds(SEEDS);
    let (tx, rx) = mpsc::channel();
    for seed in seeds.clone() {
        let (tx, brokers) = (tx.clone(), brokers.clone());
        std::thread::spawn(move || {
            let plan = Plan::new(seed, &format!("kfz-{run}-s{seed}"));
            let result = match catch_unwind(AssertUnwindSafe(|| execute(&plan, &brokers))) {
                Ok(Ok(reached)) => Ok(reached),
                Ok(Err(why)) => Err(format!("{why}\n{plan:#?}")),
                Err(panic) => {
                    let why = panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("");
                    Err(format!("panicked: {why}\n{plan:#?}"))
                }
            };
            let _ = tx.send((seed, result));
        });
    }
    drop(tx);
    let mut results: HashMap<u64, Result<BTreeSet<&'static str>, String>> = HashMap::new();
    let deadline = Instant::now() + SEED_BUDGET;
    while results.len() < seeds.clone().count() {
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok((seed, result)) => {
                results.insert(seed, result);
            }
            Err(_) => break,
        }
    }
    let mut coverage = Coverage::new(COVERAGE.iter().copied());
    fuzz::run(SEEDS, |seed| match results.remove(&seed) {
        Some(Ok(reached)) => {
            reached.into_iter().for_each(|r| coverage.reach(r));
            Ok(())
        }
        Some(Err(why)) => Err(why),
        None => Err(format!(
            "did not finish in {SEED_BUDGET:?}: a hang\n{:#?}",
            Plan::new(seed, &format!("kfz-{run}-s{seed}"))
        )),
    });
    coverage.assert_complete();
}

// -- the plan ------------------------------------------------------------------

/// What one seed writes and does, all drawn before anything runs, so a failure prints it whole.
#[derive(Debug)]
struct Plan {
    prefix: String,
    topics: Vec<TopicSpec>,
    patterns: Vec<String>,
    sources: Vec<Spec>,
    steps: Vec<Step>,
}

#[derive(Debug)]
struct TopicSpec {
    name: String,
    /// Written before the sources open. Only these are named outright: a named topic that does not exist yet waits
    /// on the client's metadata refresh, minutes away.
    initial: bool,
    /// Records each partition starts with.
    counts: Vec<usize>,
    /// The smallest payload; each record's is up to `spread` bytes larger.
    payload: usize,
    spread: usize,
    headers: bool,
}

#[derive(Debug)]
struct Spec {
    /// Topics it names outright, by index into `Plan::topics`.
    named: Vec<usize>,
    /// Patterns it names, by index into `Plan::patterns`.
    patterns: Vec<usize>,
    group: usize,
    brokers: usize,
    fetch_bytes: usize,
    start: Start,
    /// Its group has committed the end of each topic it names under an older `start`, so its own `start` moves it.
    moved: bool,
    /// Just before the record at this offset in every partition, as `Start::Time` is.
    stop: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
enum Start {
    Earliest,
    /// Just before the record at this offset in every partition: records are stamped one second apart by offset.
    Time(i64),
}

#[derive(Debug)]
enum Step {
    Poll {
        source: usize,
        rows: usize,
        bytes: usize,
        wait_ms: u64,
    },
    Headers {
        source: usize,
        keep: bool,
    },
    Block(usize),
    Release(usize),
    KeepMembership {
        source: usize,
        ms: u64,
    },
    Commit(usize),
    Write {
        topic: usize,
        partition: i32,
        count: usize,
    },
    Create(usize),
    Delete(usize),
}

impl Plan {
    fn new(seed: u64, prefix: &str) -> Plan {
        let mut rng = Rng::seeded(seed);
        // Drawn apart from the rest, so adding moves left every other choice each seed makes as it was.
        let mut moves = Rng::seeded(seed ^ 0x5e5e7);
        let families = ["a", "b", "c"];
        let initial = 2 + rng.below(3);
        let late = 1 + rng.below(2);
        let big = rng.chance(2).then(|| rng.below(initial));
        let topics: Vec<TopicSpec> = (0..initial + late)
            .map(|i| {
                let partitions = if rng.chance(3) { 2 + rng.below(2) } else { 1 };
                let is_big = big == Some(i);
                TopicSpec {
                    name: format!("{prefix}-{}{i}", rng.pick(&families)),
                    initial: i < initial,
                    counts: (0..partitions)
                        .map(|_| if is_big { 1500 } else { LEAST_RECORDS + rng.below(60) })
                        .collect(),
                    payload: if is_big { 1500 } else { 20 },
                    spread: if is_big { 1 } else { 1 + rng.below(400) },
                    headers: rng.chance(2),
                }
            })
            .collect();
        let escaped = regex::escape(prefix);
        let mut patterns: Vec<String> = Vec::new();
        while patterns.len() < 3 {
            let p = rng.pick(PATTERNS).replace('@', &escaped);
            if !patterns.contains(&p) {
                patterns.push(p);
            }
        }
        let sources: Vec<Spec> = (0..1 + rng.below(4))
            .map(|_| {
                let (mut named, mut pats) = (Vec::new(), Vec::new());
                if rng.chance(2) {
                    for _ in 0..1 + rng.below(2) {
                        let t = rng.below(initial);
                        if !named.contains(&t) {
                            named.push(t);
                        }
                    }
                } else {
                    for _ in 0..1 + rng.below(2) {
                        let p = rng.below(patterns.len());
                        if !pats.contains(&p) {
                            pats.push(p);
                        }
                    }
                }
                Spec {
                    named,
                    patterns: pats,
                    group: usize::from(rng.chance(4)),
                    brokers: usize::from(rng.chance(3)),
                    fetch_bytes: if rng.chance(2) { SMALL_FETCH } else { LARGE_FETCH },
                    start: match rng.below(2) {
                        0 => Start::Earliest,
                        _ => Start::Time(rng.below(LEAST_RECORDS) as i64),
                    },
                    moved: moves.chance(3),
                    stop: rng.chance(2).then(|| *rng.pick(&[2, 5, 40, 1_000_000])),
                }
            })
            .collect();

        let mut exists: Vec<bool> = topics.iter().map(|t| t.initial).collect();
        let mut deleted = false;
        let mut steps = Vec::new();
        for _ in 0..15 + rng.below(20) {
            let source = rng.below(sources.len());
            let live: Vec<usize> = (0..topics.len()).filter(|&t| exists[t]).collect();
            let unborn: Vec<usize> = (0..topics.len())
                .filter(|&t| !topics[t].initial && !exists[t])
                .collect();
            let step = match rng.below(20) {
                0..=7 => Step::Poll {
                    source,
                    rows: *rng.pick(&[1, 7, 100, 10_000]),
                    bytes: *rng.pick(&[1, 4096, usize::MAX]),
                    wait_ms: *rng.pick(&[0, 50, 300]),
                },
                8 | 9 => Step::Headers {
                    source,
                    keep: rng.chance(2),
                },
                10 => Step::Block(source),
                11 => Step::Release(source),
                12 => Step::KeepMembership {
                    source,
                    ms: *rng.pick(&[0, 200]),
                },
                13 | 14 => Step::Commit(source),
                15 | 16 if !live.is_empty() => {
                    let topic = *rng.pick(&live);
                    Step::Write {
                        topic,
                        partition: rng.below(topics[topic].counts.len()) as i32,
                        count: 1 + rng.below(50),
                    }
                }
                17 | 18 if !unborn.is_empty() => {
                    let t = *rng.pick(&unborn);
                    exists[t] = true;
                    Step::Create(t)
                }
                19 if !deleted && live.len() > 1 => {
                    let t = *rng.pick(&live);
                    exists[t] = false;
                    deleted = true;
                    Step::Delete(t)
                }
                _ => Step::Poll {
                    source,
                    rows: 100,
                    bytes: usize::MAX,
                    wait_ms: 50,
                },
            };
            // A switch of headers shows only in what is read after it.
            let read_after = matches!(step, Step::Headers { .. }) && rng.chance(2);
            steps.push(step);
            if read_after {
                steps.push(Step::Poll {
                    source,
                    rows: 100,
                    bytes: usize::MAX,
                    wait_ms: 300,
                });
            }
        }
        Plan {
            prefix: prefix.to_string(),
            topics,
            patterns,
            sources,
            steps,
        }
    }

    /// Whether source `s` reads topic `t` by name or by pattern, if no other source reads it.
    fn wants(&self, s: usize, t: usize) -> bool {
        self.names(s, t) || self.matches(s, t)
    }

    fn names(&self, s: usize, t: usize) -> bool {
        self.sources[s].named.contains(&t)
    }

    fn matches(&self, s: usize, t: usize) -> bool {
        self.sources[s].patterns.iter().any(|&p| {
            let pattern = regex::Regex::new(&format!("^(?:{})$", &self.patterns[p][1..])).unwrap();
            pattern.is_match(&self.topics[t].name)
        })
    }

    /// The offset source `s` starts every partition at.
    fn start(&self, s: usize) -> i64 {
        match self.sources[s].start {
            Start::Earliest => 0,
            Start::Time(n) => n,
        }
    }

    /// What two sources must agree on to share a client. Every start here resets to the earliest.
    fn client_key(&self, s: usize) -> (usize, usize, usize) {
        let spec = &self.sources[s];
        (spec.group, spec.brokers, spec.fetch_bytes)
    }
}

/// A record's headers, each key and value.
type Headers = Vec<(String, Vec<u8>)>;

impl TopicSpec {
    /// The record at offset `k` of partition `p`: its payload, and the headers a reader keeps of it.
    fn record(&self, p: i32, k: usize) -> (Vec<u8>, Option<Headers>) {
        shaped(&self.name, self.payload, self.spread, self.headers, p, k)
    }
}

/// The `k`th record `tag` writes to partition `p`: a payload that names it, padded to between `payload` and
/// `payload + spread` bytes, and, when `headers`, most records' headers.
fn shaped(tag: &str, payload: usize, spread: usize, headers: bool, p: i32, k: usize) -> (Vec<u8>, Option<Headers>) {
    let mut bytes = format!("{tag}/{p}/{k}/").into_bytes();
    bytes.resize(bytes.len() + payload + (k * 7919) % spread, b'x');
    let headers = (headers && k % 4 != 3).then(|| {
        let mut h = vec![("i".to_string(), k.to_string().into_bytes())];
        if k.is_multiple_of(2) {
            h.push(("p".to_string(), p.to_string().into_bytes()));
        }
        h
    });
    (bytes, headers)
}

/// Every record is stamped one second after the one before it in its partition, so a start time names the same
/// offset in every partition.
fn stamp(base: i64, k: usize) -> i64 {
    base + k as i64 * 1000
}

// -- the broker ----------------------------------------------------------------

struct Broker {
    producer: BaseProducer,
    admin: AdminClient<DefaultClientContext>,
    runtime: tokio::runtime::Runtime,
    base: i64,
}

impl Broker {
    fn new(brokers: &str) -> Broker {
        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 3_600_000;
        Broker {
            // Idempotent, so each record lands at the offset written for it.
            producer: ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("enable.idempotence", "true")
                .set("queue.buffering.max.kbytes", "1048576")
                .create()
                .unwrap(),
            admin: ClientConfig::new().set("bootstrap.servers", brokers).create().unwrap(),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            base,
        }
    }

    fn create(&self, topic: &TopicSpec) -> Result<(), String> {
        let new = NewTopic::new(&topic.name, topic.counts.len() as i32, TopicReplication::Fixed(1));
        let answers = self
            .runtime
            .block_on(self.admin.create_topics([&new], &AdminOptions::new()))
            .map_err(|e| format!("creating {}: {e}", topic.name))?;
        for answer in answers {
            answer.map_err(|(name, code)| format!("creating {name}: {code}"))?;
        }
        Ok(())
    }

    fn delete(&self, name: &str) -> Result<(), String> {
        let answers = self
            .runtime
            .block_on(self.admin.delete_topics(&[name], &AdminOptions::new()))
            .map_err(|e| format!("deleting {name}: {e}"))?;
        for answer in answers {
            answer.map_err(|(name, code)| format!("deleting {name}: {code}"))?;
        }
        Ok(())
    }

    /// Writes offsets `from..to` of `topic`'s partition `p`, and checks they landed there.
    fn write(&self, topic: &TopicSpec, p: i32, from: usize, to: usize) -> Result<(), String> {
        let records = (from..to).map(|k| {
            let (payload, headers) = topic.record(p, k);
            (payload, headers, Some(stamp(self.base, k)))
        });
        write(&self.producer, &topic.name, p, records, to as i64)
    }
}

/// Writes `records` -- each a payload, its headers and its timestamp -- to partition `p` of `topic` in order, and
/// checks they end it at offset `end`, so each landed where it was counted.
fn write(
    producer: &BaseProducer,
    topic: &str,
    p: i32,
    records: impl IntoIterator<Item = (Vec<u8>, Option<Headers>, Option<i64>)>,
    end: i64,
) -> Result<(), String> {
    for (payload, headers, timestamp) in records {
        let mut owned = OwnedHeaders::new();
        if let Some(headers) = &headers {
            for (key, value) in headers {
                owned = owned.insert(Header {
                    key,
                    value: Some(value),
                });
            }
            // A header with no value, which a reader leaves out.
            owned = owned.insert(Header::<&[u8]> {
                key: "empty",
                value: None,
            });
        }
        let mut out = BaseRecord::<(), [u8]>::to(topic).payload(&payload).partition(p);
        if let Some(t) = timestamp {
            out = out.timestamp(t);
        }
        if headers.is_some() {
            out = out.headers(owned);
        }
        loop {
            match producer.send(out) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    producer.poll(Duration::from_millis(50));
                    out = back;
                }
                Err((e, _)) => return Err(format!("writing {topic}/{p}: {e}")),
            }
        }
    }
    producer
        .flush(Timeout::After(Duration::from_secs(30)))
        .map_err(|e| format!("flushing {topic}/{p}: {e}"))?;
    let high = watermark(producer, topic, p)?;
    if high != end {
        return Err(format!(
            "{topic}/{p} ends at {high} after writing up to {end}: the broker did not take the records as written"
        ));
    }
    Ok(())
}

/// Where partition `p` of `topic` ends.
fn watermark(producer: &BaseProducer, topic: &str, p: i32) -> Result<i64, String> {
    let (_, high) = producer
        .client()
        .fetch_watermarks(topic, p, Duration::from_secs(10))
        .map_err(|e| format!("watermarks of {topic}/{p}: {e}"))?;
    Ok(high)
}

// -- reading -------------------------------------------------------------------

/// One source and what it has read.
struct Reader {
    source: KafkaSource,
    keep: bool,
    ever_kept: bool,
    /// The offset each partition must give next, once one has been read.
    next: HashMap<(String, i32), i64>,
    /// Partitions revoked since their last record: the next may start over, where the group last committed.
    revoked: HashSet<(String, i32)>,
    /// Every offset handed to `commit`, per partition.
    committed: HashMap<(String, i32), Vec<i64>>,
    /// Records read per topic.
    read: BTreeMap<String, usize>,
    bytes: usize,
    headers_dropped_then_read: bool,
    headers_read: bool,
}

/// What a source's refusal says.
const REFUSED: &str = "already read in consumer group";

impl Reader {
    fn refused(&self) -> Option<String> {
        self.source.connection_error().filter(|e| e.contains(REFUSED))
    }

    fn poll(
        &mut self,
        me: usize,
        plan: &Plan,
        written: &[Vec<usize>],
        rows: usize,
        bytes: usize,
        wait: Duration,
    ) -> Result<usize, String> {
        let got = self
            .source
            .poll_batch(rows, bytes, wait)
            .map_err(|e| format!("source {me}: poll_batch({rows}, {bytes}, {wait:?}) failed: {e}"))?;
        if got.len() > rows {
            return Err(format!("source {me}: asked for {rows} records, got {}", got.len()));
        }
        for r in &got {
            self.check(me, plan, written, r)?;
        }
        for tp in self.source.take_revoked() {
            self.revoked.insert((tp.topic.to_string(), tp.partition));
        }
        Ok(got.len())
    }

    fn check(&mut self, me: usize, plan: &Plan, written: &[Vec<usize>], r: &SourceRecord) -> Result<(), String> {
        let at = format!("source {me} read {}/{}@{}", r.topic, r.partition, r.offset);
        let Some(t) = plan.topics.iter().position(|t| *t.name == *r.topic) else {
            return Err(format!("{at}: not a topic of this seed"));
        };
        if !plan.wants(me, t) {
            return Err(format!("{at}: a topic it neither names nor matches"));
        }
        let topic = &plan.topics[t];
        let Some(&count) = written[t].get(r.partition as usize) else {
            return Err(format!("{at}: no such partition"));
        };
        if r.offset < 0 || r.offset as usize >= count {
            return Err(format!("{at}: only {count} records were written there"));
        }
        let (payload, headers) = topic.record(r.partition, r.offset as usize);
        if r.bytes() != Some(payload.as_slice()) {
            return Err(format!("{at}: not the record written there"));
        }
        let kept = Some(headers.unwrap_or_default());
        match &r.headers {
            Some(_) if r.headers != kept => return Err(format!("{at}: headers {:?}, written {kept:?}", r.headers)),
            Some(_) if !self.ever_kept => return Err(format!("{at}: headers before they were ever asked for")),
            None if self.keep => return Err(format!("{at}: no headers while they are asked for")),
            None if self.ever_kept && kept.as_ref().is_some_and(|h| !h.is_empty()) => {
                self.headers_dropped_then_read = true
            }
            Some(h) if !h.is_empty() => self.headers_read = true,
            _ => {}
        }
        if let Some(stop) = plan.sources[me].stop
            && r.offset >= stop
        {
            return Err(format!("{at}: at or past its stop {stop}"));
        }
        let key = (r.topic.to_string(), r.partition);
        let start = plan.start(me);
        if self.revoked.remove(&key) {
            let mut allowed: Vec<i64> = vec![start];
            allowed.extend(self.committed.get(&key).into_iter().flatten().map(|c| c + 1));
            if !allowed.contains(&r.offset) {
                return Err(format!(
                    "{at}: after a revoke it starts over at its start or a commit, one of {allowed:?}"
                ));
            }
        } else {
            let want = self.next.get(&key).copied().unwrap_or(start);
            if r.offset != want {
                return Err(format!("{at}: the next record there is {want}"));
            }
        }
        self.next.insert(key, r.offset + 1);
        *self.read.entry(r.topic.to_string()).or_default() += 1;
        self.bytes += payload.len();
        Ok(())
    }

    /// Commits the last offset read of each partition it owns.
    fn commit(&mut self, me: usize) -> Result<(), String> {
        let owned: HashSet<(String, i32)> = self.source.assigned_partitions().into_iter().collect();
        let mut offsets = CommitMap::new();
        for (key, next) in &self.next {
            if owned.contains(key) && !self.revoked.contains(key) {
                offsets.insert(TopicPartition::new(Arc::from(key.0.as_str()), key.1), next - 1);
            }
        }
        let answer = self.source.commit(&offsets);
        for (tp, offset) in &offsets {
            self.committed
                .entry((tp.topic.to_string(), tp.partition))
                .or_default()
                .push(*offset);
        }
        answer.map_err(|e| format!("source {me}: commit of {offsets:?} failed: {e}"))
    }
}

// -- one seed ------------------------------------------------------------------

fn execute(plan: &Plan, brokers: &str) -> Result<BTreeSet<&'static str>, String> {
    let broker = Broker::new(brokers);
    let mut written: Vec<Vec<usize>> = plan.topics.iter().map(|t| vec![0; t.counts.len()]).collect();
    let mut exists: Vec<bool> = vec![false; plan.topics.len()];
    let mut deleted: Vec<bool> = vec![false; plan.topics.len()];
    let create = |t: usize, written: &mut Vec<Vec<usize>>| -> Result<(), String> {
        let topic = &plan.topics[t];
        broker.create(topic)?;
        for (p, &count) in topic.counts.iter().enumerate() {
            broker.write(topic, p as i32, 0, count)?;
            written[t][p] = count;
        }
        Ok(())
    };
    for (t, topic) in plan.topics.iter().enumerate() {
        if topic.initial {
            create(t, &mut written)?;
            exists[t] = true;
        }
    }

    let result = (|| {
        let brokers = [brokers.to_string(), format!("{brokers},{brokers}")];
        let groups = [format!("{}-g0", plan.prefix), format!("{}-g1", plan.prefix)];
        let clients = Arc::new(Clients::default());
        let mut readers: Vec<Reader> = Vec::new();
        // Before any source joins, while a commit from outside the group is still taken.
        for spec in plan.sources.iter().filter(|spec| spec.moved) {
            let committer: rdkafka::consumer::BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers[spec.brokers])
                .set("group.id", &groups[spec.group])
                .create()
                .map_err(|e| format!("a committer did not open: {e}"))?;
            let mut ends = rdkafka::TopicPartitionList::new();
            for &t in spec.named.iter().filter(|&&t| exists[t]) {
                for (p, &count) in written[t].iter().enumerate() {
                    let end = rdkafka::Offset::Offset(count as i64);
                    ends.add_partition_offset(&plan.topics[t].name, p as i32, end)
                        .map_err(|e| e.to_string())?;
                    if let Some(mut e) = ends.find_partition(&plan.topics[t].name, p as i32) {
                        e.set_metadata(OLDER_TAG);
                    }
                }
            }
            if ends.count() > 0 {
                rdkafka::consumer::Consumer::commit(&committer, &ends, rdkafka::consumer::CommitMode::Sync)
                    .map_err(|e| format!("committing the older start failed: {e}"))?;
            }
        }
        for (s, spec) in plan.sources.iter().enumerate() {
            let topics = spec
                .named
                .iter()
                .map(|&t| plan.topics[t].name.clone())
                .chain(spec.patterns.iter().map(|&p| plan.patterns[p].clone()))
                .collect();
            let config = KafkaSourceConfig {
                service: KafkaService::Kafka,
                encoding: Encoding::Auto,
                fetch_bytes: spec.fetch_bytes,
                brokers: brokers[spec.brokers].clone(),
                topics,
                group_id: groups[spec.group].clone(),
                identity: None,
                start: match spec.start {
                    Start::Earliest => StartPosition::Earliest,
                    Start::Time(n) => StartPosition::Timestamp(stamp(broker.base, n as usize) - 500),
                },
                stop: spec
                    .stop
                    .map(|n| StopPosition::Timestamp(stamp(broker.base, n as usize) - 500)),
                deployed_at: DEPLOYED_AT,
                member: None,
                rack: None,
                props: Default::default(),
                avro: None,
            };
            let source = KafkaSource::open(&config, &Default::default(), &clients)
                .map_err(|e| format!("source {s} did not open: {e}"))?;
            readers.push(Reader {
                source,
                keep: false,
                ever_kept: false,
                next: HashMap::new(),
                revoked: HashSet::new(),
                committed: HashMap::new(),
                read: BTreeMap::new(),
                bytes: 0,
                headers_dropped_then_read: false,
                headers_read: false,
            });
        }

        let mut reached: BTreeSet<&'static str> = BTreeSet::new();
        for (i, step) in plan.steps.iter().enumerate() {
            let ctx = |e: String| format!("step {i} {step:?}: {e}");
            match *step {
                Step::Poll {
                    source,
                    rows,
                    bytes,
                    wait_ms,
                } => {
                    let got = readers[source]
                        .poll(source, plan, &written, rows, bytes, Duration::from_millis(wait_ms))
                        .map_err(ctx)?;
                    if got > 1 && bytes == 4096 {
                        reached.insert("a batch over its bytes");
                    }
                }
                Step::Headers { source, keep } => {
                    let r = &mut readers[source];
                    r.source.keep_headers(keep);
                    r.keep = keep;
                    r.ever_kept |= keep;
                }
                Step::Block(s) => {
                    readers[s].source.block().map_err(|e| ctx(e.to_string()))?;
                    reached.insert("block");
                }
                Step::Release(s) => {
                    readers[s].source.release().map_err(|e| ctx(e.to_string()))?;
                    reached.insert("release");
                }
                Step::KeepMembership { source, ms } => {
                    readers[source]
                        .source
                        .keep_membership(Duration::from_millis(ms))
                        .map_err(|e| ctx(e.to_string()))?;
                    reached.insert("keep membership");
                }
                Step::Commit(s) => {
                    readers[s].commit(s).map_err(ctx)?;
                    if !readers[s].committed.is_empty() {
                        reached.insert("commit");
                    }
                }
                Step::Write {
                    topic,
                    partition,
                    count,
                } => {
                    let from = written[topic][partition as usize];
                    broker
                        .write(&plan.topics[topic], partition, from, from + count)
                        .map_err(ctx)?;
                    written[topic][partition as usize] = from + count;
                }
                Step::Create(t) => {
                    create(t, &mut written).map_err(ctx)?;
                    exists[t] = true;
                }
                Step::Delete(t) => {
                    broker.delete(&plan.topics[t].name).map_err(ctx)?;
                    deleted[t] = true;
                    reached.insert("a topic deleted");
                }
            }
        }

        for (s, r) in readers.iter_mut().enumerate() {
            r.source
                .release()
                .map_err(|e| format!("source {s}: release at the drain: {e}"))?;
        }
        let live: Vec<usize> = (0..plan.topics.len()).filter(|&t| exists[t] && !deleted[t]).collect();
        let deadline = Instant::now() + DRAIN;
        loop {
            for (s, r) in readers.iter_mut().enumerate() {
                r.poll(s, plan, &written, 1000, usize::MAX, Duration::from_millis(20))
                    .map_err(|e| format!("draining: {e}"))?;
            }
            let unread = unread(plan, &readers, &written, &live);
            if unread.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "after {DRAIN:?} of draining, still unread:\n  {}",
                    unread.join("\n  ")
                ));
            }
        }
        // Anything more is a record read twice or a topic read by a second source.
        let quiet = Instant::now() + Duration::from_secs(2);
        while Instant::now() < quiet {
            for (s, r) in readers.iter_mut().enumerate() {
                r.poll(s, plan, &written, 1000, usize::MAX, Duration::from_millis(50))
                    .map_err(|e| format!("after the drain: {e}"))?;
            }
        }

        settle(plan, &readers, &live, &mut reached)?;
        Ok(reached)
    })();

    for t in 0..plan.topics.len() {
        if exists[t] && !deleted[t] {
            let _ = broker.delete(&plan.topics[t].name);
        }
    }
    result
}

/// The sources of `group` that read topic `t`, and are not refused: the one that names it, or else every one whose
/// pattern matches it.
fn readers_of(plan: &Plan, refused: &[bool], group: usize, t: usize) -> Vec<usize> {
    let all: Vec<usize> = (0..plan.sources.len())
        .filter(|&s| plan.sources[s].group == group && !refused[s] && plan.wants(s, t))
        .collect();
    match all.iter().find(|&&s| plan.names(s, t)) {
        Some(&s) => vec![s],
        None => all,
    }
}

/// Each live topic, in each group, that no source has read through: one of `readers_of` owns every partition and
/// has read each to its end.
fn unread(plan: &Plan, readers: &[Reader], written: &[Vec<usize>], live: &[usize]) -> Vec<String> {
    let refused: Vec<bool> = readers.iter().map(|r| r.refused().is_some()).collect();
    let mut out = Vec::new();
    for group in 0..2 {
        for &t in live {
            let candidates = readers_of(plan, &refused, group, t);
            if candidates.is_empty() {
                continue;
            }
            let name = &plan.topics[t].name;
            let progress: Vec<String> = candidates
                .iter()
                .map(|&s| {
                    let r = &readers[s];
                    let owned: HashSet<(String, i32)> = r.source.assigned_partitions().into_iter().collect();
                    let behind: Vec<String> = written[t]
                        .iter()
                        .enumerate()
                        .filter_map(|(p, &count)| {
                            let key = (name.clone(), p as i32);
                            let end = plan.sources[s].stop.map_or(count as i64, |stop| stop.min(count as i64));
                            let at = r.next.get(&key).copied().unwrap_or(plan.start(s));
                            if !owned.contains(&key) {
                                Some(format!("p{p} unassigned"))
                            } else if at < end {
                                Some(format!("p{p} at {at} of {end}"))
                            } else {
                                None
                            }
                        })
                        .collect();
                    (s, behind)
                })
                .filter(|(_, behind)| !behind.is_empty())
                .map(|(s, behind)| format!("source {s}: {}", behind.join(", ")))
                .collect();
            if progress.len() == candidates.len() {
                out.push(format!("{name} in group {group}: {}", progress.join("; ")));
            }
        }
    }
    out
}

/// What must hold once every source has read through, and what this seed reached.
fn settle(plan: &Plan, readers: &[Reader], live: &[usize], reached: &mut BTreeSet<&'static str>) -> Result<(), String> {
    let refusals: Vec<Option<String>> = readers.iter().map(Reader::refused).collect();
    let refused: Vec<bool> = refusals.iter().map(Option::is_some).collect();
    for (s, why) in refusals.iter().enumerate() {
        let spec = &plan.sources[s];
        let blame = |earlier: usize| {
            !refused[earlier]
                && plan.sources[earlier].group == spec.group
                && spec.named.iter().any(|&t| plan.wants(earlier, t))
        };
        match why {
            Some(why) => {
                reached.insert("a source refused");
                if spec.named.is_empty() {
                    return Err(format!("source {s} names only patterns, and is refused: {why}"));
                }
                if !(0..s).any(blame) {
                    return Err(format!(
                        "source {s} is refused, and no earlier source of its group reads its topics: {why}"
                    ));
                }
                if !spec.named.iter().any(|&t| why.contains(&plan.topics[t].name)) {
                    return Err(format!("source {s}'s refusal names none of its topics: {why}"));
                }
                if !readers[s].read.is_empty() {
                    return Err(format!("source {s} is refused, and read {:?}", readers[s].read));
                }
                if !readers[s].source.assigned_partitions().is_empty() {
                    return Err(format!(
                        "source {s} is refused, and owns {:?}",
                        readers[s].source.assigned_partitions()
                    ));
                }
            }
            None => {
                if let Some(e) = (0..s).find(|&e| {
                    !refused[e] && plan.sources[e].group == spec.group && spec.named.iter().any(|&t| plan.names(e, t))
                }) {
                    return Err(format!(
                        "source {s} names a topic source {e} of its group names first, and is not refused"
                    ));
                }
            }
        }
    }

    for group in 0..2 {
        for t in 0..plan.topics.len() {
            let name = &plan.topics[t].name;
            let read_by: Vec<usize> = (0..readers.len())
                .filter(|&s| plan.sources[s].group == group && readers[s].read.contains_key(name))
                .collect();
            if read_by.len() > 1 {
                return Err(format!("{name} in group {group} was read by sources {read_by:?}"));
            }
            let candidates = readers_of(plan, &refused, group, t);
            if let Some(&s) = read_by.first()
                && !candidates.contains(&s)
            {
                return Err(format!(
                    "{name} in group {group} was read by source {s}, not by the one that names it, {candidates:?}"
                ));
            }
            if !live.contains(&t) {
                continue;
            }
            for &s in &read_by {
                if plan.matches(s, t) && !plan.names(s, t) {
                    reached.insert("a pattern read a topic");
                    if !plan.topics[t].initial {
                        reached.insert("a topic created mid-run read");
                    }
                }
            }
            for (s, &refused) in refused.iter().enumerate() {
                if plan.sources[s].group == group
                    && !refused
                    && plan.matches(s, t)
                    && !plan.names(s, t)
                    && read_by.first().is_some_and(|&r| r != s)
                {
                    reached.insert("a pattern skipped a topic");
                }
            }
        }
    }

    for (s, spec) in plan.sources.iter().enumerate() {
        let r = &readers[s];
        reached.insert(match spec.start {
            Start::Earliest => "start earliest",
            Start::Time(_) => "start time",
        });
        if spec.moved {
            reached.insert("a start moved over an older commit");
        }
        reached.insert(if spec.stop.is_some() { "stop time" } else { "no stop" });
        if spec.stop.is_some_and(|stop| stop <= plan.start(s)) {
            reached.insert("an empty range");
        }
        if r.headers_read {
            reached.insert("headers kept");
        }
        if r.headers_dropped_then_read {
            reached.insert("headers dropped after kept");
        }
        if spec.fetch_bytes == SMALL_FETCH && r.bytes > SMALL_FETCH / 2 {
            reached.insert("an inbox over its cap");
        }
        if r.read
            .keys()
            .any(|name| plan.topics.iter().any(|t| t.name == *name && t.counts.len() > 1))
        {
            reached.insert("several partitions");
        }
        for e in 0..s {
            if plan.sources[e].group == spec.group {
                reached.insert(if plan.client_key(e) == plan.client_key(s) {
                    "a client shared"
                } else {
                    "a group across clients"
                });
            }
            if plan.sources[e].group != spec.group {
                reached.insert("two groups");
            }
        }
    }
    Ok(())
}

// -- Event Hubs ----------------------------------------------------------------

/// Seeds of the Event Hubs fuzz. They run one after another: every seed reads the emulator's one hub in its one
/// consumer group, and two members of that group would split its partitions.
const HUB_SEEDS: std::ops::Range<u64> = 0..8;

/// The emulator's published connection string, the same on every install.
const EVENT_HUBS_EMULATOR: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

/// The emulator's fixed hub and consumer group (`tests/fixtures/event-hubs-emulator.json`).
const HUB: &str = "smeltr-live";
const HUB_GROUP: &str = "smeltr";
const HUB_PARTITIONS: i32 = 2;

/// How long a hub seed waits for the records it sent.
const HUB_DRAIN: Duration = Duration::from_secs(60);

const HUB_FETCHES: &[Option<&str>] = &[None, Some("4 MiB"), Some("8MB"), Some("64MiB")];
const HUB_ENCODINGS: &[&str] = &["auto", "utf-8", "latin1", "utf-16le", "none"];

const HUB_COVERAGE: &[&str] = &[
    "start earliest",
    "start time",
    "a stop inside what was sent",
    "no stop",
    "fetch default",
    "fetch 4 MiB",
    "fetch 8MB",
    "fetch 64MiB",
    "encoding auto",
    "encoding utf-8",
    "encoding latin1",
    "encoding utf-16le",
    "encoding none",
    "headers kept",
    "headers dropped after kept",
    "block",
    "release",
    "keep membership",
    "commit",
];

/// `eventHubs` sources, loaded from a config file, against the Event Hubs emulator. The hub and its consumer group are
/// shared by every run and keep what earlier runs committed, so a start applies only where nothing is committed yet,
/// and each seed holds what it can of the records it sent itself: with a random start, stop, fetch size and encoding,
/// under random polls, header switches, blocks, releases, kept membership, commits and records sent mid-run,
///
/// 1. the source loads with what its file says;
/// 2. every partition reads on from its first record read, each offset once, in order;
/// 3. each record the seed sent before its stop arrives once, as it was sent, at the offset it was counted at, with its
///    headers exactly as `kafka_sources_read_each_record_once_at_its_owner_whatever_the_sequence` holds them, and none
///    at or past its stop.
#[test]
fn event_hubs_sources_read_each_record_this_run_sent_once_whatever_the_sequence() {
    let Some(namespace) =
        live::event_hubs("event_hubs_sources_read_each_record_this_run_sent_once_whatever_the_sequence")
    else {
        return;
    };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut coverage = Coverage::new(HUB_COVERAGE.iter().copied());
    fuzz::run(HUB_SEEDS, |seed| {
        let plan = HubPlan::new(seed, &format!("kfz-{run}-s{seed}"));
        match hub_execute(&plan, &namespace) {
            Ok(reached) => {
                reached.into_iter().for_each(|r| coverage.reach(r));
                Ok(())
            }
            Err(why) => Err(format!("{why}\n{plan:#?}")),
        }
    });
    coverage.assert_complete();
}

#[derive(Debug)]
struct HubPlan {
    /// What names this seed's records, and only them.
    tag: String,
    /// Records sent to each partition before the source opens.
    counts: Vec<usize>,
    payload: usize,
    spread: usize,
    headers: bool,
    start: HubStart,
    /// Where each partition ends: this many records into what the seed sent to the partition given.
    stop: Option<(i32, usize)>,
    fetch: Option<&'static str>,
    encoding: &'static str,
    steps: Vec<HubStep>,
}

#[derive(Clone, Copy, Debug)]
enum HubStart {
    Earliest,
    /// An hour before the seed sends.
    Time,
}

#[derive(Debug)]
enum HubStep {
    Poll { rows: usize, bytes: usize, wait_ms: u64 },
    Headers(bool),
    Block,
    Release,
    KeepMembership(u64),
    Commit,
    Send { partition: i32, count: usize },
}

impl HubPlan {
    fn new(seed: u64, tag: &str) -> HubPlan {
        let mut rng = Rng::seeded(seed);
        let counts: Vec<usize> = (0..HUB_PARTITIONS).map(|_| 1 + rng.below(120)).collect();
        let stop = rng.chance(2).then(|| {
            let p = rng.below(HUB_PARTITIONS as usize);
            (p as i32, rng.below(counts[p] + 1))
        });
        let mut steps = Vec::new();
        for _ in 0..10 + rng.below(20) {
            let step = match rng.below(12) {
                0..=4 => HubStep::Poll {
                    rows: *rng.pick(&[1, 7, 100, 10_000]),
                    bytes: *rng.pick(&[1, 4096, 4096, usize::MAX]),
                    wait_ms: *rng.pick(&[0, 50, 300]),
                },
                5 | 6 => HubStep::Headers(rng.chance(2)),
                7 => HubStep::Block,
                8 => HubStep::Release,
                9 => HubStep::KeepMembership(*rng.pick(&[0, 200])),
                10 => HubStep::Send {
                    partition: rng.below(HUB_PARTITIONS as usize) as i32,
                    count: 1 + rng.below(30),
                },
                _ => HubStep::Commit,
            };
            // A switch of headers shows only in what is read after it, and the hub's records are soon all read.
            let read_after = matches!(step, HubStep::Headers(_)) && rng.chance(2);
            steps.push(step);
            if read_after {
                steps.push(HubStep::Send {
                    partition: rng.below(HUB_PARTITIONS as usize) as i32,
                    count: 5,
                });
                steps.push(HubStep::Poll {
                    rows: 100,
                    bytes: usize::MAX,
                    wait_ms: 300,
                });
            }
        }
        // One seed in four ends by turning headers on and then off, each followed by records sent, so the few
        // default seeds read records with headers both kept and dropped.
        let switches = seed % 4 == 1;
        if switches {
            for keep in [true, false] {
                steps.push(HubStep::Headers(keep));
                steps.push(HubStep::Send {
                    partition: rng.below(HUB_PARTITIONS as usize) as i32,
                    count: 5,
                });
            }
        }
        HubPlan {
            tag: tag.to_string(),
            counts,
            payload: 10,
            spread: 1 + rng.below(500),
            headers: rng.chance(2) || switches,
            // Each its own rotation, so a few seeds reach every one in a different combination.
            start: [HubStart::Earliest, HubStart::Time][seed as usize % 2],
            stop,
            fetch: HUB_FETCHES[seed as usize % HUB_FETCHES.len()],
            encoding: HUB_ENCODINGS[seed as usize % HUB_ENCODINGS.len()],
            steps,
        }
    }

    fn record(&self, p: i32, k: usize) -> (Vec<u8>, Option<Headers>) {
        shaped(&self.tag, self.payload, self.spread, self.headers, p, k)
    }

    /// The partition and index of a record this seed sent, from its payload.
    fn sent(&self, payload: &[u8]) -> Option<(i32, usize)> {
        let rest = payload.strip_prefix(format!("{}/", self.tag).as_bytes())?;
        let text = std::str::from_utf8(rest).ok()?;
        let mut parts = text.split('/');
        Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
    }
}

fn hub_execute(plan: &HubPlan, namespace: &str) -> Result<BTreeSet<&'static str>, String> {
    let mut reached: BTreeSet<&'static str> = BTreeSet::new();
    let sent_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut source_json = serde_json::json!({
        "id": "hub", "type": "eventHubs", "namespace": namespace, "eventHubs": [HUB],
        "consumerGroup": HUB_GROUP, "connectionString": {"secret": "event-hubs", "version": 1},
        "encoding": plan.encoding,
        "start": match plan.start {
            HubStart::Earliest => serde_json::json!({"type": "earliest"}),
            HubStart::Time => serde_json::json!({"type": "timestamp", "millis": sent_at - 3_600_000}),
        },
    });
    if let Some(fetch) = plan.fetch {
        source_json["fetchBytes"] = fetch.into();
    }

    // Sent with the settings the source signs in with, the password written out. The partitions' ends are read
    // first, so each record sent is counted at its offset; the stop is one of them.
    let loaded = load_hub_source(&source_json)?;
    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", &loaded.brokers)
        // One request in flight, so a retry cannot reorder what is sent.
        .set("max.in.flight.requests.per.connection", "1");
    for (k, v) in &loaded.props.literal {
        producer_config.set(k, v);
    }
    producer_config.set("sasl.password", EVENT_HUBS_EMULATOR);
    let producer: BaseProducer = producer_config
        .create()
        .map_err(|e| format!("a producer for the emulator: {e}"))?;
    let mut first: Vec<i64> = Vec::new();
    for p in 0..HUB_PARTITIONS {
        first.push(watermark(&producer, HUB, p)?);
    }
    // The hub stamps each record with when it took it, so the stop is a time taken between sends: each
    // partition's first `k` records go before it, the rest after.
    let cut: Vec<usize> = plan
        .counts
        .iter()
        .map(|&count| plan.stop.map_or(count, |(_, k)| k.min(count)))
        .collect();
    let send = |p: i32, ks: std::ops::Range<usize>| {
        let end = first[p as usize] + ks.end as i64;
        let records = ks.map(|k| {
            let (payload, headers) = plan.record(p, k);
            (payload, headers, None)
        });
        write(&producer, HUB, p, records, end)
    };
    for p in 0..HUB_PARTITIONS {
        send(p, 0..cut[p as usize])?;
    }
    let stop_at = plan.stop.map(|_| {
        std::thread::sleep(Duration::from_millis(1_100));
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        std::thread::sleep(Duration::from_millis(1_100));
        at
    });
    for p in 0..HUB_PARTITIONS {
        send(p, cut[p as usize]..plan.counts[p as usize])?;
    }
    if let Some(millis) = stop_at {
        source_json["stop"] = serde_json::json!({"type": "timestamp", "millis": millis});
    }
    let config = load_hub_source(&source_json)?;
    check_loaded(plan, stop_at, &config, &mut reached)?;
    let stop = plan.stop.map(|_| cut.as_slice());

    let mut secrets = smeltr::plan::topology::HeldSecrets::default();
    secrets.set_stored([("event-hubs".to_string(), 1, EVENT_HUBS_EMULATOR.to_string())]);
    let mut reader = HubReader {
        source: KafkaSource::new(&config, &secrets).map_err(|e| format!("the source did not open: {e}"))?,
        keep: false,
        ever_kept: false,
        next: HashMap::new(),
        seen: HashSet::new(),
        sent: plan.counts.clone(),
        headers_read: false,
        headers_dropped_then_read: false,
    };
    // Joining the group takes the emulator seconds; the steps are for a source that reads.
    let deadline = Instant::now() + HUB_DRAIN;
    while reader.source.assigned_partitions().len() < HUB_PARTITIONS as usize {
        if Instant::now() >= deadline {
            return Err(format!(
                "the source owns no more than {:?} after {HUB_DRAIN:?}",
                reader.source.assigned_partitions()
            ));
        }
        reader
            .source
            .keep_membership(Duration::from_millis(100))
            .map_err(|e| format!("joining: {e}"))?;
    }
    for (i, step) in plan.steps.iter().enumerate() {
        let ctx = |e: String| format!("step {i} {step:?}: {e}");
        match *step {
            HubStep::Poll { rows, bytes, wait_ms } => {
                reader
                    .poll(plan, &first, stop, rows, bytes, Duration::from_millis(wait_ms))
                    .map_err(ctx)?;
            }
            HubStep::Headers(keep) => {
                reader.source.keep_headers(keep);
                reader.keep = keep;
                reader.ever_kept |= keep;
            }
            HubStep::Block => {
                reader.source.block().map_err(|e| ctx(e.to_string()))?;
                reached.insert("block");
            }
            HubStep::Release => {
                reader.source.release().map_err(|e| ctx(e.to_string()))?;
                reached.insert("release");
            }
            HubStep::KeepMembership(ms) => {
                reader
                    .source
                    .keep_membership(Duration::from_millis(ms))
                    .map_err(|e| ctx(e.to_string()))?;
                reached.insert("keep membership");
            }
            HubStep::Commit => {
                if reader.commit().map_err(ctx)? {
                    reached.insert("commit");
                }
            }
            HubStep::Send { partition, count } => {
                let from = reader.sent[partition as usize];
                send(partition, from..from + count).map_err(ctx)?;
                reader.sent[partition as usize] = from + count;
            }
        }
    }

    reader
        .source
        .release()
        .map_err(|e| format!("release at the drain: {e}"))?;
    let wanted: Vec<(i32, usize)> = (0..HUB_PARTITIONS)
        .flat_map(|p| (0..reader.sent[p as usize]).map(move |k| (p, k)))
        .filter(|&(p, k)| stop.is_none_or(|cut| k < cut[p as usize]))
        .collect();
    let deadline = Instant::now() + HUB_DRAIN;
    loop {
        reader.poll(plan, &first, stop, 1000, usize::MAX, Duration::from_millis(100))?;
        if wanted.iter().all(|w| reader.seen.contains(w)) {
            break;
        }
        if Instant::now() >= deadline {
            let missing: Vec<&(i32, usize)> = wanted.iter().filter(|w| !reader.seen.contains(w)).collect();
            return Err(format!(
                "after {HUB_DRAIN:?} of draining, {} of {} records sent before the stop are unread, first {:?}",
                missing.len(),
                wanted.len(),
                &missing[..missing.len().min(5)]
            ));
        }
    }
    // Anything more of this seed's is a record read twice or one past the stop.
    let quiet = Instant::now() + Duration::from_secs(2);
    while Instant::now() < quiet {
        reader.poll(plan, &first, stop, 1000, usize::MAX, Duration::from_millis(100))?;
    }
    // So the next seed starts after what this one read.
    reader.commit()?;
    if reader.headers_read {
        reached.insert("headers kept");
    }
    if reader.headers_dropped_then_read {
        reached.insert("headers dropped after kept");
    }
    Ok(reached)
}

/// The one source a config file holding `source` loads.
fn load_hub_source(source: &serde_json::Value) -> Result<KafkaSourceConfig, String> {
    let json = serde_json::json!({
        "version": 1,
        "sources": [source],
        "destinations": [{"id": "es", "type": "elasticsearch", "url": "https://es.internal:9200", "defaultIndex": "logs"}],
        "pipelines": [{"id": "p", "name": "p", "steps": []}],
        "routes": [{"id": "r", "source": "hub", "pipeline": "p", "deliver": [{"to": ["es"]}]}]
    });
    let system =
        smeltr::config_file::load_str(&json.to_string()).map_err(|e| format!("{source} does not load: {e}"))?;
    match &system.topology.sources[0].kind {
        smeltr::plan::topology::SourceKind::Kafka(config) => Ok(config.clone()),
        _ => Err(format!("{source} does not load as a Kafka source")),
    }
}

/// The loaded source is what its file says.
fn check_loaded(
    plan: &HubPlan,
    stop: Option<i64>,
    config: &KafkaSourceConfig,
    reached: &mut BTreeSet<&'static str>,
) -> Result<(), String> {
    let mismatch = |what: &str, got: String| Err(format!("the source loads with {what} {got}"));
    if config.service != KafkaService::EventHubs {
        return mismatch("service", format!("{:?}", config.service));
    }
    if config.topics != [HUB] || config.group_id != HUB_GROUP {
        return mismatch("topics and group", format!("{:?} {:?}", config.topics, config.group_id));
    }
    if Some(config.encoding) != Encoding::for_label(plan.encoding) {
        return mismatch("encoding", format!("{:?} for {:?}", config.encoding, plan.encoding));
    }
    let fetch = match plan.fetch {
        None => LARGE_FETCH,
        Some("4 MiB") => 4 << 20,
        Some("8MB") => 8_000_000,
        Some(_) => 64 << 20,
    };
    if config.fetch_bytes != fetch {
        return mismatch("fetch bytes", config.fetch_bytes.to_string());
    }
    let start_ok = matches!(
        (plan.start, config.start),
        (HubStart::Earliest, StartPosition::Earliest) | (HubStart::Time, StartPosition::Timestamp(_))
    );
    if !start_ok {
        return mismatch("start", format!("{:?}", config.start));
    }
    if config.stop != stop.map(StopPosition::Timestamp) {
        return mismatch("stop", format!("{:?}", config.stop));
    }
    reached.insert(match plan.start {
        HubStart::Earliest => "start earliest",
        HubStart::Time => "start time",
    });
    reached.insert(match plan.stop {
        Some((p, k)) if k < plan.counts[p as usize] => "a stop inside what was sent",
        Some(_) | None => "no stop",
    });
    reached.insert(match plan.fetch {
        None => "fetch default",
        Some("4 MiB") => "fetch 4 MiB",
        Some("8MB") => "fetch 8MB",
        Some(_) => "fetch 64MiB",
    });
    reached.insert(match plan.encoding {
        "auto" => "encoding auto",
        "utf-8" => "encoding utf-8",
        "latin1" => "encoding latin1",
        "utf-16le" => "encoding utf-16le",
        _ => "encoding none",
    });
    Ok(())
}

/// The hub's source and what it has read.
struct HubReader {
    source: KafkaSource,
    keep: bool,
    ever_kept: bool,
    /// The offset each partition must give next, once one has been read.
    next: HashMap<i32, i64>,
    /// This seed's records read, by partition and index.
    seen: HashSet<(i32, usize)>,
    /// How many records the seed has sent to each partition.
    sent: Vec<usize>,
    headers_read: bool,
    headers_dropped_then_read: bool,
}

impl HubReader {
    fn poll(
        &mut self,
        plan: &HubPlan,
        first: &[i64],
        stop: Option<&[usize]>,
        rows: usize,
        bytes: usize,
        wait: Duration,
    ) -> Result<usize, String> {
        let got = self
            .source
            .poll_batch(rows, bytes, wait)
            .map_err(|e| format!("poll_batch({rows}, {bytes}, {wait:?}) failed: {e}"))?;
        if got.len() > rows {
            return Err(format!("asked for {rows} records, got {}", got.len()));
        }
        for r in &got {
            self.check(plan, first, stop, r)?;
        }
        let revoked = self.source.take_revoked();
        if !revoked.is_empty() {
            return Err(format!(
                "the only member of its group had {revoked:?} revoked: another reader shares the group"
            ));
        }
        Ok(got.len())
    }

    /// `stop` is how many records each partition holds before the stop, when there is one.
    fn check(&mut self, plan: &HubPlan, first: &[i64], stop: Option<&[usize]>, r: &SourceRecord) -> Result<(), String> {
        let at = format!("read {}/{}@{}", r.topic, r.partition, r.offset);
        if *r.topic != *HUB {
            return Err(format!("{at}: not the hub"));
        }
        if let Some(cut) = stop
            && r.offset - first[r.partition as usize] >= cut[r.partition as usize] as i64
        {
            return Err(format!(
                "{at}: at or past its stop, after the {}th record sent",
                cut[r.partition as usize]
            ));
        }
        if let Some(&want) = self.next.get(&r.partition)
            && r.offset != want
        {
            return Err(format!("{at}: the next record there is {want}"));
        }
        self.next.insert(r.partition, r.offset + 1);
        let payload = r.bytes().unwrap_or_default();
        let Some((p, k)) = plan.sent(payload) else {
            return Ok(());
        };
        let at = format!("{at}, sent as {p}/{k}");
        if p != r.partition || k >= self.sent[p as usize] || r.offset != first[p as usize] + k as i64 {
            return Err(format!("{at}: not where it was sent"));
        }
        if !self.seen.insert((p, k)) {
            return Err(format!("{at}: read twice"));
        }
        let (want, headers) = plan.record(p, k);
        if payload != want.as_slice() {
            return Err(format!("{at}: not the record sent"));
        }
        let kept = Some(headers.unwrap_or_default());
        match &r.headers {
            Some(_) if r.headers != kept => return Err(format!("{at}: headers {:?}, sent {kept:?}", r.headers)),
            Some(_) if !self.ever_kept => return Err(format!("{at}: headers before they were ever asked for")),
            None if self.keep => return Err(format!("{at}: no headers while they are asked for")),
            None if self.ever_kept && kept.as_ref().is_some_and(|h| !h.is_empty()) => {
                self.headers_dropped_then_read = true
            }
            Some(h) if !h.is_empty() => self.headers_read = true,
            _ => {}
        }
        Ok(())
    }

    /// Commits the last offset read of each partition it owns. Answers whether there was anything to commit.
    fn commit(&mut self) -> Result<bool, String> {
        let owned: HashSet<(String, i32)> = self.source.assigned_partitions().into_iter().collect();
        let mut offsets = CommitMap::new();
        for (&p, next) in &self.next {
            if owned.contains(&(HUB.to_string(), p)) {
                offsets.insert(TopicPartition::new(Arc::from(HUB), p), next - 1);
            }
        }
        self.source
            .commit(&offsets)
            .map_err(|e| format!("commit of {offsets:?} failed: {e}"))?;
        Ok(!offsets.is_empty())
    }
}
