//! Whether what a deployment may hold in memory fits what its worker may use.
//!
//! Each part of a deployment has a ceiling of its own: a source's fetch queue, a destination's buffer, the columnar
//! budget the transform threads share. A deployment whose ceilings add up to more than the worker's memory limit can
//! be killed for memory under load. It is warned of, never refused: the ceilings are worst cases that rarely all fill
//! at once. The warning is logged at deploy time and again every [`crate::logs::RELOG_EVERY`] while it holds,
//! because the leader's warning for the site fades a while after the last log; a deploy that fits logs nothing, and
//! its warning fades.
//!
//! A worker that knows its memory sizes each deployment to it ([`fit`]): what the bundle fixes is set aside, and the
//! columnar budget, the fetch queues the bundle leaves out and the pool its destination queues borrow from share the
//! rest.

use std::cell::Cell;
use std::time::Instant;

use crate::config_file::{BufferBytes, FetchBytes};
use crate::core::id::{DestId, SourceId};
use crate::plan::topology::{
    DestinationConfig, DestinationKind, KafkaSourceConfig, SourceConfig, SourceKind, StartPosition, TopologyConfig,
};

/// One thing a deployment holds memory for, and the most it may hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Part {
    /// What the warning calls it, naming its owner.
    pub name: String,
    /// What it is, the same for every owner: the `part` label of `smeltr_memory_part_bytes`.
    pub part: &'static str,
    pub owner: Owner,
    pub bytes: u64,
}

/// Whose a part is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Owner {
    /// The worker's, shared by everything it runs.
    Worker,
    Source(SourceId),
    Destination(DestId),
}

impl Part {
    /// A part the worker holds whatever its sources and destinations, called `name` in the warning.
    pub(super) fn worker(name: impl Into<String>, part: &'static str, bytes: usize) -> Part {
        Part {
            name: name.into(),
            part,
            owner: Owner::Worker,
            bytes: bytes as u64,
        }
    }

    pub(super) fn source(source: &SourceConfig, part: &'static str, bytes: usize) -> Part {
        Part {
            name: format!("source {:?} {part}", source.name),
            part,
            owner: Owner::Source(source.id),
            bytes: bytes as u64,
        }
    }

    pub(super) fn destination(destination: &DestinationConfig, part: &'static str, bytes: usize) -> Part {
        Part {
            name: format!("destination {:?} {part}", destination.name),
            part,
            owner: Owner::Destination(destination.id),
            bytes: bytes as u64,
        }
    }
}

/// What `sources` hold beyond their lanes, at most: each source's own part, and each Kafka client once, however many
/// of the sources share it (`io::kafka::Clients`).
pub(super) fn source_parts<'a>(sources: impl IntoIterator<Item = &'a SourceConfig>) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut clients: Vec<&KafkaSourceConfig> = Vec::new();
    for source in sources {
        parts.extend(source_part(source));
        if let SourceKind::Kafka(k) = &source.kind
            && !clients.iter().any(|c| one_client(c, k))
        {
            clients.push(k);
            parts.push(Part::source(
                source,
                "kafka client",
                kafka_fetch_ceiling(k.fetch_bytes, &k.brokers),
            ));
        }
    }
    parts
}

/// Whether two Kafka sources read through one client: the same brokers, group, identity, properties, `fetchBytes`
/// and fallback for a partition with no committed offset (`io::kafka::Clients`).
fn one_client(a: &KafkaSourceConfig, b: &KafkaSourceConfig) -> bool {
    let latest = |k: &KafkaSourceConfig| matches!(k.start, StartPosition::Latest);
    a.brokers == b.brokers
        && a.group_id == b.group_id
        && a.identity == b.identity
        && a.props == b.props
        && a.fetch_bytes == b.fetch_bytes
        && latest(a) == latest(b)
}

/// What `source` holds beyond its lane, at most, and beyond any client it shares. `None` for a kind that sets no bound
/// of its own: what it reads goes straight to its lane.
fn source_part(source: &SourceConfig) -> Option<Part> {
    match &source.kind {
        // Its inbox, which pauses its partitions once it passes half a fetch, so the record that crosses it, as large
        // as half a fetch, can take it to a whole one.
        SourceKind::Kafka(k) => Some(Part::source(source, "inbox", k.fetch_bytes)),
        SourceKind::Receiver(_) => Some(Part::source(source, "requests", crate::io::receiver::INBOX_BYTES)),
        SourceKind::Objects(_) => Some(Part::source(
            source,
            "record",
            crate::io::object_source::MAX_RECORD_BYTES as usize,
        )),
        // The whole answer is held while it is cut into records, and a record as large as the answer is copied once
        // more as it is handed out.
        SourceKind::Api(_) => Some(Part::source(
            source,
            "answer",
            2 * crate::io::api::MAX_ANSWER_BYTES as usize,
        )),
        SourceKind::Sql(_) => Some(Part::source(source, "result", crate::io::sql_source::MAX_RESULT_BYTES)),
        SourceKind::Metrics(_) | SourceKind::Logs(_) => None,
    }
}

/// A Kafka client's queue, plus the fetch already asked of each broker, which still lands once the queue is full
/// (`io::kafka`). The bootstrap list is the brokers it names; a cluster with more can hold more.
fn kafka_fetch_ceiling(fetch_bytes: usize, brokers: &str) -> usize {
    let named = brokers.split(',').filter(|b| !b.trim().is_empty()).count().max(1);
    fetch_bytes + fetch_bytes / 2 * named
}

/// What a receiver keeps for its turn whatever the others do (`io::receiver::reserve`). `None`
/// for a source that is no receiver.
pub(super) fn receiver_reserve(kind: &SourceKind) -> Option<usize> {
    match kind {
        SourceKind::Receiver(r) => Some(crate::io::receiver::reserve(r)),
        _ => None,
    }
}

/// What one sender of an HTTP-like destination holds whatever it sends: its thread, its connection, and what the
/// allocator keeps of the requests it sent. Measured under load at about 6 MiB.
const SENDER_BYTES: usize = 6 * 1024 * 1024;

/// What the senders of a destination hold: one per request it may have outstanding. 0 for a destination with none.
pub(super) fn senders_bytes(kind: &DestinationKind) -> usize {
    senders(kind).map_or(0, |s| s.most) * SENDER_BYTES
}

/// The senders of a destination that sends requests: the most it may have outstanding, the count it starts with, and
/// the bytes of one request. `None` for a destination with none.
struct Senders {
    most: usize,
    start: usize,
    request_bytes: usize,
}

fn senders(kind: &DestinationKind) -> Option<Senders> {
    let (most, start, request_bytes) = match kind {
        DestinationKind::Elasticsearch(e) => (e.max_in_flight(), e.in_flight, e.max_bulk_bytes),
        DestinationKind::SplunkHec(s) => (s.max_in_flight(), s.in_flight, s.max_request_bytes),
        DestinationKind::Http(h) => (h.max_in_flight(), h.in_flight, h.max_request_bytes),
        DestinationKind::LogAnalytics(s) => (s.http.max_in_flight(), s.http.in_flight, s.http.max_request_bytes),
        // Prometheus sends from the destination's own thread, one request at a time.
        DestinationKind::Kafka(_)
        | DestinationKind::Prometheus(_)
        | DestinationKind::Objects(_)
        | DestinationKind::Iceberg(_)
        | DestinationKind::Delta(_) => return None,
    };
    Some(Senders {
        most,
        start: start.clamp(1, most),
        request_bytes,
    })
}

/// Cuts each of `kinds`' senders toward the count it starts with, in proportion, until together they hold no more than
/// `bytes`, and never below the least buffer a bundle may state. A destination's senders are as many as its
/// `bufferBytes` holds requests, so that is what is cut.
fn cut_senders<'a>(kinds: impl Iterator<Item = &'a mut DestinationKind>, bytes: usize) {
    let mut kinds: Vec<&mut DestinationKind> = kinds.collect();
    let wanted: usize = kinds.iter().map(|k| senders_bytes(k)).sum();
    if wanted <= bytes {
        return;
    }
    for kind in kinds.iter_mut() {
        let Some(s) = senders(kind) else { continue };
        let cut = (s.most as u128 * bytes as u128 / wanted as u128) as usize;
        // A cut destination runs a configuration its bundle could state.
        kind.set_buffer_bytes((cut.max(s.start) * s.request_bytes).max(BufferBytes::MIN));
    }
}

/// What `destination` holds, at most: its buffer, its senders, and for a file destination (objects, a table) the
/// files it has open.
pub(super) fn destination_parts(destination: &DestinationConfig) -> Vec<Part> {
    let buffer = destination.kind.buffer_bytes();
    let mut parts = vec![Part::destination(destination, "buffer", buffer)];
    let senders = senders_bytes(&destination.kind);
    if senders > 0 {
        parts.push(Part::destination(destination, "senders", senders));
    }
    let in_flight = match &destination.kind {
        DestinationKind::Objects(c) => Some(c.in_flight),
        DestinationKind::Iceberg(c) => Some(c.in_flight),
        DestinationKind::Delta(c) => Some(c.in_flight),
        _ => None,
    };
    if let Some(in_flight) = in_flight {
        // An open file and its upload hold rows its buffer holds until they commit, so all of them hold no more than
        // the buffer.
        parts.push(Part::destination(
            destination,
            "open files",
            (crate::io::files::MAX_OPEN_FILES * crate::io::files::open_file_bytes(in_flight)).min(buffer),
        ));
    }
    parts
}

/// What a worker process holds whatever it runs: its code, its allocator's slack and its threads' stacks.
pub(super) const BASELINE_BYTES: usize = 150 * 1024 * 1024;

/// The least columnar budget a worker sized to its memory gives its transform threads, however little is left.
const LEAST_COLUMNAR_BYTES: usize = 64 * 1024 * 1024;

/// The most columnar budget a worker sized to its memory gives each transform thread. Past it a thread's batches no
/// longer wait on the budget, so more memory serves the queues.
const COLUMNAR_BYTES_PER_THREAD: usize = 128 * 1024 * 1024;

/// What [`fit`] gives a deployment to run under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Fit {
    /// The columnar budget.
    pub columnar: usize,
    /// What the destination queues the bundle leaves unset may borrow above their reserves, together
    /// (`egress::QueuePool`). 0 when there are none.
    pub pool: usize,
    /// The most raw bytes one batch holds. A lane holds its batches, and a destination queue left unset reserves one.
    pub batch_bytes: usize,
    /// What the receivers may hold above their reserves, together (`io::receiver::Holdings`). 0 when there are none.
    pub receivers: usize,
}

/// The least a batch may be cut to when memory is short: below it the work per batch outweighs the batch.
const LEAST_BATCH_BYTES: usize = 512 * 1024;

/// How many times its default a destination the bundle leaves unset may grow to, when other queues leave it room.
/// Past that a busier destination gains little: its senders are all busy and its files are large.
const CEILING_DEFAULTS: usize = 4;

/// Sizes `topology` to `memory`, the bytes its worker may use, and gives what to run it under.
///
/// The parts held whatever the traffic are set aside first:
/// - the worker's two processes, and the lanes (`lane_batches` batches);
/// - the fetch queue and buffer of every source and destination that states one;
/// - the reserve of each destination queue the bundle leaves out (`TopologyConfig::fitted`), one batch, and every
///   destination's senders;
/// - each receiver's reserve, one request of the largest it reads.
///
/// Those grow with the count of sources and destinations, so two of them are cut when memory is short. A batch is
/// 4 MiB, cut down to 512 KiB when the lanes and reserves would otherwise take more than half the memory. Senders are
/// cut toward the count each destination starts with when together they would hold more than a quarter of it.
///
/// The rest is shared: the columnar budget, the fetch queues the bundle leaves out, the pool those destination queues
/// borrow from above their reserves, and the pool the receivers borrow from above theirs. The columnar budget takes
/// half, unless `stated_columnar` fixes it, and below 128 MiB for each of its `threads` transform threads. A queue's
/// share follows its default, so an object store's buffer, which sets how large its files grow, weighs four times
/// another's; a receiver weighs as one default queue. A claim held at a bound gives the rest what its share would have
/// exceeded.
///
/// The queue of a destination left out may grow to its ceiling ([`queue_budget`]) while the pool has room, so a busy
/// one takes what idle ones leave, and a stalled one holds no more than its ceiling. A receiver grows the same way, to
/// `io::receiver::INBOX_BYTES`. Only `delivered` destinations are sized: the rest are never built.
///
/// Lookup tables are left out: they are mapped files, whose pages the kernel drops under pressure.
pub(super) fn fit(
    topology: &mut TopologyConfig,
    delivered: &[DestId],
    lane_batches: usize,
    memory: usize,
    threads: usize,
    stated_columnar: Option<usize>,
) -> Fit {
    let sources: Vec<usize> = (0..topology.sources.len())
        .filter(|&i| topology.fitted.sources.contains(&topology.sources[i].id))
        .collect();
    let destinations: Vec<usize> = (0..topology.destinations.len())
        .filter(|&i| {
            let d = &topology.destinations[i];
            delivered.contains(&d.id) && topology.fitted.destinations.contains(&d.id)
        })
        .collect();
    // The senders are held whatever the traffic, as the lanes and reserves are, so they are cut the same way: to a
    // quarter of the memory, down to the count each starts with.
    cut_senders(
        topology
            .destinations
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| destinations.contains(i))
            .map(|(_, d)| &mut d.kind),
        memory.saturating_sub(2 * BASELINE_BYTES) / 4,
    );
    let pooled = || destinations.iter().map(|&i| &topology.destinations[i].kind);
    let batches = lane_batches + pooled().map(queue_charge).sum::<usize>();
    let batch_bytes = (memory.saturating_sub(2 * BASELINE_BYTES) / 2 / batches.max(1))
        .clamp(LEAST_BATCH_BYTES, crate::worker::TICK_MAX_BYTES)
        / LEAST_BATCH_BYTES
        * LEAST_BATCH_BYTES;
    let reserves: Vec<usize> = topology
        .sources
        .iter()
        .filter_map(|s| receiver_reserve(&s.kind))
        .collect();
    // A worker is two processes, its supervisor and the runtime running the deployment.
    let fixed = 2 * BASELINE_BYTES as u64
        + source_parts(
            topology
                .sources
                .iter()
                .filter(|s| !topology.fitted.sources.contains(&s.id) && receiver_reserve(&s.kind).is_none()),
        )
        .iter()
        .map(|p| p.bytes)
        .sum::<u64>()
        + reserves.iter().sum::<usize>() as u64
        + (lane_batches * batch_bytes) as u64
        + topology
            .destinations
            .iter()
            .filter(|d| delivered.contains(&d.id) && !topology.fitted.destinations.contains(&d.id))
            .flat_map(destination_parts)
            .map(|p| p.bytes)
            .sum::<u64>()
        + pooled()
            .map(|k| (batch_bytes * queue_charge(k) + senders_bytes(k)) as u64)
            .sum::<u64>();
    let left = (memory as u64).saturating_sub(fixed) as usize;

    let mut claims: Vec<Claim> = fetch_claims(sources.iter().map(|&i| &topology.sources[i].kind));
    // The pool weighs what its queues would together: each its weight, as often as its buffer is held.
    let buffers: u64 = pooled().map(|k| buffer_weight(k) * buffer_halves(k)).sum();
    if buffers > 0 {
        claims.push(Claim {
            weight: buffers / 2,
            halves: 2,
            least: 0,
            most: pooled()
                .map(|k| queue_budget(k).saturating_sub(batch_bytes) * queue_charge(k))
                .sum(),
        });
    }
    // The receivers weigh as many default queues: each receiver holds about as much as one.
    if !reserves.is_empty() {
        claims.push(Claim {
            weight: 2 * reserves.len() as u64,
            halves: 2,
            least: 0,
            most: reserves.len() * crate::io::receiver::INBOX_BYTES,
        });
    }
    let left = match stated_columnar {
        Some(stated) => left.saturating_sub(stated),
        None => {
            // The columnar budget weighs as much as the queues together, as often as they are held: half of what is
            // left between them, or all of it when there are none.
            let queues: u64 = claims.iter().map(|c| c.weight * c.halves).sum();
            claims.push(Claim {
                weight: (queues / 2).max(1),
                halves: 2,
                least: LEAST_COLUMNAR_BYTES,
                most: (threads * COLUMNAR_BYTES_PER_THREAD).max(LEAST_COLUMNAR_BYTES),
            });
            left
        }
    };
    let mut sizes = share(&claims, left).into_iter();
    for &i in &sources {
        topology.sources[i]
            .kind
            .set_fetch_bytes(sizes.next().expect("one size per claim"));
    }
    let pool = if buffers > 0 {
        sizes.next().expect("the pool's size")
    } else {
        0
    };
    let receivers = if reserves.is_empty() {
        0
    } else {
        sizes.next().expect("the receivers' size")
    };
    Fit {
        columnar: stated_columnar.unwrap_or_else(|| sizes.next().expect("the columnar budget's size")),
        pool,
        batch_bytes,
        receivers,
    }
}

/// What the egress queue of a destination the bundle leaves unset may hold, borrowing: its queue at its ceiling. Its
/// `bufferBytes` stays at its default, since that also sets its senders, and each sender holds memory of its own
/// however little is queued.
pub(super) fn queue_budget(kind: &DestinationKind) -> usize {
    let mut ceiling = kind.clone();
    ceiling.set_buffer_bytes(queue_ceiling(kind));
    ceiling.queue_bytes()
}

/// The most a destination the bundle leaves unset may hold, as its `bufferBytes`: [`CEILING_DEFAULTS`] times its
/// default, within what it may be set to and above the requests it starts with outstanding.
fn queue_ceiling(kind: &DestinationKind) -> usize {
    let (default, most) = match kind {
        DestinationKind::Objects(_) | DestinationKind::Iceberg(_) | DestinationKind::Delta(_) => {
            (BufferBytes::OBJECT_DEFAULT, BufferBytes::OBJECT_MAX)
        }
        _ => (BufferBytes::DEFAULT, BufferBytes::MAX),
    };
    (CEILING_DEFAULTS * default).min(most).max(kind.least_buffer_bytes())
}

/// The bytes of memory each byte in a destination's egress queue stands for: twice for Kafka, whose producer holds a
/// copy, and for an object store or table, whose open files encode what its buffer holds.
pub(super) fn queue_charge(kind: &DestinationKind) -> usize {
    match kind {
        DestinationKind::Kafka(_)
        | DestinationKind::Objects(_)
        | DestinationKind::Iceberg(_)
        | DestinationKind::Delta(_) => 2,
        _ => 1,
    }
}

/// Each Kafka source's claim on its `fetchBytes`. A source holds all of it in its inbox; the first source of each
/// client holds that client too, its queue once more and half of it again for each broker it names. Equal weights
/// get equal sizes, so the sources of one client are sized alike and still share it.
fn fetch_claims<'a>(sources: impl IntoIterator<Item = &'a SourceKind>) -> Vec<Claim> {
    let mut clients: Vec<&KafkaSourceConfig> = Vec::new();
    sources
        .into_iter()
        .map(|kind| {
            let client = match kind {
                SourceKind::Kafka(k) if !clients.iter().any(|c| one_client(c, k)) => {
                    clients.push(k);
                    2 + k.brokers.split(',').filter(|b| !b.trim().is_empty()).count().max(1)
                }
                _ => 0,
            };
            Claim {
                weight: 2,
                halves: 2 + client as u64,
                least: FetchBytes::MIN,
                most: FetchBytes::MAX,
            }
        })
        .collect()
}

/// How many halves of its `bufferBytes` a destination holds: an object store or table holds it twice, once more for
/// the open files encoding it.
fn buffer_halves(kind: &DestinationKind) -> u64 {
    match kind {
        DestinationKind::Objects(_) | DestinationKind::Iceberg(_) | DestinationKind::Delta(_) => 4,
        _ => 2,
    }
}

/// A destination's weight in sharing memory: its default, against a default buffer's 2.
fn buffer_weight(kind: &DestinationKind) -> u64 {
    match kind {
        DestinationKind::Objects(_) | DestinationKind::Iceberg(_) | DestinationKind::Delta(_) => {
            2 * (BufferBytes::OBJECT_DEFAULT / BufferBytes::DEFAULT) as u64
        }
        _ => 2,
    }
}

/// One part's claim on what the fixed parts leave: its share, how many halves of its size it holds, and the least and
/// most it may be. A queue at its default weighs 2, so the columnar budget can weigh half of all the queues exactly.
struct Claim {
    weight: u64,
    halves: u64,
    least: usize,
    most: usize,
}

/// Each claim's size, in proportion to its weight within its bounds, the whole held within `bytes` wherever every
/// least fits. A claim held at a bound gives what its share would have exceeded back to the rest.
fn share(claims: &[Claim], bytes: usize) -> Vec<usize> {
    let mut sizes: Vec<Option<usize>> = vec![None; claims.len()];
    loop {
        let settled: u64 = claims
            .iter()
            .zip(&sizes)
            .filter_map(|(c, s)| s.map(|s| c.halves * s as u64 / 2))
            .sum();
        let pool = (bytes as u64).saturating_sub(settled);
        let shares: u64 = claims
            .iter()
            .zip(&sizes)
            .filter(|(_, s)| s.is_none())
            .map(|(c, _)| c.weight * c.halves)
            .sum();
        let size = |c: &Claim| (pool.saturating_mul(2 * c.weight) / shares.max(1)).min(usize::MAX as u64) as usize;
        // A claim below its least is settled first: raising it takes from the others. One above its most is settled
        // once none is below, as lowering it gives to them.
        let below: Vec<usize> = (0..claims.len())
            .filter(|&i| sizes[i].is_none() && size(&claims[i]) < claims[i].least)
            .collect();
        let bound: Vec<usize> = if below.is_empty() {
            (0..claims.len())
                .filter(|&i| sizes[i].is_none() && size(&claims[i]) > claims[i].most)
                .collect()
        } else {
            below
        };
        if bound.is_empty() {
            return claims
                .iter()
                .zip(sizes)
                .map(|(c, s)| s.unwrap_or_else(|| size(c)))
                .collect();
        }
        for i in bound {
            sizes[i] = Some(size(&claims[i]).clamp(claims[i].least, claims[i].most));
        }
    }
}

/// The parts' sum, when it is more than `limit`.
pub(super) fn overcommitted(parts: &[Part], limit: u64) -> Option<u64> {
    let sum = parts.iter().map(|p| p.bytes).sum::<u64>();
    (sum > limit).then_some(sum)
}

/// Each part and its share, largest first, for the log.
fn describe(parts: &[Part]) -> String {
    let mut sorted: Vec<&Part> = parts.iter().collect();
    sorted.sort_by_key(|p| std::cmp::Reverse(p.bytes));
    sorted
        .iter()
        .map(|p| format!("{} {}", p.name, mib(p.bytes)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn mib(bytes: u64) -> String {
    format!("{:.0} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Logs the warning when `parts` add up to more than `limit`. Whether it did.
fn warn_if_overcommitted(parts: &[Part], limit: Option<u64>) -> bool {
    // A limit that cannot be read says nothing about fitting.
    let Some(limit) = limit else {
        return false;
    };
    let Some(sum) = overcommitted(parts, limit) else {
        return false;
    };
    let described = describe(parts);
    tracing::warn!(
        target: "worker.memory_overcommitted",
        parts = %described,
        sum_bytes = sum,
        limit_bytes = limit,
        "what this deployment may hold in memory adds up to {} against a limit of {}: {}",
        mib(sum),
        mib(limit),
        described,
    );
    true
}

/// A deployment whose parts do not fit, and when that was last logged.
pub(super) struct Overcommit {
    parts: Vec<Part>,
    logged: Cell<Instant>,
}

/// Logs the warning now when `parts` do not fit `limit`, and gives what to log it again from. `None` when they fit:
/// a deployment's parts do not grow, so one that fits at deploy time is not checked again.
pub(super) fn check(parts: Vec<Part>, limit: Option<u64>) -> Option<Overcommit> {
    warn_if_overcommitted(&parts, limit).then(|| Overcommit {
        parts,
        logged: Cell::new(Instant::now()),
    })
}

impl Overcommit {
    /// Logs the warning again once its last log is [`crate::logs::RELOG_EVERY`] old, while `limit` still says the
    /// parts do not fit. `limit` is read each time, so a limit raised under a running deployment ends the warning.
    pub(super) fn relog_if_due(&self, limit: impl FnOnce() -> Option<u64>) {
        if self.logged.get().elapsed() >= crate::logs::RELOG_EVERY {
            warn_if_overcommitted(&self.parts, limit());
            self.logged.set(Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    /// One Kafka source on one broker that states its 64 MiB `fetchBytes`; an Elasticsearch and an S3 destination that
    /// leave `bufferBytes` out, and a Kafka one that states 8 MiB.
    fn bundle() -> TopologyConfig {
        crate::config_file::load_str(
            r#"{
              "version": 1,
              "sources": [
                { "id": "in", "type": "kafka", "brokers": "b1:9092", "topics": ["in"], "groupId": "g", "fetchBytes": 67108864 }
              ],
              "destinations": [
                { "id": "es", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "i" },
                { "id": "lake", "type": "s3", "bucket": "b", "format": "parquet" },
                { "id": "out", "type": "kafka", "brokers": "b1:9092", "defaultTopic": "t", "bufferBytes": 8388608 }
              ]
            }"#,
        )
        .expect("the bundle loads")
        .topology
    }

    fn buffer(topology: &TopologyConfig, name: &str) -> usize {
        let d = topology.destinations.iter().find(|d| d.name == name).expect("named");
        d.kind.buffer_bytes()
    }

    fn every(topology: &TopologyConfig) -> Vec<DestId> {
        topology.destinations.iter().map(|d| d.id).collect()
    }

    /// What the bundle fixes: the process, the source's client and inbox, and the stated buffer.
    const FIXED: usize = 2 * BASELINE_BYTES + 64 * MIB + 32 * MIB + 64 * MIB + 8 * MIB;

    #[test]
    fn open_files_hold_their_takes_and_uploads_up_to_their_buffer() {
        let open_files = |extra: &str| {
            let topology = crate::config_file::load_str(&format!(
                r#"{{ "version": 1, "destinations": [
                     {{ "id": "lake", "type": "s3", "bucket": "b", "format": "parquet"{extra} }} ] }}"#
            ))
            .expect("the bundle loads")
            .topology;
            let parts = destination_parts(&topology.destinations[0]);
            parts.iter().find(|p| p.part == "open files").expect("open files").bytes
        };
        assert_eq!(open_files(""), BufferBytes::OBJECT_DEFAULT as u64);
        // Each of 64 files: a 32 MiB take, the part its upload fills and the 2 on their way.
        let large = r#", "bufferBytes": 8589934592, "inFlight": 2"#;
        assert_eq!(open_files(large), 64 * (32 + 3 * 8) * MIB as u64);
    }

    #[test]
    fn only_the_buffers_a_bundle_leaves_out_are_fitted() {
        let topology = bundle();
        let fitted: Vec<&str> = topology
            .destinations
            .iter()
            .filter(|d| topology.fitted.destinations.contains(&d.id))
            .map(|d| d.name.as_str())
            .collect();
        assert_eq!(fitted, ["es", "lake"]);
    }

    /// The reserves of the buffers the bundle leaves out: Elasticsearch's 4 MiB, and the object store's held twice.
    const RESERVES: usize = 4 * MIB + 2 * 4 * MIB + SENDERS;

    /// Elasticsearch's senders: one per 5 MiB bulk request its default 64 MiB buffer holds.
    const SENDERS: usize = 12 * SENDER_BYTES;

    /// What the pool may lend, at most: each buffer left out from its reserve to its ceiling, as often as it is held.
    const POOL_MOST: usize = (4 * BufferBytes::DEFAULT - 4 * MIB) + 2 * (4 * BufferBytes::OBJECT_DEFAULT - 4 * MIB);

    #[test]
    fn the_columnar_budget_and_the_pool_share_what_the_bundle_leaves_half_each() {
        let mut topology = bundle();
        let delivered = every(&topology);
        let left = 4096 * MIB - FIXED - RESERVES;
        let fitted = fit(&mut topology, &delivered, 0, 4096 * MIB, 32, None);
        assert_eq!(fitted.columnar, left / 2);
        assert_eq!(fitted.pool, left / 2);
        assert_eq!(
            buffer(&topology, "es"),
            BufferBytes::DEFAULT,
            "a buffer left out keeps its default, and so its senders"
        );
        let es = topology.destinations.iter().find(|d| d.name == "es").expect("named");
        assert_eq!(
            queue_budget(&es.kind),
            4 * BufferBytes::DEFAULT,
            "its queue grows to its ceiling"
        );
        assert_eq!(buffer(&topology, "out"), 8 * MIB, "a stated buffer is kept");
    }

    #[test]
    fn the_columnar_budget_stops_at_128_mib_a_thread_and_the_pool_at_what_its_queues_hold() {
        let mut topology = bundle();
        let delivered = every(&topology);
        let fitted = fit(&mut topology, &delivered, 0, 4096 * MIB, 2, None);
        assert_eq!(fitted.columnar, 256 * MIB);
        assert_eq!(fitted.pool, POOL_MOST);
    }

    #[test]
    fn a_stated_columnar_budget_leaves_the_rest_to_the_pool() {
        let mut topology = bundle();
        let delivered = every(&topology);
        let fitted = fit(&mut topology, &delivered, 0, 1024 * MIB, 8, Some(256 * MIB));
        assert_eq!(fitted.columnar, 256 * MIB);
        assert_eq!(fitted.pool, 1024 * MIB - FIXED - RESERVES - 256 * MIB);
    }

    #[test]
    fn an_unset_fetch_queue_takes_a_share_held_in_its_inbox_its_client_and_halves_per_broker() {
        let mut topology = crate::config_file::load_str(
            r#"{
              "version": 1,
              "sources": [
                { "id": "in", "type": "kafka", "brokers": "b1:9092,b2:9092", "topics": ["in"], "groupId": "g" }
              ],
              "destinations": [
                { "id": "es", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "i" }
              ]
            }"#,
        )
        .expect("the bundle loads")
        .topology;
        let delivered = every(&topology);
        let left = 1024 * MIB - 2 * BASELINE_BYTES - 4 * MIB - SENDERS;
        let fitted = fit(&mut topology, &delivered, 0, 1024 * MIB, 8, None);
        let SourceKind::Kafka(k) = &topology.sources[0].kind else {
            panic!("a Kafka source")
        };
        // `fetchBytes` is held three times over: once in the source's inbox, once in its client's queue, and half again
        // per broker; the pool once; the columnar budget as much as both together.
        assert_eq!(fitted.columnar, left / 2);
        assert_eq!(k.fetch_bytes, left / 8);
        assert_eq!(fitted.pool, left / 8);
    }

    /// Kafka sources that share a client hold it once, each with an inbox of its own; one in another group holds a
    /// client of its own. Sized to memory, the sources of one client are sized alike, so they still share it.
    #[test]
    fn sources_sharing_a_kafka_client_hold_it_once() {
        let mut topology = crate::config_file::load_str(
            r#"{
              "version": 1,
              "sources": [
                { "id": "a", "type": "kafka", "brokers": "b1:9092", "topics": ["a"], "groupId": "g" },
                { "id": "b", "type": "kafka", "brokers": "b1:9092", "topics": ["b"], "groupId": "g" },
                { "id": "c", "type": "kafka", "brokers": "b1:9092", "topics": ["c"], "groupId": "h" }
              ],
              "destinations": [
                { "id": "es", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "i" }
              ]
            }"#,
        )
        .expect("the bundle loads")
        .topology;
        let names: Vec<String> = source_parts(&topology.sources).into_iter().map(|p| p.name).collect();
        assert_eq!(
            names,
            [
                "source \"a\" inbox",
                "source \"a\" kafka client",
                "source \"b\" inbox",
                "source \"c\" inbox",
                "source \"c\" kafka client",
            ]
        );

        let delivered = every(&topology);
        fit(&mut topology, &delivered, 0, 1024 * MIB, 8, None);
        let fetch: Vec<usize> = topology
            .sources
            .iter()
            .map(|s| match &s.kind {
                SourceKind::Kafka(k) => k.fetch_bytes,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(fetch[0], fetch[1], "one client, one size");
        assert_eq!(fetch[0], fetch[2]);
    }

    #[test]
    fn the_lanes_are_set_aside() {
        let mut topology = bundle();
        let delivered = every(&topology);
        let fitted = fit(&mut topology, &delivered, 16, 4096 * MIB, 32, None);
        assert_eq!(fitted.columnar, (4096 * MIB - FIXED - RESERVES - 64 * MIB) / 2);
    }

    #[test]
    fn with_no_buffer_left_out_the_columnar_budget_takes_its_threads_most() {
        let mut topology = bundle();
        let delivered: Vec<DestId> = topology
            .destinations
            .iter()
            .filter(|d| d.name == "out")
            .map(|d| d.id)
            .collect();
        let fitted = fit(&mut topology, &delivered, 0, 4096 * MIB, 8, None);
        assert_eq!(fitted.columnar, 8 * COLUMNAR_BYTES_PER_THREAD);
        assert_eq!(fitted.pool, 0);
        assert_eq!(
            buffer(&topology, "es"),
            BufferBytes::DEFAULT,
            "an undelivered destination is not sized"
        );
    }

    #[test]
    fn too_little_memory_leaves_the_columnar_budget_at_its_least_and_nothing_to_lend() {
        let mut topology = bundle();
        let delivered = every(&topology);
        let fitted = fit(&mut topology, &delivered, 0, 256 * MIB, 8, None);
        assert_eq!(fitted.columnar, LEAST_COLUMNAR_BYTES);
        assert_eq!(fitted.pool, 0, "each queue still holds its reserve");
    }

    #[test]
    fn a_ceiling_is_above_the_requests_its_destination_starts_with() {
        let topology = bundle();
        for d in &topology.destinations {
            assert!(queue_ceiling(&d.kind) >= d.kind.least_buffer_bytes());
        }
    }

    #[test]
    fn senders_are_cut_toward_their_start_when_they_would_hold_more_than_a_quarter_of_memory() {
        let counts = |t: &TopologyConfig| -> Vec<(usize, usize)> {
            t.destinations
                .iter()
                .filter_map(|d| senders(&d.kind))
                .map(|s| (s.most, s.start))
                .collect()
        };
        let senders_held =
            |t: &TopologyConfig| -> usize { t.destinations.iter().map(|d| senders_bytes(&d.kind)).sum() };

        let mut roomy = mix(1, 30, false);
        let delivered = every(&roomy);
        fit(&mut roomy, &delivered, 0, 32 * 1024 * MIB, 8, None);
        assert!(
            counts(&roomy).iter().all(|&(most, _)| most == 12),
            "{:?}",
            counts(&roomy)
        );

        let memory = 2048 * MIB;
        let mut short = mix(1, 30, false);
        fit(&mut short, &delivered, 0, memory, 8, None);
        assert!(
            counts(&short).iter().all(|&(most, start)| start <= most && most < 12),
            "{:?}",
            counts(&short)
        );
        assert!(senders_held(&short) <= (memory - 2 * BASELINE_BYTES) / 4);

        let mut starved = mix(1, 600, false);
        let delivered = every(&starved);
        fit(&mut starved, &delivered, 0, memory, 8, None);
        assert!(
            counts(&starved).iter().all(|&(most, start)| most == start),
            "never below the start"
        );
    }

    #[test]
    fn a_cut_buffer_is_never_below_what_a_bundle_may_state() {
        let mut t = mix(1, 3, false);
        let small = t
            .destinations
            .iter_mut()
            .find_map(|d| match &mut d.kind {
                DestinationKind::Elasticsearch(e) => Some(e),
                _ => None,
            })
            .expect("the mix has an Elasticsearch destination");
        small.max_bulk_bytes = MIB;
        small.in_flight = 1;
        cut_senders(t.destinations.iter_mut().map(|d| &mut d.kind), 0);
        assert!(t.destinations.iter().all(|d| d.kind.buffer_bytes() >= BufferBytes::MIN));
    }

    fn part(name: &str, bytes: u64) -> Part {
        Part {
            name: name.to_string(),
            part: "lanes",
            owner: Owner::Worker,
            bytes,
        }
    }

    #[test]
    fn parts_over_the_limit_are_overcommitted_by_their_sum() {
        let parts = [part("a", 600), part("b", 500)];
        assert_eq!(overcommitted(&parts, 1000), Some(1100));
    }

    #[test]
    fn parts_at_the_limit_fit() {
        let parts = [part("a", 600), part("b", 400)];
        assert_eq!(overcommitted(&parts, 1000), None);
    }

    #[test]
    fn an_unread_limit_warns_of_nothing() {
        assert!(!warn_if_overcommitted(&[part("a", u64::MAX)], None));
    }

    #[test]
    fn a_kafka_consumer_holds_its_queue_and_half_of_it_per_named_broker() {
        let fetch = 64 * 1024 * 1024;
        assert_eq!(kafka_fetch_ceiling(fetch, "b1:9092"), fetch + fetch / 2);
        assert_eq!(
            kafka_fetch_ceiling(fetch, "b1:9092,b2:9092, b3:9092"),
            fetch + 3 * (fetch / 2)
        );
        assert_eq!(
            kafka_fetch_ceiling(fetch, ""),
            fetch + fetch / 2,
            "it reaches one broker at least"
        );
    }

    #[test]
    fn the_description_names_every_part_largest_first() {
        let parts = [
            part("lanes", 8 * 1024 * 1024),
            part("columnar budget", 1024 * 1024 * 1024),
        ];
        assert_eq!(describe(&parts), "columnar budget 1024 MiB, lanes 8 MiB");
    }

    #[test]
    fn a_deployment_that_fits_is_not_checked_again() {
        assert!(check(vec![part("a", 10)], Some(100)).is_none());
    }

    #[test]
    fn the_warning_is_logged_again_only_once_its_last_log_is_old() {
        let over = check(vec![part("a", 200)], Some(100)).expect("it does not fit");
        let reads = Cell::new(0);
        let limit = || {
            reads.set(reads.get() + 1);
            Some(100)
        };
        over.relog_if_due(limit);
        assert_eq!(reads.get(), 0, "just logged, so not again");

        over.logged.set(
            Instant::now()
                .checked_sub(crate::logs::RELOG_EVERY)
                .expect("the clock reaches back"),
        );
        over.relog_if_due(limit);
        assert_eq!(reads.get(), 1, "due, so the limit is read again");
        over.relog_if_due(limit);
        assert_eq!(reads.get(), 1, "and not due again until another relog has passed");
    }

    /// A bundle of `sources` sources, Kafka on three brokers each or else HTTP receivers, and `destinations`
    /// destinations taking turns Elasticsearch, S3 and Kafka, none stating `bufferBytes`.
    fn mix(sources: usize, destinations: usize, receiving: bool) -> TopologyConfig {
        let sources: Vec<String> = (0..sources)
            .map(|i| match receiving {
                false => format!(
                    r#"{{ "id": "in{i}", "type": "kafka", "brokers": "b1:9092,b2:9092,b3:9092", "topics": ["t{i}"], "groupId": "g" }}"#
                ),
                true => format!(r#"{{ "id": "in{i}", "type": "httpReceiver", "port": {}, "records": "lines", "tokens": {{ "t": "T" }} }}"#, 20000 + i),
            })
            .collect();
        let destinations: Vec<String> = (0..destinations)
            .map(|i| match i % 3 {
                0 => format!(
                    r#"{{ "id": "es{i}", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "i" }}"#
                ),
                1 => format!(r#"{{ "id": "lake{i}", "type": "s3", "bucket": "b", "format": "parquet" }}"#),
                _ => format!(r#"{{ "id": "out{i}", "type": "kafka", "brokers": "b1:9092", "defaultTopic": "t" }}"#),
            })
            .collect();
        crate::config_file::load_str(&format!(
            r#"{{ "version": 1, "sources": [{}], "destinations": [{}] }}"#,
            sources.join(","),
            destinations.join(",")
        ))
        .expect("the bundle loads")
        .topology
    }

    /// Every part a deployment of `topology`, all of whose destinations the worker fitted, holds at most: the fixed
    /// parts, each queue's reserve, the pool and the columnar budget.
    fn held(topology: &TopologyConfig, lane_batches: usize, fitted: Fit) -> u64 {
        2 * BASELINE_BYTES as u64
            + topology
                .sources
                .iter()
                .filter_map(|s| receiver_reserve(&s.kind))
                .sum::<usize>() as u64
            + source_parts(topology.sources.iter().filter(|s| receiver_reserve(&s.kind).is_none()))
                .iter()
                .map(|p| p.bytes)
                .sum::<u64>()
            + fitted.receivers as u64
            + (lane_batches * fitted.batch_bytes) as u64
            + topology
                .destinations
                .iter()
                .map(|d| (fitted.batch_bytes * queue_charge(&d.kind) + senders_bytes(&d.kind)) as u64)
                .sum::<u64>()
            + fitted.pool as u64
            + fitted.columnar as u64
    }

    /// Sizes mixes of up to 100 sources and 20 destinations, and of 1000 sources and destinations together, to 2 to
    /// 32 GiB on 2 to 32 threads, the sources Kafka or receivers. Wherever the fixed parts and every part's least leave room, what the deployment
    /// holds stays within its memory. Run with `--nocapture` for the table.
    #[test]
    fn every_likely_mix_fits_its_memory() {
        println!("sources dests threads memory | batch KiB fixed reserves columnar fetch pool receivers | held fits");
        let mixes = [1, 10, 100]
            .into_iter()
            .flat_map(|s| [1, 3, 20].map(|d| (s, d)))
            .chain([(800, 200), (500, 500), (200, 800)]);
        let mixes = mixes.flat_map(|(s, d)| [(s, d, false), (s, d, true)]);
        for (sources, destinations, receiving) in mixes {
            for threads in [2, 8, 32] {
                for gib in [2, 4, 8, 16, 32] {
                    let memory = gib * 1024 * MIB;
                    let mut topology = mix(sources, destinations, receiving);
                    let delivered = every(&topology);
                    let lane_batches = sources * super::super::lane_depth(threads, sources);
                    let fitted = fit(&mut topology, &delivered, lane_batches, memory, threads, None);
                    let fixed = 2 * BASELINE_BYTES as u64 + (lane_batches * fitted.batch_bytes) as u64;
                    let reserves: u64 = topology
                        .destinations
                        .iter()
                        .map(|d| (fitted.batch_bytes * queue_charge(&d.kind) + senders_bytes(&d.kind)) as u64)
                        .sum::<u64>()
                        + topology
                            .sources
                            .iter()
                            .filter_map(|s| receiver_reserve(&s.kind))
                            .sum::<usize>() as u64;
                    let total = held(&topology, lane_batches, fitted);
                    let least = fixed
                        + reserves
                        + LEAST_COLUMNAR_BYTES as u64
                        + fetch_claims(
                            topology
                                .sources
                                .iter()
                                .filter(|s| topology.fitted.sources.contains(&s.id))
                                .map(|s| &s.kind),
                        )
                        .iter()
                        .map(|c| c.halves * c.least as u64 / 2)
                        .sum::<u64>();
                    println!(
                        "{sources:>6}{} {destinations:>5} {threads:>7} {gib:>4}GiB | {:>9} {:>5} {:>8} {:>8} {:>5} {:>5} {:>9} | {:>5} {}",
                        if receiving { "r" } else { "k" },
                        fitted.batch_bytes / 1024,
                        fixed / MIB as u64,
                        reserves / MIB as u64,
                        fitted.columnar / MIB,
                        match &topology.sources[0].kind {
                            SourceKind::Kafka(k) => k.fetch_bytes / MIB,
                            _ => 0,
                        },
                        fitted.pool / MIB,
                        fitted.receivers / MIB,
                        total / MIB as u64,
                        total <= memory as u64,
                    );
                    if least <= memory as u64 {
                        assert!(
                            total <= memory as u64,
                            "{sources} sources, {destinations} destinations, {threads} threads in {gib} GiB hold {} MiB",
                            total / MIB as u64
                        );
                    }
                }
            }
        }
    }
}
